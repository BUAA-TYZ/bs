"""同观测权限的保守预测启发式：考虑到达后的窗口，READY 分片继续执行。"""

import math

from sim.entities import Action, ActionType
from sim.marl.decision import candidate_neighbors
from sim.topology import link_key


class PredictivePolicy:
    def select_actions(self, state):
        actions = []
        dt = state.config["dt"]
        continuous_compute = state.config.get("compute_service_mode") == "work_conserving"
        for tid, tile in state.tiles.items():
            src = tile["location"]
            if tile["in_transfer"]:
                continue
            local = Action(tid, ActionType.LOCAL)
            if tile["state"] == "READY":
                actions.append(local)
                continue
            count = max(1, tile["task_pending_count"])
            pixel_workload = bool(state.config.get("image_tiling"))
            group = None
            if pixel_workload:
                # 只预测同来源尚未承诺的兄弟片，边缘窗口不再按当前片复制成本。
                group = {other_id: other for other_id, other in
                         state.pending_task_tiles.get(tile["parent_task_id"], {}).items()
                         if other["location"] == src and other["state"] == "QUEUED"
                         and not other["in_transfer"]}
                group[tid] = tile
                count = len(group)
            input_mb = (sum(other["data_size_mb"] for other in group.values())
                        if group is not None else count * tile["data_size_mb"])
            input_gb = (sum(other["data_size_gb"] for other in group.values())
                        if group is not None else count * tile["data_size_gb"])
            result_mb = (sum(other["result_size_mb"] for other in group.values())
                         if group is not None else count * tile["result_size_mb"])

            def group_service(rate):
                return sum(other["compute_cost"] / max(1e-6, rate) if continuous_compute else
                           max(dt, math.ceil(other["compute_cost"] / max(1e-6, rate) / dt) * dt)
                           for other in group.values())

            def finish(sid, tx):
                sat = state.satellites[sid]
                service = tile["compute_cost"] / max(1e-6, sat["compute_rate"])
                if not continuous_compute:
                    service = math.ceil(service / dt) * dt
                queued = max(0, sat["queue_len"] - (count if sid == src else 0))
                ready = tx + sat.get("executing_remaining_s", 0) + (queued + count) * service
                if pixel_workload:
                    ready = (tx + sat.get("committed_compute_s", sat.get("executing_remaining_s", 0))
                             + group_service(sat["compute_rate"]))
                if continuous_compute:
                    ready = math.ceil(ready / dt - 1e-12) * dt
                best = float("inf")
                for gs_id, window in sat.get("next_gs_windows", {}).items():
                    if window is None:
                        continue
                    gs = state.ground_stations[gs_id]
                    # 与 actor 相同的当前竞争估计，不读取未来带宽或负载。
                    share = min(1, gs["receive_channels"] / max(1, gs.get("visible_contenders", 1)))
                    transfer = (result_mb +
                                sat.get("ground_backlog_mb", {}).get(gs_id, 0)) * 8 / max(1e-6, gs["bandwidth_mbps"] * share)
                    end = max(ready, window[0]) + max(dt, transfer)
                    if end <= window[0] + window[1]:
                        best = min(best, end)
                return best

            best, action = finish(src, 0), local
            for nb in candidate_neighbors(state, src):
                if state.satellites[nb]["mem_remaining_gb"] < input_gb:
                    continue
                link = state.links[link_key(src, nb)]
                tx = (input_mb + state.satellites[src].get("isl_backlog_mb", {}).get(nb, 0)) * 8 / max(1e-6, link["bandwidth_mbps"])
                windows = link.get("contact_windows", [])
                remaining = next((duration for start, duration in windows if start == 0), 0)
                if tx > remaining:
                    continue  # 启发式保守选择，环境仍允许跨窗口续传。
                value = finish(nb, math.ceil(tx / dt) * dt)
                if value + 2 * dt < best:
                    best, action = value, Action(tid, ActionType.OFFLOAD, target_sat_id=nb)
            for option in state.ground_options.get(src, []):
                gs_id = option["gs_id"]
                gs = state.ground_stations[gs_id]
                window = state.satellites[src].get("next_gs_windows", {}).get(gs_id)
                share = min(1, gs["receive_channels"] / max(1, gs.get("visible_contenders", 1)))
                tx = input_mb * 8 / max(1e-6, gs["bandwidth_mbps"] * share)
                if window is None or tx > window[1]:
                    continue
                value = tx + (gs["queue_len"] + gs["running"] + count) * max(dt, tile["compute_cost"] / gs["compute_rate"])
                if pixel_workload:
                    value = tx + gs.get("committed_compute_s", 0) + group_service(gs["compute_rate"])
                if continuous_compute:
                    service = (group_service(gs["compute_rate"]) if pixel_workload else
                               count * tile["compute_cost"] / gs["compute_rate"])
                    value = math.ceil(tx / dt - 1e-12) * dt + gs.get("committed_compute_s", 0) + service
                    value = math.ceil(value / dt - 1e-12) * dt
                if value + 2 * dt < best:
                    best, action = value, Action(tid, ActionType.OFFLOAD, target_gs_id=gs_id)
            actions.append(action)
        return actions
