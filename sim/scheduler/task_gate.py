"""整图源端可交付性门控 + CGS：有完整余量才保留源端，否则允许协同卸载。

把所有待派发兄弟片一起报价，结果总量按单包估算作为保守候选；实际结果仍逐块
中继。30秒级余量来自接触扫描精度，不读取目标真值或未来任务。门控是启发式，
不保证未来未知竞争下的完成；按时性优先可能牺牲已按时任务的部分时延。
"""

from collections import Counter
from dataclasses import replace
import math

from sim.entities import Action, ActionType
from sim.scheduler.contact_graph import quantize
from sim.scheduler.delivery_guard import CGSDeliveryGuard
from sim.scheduler.literature import ground_node


def whole_service(state, tiles, source):
    profile = state.config.get("onboard_compute_profile")
    count = sum(tile["compute_cost"] for tile in tiles)
    if profile is None or not float(count).is_integer():
        if state.config.get("compute_service_mode") != "work_conserving":
            return sum(quantize(tile["compute_cost"] / state.satellites[source]["compute_rate"],
                                state.config["dt"]) for tile in tiles)
        return count / state.satellites[source]["compute_rate"]
    remaining = int(count)
    sizes = sorted((int(key) for key in profile["service_s"]), reverse=True)
    seconds = 0.0
    while remaining:
        size = next(size for size in sizes if size <= remaining)
        seconds += profile["service_s"][str(size)]
        remaining -= size
    return seconds


class CGSTaskGate(CGSDeliveryGuard):
    def __init__(self, sla_seconds=300.0):
        super().__init__()
        if not math.isfinite(sla_seconds) or sla_seconds <= 0:
            raise ValueError("sla_seconds must be finite and positive")
        self.sla_seconds = sla_seconds
        self.last_prediction = {}

    def select_actions(self, state):
        active = {tid: tile for tid, tile in state.tiles.items()
                  if tile["state"] == "QUEUED" and not tile["in_transfer"] and not tile["task_progress"]["failed"]}
        if not active:
            return super().select_actions(state)
        tasks = {tile["parent_task_id"] for tile in active.values()}
        pending = {tid: tile for task, rows in state.pending_task_tiles.items() if task in tasks
                   for tid, tile in rows.items() if tile["state"] == "QUEUED" and not tile["in_transfer"]}
        pending.update(active)
        groups = {}
        for tid, tile in sorted(pending.items(), key=lambda row: (-row[1]["waiting_time"], row[0])):
            groups.setdefault(tile["parent_task_id"], []).append(tid)
        plan = self._contact_plan(state)
        ground = {ground_node(gid) for gid in state.ground_stations}
        busy = {sid: row["committed_compute_s"] for sid, row in state.satellites.items()}
        extra_busy = Counter()
        chosen, predictions = [], {}
        dt = state.config["dt"]
        hops = state.config.get("relay_max_hops", 8) if state.config.get("result_relay") else 0
        margin = state.config.get("contact_scan_s", 0)
        interval = state.config.get("decision_interval_seconds", 1.0)
        limit = state.config.get("decision_tile_limit_per_sat", 4)
        for task, ids in groups.items():
            tiles = [pending[tid] for tid in ids]
            sources = {tile["location"] for tile in tiles}
            if len(sources) != 1 or any(tile["vram_req_gb"] > state.config["vram_capacity_gb"] for tile in tiles):
                self.stats["gate_ineligible"] += 1
                continue
            source = next(iter(sources))
            hidden = sum(tid not in active for tid in ids)
            release = math.ceil(hidden / limit) * interval
            service = whole_service(state, tiles, source)
            ready = max(busy[source], release) + service
            size = sum(tile["result_size_mb"] for tile in tiles)
            route = plan.delivery(source, ground, quantize(ready, dt), size, dt=dt, max_hops=hops + 1)
            age = max(tile["waiting_time"] for tile in tiles)
            latency = None if route is None else age + route[0] + state.config.get("postprocessing_delay_s", 0)
            # 已派发兄弟片的尾部不可从当前摘要恢复；门控只对仍在源端的剩余部分报价。
            safe = latency is not None and latency + margin <= self.sla_seconds + 1e-9
            predictions[task] = {"remaining_tiles": len(ids), "local_latency_seconds": latency,
                                 "margin_seconds": margin, "selected_local": safe}
            if not safe:
                self.stats["gate_cgs_tasks"] += 1
                continue
            plan.reserve(route[1], size)
            busy[source] = ready
            visible = [tid for tid in ids if tid in active]
            chosen.extend(visible)
            extra_busy[source] += whole_service(state, [pending[tid] for tid in visible], source)
            self.stats["gate_local_tasks"] += 1
            self.stats["gate_local_tiles"] += len(visible)
        selected = set(chosen)
        # 同轮先接纳的本地工作也要进入CGS排队报价，不能给后续卸载免费算力。
        satellites = {sid: {**row, "committed_compute_s": row["committed_compute_s"] + extra_busy[sid]}
                      for sid, row in state.satellites.items()}
        rest = replace(state, satellites=satellites,
                       tiles={tid: tile for tid, tile in state.tiles.items() if tid not in selected})
        actions = [Action(tid, ActionType.LOCAL) for tid in chosen]
        actions.extend(self._select_actions_with_plan(rest, plan))
        self.last_prediction = predictions
        return actions
