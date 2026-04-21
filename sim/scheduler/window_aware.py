"""窗口感知贪心调度器（WindowAwareGreedy）。

在 GreedyEarliestFinish 基础上的关键改进：
  - 利用 export_state 中 satellites[sat_id]["next_gs_windows"] 字段
    精确估算下传等待时间，替换原来固定 60s 的保守惩罚。
  - 若当前不可见任何地面站，从过境窗口预测中取最近一次窗口的 starts_in_s
    作为等待时间，再叠加 result 下传时间，得到更准确的完成时间估算。
  - 过境期间（starts_in_s == 0 或已在窗口内），等待时间为 0。
  - 若当前和预测窗口内均无可见地面站（返回 None），则使用最大惩罚值，
    让调度器优先选择其他路径，但不强制 WAIT。

其余逻辑（ISL 1跳、2跳路径、直接卸载到地面站）与 GreedyEarliestFinish 相同。
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from sim.entities import Action, ActionType, EnvState
from sim.scheduler.base import SchedulerPolicy
from sim.topology import link_key


# 无过境窗口时的保守惩罚（秒），应远大于正常轨道周期的一半
_NO_WINDOW_PENALTY_S = 5400.0  # 1.5h，超过 LEO 轨道半个周期


class WindowAwareGreedy(SchedulerPolicy):
    """利用过境窗口预测提升下传等待估算精度的贪心策略。

    与 GreedyEarliestFinish 接口完全兼容，可直接替换进 run_mode_compare.py。
    """

    def select_actions(self, env_state: EnvState) -> List[Action]:
        actions: List[Action] = []
        dt = env_state.config["dt"]
        result_size_mb = env_state.config.get("result_size_mb", 1.0)

        for tile_id, tile in env_state.tiles.items():
            if tile["state"] not in ("QUEUED", "READY"):
                continue
            if tile["in_transfer"]:
                continue

            src = tile["location"]
            tile_size_mb = tile["data_size_mb"]
            compute_cost = tile["compute_cost"]

            best_action = Action(tile_id=tile_id, action_type=ActionType.WAIT)
            best_finish = float("inf")

            # ── 工具函数 ─────────────────────────────────────────────────
            def _downlink_wait(sat_id: int) -> float:
                """估算 sat_id 下传 result 到地面的等待时间（秒）。

                利用 next_gs_windows 预测：
                  - 若已在过境窗口内（starts_in_s == 0）：等待时间 = 0
                  - 若有预测窗口：等待时间 = starts_in_s（窗口开始前的秒数）
                  - 若无任何窗口预测：使用保守惩罚值
                """
                gs_opts = env_state.ground_options.get(sat_id, [])
                if gs_opts:
                    # 已在可见窗口内，等待时间为 0
                    return 0.0

                windows: Dict[str, Optional[Tuple]] = env_state.satellites.get(
                    sat_id, {}
                ).get("next_gs_windows", {})
                min_wait = _NO_WINDOW_PENALTY_S
                for gs_id, win in windows.items():
                    if win is None:
                        continue
                    starts_in_s, duration_s = win
                    if starts_in_s <= 0:
                        # 刚好在窗口边界或已过境，视为可立即下传
                        return 0.0
                    min_wait = min(min_wait, starts_in_s)
                return min_wait

            def _best_downlink_bw(sat_id: int) -> float:
                """取 sat_id 当前可见地面站中的最大带宽（Mbps）。

                若当前不可见，从 ground_stations 中取全局最大带宽（保守估算）。
                """
                gs_opts = env_state.ground_options.get(sat_id, [])
                if gs_opts:
                    return max(o["bandwidth_mbps"] for o in gs_opts)
                # 不可见时：用全局最大带宽（保守估算，实际可能更低）
                if env_state.ground_stations:
                    return max(
                        gs["bandwidth_mbps"]
                        for gs in env_state.ground_stations.values()
                    )
                return 100.0  # fallback

            def _sat_finish_with_downlink(sat_id: int, extra_tx: float) -> float:
                """估算 tile 送到 sat_id 后的完整完成时间（含下传）。

                extra_tx: 从当前卫星传到 sat_id 的传输时间（LOCAL 时为 0）。
                """
                sat = env_state.satellites[sat_id]
                single_tile_compute = compute_cost / max(1e-9, sat["compute_rate"])
                queue_wait = sat["queue_len"] * single_tile_compute
                compute_time = single_tile_compute

                wait_for_window = _downlink_wait(sat_id)
                dl_bw = _best_downlink_bw(sat_id)
                downlink_time = (result_size_mb * 8.0) / max(1e-6, dl_bw)

                return (
                    extra_tx
                    + queue_wait
                    + compute_time
                    + wait_for_window
                    + downlink_time
                )

            # ── 选项 A：LOCAL ─────────────────────────────────────────────
            local_finish = _sat_finish_with_downlink(src, 0.0)
            if local_finish < best_finish:
                best_finish = local_finish
                best_action = Action(tile_id=tile_id, action_type=ActionType.LOCAL)

            # ── 选项 B/C：1跳 ISL 卸载到邻居（B），或 2跳到地面（C）─────
            for nb in env_state.neighbors.get(src, []):
                lk = env_state.links.get(link_key(src, nb))
                if not lk or not lk["up"]:
                    continue
                bw_isl = lk["bandwidth_mbps"]
                tx1 = (tile_size_mb * 8.0) / max(1e-6, bw_isl)

                # 选项 B：邻居卫星计算 + 等待窗口 + 下传
                nb_finish = _sat_finish_with_downlink(nb, tx1)
                if nb_finish < best_finish:
                    best_finish = nb_finish
                    best_action = Action(
                        tile_id=tile_id,
                        action_type=ActionType.OFFLOAD,
                        target_sat_id=nb,
                    )

                # 选项 C：sat→nb→GS 地面计算（直接 DONE，无需下传）
                for opt in env_state.ground_options.get(nb, []):
                    gs_id = opt["gs_id"]
                    gs_bw = opt["bandwidth_mbps"]
                    tx2 = (tile_size_mb * 8.0) / max(1e-6, gs_bw)
                    gs = env_state.ground_stations.get(gs_id, {})
                    gs_queue = gs.get("queue_len", 0) + gs.get("running", 0)
                    gs_rate = max(1e-9, gs.get("compute_rate", 1.0))
                    single_gs_compute = compute_cost / gs_rate
                    gs_queue_wait = gs_queue * single_gs_compute
                    gs_compute = single_gs_compute
                    finish_2hop = tx1 + tx2 + gs_queue_wait + gs_compute
                    if finish_2hop < best_finish:
                        best_finish = finish_2hop
                        best_action = Action(
                            tile_id=tile_id,
                            action_type=ActionType.OFFLOAD,
                            target_sat_id=nb,
                        )

            # ── 选项 D：直接 1跳到地面站（源卫星当前可见）────────────────
            for opt in env_state.ground_options.get(src, []):
                gs_id = opt["gs_id"]
                gs_bw = opt["bandwidth_mbps"]
                tx_gs = (tile_size_mb * 8.0) / max(1e-6, gs_bw)
                gs = env_state.ground_stations.get(gs_id, {})
                gs_queue = gs.get("queue_len", 0) + gs.get("running", 0)
                gs_rate = max(1e-9, gs.get("compute_rate", 1.0))
                single_gs_compute = compute_cost / gs_rate
                gs_queue_wait = gs_queue * single_gs_compute
                gs_compute = single_gs_compute
                finish_gs = gs_queue_wait + tx_gs + gs_compute
                if finish_gs < best_finish:
                    best_finish = finish_gs
                    best_action = Action(
                        tile_id=tile_id,
                        action_type=ActionType.OFFLOAD,
                        target_gs_id=gs_id,
                    )

            actions.append(best_action)

        return actions
