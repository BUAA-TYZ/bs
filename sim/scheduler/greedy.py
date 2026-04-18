from __future__ import annotations

from typing import List, Optional, Tuple

from sim.entities import Action, ActionType, EnvState
from sim.topology import link_key
from sim.scheduler.base import SchedulerPolicy


class GreedyEarliestFinish(SchedulerPolicy):
    """贪心最早完成时间策略（修复版）。

    修复的核心问题：
    1. 邻居估算缺少下传时间：卫星计算完后必须把 result 下传到地面才算 DONE，
       策略原来只算「传输 + 邻居计算」，忽略了邻居计算完成后仍需下传到地面站
       的时间，导致严重低估邻居卸载的实际完成时间，反而让无法卸载到地面的邻居
       看起来「很快」，占用了网络带宽却无法完成任务。

    2. 2跳路径（sat→nb→GS）：当某个邻居卫星可见地面站时，应把数据先传给邻居，
       再由邻居上传到地面站计算（地面计算完直接 DONE，不需额外下传），总时间往往
       远短于在卫星本地计算+下传。原策略不计算这条路径。

    3. 队列等待时间低估：原代码用 queue_len * dt 估算等待，但每个 tile 实际
       计算时间是 compute_cost / compute_rate（通常远大于 dt），导致拥挤队列
       被严重低估，LOCAL 显得比实际快得多。

    4. LOCAL 缺少下传时间：LOCAL 计算完后仍需下传 result 到地面，
       若当前卫星不可见地面站，还需等待可见性窗口，原策略完全忽略。
    """

    def select_actions(self, env_state: EnvState) -> List[Action]:
        actions: List[Action] = []
        dt = env_state.config["dt"]

        for tile_id, tile in env_state.tiles.items():
            if tile["state"] not in ("QUEUED", "READY"):
                continue
            src = tile["location"]
            if tile["in_transfer"]:
                continue

            best_action = Action(tile_id=tile_id, action_type=ActionType.WAIT)
            best_finish = float("inf")

            tile_size_mb = tile["data_size_mb"]
            compute_cost = tile["compute_cost"]

            # ------------------------------------------------------------------
            # 工具函数：估算在某卫星本地计算后下传 result 的完成时间
            # ------------------------------------------------------------------
            def _sat_finish_with_downlink(sat_id: int, extra_tx: float) -> float:
                """估算 tile 送到 sat_id 后的完整完成时间（含下传到地面）。

                extra_tx: 从当前卫星传到 sat_id 的传输时间（LOCAL 时为 0）。
                返回: extra_tx + 队列等待 + 计算 + 下传 result 到地面的时间。
                若 sat_id 当前不可见任何地面站，则对下传估算一个保守惩罚。
                """
                sat = env_state.satellites[sat_id]
                # 修复3：用 compute_cost/compute_rate 替代 queue_len*dt
                single_tile_compute = compute_cost / sat["compute_rate"]
                queue_wait = sat["queue_len"] * single_tile_compute
                compute_time = compute_cost / sat["compute_rate"]

                # 下传 result（只有几 kB~MB，时间极短），估算地面可见性
                gs_opts = env_state.ground_options.get(sat_id, [])
                if gs_opts:
                    # 取带宽最好的地面站做下传估算（result 很小，影响不大）
                    best_dl_bw = max(o["bandwidth_mbps"] for o in gs_opts)
                    # result_size 未暴露在 env_state，这里用一个极保守的 1 MB 估算
                    # 实际 result_size_mb 通常 <= 0.1 MB，这里的误差可忽略
                    result_size_mb_est = 1.0
                    downlink_time = (result_size_mb_est * 8.0) / max(1e-6, best_dl_bw)
                else:
                    # 暂时不可见地面站：给一个惩罚，让调度器倾向能直接看到地面的方案
                    # 不使用 inf，避免因为瞬间不可见就完全放弃 LOCAL
                    downlink_time = 60.0  # 保守估计需等约 60s 才能下传

                return extra_tx + queue_wait + compute_time + downlink_time

            # ------------------------------------------------------------------
            # 选项 A：LOCAL（在源卫星本地计算，完成后下传 result）
            # ------------------------------------------------------------------
            local_finish = _sat_finish_with_downlink(src, 0.0)
            if local_finish < best_finish:
                best_finish = local_finish
                best_action = Action(tile_id=tile_id, action_type=ActionType.LOCAL)

            # ------------------------------------------------------------------
            # 选项 B：1跳卸载到邻居卫星，邻居在本地计算后下传
            # 选项 C：2跳路径：先传到可见地面站的邻居，再由邻居上传地面计算
            #         （地面计算完直接 DONE，无需再下传，通常最快）
            # ------------------------------------------------------------------
            for nb in env_state.neighbors.get(src, []):
                lk = env_state.links.get(link_key(src, nb))
                if not lk or not lk["up"]:
                    continue
                bw_isl = lk["bandwidth_mbps"]
                tx1 = (tile_size_mb * 8.0) / max(1e-6, bw_isl)  # sat→nb 传输时间

                # 选项 B：邻居卫星计算 + 下传
                nb_finish = _sat_finish_with_downlink(nb, tx1)
                if nb_finish < best_finish:
                    best_finish = nb_finish
                    best_action = Action(
                        tile_id=tile_id,
                        action_type=ActionType.OFFLOAD,
                        target_sat_id=nb,
                    )

                # 选项 C：2跳到地面（sat→nb→GS→地面计算，直接 DONE）
                for opt in env_state.ground_options.get(nb, []):
                    gs_id = opt["gs_id"]
                    gs_bw = opt["bandwidth_mbps"]
                    tx2 = (tile_size_mb * 8.0) / max(1e-6, gs_bw)  # nb→GS 上传时间
                    gs = env_state.ground_stations.get(gs_id, {})
                    gs_queue = gs.get("queue_len", 0) + gs.get("running", 0)
                    gs_rate = max(1e-9, gs.get("compute_rate", 1.0))
                    # 修复3：地面队列等待也用实际计算时间估算
                    single_gs_compute = compute_cost / gs_rate
                    gs_queue_wait = gs_queue * single_gs_compute
                    gs_compute = compute_cost / gs_rate
                    # 地面计算完直接 DONE，无需下传
                    finish_2hop = tx1 + gs_queue_wait + tx2 + gs_compute
                    if finish_2hop < best_finish:
                        best_finish = finish_2hop
                        # 先把数据卸载给邻居，邻居会继续转发给 GS
                        # 注意：这里我们选择 OFFLOAD 给邻居卫星，
                        # 邻居收到后下一步策略会决定发送给地面站
                        best_action = Action(
                            tile_id=tile_id,
                            action_type=ActionType.OFFLOAD,
                            target_sat_id=nb,
                        )

            # ------------------------------------------------------------------
            # 选项 D：直接 1跳卸载到地面站（源卫星直接可见地面站时）
            # ------------------------------------------------------------------
            for opt in env_state.ground_options.get(src, []):
                gs_id = opt["gs_id"]
                gs_bw = opt["bandwidth_mbps"]
                tx_gs = (tile_size_mb * 8.0) / max(1e-6, gs_bw)  # 上传时间
                gs = env_state.ground_stations.get(gs_id, {})
                gs_queue = gs.get("queue_len", 0) + gs.get("running", 0)
                gs_rate = max(1e-9, gs.get("compute_rate", 1.0))
                # 修复3：地面队列等待用实际计算时间
                single_gs_compute = compute_cost / gs_rate
                gs_queue_wait = gs_queue * single_gs_compute
                gs_compute = compute_cost / gs_rate
                # 地面计算完直接 DONE
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
