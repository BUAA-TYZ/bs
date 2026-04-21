"""地面站整图计算 pipeline (ground_compute 模式)。

数据流：
  任务到达后整图排队上传 →
  卫星可见时上传至地面站 (start_ground_task_uploads / advance_ground_task_uploads) →
  地面站计算 (advance_ground_task_compute)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

from sim.entities import (
    FailureReason,
    GroundTaskCompute,
    GroundTaskTransfer,
    Task,
    Tile,
    TileState,
)

if TYPE_CHECKING:
    from sim.env import SimulationEnv


class GroundPipeline:
    """封装 ground_compute 模式的所有推进逻辑，持有对 env 的引用。"""

    def __init__(self, env: "SimulationEnv") -> None:
        self.env = env

    # ------------------------------------------------------------------ #
    # 公共入口（由 env.step 调用）                                          #
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        self.start_ground_task_uploads()
        self.advance_ground_task_uploads()
        self.advance_ground_task_compute()

    # ------------------------------------------------------------------ #
    # 任务入队（由 env._task_arrivals 调用）                                #
    # ------------------------------------------------------------------ #

    def enqueue_ground_task(self, task: Task, tiles: List[Tile]) -> None:
        env = self.env
        src_sat = env.satellites[task.source_sat_id]
        required_gb = task.image_size_mb / 1024.0
        if src_sat.mem_used_gb + required_gb > src_sat.mem_capacity_gb:
            for tile in tiles:
                env._fail_tile(tile, FailureReason.MEM_FULL)
            return
        src_sat.mem_used_gb += required_gb
        env.task_source_mem_gb[task.task_id] = required_gb
        for tile in tiles:
            tile.state = TileState.QUEUED
            env._log_tile_event(
                tile, "queued", sat_from=None, sat_to=task.source_sat_id
            )
        env.ground_task_queued.setdefault(task.source_sat_id, []).append(task.task_id)

    # ------------------------------------------------------------------ #
    # 上传启动                                                              #
    # ------------------------------------------------------------------ #

    def start_ground_task_uploads(self) -> None:
        env = self.env
        for sat_id, task_ids in env.ground_task_queued.items():
            if not task_ids:
                continue
            gs = env._select_ground_station(sat_id)
            if gs is None:
                continue
            still_waiting: List[str] = []
            for task_id in task_ids:
                if not self._task_has_pending_tiles(task_id):
                    continue
                if task_id in env.ground_task_transfering:
                    still_waiting.append(task_id)
                    continue
                task = env.tasks[task_id]
                tr = GroundTaskTransfer(
                    task_id=task_id,
                    src_sat=sat_id,
                    gs_id=gs.gs_id,
                    remaining_mb=task.image_size_mb,
                    start_time=env.time,
                )
                env.ground_task_transfers.append(tr)
                env.ground_task_transfering.add(task_id)
                env._log_task_event(
                    task_id,
                    "ground_upload_start",
                    sat_from=sat_id,
                    sat_to=gs.gs_id,
                )
            env.ground_task_queued[sat_id] = still_waiting

    # ------------------------------------------------------------------ #
    # 上传推进                                                              #
    # ------------------------------------------------------------------ #

    def advance_ground_task_uploads(self) -> None:
        env = self.env
        still: List[GroundTaskTransfer] = []
        for tr in env.ground_task_transfers:
            gs = env.ground_stations.get(tr.gs_id)
            if gs is None:
                still.append(tr)
                continue
            if not env.vis_cache.is_visible(tr.src_sat, tr.gs_id, env.ground_stations):
                still.append(tr)
                continue
            sent = (gs.bandwidth_mbps / 8.0) * env.cfg.dt
            tr.remaining_mb -= sent
            if tr.remaining_mb <= 1e-6:
                env.ground_task_transfering.discard(tr.task_id)
                src_mem = env.task_source_mem_gb.pop(tr.task_id, 0.0)
                src_sat = env.satellites[tr.src_sat]
                src_sat.mem_used_gb = max(0.0, src_sat.mem_used_gb - src_mem)
                env.ground_task_queued.setdefault(tr.src_sat, [])
                env.ground_task_queued[tr.src_sat] = [
                    tid
                    for tid in env.ground_task_queued[tr.src_sat]
                    if tid != tr.task_id
                ]
                self._enqueue_ground_task_compute(tr.task_id, tr.gs_id)
                env._log_task_event(
                    tr.task_id,
                    "ground_upload_end",
                    sat_from=tr.src_sat,
                    sat_to=tr.gs_id,
                )
            else:
                still.append(tr)
        env.ground_task_transfers = still

    def _enqueue_ground_task_compute(self, task_id: str, gs_id: str) -> None:
        env = self.env
        if not self._task_has_pending_tiles(task_id):
            return
        env.ground_compute_queue.setdefault(gs_id, []).append(task_id)

    # ------------------------------------------------------------------ #
    # 地面计算推进                                                          #
    # ------------------------------------------------------------------ #

    def advance_ground_task_compute(self) -> None:
        env = self.env
        # 调度空闲地面站
        for gs_id, gs in env.ground_stations.items():
            if gs_id in env.ground_task_running:
                continue
            queue = env.ground_compute_queue.setdefault(gs_id, [])
            if not queue:
                continue
            picked_task = queue.pop(0)
            if not self._task_has_pending_tiles(picked_task):
                continue
            task = env.tasks[picked_task]
            total_compute = env.cfg.compute_cost_per_tile * task.num_tiles
            remaining = total_compute / max(1e-9, gs.compute_rate)
            env.ground_task_running[gs_id] = GroundTaskCompute(
                task_id=picked_task,
                gs_id=gs_id,
                remaining_compute=remaining,
                start_time=env.time,
            )
            env._log_task_event(
                picked_task,
                "ground_compute_start",
                sat_from=gs_id,
                sat_to=gs_id,
            )

        # 推进正在运行的任务
        done_gs: List[str] = []
        for gs_id, comp in env.ground_task_running.items():
            comp.remaining_compute = max(0.0, comp.remaining_compute - env.cfg.dt)
            if comp.remaining_compute > 1e-6:
                continue
            task = env.tasks[comp.task_id]
            for tid in task.tile_ids:
                tile = env.tiles[tid]
                tile.state = TileState.DONE
                tile.timestamps.end_compute = env.time
                env.metrics.record_tile_latency(
                    (tile.timestamps.end_compute or env.time) - tile.timestamps.created
                )
                env._log_tile_event(
                    tile, "done", sat_from=task.source_sat_id, sat_to=comp.gs_id
                )
            env._check_task_done(comp.task_id)
            env._log_task_event(
                comp.task_id,
                "ground_compute_end",
                sat_from=comp.gs_id,
                sat_to=comp.gs_id,
            )
            done_gs.append(gs_id)
        for gs_id in done_gs:
            env.ground_task_running.pop(gs_id, None)

    # ------------------------------------------------------------------ #
    # 辅助                                                                  #
    # ------------------------------------------------------------------ #

    def _task_has_pending_tiles(self, task_id: str) -> bool:
        env = self.env
        task = env.tasks[task_id]
        for tid in task.tile_ids:
            if env.tiles[tid].state not in (TileState.DONE, TileState.FAILED):
                return True
        return False
