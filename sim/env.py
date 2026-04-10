from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import random

import numpy as np

from sim.config import SimConfig
from sim.entities import (
    Action,
    ActionType,
    DownlinkTransfer,
    EnvState,
    FailureReason,
    GroundTileTransfer,
    GroundTaskCompute,
    GroundTaskTransfer,
    GroundStation,
    Satellite,
    Task,
    Tile,
    TileState,
    TileTimestamps,
    Transfer,
)
from sim.metrics import Metrics
from sim.lifecycle import TileLifecycleLogger
from sim.orbit import build_ground_station, sat_to_ground_geometry
from sim.topology import TopologyConfig, TopologyModel, link_key


@dataclass
class StepResult:
    time: int
    metrics: Metrics


class SimulationEnv:
    def __init__(self, cfg: SimConfig) -> None:
        self.cfg = cfg
        self.time = 0
        self.rng = np.random.default_rng(cfg.seed)
        self.py_rng = random.Random(cfg.seed)
        self.pipeline_mode = cfg.pipeline_mode
        if self.pipeline_mode not in {"distributed", "ground_compute"}:
            raise ValueError("pipeline_mode must be one of: distributed, ground_compute")
        tle_lines = _resolve_tle_lines(cfg.topology)
        topo_cfg = TopologyConfig(
            num_sats=cfg.num_sats,
            bandwidth_mbps_min=float(cfg.topology.get("bandwidth_mbps_min", 50.0)),
            bandwidth_mbps_max=float(cfg.topology.get("bandwidth_mbps_max", 300.0)),
            bandwidth_period=int(cfg.topology.get("bandwidth_period", 50)),
            bandwidth_noise=float(cfg.topology.get("bandwidth_noise", 5.0)),
            latency_ms=float(cfg.topology.get("latency_ms", 20.0)),
            seed=cfg.seed,
            start_time_utc=str(cfg.topology.get("start_time_utc", "2025-01-01T00:00:00Z")),
            tle_lines=tle_lines,
            earth_radius_km=float(cfg.topology.get("earth_radius_km", 6378.137)),
            min_elevation_deg=float(cfg.topology.get("min_elevation_deg", 0.0)),
            max_range_km=float(cfg.topology.get("max_range_km", 0.0)),
            bandwidth_distance_scale_km=float(cfg.topology.get("bandwidth_distance_scale_km", 0.0)),
            visibility_workers=int(cfg.topology.get("visibility_workers", 1)),
        )
        if not topo_cfg.tle_lines:
            raise ValueError("Skyfield topology requires non-empty topology.tle_lines or topology.tle_file")
        self.topology = TopologyModel(topo_cfg)
        self.satellites: Dict[int, Satellite] = {
            i: Satellite(
                sat_id=i,
                compute_rate=cfg.compute_rate,
                mem_capacity_gb=cfg.mem_capacity_gb,
                vram_capacity_gb=cfg.vram_capacity_gb,
            )
            for i in range(cfg.num_sats)
        }
        self.tasks: Dict[str, Task] = {}
        self.tiles: Dict[str, Tile] = {}
        self.transfers: List[Transfer] = []
        self.downlink_transfers: List[DownlinkTransfer] = []
        self.ground_tile_transfers: List[GroundTileTransfer] = []
        self.ground_tile_queues: Dict[str, List[str]] = {}
        self.ground_tile_running: Dict[str, str] = {}
        self.ground_task_transfers: List[GroundTaskTransfer] = []
        self.ground_task_transfering: set[str] = set()
        self.ground_task_queued: Dict[int, List[str]] = {}
        self.ground_task_running: Dict[str, GroundTaskCompute] = {}
        self.task_source_mem_gb: Dict[str, float] = {}
        self.ground_stations = _load_ground_stations(cfg.ground_stations)
        if not self.ground_stations:
            raise ValueError("At least one ground station is required in config: ground_stations")
        self.ground_station_objs = {
            gs.gs_id: build_ground_station(gs.lat_deg, gs.lon_deg, gs.alt_m) for gs in self.ground_stations.values()
        }
        self.ground_compute_queue: Dict[str, List[str]] = {
            gs_id: [] for gs_id in self.ground_stations.keys()
        }
        self.ground_tile_queues = {gs_id: [] for gs_id in self.ground_stations.keys()}
        self.metrics = Metrics()
        self.lifecycle_logger = TileLifecycleLogger(cfg.tile_lifecycle_log)
        self._links_cache_time: Optional[int] = None
        self._links_cache: Dict[str, object] = {}
        self._step_link_used_mb: Dict[str, float] = {}

    def reset(self) -> None:
        self.close()
        self.__init__(self.cfg)

    def close(self) -> None:
        self.lifecycle_logger.close()

    def step(self, actions: List[Action]) -> StepResult:
        t = self.time
        links = self._get_links(t)

        # 每步：任务到达 -> 调度决策/地算流程 -> 统计
        self._task_arrivals(t)
        self._step_link_used_mb = {}
        if self.pipeline_mode == "distributed":
            self._apply_actions(actions, links)
            self._advance_transfers(links)
            self._advance_ground_tile_transfers()
            self._advance_compute()
            self._advance_ground_tile_compute()
            self._start_downlinks()
            self._advance_downlinks()
        else:
            self._start_ground_task_uploads()
            self._advance_ground_task_uploads()
            self._advance_ground_task_compute()
        self._deadline_check()
        self._update_stats(links)

        self.time += 1
        return StepResult(time=t, metrics=self.metrics)

    def export_state(self) -> EnvState:
        links = self._get_links(self.time)
        neighbors: Dict[int, List[int]] = {i: [] for i in range(self.cfg.num_sats)}
        link_view: Dict[str, Dict] = {}
        for k, lk in links.items():
            if lk.up:
                neighbors[lk.i].append(lk.j)
                neighbors[lk.j].append(lk.i)
            link_view[k] = {
                "up": lk.up,
                "bandwidth_mbps": lk.bandwidth_mbps,
                "latency_ms": lk.latency_ms,
            }

        sat_view: Dict[int, Dict] = {}
        for sat_id, sat in self.satellites.items():
            sat_view[sat_id] = {
                "queue_len": len(sat.queue),
                "compute_rate": sat.compute_rate,
                "mem_remaining_gb": sat.mem_capacity_gb - sat.mem_used_gb,
                "vram_remaining_gb": sat.vram_capacity_gb - sat.vram_used_gb,
            }

        gs_view: Dict[str, Dict] = {}
        for gs_id, gs in self.ground_stations.items():
            gs_view[gs_id] = {
                "queue_len": len(self.ground_tile_queues.get(gs_id, [])),
                "running": 1 if gs_id in self.ground_tile_running else 0,
                "compute_rate": gs.compute_rate,
                "bandwidth_mbps": gs.bandwidth_mbps,
                "min_elevation_deg": gs.min_elevation_deg,
            }

        ground_options: Dict[int, List[Dict]] = {i: [] for i in range(self.cfg.num_sats)}
        for sat_id in range(self.cfg.num_sats):
            for gs_id, gs in self.ground_stations.items():
                elev_deg, _dist_km = sat_to_ground_geometry(
                    sat=self.topology.sat_recs[sat_id],
                    ground_station=self.ground_station_objs[gs_id],
                    t0=self.topology.t0,
                    t_seconds=self.time,
                )
                if elev_deg < gs.min_elevation_deg:
                    continue
                ground_options[sat_id].append(
                    {
                        "gs_id": gs_id,
                        "bandwidth_mbps": gs.bandwidth_mbps,
                    }
                )

        tile_view: Dict[str, Dict] = {}
        in_transfer = {tr.tile_id for tr in self.transfers}
        for tile_id, tile in self.tiles.items():
            tile_view[tile_id] = {
                "state": tile.state.value,
                "location": tile.location,
                "data_size_mb": tile.data_size_mb,
                "data_size_gb": tile.data_size_mb / 1024.0,
                "compute_cost": tile.compute_cost,
                "vram_req_gb": tile.vram_req_gb,
                "in_transfer": tile_id in in_transfer,
            }

        return EnvState(
            time=self.time,
            satellites=sat_view,
            ground_stations=gs_view,
            ground_options=ground_options,
            neighbors=neighbors,
            links=link_view,
            tiles=tile_view,
            config={
                "dt": self.cfg.dt,
            },
        )

    def _task_arrivals(self, t: int) -> None:
        lam = self.cfg.task_arrival_rate
        # 泊松到达
        num_tasks = int(self.rng.poisson(lam)) if lam > 0 else 0
        for _ in range(num_tasks):
            task_id = f"task_{len(self.tasks)}"
            src = self.py_rng.randrange(self.cfg.num_sats)
            deadline = t + self.cfg.deadline_steps if self.cfg.deadline_steps > 0 else None
            task = Task(
                task_id=task_id,
                source_sat_id=src,
                release_time=t,
                image_size_mb=self.cfg.image_size_mb,
                num_tiles=self.cfg.num_tiles,
                deadline=deadline,
            )
            self.tasks[task_id] = task
            self.metrics.total_tasks += 1
            tile_size = self.cfg.image_size_mb / self.cfg.num_tiles
            created_tiles: List[Tile] = []
            for k in range(self.cfg.num_tiles):
                tile_id = f"{task_id}_tile_{k}"
                # tile 大小加入轻微噪声，模拟成像差异
                size_mb = max(1.0, tile_size * (1.0 + self.rng.normal(0.0, 0.02)))
                vram_req = self.cfg.vram_base_gb + self.cfg.vram_alpha_per_mb * size_mb
                tile = Tile(
                    tile_id=tile_id,
                    parent_task_id=task_id,
                    data_size_mb=size_mb,
                    compute_cost=self.cfg.compute_cost_per_tile,
                    vram_req_gb=vram_req,
                    state=TileState.CREATED,
                    location=src,
                    timestamps=TileTimestamps(created=t),
                    deadline=deadline,
                )
                self.tiles[tile_id] = tile
                task.tile_ids.append(tile_id)
                self.metrics.total_tiles += 1
                self._log_tile_event(tile, "created", sat_from=None, sat_to=src)
                created_tiles.append(tile)

            if self.pipeline_mode == "distributed":
                for tile in created_tiles:
                    self._enqueue_tile(src, tile)
            else:
                self._enqueue_ground_task(task, created_tiles)

    def _enqueue_ground_task(self, task: Task, tiles: List[Tile]) -> None:
        src_sat = self.satellites[task.source_sat_id]
        required_gb = task.image_size_mb / 1024.0
        if src_sat.mem_used_gb + required_gb > src_sat.mem_capacity_gb:
            for tile in tiles:
                self._fail_tile(tile, FailureReason.MEM_FULL)
            return
        src_sat.mem_used_gb += required_gb
        self.task_source_mem_gb[task.task_id] = required_gb
        for tile in tiles:
            tile.state = TileState.QUEUED
            self._log_tile_event(tile, "queued", sat_from=None, sat_to=task.source_sat_id)
        self.ground_task_queued.setdefault(task.source_sat_id, []).append(task.task_id)

    def _enqueue_tile(self, sat_id: int, tile: Tile) -> None:
        sat = self.satellites[sat_id]
        needed_gb = tile.data_size_mb / 1024.0
        if sat.mem_used_gb + needed_gb > sat.mem_capacity_gb:
            self._fail_tile(tile, FailureReason.MEM_FULL)
            return
        sat.mem_used_gb += needed_gb
        tile.state = TileState.QUEUED
        sat.queue.append(tile.tile_id)
        self._log_tile_event(tile, "queued", sat_from=None, sat_to=sat_id)

    def _apply_actions(self, actions: List[Action], links: Dict[str, object]) -> None:
        action_map = {a.tile_id: a for a in actions}
        transfer_tile_ids = {tr.tile_id for tr in self.transfers}
        transfer_tile_ids.update(tr.tile_id for tr in self.ground_tile_transfers)
        for tile_id, tile in self.tiles.items():
            if tile.state not in (TileState.QUEUED, TileState.READY):
                continue
            if tile_id in transfer_tile_ids:
                continue
            action = action_map.get(tile_id)
            if not action:
                continue
            if action.action_type == ActionType.WAIT:
                continue
            if action.action_type == ActionType.LOCAL:
                # 本地计算，直接标记 READY
                tile.state = TileState.READY
                self._log_tile_event(tile, "local_ready", sat_from=tile.location, sat_to=tile.location)
                continue
            if action.action_type == ActionType.OFFLOAD:
                if action.target_gs_id is not None:
                    gs = self.ground_stations.get(action.target_gs_id)
                    if gs is None:
                        self._fail_tile(tile, FailureReason.NO_ROUTE)
                        continue
                    elevation_deg, _distance_km = sat_to_ground_geometry(
                        sat=self.topology.sat_recs[tile.location],
                        ground_station=self.ground_station_objs[gs.gs_id],
                        t0=self.topology.t0,
                        t_seconds=self.time,
                    )
                    if elevation_deg < gs.min_elevation_deg:
                        self._fail_tile(tile, FailureReason.LINK_DOWN)
                        continue
                    tr = GroundTileTransfer(
                        tile_id=tile.tile_id,
                        src_sat=tile.location,
                        gs_id=gs.gs_id,
                        remaining_mb=tile.data_size_mb,
                        start_time=self.time,
                    )
                    tile.state = TileState.TRANSFERRING
                    tile.timestamps.start_tx = self.time
                    src_sat = self.satellites[tile.location]
                    if tile.tile_id in src_sat.queue:
                        src_sat.queue.remove(tile.tile_id)
                    self.ground_tile_transfers.append(tr)
                    self._log_tile_event(
                        tile,
                        "tx_start_ground",
                        sat_from=tr.src_sat,
                        sat_to=tr.gs_id,
                        extra={"remaining_mb": tr.remaining_mb},
                    )
                    continue
                if action.target_sat_id is None:
                    continue
                if action.target_sat_id == tile.location:
                    tile.state = TileState.READY
                    continue
                lk = links.get(link_key(tile.location, action.target_sat_id))
                if not lk:
                    self._fail_tile(tile, FailureReason.NO_ROUTE)
                    continue
                if not lk.up:
                    self._fail_tile(tile, FailureReason.LINK_DOWN)
                    continue
                dst = self.satellites[action.target_sat_id]
                needed_gb = tile.data_size_mb / 1024.0
                if dst.mem_used_gb + needed_gb > dst.mem_capacity_gb:
                    self._fail_tile(tile, FailureReason.MEM_FULL)
                    continue
                # 预留目标存储，避免并发时超配
                dst.mem_used_gb += needed_gb
                transfer = Transfer(
                    tile_id=tile_id,
                    src=tile.location,
                    dst=action.target_sat_id,
                    remaining_mb=tile.data_size_mb,
                    start_time=self.time,
                    link_key=link_key(tile.location, action.target_sat_id),
                )
                tile.state = TileState.TRANSFERRING
                tile.timestamps.start_tx = self.time
                src_sat = self.satellites[tile.location]
                if tile.tile_id in src_sat.queue:
                    src_sat.queue.remove(tile.tile_id)
                self.transfers.append(transfer)
                self._log_tile_event(
                    tile,
                    "tx_start",
                    sat_from=transfer.src,
                    sat_to=transfer.dst,
                    extra={"remaining_mb": transfer.remaining_mb},
                )

    def _advance_transfers(self, links: Dict[str, object]) -> None:
        still_transfers: List[Transfer] = []
        for tr in self.transfers:
            lk = links.get(tr.link_key)
            if not lk or not lk.up:
                if self.cfg.transfer_fail_on_link_down:
                    tile = self.tiles[tr.tile_id]
                    dst_sat = self.satellites[tr.dst]
                    size_gb = tile.data_size_mb / 1024.0
                    dst_sat.mem_used_gb = max(0.0, dst_sat.mem_used_gb - size_gb)
                    self._fail_tile(tile, FailureReason.LINK_DOWN)
                else:
                    still_transfers.append(tr)
                continue
            rate_mb_s = lk.bandwidth_mbps / 8.0
            sent = rate_mb_s * self.cfg.dt
            actual_sent = min(max(0.0, tr.remaining_mb), sent)
            self._step_link_used_mb[tr.link_key] = self._step_link_used_mb.get(tr.link_key, 0.0) + actual_sent
            tr.remaining_mb -= sent
            if tr.remaining_mb <= 1e-6:
                tile = self.tiles[tr.tile_id]
                src_sat = self.satellites[tr.src]
                dst_sat = self.satellites[tr.dst]
                size_gb = tile.data_size_mb / 1024.0
                src_sat.mem_used_gb = max(0.0, src_sat.mem_used_gb - size_gb)
                tile.location = tr.dst
                tile.state = TileState.READY
                tile.timestamps.end_tx = self.time
                dst_sat.queue.append(tile.tile_id)
                self._log_tile_event(tile, "tx_end", sat_from=tr.src, sat_to=tr.dst)
                self._log_tile_event(tile, "queued", sat_from=tr.src, sat_to=tr.dst)
            else:
                still_transfers.append(tr)
        self.transfers = still_transfers

    def _advance_compute(self) -> None:
        for sat_id, sat in self.satellites.items():
            if sat.executing is None:
                # pick first ready tile
                next_tile_id = None
                for tid in list(sat.queue):
                    tile = self.tiles[tid]
                    if tile.state == TileState.READY and tile.location == sat_id:
                        next_tile_id = tid
                        break
                if next_tile_id is not None:
                    tile = self.tiles[next_tile_id]
                    if sat.vram_used_gb + tile.vram_req_gb > sat.vram_capacity_gb:
                        if self.cfg.vram_policy == "reject":
                            sat.queue.remove(next_tile_id)
                            self._fail_tile(tile, FailureReason.VRAM_OOM)
                        # vram_policy=wait 时保持在队列中等待
                        continue
                    sat.queue.remove(next_tile_id)
                    tile.state = TileState.RUNNING
                    tile.timestamps.start_compute = self.time
                    tile.remaining_compute = tile.compute_cost / sat.compute_rate
                    sat.executing = next_tile_id
                    sat.vram_used_gb += tile.vram_req_gb
                    # Capture peak immediately, otherwise same-step completion may hide usage.
                    self.metrics.update_vram_peak(sat_id, sat.vram_used_gb)
                    self._log_tile_event(tile, "compute_start", sat_from=sat_id, sat_to=sat_id)

            if sat.executing is not None:
                tile = self.tiles[sat.executing]
                tile.remaining_compute = max(0.0, float(tile.remaining_compute) - self.cfg.dt)
                if tile.remaining_compute <= 1e-6:
                    tile.state = TileState.COMPUTED
                    tile.timestamps.end_compute = self.time
                    sat.executing = None
                    sat.vram_used_gb = max(0.0, sat.vram_used_gb - tile.vram_req_gb)
                    size_gb = tile.data_size_mb / 1024.0
                    sat.mem_used_gb = max(0.0, sat.mem_used_gb - size_gb)
                    self._log_tile_event(tile, "computed", sat_from=sat_id, sat_to=sat_id)

    def _advance_ground_tile_transfers(self) -> None:
        still: List[GroundTileTransfer] = []
        for tr in self.ground_tile_transfers:
            tile = self.tiles[tr.tile_id]
            gs = self.ground_stations.get(tr.gs_id)
            if gs is None:
                still.append(tr)
                continue
            elevation_deg, _distance_km = sat_to_ground_geometry(
                sat=self.topology.sat_recs[tr.src_sat],
                ground_station=self.ground_station_objs[tr.gs_id],
                t0=self.topology.t0,
                t_seconds=self.time,
            )
            if elevation_deg < gs.min_elevation_deg:
                still.append(tr)
                continue
            sent = (gs.bandwidth_mbps / 8.0) * self.cfg.dt
            actual_sent = min(max(0.0, tr.remaining_mb), sent)
            self.metrics.update_link_usage(
                f"sat{tr.src_sat}->{tr.gs_id}",
                used_mb=actual_sent,
                avail_mb=sent,
            )
            tr.remaining_mb -= sent
            if tr.remaining_mb <= 1e-6:
                src_sat = self.satellites[tr.src_sat]
                size_gb = tile.data_size_mb / 1024.0
                src_sat.mem_used_gb = max(0.0, src_sat.mem_used_gb - size_gb)
                tile.state = TileState.GROUND_QUEUED
                tile.timestamps.end_tx = self.time
                self.ground_tile_queues[tr.gs_id].append(tile.tile_id)
                self._log_tile_event(tile, "tx_end_ground", sat_from=tr.src_sat, sat_to=tr.gs_id)
            else:
                still.append(tr)
        self.ground_tile_transfers = still

    def _advance_ground_tile_compute(self) -> None:
        for gs_id, gs in self.ground_stations.items():
            if gs_id not in self.ground_tile_running:
                queue = self.ground_tile_queues.get(gs_id, [])
                if queue:
                    tid = queue.pop(0)
                    tile = self.tiles[tid]
                    tile.state = TileState.GROUND_RUNNING
                    tile.remaining_compute = tile.compute_cost / max(1e-9, gs.compute_rate)
                    self.ground_tile_running[gs_id] = tid
                    self._log_tile_event(tile, "ground_compute_start", sat_from=tile.location, sat_to=gs_id)

            if gs_id in self.ground_tile_running:
                tid = self.ground_tile_running[gs_id]
                tile = self.tiles[tid]
                tile.remaining_compute = max(0.0, float(tile.remaining_compute) - self.cfg.dt)
                if tile.remaining_compute <= 1e-6:
                    tile.state = TileState.DONE
                    tile.timestamps.end_compute = self.time
                    tile.timestamps.end_downlink = self.time
                    self.metrics.record_tile_latency((tile.timestamps.end_downlink or self.time) - tile.timestamps.created)
                    self._log_tile_event(tile, "ground_compute_end", sat_from=tile.location, sat_to=gs_id)
                    self._log_tile_event(tile, "done", sat_from=tile.location, sat_to=gs_id)
                    self._check_task_done(tile.parent_task_id)
                    self.ground_tile_running.pop(gs_id, None)

    def _start_ground_task_uploads(self) -> None:
        for sat_id, task_ids in self.ground_task_queued.items():
            if not task_ids:
                continue
            gs = self._select_ground_station(sat_id)
            if gs is None:
                continue
            still_waiting: List[str] = []
            for task_id in task_ids:
                if not self._task_has_pending_tiles(task_id):
                    continue
                if task_id in self.ground_task_transfering:
                    still_waiting.append(task_id)
                    continue
                task = self.tasks[task_id]
                tr = GroundTaskTransfer(
                    task_id=task_id,
                    src_sat=sat_id,
                    gs_id=gs.gs_id,
                    remaining_mb=task.image_size_mb,
                    start_time=self.time,
                )
                self.ground_task_transfers.append(tr)
                self.ground_task_transfering.add(task_id)
                self._log_task_event(task_id, "ground_upload_start", sat_from=sat_id, sat_to=gs.gs_id)
            self.ground_task_queued[sat_id] = still_waiting

    def _advance_ground_task_uploads(self) -> None:
        still: List[GroundTaskTransfer] = []
        for tr in self.ground_task_transfers:
            gs = self.ground_stations.get(tr.gs_id)
            if gs is None:
                still.append(tr)
                continue
            elevation_deg, _distance_km = sat_to_ground_geometry(
                sat=self.topology.sat_recs[tr.src_sat],
                ground_station=self.ground_station_objs[tr.gs_id],
                t0=self.topology.t0,
                t_seconds=self.time,
            )
            if elevation_deg < gs.min_elevation_deg:
                still.append(tr)
                continue
            sent = (gs.bandwidth_mbps / 8.0) * self.cfg.dt
            tr.remaining_mb -= sent
            if tr.remaining_mb <= 1e-6:
                self.ground_task_transfering.discard(tr.task_id)
                src_mem = self.task_source_mem_gb.pop(tr.task_id, 0.0)
                src_sat = self.satellites[tr.src_sat]
                src_sat.mem_used_gb = max(0.0, src_sat.mem_used_gb - src_mem)
                self.ground_task_queued.setdefault(tr.src_sat, [])
                self.ground_task_queued[tr.src_sat] = [
                    tid for tid in self.ground_task_queued[tr.src_sat] if tid != tr.task_id
                ]
                self._enqueue_ground_task_compute(tr.task_id, tr.gs_id)
                self._log_task_event(tr.task_id, "ground_upload_end", sat_from=tr.src_sat, sat_to=tr.gs_id)
            else:
                still.append(tr)
        self.ground_task_transfers = still

    def _enqueue_ground_task_compute(self, task_id: str, gs_id: str) -> None:
        if not self._task_has_pending_tiles(task_id):
            return
        self.ground_compute_queue.setdefault(gs_id, []).append(task_id)

    def _advance_ground_task_compute(self) -> None:
        # Dispatch idle ground stations.
        for gs_id, gs in self.ground_stations.items():
            if gs_id in self.ground_task_running:
                continue
            queue = self.ground_compute_queue.setdefault(gs_id, [])
            if not queue:
                continue
            picked_task = queue.pop(0)
            if not self._task_has_pending_tiles(picked_task):
                continue
            task = self.tasks[picked_task]
            total_compute = self.cfg.compute_cost_per_tile * task.num_tiles
            remaining = total_compute / max(1e-9, gs.compute_rate)
            self.ground_task_running[gs_id] = GroundTaskCompute(
                task_id=picked_task,
                gs_id=gs_id,
                remaining_compute=remaining,
                start_time=self.time,
            )
            self._log_task_event(picked_task, "ground_compute_start", sat_from=gs_id, sat_to=gs_id)

        # Advance running computations.
        done_gs: List[str] = []
        for gs_id, comp in self.ground_task_running.items():
            comp.remaining_compute = max(0.0, comp.remaining_compute - self.cfg.dt)
            if comp.remaining_compute > 1e-6:
                continue
            task = self.tasks[comp.task_id]
            for tid in task.tile_ids:
                tile = self.tiles[tid]
                tile.state = TileState.DONE
                tile.timestamps.end_compute = self.time
                self.metrics.record_tile_latency((tile.timestamps.end_compute or self.time) - tile.timestamps.created)
                self._log_tile_event(tile, "done", sat_from=task.source_sat_id, sat_to=comp.gs_id)
            self._check_task_done(comp.task_id)
            self._log_task_event(comp.task_id, "ground_compute_end", sat_from=comp.gs_id, sat_to=comp.gs_id)
            done_gs.append(gs_id)
        for gs_id in done_gs:
            self.ground_task_running.pop(gs_id, None)

    def _task_has_pending_tiles(self, task_id: str) -> bool:
        task = self.tasks[task_id]
        for tid in task.tile_ids:
            if self.tiles[tid].state not in (TileState.DONE, TileState.FAILED):
                return True
        return False

    def _start_downlinks(self) -> None:
        downlinking = {tr.tile_id for tr in self.downlink_transfers}
        for tile in self.tiles.values():
            if tile.state != TileState.COMPUTED:
                continue
            if tile.tile_id in downlinking:
                continue
            gs = self._select_ground_station(tile.location)
            if gs is None:
                continue
            tr = DownlinkTransfer(
                tile_id=tile.tile_id,
                src_sat=tile.location,
                gs_id=gs.gs_id,
                remaining_mb=self.cfg.result_size_mb,
                start_time=self.time,
            )
            tile.state = TileState.DOWNLINKING
            tile.timestamps.start_downlink = self.time
            self.downlink_transfers.append(tr)
            self._log_tile_event(
                tile,
                "downlink_start",
                sat_from=tile.location,
                sat_to=gs.gs_id,
                extra={"remaining_mb": tr.remaining_mb},
            )

    def _advance_downlinks(self) -> None:
        still: List[DownlinkTransfer] = []
        for tr in self.downlink_transfers:
            tile = self.tiles[tr.tile_id]
            gs = self.ground_stations.get(tr.gs_id)
            if gs is None:
                still.append(tr)
                continue
            elevation_deg, _distance_km = sat_to_ground_geometry(
                sat=self.topology.sat_recs[tr.src_sat],
                ground_station=self.ground_station_objs[tr.gs_id],
                t0=self.topology.t0,
                t_seconds=self.time,
            )
            if elevation_deg < gs.min_elevation_deg:
                still.append(tr)
                continue
            sent = (gs.bandwidth_mbps / 8.0) * self.cfg.dt
            tr.remaining_mb -= sent
            if tr.remaining_mb <= 1e-6:
                tile.state = TileState.DONE
                tile.timestamps.end_downlink = self.time
                self.metrics.record_tile_latency(tile.timestamps.end_downlink - tile.timestamps.created)
                self._log_tile_event(tile, "downlink_end", sat_from=tr.src_sat, sat_to=tr.gs_id)
                self._log_tile_event(tile, "done", sat_from=tr.src_sat, sat_to=tr.gs_id)
                self._check_task_done(tile.parent_task_id)
            else:
                still.append(tr)
        self.downlink_transfers = still

    def _check_task_done(self, task_id: str) -> None:
        task = self.tasks[task_id]
        for tid in task.tile_ids:
            if self.tiles[tid].state != TileState.DONE:
                return
        if self.pipeline_mode == "distributed":
            end_time = max(self.tiles[tid].timestamps.end_downlink or 0 for tid in task.tile_ids)
        else:
            end_time = max(self.tiles[tid].timestamps.end_compute or 0 for tid in task.tile_ids)
        self.metrics.record_task_latency(end_time - task.release_time)

    def _update_stats(self, links: Dict[str, object]) -> None:
        for sat_id, sat in self.satellites.items():
            self.metrics.update_queue_stats(sat_id, len(sat.queue))
            if sat.executing is not None:
                self.metrics.update_compute_busy(sat_id, self.cfg.dt)
            self.metrics.update_mem_peak(sat_id, sat.mem_used_gb)
            self.metrics.update_vram_peak(sat_id, sat.vram_used_gb)
        # link usage
        for k, lk in links.items():
            if not lk.up:
                continue
            avail = (lk.bandwidth_mbps / 8.0) * self.cfg.dt
            used = self._step_link_used_mb.get(k, 0.0)
            self.metrics.update_link_usage(k, used, avail)
        self.metrics.finalize_step()

    def _fail_tile(self, tile: Tile, reason: FailureReason) -> None:
        sat_before = tile.location
        self.downlink_transfers = [tr for tr in self.downlink_transfers if tr.tile_id != tile.tile_id]
        self.ground_tile_transfers = [tr for tr in self.ground_tile_transfers if tr.tile_id != tile.tile_id]
        for gs_id, tid in list(self.ground_tile_running.items()):
            if tid == tile.tile_id:
                self.ground_tile_running.pop(gs_id, None)
        for gs_id, q in self.ground_tile_queues.items():
            self.ground_tile_queues[gs_id] = [tid for tid in q if tid != tile.tile_id]
        # If in transfer, release destination reservation and remove transfer entry
        for tr in list(self.transfers):
            if tr.tile_id == tile.tile_id:
                dst_sat = self.satellites.get(tr.dst)
                if dst_sat is not None:
                    size_gb = tile.data_size_mb / 1024.0
                    dst_sat.mem_used_gb = max(0.0, dst_sat.mem_used_gb - size_gb)
                self.transfers.remove(tr)
                break
        sat = self.satellites.get(tile.location)
        if sat is not None:
            size_gb = tile.data_size_mb / 1024.0
            sat.mem_used_gb = max(0.0, sat.mem_used_gb - size_gb)
            if tile.tile_id in sat.queue:
                sat.queue.remove(tile.tile_id)
            if sat.executing == tile.tile_id:
                sat.executing = None
                sat.vram_used_gb = max(0.0, sat.vram_used_gb - tile.vram_req_gb)
        tile.state = TileState.FAILED
        tile.failure_reason = reason
        self.metrics.record_failure(reason.value)
        self._log_tile_event(
            tile,
            "failed",
            sat_from=sat_before,
            sat_to=tile.location,
            extra={"reason": reason.value},
        )

    def _deadline_check(self) -> None:
        if self.cfg.deadline_steps <= 0:
            return
        for tile in self.tiles.values():
            if tile.state in (TileState.DONE, TileState.FAILED):
                continue
            if tile.deadline is not None and self.time > tile.deadline:
                self._fail_tile(tile, FailureReason.DEADLINE_MISS)

    def _get_links(self, t: int) -> Dict[str, object]:
        if self._links_cache_time != t:
            self._links_cache = self.topology.snapshot(t)
            self._links_cache_time = t
        return self._links_cache

    def _log_tile_event(
        self,
        tile: Tile,
        event: str,
        sat_from: Optional[object],
        sat_to: Optional[object],
        extra: Optional[Dict[str, object]] = None,
    ) -> None:
        if not self.lifecycle_logger.enabled:
            return
        record: Dict[str, object] = {
            "time": self.time,
            "event": event,
            "task_id": tile.parent_task_id,
            "tile_id": tile.tile_id,
            "state": tile.state.value,
            "sat_from": sat_from,
            "sat_to": sat_to,
            "location": tile.location,
        }
        if extra:
            record.update(extra)
        self.lifecycle_logger.log(record)

    def _log_task_event(
        self,
        task_id: str,
        event: str,
        sat_from: Optional[object],
        sat_to: Optional[object],
        extra: Optional[Dict[str, object]] = None,
    ) -> None:
        if not self.lifecycle_logger.enabled:
            return
        record: Dict[str, object] = {
            "time": self.time,
            "event": event,
            "task_id": task_id,
            "tile_id": None,
            "sat_from": sat_from,
            "sat_to": sat_to,
        }
        if extra:
            record.update(extra)
        self.lifecycle_logger.log(record)

    def _select_ground_station(self, sat_id: int) -> Optional[GroundStation]:
        best: Optional[GroundStation] = None
        best_bw = -1.0
        for gs in self.ground_stations.values():
            elevation_deg, _distance_km = sat_to_ground_geometry(
                sat=self.topology.sat_recs[sat_id],
                ground_station=self.ground_station_objs[gs.gs_id],
                t0=self.topology.t0,
                t_seconds=self.time,
            )
            if elevation_deg < gs.min_elevation_deg:
                continue
            if gs.bandwidth_mbps > best_bw:
                best = gs
                best_bw = gs.bandwidth_mbps
        return best


def _resolve_tle_lines(topology_cfg: Dict[str, object]) -> List[tuple[str, str]]:
    raw_lines = topology_cfg.get("tle_lines", [])
    if raw_lines:
        return [tuple(x) for x in raw_lines]
    tle_file = topology_cfg.get("tle_file")
    if not tle_file:
        return []
    path = Path(str(tle_file)).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"TLE file not found: {path}")
    text_lines = [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    pairs: List[tuple[str, str]] = []
    idx = 0
    while idx < len(text_lines):
        line = text_lines[idx]
        if line.startswith("1 ") and idx + 1 < len(text_lines) and text_lines[idx + 1].startswith("2 "):
            pairs.append((line, text_lines[idx + 1]))
            idx += 2
            continue
        if idx + 2 < len(text_lines) and text_lines[idx + 1].startswith("1 ") and text_lines[idx + 2].startswith("2 "):
            pairs.append((text_lines[idx + 1], text_lines[idx + 2]))
            idx += 3
            continue
        idx += 1
    if not pairs:
        raise ValueError(f"No valid TLE entries parsed from file: {path}")
    return pairs


def _load_ground_stations(raw_ground_stations: List[Dict[str, object]]) -> Dict[str, GroundStation]:
    stations: Dict[str, GroundStation] = {}
    for idx, item in enumerate(raw_ground_stations):
        gs_id = str(item.get("id", f"gs_{idx}"))
        stations[gs_id] = GroundStation(
            gs_id=gs_id,
            lat_deg=float(item.get("lat_deg", 0.0)),
            lon_deg=float(item.get("lon_deg", 0.0)),
            alt_m=float(item.get("alt_m", 0.0)),
            min_elevation_deg=float(item.get("min_elevation_deg", 5.0)),
            bandwidth_mbps=float(item.get("bandwidth_mbps", 200.0)),
            latency_ms=float(item.get("latency_ms", 20.0)),
            compute_rate=float(item.get("compute_rate", 8.0)),
        )
    return stations
