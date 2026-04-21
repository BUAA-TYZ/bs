"""仿真环境主入口。

SimulationEnv 负责：
  - 状态管理（卫星、任务、tile、传输、地面站）
  - 步进控制（step / reset / close）
  - 状态导出（export_state）
  - 任务到达与 tile 入队
  - 失败处理与截止检查
  - 日志与统计

具体 pipeline 逻辑已委托给：
  sim.pipeline.distributed.DistributedPipeline
  sim.pipeline.ground.GroundPipeline

可见性与过境窗口缓存委托给：
  sim.cache.visibility.VisibilityCache
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import random

import numpy as np

from sim.cache.visibility import VisibilityCache
from sim.config import SimConfig
from sim.entities import (
    Action,
    DownlinkTransfer,
    EnvState,
    FailureReason,
    GroundStation,
    GroundTaskCompute,
    GroundTaskTransfer,
    GroundTileTransfer,
    Satellite,
    Task,
    Tile,
    TileState,
    TileTimestamps,
    Transfer,
)
from sim.lifecycle import TileLifecycleLogger
from sim.marl.reward import RewardConfig, StepEvents, compute_reward, diff_events
from sim.metrics import Metrics
from sim.orbit import build_ground_station
from sim.pipeline.distributed import DistributedPipeline
from sim.pipeline.ground import GroundPipeline
from sim.topology import TopologyConfig, TopologyModel


@dataclass
class StepResult:
    time: int
    metrics: Metrics
    reward: float = 0.0  # MARL reward（合作式，所有 Agent 共享）


class SimulationEnv:
    def __init__(self, cfg: SimConfig) -> None:
        self.cfg = cfg
        self.time = 0
        self.rng = np.random.default_rng(cfg.seed)
        self.py_rng = random.Random(cfg.seed)
        self.pipeline_mode = cfg.pipeline_mode
        self.topology_update_steps = max(1, cfg.topology_update_steps)
        self.ground_visibility_update_steps = max(1, cfg.ground_visibility_update_steps)

        if self.pipeline_mode not in {"distributed", "ground_compute"}:
            raise ValueError(
                "pipeline_mode must be one of: distributed, ground_compute"
            )

        # ── 拓扑 ──────────────────────────────────────────────────────────
        tle_lines = _resolve_tle_lines(cfg.topology)
        topo_cfg = TopologyConfig(
            num_sats=cfg.num_sats,
            bandwidth_mbps_min=float(cfg.topology.get("bandwidth_mbps_min", 50.0)),
            bandwidth_mbps_max=float(cfg.topology.get("bandwidth_mbps_max", 300.0)),
            bandwidth_period=int(cfg.topology.get("bandwidth_period", 50)),
            bandwidth_noise=float(cfg.topology.get("bandwidth_noise", 5.0)),
            latency_ms=float(cfg.topology.get("latency_ms", 20.0)),
            seed=cfg.seed,
            start_time_utc=str(
                cfg.topology.get("start_time_utc", "2025-01-01T00:00:00Z")
            ),
            tle_lines=tle_lines,
            earth_radius_km=float(cfg.topology.get("earth_radius_km", 6378.137)),
            min_elevation_deg=float(cfg.topology.get("min_elevation_deg", 0.0)),
            max_range_km=float(cfg.topology.get("max_range_km", 0.0)),
            bandwidth_distance_scale_km=float(
                cfg.topology.get("bandwidth_distance_scale_km", 0.0)
            ),
            visibility_workers=int(cfg.topology.get("visibility_workers", 1)),
        )
        if not topo_cfg.tle_lines:
            raise ValueError(
                "Skyfield topology requires non-empty topology.tle_lines or topology.tle_file"
            )
        self.topology = TopologyModel(topo_cfg)

        # ── 实体状态 ──────────────────────────────────────────────────────
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

        # ── 地面站 ────────────────────────────────────────────────────────
        self.ground_stations = _load_ground_stations(cfg.ground_stations)
        if not self.ground_stations:
            raise ValueError(
                "At least one ground station is required in config: ground_stations"
            )
        self.ground_station_objs = {
            gs.gs_id: build_ground_station(gs.lat_deg, gs.lon_deg, gs.alt_m)
            for gs in self.ground_stations.values()
        }
        self.ground_compute_queue: Dict[str, List[str]] = {
            gs_id: [] for gs_id in self.ground_stations.keys()
        }
        self.ground_tile_queues = {gs_id: [] for gs_id in self.ground_stations.keys()}

        # ── 统计 / 日志 ───────────────────────────────────────────────────
        self.metrics = Metrics()
        self.lifecycle_logger = TileLifecycleLogger(cfg.tile_lifecycle_log)

        # ── 可见性 & 窗口缓存 ─────────────────────────────────────────────
        gs_window_lookahead_s = float(cfg.topology.get("window_lookahead_s", 1800.0))
        self.vis_cache = VisibilityCache(
            num_sats=cfg.num_sats,
            ground_visibility_update_steps=cfg.ground_visibility_update_steps,
            gs_window_lookahead_s=gs_window_lookahead_s,
        )

        # ── 链路缓存 ──────────────────────────────────────────────────────
        self._links_cache_time: Optional[int] = None
        self._links_cache: Dict[str, object] = {}

        # ── 内部辅助状态 ──────────────────────────────────────────────────
        self._computed_waiting_downlink: set[str] = set()
        self._reward_cfg = RewardConfig()
        # 任务级反向索引：task_id → tile_id 列表
        self._task_tile_index: Dict[str, List[str]] = {}

        # ── Pipeline 委托对象 ─────────────────────────────────────────────
        self._dist_pipeline = DistributedPipeline(self)
        self._ground_pipeline = GroundPipeline(self)

    # ------------------------------------------------------------------ #
    # 生命周期                                                              #
    # ------------------------------------------------------------------ #

    def reset(self) -> None:
        self.close()
        self.__init__(self.cfg)

    def close(self) -> None:
        self.lifecycle_logger.close()

    # ------------------------------------------------------------------ #
    # 主步进                                                               #
    # ------------------------------------------------------------------ #

    def step(self, actions: List[Action]) -> StepResult:
        t = self.time
        links = self._get_links(t)
        self.vis_cache.refresh(
            t,
            self.topology.sat_recs,
            self.topology.t0,
            self.ground_station_objs,
            self.ground_stations,
        )

        # reward 快照
        tiles_done_before = self.metrics.completed_tiles
        tasks_done_before = self.metrics.completed_tasks
        failures_before = dict(self.metrics.failure_reasons)

        self._task_arrivals(t)

        if self.pipeline_mode == "distributed":
            self._dist_pipeline.run(actions, links)
        else:
            self._ground_pipeline.run()

        self._deadline_check()
        self._update_stats(links)

        # reward 计算
        num_waiting = sum(
            1 for tile in self.tiles.values() if tile.state.value in ("QUEUED", "READY")
        )
        events = diff_events(
            tiles_done_before,
            tasks_done_before,
            failures_before,
            self.metrics.completed_tiles,
            self.metrics.completed_tasks,
            dict(self.metrics.failure_reasons),
            num_waiting,
        )
        reward = compute_reward(events, self._reward_cfg)

        self.time += 1
        return StepResult(time=t, metrics=self.metrics, reward=reward)

    # ------------------------------------------------------------------ #
    # 状态导出                                                              #
    # ------------------------------------------------------------------ #

    def export_state(self) -> EnvState:
        links = self._get_links(self.time)
        self.vis_cache.refresh(
            self.time,
            self.topology.sat_recs,
            self.topology.t0,
            self.ground_station_objs,
            self.ground_stations,
        )

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
            executing_remaining = 0.0
            if sat.executing is not None:
                exec_tile = self.tiles.get(sat.executing)
                if exec_tile is not None and exec_tile.compute_cost > 0:
                    executing_remaining = (
                        exec_tile.remaining_compute or 0.0
                    ) / exec_tile.compute_cost
            sat_view[sat_id] = {
                "queue_len": len(sat.queue),
                "compute_rate": sat.compute_rate,
                "mem_remaining_gb": sat.mem_capacity_gb - sat.mem_used_gb,
                "vram_remaining_gb": sat.vram_capacity_gb - sat.vram_used_gb,
                "executing_remaining": executing_remaining,
                "next_gs_windows": self.vis_cache.window_cache.get(sat_id, {}),
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

        ground_options: Dict[int, List[Dict]] = {
            i: [] for i in range(self.cfg.num_sats)
        }
        for sat_id in range(self.cfg.num_sats):
            for gs_id in self.vis_cache.visible_gs_by_sat.get(sat_id, []):
                gs = self.ground_stations[gs_id]
                ground_options[sat_id].append(
                    {"gs_id": gs_id, "bandwidth_mbps": gs.bandwidth_mbps}
                )

        tile_view: Dict[str, Dict] = {}
        in_transfer = {tr.tile_id for tr in self.transfers}
        for tile_id, tile in self.tiles.items():
            if tile.state not in (TileState.QUEUED, TileState.READY):
                continue
            sibling_locations: Dict[int, int] = {}
            for sibling_id in self._task_tile_index.get(tile.parent_task_id, []):
                if sibling_id == tile_id:
                    continue
                sibling = self.tiles.get(sibling_id)
                if sibling is not None and sibling.state not in (
                    TileState.DONE,
                    TileState.FAILED,
                ):
                    loc = sibling.location
                    sibling_locations[loc] = sibling_locations.get(loc, 0) + 1
            tile_view[tile_id] = {
                "state": tile.state.value,
                "location": tile.location,
                "data_size_mb": tile.data_size_mb,
                "data_size_gb": tile.data_size_mb / 1024.0,
                "compute_cost": tile.compute_cost,
                "vram_req_gb": tile.vram_req_gb,
                "in_transfer": tile_id in in_transfer,
                "parent_task_id": tile.parent_task_id,
                "waiting_time": self.time - tile.timestamps.created,
                "result_size_mb": self.cfg.result_size_mb,
                "sibling_locations": sibling_locations,
                "task_pending_count": len(sibling_locations) + 1,
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
                "num_sats": self.cfg.num_sats,
                "num_tiles": self.cfg.num_tiles,
                "result_size_mb": self.cfg.result_size_mb,
                "mem_capacity_gb": self.cfg.mem_capacity_gb,
                "vram_capacity_gb": self.cfg.vram_capacity_gb,
                "gs_window_lookahead_s": self.vis_cache.lookahead_s,
            },
        )

    # ------------------------------------------------------------------ #
    # 任务到达 & tile 入队                                                  #
    # ------------------------------------------------------------------ #

    def _task_arrivals(self, t: int) -> None:
        lam = self.cfg.task_arrival_rate
        num_tasks = int(self.rng.poisson(lam)) if lam > 0 else 0
        for _ in range(num_tasks):
            task_id = f"task_{len(self.tasks)}"
            src = self.py_rng.randrange(self.cfg.num_sats)
            deadline = (
                t + self.cfg.deadline_steps if self.cfg.deadline_steps > 0 else None
            )
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
                self._task_tile_index.setdefault(task.task_id, []).append(tile_id)
                self.metrics.total_tiles += 1
                self._log_tile_event(tile, "created", sat_from=None, sat_to=src)
                created_tiles.append(tile)

            if self.pipeline_mode == "distributed":
                for tile in created_tiles:
                    self._enqueue_tile(src, tile)
            else:
                self._ground_pipeline.enqueue_ground_task(task, created_tiles)

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

    # ------------------------------------------------------------------ #
    # 失败处理                                                              #
    # ------------------------------------------------------------------ #

    def _fail_tile(self, tile: Tile, reason: FailureReason) -> None:
        sat_before = tile.location
        self._computed_waiting_downlink.discard(tile.tile_id)
        self.downlink_transfers = [
            tr for tr in self.downlink_transfers if tr.tile_id != tile.tile_id
        ]
        self.ground_tile_transfers = [
            tr for tr in self.ground_tile_transfers if tr.tile_id != tile.tile_id
        ]
        for gs_id, tid in list(self.ground_tile_running.items()):
            if tid == tile.tile_id:
                self.ground_tile_running.pop(gs_id, None)
        for gs_id, q in self.ground_tile_queues.items():
            self.ground_tile_queues[gs_id] = [tid for tid in q if tid != tile.tile_id]
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

    # ------------------------------------------------------------------ #
    # 截止检查 & 统计                                                        #
    # ------------------------------------------------------------------ #

    def _deadline_check(self) -> None:
        if self.cfg.deadline_steps <= 0:
            return
        for tile in self.tiles.values():
            if tile.state in (TileState.DONE, TileState.FAILED):
                continue
            if tile.deadline is not None and self.time > tile.deadline:
                self._fail_tile(tile, FailureReason.DEADLINE_MISS)

    def _update_stats(self, links: Dict[str, object]) -> None:
        for sat_id, sat in self.satellites.items():
            self.metrics.update_queue_stats(sat_id, len(sat.queue))
            if sat.executing is not None:
                self.metrics.update_compute_busy(sat_id, self.cfg.dt)
            self.metrics.update_mem_peak(sat_id, sat.mem_used_gb)
            self.metrics.update_vram_peak(sat_id, sat.vram_used_gb)
        self.metrics.finalize_step()

    def _check_task_done(self, task_id: str) -> None:
        task = self.tasks[task_id]
        for tid in task.tile_ids:
            if self.tiles[tid].state != TileState.DONE:
                return
        if self.pipeline_mode == "distributed":
            end_time = max(
                self.tiles[tid].timestamps.end_downlink or 0 for tid in task.tile_ids
            )
        else:
            end_time = max(
                self.tiles[tid].timestamps.end_compute or 0 for tid in task.tile_ids
            )
        self.metrics.record_task_latency(end_time - task.release_time)

    # ------------------------------------------------------------------ #
    # 辅助                                                                  #
    # ------------------------------------------------------------------ #

    def _get_links(self, t: int) -> Dict[str, object]:
        sampled_t = (t // self.topology_update_steps) * self.topology_update_steps
        if self._links_cache_time != sampled_t:
            self._links_cache = self.topology.snapshot(sampled_t)
            self._links_cache_time = sampled_t
        return self._links_cache

    def _select_ground_station(self, sat_id: int) -> Optional[GroundStation]:
        best_gs_id = self.vis_cache.best_gs_by_sat.get(sat_id)
        if best_gs_id is None:
            return None
        return self.ground_stations.get(best_gs_id)

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


# ────────────────────────────────────────────────────────────────────── #
# 模块级辅助函数                                                           #
# ────────────────────────────────────────────────────────────────────── #


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
    text_lines = [
        ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    pairs: List[tuple[str, str]] = []
    idx = 0
    while idx < len(text_lines):
        line = text_lines[idx]
        if (
            line.startswith("1 ")
            and idx + 1 < len(text_lines)
            and text_lines[idx + 1].startswith("2 ")
        ):
            pairs.append((line, text_lines[idx + 1]))
            idx += 2
            continue
        if (
            idx + 2 < len(text_lines)
            and text_lines[idx + 1].startswith("1 ")
            and text_lines[idx + 2].startswith("2 ")
        ):
            pairs.append((text_lines[idx + 1], text_lines[idx + 2]))
            idx += 3
            continue
        idx += 1
    if not pairs:
        raise ValueError(f"No valid TLE entries parsed from file: {path}")
    return pairs


def _load_ground_stations(
    raw_ground_stations: List[Dict[str, object]],
) -> Dict[str, GroundStation]:
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
