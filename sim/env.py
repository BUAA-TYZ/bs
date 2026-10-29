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

from collections import Counter
from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
from typing import Dict, List, Optional
import random

import numpy as np

from sim.cache.visibility import VisibilityCache
from sim.cache.contacts import ContactCache, reconcile_windows
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
from sim.reproducibility import runtime_metadata
from sim.orbit import build_ground_station
from sim.pipeline.distributed import DistributedPipeline
from sim.pipeline.ground import GroundPipeline
from sim.topology import TopologyConfig, TopologyModel
from sim.workload import ImageTiling


@dataclass
class StepResult:
    time: int
    metrics: Metrics
    reward: float = 0.0  # MARL reward（合作式，所有 Agent 共享）


class SimulationEnv:
    def __init__(self, cfg: SimConfig) -> None:
        self.cfg = cfg
        self.image_workload = ImageTiling.from_dict(cfg.image_tiling) if cfg.image_tiling is not None else None
        self.image_tiles = self.image_workload.tiles() if self.image_workload else ()
        self._task_workloads = {}
        self._task_unique_blocks = {}
        self._aggregation_busy_until = 0
        self.time = 0
        self.rng = np.random.default_rng(cfg.seed)
        self.py_rng = random.Random(cfg.seed)
        self._scheduled_task_arrivals = {}
        for event in cfg.scheduled_task_arrivals or []:
            self._scheduled_task_arrivals.setdefault(event["release_step"], []).append(event)
        self.pipeline_mode = cfg.pipeline_mode
        self.topology_update_steps = max(1, cfg.topology_update_steps)
        self.ground_visibility_update_steps = max(1, cfg.ground_visibility_update_steps)

        if self.pipeline_mode not in {"distributed", "ground_compute", "ground_relay", "ground_tiles",
                                      "ground_unique_relay"}:
            raise ValueError(
                "pipeline_mode must be one of: distributed, ground_compute, ground_relay, ground_tiles, "
                "ground_unique_relay"
            )
        # 唯一像素中继按不重叠块传输原图，块数与窗口分片数一一对应。
        self.unique_transport_blocks = ()
        if self.pipeline_mode == "ground_unique_relay":
            if self.image_workload is None:
                raise ValueError("ground_unique_relay requires image_tiling")
            self.unique_transport_blocks = self.image_workload.unique_transport_blocks(len(self.image_tiles))

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
            max_neighbors=int(cfg.topology.get("max_neighbors", 0)),
            source_sat_ids=tuple(cfg.task_source_sat_ids or ()) if cfg.sar_source_isolation else (),
            neighbor_limits=tuple(cfg.topology.get("neighbor_limits", ())),
            preferred_edges=tuple(tuple(edge) for edge in cfg.topology.get("preferred_edges", ())),
            acquisition_delay_s=float(cfg.topology.get("acquisition_delay_s", 0)),
        )
        if not topo_cfg.tle_lines:
            raise ValueError(
                "Skyfield topology requires non-empty topology.tle_lines or topology.tle_file"
            )
        self.topology = TopologyModel(topo_cfg)
        self.contact_cache = ContactCache(
            self.topology, float(cfg.topology.get("window_lookahead_s", 1800)),
            float(cfg.topology.get("contact_scan_s", 30)),
            float(cfg.topology.get("contact_update_s", 60)),
        ) if cfg.topology.get("predict_contacts", False) else None
        if len(self.topology.sat_recs) < cfg.num_sats:
            self.topology.close()
            raise ValueError("num_sats exceeds the number of TLE records")

        # ── 实体状态 ──────────────────────────────────────────────────────
        self.satellites: Dict[int, Satellite] = {
            i: Satellite(
                sat_id=i,
                compute_rate=(cfg.satellite_compute_rates[i] if cfg.satellite_compute_rates is not None
                              else cfg.compute_rate),
                mem_capacity_gb=cfg.mem_capacity_gb,
                vram_capacity_gb=cfg.vram_capacity_gb,
            )
            for i in range(cfg.num_sats)
        }
        self.tasks: Dict[str, Task] = {}
        self.task_sources = cfg.task_source_sat_ids if cfg.task_source_sat_ids is not None else list(range(cfg.num_sats))
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
        self.ground_compute_busy_time: Dict[str, float] = {}
        self.compute_cohort_records: Dict[str, dict] = {}
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
        self.metrics = Metrics(step_seconds=cfg.dt)
        self.lifecycle_logger = TileLifecycleLogger(cfg.tile_lifecycle_log)

        # ── 可见性 & 窗口缓存 ─────────────────────────────────────────────
        gs_window_lookahead_s = float(cfg.topology.get("window_lookahead_s", 1800.0))
        self.vis_cache = VisibilityCache(
            num_sats=cfg.num_sats,
            ground_visibility_update_steps=cfg.ground_visibility_update_steps,
            gs_window_lookahead_s=gs_window_lookahead_s,
            dt=cfg.dt,
        )
        if cfg.topology.get("precompute_visibility", False):
            self.vis_cache.precompute(self.topology.sat_recs, self.topology.t0,
                                      self.ground_station_objs, cfg.sim_steps)

        # ── 链路缓存 ──────────────────────────────────────────────────────
        self._links_cache_time: Optional[int] = None
        self._links_cache: Dict[str, object] = {}
        # 真实 TopologyModel 每次 snapshot 返回新字典与新 Link；替身拓扑可能原地改写，禁用派生缓存。
        # 测试会 patch 本模块的 TopologyModel 名称，故按真实类判断而不是按当前名称。
        import sim.topology as _topology_module
        self._links_mutable = type(self.topology) is not _topology_module.TopologyModel

        # ── 内部辅助状态 ──────────────────────────────────────────────────
        self._computed_waiting_downlink: set[str] = set()
        self._reward_cfg = RewardConfig()
        # 任务级反向索引：task_id → tile_id 列表
        self._task_tile_index: Dict[str, List[str]] = {}
        # 分片已全部终结的任务；终结分片不会再改变状态，跳过它们以免逐步遍历全部历史分片。
        self._closed_task_ids: set[str] = set()
        self._completed_task_ids: set[str] = set()
        self._pending_aggregation: Dict[str, int] = {}
        self._ground_rr_last: Dict[str, int] = {}

        # ── Pipeline 委托对象 ─────────────────────────────────────────────
        self._dist_pipeline = DistributedPipeline(self)
        self._ground_pipeline = GroundPipeline(self)

    # ------------------------------------------------------------------ #
    # 生命周期                                                              #
    # ------------------------------------------------------------------ #

    def reset(self) -> None:
        self.close()
        self.__init__(self.cfg)

    def open_tiles(self):
        """按插入顺序返回未终结分片，与遍历 self.tiles 后筛选未终结分片的顺序一致。

        DONE/FAILED 不会再离开，故分片全部终结的任务记入缓存后不再扫描。
        """
        newly_closed = []
        for task_id, task in self.tasks.items():
            if task_id in self._closed_task_ids:
                continue
            live = False
            for tid in task.tile_ids:
                tile = self.tiles[tid]
                if tile.state not in (TileState.DONE, TileState.FAILED):
                    live = True
                    yield tile
            if not live:
                newly_closed.append(task_id)
        self._closed_task_ids.update(newly_closed)

    def close(self) -> None:
        self.lifecycle_logger.close()
        self.topology.close()

    def summary(self) -> Dict:
        summary = self.metrics.summary()
        failed_tasks = sum(task.failure_reason is not None or
                           any(self.tiles[tid].state == TileState.FAILED for tid in task.tile_ids)
                           for task in self.tasks.values())
        summary["overall"].update(
            failed_tasks=failed_tasks,
            pending_tasks=len(self.tasks) - self.metrics.completed_tasks - failed_tasks,
            aggregating_tasks=len(self._pending_aggregation),
        )
        summary["task_outcomes"] = []
        for task in self.tasks.values():
            tile_failure = next((self.tiles[tid].failure_reason.value for tid in task.tile_ids
                                 if self.tiles[tid].failure_reason is not None), None)
            failure = task.failure_reason or tile_failure
            completed = task.completion_time is not None
            summary["task_outcomes"].append({
                "task_id": task.task_id, "source_sat_id": task.source_sat_id,
                "release_seconds": task.release_time * self.cfg.dt,
                "deadline_seconds": task.deadline * self.cfg.dt if task.deadline is not None else None,
                "results_ready_seconds": task.results_ready_time * self.cfg.dt
                if task.results_ready_time is not None else None,
                "completion_seconds": task.completion_time * self.cfg.dt if completed else None,
                "latency_seconds": (task.completion_time - task.release_time) * self.cfg.dt
                if completed else None,
                "status": "completed" if completed else "failed" if failure else "pending",
                "stage": "aggregating" if task.task_id in self._pending_aggregation else None,
                "failure_reason": failure,
                "observed_age_seconds": (self.time - task.release_time) * self.cfg.dt,
                "result_relay_hops": sum(max(0, len(self.tiles[tid].result_path) - 1) for tid in task.tile_ids),
                "raw_relay_hops": sum(max(0, len(self.tiles[tid].raw_relay_path) - 1) for tid in task.tile_ids),
            })
        trace = [(task.task_id, task.source_sat_id, task.release_time,
                  [self.tiles[tid].data_size_mb for tid in task.tile_ids])
                 for task in self.tasks.values()]
        summary["experiment"] = {
            "config": asdict(self.cfg), "elapsed_sim_seconds": self.time * self.cfg.dt,
            "task_trace_sha256": hashlib.sha256(json.dumps(trace).encode()).hexdigest(),
            "tle_sha256": hashlib.sha256(json.dumps(self.topology.cfg.tle_lines).encode()).hexdigest(),
            "decision_tile_limit_per_sat": self.cfg.decision_tile_limit_per_sat,
            "compute_service_mode": self.cfg.compute_service_mode,
            "compute_timestamp_resolution_seconds": self.cfg.dt,
            "delivery_semantics": "all_results_at_ground_plus_fixed_relay_and_aggregation_v1",
            "aggregation_model": "uncontended_fixed_delay_rounded_up_to_step",
            "effective_postprocessing_delay_seconds": self._postprocessing_steps() * self.cfg.dt,
            "splitting_delay_seconds": 0.0,
            "result_buffering": self.cfg.result_buffering,
            "result_relay": self.cfg.result_relay,
            "relay_contact_guard": self.cfg.relay_contact_guard,
            "routing_information": "current global link graph and predicted ground windows; instantaneous telemetry; no control-plane cost",
            **runtime_metadata(),
        }
        if self.image_workload is not None:
            manifest = self.image_workload.manifest()
            summary["experiment"]["workload"] = manifest
            summary["experiment"]["workload_sha256"] = hashlib.sha256(
                json.dumps(manifest, sort_keys=True).encode()).hexdigest()
            summary["experiment"]["result_merge_model"] = (
                "all window outputs required; global merge charged by fixed aggregation_delay_s; "
                "no image inference or accuracy validation")
        if self.unique_transport_blocks:
            summary["experiment"]["unique_transport"] = {
                "rule": "non_overlapping_blocks_relayed_then_all_windows_queued_at_last_receiving_station",
                "blocks_xyxy": [box for box, _ in self.unique_transport_blocks],
                "block_mb": [size for _, size in self.unique_transport_blocks],
                "image_mb": math.fsum(size for _, size in self.unique_transport_blocks),
                "ground_windowing_delay_seconds": 0.0,
            }
        if self.cfg.compute_service_mode == "work_conserving":
            summary["resource"]["ground_compute_busy_time"] = dict(self.ground_compute_busy_time)
        return summary

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
        self._deadline_check()

        if self.pipeline_mode in {"distributed", "ground_relay", "ground_tiles", "ground_unique_relay"}:
            self._dist_pipeline.run(actions, links)
        else:
            self._ground_pipeline.run()

        self._advance_task_delivery(now=self.time + 1)
        self._deadline_check(now=self.time + 1)
        self._update_stats(links)

        # reward 计算
        waiting = (TileState.QUEUED, TileState.READY)
        tiles = self.tiles
        num_waiting = sum(
            1 for task_id, task in self.tasks.items() if task_id not in self._closed_task_ids
            for tid in task.tile_ids if tiles[tid].state in waiting
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
        contacts = self.contact_cache.windows_at(self.time * self.cfg.dt) if self.contact_cache else {}
        for k, lk in links.items():
            if lk.up:
                neighbors[lk.i].append(lk.j)
                neighbors[lk.j].append(lk.i)
            link_view[k] = {
                "up": lk.up,
                "bandwidth_mbps": lk.bandwidth_mbps,
                "latency_ms": lk.latency_ms,
                "contact_windows": reconcile_windows(contacts.get(k, []), lk.up,
                                                      float(self.cfg.topology.get("contact_scan_s", 30))),
            }

        sat_view: Dict[int, Dict] = {}
        for sat_id, sat in self.satellites.items():
            executing_remaining = 0.0
            exec_tile = None
            if sat.executing is not None:
                exec_tile = self.tiles.get(sat.executing)
                if exec_tile is not None and exec_tile.compute_cost > 0:
                    executing_remaining = (
                        exec_tile.remaining_compute or 0.0
                    ) / (exec_tile.compute_cost / sat.compute_rate)
            sat_view[sat_id] = {
                "queue_len": len(sat.queue),
                "compute_rate": sat.compute_rate,
                "mem_remaining_gb": sat.mem_capacity_gb - sat.mem_used_gb,
                "vram_remaining_gb": sat.vram_capacity_gb - sat.vram_used_gb,
                "executing_remaining": executing_remaining,
                "executing_remaining_s": float(exec_tile.remaining_compute or 0.0)
                if sat.executing is not None and exec_tile is not None else 0.0,
                "next_gs_windows": self.vis_cache.windows_at(sat_id, self.time),
            }
            if self.cfg.onboard_compute_profile is not None:
                sat_view[sat_id]["running_count"] = len(sat.executing_batch)
                sat_view[sat_id]["executing_remaining_s"] = sat.batch_remaining_s
                sat_view[sat_id]["executing_remaining"] = (
                    sat.batch_remaining_s / sat.batch_service_s if sat.batch_service_s else 0.0)

        gs_view: Dict[str, Dict] = {}
        for gs_id, gs in self.ground_stations.items():
            gs_view[gs_id] = {
                "queue_len": len(self.ground_tile_queues.get(gs_id, [])),
                "running": 1 if gs_id in self.ground_tile_running else 0,
                "compute_rate": gs.compute_rate,
                "bandwidth_mbps": gs.bandwidth_mbps,
                "min_elevation_deg": gs.min_elevation_deg,
                "receive_channels": gs.receive_channels,
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
        task_progress = {}
        task_pending_locations = {}
        for task_id, task in self.tasks.items():
            # 只有可调度分片引用这两项，其父任务必未终结；已终结任务不再汇总。
            if task_id in self._closed_task_ids:
                continue
            states = [self.tiles[tid].state for tid in task.tile_ids]
            task_progress[task_id] = {
                "total": len(states),
                "done": states.count(TileState.DONE),
                "failed": states.count(TileState.FAILED),
                "running": sum(s in (TileState.RUNNING, TileState.GROUND_RUNNING) for s in states),
                "awaiting_delivery": sum(s in (TileState.COMPUTED, TileState.RESULT_TRANSFERRING,
                                                TileState.DOWNLINKING) for s in states),
            }
            # 大图先按任务汇总位置，避免每片再次扫描所有兄弟分片。
            task_pending_locations[task_id] = Counter(
                self.tiles[tid].location for tid in self._task_tile_index.get(task_id, [])
                if tid in self.tiles and self.tiles[tid].state not in (TileState.DONE, TileState.FAILED))
        for sat_id, view in sat_view.items():
            view["ground_backlog_mb"] = {
                gs_id: sum(tr.remaining_mb for tr in self.downlink_transfers + self.ground_tile_transfers
                           if tr.src_sat == sat_id and tr.gs_id == gs_id)
                for gs_id in self.ground_stations
            }
            view["isl_backlog_mb"] = {
                nb: sum(tr.remaining_mb for tr in self.transfers if tr.src == sat_id and tr.dst == nb)
                for nb in neighbors[sat_id]
            }
        for gs_id, view in gs_view.items():
            view["visible_contenders"] = sum(
                any(option["gs_id"] == gs_id for option in options)
                for options in ground_options.values()
            )
        # 接收方广播已承诺工作量，含传输中的原始分片；尚未接单的 QUEUED 不计入。
        incoming = {tr.tile_id: tr.dst for tr in self.transfers if not tr.is_result}
        downlink_ids = {tr.tile_id for tr in self.downlink_transfers}
        for sid, view in sat_view.items():
            view["committed_compute_s"] = 0.0
            view["committed_result_mb"] = 0.0
            view["unrouted_result_mb"] = 0.0
        committed_windows = Counter()
        for tile in self.open_tiles():
            if (tile.ground_relay_target is not None and tile.state in
                    (TileState.GROUND_RELAY_PENDING, TileState.TRANSFERRING)):
                continue
            sid = incoming.get(tile.tile_id, tile.location)
            routed_input = (tile.input_target_sat_id is not None and
                            tile.state in (TileState.TRANSFERRING, TileState.INPUT_RELAY))
            if routed_input:
                # 中转节点只存转数据，计算承诺从派发起始终属于最终接收方。
                sid = tile.input_target_sat_id
            if routed_input or tile.tile_id in incoming or tile.state in (TileState.READY, TileState.RUNNING):
                seconds = (float(tile.remaining_compute or 0) if tile.state == TileState.RUNNING
                           else tile.compute_cost / self.satellites[sid].compute_rate)
                sat_view[sid]["committed_compute_s"] += self.compute_service_seconds(seconds)
                sat_view[sid]["committed_result_mb"] += self.tile_result_size_mb(tile)
                committed_windows[sid] += tile.compute_cost - tile.completed_compute_windows
            elif tile.state == TileState.COMPUTED and tile.tile_id not in downlink_ids:
                sat_view[sid]["unrouted_result_mb"] += self.tile_result_size_mb(tile)
        if self.cfg.onboard_compute_profile is not None:
            # 同一并发组的剩余墙钟只计一次；在途工作仍按已到齐估计，和原承诺口径一致。
            service = self.cfg.onboard_compute_profile["service_s"]
            sizes = sorted((int(key) for key in service), reverse=True)
            for sid, view in sat_view.items():
                sat = self.satellites[sid]
                pending = max(0, round(committed_windows[sid]) - len(sat.executing_batch))
                seconds = sat.batch_remaining_s
                for size in sizes:
                    count, pending = divmod(pending, size)
                    seconds += count * service[str(size)]
                view["committed_compute_s"] = seconds
        for gid, view in gs_view.items():
            tids = set(self.ground_tile_queues.get(gid, []))
            running = self.ground_tile_running.get(gid)
            if running is not None:
                tids.add(running)
            tids.update(tr.tile_id for tr in self.ground_tile_transfers if tr.gs_id == gid)
            # 原始数据尚在轨道网络时，最终地面计算方也必须广播这份接单负载。
            tids.update(tile.tile_id for tile in self.open_tiles()
                        if tile.ground_relay_target == gid and tile.state in
                        (TileState.GROUND_RELAY_PENDING, TileState.TRANSFERRING,
                         TileState.GROUND_QUEUED, TileState.GROUND_RUNNING))
            view["committed_compute_s"] = sum(
                self.compute_service_seconds(float(self.tiles[tid].remaining_compute or 0) if tid == running else
                                             self.tiles[tid].compute_cost / self.ground_stations[gid].compute_rate)
                    for tid in sorted(tids))
            if self.cfg.compute_service_mode == "work_conserving" and self.pipeline_mode == "ground_compute":
                comp = self.ground_task_running.get(gid)
                view["running"] = int(comp is not None)
                view["queue_len"] = len(self.ground_compute_queue.get(gid, []))
                committed_tasks = list(self.ground_compute_queue.get(gid, []))
                committed_tasks.extend(tr.task_id for tr in self.ground_task_transfers if tr.gs_id == gid)
                view["committed_compute_s"] = (comp.remaining_compute if comp else 0.0) + sum(
                    sum(self.tiles[tid].compute_cost for tid in self.tasks[task_id].tile_ids
                        if self.tiles[tid].state not in (TileState.DONE, TileState.FAILED)) /
                    self.ground_stations[gid].compute_rate
                    for task_id in committed_tasks)
        in_transfer = {tr.tile_id for tr in self.transfers}
        for tile in self.open_tiles():
            tile_id = tile.tile_id
            if tile.state not in (TileState.QUEUED, TileState.READY):
                continue
            if tile.completed_compute_windows:
                # 部分执行的大块已承诺在当前节点完成，不把它再次当作全新任务迁移。
                continue
            if self.cfg.execution_commitment and tile.state == TileState.READY:
                continue
            sibling_locations = dict(task_pending_locations[tile.parent_task_id])
            sibling_locations[tile.location] -= 1
            if not sibling_locations[tile.location]:
                del sibling_locations[tile.location]
            tile_view[tile_id] = {
                "source_sat_id": self.tasks[tile.parent_task_id].source_sat_id,
                "task_image_size_mb": self.tasks[tile.parent_task_id].image_size_mb,
                "unique_input_size_mb": self._task_unique_blocks.get(tile.parent_task_id, {}).get(tile_id),
                "state": tile.state.value,
                "location": tile.location,
                "data_size_mb": tile.data_size_mb,
                "data_size_gb": self.memory_size_gb(tile.data_size_mb),
                "compute_cost": tile.compute_cost,
                "vram_req_gb": tile.vram_req_gb,
                "in_transfer": tile_id in in_transfer,
                "parent_task_id": tile.parent_task_id,
                "task_progress": task_progress[tile.parent_task_id],
                "waiting_time": (self.time - tile.timestamps.created) * self.cfg.dt,
                "result_size_mb": self.tile_result_size_mb(tile),
                "sibling_locations": sibling_locations,
                "task_pending_count": sum(sibling_locations.values()) + 1,
                "deadline_remaining_s": None if tile.deadline is None else
                max(0.0, (tile.deadline - self.time) * self.cfg.dt),
            }

        pending_task_tiles: Dict[str, Dict[str, Dict]] = {}
        for tid, view in tile_view.items():
            if view["state"] == "QUEUED" and not view["in_transfer"]:
                pending_task_tiles.setdefault(view["parent_task_id"], {})[tid] = view
        return EnvState(
            time=self.time,
            satellites=sat_view,
            ground_stations=gs_view,
            ground_options=ground_options,
            neighbors=neighbors,
            links=link_view,
            tiles=tile_view,
            pending_task_tiles=pending_task_tiles,
            config={
                "sar_source_isolation": self.cfg.sar_source_isolation,
                "task_source_sat_ids": self.cfg.task_source_sat_ids,
                "dt": self.cfg.dt,
                "compute_service_mode": self.cfg.compute_service_mode,
                "onboard_compute_profile": self.cfg.onboard_compute_profile,
                "num_sats": self.cfg.num_sats,
                "num_tiles": self.cfg.num_tiles,
                "decision_tile_limit_per_sat": self.cfg.decision_tile_limit_per_sat,
                "image_tiling": self.image_workload is not None,
                "memory_mb_per_gb": 1000 if self.image_workload else 1024,
                "result_size_mb": self.cfg.result_size_mb,
                "mem_capacity_gb": self.cfg.mem_capacity_gb,
                "vram_capacity_gb": self.cfg.vram_capacity_gb,
                "image_size_mb": self.cfg.image_size_mb,
                "gs_window_lookahead_s": self.vis_cache.lookahead_s,
                "remaining_sim_seconds": max(0, self.cfg.sim_steps - self.time) * self.cfg.dt,
                "sim_duration_seconds": self.cfg.sim_steps * self.cfg.dt,
                "postprocessing_delay_s": self._postprocessing_steps() * self.cfg.dt,
                "contact_scan_s": float(self.cfg.topology.get("contact_scan_s", 30)),
                "isl_bandwidth_floor_mbps": self.topology.cfg.bandwidth_mbps_min / (
                    1 + self.topology.cfg.max_range_km / self.topology.cfg.bandwidth_distance_scale_km
                ) if self.topology.cfg.bandwidth_distance_scale_km > 0 and self.topology.cfg.max_range_km > 0
                else (self.topology.cfg.bandwidth_mbps_min if self.topology.cfg.bandwidth_distance_scale_km <= 0 else 0),
                "result_relay": self.cfg.result_relay,
                "relay_max_hops": self.cfg.relay_max_hops,
                "relay_contact_guard": self.cfg.relay_contact_guard,
                "relay_bandwidth_floor_mbps": self.topology.cfg.bandwidth_mbps_min,
                "input_relay_max_hops": self.cfg.input_relay_max_hops,
                "ground_input_relay": self.cfg.ground_input_relay,
                "decision_interval_seconds": self.cfg.decision_interval_steps * self.cfg.dt,
            },
        )

    # ------------------------------------------------------------------ #
    # 任务到达 & tile 入队                                                  #
    # ------------------------------------------------------------------ #

    def _task_arrivals(self, t: int) -> None:
        if self.cfg.task_arrival_end_step is not None and t >= self.cfg.task_arrival_end_step:
            return
        scheduled = self._scheduled_task_arrivals.get(t, [])
        if self.cfg.scheduled_task_arrivals is None:
            lam = self.cfg.task_arrival_rate
            num_tasks = int(self.rng.poisson(lam)) if lam > 0 else 0
        else:
            num_tasks = len(scheduled)
        for index in range(num_tasks):
            task_id = f"task_{len(self.tasks)}"
            src = (self.task_sources[self.py_rng.randrange(len(self.task_sources))]
                   if self.cfg.scheduled_task_arrivals is None else scheduled[index]["source_sat_id"])
            deadline = (
                t + self.cfg.deadline_steps if self.cfg.deadline_steps > 0 else None
            )
            event = scheduled[index] if scheduled else {}
            workload = ImageTiling.from_dict(event["image_tiling"]) if "image_tiling" in event else self.image_workload
            descriptors = workload.tiles() if workload else ()
            tile_count = len(descriptors) if workload else self.cfg.num_tiles
            unique_blocks = (workload.unique_transport_blocks(tile_count)
                             if workload and math.isqrt(tile_count) ** 2 == tile_count else ())
            self._task_workloads[task_id] = workload
            task = Task(
                task_id=task_id,
                source_sat_id=src,
                release_time=t,
                image_size_mb=workload.image_size_mb if workload else self.cfg.image_size_mb,
                num_tiles=tile_count,
                deadline=deadline,
                aggregation_delay_s=event.get("aggregation_delay_s"),
            )
            self.tasks[task_id] = task
            self.metrics.total_tasks += 1
            if workload is None:
                sizes = np.maximum(0.01, 1.0 + self.rng.normal(0.0, 0.02, self.cfg.num_tiles))
                sizes *= self.cfg.image_size_mb / sizes.sum()
            elif self.pipeline_mode == "ground_unique_relay":
                sizes = [size for _, size in unique_blocks]
            else:
                # 几何分片不消耗到达 RNG，overlap/打包消融可共享同一任务到达轨迹。
                sizes = [tile.data_size_mb for tile in descriptors]
            created_tiles: List[Tile] = []
            actual_outputs = scheduled[index].get("window_result_bytes") if scheduled else None
            output_by_box = {}
            if actual_outputs is not None:
                boxes = sorted({box for descriptor in descriptors for box in descriptor.windows_xyxy},
                               key=lambda box: (box[1], box[0]))
                output_by_box = dict(zip(boxes, actual_outputs, strict=True))
            self._task_unique_blocks[task_id] = {}
            for k in range(tile_count):
                tile_id = f"{task_id}_tile_{k}"
                size_mb = float(sizes[k])
                descriptor = descriptors[k] if workload is not None else None
                # 一个包内窗口顺序推理；显存代理按单窗而非整个传输包计算。
                inference_mb = workload.window_size_mb if workload else size_mb
                if unique_blocks:
                    self._task_unique_blocks[task_id][tile_id] = unique_blocks[k][1]
                vram_req = self.cfg.vram_base_gb + self.cfg.vram_alpha_per_mb * inference_mb
                tile = Tile(
                    tile_id=tile_id,
                    parent_task_id=task_id,
                    data_size_mb=size_mb,
                    compute_cost=descriptor.compute_cost if descriptor else self.cfg.compute_cost_per_tile,
                    vram_req_gb=vram_req,
                    state=TileState.CREATED,
                    location=src,
                    timestamps=TileTimestamps(created=t),
                    deadline=deadline,
                    result_size_mb=descriptor.result_size_mb if descriptor else None,
                    windows_xyxy=descriptor.windows_xyxy if descriptor else (),
                    actual_result_size_mb=(sum(output_by_box[box] for box in descriptor.windows_xyxy) / 1e6
                                           if actual_outputs is not None else None),
                )
                self.tiles[tile_id] = tile
                task.tile_ids.append(tile_id)
                self._task_tile_index.setdefault(task.task_id, []).append(tile_id)
                self.metrics.total_tiles += 1
                self._log_tile_event(tile, "created", sat_from=None, sat_to=src)
                created_tiles.append(tile)

            if self.pipeline_mode in {"distributed", "ground_relay", "ground_tiles", "ground_unique_relay"}:
                for tile in created_tiles:
                    self._enqueue_tile(src, tile)
            else:
                self._ground_pipeline.enqueue_ground_task(task, created_tiles)

    def _enqueue_tile(self, sat_id: int, tile: Tile) -> None:
        sat = self.satellites[sat_id]
        if not self._reserve_tile_memory(tile, sat_id):
            self._fail_tile(tile, FailureReason.MEM_FULL)
            return
        tile.state = TileState.QUEUED
        sat.queue.append(tile.tile_id)
        self._log_tile_event(tile, "queued", sat_from=None, sat_to=sat_id)

    def _reserve_tile_memory(self, tile: Tile, sat_id: int) -> bool:
        sat = self.satellites[sat_id]
        if sat_id in tile.memory_holders:
            return True
        size_gb = self.memory_size_gb(tile.data_size_mb)
        if sat.mem_used_gb + size_gb > sat.mem_capacity_gb:
            return False
        sat.mem_used_gb += size_gb
        tile.memory_holders.add(sat_id)
        self.metrics.update_mem_peak(sat_id, sat.mem_used_gb)
        return True

    def _release_tile_memory(self, tile: Tile, sat_id: int) -> None:
        if sat_id in tile.memory_holders:
            sat = self.satellites[sat_id]
            sat.mem_used_gb = max(0.0, sat.mem_used_gb - self.memory_size_gb(tile.data_size_mb))
            tile.memory_holders.remove(sat_id)

    def _reserve_result_memory(self, tile: Tile, sat_id: int) -> bool:
        if sat_id in tile.result_memory_holders:
            return True
        sat = self.satellites[sat_id]
        size = self.memory_size_gb(self.tile_result_size_mb(tile))
        if sat.mem_used_gb + size > sat.mem_capacity_gb:
            return False
        sat.mem_used_gb += size
        tile.result_memory_holders.add(sat_id)
        self.metrics.update_mem_peak(sat_id, sat.mem_used_gb)
        return True

    def _release_result_memory(self, tile: Tile, sat_id: int) -> None:
        if sat_id in tile.result_memory_holders:
            sat = self.satellites[sat_id]
            sat.mem_used_gb = max(0.0, sat.mem_used_gb - self.memory_size_gb(self.tile_result_size_mb(tile)))
            tile.result_memory_holders.remove(sat_id)

    # ------------------------------------------------------------------ #
    # 失败处理                                                              #
    # ------------------------------------------------------------------ #

    def _fail_tile(self, tile: Tile, reason: FailureReason) -> None:
        if tile.state in (TileState.DONE, TileState.FAILED):
            return
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
                self.transfers.remove(tr)
                break
        for sat_id in list(tile.memory_holders):
            self._release_tile_memory(tile, sat_id)
        for sat_id in list(tile.result_memory_holders):
            self._release_result_memory(tile, sat_id)
        sat = self.satellites.get(tile.location)
        if sat is not None:
            if tile.tile_id in sat.queue:
                sat.queue.remove(tile.tile_id)
            if tile.tile_id in sat.executing_batch:
                sat.executing_batch[:] = [tid for tid in sat.executing_batch if tid != tile.tile_id]
                sat.executing = sat.executing_batch[0] if sat.executing_batch else None
                if not sat.executing_batch:
                    sat.batch_remaining_s = 0.0
                    sat.vram_used_gb = 0.0
            elif sat.executing == tile.tile_id:
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

    def _deadline_check(self, now: Optional[int] = None) -> None:
        if self.cfg.deadline_steps <= 0:
            return
        now = self.time if now is None else now
        for tile in list(self.open_tiles()):
            if tile.state in (TileState.DONE, TileState.FAILED):
                continue
            if tile.deadline is not None and now >= tile.deadline:
                self._fail_tile(tile, FailureReason.DEADLINE_MISS)
        if self.pipeline_mode == "ground_compute":
            self._ground_pipeline.cancel_failed_tasks()

    def _update_stats(self, links: Dict[str, object]) -> None:
        # 与逐星调用 update_queue_stats/update_mem_peak/update_vram_peak 等价，内联以省去每步数百次方法调用。
        queue_sum, mem_peak, vram_peak = (self.metrics.queue_len_sum, self.metrics.mem_peak,
                                          self.metrics.vram_peak)
        for sat_id, sat in self.satellites.items():
            queue_sum[sat_id] = queue_sum.get(sat_id, 0.0) + len(sat.queue)
            mem_peak[sat_id] = max(mem_peak.get(sat_id, 0.0), sat.mem_used_gb)
            vram_peak[sat_id] = max(vram_peak.get(sat_id, 0.0), sat.vram_used_gb)
        self.metrics.finalize_step()

    def _check_task_done(self, task_id: str) -> None:
        if task_id in self._completed_task_ids or task_id in self._pending_aggregation:
            return
        task = self.tasks[task_id]
        if task.failure_reason is not None:
            return
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
        task.results_ready_time = end_time
        delay_s = self.cfg.aggregation_delay_s if task.aggregation_delay_s is None else task.aggregation_delay_s
        ready = end_time + math.ceil(self.cfg.ground_relay_delay_s / self.cfg.dt)
        if self.cfg.serialized_aggregation:
            ready = max(ready, self._aggregation_busy_until)
        due = ready + math.ceil(delay_s / self.cfg.dt)
        if not self.cfg.serialized_aggregation and task.aggregation_delay_s is None:
            due = end_time + self._postprocessing_steps()
        self._pending_aggregation[task_id] = due
        if self.cfg.serialized_aggregation:
            self._aggregation_busy_until = due

    def _postprocessing_steps(self) -> int:
        return math.ceil((self.cfg.ground_relay_delay_s + self.cfg.aggregation_delay_s) / self.cfg.dt)

    def _advance_task_delivery(self, now: int) -> None:
        # 聚合完成才产生任务奖励；分片 DONE 不代表整图已经交付。
        for task_id, due in list(self._pending_aggregation.items()):
            task = self.tasks[task_id]
            if task.deadline is not None and due > task.deadline and now >= task.deadline:
                task.failure_reason = "aggregation_deadline_miss"
                del self._pending_aggregation[task_id]
                self._log_task_event(task_id, "task_failed", None, None,
                                     {"reason": task.failure_reason, "time": now,
                                      "time_seconds": now * self.cfg.dt})
            elif due <= now:
                task.completion_time = due
                self.metrics.record_task_latency(due - task.release_time)
                self._completed_task_ids.add(task_id)
                del self._pending_aggregation[task_id]
                self._log_task_event(task_id, "task_delivered", None, None,
                                     {"time": due, "time_seconds": due * self.cfg.dt})

    # ------------------------------------------------------------------ #
    # 辅助                                                                  #
    # ------------------------------------------------------------------ #

    def tile_result_size_mb(self, tile: Tile) -> float:
        if tile.actual_result_size_mb is not None and tile.timestamps.end_compute is not None:
            return tile.actual_result_size_mb
        return self.cfg.result_size_mb if tile.result_size_mb is None else tile.result_size_mb

    def compute_service_seconds(self, seconds: float) -> float:
        if self.cfg.compute_service_mode == "work_conserving":
            return max(0.0, seconds)
        return math.ceil(seconds / self.cfg.dt) * self.cfg.dt

    def memory_size_gb(self, size_mb: float) -> float:
        return size_mb / (1000 if self.image_workload else 1024)

    def _get_links(self, t: int) -> Dict[str, object]:
        sampled_t = (t // self.topology_update_steps) * self.topology_update_steps
        if self._links_cache_time != sampled_t:
            self._links_cache = self.topology.snapshot(sampled_t, dt=self.cfg.dt)
            self._links_cache_time = sampled_t
        if self.cfg.topology.get("acquisition_delay_s", 0):
            for link in self._links_cache.values():
                if not link.up and t * self.cfg.dt >= link.acquisition_ready_s:
                    link.up = True
                    self._dist_pipeline._up_cache = None
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
            "time_seconds": self.time * self.cfg.dt,
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
        if event == "created" and self.image_workload is not None:
            record.update(windows_xyxy=tile.windows_xyxy, data_size_mb=tile.data_size_mb,
                          compute_cost=tile.compute_cost, result_size_mb=self.tile_result_size_mb(tile))
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
            "time_seconds": self.time * self.cfg.dt,
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
            receive_channels=int(item.get("receive_channels", 1)),
        )
        gs = stations[gs_id]
        if gs.receive_channels <= 0 or gs.bandwidth_mbps <= 0 or gs.compute_rate <= 0:
            raise ValueError("ground station channels, bandwidth and compute_rate must be positive")
    return stations
