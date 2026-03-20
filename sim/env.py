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
    EnvState,
    FailureReason,
    Satellite,
    Task,
    Tile,
    TileState,
    TileTimestamps,
    Transfer,
)
from sim.metrics import Metrics
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
        tle_lines = _resolve_tle_lines(cfg.topology)
        topo_cfg = TopologyConfig(
            num_sats=cfg.num_sats,
            mode=str(cfg.topology.get("mode", "random")),
            link_up_prob=float(cfg.topology.get("link_up_prob", 0.6)),
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
        )
        if topo_cfg.mode == "skyfield" and not topo_cfg.tle_lines:
            raise ValueError("topology.mode=skyfield requires non-empty topology.tle_lines")
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
        self.metrics = Metrics()

    def reset(self) -> None:
        self.__init__(self.cfg)

    def step(self, actions: List[Action]) -> StepResult:
        t = self.time
        links = self.topology.snapshot(t)

        # 每步：任务到达 -> 调度决策 -> 传输推进 -> 计算推进 -> 统计
        self._task_arrivals(t)
        self._apply_actions(actions, links)
        self._advance_transfers(links)
        self._advance_compute()
        self._deadline_check()
        self._update_stats(links)

        self.time += 1
        return StepResult(time=t, metrics=self.metrics)

    def export_state(self) -> EnvState:
        links = self.topology.snapshot(self.time)
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
                self._enqueue_tile(src, tile)

    def _enqueue_tile(self, sat_id: int, tile: Tile) -> None:
        sat = self.satellites[sat_id]
        needed_gb = tile.data_size_mb / 1024.0
        if sat.mem_used_gb + needed_gb > sat.mem_capacity_gb:
            self._fail_tile(tile, FailureReason.MEM_FULL)
            return
        sat.mem_used_gb += needed_gb
        tile.state = TileState.QUEUED
        sat.queue.append(tile.tile_id)

    def _apply_actions(self, actions: List[Action], links: Dict[str, object]) -> None:
        action_map = {a.tile_id: a for a in actions}
        for tile_id, tile in self.tiles.items():
            if tile.state not in (TileState.QUEUED, TileState.READY):
                continue
            if any(tr.tile_id == tile_id for tr in self.transfers):
                continue
            action = action_map.get(tile_id)
            if not action:
                continue
            if action.action_type == ActionType.WAIT:
                continue
            if action.action_type == ActionType.LOCAL:
                # 本地计算，直接标记 READY
                tile.state = TileState.READY
                continue
            if action.action_type == ActionType.OFFLOAD:
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

            if sat.executing is not None:
                tile = self.tiles[sat.executing]
                tile.remaining_compute = max(0.0, float(tile.remaining_compute) - self.cfg.dt)
                if tile.remaining_compute <= 1e-6:
                    tile.state = TileState.DONE
                    tile.timestamps.end_compute = self.time
                    sat.executing = None
                    sat.vram_used_gb = max(0.0, sat.vram_used_gb - tile.vram_req_gb)
                    size_gb = tile.data_size_mb / 1024.0
                    sat.mem_used_gb = max(0.0, sat.mem_used_gb - size_gb)
                    self.metrics.record_tile_latency(tile.timestamps.end_compute - tile.timestamps.created)
                    self._check_task_done(tile.parent_task_id)

    def _check_task_done(self, task_id: str) -> None:
        task = self.tasks[task_id]
        for tid in task.tile_ids:
            if self.tiles[tid].state != TileState.DONE:
                return
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
        used_by_link: Dict[str, float] = {}
        for tr in self.transfers:
            lk = links.get(tr.link_key)
            if not lk or not lk.up:
                continue
            rate_mb_s = lk.bandwidth_mbps / 8.0
            used_by_link[tr.link_key] = used_by_link.get(tr.link_key, 0.0) + rate_mb_s * self.cfg.dt
        for k, lk in links.items():
            if not lk.up:
                continue
            avail = (lk.bandwidth_mbps / 8.0) * self.cfg.dt
            used = used_by_link.get(k, 0.0)
            self.metrics.update_link_usage(k, used, avail)
        self.metrics.finalize_step()

    def _fail_tile(self, tile: Tile, reason: FailureReason) -> None:
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

    def _deadline_check(self) -> None:
        if self.cfg.deadline_steps <= 0:
            return
        for tile in self.tiles.values():
            if tile.state in (TileState.DONE, TileState.FAILED):
                continue
            if tile.deadline is not None and self.time > tile.deadline:
                self._fail_tile(tile, FailureReason.DEADLINE_MISS)


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
