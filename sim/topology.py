from __future__ import annotations

import atexit
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Dict, List, Tuple
import math
import random

from sim.entities import Link
from sim.orbit import load_satellites, parse_start_time_utc, position_km


def link_key(i: int, j: int) -> str:
    a, b = sorted((i, j))
    return f"{a}-{b}"


@dataclass
class TopologyConfig:
    num_sats: int
    bandwidth_mbps_min: float
    bandwidth_mbps_max: float
    bandwidth_period: int
    bandwidth_noise: float
    latency_ms: float
    seed: int
    start_time_utc: str
    tle_lines: List[Tuple[str, str]]
    earth_radius_km: float
    min_elevation_deg: float
    max_range_km: float
    bandwidth_distance_scale_km: float
    visibility_workers: int


class TopologyModel:
    def __init__(self, cfg: TopologyConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        # 为每条链路生成固定相位，保证带宽随时间的变化可复现
        self.phases = {
            (i, j): self.rng.random() * 2.0 * math.pi
            for i in range(cfg.num_sats)
            for j in range(i + 1, cfg.num_sats)
        }
        self.t0 = parse_start_time_utc(cfg.start_time_utc)
        self.sat_recs = load_satellites(cfg.tle_lines) if cfg.tle_lines else []
        self.visibility_workers = max(1, cfg.visibility_workers)
        self._executor = None
        if self.visibility_workers > 1:
            try:
                self._executor = ProcessPoolExecutor(max_workers=self.visibility_workers)
            except Exception:
                # Fallback for restricted environments (e.g. semaphore limits).
                self._executor = None
                self.visibility_workers = 1
        if self._executor is not None:
            atexit.register(self._executor.shutdown, wait=False, cancel_futures=True)

    def _get_bandwidth(self, a: int, b: int, t: int, distance_km: float) -> float:
        phase = self.phases[(a, b)]
        if self.cfg.bandwidth_period <= 0:
            bw = self.cfg.bandwidth_mbps_max
        else:
            # 带宽：正弦波 + 噪声，模拟随时间波动
            base = (self.cfg.bandwidth_mbps_min + self.cfg.bandwidth_mbps_max) / 2.0
            amp = (self.cfg.bandwidth_mbps_max - self.cfg.bandwidth_mbps_min) / 2.0
            bw = base + amp * math.sin(2.0 * math.pi * t / self.cfg.bandwidth_period + phase)
        noise = self.rng.uniform(-self.cfg.bandwidth_noise, self.cfg.bandwidth_noise)
        bw = max(self.cfg.bandwidth_mbps_min, min(self.cfg.bandwidth_mbps_max, bw + noise))
        if self.cfg.bandwidth_distance_scale_km > 0:
            bw = bw / (1.0 + distance_km / self.cfg.bandwidth_distance_scale_km)
        return bw

    def snapshot(self, t: int) -> Dict[str, Link]:
        links: Dict[str, Link] = {}
        n = min(self.cfg.num_sats, len(self.sat_recs))
        positions = [position_km(self.sat_recs[i], self.t0, t) for i in range(n)]
        pairs: List[Tuple[int, int]] = [
            (i, j) for i in range(self.cfg.num_sats) for j in range(i + 1, self.cfg.num_sats)
        ]

        if self._executor is None or len(pairs) < 64:
            results = [
                _visible_from_positions(
                    positions[i] if i < n else (0.0, 0.0, 0.0),
                    positions[j] if j < n else (0.0, 0.0, 0.0),
                    self.cfg.earth_radius_km,
                    self.cfg.min_elevation_deg,
                    self.cfg.max_range_km,
                )
                for i, j in pairs
            ]
        else:
            tasks = [
                (
                    positions[i] if i < n else (0.0, 0.0, 0.0),
                    positions[j] if j < n else (0.0, 0.0, 0.0),
                    self.cfg.earth_radius_km,
                    self.cfg.min_elevation_deg,
                    self.cfg.max_range_km,
                )
                for i, j in pairs
            ]
            results = list(self._executor.map(_visible_task, tasks, chunksize=256))

        for (i, j), (up, distance_km) in zip(pairs, results):
            bw = self._get_bandwidth(i, j, t, distance_km)
            links[link_key(i, j)] = Link(
                i=i, j=j, up=up, bandwidth_mbps=bw, latency_ms=self.cfg.latency_ms
            )
        return links


def _elevation_deg(r: Tuple[float, float, float], los: Tuple[float, float, float]) -> float:
    # r 为卫星地心位置向量，los 为指向对方的视线向量
    r_norm = math.sqrt(r[0] * r[0] + r[1] * r[1] + r[2] * r[2])
    l_norm = math.sqrt(los[0] * los[0] + los[1] * los[1] + los[2] * los[2])
    if r_norm <= 0 or l_norm <= 0:
        return -90.0
    # local vertical 指向地心，因此用 -r
    dot = (-r[0] * los[0] + -r[1] * los[1] + -r[2] * los[2]) / (r_norm * l_norm)
    dot = max(-1.0, min(1.0, dot))
    angle = math.degrees(math.acos(dot))
    return 90.0 - angle


def _visible_task(args: Tuple[Tuple[float, float, float], Tuple[float, float, float], float, float, float]) -> Tuple[bool, float]:
    return _visible_from_positions(*args)


def _visible_from_positions(
    r1: Tuple[float, float, float],
    r2: Tuple[float, float, float],
    earth_radius_km: float,
    min_elevation_deg: float,
    max_range_km: float,
) -> Tuple[bool, float]:
    if r1 == (0.0, 0.0, 0.0) or r2 == (0.0, 0.0, 0.0):
        return False, 0.0
    dx = r2[0] - r1[0]
    dy = r2[1] - r1[1]
    dz = r2[2] - r1[2]
    distance_km = math.sqrt(dx * dx + dy * dy + dz * dz)
    if max_range_km > 0 and distance_km > max_range_km:
        return False, distance_km

    r1_norm = math.sqrt(r1[0] * r1[0] + r1[1] * r1[1] + r1[2] * r1[2])
    r2_norm = math.sqrt(r2[0] * r2[0] + r2[1] * r2[1] + r2[2] * r2[2])
    if r1_norm <= earth_radius_km or r2_norm <= earth_radius_km:
        return False, distance_km

    dot = r1[0] * r2[0] + r1[1] * r2[1] + r1[2] * r2[2]
    cos_sep = dot / (r1_norm * r2_norm)
    cos_sep = max(-1.0, min(1.0, cos_sep))
    separation_deg = math.degrees(math.acos(cos_sep))
    h1 = math.degrees(math.acos(max(-1.0, min(1.0, earth_radius_km / r1_norm))))
    h2 = math.degrees(math.acos(max(-1.0, min(1.0, earth_radius_km / r2_norm))))
    if separation_deg > (h1 + h2):
        return False, distance_km

    if min_elevation_deg > 0:
        los1 = (dx, dy, dz)
        los2 = (-dx, -dy, -dz)
        elev1 = _elevation_deg(r1, los1)
        elev2 = _elevation_deg(r2, los2)
        if elev1 < min_elevation_deg or elev2 < min_elevation_deg:
            return False, distance_km
    return True, distance_km
