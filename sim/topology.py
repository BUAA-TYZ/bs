from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple
import math
import random

from sim.entities import Link
from sim.orbit import EphemerisConfig, load_satellites, parse_start_time_utc, position_km


def link_key(i: int, j: int) -> str:
    a, b = sorted((i, j))
    return f"{a}-{b}"


@dataclass
class TopologyConfig:
    num_sats: int
    mode: str
    link_up_prob: float
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

    def get_link(self, i: int, j: int, t: int) -> Link:
        a, b = sorted((i, j))
        # 链路可见性（up/down）简化为伯努利随机过程
        up = self.rng.random() < self.cfg.link_up_prob
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
        return Link(i=a, j=b, up=up, bandwidth_mbps=bw, latency_ms=self.cfg.latency_ms)

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

    def _visible_sgp4(self, i: int, j: int, t: int) -> Tuple[bool, float]:
        if i >= len(self.sat_recs) or j >= len(self.sat_recs):
            return False, 0.0
        r1 = position_km(self.sat_recs[i], self.t0, t)
        r2 = position_km(self.sat_recs[j], self.t0, t)
        # 若 SGP4 出错，位置为原点，直接判不可见
        if r1 == (0.0, 0.0, 0.0) or r2 == (0.0, 0.0, 0.0):
            return False, 0.0
        dx = r2[0] - r1[0]
        dy = r2[1] - r1[1]
        dz = r2[2] - r1[2]
        distance_km = math.sqrt(dx * dx + dy * dy + dz * dz)
        if self.cfg.max_range_km > 0 and distance_km > self.cfg.max_range_km:
            return False, distance_km
        # 地球遮挡判定：线段到地心的最小距离必须大于地球半径
        r1_dot_r1 = r1[0] * r1[0] + r1[1] * r1[1] + r1[2] * r1[2]
        r2_dot_r2 = r2[0] * r2[0] + r2[1] * r2[1] + r2[2] * r2[2]
        r1_dot_r2 = r1[0] * r2[0] + r1[1] * r2[1] + r1[2] * r2[2]
        # 参数化线段最短距离
        denom = r1_dot_r1 - 2.0 * r1_dot_r2 + r2_dot_r2
        if denom <= 0:
            return False, distance_km
        t_min = max(0.0, min(1.0, (r1_dot_r1 - r1_dot_r2) / denom))
        cx = r1[0] + t_min * (r2[0] - r1[0])
        cy = r1[1] + t_min * (r2[1] - r1[1])
        cz = r1[2] + t_min * (r2[2] - r1[2])
        min_dist = math.sqrt(cx * cx + cy * cy + cz * cz)
        if min_dist <= self.cfg.earth_radius_km:
            return False, distance_km
        # 最小仰角约束（可选）
        if self.cfg.min_elevation_deg > 0:
            # elevation = 90 - angle(los, -r1)
            los1 = (dx, dy, dz)
            los2 = (-dx, -dy, -dz)
            elev1 = _elevation_deg(r1, los1)
            elev2 = _elevation_deg(r2, los2)
            if elev1 < self.cfg.min_elevation_deg or elev2 < self.cfg.min_elevation_deg:
                return False, distance_km
        return True, distance_km

    def snapshot(self, t: int) -> Dict[str, Link]:
        links: Dict[str, Link] = {}
        for i in range(self.cfg.num_sats):
            for j in range(i + 1, self.cfg.num_sats):
                if self.cfg.mode == "sgp4":
                    up, distance_km = self._visible_sgp4(i, j, t)
                    bw = self._get_bandwidth(i, j, t, distance_km)
                    lk = Link(i=i, j=j, up=up, bandwidth_mbps=bw, latency_ms=self.cfg.latency_ms)
                else:
                    lk = self.get_link(i, j, t)
                links[link_key(i, j)] = lk
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
