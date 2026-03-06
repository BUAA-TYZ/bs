from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List
import json
import math


@dataclass
class Metrics:
    total_tiles: int = 0
    completed_tiles: int = 0
    failed_tiles: int = 0
    total_tasks: int = 0
    completed_tasks: int = 0
    tile_latencies: List[float] = field(default_factory=list)
    task_latencies: List[float] = field(default_factory=list)
    failure_reasons: Dict[str, int] = field(default_factory=dict)
    queue_len_sum: Dict[int, float] = field(default_factory=dict)
    queue_len_steps: int = 0
    compute_busy_time: Dict[int, float] = field(default_factory=dict)
    mem_peak: Dict[int, float] = field(default_factory=dict)
    vram_peak: Dict[int, float] = field(default_factory=dict)
    link_used_mb: Dict[str, float] = field(default_factory=dict)
    link_avail_mb: Dict[str, float] = field(default_factory=dict)

    def record_failure(self, reason: str) -> None:
        self.failure_reasons[reason] = self.failure_reasons.get(reason, 0) + 1
        self.failed_tiles += 1

    def record_tile_latency(self, latency: float) -> None:
        self.completed_tiles += 1
        self.tile_latencies.append(latency)

    def record_task_latency(self, latency: float) -> None:
        self.completed_tasks += 1
        self.task_latencies.append(latency)

    def update_queue_stats(self, sat_id: int, queue_len: int) -> None:
        self.queue_len_sum[sat_id] = self.queue_len_sum.get(sat_id, 0.0) + queue_len

    def update_compute_busy(self, sat_id: int, dt: float) -> None:
        self.compute_busy_time[sat_id] = self.compute_busy_time.get(sat_id, 0.0) + dt

    def update_mem_peak(self, sat_id: int, mem_gb: float) -> None:
        self.mem_peak[sat_id] = max(self.mem_peak.get(sat_id, 0.0), mem_gb)

    def update_vram_peak(self, sat_id: int, vram_gb: float) -> None:
        self.vram_peak[sat_id] = max(self.vram_peak.get(sat_id, 0.0), vram_gb)

    def update_link_usage(self, link_key: str, used_mb: float, avail_mb: float) -> None:
        self.link_used_mb[link_key] = self.link_used_mb.get(link_key, 0.0) + used_mb
        self.link_avail_mb[link_key] = self.link_avail_mb.get(link_key, 0.0) + avail_mb

    def finalize_step(self) -> None:
        self.queue_len_steps += 1

    def _percentiles(self, values: List[float], ps: List[float]) -> Dict[str, float]:
        if not values:
            return {f"p{int(p)}": math.nan for p in ps}
        arr = sorted(values)
        res = {}
        for p in ps:
            k = int(math.ceil(p / 100.0 * len(arr))) - 1
            k = max(0, min(k, len(arr) - 1))
            res[f"p{int(p)}"] = arr[k]
        return res

    def summary(self) -> Dict:
        tile_p = self._percentiles(self.tile_latencies, [95, 99])
        task_p = self._percentiles(self.task_latencies, [95, 99])
        queue_avg = {
            sat_id: (self.queue_len_sum.get(sat_id, 0.0) / max(1, self.queue_len_steps))
            for sat_id in self.queue_len_sum
        }
        # 链路利用率 = 实际发送 / 可发送
        link_util = {
            k: (self.link_used_mb.get(k, 0.0) / max(1e-9, self.link_avail_mb.get(k, 1e-9)))
            for k in self.link_avail_mb
        }
        return {
            "overall": {
                "completed_tiles": self.completed_tiles,
                "total_tiles": self.total_tiles,
                "completed_tasks": self.completed_tasks,
                "total_tasks": self.total_tasks,
            },
            "latency": {
                "tile_mean": sum(self.tile_latencies) / max(1, len(self.tile_latencies)),
                "tile_p95": tile_p["p95"],
                "tile_p99": tile_p["p99"],
                "task_mean": sum(self.task_latencies) / max(1, len(self.task_latencies)),
                "task_p95": task_p["p95"],
                "task_p99": task_p["p99"],
            },
            "resource": {
                "avg_queue_len": queue_avg,
                "compute_busy_time": self.compute_busy_time,
                "mem_peak_gb": self.mem_peak,
                "vram_peak_gb": self.vram_peak,
            },
            "network": {
                "link_utilization": link_util,
            },
            "failures": self.failure_reasons,
        }

    def to_json(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.summary(), f, indent=2)
