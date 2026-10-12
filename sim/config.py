from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
import json
import math
import yaml


@dataclass
class SimConfig:
    seed: int
    num_sats: int
    sim_steps: int
    dt: float
    decision_interval_steps: int
    topology_update_steps: int
    ground_visibility_update_steps: int
    task_arrival_rate: float
    image_size_mb: float
    num_tiles: int
    compute_cost_per_tile: float
    vram_base_gb: float
    vram_alpha_per_mb: float
    result_size_mb: float
    deadline_steps: int
    mem_capacity_gb: float
    vram_capacity_gb: float
    compute_rate: float
    transfer_fail_on_link_down: bool
    vram_policy: str
    pipeline_mode: str
    tile_lifecycle_log: str
    ground_stations: list[Dict[str, Any]]
    topology: Dict[str, Any]
    ground_relay_delay_s: float = 0.0
    aggregation_delay_s: float = 0.0
    task_arrival_end_step: int | None = None
    execution_commitment: bool = False
    task_source_sat_ids: list[int] | None = None
    satellite_compute_rates: list[float] | None = None
    result_buffering: bool = False
    result_relay: bool = False
    relay_max_hops: int = 8
    relay_contact_guard: bool = False

    def __post_init__(self) -> None:
        if self.result_relay and not self.result_buffering:
            raise ValueError("result_relay requires result_buffering")
        if self.relay_contact_guard and not self.topology.get("predict_contacts", False):
            raise ValueError("relay_contact_guard requires predict_contacts")
        if type(self.relay_max_hops) is not int or self.relay_max_hops < 1:
            raise ValueError("relay_max_hops must be a positive integer")
        for name in ("num_sats", "sim_steps", "dt", "num_tiles", "image_size_mb",
                     "compute_cost_per_tile", "compute_rate", "mem_capacity_gb",
                     "vram_capacity_gb"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.task_arrival_rate < 0 or self.result_size_mb < 0:
            raise ValueError("task_arrival_rate and result_size_mb must be nonnegative")
        if self.vram_policy not in {"wait", "reject"}:
            raise ValueError("vram_policy must be wait or reject")
        for name in ("ground_relay_delay_s", "aggregation_delay_s"):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
        if self.task_arrival_end_step is not None and not 0 <= self.task_arrival_end_step <= self.sim_steps:
            raise ValueError("task_arrival_end_step must be within the simulation horizon")
        if self.task_source_sat_ids is not None:
            ids = self.task_source_sat_ids
            if (not isinstance(ids, list) or not ids or
                    any(type(sid) is not int or not 0 <= sid < self.num_sats for sid in ids) or
                    len(set(ids)) != len(ids)):
                raise ValueError("task_source_sat_ids must be nonempty, unique satellite IDs")
        if self.satellite_compute_rates is not None:
            rates = self.satellite_compute_rates
            if (not isinstance(rates, list) or len(rates) != self.num_sats or
                    any(type(rate) not in (int, float) or not math.isfinite(rate) or rate <= 0 for rate in rates)):
                raise ValueError("satellite_compute_rates must contain one finite positive rate per satellite")


def load_config(path: str) -> SimConfig:
    if path.endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

    def get(key: str, default: Any) -> Any:
        return cfg.get(key, default)

    return SimConfig(
        seed=int(get("seed", 0)),
        num_sats=int(get("num_sats", 4)),
        sim_steps=int(get("sim_steps", 200)),
        dt=float(get("dt", 1.0)),
        decision_interval_steps=max(1, int(get("decision_interval_steps", 1))),
        topology_update_steps=max(1, int(get("topology_update_steps", 1))),
        ground_visibility_update_steps=max(1, int(get("ground_visibility_update_steps", 1))),
        task_arrival_rate=float(get("task_arrival_rate", 0.3)),
        image_size_mb=float(get("image_size_mb", 512.0)),
        num_tiles=int(get("num_tiles", 16)),
        compute_cost_per_tile=float(get("compute_cost_per_tile", 4.0)),
        vram_base_gb=float(get("vram_base_gb", 0.8)),
        vram_alpha_per_mb=float(get("vram_alpha_per_mb", 0.002)),
        result_size_mb=float(get("result_size_mb", 1.0)),
        deadline_steps=int(get("deadline_steps", 0)),
        mem_capacity_gb=float(get("mem_capacity_gb", 32.0)),
        vram_capacity_gb=float(get("vram_capacity_gb", 8.0)),
        compute_rate=float(get("compute_rate", 1.0)),
        transfer_fail_on_link_down=bool(get("transfer_fail_on_link_down", False)),
        vram_policy=str(get("vram_policy", "wait")),
        pipeline_mode=str(get("pipeline_mode", "distributed")),
        tile_lifecycle_log=str(get("tile_lifecycle_log", "")),
        ground_stations=list(get("ground_stations", [])),
        topology=cfg.get("topology", {}),
        ground_relay_delay_s=float(get("ground_relay_delay_s", 0.0)),
        aggregation_delay_s=float(get("aggregation_delay_s", 0.0)),
        task_arrival_end_step=(int(cfg["task_arrival_end_step"])
                               if cfg.get("task_arrival_end_step") is not None else None),
        execution_commitment=bool(get("execution_commitment", False)),
        task_source_sat_ids=get("task_source_sat_ids", None),
        satellite_compute_rates=get("satellite_compute_rates", None),
        result_buffering=bool(get("result_buffering", False)),
        result_relay=bool(get("result_relay", False)),
        relay_max_hops=get("relay_max_hops", 8),
        relay_contact_guard=bool(get("relay_contact_guard", False)),
    )
