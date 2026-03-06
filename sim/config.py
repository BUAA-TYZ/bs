from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
import json
import yaml


@dataclass
class SimConfig:
    seed: int
    num_sats: int
    sim_steps: int
    dt: float
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
    topology: Dict[str, Any]


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
        topology=cfg.get("topology", {}),
    )
