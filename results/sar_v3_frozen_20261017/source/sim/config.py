from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
import json
import math
import yaml

from sim.workload import ImageTiling


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
    relay_routing: str = "snapshot"
    image_tiling: Dict[str, Any] | None = None
    compute_service_mode: str = "step"
    scheduled_task_arrivals: list[Dict[str, Any]] | None = None
    decision_tile_limit_per_sat: int = 4
    input_relay_max_hops: int = 1
    ground_input_relay: bool = False
    onboard_compute_profile: Dict[str, Any] | None = None
    shared_memory_gb: float | None = None
    system_memory_reserve_gb: float = 0.0

    def __post_init__(self) -> None:
        if self.relay_routing not in {"snapshot", "temporal"}:
            raise ValueError("relay_routing must be snapshot or temporal")
        if self.relay_routing == "temporal" and not self.topology.get("predict_contacts", False):
            raise ValueError("temporal relay routing requires predict_contacts")
        if type(self.ground_input_relay) is not bool:
            raise ValueError("ground_input_relay must be a boolean")
        if type(self.input_relay_max_hops) is not int or self.input_relay_max_hops < 1:
            raise ValueError("input_relay_max_hops must be a positive integer")
        if type(self.decision_tile_limit_per_sat) is not int or self.decision_tile_limit_per_sat < 1:
            raise ValueError("decision_tile_limit_per_sat must be a positive integer")
        if self.compute_service_mode not in {"step", "work_conserving"}:
            raise ValueError("compute_service_mode must be step or work_conserving")
        if self.image_tiling is not None:
            workload = ImageTiling.from_dict(self.image_tiling)
            tiles = workload.tiles()
            # 新模式以像素几何为唯一来源；旧字段只保留导出/观测所需的派生值。
            self.image_size_mb = workload.image_size_mb
            self.num_tiles = len(tiles)
            self.compute_cost_per_tile = math.fsum(tile.compute_cost for tile in tiles) / len(tiles)
            self.result_size_mb = math.fsum(tile.result_size_mb for tile in tiles) / len(tiles)
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
        if self.scheduled_task_arrivals is not None:
            if not isinstance(self.scheduled_task_arrivals, list):
                raise ValueError("scheduled_task_arrivals must be a list of event mappings")
            if self.task_arrival_rate != 0:
                raise ValueError("scheduled_task_arrivals requires task_arrival_rate=0")
            allowed_sources = set(self.task_source_sat_ids or range(self.num_sats))
            cutoff = self.sim_steps if self.task_arrival_end_step is None else self.task_arrival_end_step
            for event in self.scheduled_task_arrivals:
                if not isinstance(event, dict):
                    raise ValueError("scheduled_task_arrivals events must be mappings")
                step, source = event.get("release_step"), event.get("source_sat_id")
                if type(step) is not int or not 0 <= step < cutoff:
                    raise ValueError("scheduled_task_arrivals.release_step must be an integer before the arrival cutoff")
                if type(source) is not int or source not in allowed_sources:
                    raise ValueError("scheduled_task_arrivals.source_sat_id must name an allowed task source")
                if "window_result_bytes" in event:
                    counts = event["window_result_bytes"]
                    if (self.image_tiling is None or not isinstance(counts, list)
                            or len(counts) != sum(len(t.windows_xyxy) for t in workload.tiles())
                            or any(type(n) is not int or n <= 0 for n in counts)):
                        raise ValueError("window_result_bytes requires one positive byte count per inference window")
        if self.satellite_compute_rates is not None:
            rates = self.satellite_compute_rates
            if (not isinstance(rates, list) or len(rates) != self.num_sats or
                    any(type(rate) not in (int, float) or not math.isfinite(rate) or rate <= 0 for rate in rates)):
                raise ValueError("satellite_compute_rates must contain one finite positive rate per satellite")
        if self.shared_memory_gb is not None:
            values = (self.shared_memory_gb, self.system_memory_reserve_gb)
            if any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in values):
                raise ValueError("shared_memory_gb and system_memory_reserve_gb must be finite and nonnegative")
            if self.mem_capacity_gb + self.vram_capacity_gb + self.system_memory_reserve_gb > self.shared_memory_gb + 1e-9:
                raise ValueError("Shared memory partitions exceed shared_memory_gb")
        if self.onboard_compute_profile is not None:
            profile = self.onboard_compute_profile
            if (not isinstance(profile, dict) or self.compute_service_mode != "work_conserving"
                    or self.satellite_compute_rates is not None):
                raise ValueError("onboard_compute_profile requires uniform work_conserving execution")
            if self.image_tiling is not None:
                if ImageTiling.from_dict(self.image_tiling).compute_cost_per_window != 1:
                    raise ValueError("onboard_compute_profile requires one unit per inference window")
            elif self.compute_cost_per_tile != 1:
                raise ValueError("onboard_compute_profile without geometry requires unit-cost tiles")
            if set(profile) != {"service_s", "workspace_gb"} or "1" not in profile["service_s"]:
                raise ValueError("onboard_compute_profile requires service_s/workspace_gb including size 1")
            if set(profile["service_s"]) != set(profile["workspace_gb"]):
                raise ValueError("onboard_compute_profile service and workspace sizes must match")
            for key, seconds in profile["service_s"].items():
                if not isinstance(key, str) or key not in {"1", "2", "3", "4"}:
                    raise ValueError("onboard_compute_profile supports measured sizes 1 to 4")
                memory = profile["workspace_gb"][key]
                if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in (seconds, memory)):
                    raise ValueError("onboard_compute_profile service and workspace must be finite and positive")
                if memory > self.vram_capacity_gb:
                    raise ValueError("onboard_compute_profile workspace exceeds execution capacity")
            if not math.isclose(self.compute_rate, 1 / profile["service_s"]["1"], rel_tol=1e-9):
                raise ValueError("compute_rate must describe measured single-window service")


def load_config(path: str) -> SimConfig:
    if path.endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    return config_from_dict(cfg)


def config_from_dict(cfg: Dict[str, Any]) -> SimConfig:
    """与 load_config 相同的字段解析与默认值，供已在内存中的配置使用。"""

    def get(key: str, default: Any) -> Any:
        return cfg.get(key, default)

    if cfg.get("image_tiling") is not None:
        workload = ImageTiling.from_dict(cfg["image_tiling"])
        tiles = workload.tiles()
        derived = {
            "image_size_mb": workload.image_size_mb,
            "num_tiles": len(tiles),
            "compute_cost_per_tile": math.fsum(tile.compute_cost for tile in tiles) / len(tiles),
            "result_size_mb": math.fsum(tile.result_size_mb for tile in tiles) / len(tiles),
        }
        for key, value in derived.items():
            if key in cfg and (type(cfg[key]) not in (int, float) or
                               not math.isclose(cfg[key], value, rel_tol=1e-12, abs_tol=1e-12)):
                raise ValueError(f"{key} conflicts with derived image_tiling value {value}")

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
        relay_routing=get("relay_routing", "snapshot"),
        image_tiling=get("image_tiling", None),
        compute_service_mode=get("compute_service_mode", "step"),
        scheduled_task_arrivals=get("scheduled_task_arrivals", None),
        decision_tile_limit_per_sat=get("decision_tile_limit_per_sat", 4),
        input_relay_max_hops=get("input_relay_max_hops", 1),
        ground_input_relay=get("ground_input_relay", False),
        onboard_compute_profile=get("onboard_compute_profile", None),
        shared_memory_gb=get("shared_memory_gb", None),
        system_memory_reserve_gb=get("system_memory_reserve_gb", 0.0),
    )
