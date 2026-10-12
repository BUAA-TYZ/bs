"""固定维度下的训练场景分布；按 episode 独立采样，显式隔离轨道时间段。"""

from dataclasses import asdict, replace
from datetime import datetime, timedelta
import hashlib
import json
import math
from pathlib import Path

import numpy as np
from sim.constellation import walker_tle, clustered_walker_tle


FIELDS = ("orbit_offset_seconds", "arrival_rate_per_second", "compute_cost_per_tile",
          "isl_bandwidth_scale", "ground_bandwidth_scale")
ORBIT_FIELDS = ("altitude_km", "inclination_deg", "raan_offset_deg", "phase_offset_deg")


def config_digest(cfg):
    return hashlib.sha256(json.dumps(asdict(cfg), sort_keys=True, allow_nan=False).encode()).hexdigest()


class ScenarioDistribution:
    def __init__(self, path, base):
        self.spec = json.loads(Path(path).read_text(encoding="utf-8"))
        self.base = base
        self.sha256 = hashlib.sha256(json.dumps(self.spec, sort_keys=True, allow_nan=False).encode()).hexdigest()
        version = self.spec.get("version")
        valid_schema = (version == 1 and set(self.spec) in (
            {"version", "training", "validation", "test"},
            {"version", "training", "validation", "test", "walker"})) or (
            version == 2 and set(self.spec) == {"version", "training", "validation", "test", "walker", "roles"})
        if not valid_schema:
            raise ValueError("Unknown scenario distribution schema")
        self.roles = self.spec.get("roles")
        if self.roles:
            if (set(self.roles) != {"profiles", "load_reference_sources"} or
                    type(self.roles["load_reference_sources"]) is not int or
                    self.roles["load_reference_sources"] < 1 or not self.roles["profiles"]):
                raise ValueError("Invalid role distribution")
            names = set()
            for profile in self.roles["profiles"]:
                self.validate_profile(profile)
                if profile["id"] in names:
                    raise ValueError("Role profile IDs must be unique")
                names.add(profile["id"])
        elif version == 2:
            raise ValueError("Version 2 requires role profiles")
        ranges = self.spec["training"]
        if set(ranges) != set(FIELDS):
            raise ValueError("Training must specify exactly the supported randomization fields")
        for field, bounds in ranges.items():
            if not isinstance(bounds, list) or len(bounds) != 2 or not all(
                    isinstance(v, (int, float)) and math.isfinite(v) for v in bounds):
                raise ValueError(f"Invalid bounds: {field}")
            if bounds[0] > bounds[1] or bounds[0] < 0 or (field != "orbit_offset_seconds" and bounds[0] <= 0):
                raise ValueError(f"Invalid positive range: {field}")
        duration = base.sim_steps * base.dt
        self.walker = self.spec.get("walker")
        if self.roles and not self.walker:
            raise ValueError("Role distributions require Walker layouts")
        if self.walker and not self.roles and (base.task_source_sat_ids is not None or
                                              base.satellite_compute_rates is not None):
            raise ValueError("Variable Walker layouts require role profiles instead of fixed role arrays")
        self.max_training_sats = base.num_sats
        if self.walker:
            if set(self.walker) != {"layouts", *ORBIT_FIELDS} or not self.walker["layouts"]:
                raise ValueError("Invalid Walker distribution")
            for layout in self.walker["layouts"]:
                self.validate_layout(layout)
                if self.roles:
                    for profile in self.roles["profiles"]:
                        self.validate_profile_layout(profile, layout)
            self.max_training_sats = max(x["planes"] * x["satellites_per_plane"] for x in self.walker["layouts"])
            for field in ORBIT_FIELDS:
                bounds = self.walker[field]
                if not isinstance(bounds, list) or len(bounds) != 2 or not all(
                        isinstance(v, (int, float)) and math.isfinite(v) for v in bounds) or bounds[0] > bounds[1]:
                    raise ValueError(f"Invalid Walker range: {field}")
            if not 300 <= self.walker["altitude_km"][0] <= self.walker["altitude_km"][1] <= 1500:
                raise ValueError("Walker altitude outside supported LEO range")
            if not 0 < self.walker["inclination_deg"][0] <= self.walker["inclination_deg"][1] < 180:
                raise ValueError("Invalid inclination range")
        train_interval = (ranges["orbit_offset_seconds"][0], ranges["orbit_offset_seconds"][1] + duration)
        ids, seeds, intervals = set(), set(), {"validation": [], "test": []}
        for split in ("validation", "test"):
            cases = self.spec[split]
            if not cases:
                raise ValueError(f"Empty {split} scenarios")
            for case in cases:
                expected_fields = {"id", "seed", *FIELDS} | ({"walker"} if self.walker else set()) | (
                    {"role_profile"} if self.roles else set())
                if set(case) != expected_fields:
                    raise ValueError(f"Invalid {split} case fields")
                if not isinstance(case["id"], str) or not case["id"] or case["id"] in ids:
                    raise ValueError("Scenario IDs must be nonempty and globally unique")
                if type(case["seed"]) is not int or case["seed"] < 0 or case["seed"] in seeds:
                    raise ValueError("Held-out seeds must be nonnegative integers and globally unique")
                ids.add(case["id"])
                seeds.add(case["seed"])
                for field in FIELDS:
                    value = case[field]
                    if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 or (
                            field != "orbit_offset_seconds" and value == 0):
                        raise ValueError(f"Invalid case value: {field}")
                interval = (case["orbit_offset_seconds"], case["orbit_offset_seconds"] + duration)
                if self.overlap(train_interval, interval):
                    raise ValueError(f"{split} orbital interval overlaps training")
                intervals[split].append(interval)
                self.materialize(case, case["seed"])
        if any(self.overlap(a, b) for a in intervals["validation"] for b in intervals["test"]):
            raise ValueError("Validation and test orbital intervals overlap")

    @staticmethod
    def overlap(a, b):
        return max(a[0], b[0]) < min(a[1], b[1])

    @staticmethod
    def validate_layout(layout):
        if set(layout) != {"planes", "satellites_per_plane", "phasing"}:
            raise ValueError("Invalid Walker layout fields")
        if any(type(v) is not int for v in layout.values()) or min(layout["planes"], layout["satellites_per_plane"]) < 1:
            raise ValueError("Invalid Walker layout counts")
        if not 0 <= layout["phasing"] < layout["planes"]:
            raise ValueError("Invalid Walker inter-plane phasing")

    @staticmethod
    def validate_profile(profile):
        if set(profile) != {"id", "members", "source_slots", "phase_offsets_deg",
                            "mean_compute_rate", "imager_compute_rate"}:
            raise ValueError("Invalid role profile fields")
        members, slots = profile["members"], profile["source_slots"]
        if (not isinstance(profile["id"], str) or not profile["id"] or type(members) is not int or
                members < 2 or not isinstance(slots, list) or not slots or len(slots) >= members or
                any(type(s) is not int or not 0 <= s < members for s in slots) or len(set(slots)) != len(slots)):
            raise ValueError("Role profile requires distinct imaging and computing members")
        rates = [profile["mean_compute_rate"], profile["imager_compute_rate"]]
        if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in rates):
            raise ValueError("Invalid role compute rates")
        if rates[1] >= rates[0]:
            raise ValueError("Imager compute rate must be below the mean compute budget")
        offsets = profile["phase_offsets_deg"]
        if offsets is not None and (not isinstance(offsets, list) or len(offsets) != members or
                any(type(v) not in (int, float) or not math.isfinite(v) for v in offsets) or
                len(set(offsets)) != members or offsets[0] != 0):
            raise ValueError("Invalid role phase offsets")

    @staticmethod
    def validate_profile_layout(profile, layout):
        size = layout["satellites_per_plane"]
        if size % profile["members"]:
            raise ValueError("Role group size must divide satellites per plane")
        offsets = profile["phase_offsets_deg"]
        if offsets is not None and max(offsets) - min(offsets) >= 360 / (size // profile["members"]):
            raise ValueError("Role phase offsets overlap neighboring groups")

    def materialize(self, values, seed):
        topology = dict(self.base.topology)
        start = datetime.fromisoformat(topology["start_time_utc"].replace("Z", "+00:00"))
        topology["start_time_utc"] = (start + timedelta(seconds=values["orbit_offset_seconds"])).isoformat()
        for key in ("bandwidth_mbps_min", "bandwidth_mbps_max", "bandwidth_noise"):
            if key in topology:
                topology[key] *= values["isl_bandwidth_scale"]
        stations = [{**gs, "bandwidth_mbps": gs["bandwidth_mbps"] * values["ground_bandwidth_scale"]}
                    for gs in self.base.ground_stations]
        count = self.base.num_sats
        role_values = {}
        if self.walker:
            orbit = values["walker"]
            if set(orbit) != {"planes", "satellites_per_plane", "phasing", *ORBIT_FIELDS}:
                raise ValueError("Invalid Walker case fields")
            self.validate_layout({key: orbit[key] for key in ("planes", "satellites_per_plane", "phasing")})
            topology.pop("tle_file", None)
            topology["tle_lines"] = walker_tle(self.base.topology["start_time_utc"], **orbit)
            count = orbit["planes"] * orbit["satellites_per_plane"]
            if self.roles:
                profile = next((p for p in self.roles["profiles"] if p["id"] == values["role_profile"]), None)
                if profile is None:
                    raise ValueError("Unknown role profile")
                self.validate_profile_layout(profile, orbit)
                members = profile["members"]
                sources = [sid for sid in range(count) if sid % members in profile["source_slots"]]
                worker_rate = (count * profile["mean_compute_rate"] -
                               len(sources) * profile["imager_compute_rate"]) / (count - len(sources))
                role_values = {"task_source_sat_ids": sources, "satellite_compute_rates": [
                    profile["imager_compute_rate"] if sid in sources else worker_rate for sid in range(count)]}
                if profile["phase_offsets_deg"] is not None:
                    clustered = {k: v for k, v in orbit.items() if k != "satellites_per_plane"}
                    topology["tle_lines"] = clustered_walker_tle(
                        self.base.topology["start_time_utc"],
                        groups_per_plane=orbit["satellites_per_plane"] // members,
                        phase_offsets_deg=profile["phase_offsets_deg"], **clustered)
        load_scale = len(role_values["task_source_sat_ids"]) / self.roles["load_reference_sources"] if self.roles else 1
        return replace(self.base, seed=seed, num_sats=count, tile_lifecycle_log="", topology=topology,
                       ground_stations=stations, compute_cost_per_tile=values["compute_cost_per_tile"],
                       task_arrival_rate=values["arrival_rate_per_second"] * self.base.dt * load_scale, **role_values)

    def training_case(self, run_seed, episode):
        if run_seed < 0 or episode < 1:
            raise ValueError("Training seed must be nonnegative and episode positive")
        # 独立 RNG 避免环境抽样被网络初始化、评估或 minibatch 顺序改变。
        rng = np.random.default_rng(np.random.SeedSequence([run_seed, episode, 20261008]))
        values = {field: float(rng.uniform(*self.spec["training"][field])) for field in FIELDS}
        if self.walker:
            # 每轮重排布局再循环覆盖，避免短开发训练恰好遗漏某个星座规模。
            layouts = self.walker["layouts"]
            profiles = self.roles["profiles"] if self.roles else [None]
            combinations = [(layout, profile) for layout in layouts for profile in profiles]
            block, index = divmod(episode - 1, len(combinations))
            order = np.random.default_rng(np.random.SeedSequence([run_seed, block, 312])).permutation(len(combinations))
            layout, profile = combinations[int(order[index])]
            values["walker"] = {**layout,
                                **{field: float(rng.uniform(*self.walker[field])) for field in ORBIT_FIELDS}}
            if profile:
                values["role_profile"] = profile["id"]
        seed = run_seed + episode
        if seed in {case["seed"] for split in ("validation", "test") for case in self.spec[split]}:
            raise ValueError("Training episode seed overlaps a held-out seed")
        cfg = self.materialize(values, seed)
        return cfg, {"id": f"train_{run_seed}_{episode}", "seed": seed, **values,
                     "num_sats": cfg.num_sats, "num_task_sources": len(cfg.task_source_sat_ids or range(cfg.num_sats)),
                     "effective_arrival_rate_per_second": cfg.task_arrival_rate / cfg.dt,
                     "config_sha256": config_digest(cfg)}

    def cases(self, split):
        if split not in ("validation", "test"):
            raise ValueError("Expected validation or test split")
        return [(self.materialize(case, case["seed"]), {**case}) for case in self.spec[split]]
