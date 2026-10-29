"""预先固定四档成像占用压力面板；仅准备几何与配置，不运行调度策略。"""

import argparse
from collections import Counter
from copy import deepcopy
from dataclasses import asdict
from datetime import timedelta
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sim.acquisition import generate_acquisition_schedule
from sim.config import config_from_dict
from sim.orbit import parse_start_time_utc
from sim.reproducibility import runtime_metadata


EPOCH = "2026-10-15T00:00:00Z"
DT = 0.1
FOLLOWUP_SECONDS = 301
LEVELS = (("s3_gap30", 3, 30), ("s6_gap30", 6, 30),
          ("s6_gap20", 6, 20), ("s6_gap10", 6, 10))
BASELINES = ["ground_relay", "heft_adapted", "cgs_compute_adapted", "dts_tmax_hybrid"]


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def steps(seconds):
    value = seconds / DT
    if not math.isfinite(value) or value < 0 or not math.isclose(value, round(value), abs_tol=1e-8):
        raise ValueError(f"Time {seconds} cannot be represented at dt={DT}")
    return round(value)


def physical_events(cfg, origin):
    return [{**event, "release_step": origin + event["release_step"] * cfg["dt"]}
            for event in cfg["scheduled_task_arrivals"]]


def geometry_evidence(events, schedule):
    metadata = schedule["metadata"]
    by_window, by_source = Counter(), {}
    for event in events:
        geometry = event["geometry"]
        assert geometry["off_nadir_deg"] <= metadata["max_off_nadir_deg"]
        assert geometry["sat_elevation_deg"] > 0
        assert geometry["sun_elevation_deg"] >= metadata["min_sun_elevation_deg"] + metadata["sun_guard_deg"]
        window = schedule["windows"][event["window_id"]]
        assert window["start_s"] <= event["release_step"] < window["end_s"]
        by_window[event["window_id"]] += 1
        by_source.setdefault(event["source_sat_id"], []).append(event["release_step"])
    assert all(count <= metadata["strips_per_pass"] for count in by_window.values())
    gaps = [later - earlier for times in by_source.values() for earlier, later in zip(times, times[1:])]
    assert all(gap >= metadata["min_spacing_s"] for gap in gaps)
    return {
        "all_events_pass_geometry_and_daylight": True,
        "event_count_by_source": dict(Counter(str(e["source_sat_id"]) for e in events)),
        "event_count_by_target": dict(Counter(e["target_id"] for e in events)),
        "used_window_count": len(by_window),
        "maximum_strips_in_one_window": max(by_window.values(), default=0),
        "minimum_same_source_spacing_seconds": min(gaps, default=None),
        "maximum_off_nadir_deg": max((e["geometry"]["off_nadir_deg"] for e in events), default=None),
        "minimum_sun_elevation_deg": min((e["geometry"]["sun_elevation_deg"] for e in events), default=None),
        "minimum_sat_elevation_deg": min((e["geometry"]["sat_elevation_deg"] for e in events), default=None),
        "ground_visibility_used_for_selection": False,
    }


def prepare(source, target):
    original = json.loads((source / "run.json").read_text())
    infos = {info["id"]: info for info in original["case_info"]}
    expected_days = [(parse_start_time_utc(EPOCH) + timedelta(days=d)).date().isoformat() for d in range(3)]
    bases = {}
    for day, calendar_day in enumerate(expected_days, 1):
        key = f"day{day:02d}"
        if infos[key]["calendar_day"] != calendar_day:
            raise ValueError("Pressure preparation only accepts the three fixed development days")
        base = json.loads((source / f"{key}_config.json").read_text())
        if (base["num_sats"] != 24 or base["task_source_sat_ids"] != [0, 1]
                or base["deadline_steps"] != 0 or base["task_arrival_rate"] != 0):
            raise ValueError("Requires 2 imagers, 22 helpers, scheduled arrivals and no hard expiry")
        for name, period in (("decision_interval_steps", 1), ("topology_update_steps", 10),
                             ("ground_visibility_update_steps", 10)):
            if not math.isclose(base[name] * base["dt"], period):
                raise ValueError(f"Unexpected source physical period: {name}")
        bases[key] = base
    tle = bases["day01"]["topology"]["tle_lines"]
    if any(base["topology"]["tle_lines"] != tle for base in bases.values()):
        raise ValueError("All days must share the same fixed constellation")

    selection = {
        "epoch": EPOCH, "calendar_days": expected_days, "split": "development",
        "levels": [{"id": name, "max_strips_per_pass": count, "min_spacing_seconds": gap,
                    "role": "original_main_panel" if name == "s3_gap30" else "pressure_sensitivity"}
                   for name, count, gap in LEVELS],
        "selection": "All four levels and all three days fixed before pressure outcomes; no replacement, filtering, or best-level selection",
        "prior_information": "These dates and the original 3-strip/30-second level are previously inspected development data, not unseen confirmation",
        "baseline_methods": BASELINES,
        "method_rule": "Freeze candidate specifications in each evaluation manifest before running the complete panel; no best-method-per-case selection",
        "all_levels_required_for_reporting": True, "original_main_panel_must_be_retained": True,
        "primary_seconds": 300, "reported_horizons_seconds": [300], "followup_seconds": FOLLOWUP_SECONDS,
        "dt": DT, "decision_period_seconds": 1, "geometry_period_seconds": 10,
        "input_relay_max_hops": 2, "ground_input_relay": True, "hard_expiry": False,
        "pressure_interpretation": "Uncalibrated imaging-occupancy sensitivity; 10/20-second spacing is not measured flight payload performance",
        "geometry_rule": "Same SGP4/WGS84 imaging regions, off-nadir<=30 degrees, ground elevation>0, approximate solar elevation>=21 degrees; no ground-station-window selection",
        "empty_case_rule": "Retain an empty case and report its zero arrivals; never replace its date",
    }
    target.mkdir(parents=True, exist_ok=False)
    # 先落盘选择规则，随后仅计算轨道几何；本脚本从不读取策略结果。
    save(target / "selection.json", selection)
    runtime = runtime_metadata()
    run_infos, config_hashes, evidence, schedules, empty = [], {}, {}, {}, []
    window_hash = None
    allowed = {"dt", "sim_steps", "scheduled_task_arrivals", "task_arrival_end_step",
               "decision_interval_steps", "topology_update_steps", "ground_visibility_update_steps",
               "input_relay_max_hops", "ground_input_relay", "topology"}
    for level, count, spacing in LEVELS:
        schedule = generate_acquisition_schedule(
            tle, EPOCH, [0, 1], duration_s=3 * 86400, dt_s=1, scan_step_s=10,
            strips_per_pass=count, min_spacing_s=spacing, pass_cooldown_s=600,
            max_off_nadir_deg=30, min_sun_elevation_deg=20, sun_guard_deg=1)
        windows = [{k: v for k, v in window.items() if k != "scheduled_strips"}
                   for window in schedule["windows"]]
        if window_hash is None:
            window_hash = digest(windows)
        assert digest(windows) == window_hash
        schedules[level] = schedule
        for day, calendar_day in enumerate(expected_days):
            key, case_id = f"day{day + 1:02d}", f"day{day + 1:02d}_{level}"
            base = bases[key]
            events = [e for e in schedule["events"] if day * 86400 <= e["release_step"] < (day + 1) * 86400]
            origin = max(day * 86400, min((e["release_step"] for e in events), default=day * 86400) - 120)
            last = max((e["release_step"] - origin for e in events), default=0)
            base_last = max(e["release_step"] for e in base["scheduled_task_arrivals"])
            arrival_padding_s = (base["task_arrival_end_step"] - base_last) * base["dt"]
            assert arrival_padding_s > 0
            cfg = deepcopy(base)
            cfg.update(dt=DT, sim_steps=steps(last + FOLLOWUP_SECONDS),
                       scheduled_task_arrivals=[{**e, "release_step": steps(e["release_step"] - origin)} for e in events],
                       task_arrival_end_step=steps(last + arrival_padding_s) if events else 0,
                       decision_interval_steps=steps(1), topology_update_steps=steps(10),
                       ground_visibility_update_steps=steps(10),
                       input_relay_max_hops=2, ground_input_relay=True)
            cfg["topology"]["start_time_utc"] = (parse_start_time_utc(EPOCH) + timedelta(seconds=origin)).isoformat().replace("+00:00", "Z")
            if base["topology"].get("bandwidth_period", 0) != 0:
                raise ValueError("Pressure panel requires the original deterministic constant-rate links")
            cfg = asdict(config_from_dict(cfg))
            normalized = asdict(config_from_dict(base))
            assert all(cfg[k] == value for k, value in normalized.items() if k not in allowed)
            assert {k: v for k, v in cfg["topology"].items() if k != "start_time_utc"} == {
                k: v for k, v in base["topology"].items() if k != "start_time_utc"}
            assert cfg["ground_input_relay"] is True and cfg["input_relay_max_hops"] == 2
            assert physical_events(cfg, origin) == events
            assert cfg["sim_steps"] * DT - last >= 300
            if level == "s3_gap30":
                assert origin == infos[key]["epoch_offset_seconds"]
                assert physical_events(base, origin) == events, "Original 30-second main panel changed"
            if not events:
                empty.append(case_id)
            info = {"id": case_id, "base_day_id": key, "calendar_day": calendar_day,
                    "epoch_offset_seconds": origin, "level": level, "max_strips_per_pass": count,
                    "min_spacing_seconds": spacing, "split": "development",
                    "all_qualifying_events_retained": True, "num_acquisitions": len(events)}
            run_infos.append(info)
            evidence[case_id] = {**geometry_evidence(events, schedule),
                                 "arrival_sha256": digest(events), "last_release_seconds": last,
                                 "observation_end_seconds": cfg["sim_steps"] * DT}
            path = target / f"{case_id}_config.json"
            save(path, cfg)
            config_hashes[path.name] = sha(path)
    assert len(run_infos) == 12
    save(target / "acquisition.json", schedules)
    save(target / "geometry_evidence.json", {"same_physical_windows_sha256": window_hash, "cases": evidence})
    dependencies = [Path(__file__), ROOT / "sim/acquisition.py", ROOT / "sim/config.py", ROOT / "sim/orbit.py"]
    provenance = deepcopy(original["provenance"])
    provenance["pressure_protocol"] = selection
    manifest = {**runtime, "scope": "Preparation only; fixed development pressure sensitivity, not strategy results or flight calibration",
                "effective_config_source": str(source.resolve()), "base_manifest_sha256": sha(source / "run.json"),
                "base_config_sha256": {f"{key}_config.json": sha(source / f"{key}_config.json") for key in bases},
                "analysis_source_sha256": {str(path.relative_to(ROOT)): sha(path) for path in dependencies},
                "selection_sha256": sha(target / "selection.json"), "config_sha256": config_hashes,
                "acquisition_sha256": sha(target / "acquisition.json"),
                "geometry_evidence_sha256": sha(target / "geometry_evidence.json"),
                "delivery_horizons_seconds": [300], "case_info": run_infos, "empty_imaging_cases": empty,
                "provenance": provenance,
                "source_sha256_after_preparation": runtime_metadata()["source_sha256"]}
    save(target / "run.json", manifest)
    return {"cases": [{"id": info["id"], "arrivals": info["num_acquisitions"]} for info in run_infos],
            "selection_sha256": manifest["selection_sha256"], "manifest_sha256": sha(target / "run.json"),
            "core_changed_during_preparation": manifest["source_sha256"] != manifest["source_sha256_after_preparation"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-dir", type=Path, default=ROOT / "results/sla_hybrid_dt01_v2_prepared_20261015")
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.prepared_dir, args.out_dir), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
