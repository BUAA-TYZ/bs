"""从真实检测计时生成固定 overlap 的受控代理配置及参数来源清单。"""

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.config import config_from_dict, load_config
from sim.marl.scenarios import ScenarioDistribution
from sim.workload import ImageTiling


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calibration-dir", default="results/image_workload_calibration_20261014")
    parser.add_argument("--output-dir", default="examples/overlap_proxy")
    parser.add_argument("--dt", type=float, default=1.0)
    args = parser.parse_args()
    source, output = Path(args.calibration_dir), Path(args.output_dir)
    if output.exists():
        raise FileExistsError("Choose a fresh protocol output directory")
    summary = json.loads((source / "summary.json").read_text())
    measured = {(row["sample"], row["device"]): row for row in summary["per_image"]}
    names = ("P0019", "P0007")
    cpu_ref = measured[(names[0], "cpu")]["window_service_ms"]["mean"] / 1000
    mps_ref = measured[(names[0], "mps")]["window_service_ms"]["mean"] / 1000
    ground_rate = cpu_ref / mps_ref
    profiles, workload_reasons = [], []
    for name in names:
        cpu, mps = measured[(name, "cpu")], measured[(name, "mps")]
        window_s = cpu["window_service_ms"]["mean"] / 1000
        image = ImageTiling(cpu["width"], cpu["height"], 3, 8, 1024, 824, 2,
                           window_s, statistics.mean(cpu["unmerged_output_bytes"]) / cpu["window_count"] / 1e6)
        profiles.append({"id": name.lower(), "image_tiling": asdict(image)})
        estimated_mps = window_s / ground_rate
        actual_mps = mps["window_service_ms"]["mean"] / 1000
        workload_reasons.append({
            "id": name.lower(), "sample": name, "manifest": image.manifest(),
            "compute_unit": "one U is one second of measured local_cpu window service",
            "CPU_window_mean_seconds": window_s, "MPS_window_mean_seconds": actual_mps,
            "MPS_shared_rate_predicted_window_seconds": estimated_mps,
            "MPS_shared_rate_relative_error": estimated_mps / actual_mps - 1,
            "output_model": "sample mean serialized bytes per window, includes 28-byte completion header; "
                            "preserves per-image total, not the within-image spatial distribution",
        })
    dt = args.dt
    if dt <= 0 or any(abs(seconds / dt - round(seconds / dt)) > 1e-9 for seconds in (1, 10, 600, 7200, 21600)):
        raise ValueError("dt must divide all physical protocol periods, including one second")
    base = asdict(load_config("examples/config_relay.yaml"))
    for key in ("image_size_mb", "num_tiles", "compute_cost_per_tile", "result_size_mb"):
        base.pop(key)
    merge_delay = max(measured[(name, "mps")]["merge_service_ms"]["mean"] for name in names) / 1000
    base.update({
        "seed": 1701, "dt": dt, "sim_steps": round(21600 / dt),
        "task_arrival_end_step": round(7200 / dt), "task_arrival_rate": dt / 75,
        "decision_interval_steps": round(1 / dt), "topology_update_steps": round(10 / dt),
        "ground_visibility_update_steps": round(10 / dt), "image_tiling": profiles[0]["image_tiling"],
        "compute_rate": 1.0, "compute_service_mode": "work_conserving",
        "mem_capacity_gb": 8.0, "vram_capacity_gb": 1.0,
        "vram_base_gb": 0.8, "vram_alpha_per_mb": 0.0,
        "ground_relay_delay_s": 0.0, "aggregation_delay_s": merge_delay,
    })
    base["topology"].update({"bandwidth_period": round(600 / dt), "bandwidth_noise": 0,
                              "latency_ms": 0, "visibility_workers": 1})
    for station in base["ground_stations"]:
        station["compute_rate"] = ground_rate
        station["bandwidth_mbps"] = 20.0
        station["receive_channels"] = 1
    base = asdict(config_from_dict(base))
    orbit = {"planes": 4, "satellites_per_plane": 6, "phasing": 1,
             "altitude_km": 705, "inclination_deg": 98.2,
             "raan_offset_deg": 15, "phase_offset_deg": 20}
    roles = [{"id": name, "members": 3, "source_slots": [0], "phase_offsets_deg": offsets,
              "mean_compute_rate": 1.0, "imager_compute_rate": 1.0}
             for name, offsets in (("near", [0, -2, 2]), ("uniform", None))]
    # 先固定条件，再观察策略表现；低/高负载是四倍到达率，不预称为饱和。
    conditions = [
        ("near_light", "near", 1 / 300, 1, "p0019"),
        ("near_high", "near", 1 / 75, 1, "p0019"),
        ("near_high_fast_ground", "near", 1 / 75, 19.2, "p0019"),
        ("uniform_high", "uniform", 1 / 75, 1, "p0019"),
        ("uniform_high_fast_ground", "uniform", 1 / 75, 19.2, "p0019"),
        ("near_small_image", "near", 1 / 75, 1, "p0007"),
    ]
    panels = {}
    for split, offset, seed_start in (("validation", 259200, 181001), ("test", 604800, 191001)):
        cases = []
        for index, (name, role, arrival, downlink, workload) in enumerate(conditions):
            for replicate in range(2):
                cases.append({"id": f"{split}_{name}_{replicate + 1}",
                              "seed": seed_start + 2 * index + replicate,
                              "orbit_offset_seconds": offset, "arrival_rate_per_second": arrival,
                              "isl_bandwidth_scale": 1.0, "ground_bandwidth_scale": downlink,
                              "walker": orbit, "role_profile": role, "workload_profile": workload})
        panels[split] = cases
    distribution = {
        "version": 3,
        "training": {"orbit_offset_seconds": [0, 172800], "arrival_rate_per_second": [1 / 300, 1 / 75],
                     "isl_bandwidth_scale": [0.5, 1.5], "ground_bandwidth_scale": [1, 19.2]},
        "walker": {"layouts": [{"planes": n, "satellites_per_plane": 6, "phasing": 1} for n in (2, 4)],
                   "altitude_km": [650, 750], "inclination_deg": [97, 99],
                   "raan_offset_deg": [0, 360], "phase_offset_deg": [0, 360]},
        "roles": {"profiles": roles, "load_reference_sources": 8},
        "workload": {"profiles": profiles, "training_profiles": [p["id"] for p in profiles]}, **panels,
    }
    dump(output / "config.json", base)
    dump(output / "distribution.json", distribution)
    ScenarioDistribution(output / "distribution.json", config_from_dict(base))
    reasons = {
        "status": "frozen controlled-proxy development protocol; not flight hardware calibration",
        "calibration": str(source),
        "calibration_sha256": {name: hashlib.sha256((source / name).read_bytes()).hexdigest()
                               for name in ("summary.json", "protocol.json", "windows.csv", "images.json")},
        "workloads": workload_reasons, "ground_rate": ground_rate,
        "resource_assumptions": {
            "node_profiles": "all satellites use measured local_cpu; both ground slots use local_mps-derived rate; "
                             "same capabilities for all policies, not actual spacecraft/server hardware",
            "memory": "8 GB per satellite is a controlled cache budget; source image plus materialized-patch peak excluded",
            "vram": "0.8 GB constant per single-window service, 1 GB capacity; rounded CPU RSS-based budget proxy, "
                    "not a measured discrete-GPU VRAM peak; no inference concurrency",
            "merge": "max mean MPS merge among the two templates, shared fixed uncongested service, rounded to dt",
            "ground_relay": "zero inter-ground network delay: an explicitly favorable ground baseline assumption",
        },
        "communication_assumptions": {
            "ground": "20/384 Mbps effective per-channel rates, 1 channel at each of two city-reference sites; "
                      "controlled contention factors, not a measured terminal",
            "isl": "20-120 Mbps distance-dependent effective rate, common four-neighbor topology, full duplex; "
                   "zero random noise for interpretable numerical controls; sinusoid period 600 s",
            "unmodeled": "acquisition/control/propagation delay and source-image decode; stored latency_ms set to zero",
        },
        "scenario_design": "six fixed conditions x two environment seeds; sparse conditional comparisons, not full factorial",
        "roles": "near/uniform preserve imager orbit and identical per-node compute; near companions displaced +/-2 degrees",
        "arrival": "Poisson 1/300 or 1/75 tasks/s globally at eight sources, fourfold controlled load; not asserted saturated",
        "duration": "2 h arrival + 4 h drain, shared complete follow-up",
        "evaluation": "prespecified F(1800/3600/7200/14400), equal-weight age score; show F(3600) separately",
        "training": "12/24 satellites, two layouts and two measured image templates; fixed overlap and packing; "
                    "orbit/time separation, independent environmental and learner seeds",
        "limitations": ["two image templates, not general remote-sensing workload distribution",
                        "mean per-window service/output, not heterogeneous per-window trace replay",
                        "CPU/MPS on one laptop, not two target devices; three short repeats only",
                        "end-to-end timestep sensitivity and additional training seeds required"],
    }
    dump(output / "parameter_reasons.json", reasons)
    print(json.dumps({"output": str(output), "CPU_to_MPS_rate": ground_rate,
                      "aggregation_delay_s": merge_delay,
                      "profiles": [{"id": row["id"], "image_size_mb": row["manifest"]["image_size_mb"],
                                    "windows": row["manifest"]["num_windows"], "tiles": row["manifest"]["num_tiles"]}
                                   for row in workload_reasons]}, indent=2))


if __name__ == "__main__":
    main()
