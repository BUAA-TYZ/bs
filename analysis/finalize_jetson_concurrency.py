"""Audit Jetson cohort measurements and derive a current SAR configuration."""

import argparse
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import yaml

from sim.config import config_from_dict


def read(path):
    return json.loads(path.read_text())


def source(path):
    return {"path": str(path.relative_to(ROOT)), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def timed_swap(report):
    start, end = report["memory_before_measurement"], report["memory_samples"][-1]
    return {key: end[key] - start[key] for key in ("pswpin", "pswpout")}


def audit(series):
    manifest = read(series / "series.json")
    if manifest["status"] not in {"passed", "completed_with_failed_modes"}:
        raise ValueError("Measurement series is not complete")
    modes, reference = {}, None
    hashes = None
    for run in manifest["runs"]:
        if run["returncode"]:
            continue
        report = read(series / run["name"] / "report.json")
        if report["status"] != "passed" or report["summary"]["windows"] != 64:
            raise ValueError("Completed runs must contain all 64 windows")
        identity = {key: report[key] for key in
                    ("weights_sha256", "fixture_sha256", "raster_sha256", "backend_flags", "versions")}
        if reference is None:
            reference, hashes = identity, report["output_hashes"]
        if identity != reference or report["output_hashes"] != hashes:
            raise ValueError("Workload identity, precision, software or output changed between modes")
        rows = report["rows"]
        elapsed = sum(row["cohort_s"] for row in rows)
        if sum(row["windows"] for row in rows) != 64 or not math.isclose(
                64 / elapsed, report["summary"]["windows_per_s"], rel_tol=1e-12):
            raise ValueError("Throughput does not match raw cohort timings")
        report["round"] = run["round"]
        report["measured_phase_swap"] = timed_swap(report)
        modes.setdefault(run["mode"], []).append(report)
    if len(modes.get("batch1", [])) != 3:
        raise ValueError("Three serial control runs are required")
    baseline = {r["round"]: r["summary"]["full_64_window_service_s"] for r in modes["batch1"]}
    rows = []
    eligible = []
    for mode, reports in modes.items():
        speedups = [baseline[r["round"]] / r["summary"]["full_64_window_service_s"] for r in reports]
        swap = [r["measured_phase_swap"] for r in reports]
        row = {"mode": mode, "rounds": len(reports), "cohort_size": reports[0]["cohort_size"],
               "image_service_s": [r["summary"]["full_64_window_service_s"] for r in reports],
               "mean_image_service_s": statistics.mean(r["summary"]["full_64_window_service_s"] for r in reports),
               "paired_speedups": speedups, "measured_phase_swap": swap,
               "min_available_gb": min(r["memory"]["min_available_bytes"] for r in reports) / 1e9,
               "peak_reserved_gb": max(r["cuda_memory"]["peak_reserved_bytes"] for r in reports) / 1e9}
        row["eligible"] = len(reports) == 3 and all(not any(v.values()) for v in swap) and (
            mode == "batch1" or min(speedups) >= 1.05)
        if row["eligible"]:
            eligible.append(row)
        rows.append(row)
    if not eligible:
        raise ValueError("No measurement mode satisfies the preregistered criteria")
    selected = min(eligible, key=lambda row: row["mean_image_service_s"])["mode"]
    return {"selected_mode": selected, "modes": rows, "identity": reference,
            "criterion": "Three paired rounds >=5% speedup, no swap in measured service phase, identical outputs",
            "memory_scope": "Startup swap reported separately; MemAvailable is system-wide, not exclusive process memory"}, modes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurement", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path)
    args = parser.parse_args()
    measurement = args.measurement.resolve()
    summary, modes = audit(measurement / "series")
    if args.out_dir is None:
        print(json.dumps(summary, indent=2))
        return
    selected = summary["selected_mode"]
    sustained = read(measurement / "sustained" / "report.json")
    if (sustained["status"] != "passed" or sustained["mode"] != selected
            or sustained["summary"]["measured_wall_s"] < 300 or any(timed_swap(sustained).values())):
        raise ValueError("Selected mode needs a successful 300-second sustained check without timed swap")
    for key in summary["identity"]:
        if sustained[key] != summary["identity"][key]:
            raise ValueError("Sustained workload differs from series")
    if sustained["output_hashes"] != modes[selected][0]["output_hashes"]:
        raise ValueError("Sustained result payloads differ")
    size = sustained["cohort_size"]
    services, workspace = {}, {}
    selected_reports = []
    needed = [(1, "batch1"), (size, selected)] if size > 1 else [(1, "batch1")]
    if size == 4:
        needed.insert(1, (2, "batch2"))
    for count, mode in needed:
        reports = list(modes[mode]) + ([sustained] if mode == selected else [])
        selected_reports.extend(reports)
        seconds = max(r["summary"]["cohort_mean_s"] for r in reports)
        services[str(count)] = math.ceil(seconds * 1000) / 1000
        # 保守预算上界允许统计重叠；这是配额，不冒充物理峰值的测量。
        envelope = max(r["cuda_memory"]["peak_reserved_bytes"] + r["memory"]["process_max_rss_bytes"]
                       for r in reports) / 1e9
        workspace[str(count)] = math.ceil(envelope * 1.1)
    total = min(r["memory_before_import"]["MemTotal"] for r in selected_reports) / 1e9
    system = math.ceil(max(r["memory_before_import"]["MemTotal"] - r["memory_before_import"]["MemAvailable"]
                           for r in selected_reports) / 1e9) + 1
    execution = max(workspace.values())
    cache = math.floor(total - system - execution)
    if cache < 64 * 4096 ** 2 * 2 / 1e9:
        raise ValueError("Calibrated partitions cannot hold one complete overlapped image")
    base_path = ROOT / "results/sar_baseline_b10_dt0p01_20261016/derived_config.json"
    config = deepcopy(read(base_path))
    if config.get("satellite_compute_rates") and any(
            not math.isclose(rate, config["compute_rate"]) for rate in config["satellite_compute_rates"]):
        raise ValueError("Cannot replace heterogeneous hardware with a uniform measured profile")
    config.update(compute_rate=1 / services["1"], vram_base_gb=workspace["1"], vram_alpha_per_mb=0,
                  satellite_compute_rates=None,
                  vram_capacity_gb=execution, mem_capacity_gb=cache,
                  shared_memory_gb=total, system_memory_reserve_gb=system,
                  onboard_compute_profile={"service_s": services, "workspace_gb": workspace})
    throughput = size / services[str(size)]
    for station in config["ground_stations"]:
        station["compute_rate"] = 10 * throughput
    config = asdict(config_from_dict(config))
    summary.update(service_s=services, workspace_gb=workspace, max_cohort_size=size,
                   saturated_windows_per_s=throughput, ground_beta=10,
                   full_image_saturated_service_s=64 / throughput,
                   single_window_service_s=services["1"],
                   memory_partitions_gb={"physical_os_visible": total, "system_reserve": system,
                                         "execution": execution, "data_cache": cache,
                                         "unassigned_margin": total - system - execution - cache},
                   memory_budget_rule="ceil(1.1*(CUDA reserved peak + process RSS peak)); conservative overlapping upper budget, not physical peak",
                   system_budget_rule="ceil(max pre-import system unavailable GB) + 1 GB margin",
                   sustained=sustained["summary"],
                   planner_scope="Single-window service plus cohort-aware existing commitment; new hypothetical assignments conservatively serialized",
                   historical_results="Previous serial day results are not scores for this configuration")
    out = args.out_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    payload = (json.dumps(config, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()
    (out / "main_config.json").write_bytes(payload)
    (measurement / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    manifest = {"schema_version": 1, "config_sha256": {"main_config.json": hashlib.sha256(payload).hexdigest()},
                "inputs": {"previous_config": source(base_path), "calibration": source(measurement / "summary.json"),
                           "series": source(measurement / "series/series.json"),
                           "sustained": source(measurement / "sustained/report.json")},
                "parameter_basis": summary}
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    example = ROOT / "examples/config_sar_current.yaml"
    example.write_text("# 当前SAR开发场景：Jetson并发实测，24源星+22计算中继星。\n"
                       "# 4096窗口，180图，P300；历史串行结果不适用此配置。\n"
                       + yaml.safe_dump(config, allow_unicode=True, sort_keys=False))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
