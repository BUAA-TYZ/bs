"""核验完整 Jetson 标定序列并生成 SAR 配置使用的测量摘要。"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import sys


RUN_NAMES = ("round1", "round2", "round3", "sustained")
PHASES = ("preprocess_ms", "host_to_device_ms", "forward_ms", "device_to_host_ms",
          "controlled_nms_ms", "preview_encode_ms")
CYCLE_WINDOWS = 64
MEMORY_BUDGET_BYTES = 8_000_000_000
ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value, label, *, allow_zero=False):
    require(type(value) in (int, float) and math.isfinite(value)
            and (value >= 0 if allow_zero else value > 0), f"Invalid finite value: {label}")
    return float(value)


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def read_rows(path):
    rows = []
    with Path(path).open(encoding="utf-8") as source:
        for index, line in enumerate(source, 1):
            require(bool(line.strip()), f"Blank JSONL line: {path}:{index}")
            rows.append(json.loads(line))
    return rows


def provenance(path):
    path = Path(path).resolve()
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    try:
        label = str(path.relative_to(ROOT))
    except ValueError:
        label = str(path)
    return {"path": label, "sha256": digest.hexdigest(), "bytes": path.stat().st_size}


def utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "Timestamps require an explicit UTC offset")
    return parsed


def stats(values):
    if not values:
        return {"count": 0}
    return {"count": len(values), "min": min(values), "max": max(values),
            "mean": statistics.mean(values), "first": values[0], "last": values[-1]}


def validate_run(name, report, rows):
    require(report.get("status") == "passed", f"{name} is incomplete or failed")
    require(bool(rows), f"{name} has no window samples")
    protocol, model, fixture = report["protocol"], report["model"], report["workload_fixture"]
    require(protocol["window_px"] == 4096 and protocol["batch"] == 1
            and protocol["precision"] == "fp32" and protocol["resize_for_inference"] is False,
            f"{name} does not use native 4096, batch-one FP32 inference")
    require(model["dtype"] == "torch.float32" and model["device"] == "cuda:0"
            and model["head_classes"] == 15 and model["fused"] is True,
            f"{name} actual model differs from its protocol")
    require(report["warmup_completed"] == protocol["warmup"] == 50, f"{name} warmup is incomplete")
    targets = fixture["targets_per_window"]
    require(fixture["windows"] == len(targets) == CYCLE_WINDOWS
            and fixture["targets_per_cycle"] == sum(targets), f"{name} fixture counts differ")
    require(all(type(value) is int and value >= 0 for value in targets), "Invalid fixture target count")
    expected_count = 64 if name != "sustained" else None
    if expected_count is not None:
        require(len(rows) == protocol["repeats"] == expected_count, f"{name} must contain one 64-window cycle")
    else:
        require(protocol["duration_sec"] == 900, "Sustained measurement must request 900 seconds")
    previous_elapsed = 0.0
    starts = [0, 2304, 4608, 6912, 9216, 11520, 13824, 15904]
    for index, row in enumerate(rows):
        require(type(row["iteration"]) is int and row["iteration"] == index, f"{name} iterations are not contiguous")
        window_id = index % CYCLE_WINDOWS
        require(row["window_id"] == window_id
                and row["window_origin_xy"] == [starts[window_id % 8], starts[window_id // 8]],
                f"{name} window order or geometry differs")
        service = number(row["service_ms"], "service_ms")
        phases = [number(row[key], key, allow_zero=True) for key in PHASES]
        require(math.isclose(sum(phases), service, rel_tol=1e-10, abs_tol=1e-6),
                f"{name} phase times do not sum to service time")
        elapsed = number(row["elapsed_wall_s"], "elapsed_wall_s")
        require(elapsed > previous_elapsed and elapsed - previous_elapsed + 1e-5 >= service / 1000,
                f"{name} wall-time intervals are inconsistent with service time")
        previous_elapsed = elapsed
        require(row["targets"] == targets[window_id]
                and row["output_bytes"] == 28 + targets[window_id] * (40 + 20 + 512 ** 2),
                f"{name} measured payload differs from fixed SRP1 geometry")
        require(row["decoded_output_shape"] == [1, 20, 344064]
                and row["decoded_output_bytes"] == 20 * 344064 * 4,
                f"{name} decoded output shape or dtype bytes differ")
        utc(row["finished_at_utc"])
    require(report["summary"]["count"] == len(rows), f"{name} report and JSONL counts differ")
    mean_ms = statistics.mean(row["service_ms"] for row in rows)
    require(math.isclose(mean_ms, report["summary"]["mean_ms"], rel_tol=1e-10, abs_tol=1e-6),
            f"{name} report mean does not match raw samples")
    duration = number(report["measured_wall_s"], "measured_wall_s")
    require(duration + 1e-6 >= rows[-1]["elapsed_wall_s"], f"{name} duration ends before its last sample")
    if name == "sustained":
        require(duration >= 900, "Sustained measurement ended before 900 seconds")
    require(report["workload_cycles"] == {"completed": len(rows) // CYCLE_WINDOWS,
                                          "partial_cycle_windows": len(rows) % CYCLE_WINDOWS},
            f"{name} cycle accounting differs")
    return {"name": name, "sample_count": len(rows), "raw_mean_service_ms": mean_ms,
            "active_service_rate_windows_per_s": 1000 / mean_ms,
            "observed_wall_rate_windows_per_s": len(rows) / duration,
            "measured_wall_s": duration,
            "phase_mean_ms": {key: statistics.mean(row[key] for row in rows) for key in PHASES},
            "memory": report["memory"]}


def select_sustained(report, rows):
    end_s = report["measured_wall_s"]
    complete_minutes = math.floor(end_s / 60)
    require(complete_minutes >= 5, "Fewer than five complete one-minute observation bins")
    minute_bins = []
    for minute in range(complete_minutes - 5, complete_minutes):
        selected = [row for row in rows if minute * 60 <= row["elapsed_wall_s"] < (minute + 1) * 60]
        minute_bins.append({"minute": minute, "start_s": minute * 60, "end_s": (minute + 1) * 60,
                            "completed_windows": len(selected), "wall_throughput_windows_per_s": len(selected) / 60,
                            "mean_service_ms": statistics.mean(row["service_ms"] for row in selected) if selected else None})
    mean_rate = statistics.mean(row["wall_throughput_windows_per_s"] for row in minute_bins)
    deviations = [abs(row["wall_throughput_windows_per_s"] / mean_rate - 1) if mean_rate else None
                  for row in minute_bins]
    for row, deviation in zip(minute_bins, deviations):
        row["relative_deviation_from_five_bin_mean"] = deviation
    accepted = mean_rate > 0 and max(deviations) <= 0.05
    interval_start = end_s - 300
    selected, cycles = [], []
    for start_index in range(0, len(rows) - CYCLE_WINDOWS + 1, CYCLE_WINDOWS):
        cycle = rows[start_index:start_index + CYCLE_WINDOWS]
        # 前一条已完成记录是本周期开始时间的保守下界，避免把跨边界周期混入。
        conservative_start = rows[start_index - 1]["elapsed_wall_s"] if start_index else 0.0
        if conservative_start < interval_start or cycle[-1]["elapsed_wall_s"] > end_s:
            continue
        selected.extend(cycle)
        cycles.append({"cycle_index": start_index // CYCLE_WINDOWS, "iteration_start": start_index,
                       "iteration_end_inclusive": start_index + CYCLE_WINDOWS - 1,
                       "conservative_start_s": conservative_start, "last_completion_s": cycle[-1]["elapsed_wall_s"]})
    require(bool(selected), "The last 300 seconds contain no complete 64-window cycle")
    raw_mean = statistics.mean(row["service_ms"] for row in selected)
    return {
        "throughput_acceptance": {
            "status": "passed" if accepted else "failed", "threshold_relative_deviation": 0.05,
            "mean_wall_throughput_windows_per_s": mean_rate,
            "maximum_relative_deviation": max(deviations) if mean_rate else None,
            "bins": minute_bins,
            "meaning": "Completed windows per 60 seconds over the last five complete bins; finite-run throughput stability, not thermal equilibrium",
        },
        "selection": {
            "interval_start_s": interval_start, "interval_end_s": end_s, "window_seconds": 300,
            "rule": "Use only complete iteration//64 cycles whose preceding sample completion is at/after the lower boundary and final completion is within the interval",
            "selected_cycles": cycles, "sample_count": len(selected), "cycle_count": len(cycles),
            "raw_t_window_s": raw_mean / 1000, "raw_mean_service_ms": raw_mean,
            "rounded_t_window_s": math.ceil(raw_mean) / 1000, "rounding": "Ceiling to the next whole millisecond",
            "phase_mean_ms": {key: statistics.mean(row[key] for row in selected) for key in PHASES},
            "mean_output_bytes_per_window": statistics.mean(row["output_bytes"] for row in selected),
            "mean_targets_per_window": statistics.mean(row["targets"] for row in selected),
        },
    }


def validate_aggregation(report, fixture_hash):
    require(report.get("status") == "passed", "Aggregation measurement is incomplete or failed")
    require(report["fixture_sha256"] == fixture_hash, "Aggregation and inference fixture hashes differ")
    validation = report["validation"]
    require(validation["packet_count"] == 64 and validation["canonical_global_objects_match"] is True
            and validation["arrival_order_invariant"] is True, "Aggregation correctness checks failed")
    samples = report["samples"]
    require(len(samples) == report["repeats"] == report["summary"]["completed_repeats"] and samples,
            "Aggregation sample accounting differs")
    values = [number(sample["service_ms"], "aggregation service_ms") for sample in samples]
    for sample in samples:
        require(sample["input_records"] == validation["input_records"]
                and sample["output_records"] == validation["output_records"]
                and sample["output_bytes"] == validation["output_bytes"]
                and sample["output_sha256"] == validation["output_sha256"],
                "Aggregation samples disagree with canonical validation")
    mean_ms = statistics.mean(values)
    require(math.isclose(mean_ms, report["summary"]["mean_ms"], rel_tol=1e-10, abs_tol=1e-6),
            "Aggregation report mean differs from raw samples")
    return {"mean_service_ms": mean_ms, "aggregation_delay_s": mean_ms / 1000,
            "sample_count": len(values), "runtime": report["runtime"], "scope": report["scope"],
            "measurement_boundary": report["measurement_boundary"], "excluded": report["excluded"],
            "validation": validation, "script_sha256": report["script_sha256"]}


def telemetry_stats(rows):
    if not rows:
        return {"sample_count": 0}
    result = {"sample_count": len(rows), "first_utc": rows[0]["utc"], "last_utc": rows[-1]["utc"],
              "system_ram_used_reported_MB": stats([row["ram_used"] for row in rows if row["ram_used"] is not None]),
              "system_ram_total_reported_MB": sorted({row["ram_total"] for row in rows if row["ram_total"] is not None}),
              "gpu_utilization_percent": stats([row["gpu_utilization"] for row in rows if row["gpu_utilization"] is not None]),
              "gpu_frequency_MHz": stats([value for row in rows for value in row["gpu_frequency"]]),
              "temperature_C": {}}
    sensors = sorted({key for row in rows for key in row["temperatures"]})
    for sensor in sensors:
        points = [(utc(row["utc"]).timestamp(), row["temperatures"][sensor])
                  for row in rows if sensor in row["temperatures"]]
        values = [value for _, value in points]
        sensor_stats = stats(values)
        if len(points) > 1:
            origin = points[0][0]
            times = [(time - origin) / 60 for time, _ in points]
            mean_time, mean_temp = statistics.mean(times), statistics.mean(values)
            denominator = sum((time - mean_time) ** 2 for time in times)
            sensor_stats["linear_trend_C_per_minute"] = (sum((time - mean_time) * (temp - mean_temp)
                                                           for time, temp in zip(times, values)) / denominator
                                                        if denominator else None)
        result["temperature_C"][sensor] = sensor_stats
    return result


def summarize_telemetry(path, reference_report, sustained_rows, selection):
    if not path.exists():
        return {"status": "unavailable", "reason": "No tegrastats.jsonl; no temperature, RAM or frequency evidence"}
    raw_rows, parsed = read_rows(path), []
    for row in raw_rows:
        utc(row["utc"])
        line = row["raw"]
        ram = re.search(r"\bRAM\s+(\d+)/(\d+)MB", line)
        gpu = re.search(r"\bGR3D_FREQ\s+(\d+(?:\.\d+)?)%(?:@(\[[^\]]+\]|[\d.]+))?", line)
        parsed.append({"utc": row["utc"], "ram_used": int(ram[1]) if ram else None,
                       "ram_total": int(ram[2]) if ram else None,
                       "gpu_utilization": float(gpu[1]) if gpu else None,
                       "gpu_frequency": [float(value) for value in re.findall(r"\d+(?:\.\d+)?", gpu[2])]
                                        if gpu and gpu[2] else [],
                       "temperatures": {match[0]: float(match[1]) for match in
                                        re.findall(r"\b([A-Za-z][A-Za-z0-9_]*)@(-?\d+(?:\.\d+)?)C", line)}})
    parsed.sort(key=lambda row: utc(row["utc"]))
    if not parsed:
        return {"status": "unavailable", "reason": "Empty tegrastats.jsonl"}
    sustained_start = utc(sustained_rows[0]["finished_at_utc"]) - timedelta(seconds=sustained_rows[0]["elapsed_wall_s"])
    last_start = sustained_start + timedelta(seconds=selection["interval_start_s"])
    last_end = sustained_start + timedelta(seconds=selection["interval_end_s"])
    startup = parsed[0]
    total = telemetry_stats(parsed)
    last = telemetry_stats([row for row in parsed if last_start <= utc(row["utc"]) <= last_end])
    has_frequency = total["gpu_frequency_MHz"]["count"] > 0
    return {
        "status": "observed", "whole_series": total, "sustained_last_300s": last,
        "startup_system_ram_reference": {
            "utc": startup["utc"], "used_reported_MB": startup["ram_used"], "total_reported_MB": startup["ram_total"],
            "meaning": "First controller telemetry sample during process startup; not a controlled idle or resident-model baseline",
        },
        "device_total_memory_bytes": reference_report["device"]["total_memory_bytes"],
        "frequency_observed": has_frequency,
        "thermal_validation": {
            "status": "not_certified",
            "frequency_stability": "Frequency samples are recorded but no separate clock-stability test is claimed" if has_frequency
                                   else "No GPU MHz samples: GR3D_FREQ utilization alone cannot establish frequency stability",
            "temperature_scope": "Record finite-run sensor levels and trends; stable throughput does not prove complete thermal equilibrium",
            "extrapolation": "No guarantee beyond the observed 15-minute sustained interval or under flight cooling and resource contention",
        },
        "energy_scope": "Power rails are preserved only in raw telemetry; no incremental energy or idle-subtracted power is inferred",
        "ram_scope": "System-wide RAM includes other processes and shared allocations; reported MB labels are retained without summing CUDA allocation and process RSS",
        "time_alignment": "UTC origin inferred from first sustained row completion minus its elapsed wall time; sub-millisecond alignment is not claimed",
    }


def summarize(series_dir, aggregation_path):
    series_dir, aggregation_path = Path(series_dir), Path(aggregation_path)
    controller_path = series_dir / "series.json"
    controller = read_json(controller_path)
    require(controller.get("status") == "passed", "Series controller is incomplete or failed")
    require([run["name"] for run in controller["runs"]] == list(RUN_NAMES)
            and all(run["returncode"] == 0 for run in controller["runs"]), "Series run list or return codes differ")
    reports, rows_by_run, run_summaries, inputs = {}, {}, [], {"controller": provenance(controller_path)}
    for name in RUN_NAMES:
        report_path, samples_path = series_dir / name / "report.json", series_dir / name / "windows.jsonl"
        report, rows = read_json(report_path), read_rows(samples_path)
        reports[name], rows_by_run[name] = report, rows
        run_summaries.append(validate_run(name, report, rows))
        inputs[name] = {"report": provenance(report_path), "samples": provenance(samples_path)}
    reference = reports["round1"]
    for name, report in reports.items():
        for key in ("script_sha256", "versions", "device", "backend_flags", "model", "raster"):
            require(report[key] == reference[key], f"{name} changed {key} within the series")
        require(report["weights"]["sha256"] == reference["weights"]["sha256"]
                and report["workload_fixture"]["sha256"] == reference["workload_fixture"]["sha256"]
                and report["workload_fixture"]["targets_per_window"] == reference["workload_fixture"]["targets_per_window"],
                f"{name} changed model or fixture content")
    sustained = select_sustained(reports["sustained"], rows_by_run["sustained"])
    aggregation = validate_aggregation(read_json(aggregation_path), reference["workload_fixture"]["sha256"])
    inputs["aggregation"] = provenance(aggregation_path)
    targets = reference["workload_fixture"]["targets_per_window"]
    expected_input_bytes = sum(28 + count * (40 + 20 + 512 ** 2) for count in targets)
    require(aggregation["validation"]["input_records"] == sum(targets)
            and aggregation["validation"]["input_bytes"] == expected_input_bytes,
            "Aggregation and GPU measurements use different output payloads")
    require(sustained["selection"]["mean_output_bytes_per_window"] == expected_input_bytes / 64,
            "Selected complete cycles do not preserve the fixture mean output size")
    memory = []
    for run in run_summaries:
        checks = {}
        for key in ("peak_cuda_allocated_bytes", "peak_cuda_reserved_bytes", "process_max_rss_bytes"):
            value = number(run["memory"][key], key)
            checks[key] = {"bytes": int(value), "below_8_decimal_GB": value < MEMORY_BUDGET_BYTES}
        memory.append({"run": run["name"], "metrics": checks})
    memory_pass = all(metric["below_8_decimal_GB"] for run in memory for metric in run["metrics"].values())
    telemetry_path = series_dir / "tegrastats.jsonl"
    telemetry = summarize_telemetry(telemetry_path, reference, rows_by_run["sustained"], sustained["selection"])
    if telemetry_path.exists():
        inputs["telemetry"] = provenance(telemetry_path)
    stderr_path = series_dir / "tegrastats.stderr.log"
    if stderr_path.exists():
        inputs["telemetry_stderr"] = provenance(stderr_path)
        telemetry["stderr_nonempty"] = bool(stderr_path.read_text(encoding="utf-8").strip())
    accepted = sustained["throughput_acceptance"]["status"] == "passed" and memory_pass
    summary = {
        "schema_version": 1, "status": "passed" if accepted else "review_required",
        "purpose": "Real Jetson service calibration of a fixed synthetic SAR computation proxy, for simulation parameter selection",
        "window_px": 4096, "compute_unit": "One native-scale 4096x4096 window is one U",
        "rounds": run_summaries[:3], "sustained_full_run": run_summaries[3], **sustained,
        "aggregation_delay_s": aggregation["aggregation_delay_s"], "aggregation": aggregation,
        "execution_memory_gb": 8.0,
        "memory_budget_validation": {
            "status": "passed" if memory_pass else "failed", "execution_reservation_gb": 8,
            "per_metric_checks": memory,
            "meaning": "Each observed CUDA peak and process RSS individually fits the proposed reservation; these overlapping/asynchronous metrics are not summed and do not prove joint physical peak usage",
            "shared_memory_budget": "Simulation reserves 8 decimal GB for execution and 8 GB for queues within a nominal 32 GB shared-memory proxy; remaining 16 GB is a system/reserve assumption",
        },
        "runtime": {"python": reference["python"], "versions": reference["versions"], "device": reference["device"],
                    "backend_flags": reference["backend_flags"], "model": reference["model"]},
        "measurement_boundary": reference["protocol"]["timing_boundary"],
        "excluded": reference["protocol"]["excluded"],
        "weights_sha256": reference["weights"]["sha256"],
        "fixture_sha256": reference["workload_fixture"]["sha256"],
        "measurement_script_sha256": reference["script_sha256"],
        "telemetry": telemetry,
        "thermal_validation": telemetry.get("thermal_validation", {"status": "not_certified", "reason": "No usable telemetry"}),
        "inputs": inputs, "summary_builder": provenance(__file__),
        "limitations": [
            "Synthetic uint16 pixels and canonical controlled detections; no SAR image recognition accuracy or real harbor density estimate",
            "Official optical YOLO11n-OBB is a computation proxy; optical predictions are discarded before controlled NMS",
            "FP32 refers to tensor and parameter dtype; reported backend flags may allow TF32 for cuDNN",
            "Active service excludes hashes, file/log IO and function-scope cleanup after the timer; it is not measured end-to-end wall throughput",
            "One resident model and serial execution; no guarantee under other onboard workloads, flight cooling, radiation or power limits",
            "Input normalization truncates uint16 amplitude to uint8; target previews retain full ROI extent but lose native detail at 512x512",
            "Image focusing and source-file decoding remain outside the calibrated boundary",
            "Aggregation is the measured CPU reference on its recorded machine, not an unmeasured production server",
            "No controlled idle or resident-model baseline was recorded; no incremental power or per-task energy is inferred",
            "Finite-run throughput acceptance does not certify GPU clock stability, complete thermal equilibrium or longer-duration behavior",
            "Individual CUDA/RSS budget checks and system RAM observations do not prove a shared-memory simultaneous peak bound",
        ],
    }
    if accepted:
        summary["t_window_s"] = sustained["selection"]["rounded_t_window_s"]
        summary["compute_rate_u_per_s"] = 1 / summary["t_window_s"]
    else:
        summary["freeze_blockers"] = [reason for condition, reason in (
            (sustained["throughput_acceptance"]["status"] != "passed", "Last-five-bin throughput deviation exceeds 5 percent"),
            (not memory_pass, "At least one observed memory metric reaches or exceeds the 8 GB reservation")) if condition]
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--series-dir", type=Path, required=True)
    parser.add_argument("--aggregation-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("Choose a new output file; previous summaries are never overwritten")
    summary = summarize(args.series_dir, args.aggregation_report)
    payload = json.dumps(summary, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as target:
        target.write(payload)
    print(json.dumps({"output": str(args.output), "status": summary["status"],
                      "t_window_s": summary.get("t_window_s"),
                      "throughput_acceptance": summary["throughput_acceptance"],
                      "selected_samples": summary["selection"]["sample_count"],
                      "aggregation_delay_s": summary["aggregation_delay_s"]}, ensure_ascii=False, indent=2))
    return 0 if summary["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
