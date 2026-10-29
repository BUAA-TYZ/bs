"""Summarize Nsight intervals without double-counting overlapping GPU work."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re
import sqlite3
import statistics


def union_duration(intervals):
    total, end = 0, None
    for start, stop in sorted(intervals):
        if stop < start:
            raise ValueError("Negative interval")
        total += max(0, stop - max(start, end if end is not None else start))
        end = max(stop, end if end is not None else stop)
    return total


def telemetry_summary(path, report):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    rows = [row for row in rows if report["measurement_epoch_start_s"] <= row["received_epoch_s"]
            <= report["measurement_epoch_end_s"]]
    patterns = {"gpu_busy_percent": r"GR3D_FREQ (\d+)%", "emc_busy_percent": r"EMC_FREQ (\d+)%",
                "system_ram_reported_MB": r"RAM (\d+)/", "gpu_temperature_C": r"\bgpu@([\d.]+)C"}
    values = {key: [float(match.group(1)) for row in rows if (match := re.search(pattern, row["raw"]))]
              for key, pattern in patterns.items()}
    values["gpu_frequency_MHz"] = [row["gpu_frequency_hz"] / 1e6 for row in rows if row.get("gpu_frequency_hz")]
    return {key: {"samples": len(items), "mean": statistics.mean(items), "min": min(items), "max": max(items)}
            if items else {"samples": 0, "status": "unavailable"} for key, items in values.items()}


def summarize(directory, mode):
    report = json.loads((directory / (mode + "_service/report.json")).read_text())
    if report["status"] != "passed":
        raise ValueError("Expected completed report")
    if not report["diagnostic"]:
        return {"mode": mode, "service": report["summary"],
                "telemetry": telemetry_summary(directory / (mode + "_telemetry.jsonl"), report)}
    db = sqlite3.connect(directory / (mode + ".sqlite"))
    db.row_factory = sqlite3.Row
    strings = dict(db.execute("SELECT id,value FROM StringIds"))
    nvtx = [dict(row) for row in db.execute("SELECT * FROM NVTX_EVENTS WHERE end IS NOT NULL")]
    for row in nvtx:
        row["label"] = row["text"] or strings.get(row["textId"], "")
    captures = [row for row in nvtx if row["label"] == "measured_service"]
    if len(captures) != 1:
        raise ValueError("Expected one measured service range")
    low, high = captures[0]["start"], captures[0]["end"]
    expected_workers = len(report["rows"]) * report["protocol"]["stream_workers"]
    phase_counts = {name: sum(r["label"] == name and low <= r["start"] < r["end"] <= high for r in nvtx)
                    for name in ("preprocess", "upload_normalize", "forward", "download", "postprocess_encode")}
    if any(count != expected_workers for count in phase_counts.values()):
        raise ValueError("Missing measured NVTX phase ranges")
    kernels = [dict(row) for row in db.execute("SELECT * FROM CUPTI_ACTIVITY_KIND_KERNEL WHERE start>=? AND end<=?", (low, high))]
    copies = [dict(row) for row in db.execute("SELECT * FROM CUPTI_ACTIVITY_KIND_MEMCPY WHERE start>=? AND end<=?", (low, high))]
    runtimes = {row["correlationId"]: dict(row) for row in db.execute("SELECT * FROM CUPTI_ACTIVITY_KIND_RUNTIME")}
    intervals = [(row["start"], row["end"]) for row in kernels]
    copy_intervals = [(row["start"], row["end"]) for row in copies]
    busy = union_duration(intervals + copy_intervals)
    top, attention_intervals = defaultdict(lambda: [0, 0]), []
    attention = [row for row in nvtx if row["label"].startswith("attention/")]
    for row in kernels:
        name = strings[row["shortName"]]
        top[name][0] += row["end"] - row["start"]
        top[name][1] += 1
        launch = runtimes.get(row["correlationId"])
        if launch and any(r["globalTid"] == launch["globalTid"] and
                          r["start"] <= launch["start"] <= r["end"] for r in attention):
            attention_intervals.append((row["start"], row["end"]))
    phases = defaultdict(list)
    for cohort in report["rows"]:
        for worker in cohort["worker_phases"]:
            for name, timing in worker.items():
                for clock, value in timing.items():
                    phases[name + "/" + clock].append(value)
    total_kernel = sum(end - start for start, end in intervals)
    result = {"mode": mode, "profiled_windows": report["summary"]["windows"],
              "complete_phase_range_counts": phase_counts,
              "capture_wall_s": (high - low) / 1e9, "kernel_union_s": union_duration(intervals) / 1e9,
              "kernel_sum_s": total_kernel / 1e9, "copy_union_s": union_duration(copy_intervals) / 1e9,
              "gpu_activity_union_s": busy / 1e9, "gpu_activity_wall_fraction": busy / (high - low),
              "copy_only_wall_fraction": (busy - union_duration(intervals)) / (high - low),
              "overlapping_kernel_excess_s": (total_kernel - union_duration(intervals)) / 1e9,
              "stage_worker_mean_ms": {key: statistics.mean(items) * 1000 for key, items in phases.items()},
              "top_kernels": [{"name": key, "total_s": values[0] / 1e9, "count": values[1],
                               "fraction_of_summed_kernel_time": values[0] / total_kernel}
                              for key, values in sorted(top.items(), key=lambda pair: pair[1][0], reverse=True)[:12]],
              "attention_kernel_union_s": union_duration(attention_intervals) / 1e9 if attention else None,
              "attention_shapes": report.get("attention_shapes"),
              "telemetry": telemetry_summary(directory / (mode + "_telemetry.jsonl"), report),
              "diagnostics": [dict(row) for row in db.execute("SELECT * FROM DIAGNOSTIC_EVENT")]}
    db.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    manifest = json.loads((args.directory / "manifest.json").read_text())
    report = {"scope": "GPU activity is not SM/DRAM saturation. CPU download wait includes prior asynchronous forward.",
              "modes": [summarize(args.directory, row["mode"]) for row in manifest["runs"] if not row["returncode"]]}
    (args.directory / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    for row in report["modes"]:
        print(json.dumps({key: row[key] for key in ("mode", "capture_wall_s", "kernel_union_s",
              "gpu_activity_wall_fraction", "copy_only_wall_fraction", "overlapping_kernel_excess_s",
              "attention_kernel_union_s", "attention_shapes", "stage_worker_mean_ms", "service") if key in row}, indent=2))


if __name__ == "__main__":
    main()
