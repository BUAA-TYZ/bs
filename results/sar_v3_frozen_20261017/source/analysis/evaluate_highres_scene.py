"""公开链路规格、高分原生像素账本及区域成像事件的配对仿真。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import timedelta
import hashlib
import json
import math
import multiprocessing
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.evaluate_literature import common_context, evaluate, summarize
from sim.acquisition import generate_acquisition_schedule
from sim.config import config_from_dict, load_config
from sim.constellation import walker_tle
from sim.orbit import parse_start_time_utc
from sim.reproducibility import runtime_metadata
from sim.workload import ImageTiling


DEFAULT_METHODS = ("ground_compute", "ground_tiles", "ground_relay", "local",
                   "greedy", "window_aware", "heft_adapted", "cgs_compute_adapted")
EPOCH = "2026-10-15T00:00:00Z"
HORIZONS = [300, 600, 1800, 3600, 7200, 14400]


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def runner_hashes():
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in
            (Path(__file__), Path(__file__).with_name("evaluate_literature.py"))}


def build_cases(args):
    tle = walker_tle(EPOCH, 2, 12, 620, 97.9, raan_offset_deg=357.5,
                     phasing=1, plane_raan_offsets_deg=[0, 15])
    source_ids = [0, 12]
    if args.architecture == "heterogeneous":
        # 保留完全相同的两颗成像星，仅替换专用计算中继星的轨道。
        tle = [tle[0], tle[12]] + walker_tle(
            EPOCH, 2, 11, 550, 53, raan_offset_deg=0, phasing=1,
            plane_raan_offsets_deg=[0, 90], first_satellite_number=25)
        source_ids = [0, 1]
    schedule = generate_acquisition_schedule(
        tle, EPOCH, source_ids, duration_s=args.days * 86400,
        scan_step_s=10, strips_per_pass=args.strips_per_pass,
        min_spacing_s=30, pass_cooldown_s=600,
    )
    base = json.loads(Path("examples/overlap_proxy/config.json").read_text())
    for key in ("image_size_mb", "num_tiles", "compute_cost_per_tile", "result_size_mb"):
        base.pop(key)
    workload = dict(base["image_tiling"], width_px=46668, height_px=66668,
                    windows_per_tile_axis=4,
                    transport_components=[
                        {"name": "PAN", "downsample": 1, "bands": 1, "bits_per_sample": 12},
                        {"name": "MS", "downsample": 4, "bands": 6, "bits_per_sample": 12},
                    ], transport_compression_ratio=args.compression_ratio)
    image = ImageTiling.from_dict(workload)
    cases = []
    for day in range(args.days):
        events = [e for e in schedule["events"] if day * 86400 <= e["release_step"] < (day + 1) * 86400]
        if not events:
            continue
        # 只省略无任务的首尾空闲段；不筛选成像机会或策略结果。
        origin = max(day * 86400, min(e["release_step"] for e in events) - 120)
        end = max(e["release_step"] for e in events) + max(HORIZONS)
        arrivals = [dict(e, release_step=round((e["release_step"] - origin) / args.dt)) for e in events]
        start = parse_start_time_utc(EPOCH) + timedelta(seconds=origin)
        cfg = dict(base, dt=args.dt, sim_steps=math.ceil((end - origin) / args.dt),
                   task_arrival_rate=0, task_arrival_end_step=max(e["release_step"] for e in arrivals) + 1,
                   scheduled_task_arrivals=arrivals, task_source_sat_ids=source_ids,
                   decision_tile_limit_per_sat=args.decision_batch,
                   decision_interval_steps=max(1, round(1 / args.dt)),
                   topology_update_steps=max(1, round(10 / args.dt)),
                   ground_visibility_update_steps=max(1, round(10 / args.dt)),
                   mem_capacity_gb=64, vram_capacity_gb=1,
                   image_tiling=workload,
                   satellite_compute_rates=[args.source_compute_rate if i in source_ids else 1.0 for i in range(24)])
        cfg["ground_stations"] = [dict(base["ground_stations"][0],
            bandwidth_mbps=args.downlink_mbps * args.efficiency,
            compute_rate=base["ground_stations"][0]["compute_rate"] * args.ground_compute_multiplier)]
        cfg["topology"] = dict(base["topology"], tle_lines=tle,
            start_time_utc=start.isoformat().replace("+00:00", "Z"),
            bandwidth_mbps_min=args.isl_mbps * args.efficiency,
            bandwidth_mbps_max=args.isl_mbps * args.efficiency,
            bandwidth_distance_scale_km=0, bandwidth_noise=0, bandwidth_period=0)
        cfg["topology"].pop("tle_file", None)
        info = {"id": f"day{day + 1:02d}", "calendar_day": (parse_start_time_utc(EPOCH) + timedelta(days=day)).date().isoformat(),
                "epoch_offset_seconds": origin, "all_qualifying_events_retained": True,
                "num_acquisitions": len(events), "split": "development"}
        cases.append((config_from_dict(cfg), info))
    if not cases:
        raise ValueError("No daylight acquisitions; retain this geometry outcome rather than selecting a favorable date")
    provenance = {
        "scope": "specification-based network pilot with proxy compute; not flight measurements or final MARL evaluation",
        "epoch": EPOCH, "orbit": {"architecture": args.architecture, "source_ids": source_ids,
            "imagers": {"count": 2, "altitude_km": 620, "inclination_deg": 97.9,
                        "raan_deg": [357.5, 12.5], "phase_deg": [0, 15]},
            "helpers": {"count": 22, "description": ("same two imager planes; 12 satellites per plane including one imager"
                if args.architecture == "coplanar" else "two 550 km, 53 degree planes; 11 per plane; RAAN 0 and 90; Walker phase 1")},
            "synthetic_tle": True},
        "nominal_rates_mbps": {"isl": args.isl_mbps, "downlink": args.downlink_mbps},
        "payload_efficiency_assumption": args.efficiency,
        "parameter_evidence": "docs/thesis/highres_link_evidence.md",
        "compute_proxy": "RGB detector CPU/MPS calibration on one Apple M5 Pro; native PAN/MS preprocessing, codec, flight hardware uncalibrated",
        "result_proxy": "187 bytes per window transferred from old measured RGB workload; high-resolution detection density unvalidated",
        "aggregation_proxy": "fixed old measured merge time; full-strip merge scaling uncalibrated",
        "assumptions": ["64 GB per-node data buffer is a design allocation, not a SCOT80 memory specification",
                        "four simultaneous ISL neighbors, 4500 km design range, instantaneous acquisition, zero terminal setup delay",
                        f"one Beijing-coordinate receiver, one satellite at a time using aggregate {args.downlink_mbps} Mbps nominal",
                        "full topology prediction and current queue information; no control-plane delay",
                        "day episodes start empty before first acquisition; no cross-day residual carry-over",
                        "fixed one-second decision cadence for every dt; packet batch shared by all star heuristics",
                        "source compression and image conversion are excluded timing terms; equal representation for both paths"],
        "workload": {k: v for k, v in image.manifest().items() if k != "tiles"},
        "maximum_packet_mb": max(t.data_size_mb for t in image.tiles()),
        "serialization_seconds": {
            "whole_image_downlink": image.image_size_mb * 8 / (args.downlink_mbps * args.efficiency),
            "largest_packet_isl": max(t.data_size_mb for t in image.tiles()) * 8 / (args.isl_mbps * args.efficiency),
            "largest_packet_downlink": max(t.data_size_mb for t in image.tiles()) * 8 / (args.downlink_mbps * args.efficiency)},
    }
    return cases, schedule, provenance


def render_summary(summary, records):
    all_events = all(info.get("all_qualifying_events_retained", False) for info in summary["case_info"])
    lines = ["# 高分区域成像：公开链路规格配对实验", "", summary["scope"], "",
             ("所有自然日内合格成像事件均保留；每个任务至少跟踪4小时。" if all_events else
              "仅保留预先指定的首个成像窗口，用于数值诊断；每个任务至少跟踪4小时，不作为全日性能面板。"), "",
             "| 方法 | 完整交付/到达 | 5分钟 | 10分钟 | 30分钟 | 60分钟 | 120分钟 | 240分钟 | 截断平均时延(s) |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in summary["aggregate"]:
        selected = [r for r in records if r["method"] == row["method"]]
        rates = [sum(r["task_age"]["delivery_curve"][i]["delivery_rate"] for r in selected) / len(selected) for i in range(len(HORIZONS))]
        lines.append(f"| {row['method']} | {row['completed_tasks']}/{row['total_tasks']} | " +
                     " | ".join(f"{100 * rate:.2f}%" for rate in rates) +
                     f" | {row['restricted_mean_latency_seconds']:.2f} |")
    lines += ["", "交付率和时延为自然日等权；完成数为汇总。J为上述6个预设任务年龄时限的等权平均，不能与旧4时限J直接横比。",
              "算力、结果体积及聚合时延仍为代理；本轮不能据此宣称真实硬件加速或强化学习优胜。", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--architecture", choices=("coplanar", "heterogeneous"), default="heterogeneous")
    parser.add_argument("--strips-per-pass", type=int, default=3)
    parser.add_argument("--dt", type=float, default=1)
    parser.add_argument("--isl-mbps", type=float, default=10000)
    parser.add_argument("--downlink-mbps", type=float, default=1200)
    parser.add_argument("--efficiency", type=float, default=1)
    parser.add_argument("--compression-ratio", type=float, default=3)
    parser.add_argument("--ground-compute-multiplier", type=float, default=1)
    parser.add_argument("--source-compute-rate", type=float, default=1)
    parser.add_argument("--decision-batch", type=int, default=64)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--methods", nargs="+", default=list(DEFAULT_METHODS))
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--prepared-dir", help="Run already frozen case configs instead of regenerating geometry")
    args = parser.parse_args()
    if (min(args.days, args.dt, args.isl_mbps, args.downlink_mbps, args.workers,
            args.source_compute_rate, args.ground_compute_multiplier) <= 0 or
            not 0 < args.efficiency <= 1 or args.compression_ratio < 1 or
            not math.isclose(1 / args.dt, round(1 / args.dt))):
        raise ValueError("Invalid time, rate, compression, or efficiency")
    if len(set(args.methods)) != len(args.methods) or not set(args.methods) <= set(DEFAULT_METHODS + ("reservation_calendar",)):
        raise ValueError("Choose distinct supported heuristic methods")
    if args.prepared_dir:
        prepared = Path(args.prepared_dir)
        old = json.loads((prepared / "run.json").read_text())
        cases = [(load_config(str(prepared / f"{info['id']}_config.json")), info) for info in old["case_info"]]
        schedule = json.loads((prepared / "acquisition.json").read_text())
        provenance = old["provenance"]
    else:
        cases, schedule, provenance = build_cases(args)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    source = runtime_metadata()
    hashes = runner_hashes()
    manifest = {"requested_cli": vars(args), **source, "scope": provenance["scope"],
                "effective_config_source": str(Path(args.prepared_dir).resolve()) if args.prepared_dir else "build_cases(requested_cli)",
                "prepared_cli_note": "When prepared-dir is set, days/architecture/dt/rates/efficiency/compression/compute/batch are read from saved configs; current CLI values for those fields are ignored",
                "delivery_horizons_seconds": HORIZONS, "provenance": provenance,
                "analysis_source_sha256": hashes,
                "case_info": [info for _, info in cases]}
    write_json(out / "run.json", manifest)
    write_json(out / "acquisition.json", schedule)
    for cfg, info in cases:
        write_json(out / f"{info['id']}_config.json", asdict(cfg))
    if args.prepare_only:
        print(json.dumps({"cases": manifest["case_info"], "workload": provenance["workload"],
                          "serialization_seconds": provenance["serialization_seconds"]}, ensure_ascii=False))
        return
    records, started = [], time.monotonic()
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = [pool.submit(evaluate, cfg, info, method, "", HORIZONS)
                   for cfg, info in cases for method in args.methods]
        for future in as_completed(futures):
            record = future.result()
            if record["metrics"]["experiment"]["source_sha256"] != source["source_sha256"] or runtime_metadata()["source_sha256"] != source["source_sha256"]:
                raise RuntimeError("Source changed during experiment")
            if runner_hashes() != hashes:
                raise RuntimeError("Analysis source changed during experiment")
            for previous in records:
                if previous["case"]["id"] == record["case"]["id"] and common_context(previous) != common_context(record):
                    raise RuntimeError("Unpaired task trace, orbit, or configuration")
            records.append(record)
            write_json(out / f"{record['case']['id']}_{record['method']}.json", record)
            print(json.dumps({"finished": len(records), "total": len(futures), "case": record["case"]["id"],
                              "method": record["method"], "score": record["task_age"]["delivery_score"],
                              "elapsed_s": record["elapsed_s"]}), flush=True)
    aggregate, comparisons = summarize(records, args.methods)
    summary = {**manifest, "elapsed_s": time.monotonic() - started, "aggregate": aggregate,
               "comparisons": comparisons,
               "records": [f"{r['case']['id']}_{r['method']}.json" for r in records]}
    write_json(out / "summary.json", summary)
    (out / "summary.md").write_text(render_summary(summary, records), encoding="utf-8")
    print(json.dumps(aggregate, indent=2))


if __name__ == "__main__":
    main()
