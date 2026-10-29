"""2026-10-21 批准场景：独立生成开发配置，不访问封存日期。"""

import argparse
from collections import Counter
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
import math
from pathlib import Path
import platform
import random
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.audit_sar_coastal_coverage import COASTAL_TARGETS
from analysis.benchmark_sar_aggregation import construct_packets, aggregate
from sim.acquisition import generate_acquisition_schedule
from sim.config import config_from_dict
from sim.constellation import walker_tle
from sim.workload import ImageTiling, window_starts


ANCHOR = "2026-10-15T00:00:00Z"
TIERS = {"S": (6, 2, 1, 12), "M": (12, 2, 1, 12),
         "L": (12, 4, 2, 12), "XL": (12, 8, 4, 12)}
MODES = {"spot": (10000, .5, 3200, 10), "fine": (20000, .25, 2304, 15),
         "slea": (30000, .5, 3200, 10)}
ROOT = Path("results/redesign_20261021")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def constellation(tier):
    sp, sq, cp, cq = TIERS[tier]
    source = walker_tle(ANCHOR, sp, sq, 570, 97.7, phasing=int(sp > 1),
                        plane_raan_offsets_deg=[180 * p / sp for p in range(sp)])
    compute = walker_tle(ANCHOR, cp, cq, 1015, 98.98, raan_offset_deg=15,
                         phasing=int(cp > 1), first_satellite_number=len(source) + 1,
                         plane_raan_offsets_deg=[180 * p / cp for p in range(cp)])
    n = len(source)
    rings = sorted({tuple(sorted((n + p * cq + k, n + p * cq + (k + 1) % cq)))
                    for p in range(cp) for k in range(cq)})
    return source + compute, n, rings


def tiling(mode, tiles=16):
    size, gsd, stride, _ = MODES[mode]
    # 预期结果量仅使用先验密度；真实目标和真实输出另存事件，不提供给策略。
    expected_records = .9 * 1.2 * (4096 * gsd / 1000) ** 2
    return ImageTiling(size, size, 3, 8, 4096, stride, 1, 1,
                       (28 + expected_records * 262204) / 1e6,
                       transport_components=({"name": "sar_amplitude", "downsample": 1,
                                              "bands": 1, "bits_per_sample": 16},),
                       deduplicate_within_tile=True, requested_num_tiles=tiles)


def fixture(mode, seed, tiles=16):
    work = tiling(mode, tiles)
    size, gsd, _, _ = MODES[mode]
    rng = random.Random(seed)
    count = 0 if rng.random() < .1 else round(1.2 * (size * gsd / 1000) ** 2)
    objects = []
    for index in range(count):
        # 用物理长度/宽度建框；对象完全落入幅面，输出按窗口求交，保持跨粒度真值一致。
        length, width = rng.uniform(30, 400) / gsd, rng.uniform(8, 61.5) / gsd
        angle = rng.uniform(0, 2 * math.pi)
        radius = math.hypot(length, width) / 2
        cx, cy = rng.uniform(radius, size - radius), rng.uniform(radius, size - radius)
        vertices = [(cx + x * math.cos(angle) - y * math.sin(angle),
                     cy + x * math.sin(angle) + y * math.cos(angle))
                    for x, y in ((-length/2, -width/2), (length/2, -width/2),
                                 (length/2, width/2), (-length/2, width/2))]
        objects.append({"object_id": index, "vertices": vertices,
                        "class_id": index % 3, "score": .9 - index * .0001})
    windows = []
    starts = window_starts(size, 4096, work.stride_px)
    for y in starts:
        for x in starts:
            records = []
            for obj in objects:
                xs, ys = zip(*obj["vertices"])
                if max(xs) <= x or min(xs) >= x + 4096 or max(ys) <= y or min(ys) >= y + 4096:
                    continue
                # 合成识别输出可含跨窗框；至少一个窗口包含完整对象，边缘框去重由 NMS 处理。
                roi = (max(0, math.floor(min(xs) - x)), max(0, math.floor(min(ys) - y)),
                       min(4096, math.ceil(max(xs) - x)), min(4096, math.ceil(max(ys) - y)))
                records.append({**obj, "vertices": [(px-x, py-y) for px, py in obj["vertices"]],
                                "roi_xyxy": roi})
            windows.append({"window_id": len(windows), "x0": x, "y0": y,
                            "records": records, "output_bytes": 28 + len(records) * 262204})
    assert all(any(all(x <= px <= x + 4096 and y <= py <= y + 4096
                       for px, py in obj["vertices"]) for y in starts for x in starts) for obj in objects)
    return {"image": {"width_px": size, "height_px": size, "mode": mode, "gsd_m": gsd},
            "tiling": asdict(work), "global_objects": objects, "windows": windows,
            "summary": {"total_output_bytes": sum(w["output_bytes"] for w in windows),
                        "object_count": count, "sum_record_count": sum(len(w["records"]) for w in windows)}}


def measure_merge(data, repeats=3):
    packets = construct_packets(data)
    size = (data["image"]["width_px"], data["image"]["height_px"])
    warm = aggregate(packets, size)
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        result = aggregate(packets, size)
        samples.append(time.perf_counter() - start)
        assert result == warm
    return {"median_seconds": statistics.median(samples), "samples_seconds": samples,
            "records_before": warm[1], "records_after": warm[2],
            "output_sha256": hashlib.sha256(warm[0]).hexdigest(),
            "scope": "Host CPU reference, parse/copy/NMS/encode; not an onboard measurement"}


def build(tier, date, tiles=16, duration_s=86400, output=ROOT):
    if date not in {f"2026-10-{day:02}" for day in range(15, 25)}:
        raise ValueError("Only approved development dates may be generated")
    if tier == "XL" and duration_s > 21600:
        raise ValueError("XL runtime estimate exceeds 3h: only 6h prefix is authorized")
    out = output / f"{tier}_{date}_k{tiles}_{duration_s}s"
    if out.exists():
        raise FileExistsError(out)
    tle, ns, rings = constellation(tier)
    schedule = generate_acquisition_schedule(
        tle, date + "T00:00:00Z", list(range(ns)), duration_s=duration_s,
        dt_s=.1, targets=[{k: row[k] for k in ("target_id", "lat_deg", "lon_deg", "radius_km")}
                         for row in COASTAL_TARGETS], scan_step_s=10, max_off_nadir_deg=40,
        min_sun_elevation_deg=-90, sun_guard_deg=0, min_spacing_s=60,
        pass_cooldown_s=0, incidence_range_deg=(20, 40))
    base = json.loads(Path("results/sar_dispatch_20261016/main_config.json").read_text())
    base.update(seed=1701, num_sats=len(tle), dt=.1, sim_steps=round((duration_s+301)/.1),
                task_arrival_end_step=round(duration_s/.1), decision_interval_steps=10,
                topology_update_steps=100, ground_visibility_update_steps=100,
                sar_source_isolation=True, serialized_aggregation=True,
                task_source_sat_ids=list(range(ns)), deadline_steps=0,
                relay_max_hops=len(tle)-1, relay_routing="temporal", task_arrival_rate=0,
                image_tiling=asdict(tiling("fine", tiles)))
    for key in ("image_size_mb", "num_tiles", "compute_cost_per_tile", "result_size_mb"):
        base.pop(key, None)
    base["topology"].update(tle_lines=tle, start_time_utc=date+"T00:00:00Z",
                            neighbor_limits=[1]*ns+[4]*(len(tle)-ns), preferred_edges=rings,
                            acquisition_delay_s=5, max_neighbors=4, contact_scan_s=30,
                            window_lookahead_s=300)
    for gs in base["ground_stations"]:
        gs.update(bandwidth_mbps=400, min_elevation_deg=10)
    rng = random.Random(1701 + int(date.replace("-", "")))
    events, details, last_start = [], [], {}
    out.mkdir(parents=True)
    for window in sorted(schedule["windows"], key=lambda w: (w["start_s"], w["source_sat_id"], w["target_id"])):
        mode = rng.choices(list(MODES), weights=(.2,.6,.2))[0]
        sid = window["source_sat_id"]
        start = max(window["start_s"], last_start.get(sid, -60) + 60)
        end = start + MODES[mode][3]
        ready = end + 60
        if end > window["end_s"] - 10 or ready >= duration_s:
            continue
        last_start[sid] = start
        truth = fixture(mode, rng.randrange(2**32), tiles)
        merge = measure_merge(truth)
        index = len(events)
        write_json(out / "truth" / f"image_{index}.json", truth)
        events.append({"release_step": round(ready/.1), "source_sat_id": sid,
                       "image_tiling": truth["tiling"],
                       "window_result_bytes": [w["output_bytes"] for w in truth["windows"]],
                       "aggregation_delay_s": merge["median_seconds"]})
        details.append({"image_id": index, "mode": mode, "source_sat_id": sid,
                        "capture_start_s": start, "capture_end_s": end, "ready_s": ready,
                        "target_id": window["target_id"], "window_id": window["window_id"],
                        "truth_sha256": digest(truth), "merge_reference": merge})
    ordered = sorted(zip(events, details), key=lambda pair: (pair[0]["release_step"], pair[0]["source_sat_id"]))
    events, details = [p[0] for p in ordered], [p[1] for p in ordered]
    for index, detail in enumerate(details):
        detail["task_id"] = f"task_{index}"
    base["scheduled_task_arrivals"] = events
    base["aggregation_delay_s"] = statistics.mean([e["aggregation_delay_s"] for e in events]) if events else 0
    config = asdict(config_from_dict(base))
    hours = Counter(int(e["release_step"] * .1 // 3600) for e in events)
    times = [0] + [e["release_step"] * .1 for e in events] + [duration_s]
    manifest = {"tier": tier, "date": date, "epoch_anchor": ANCHOR, "source_count": ns,
                "compute_count": len(tle)-ns, "requested_tiles": tiles, "arrival_duration_s": duration_s,
                "D_seconds": 300, "config_sha256": digest(config), "events_sha256": digest(events),
                "events": details, "mode_counts": dict(Counter(d["mode"] for d in details)),
                "per_hour_counts": {str(h): hours[h] for h in range(math.ceil(duration_s/3600))},
                "longest_no_arrival_s": max(b-a for a,b in zip(times,times[1:])),
                "runtime": {"machine": platform.machine(), "python": sys.version,
                            "system": platform.system()},
                "acquisition": {**schedule["metadata"], "capture_duration_s": {k: v[3] for k,v in MODES.items()},
                                "formation_delay_s": 60, "end_guard_s": 10, "one_image_per_window": True},
                "hardware_pending": ["Onboard preprocessing and streaming tile memory peak for mixed sizes"],
                "sealed_test_dates_accessed": False}
    write_json(out / "config.json", config)
    write_json(out / "manifest.json", manifest)
    write_json(out / "windows.json", schedule["windows"])
    return out, manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tier", choices=TIERS, default="M")
    parser.add_argument("--date", default="2026-10-15")
    parser.add_argument("--tiles", type=int, choices=(1,4,16,64), default=16)
    parser.add_argument("--duration", type=int, default=86400)
    parser.add_argument("--output", type=Path, default=ROOT)
    args = parser.parse_args()
    path, manifest = build(args.tier, args.date, args.tiles, args.duration, args.output)
    print(json.dumps({"path": str(path), "tasks": len(manifest["events"]),
                      "modes": manifest["mode_counts"], "hour_counts": manifest["per_hour_counts"]}))
