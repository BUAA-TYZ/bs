"""扩充有来源的沿海目标，仅生成成像机会与运行预算，不执行调度。"""

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import math
import multiprocessing
from pathlib import Path
import random
import sys
import time

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.audit_sar_coastal_coverage import COASTAL_TARGETS
from analysis.estimate_load_pressure import OUT, PANEL, peak_window, sha
from analysis.redesign_scenario import MODES, tiling, write_json
from sim.acquisition import generate_acquisition_schedule


def distance_km(a, b):
    p, q = np.deg2rad([a["lat_deg"], b["lat_deg"]])
    dlon = math.radians(a["lon_deg"]-b["lon_deg"])
    return 6371.0088*math.acos(float(np.clip(math.sin(p)*math.sin(q)+math.cos(p)*math.cos(q)*math.cos(dlon), -1, 1)))


def targets():
    original = [dict(r) for r in COASTAL_TARGETS]
    data = json.loads((OUT/"noaa_waterlevels.json").read_text())
    stations = [{"target_id": "noaa_"+r["id"], "lat_deg": r["lat"], "lon_deg": r["lng"],
                 "radius_km": 0, "noaa_station_id": r["id"], "noaa_station_name": r["name"],
                 "source_url": r["self"]} for r in data["stations"]
                if r.get("tidal") and not r.get("greatlakes") and 24 <= r["lat"] <= 50
                and -125 <= r["lng"] <= -66
                and r["id"] not in {x["noaa_station_id"] for x in original}]
    chosen = original[:]
    while stations and len(chosen) < 80:
        # 点集仅由空间距离决定，与卫星接入、下行窗口和策略成绩无关。
        ranked = sorted(((min(distance_km(r, c) for c in chosen), r["target_id"], r) for r in stations),
                        key=lambda row: (-row[0], row[1]))
        if ranked[0][0] < 20:
            break
        chosen.append(ranked[0][2])
        stations.remove(ranked[0][2])
    return chosen


def source_windows(sid, tle, points):
    schedule = generate_acquisition_schedule(
        tle, "2026-10-15T00:00:00Z", [sid], duration_s=86400, dt_s=.1,
        targets=[{k:r[k] for k in ("target_id", "lat_deg", "lon_deg", "radius_km")} for r in points],
        scan_step_s=10, max_off_nadir_deg=40, min_sun_elevation_deg=-90, sun_guard_deg=0,
        min_spacing_s=60, pass_cooldown_s=0, incidence_range_deg=(20,40))
    return sid, schedule["windows"]


def arrivals(windows, selected):
    rng = random.Random(1701+20261015)
    events, last_start = [], {}
    for window in sorted((w for w in windows if w["target_id"] in selected),
                         key=lambda w:(w["start_s"], w["source_sat_id"], w["target_id"])):
        mode = rng.choices(list(MODES), weights=(.2,.6,.2))[0]
        sid = window["source_sat_id"]
        start = max(window["start_s"], last_start.get(sid, -60)+60)
        end = start+MODES[mode][3]
        ready = end+60
        if end > window["end_s"]-10 or ready >= 86400:
            continue
        last_start[sid] = start
        seed = rng.randrange(2**32)
        work = tiling(mode, 16)
        events.append({"source_sat_id":sid, "capture_start_s":start, "capture_end_s":end,
                       "ready_s":ready, "target_id":window["target_id"], "mode":mode,
                       "truth_seed":seed, "window_id":window["window_id"],
                       "raw_bytes":work.image_size_mb*1e6,
                       "compute_windows":sum(t.compute_cost for t in work.tiles())})
    return sorted(events, key=lambda e:(e["ready_s"],e["source_sat_id"]))


def main():
    if (OUT/"target_registration.json").exists():
        raise FileExistsError("Existing registration must be preserved")
    started = time.monotonic()
    cfg = json.loads((PANEL/"protocol.json").read_text())["jobs"]["compute_4_local"]["config"]
    points = targets()
    registration = {"created_utc":datetime.now(timezone.utc).isoformat(),
                    "source_url":"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=waterlevels",
                    "source_sha256":sha(OUT/"noaa_waterlevels.json"),
                    "entry_sha256":sha(Path(__file__)), "targets":points,
                    "selection":"Original five + farthest point; CONUS tidal non-Great-Lakes, minimum 20km separation",
                    "sets":{"coast40":[r["target_id"] for r in points[:40]],
                            "coast80":[r["target_id"] for r in points[:80]]},
                    "date":"2026-10-15", "no_scheduler_run":True}
    write_json(OUT/"target_registration.json", registration)
    windows = []
    with ProcessPoolExecutor(max_workers=8, mp_context=multiprocessing.get_context("spawn")) as pool:
        jobs = [pool.submit(source_windows, sid, cfg["topology"]["tle_lines"][:24], points) for sid in range(24)]
        done = 0
        for future in as_completed(jobs):
            sid, rows = future.result()
            windows.extend(rows)
            done += 1
            write_json(OUT/"geometry_progress.json", {"done_sources":done,"total_sources":24,
                       "wall_seconds":time.monotonic()-started})
            print(json.dumps({"done_sources":done,"source":sid,"windows":len(rows)}),flush=True)
    windows.sort(key=lambda w:(w["start_s"],w["source_sat_id"],w["target_id"]))
    for i, window in enumerate(windows):
        window["window_id"] = i
    write_json(OUT/"expanded_windows.json", windows)
    rows = {}
    old = json.loads((OUT/"analytic.json").read_text())
    wall = max(json.loads((PANEL/f"compute_4_{m}.json").read_text())["wall_seconds"] for m in ("local","ground","fifo"))
    for name, ids in registration["sets"].items():
        events = arrivals(windows, set(ids))
        write_json(OUT/f"{name}_arrivals.json", events)
        times, sizes = [e["ready_s"] for e in events], [e["raw_bytes"] for e in events]
        peak = peak_window(times,sizes,600)
        six = peak_window(times,sizes,21600)
        subset = [events[i] for i in six["event_indices"]]
        # 不把全耗时归为固定步进：用图、窗和原始字节三种工作量做保守包络。
        ratios = {"simulation_time":21901/86701, "images":len(subset)/136,
                  "windows":sum(e["compute_windows"] for e in subset)/7988,
                  "bytes":six["raw_bytes"]/118.6e9}
        rows[name] = {"target_count":len(ids),"daily_images":len(events),"image_multiplier":len(events)/136,
                      "daily_bytes":sum(sizes),"byte_multiplier":sum(sizes)/old["raw_bytes"],
                      "peak_600s":peak,"peak_demand_utilization":peak["raw_bytes"]/30e9,
                      "peak_21600s":six,"six_hour_windows":sum(e["compute_windows"] for e in subset),
                      "runtime_basis_wall_s":wall,"workload_ratios":ratios,
                      "runtime_planning_lower_h":wall*ratios["simulation_time"]/3600,
                      "runtime_conservative_h":wall*max(ratios.values())/3600,
                      "runtime_note":"Scaling envelope, not a measured runtime. Historical wall includes pause. No claimed gain from fewer workers."}
    eligible = [name for name,r in rows.items() if r["peak_demand_utilization"]>=.8]
    choice = min(eligible,key=lambda name:(abs(rows[name]["byte_multiplier"]-4), rows[name]["daily_bytes"])) if eligible else None
    report = {"targets_sha256":sha(OUT/"target_registration.json"),"rows":rows,"recommended":choice,
              "stop_for_runtime_confirmation": bool(choice and rows[choice]["runtime_conservative_h"]>4),
              "wall_seconds":time.monotonic()-started,"no_scheduler_run":True,"sealed_tests_accessed":False}
    write_json(OUT/"workload_estimate.json", report)
    print(json.dumps(report,indent=2),flush=True)


if __name__ == "__main__":
    main()
