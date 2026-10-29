"""第一阶段只生成10-16训练日；保留主场景冻结模板与旧日期产物。"""

from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import multiprocessing
from pathlib import Path
import random
import sys
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.estimate_load_pressure import OUT as CALIBRATION, peak_window, sha
from analysis.redesign_scenario import MODES, tiling, fixture, measure_merge, digest, write_json
from analysis.run_main_local_panel import MAIN, verify_main
from analysis.run_load_pressure_short import verify
from sim.acquisition import generate_acquisition_schedule
from sim.config import config_from_dict
from sim.orbit import load_satellites, parse_start_time_utc, position_km

ROOT = MAIN / "rl_stage1_20261022_auditfix_v2"
DATE = "2026-10-16"
GEOMETRY_FIELDS = {"source_sat_id", "target_id", "start_s", "end_s", "sample_count",
                   "left_censored", "right_censored", "min_off_nadir_deg", "max_sun_elevation_deg"}
FIELD_CLASSES = {**dict.fromkeys(GEOMETRY_FIELDS, "geometry"),
                 "window_id": "identifier", "scheduled_strips": "derived"}


def read(path):
    return json.loads(path.read_text())


def windows_for_source(args):
    sid, tle, points, date = args
    if date not in {"2026-10-15", DATE}:
        raise ValueError("Stage1 date allowlist")
    schedule = generate_acquisition_schedule(tle, date+"T00:00:00Z", [sid], duration_s=86400, dt_s=.1,
        targets=[{k: r[k] for k in ("target_id", "lat_deg", "lon_deg", "radius_km")} for r in points],
        scan_step_s=10, max_off_nadir_deg=40, min_sun_elevation_deg=-90, sun_guard_deg=0,
        min_spacing_s=60, pass_cooldown_s=0, incidence_range_deg=(20, 40))
    return schedule["windows"]


def date_arrivals(windows, selected, date):
    if date not in {"2026-10-15", DATE}:
        raise ValueError("Stage1 date allowlist")
    rng = random.Random(1701+int(date.replace("-", "")))
    events, last_start = [], {}
    for w in sorted((w for w in windows if w["target_id"] in selected),
                    key=lambda w: (w["start_s"], w["source_sat_id"], w["target_id"])):
        mode = rng.choices(list(MODES), weights=(.2, .6, .2))[0]
        sid = w["source_sat_id"]
        start = max(w["start_s"], last_start.get(sid, -60)+60)
        end = start+MODES[mode][3]
        ready = end+60
        if end > w["end_s"]-10 or ready >= 86400:
            continue
        last_start[sid] = start
        seed = rng.randrange(2**32)
        work = tiling(mode, 16)
        events.append({"source_sat_id": sid, "capture_start_s": start, "capture_end_s": end,
                       "ready_s": ready, "target_id": w["target_id"], "mode": mode, "truth_seed": seed,
                       "window_id": w["window_id"], "raw_bytes": work.image_size_mb*1e6,
                       "compute_windows": sum(t.compute_cost for t in work.tiles())})
    return sorted(events, key=lambda e: (e["ready_s"], e["source_sat_id"]))


def window_differences(expected, actual):
    # 用源星/目标及窗口次序配对，边界变化也必须显式报告。
    def indexed(rows):
        groups = {}
        for row in sorted(rows, key=lambda r: (r["source_sat_id"], r["target_id"], r["start_s"], r["end_s"])):
            pair = row["source_sat_id"], row["target_id"]
            groups.setdefault(pair, []).append(row)
        return {(sid, target, i): row for (sid, target), group in groups.items() for i, row in enumerate(group)}
    left, right = indexed(expected), indexed(actual)
    differences = []
    for key in sorted(left.keys() | right.keys()):
        a, b = left.get(key), right.get(key)
        if a is None or b is None:
            differences.append({"key": key, "field": "__window__", "class": "geometry", "expected": a, "actual": b})
            continue
        for field in sorted(a.keys() | b.keys()):
            category = FIELD_CLASSES.get(field, "unclassified")
            if field not in a or field not in b or a[field] != b[field] or category == "unclassified":
                differences.append({"key": key, "field": field, "class": category,
                                    "expected": a.get(field), "actual": b.get(field)})
    return differences


def generate_windows(tle, points, date, progress_path):
    windows = []
    with ProcessPoolExecutor(max_workers=8, mp_context=multiprocessing.get_context("spawn")) as pool:
        args = [(sid, tle, points, date) for sid in range(24)]
        for done, ww in enumerate(pool.map(windows_for_source, args), 1):
            windows.extend(ww)
            write_json(progress_path, {"sources": done, "total": 24, "date": date, "status": "GEOMETRY"})
    windows.sort(key=lambda w: (w["start_s"], w["source_sat_id"], w["target_id"]))
    for i, w in enumerate(windows):
        w["window_id"] = i
    return windows


def audit_development(cfg, points):
    directory = ROOT / "development_audit"
    registration_path = CALIBRATION / "maritime_extension/registration.json"
    old_windows_path = CALIBRATION / "maritime_extension/windows.json"
    old_arrivals_path = CALIBRATION / "maritime_extension/maritime80_arrivals.json"
    registration = read(registration_path)
    ids = {r["target_id"] for r in points}
    assert ids == set(registration["sets"]["maritime80"])
    assert points == [r for r in registration["targets"] if r["target_id"] in ids]
    expected = read(old_windows_path)
    write_json(directory/"expected_windows.json", expected)
    write_json(directory/"targets160.json", registration["targets"])
    actual = generate_windows(cfg["topology"]["tle_lines"][:24], registration["targets"],
                              "2026-10-15", directory/"progress.json")
    write_json(directory/"actual_windows.json", actual)
    differences = window_differences(expected, actual)
    write_json(directory/"window_field_differences.json", differences)
    expected_arrivals = read(old_arrivals_path)
    actual_arrivals = date_arrivals(actual, ids, "2026-10-15")
    write_json(directory/"expected_maritime80_arrivals.json", expected_arrivals)
    write_json(directory/"actual_maritime80_arrivals.json", actual_arrivals)
    arrival_diffs = []
    for i in range(max(len(expected_arrivals), len(actual_arrivals))):
        a = expected_arrivals[i] if i < len(expected_arrivals) else None
        b = actual_arrivals[i] if i < len(actual_arrivals) else None
        if a is None or b is None:
            arrival_diffs.append({"index": i, "field": "__arrival__", "expected": a, "actual": b})
        else:
            arrival_diffs.extend({"index": i, "field": k, "expected": a.get(k), "actual": b.get(k)}
                for k in sorted(a.keys() | b.keys()) if k not in a or k not in b or a[k] != b[k])
    write_json(directory/"arrival_field_differences.json", arrival_diffs)
    blocking = [d for d in differences if d["class"] not in {"identifier", "derived"}]
    passed = not blocking and not arrival_diffs
    evidence = sorted(directory.glob("*.json")) + [registration_path, old_windows_path, old_arrivals_path]
    audit = {"status": "PASS" if passed else "BLOCKED", "comparison": "160 targets vs same160; all24 sources",
             "field_classes": FIELD_CLASSES, "strict_geometry": not blocking,
             "window_count_expected": len(expected), "window_count_actual": len(actual),
             "difference_counts": {c: sum(d["class"] == c for d in differences)
                                   for c in ("geometry", "identifier", "derived", "unclassified")},
             "development_arrivals_exact": not arrival_diffs, "arrival_count": len(expected_arrivals),
             "reason": "Strict geometry and full final maritime80 arrivals equal; only identifier/derived differences permitted"
                        if passed else "Geometry, unclassified field, or final arrival mismatch; stop before10-16",
             "evidence_hashes": {str(p): sha(p) for p in evidence}}
    write_json(directory/"AUDIT.json", audit)
    if not passed:
        raise AssertionError(audit["reason"])
    return audit


def prepare():
    started = time.perf_counter()
    verify_main()
    if ROOT.exists():
        raise FileExistsError("No automatic preparation retry")
    ROOT.mkdir()
    points = read(MAIN / "targets_maritime80.json")
    cfg = read(MAIN / "config_template.json")
    ids = {r["target_id"] for r in points}
    development_audit = audit_development(cfg, points)
    windows = generate_windows(cfg["topology"]["tle_lines"][:24], points, DATE, ROOT/"prepare_progress.json")
    events = date_arrivals(windows, ids, DATE)
    peak = peak_window([e["ready_s"] for e in events], [e["raw_bytes"] for e in events], 3600)
    start, end = peak["start_s"], peak["end_s"]
    origin = max(0, int((start-600)//60*60))
    duration = int(end+600-origin)
    original = deepcopy(cfg)
    cfg["topology"]["start_time_utc"] = (datetime.fromisoformat(DATE).replace(tzinfo=timezone.utc)+timedelta(seconds=origin)).strftime("%Y-%m-%dT%H:%M:%SZ")
    cfg["sim_steps"] = round(duration/cfg["dt"])
    cfg["task_arrival_end_step"] = round((end-origin)/cfg["dt"])
    cfg["scheduled_task_arrivals"] = []
    details = []
    for index, e in enumerate(events):
        if not origin <= e["ready_s"] < end:
            continue
        tid = f"task_{len(details)}"
        truth = fixture(e["mode"], e["truth_seed"], 16)
        merge = measure_merge(truth)
        write_json(ROOT/"truth"/f"{tid}.json", truth)
        cfg["scheduled_task_arrivals"].append({"release_step": round((e["ready_s"]-origin)/cfg["dt"]),
            "source_sat_id": e["source_sat_id"], "image_tiling": truth["tiling"],
            "window_result_bytes": [w["output_bytes"] for w in truth["windows"]],
            "aggregation_delay_s": merge["median_seconds"]})
        details.append({"task_id": tid, "daily_event_index": index, **e, "scored": e["ready_s"] >= start,
                        "truth_sha256": digest(truth), "merge_reference": merge})
    allowed = {"topology", "sim_steps", "task_arrival_end_step", "scheduled_task_arrivals"}
    assert all(cfg[k] == v for k, v in original.items() if k not in allowed)
    assert all(cfg["topology"][k] == v for k, v in original["topology"].items() if k != "start_time_utc")
    config_from_dict(cfg)
    sats = load_satellites(cfg["topology"]["tle_lines"])
    midnight = parse_start_time_utc(DATE+"T00:00:00Z")
    shifted = parse_start_time_utc(cfg["topology"]["start_time_utc"])
    for sid in (0, 23, 24, 167):
        for t in (0, start-origin, duration):
            assert position_km(sats[sid], midnight, origin+t) == position_km(sats[sid], shifted, t)
    write_json(ROOT/"windows.json", windows)
    write_json(ROOT/"daily_arrivals.json", events)
    write_json(ROOT/"date_adapter_audit.json", {"status": "PASS", "development_audit": development_audit,
        "fixed_TLE_epoch": True, "physical_config_unchanged": True,
        "window_ids_note": "New day's ids index its 80-target windows; old160-target archival ids not reused",
        "wall_seconds": time.perf_counter()-started})
    base = read(MAIN/"local_baselines/protocol.json")
    paths = [Path(__file__), Path("analysis/main_rl_interface.py"), Path("analysis/run_main_rl_stage1.py"),
             Path("tests/test_main_rl_interface.py"), Path("docs/rl_local_mappo_plan_20261022.md"),
             ROOT/"STAGE1_PROTOCOL.md", MAIN/"manifest.json", MAIN/"date_split.json", MAIN/"config_template.json",
             MAIN/"targets_maritime80.json", ROOT/"daily_arrivals.json", ROOT/"windows.json", ROOT/"date_adapter_audit.json"]
    # 协议文本由调用方先写到独立的预登记位置，再复制到本结果目录。
    protocol_source = MAIN/"RL_STAGE1_PROTOCOL_20261022.md"
    (ROOT/"STAGE1_PROTOCOL.md").write_text(protocol_source.read_text())
    paths += sorted((ROOT/"truth").glob("*.json"))
    paths += sorted((ROOT/"development_audit").glob("*.json"))
    p = {"stage": 1, "date": DATE, "workers": 3, "methods": ["reference", "teacher", "student"],
         "seed": 1701, "scored_task_ids": [e["task_id"] for e in details if e["scored"]], "events": details,
         "score_start_s": int(start-origin), "score_end_s": int(end-origin), "origin_absolute_s": origin,
         "peak": peak, "sim_duration_s": duration, "followup_s": 600, "D_seconds": 60, "D_max": 600,
         "config": cfg, "config_sha256": digest(cfg), "frozen_protocol": base["frozen_protocol"],
         "input_hashes": {str(f): sha(f) for f in paths}, "no_retry": True,
         "stop_after": "Stage1 report then human confirmation; no BC or MAPPO training", "sealed_tests": True}
    write_json(ROOT/"protocol.json", p)
    verify(p); verify_main()
    print(json.dumps({"status": "PREPARED", "main_images": len(p['scored_task_ids']), "total_images": len(details),
                      "duration_s": duration, "date": DATE, "peak": peak, "wall_seconds": time.perf_counter()-started}))


if __name__ == "__main__":
    try:
        prepare()
    except Exception:
        if ROOT.exists() and not (ROOT/"final_status.json").exists():
            write_json(ROOT/"final_status.json", {"status": "BLOCKED", "stage": "preparation",
                       "traceback": traceback.format_exc(), "no_retry": True})
        raise
