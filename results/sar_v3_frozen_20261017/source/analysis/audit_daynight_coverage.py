"""美国固定点光学/SAR 全天成像机会审计；不运行任务调度或校准 SAR 算力。"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import sys
import time

import numpy as np
from skyfield.api import wgs84
from skyfield.framelib import itrs

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sim.acquisition import US_CIVIL_REGIONS, solar_elevation_deg
from sim.constellation import walker_tle
from sim.orbit import load_satellites, parse_start_time_utc, time_grid


EPOCH = "2026-10-15T00:00:00Z"
DURATION_S = 86400
SCAN_S = 5
SAR_CAPTURE_S = 15
MIN_SPACING_S = 30
MAX_PER_WINDOW = 3
SAR_COUNTS = (12, 24, 48)
SAR_PLANES = 12
SAR_MAX_PER_PLANE = 4
FLEET_SOURCE = "https://sar.iceye.com/5.2.0/productguide/fleet/"
MODE_SOURCE = "https://sar.iceye.com/6.0.2/productspecification/imagingmodes/"


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def source_groups():
    return {
        count: [24 + plane * SAR_MAX_PER_PLANE + slot
                for plane in range(SAR_PLANES)
                for slot in range(0, SAR_MAX_PER_PLANE, SAR_MAX_PER_PLANE // (count // SAR_PLANES))]
        for count in SAR_COUNTS
    }


def selection():
    targets = [{"target_id": r.target_id, "lat_deg": r.lat_deg,
                "lon_deg": r.lon_deg, "radius_km": 0} for r in US_CIVIL_REGIONS]
    return {
        "scope": "Geometry-only development diagnostic; no scheduler, delivery result, or formal simulation config",
        "epoch": EPOCH, "duration_s": DURATION_S, "scan_step_s": SCAN_S,
        "selection_rule": "All three architecture groups fixed before propagation; all selected acquisitions retained; no ground-contact or policy-result filtering",
        "minimum_requirement": "At least one acquisition release in every UTC hour; short gaps permitted",
        "requirement_scope": "A necessary screening criterion for 24h task supply; not guaranteed continuous observation or operational imaging feasibility",
        "targets": targets,
        "target_scope": "Five previously used US civil-region centers only; radius=0; not complete US area coverage",
        "optical": {"count": 2, "source_ids": [0, 1], "altitude_km": 620,
                    "inclination_deg": 97.9, "raan_deg": [357.5, 12.5], "phase_deg": [0, 15],
                    "max_off_nadir_deg": 30, "minimum_solar_elevation_deg": 21,
                    "capture_duration_s": None, "release_rule": "sampled imaging instant; exposure and attitude uncalibrated"},
        "sar": {"counts": list(SAR_COUNTS), "planes": SAR_PLANES,
                "satellites_per_plane": [1, 2, 4], "altitude_km": 570, "inclination_deg": 97.7,
                "raan_deg": list(range(0, 360, 30)), "phasing": 0,
                "phase_rule": "Uniform in each plane; 1/2/4 slots are nested subsets of 0/90/180/270 degrees",
                "architecture_reason": "Twelve RAAN planes spread local crossing times; along-track density alone cannot fill all local-time gaps",
                "source_ids_by_count": {str(k): v for k, v in source_groups().items()},
                "ground_incidence_deg": [20, 40], "capture_duration_s": SAR_CAPTURE_S,
                "solar_filter": False, "release_rule": "capture end strictly before 86400s; focused-product ready delay remains uncalibrated",
                "squint_or_attitude_constraint": False,
                "geometry_scope": "Optimistic both-side angular screen; no squint, slew, dwell tracking, beam, swath, cloud, energy, or imaging duty-cycle validation",
                "vendor_product_size_gb_bound": [2, 3.5],
                "product_size_scope": "Vendor product bound only; not raw echo volume or actual per-event transport load",
                "compute_cost": None, "focused_product_ready_delay_s": None,
                "sources": [FLEET_SOURCE, MODE_SOURCE]},
        "helpers": {"count": 22, "source_ids": [], "retained_satellite_ids": list(range(2, 24)),
                    "altitude_km": 550, "inclination_deg": 53,
                    "propagated": False, "note": "Role metadata retained; connectivity and delivery are not audited here"},
        "acquisition_limits": {"minimum_start_spacing_s_per_source_all_targets": MIN_SPACING_S,
                               "maximum_captures_per_contiguous_eligible_target_window": MAX_PER_WINDOW,
                               "candidate_order": "capture_start_s, source_sat_id, target_id"},
        "sampling": "SAR eligibility checked at start, +5s, +10s, and end +15s; between-sample feasibility is not proved; short windows may be missed",
        "night_definition": "Approximate geometric solar elevation <0 degrees at target at capture start; 0..21 degrees is not night but is excluded for optical",
        "task_ready_caveat": "Capture-end timestamps are geometric acquisition releases, not calibrated ready-for-inference task arrivals",
        "simulation_protocol_after_calibration": {"arrival_interval_s": [0, DURATION_S],
            "arrival_end_exclusive": True, "followup_s": 301, "dt_s": 0.1,
            "start_empty_once": True, "daily_queue_reset": False, "implemented_or_run": False},
        "evidence_boundary": "No guarantee that adding sources increases compute utilization; no SAR computation model is set",
    }


def tles_by_source():
    optical = walker_tle(EPOCH, 2, 12, 620, 97.9, raan_offset_deg=357.5,
                         phasing=1, plane_raan_offsets_deg=[0, 15])
    sar = walker_tle(EPOCH, SAR_PLANES, SAR_MAX_PER_PLANE, 570, 97.7,
                     phasing=0, first_satellite_number=100)
    return {0: optical[0], 1: optical[12], **{24 + i: tle for i, tle in enumerate(sar)}}


def point_geometry(positions, target):
    """在同一 ITRS 坐标系计算地面法线入射角和卫星天底离轴角。"""
    ground = wgs84.latlon(target["lat_deg"], target["lon_deg"])
    ground_xyz = np.asarray(ground.itrs_xyz.km)
    lat, lon = np.deg2rad([target["lat_deg"], target["lon_deg"]])
    normal = np.array([np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)])
    to_satellite = positions - ground_xyz
    ranges = np.linalg.norm(to_satellite, axis=1)
    elevation = np.rad2deg(np.arcsin(np.clip(to_satellite @ normal / ranges, -1, 1)))
    off_nadir = np.rad2deg(np.arccos(np.clip(
        np.sum(to_satellite * positions, axis=1) / (ranges * np.linalg.norm(positions, axis=1)), -1, 1)))
    return {"sat_elevation_deg": elevation, "ground_incidence_deg": 90 - elevation,
            "off_nadir_deg": off_nadir, "slant_range_km": ranges}


def intervals(mask):
    boundaries = np.flatnonzero(np.diff(np.r_[False, np.asarray(mask, dtype=bool), False]))
    return list(zip(boundaries[::2], boundaries[1::2]))


def eligible_capture_starts(mask, capture_samples):
    mask = np.asarray(mask, dtype=bool)
    if type(capture_samples) is not int or capture_samples < 0:
        raise ValueError("capture_samples must be a nonnegative integer")
    count = len(mask) - capture_samples
    if count <= 0:
        return np.zeros(len(mask), dtype=bool)
    valid = np.ones(count, dtype=bool)
    for offset in range(capture_samples + 1):
        valid &= mask[offset:offset + count]
    return np.r_[valid, np.zeros(capture_samples, dtype=bool)]


def select_candidates(candidates, *, duration_s=DURATION_S,
                      min_spacing_s=MIN_SPACING_S, max_per_window=MAX_PER_WINDOW):
    selected, last_start, counts = [], {}, Counter()
    for event in sorted(candidates, key=lambda e: (e["capture_start_s"], e["source_sat_id"], e["target_id"])):
        sid, when = event["source_sat_id"], event["capture_start_s"]
        if not 0 <= when <= event["release_s"] < duration_s:
            continue
        if when - last_start.get(sid, -np.inf) < min_spacing_s:
            continue
        if counts[event["window_id"]] >= max_per_window:
            continue
        selected.append(event)
        last_start[sid] = when
        counts[event["window_id"]] += 1
    return sorted(selected, key=lambda e: (e["release_s"], e["source_sat_id"], e["target_id"]))


def generate_geometry(spec, *, tle_map=None):
    started = time.monotonic()
    offsets = np.arange(0, DURATION_S + SCAN_S, SCAN_S, dtype=float)
    epoch = parse_start_time_utc(EPOCH)
    times = time_grid(epoch, offsets)
    datetimes = [epoch + timedelta(seconds=float(t)) for t in offsets]
    tle_map = tles_by_source() if tle_map is None else tle_map
    source_ids = sorted(tle_map)
    satellites = load_satellites([tle_map[sid] for sid in source_ids])
    solar = {target["target_id"]: solar_elevation_deg(datetimes, target["lat_deg"], target["lon_deg"])
             for target in spec["targets"]}
    windows, candidates = [], []
    max_altaz_error = 0.0
    checks = np.array([0, len(offsets) // 2, len(offsets) - 1])
    # 每个源星仅传播一次；所有目标和三个嵌套星座共享同一条轨迹。
    for sid, satellite in zip(source_ids, satellites):
        positions = satellite.at(times).frame_xyz(itrs).km.T
        if not np.isfinite(positions).all():
            raise ValueError(f"Nonfinite propagated positions for source {sid}")
        sensor = "optical" if sid < 2 else "sar"
        for target in spec["targets"]:
            geometry = point_geometry(positions, target)
            sun = solar[target["target_id"]]
            if sensor == "optical":
                good = ((geometry["sat_elevation_deg"] > 0) & (geometry["off_nadir_deg"] <= 30) & (sun >= 21))
                capture_s = 0
            else:
                good = ((geometry["sat_elevation_deg"] > 0) & (geometry["ground_incidence_deg"] >= 20)
                        & (geometry["ground_incidence_deg"] <= 40))
                capture_s = SAR_CAPTURE_S
            valid_starts = eligible_capture_starts(good, capture_s // SCAN_S)
            if sid == source_ids[0]:
                ground = wgs84.latlon(target["lat_deg"], target["lon_deg"])
                expected = (satellite - ground).at(times[checks]).altaz()[0].degrees
                error = float(np.max(np.abs(expected - geometry["sat_elevation_deg"][checks])))
                max_altaz_error = max(max_altaz_error, error)
                if error > 1e-7:
                    raise AssertionError("Cached ITRS geometry differs from Skyfield altaz")
            for start, stop in intervals(good):
                wid = len(windows)
                windows.append({"window_id": wid, "sensor": sensor, "source_sat_id": sid,
                    "target_id": target["target_id"], "start_s": float(offsets[start]),
                    "last_eligible_sample_s": float(offsets[stop - 1]),
                    "end_s_exclusive_approx": float(min(DURATION_S, offsets[stop - 1] + SCAN_S)),
                    "left_censored": bool(start == 0), "right_censored": bool(stop == len(offsets)),
                    "eligible_sample_count": int(stop - start)})
                for index in range(start, stop):
                    if not valid_starts[index]:
                        continue
                    end = index + capture_s // SCAN_S
                    candidates.append({"capture_start_s": float(offsets[index]),
                        "release_s": float(offsets[end]), "capture_duration_s": capture_s if sensor == "sar" else None,
                        "source_sat_id": sid, "sensor": sensor, "target_id": target["target_id"], "window_id": wid,
                        "sun_elevation_deg_at_start": float(sun[index]),
                        "night": bool(sun[index] < 0),
                        "geometry_at_start": {key: float(value[index]) for key, value in geometry.items()},
                        "capture_ground_incidence_min_deg": float(np.min(geometry["ground_incidence_deg"][index:end + 1])),
                        "capture_ground_incidence_max_deg": float(np.max(geometry["ground_incidence_deg"][index:end + 1]))})
    events = select_candidates(candidates)
    chosen = Counter(e["window_id"] for e in events)
    for window in windows:
        window["selected_captures"] = chosen[window["window_id"]]
    return {"events": events, "windows": windows,
            "tle_by_source": {str(sid): list(tle) for sid, tle in tle_map.items()},
            "verification": {"source_trajectories_propagated": len(source_ids),
                "samples_per_trajectory": len(offsets), "helpers_propagated": 0,
                "cached_elevation_vs_skyfield_max_error_deg": max_altaz_error,
                "geometry_elapsed_s": time.monotonic() - started}}


def summarize_events(events, source_ids, target_ids, *, duration_s=DURATION_S):
    times = sorted(e["release_s"] for e in events)
    if any(not 0 <= t < duration_s for t in times):
        raise ValueError("Release outside the exclusive arrival interval")
    hourly = []
    for hour in range(int(np.ceil(duration_s / 3600))):
        rows = [e for e in events if hour * 3600 <= e["release_s"] < min((hour + 1) * 3600, duration_s)]
        hourly.append({"hour_utc": hour, "optical": sum(e["sensor"] == "optical" for e in rows),
                       "sar": sum(e["sensor"] == "sar" for e in rows),
                       "night": sum(e["night"] for e in rows), "total": len(rows)})
    edges = [0.0, *times, float(duration_s)]
    gaps = [(b - a, a, b) for a, b in zip(edges, edges[1:])]
    longest, start, stop = max(gaps)
    left, maximum, dense_start = 0, 0, None
    for right, value in enumerate(times):
        while times[left] <= value - 300:
            left += 1
        if right - left + 1 > maximum:
            maximum, dense_start = right - left + 1, times[left]
    by_sensor = Counter(e["sensor"] for e in events)
    return {"total_acquisitions": len(events), "count_by_sensor": {key: by_sensor[key] for key in ("optical", "sar")},
            "night_acquisitions": sum(e["night"] for e in events),
            "count_by_source": {str(sid): sum(e["source_sat_id"] == sid for e in events) for sid in source_ids},
            "count_by_target": {target: sum(e["target_id"] == target for e in events) for target in target_ids},
            "hourly_counts": hourly, "nonempty_hours": sum(row["total"] > 0 for row in hourly),
            "empty_hours_utc": [row["hour_utc"] for row in hourly if row["total"] == 0],
            "every_hour_has_acquisition": all(row["total"] > 0 for row in hourly),
            "longest_gap_including_start_end": {"duration_s": longest, "start_s": start, "end_s": stop},
            "densest_300s": {"count": maximum, "start_s": dense_start,
                             "interval_rule": "[start_s, start_s+300); ties at same instant all count"},
            "sar_vendor_product_volume_gb_bound": [by_sensor["sar"] * bound for bound in (2, 3.5)],
            "sar_compute_demand": None, "sar_task_ready_timing": None}


def report_text(report):
    rows = ["# 固定美国目标的全天成像机会诊断", "",
        "开发日期：2026-10-15 UTC，完整 24 小时。仅审计几何机会，不运行调度策略，不计算 P300。", "",
        "2 颗原光学星、22 颗计算中继星保留；SAR 为 12 轨道面 × 1/2/4 颗（12/24/48 颗），570 km、97.7°、RAAN 间隔 30°。三个组在传播前登记，沿轨位置嵌套。", "",
        "只看原五个美国区域的中心点（半径 0），不代表整片区域的覆盖。SAR 地面入射角 20°..40°，15 秒采集在每 5 秒样点检查；双侧成像且未约束 squint、姿态、波束及功耗，是乐观角度筛查。光学保留太阳高度至少 21°、离轴角至多 30°，曝光和姿态时间未校准。", "",
        "每源全部目标共享 30 秒采集起点间隔，每个连续合格目标窗口最多采集 3 次。SAR 在采集结束时记机会释放，严格早于 86400 秒；这不是已经校准的聚焦图像 ready 时刻。", "",
        "| SAR数 | 总星数 | 光学/SAR机会 | 夜间机会 | 有任务小时 | 最长空窗（分） | 300秒最多机会 | 小时最低条件 |",
        "|---:|---:|---:|---:|---:|---:|---:|---|"]
    for group in report["groups"]:
        s = group["summary"]
        rows.append(f"| {group['sar_count']} | {group['total_constellation_satellites']} | "
                    f"{s['count_by_sensor']['optical']}/{s['count_by_sensor']['sar']} | {s['night_acquisitions']} | "
                    f"{s['nonempty_hours']}/24 | {s['longest_gap_including_start_end']['duration_s'] / 60:.2f} | "
                    f"{s['densest_300s']['count']} | {'通过几何筛查' if s['every_hour_has_acquisition'] else '未通过'} |")
    rows += ["", "## 每小时机会数", "", "| UTC小时 | 12 SAR | 24 SAR | 48 SAR |", "|---:|---:|---:|---:|"]
    for hour in range(24):
        rows.append(f"| {hour:02d}:00 | " + " | ".join(str(g["summary"]["hourly_counts"][hour]["total"]) for g in report["groups"]) + " |")
    rows += ["", "## 解释边界", "",
        "每小时至少一次是用户要求的最低筛查条件，满足也不保证连续观察或真实载荷可成像。最长空窗包含从 00:00 到首条机会和末条机会到 24:00。夜间定义为目标处采集起点的近似太阳几何高度小于 0°。", "",
        "SAR 2..3.5 GB/图仅记录厂商产品规格量级，不设置原始回波体积、压缩率、推理成本或聚焦时延；没有将光学计算成本复制给 SAR。计算星轨迹、星间连接、地面窗口、任务完成率和算力利用率均未评估。", "",
        "正式混合仿真还需要分别定义光学/SAR的图像产品与计算负载，连续运行24小时到达并追加301秒观测尾段，不能按日清空队列。", "",
        f"轨迹缓存与 Skyfield altaz 抽检最大误差：{report['verification']['cached_elevation_vs_skyfield_max_error_deg']:.3g}°。",
        f"几何计算耗时：{report['verification']['geometry_elapsed_s']:.2f} 秒。", "",
        f"依据：[ICEYE 成像模式]({MODE_SOURCE})；[ICEYE 星座介绍]({FLEET_SOURCE})。具体星数、轨道面和采集频率为研究设计值，不是复现真实星座。", ""]
    return "\n".join(rows)


def run_audit(out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)
    spec = selection()
    save(out_dir / "selection.json", spec)
    geometry = generate_geometry(spec)
    groups = []
    for count, sar_ids in source_groups().items():
        ids = [0, 1, *sar_ids]
        events = [e for e in geometry["events"] if e["source_sat_id"] in ids]
        groups.append({"id": f"sar{count}", "sar_count": count, "total_constellation_satellites": count + 24,
                       "source_sat_ids": ids, "events": events,
                       "summary": summarize_events(events, ids, [t["target_id"] for t in spec["targets"]])})
    report = {"selection_sha256": digest(spec), "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "selection": spec, "groups": groups, "windows": geometry["windows"],
              "tle_by_source": geometry["tle_by_source"], "verification": geometry["verification"]}
    save(out_dir / "coverage.json", report)
    (out_dir / "REPORT.md").write_text(report_text(report), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=ROOT / "results/daynight_coverage_20261015")
    args = parser.parse_args()
    report = run_audit(args.out_dir)
    print(json.dumps({"out_dir": str(args.out_dir), "groups": [
        {"sar_count": g["sar_count"], **g["summary"]} for g in report["groups"]],
        "verification": report["verification"]}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
