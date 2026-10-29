"""固定美国民用海港点的纯 SAR 全天几何机会审计，不运行任务调度。"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from analysis import audit_daynight_coverage as base


COASTAL_TARGETS = (
    {"target_id": "la_harbor", "lat_deg": 33.72, "lon_deg": -118.272, "radius_km": 0,
     "noaa_station_id": "9410660", "noaa_station_name": "Los Angeles",
     "source_url": "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/9410660.json"},
    {"target_id": "san_francisco_bay", "lat_deg": 37.806305, "lon_deg": -122.46589, "radius_km": 0,
     "noaa_station_id": "9414290", "noaa_station_name": "San Francisco",
     "source_url": "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/9414290.json"},
    {"target_id": "new_york_harbor", "lat_deg": 40.700554, "lon_deg": -74.01417, "radius_km": 0,
     "noaa_station_id": "8518750", "noaa_station_name": "The Battery",
     "source_url": "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/8518750.json"},
    {"target_id": "savannah_approach", "lat_deg": 32.034695, "lon_deg": -80.90303, "radius_km": 0,
     "noaa_station_id": "8670870", "noaa_station_name": "Fort Pulaski",
     "source_url": "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/8670870.json"},
    {"target_id": "houston_ship_channel", "lat_deg": 29.681667, "lon_deg": -94.985, "radius_km": 0,
     "noaa_station_id": "8770613", "noaa_station_name": "Morgans Point, Barbours Cut",
     "source_url": "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/8770613.json"},
)


def sar_tles_by_source():
    return {sid: tle for sid, tle in base.tles_by_source().items() if sid >= 24}


def selection():
    if len(COASTAL_TARGETS) != 5:
        raise ValueError("Five documented coastal target coordinates are required before propagation")
    spec = base.selection()
    del spec["optical"]
    spec["scope"] = "SAR-only coastal geometry development diagnostic; no scheduler, delivery result, or formal simulation config"
    spec["targets"] = [dict(target) for target in COASTAL_TARGETS]
    spec["target_scope"] = "Five documented NOAA tide-station coordinates representing civilian US coastal harbor reference points; radius=0, not full harbor coverage"
    spec["application"] = {
        "task": "Civilian vessel detection in a formed and ready SAR image",
        "target_presence": "Target coordinates designate hypothetical observation requests; no claim that vessels were present or images acquired on this date",
        "sla_origin": "P300 starts when the formed SAR image is ready for tiled inference; ends when the full image result is aggregated on the ground",
        "image_formation": "Radar echo focusing and its uncalibrated ready delay are outside the P300 processing interval",
        "workload_calibration": "No inference model, tile count, compute demand, or actual image product is assigned by this geometry audit",
    }
    spec["node_identity"] = {
        "optical_count": 0,
        "sar_legacy_ids": sorted(sar_tles_by_source()),
        "helper_legacy_ids": list(range(2, 24)),
        "note": "Legacy sparse IDs preserve identical SAR orbits across geometry audits; a formal 34/46/70-node simulation must explicitly remap IDs",
    }
    spec["removal_independence"] = "Capture spacing is per source and each window belongs to one source; removing optical sources cannot change selection for retained SAR sources. Coastal target changes are newly propagated and are not filtered from prior inland events."
    spec["selection_rule"] = "All five coastal points and all three architecture groups fixed before propagation; retain every selected acquisition, including empty clock hours; no ground-contact or scheduling-result filtering"
    return spec


def summarize_groups(events, spec):
    if any(event["sensor"] != "sar" for event in events):
        raise ValueError("SAR-only audit cannot include optical events")
    groups = []
    for count, source_ids in base.source_groups().items():
        selected = [event for event in events if event["source_sat_id"] in source_ids]
        groups.append({
            "id": f"sar{count}", "sar_count": count, "optical_count": 0,
            "total_constellation_satellites": count + spec["helpers"]["count"],
            "source_sat_ids": source_ids, "events": selected,
            "summary": base.summarize_events(selected, source_ids, [t["target_id"] for t in spec["targets"]]),
        })
    return groups


def report_text(report):
    spec = report["selection"]
    lines = [
        "# 美国民用海港点的纯 SAR 全天成像机会诊断", "",
        "开发日期：2026-10-15 UTC，完整 24 小时。仅审计几何机会，不运行任务调度，不计算 P300。", "",
        "该候选架构不含光学星。SAR 为 12 轨道面 × 1/2/4 颗（12/24/48 颗），570 km、97.7°、RAAN 间隔 30°；保留 22 颗计算中继星，合计 34/46/70 颗。沿用前次 SAR 轨道与相位，不根据覆盖结果重新挑选轨道。", "",
        "## 任务与目标", "",
        "业务假设为民用船舶检测。目标是 NOAA 公开潮位站坐标所代表的海港参考点，不表示这些点当时确有船舶，也不表示已经存在对应实拍影像。点位半径为 0，不等于港区面覆盖。", "",
        "P300 从 SAR 图像形成并可分片处理时开始，直到整图识别结果在地面聚合完成；SAR 聚焦不计入该 300 秒。这里的机会时间为采集结束时间，尚未标定到图像就绪时刻的延迟，不能直接充当正式任务到达。", "",
        "| 目标 | NOAA 站号 | 纬度 | 经度 | 坐标来源 |",
        "|---|---|---:|---:|---|",
    ]
    for target in spec["targets"]:
        lines.append(f"| {target['target_id']} | {target['noaa_station_id']} | {target['lat_deg']} | {target['lon_deg']} | [NOAA]({target['source_url']}) |")
    lines += [
        "", "## 几何筛查结果", "",
        "SAR 地面入射角 20°..40°，15 秒采集在每 5 秒样点检查；双侧成像且未约束 squint、姿态、波束及功耗，是乐观角度筛查。每源全部目标共享 30 秒采集起点间隔，每个连续合格目标窗口最多采集 3 次。", "",
        "| SAR 数 | 总星数 | 机会数 | 夜间机会 | 有机会小时 | 最长空窗（分） | 300 秒最多机会 | 小时最低条件 |",
        "|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for group in report["groups"]:
        summary = group["summary"]
        lines.append(f"| {group['sar_count']} | {group['total_constellation_satellites']} | "
                     f"{summary['total_acquisitions']} | {summary['night_acquisitions']} | "
                     f"{summary['nonempty_hours']}/24 | {summary['longest_gap_including_start_end']['duration_s'] / 60:.2f} | "
                     f"{summary['densest_300s']['count']} | {'通过几何筛查' if summary['every_hour_has_acquisition'] else '未通过'} |")
    lines += ["", "## 每小时机会数", "", "| UTC 小时 | 12 SAR | 24 SAR | 48 SAR |", "|---:|---:|---:|---:|"]
    for hour in range(24):
        lines.append(f"| {hour:02d}:00 | " + " | ".join(str(group["summary"]["hourly_counts"][hour]["total"]) for group in report["groups"]) + " |")
    lines += ["", "## 各参考点机会数", "", "| 目标 | 12 SAR | 24 SAR | 48 SAR |", "|---|---:|---:|---:|"]
    for target in spec["targets"]:
        tid = target["target_id"]
        lines.append(f"| {tid} | " + " | ".join(str(group["summary"]["count_by_target"][tid]) for group in report["groups"]) + " |")
    lines += [
        "", "## 解释边界", "",
        "每个固定 UTC 整小时至少一次，是用户要求的最低筛查条件，不代表任意滚动一小时都有机会，不代表每个目标每小时都被观察，也不保证真实载荷操作可行。最长空窗包含日初和日末边界；夜间定义为目标处采集起点的近似太阳几何高度小于 0°。", "",
        "仅传播 SAR 轨迹；没有光学事件或光学节点。22 颗计算中继星仅保留角色元数据，其轨迹、星间连接、地面窗口、任务完成率及算力利用率均未评估。历史 SAR 编号 24..71 和 helper 编号 2..23 不是正式仿真连续节点编号，正式配置必须映射。", "",
        "SAR 2..3.5 GB/图只是原厂商产品规格边界，不是此处每次事件的已测图像体积；尚未给定真实产品、分片数、压缩率、推理成本或聚焦延迟。没有复制光学计算成本，也没有据此生成可运行的任务调度配置。", "",
        "后续正式调度仿真需要标定 SAR 图像产品及识别负载，连续运行完整 24 小时到达并追加 301 秒观测尾段。这里的窗口检测仍为每 5 秒离散采样，样点之间可行性未证明。", "",
        f"轨迹缓存与 Skyfield altaz 抽检最大误差：{report['verification']['cached_elevation_vs_skyfield_max_error_deg']:.3g}°。",
        f"仅传播 {report['verification']['source_trajectories_propagated']} 条 SAR 轨迹，耗时 {report['verification']['geometry_elapsed_s']:.2f} 秒。", "",
        f"参数依据：[ICEYE 成像模式]({base.MODE_SOURCE})；[ICEYE 星座介绍]({base.FLEET_SOURCE})。具体星数、轨道面和采集频率是研究设计值，不是对真实在轨星座的复现。", "",
    ]
    return "\n".join(lines)


def run_audit(out_dir):
    spec = selection()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)
    base.save(out_dir / "selection.json", spec)
    geometry = base.generate_geometry(spec, tle_map=sar_tles_by_source())
    report = {
        "selection_sha256": base.digest(spec),
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "geometry_source_sha256": hashlib.sha256(Path(base.__file__).read_bytes()).hexdigest(),
        "selection": spec, "groups": summarize_groups(geometry["events"], spec),
        "windows": geometry["windows"], "tle_by_source": geometry["tle_by_source"],
        "verification": geometry["verification"],
    }
    base.save(out_dir / "coverage.json", report)
    (out_dir / "REPORT.md").write_text(report_text(report), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=ROOT / "results/sar_coastal_coverage_20261015")
    args = parser.parse_args()
    report = run_audit(args.out_dir)
    print(json.dumps({"out_dir": str(args.out_dir), "groups": [
        {"sar_count": group["sar_count"], **group["summary"]} for group in report["groups"]],
        "verification": report["verification"]}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
