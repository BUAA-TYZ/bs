"""只读审计冻结高分场景的窗口、序列化预算及即时网络可达机会。"""

import argparse
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import sys

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.orbit import build_ground_station, load_satellites, parse_start_time_utc, time_grid
from sim.topology import _visible_from_positions, bounded_edges
from sim.workload import ImageTiling


def intervals(mask, sample_s, horizon_s):
    mask = np.asarray(mask, dtype=bool)
    edges = np.flatnonzero(np.diff(np.r_[False, mask, False]))
    return [dict(start_s=float(a * sample_s), end_s=float(min(b * sample_s, horizon_s)),
                 duration_s=float(min(b * sample_s, horizon_s) - a * sample_s),
                 left_censored=bool(a == 0), right_censored=bool(b == len(mask)))
            for a, b in zip(edges[::2], edges[1::2])]


def distribution(values):
    if not values:
        return {"count": 0, "min": None, "p10": None, "median": None, "p90": None, "p95": None, "max": None}
    return dict(count=len(values), min=float(np.min(values)), p10=float(np.percentile(values, 10)),
                median=float(np.median(values)), p90=float(np.percentile(values, 90)),
                p95=float(np.percentile(values, 95)), max=float(np.max(values)))


def contact_summary(windows, sample_s, serialization_seconds):
    complete = [window for window in windows if not window["left_censored"] and not window["right_censored"]]
    fit = {}
    for name, seconds in serialization_seconds.items():
        fit[name] = {
            "serialization_s": seconds,
            "complete_window_fraction": (sum(w["duration_s"] >= seconds for w in complete) / len(complete)
                                         if complete else None),
            "fraction_after_two_sample_margin": (
                sum(max(0, w["duration_s"] - 2 * sample_s) >= seconds for w in complete) / len(complete)
                if complete else None),
        }
    return {"all_observed_duration_s": distribution([w["duration_s"] for w in windows]),
            "complete_duration_s": distribution([w["duration_s"] for w in complete]),
            "left_censored_count": sum(w["left_censored"] for w in windows),
            "right_censored_count": sum(w["right_censored"] for w in windows),
            "fit_if_entire_window_uncontended": fit}


def first_opportunity(mask, release_s, sample_s, horizon_s):
    # 仿真缓存把采样状态保持到下次更新，任务在当前窗口内到达时等待为零。
    index = int(release_s // sample_s)
    future = np.flatnonzero(mask[index:])
    if not len(future):
        return None
    when = max(release_s, float((index + future[0]) * sample_s))
    return when if when < horizon_s else None


def audit(path):
    raw = path.read_bytes()
    cfg = json.loads(raw)
    topo = cfg["topology"]
    dt = cfg["dt"]
    sample_s = cfg["topology_update_steps"] * dt
    if sample_s != cfg["ground_visibility_update_steps"] * dt:
        raise ValueError("This frozen-case audit requires identical ISL and ground cache sampling")
    if (topo["bandwidth_mbps_min"] != topo["bandwidth_mbps_max"]
            or topo.get("bandwidth_noise", 0) or topo.get("bandwidth_distance_scale_km", 0)):
        raise ValueError("This capacity audit requires the frozen constant-rate links")
    horizon_s = cfg["sim_steps"] * dt
    offsets = np.arange(0, horizon_s, sample_s)
    weights = np.minimum(sample_s, horizon_s - offsets)
    t0 = parse_start_time_utc(topo["start_time_utc"])
    times = time_grid(t0, offsets)
    satellites = load_satellites(topo["tle_lines"][:cfg["num_sats"]])
    positions = np.stack([sat.at(times).position.km.T for sat in satellites])
    pairs = [(i, j) for i in range(len(satellites)) for j in range(i + 1, len(satellites))]
    left, right = np.asarray(pairs).T
    a, b = positions[left], positions[right]
    differences = b - a
    distances = np.linalg.norm(differences, axis=2)
    ra, rb = np.linalg.norm(a, axis=2), np.linalg.norm(b, axis=2)
    earth_radius = topo.get("earth_radius_km", 6378.137)
    separation = np.arccos(np.clip(np.sum(a * b, axis=2) / (ra * rb), -1, 1))
    horizon_angles = np.arccos(np.clip(earth_radius / ra, -1, 1)) + np.arccos(np.clip(earth_radius / rb, -1, 1))
    geometric = (ra > earth_radius) & (rb > earth_radius) & (separation <= horizon_angles)
    if topo.get("max_range_km", 0) > 0:
        geometric &= distances <= topo["max_range_km"]
    min_elev = topo.get("min_elevation_deg", 0)
    if min_elev > 0:
        elev_a = np.rad2deg(np.arcsin(np.clip(np.sum(-a * differences, axis=2) / (ra * distances), -1, 1)))
        elev_b = np.rad2deg(np.arcsin(np.clip(np.sum(b * differences, axis=2) / (rb * distances), -1, 1)))
        geometric &= (elev_a >= min_elev) & (elev_b >= min_elev)
    n = len(satellites)
    adjacency = np.zeros((len(offsets), n, n), dtype=bool)
    for index in range(len(offsets)):
        snapshots = list(zip(geometric[:, index], distances[:, index]))
        for i, j in bounded_edges(pairs, snapshots, topo.get("max_neighbors", 0)):
            adjacency[index, i, j] = adjacency[index, j, i] = True
    # 用实际标量几何函数抽检向量化审计，避免另起一套链路选择规则。
    checked_indices = sorted(set(np.linspace(0, len(offsets) - 1, min(9, len(offsets)), dtype=int)))
    for index in checked_indices:
        scalar = [_visible_from_positions(tuple(positions[i, index]), tuple(positions[j, index]),
                                         earth_radius, min_elev, topo.get("max_range_km", 0)) for i, j in pairs]
        expected = bounded_edges(pairs, scalar, topo.get("max_neighbors", 0))
        actual = {(i, j) for i, j in pairs if adjacency[index, i, j]}
        if expected != actual:
            raise AssertionError(f"Vectorized snapshot differs at {offsets[index]} seconds")
    degree = adjacency.sum(axis=2)
    if topo.get("max_neighbors", 0) and int(degree.max()) > topo["max_neighbors"]:
        raise AssertionError("Terminal degree bound violated")
    isl_windows = []
    for i, j in pairs:
        isl_windows.extend(dict(w, source_sat_id=i, peer_sat_id=j)
                           for w in intervals(adjacency[:, i, j], sample_s, horizon_s))
    workload = ImageTiling.from_dict(cfg["image_tiling"])
    manifest = workload.manifest()
    largest_packet_mb = max(t.data_size_mb for t in workload.tiles())
    packet_image_mb = manifest["packet_input_mb"]
    isl_rate = topo["bandwidth_mbps_max"]
    serialization = {"one_max_packet": largest_packet_mb * 8 / isl_rate,
                     "all_image_packets": packet_image_mb * 8 / isl_rate,
                     "num_tiles_times_max_packet_upper_bound": cfg["num_tiles"] * largest_packet_mb * 8 / isl_rate}
    isl_summary = contact_summary(isl_windows, sample_s, serialization)
    any_ground = np.zeros((len(offsets), n), dtype=bool)
    station_reports = []
    for station_cfg in cfg["ground_stations"]:
        station = build_ground_station(station_cfg["lat_deg"], station_cfg["lon_deg"], station_cfg.get("alt_m", 0))
        visible = np.stack([(sat - station).at(times).altaz()[0].degrees >= station_cfg["min_elevation_deg"]
                            for sat in satellites], axis=1)
        any_ground |= visible
        station_windows = []
        rate = station_cfg["bandwidth_mbps"]
        for sid in range(n):
            station_windows.extend(dict(w, sat_id=sid, gs_id=station_cfg["id"],
                                        uncontended_capacity_mb=w["duration_s"] * rate / 8)
                                   for w in intervals(visible[:, sid], sample_s, horizon_s))
        active_counts = visible.sum(axis=1)
        channels = station_cfg.get("receive_channels", 1)
        channel_capacity = float(np.sum(np.minimum(channels, active_counts) * weights) * rate / 8)
        metrics = contact_summary(station_windows, sample_s, {
            "one_max_packet": largest_packet_mb * 8 / rate,
            "all_image_packets": packet_image_mb * 8 / rate,
            "whole_unique_image": workload.image_size_mb * 8 / rate,
        })
        station_reports.append({"station": station_cfg, "summary": metrics, "windows": station_windows,
                                "visibility_union_windows": intervals(active_counts > 0, sample_s, horizon_s),
                                "visibility_union_s": float(np.sum(weights[active_counts > 0])),
                                "receive_channel_capacity_upper_bound_mb": channel_capacity,
                                "sum_individual_link_window_capacity_mb": sum(w["uncontended_capacity_mb"] for w in station_windows),
                                "simultaneously_visible_satellites_max": int(active_counts.max()),
                                "first_direct_contact_s_by_satellite": {
                                    str(sid): first_opportunity(visible[:, sid], 0, sample_s, horizon_s) for sid in range(n)}})
    # 每个时刻独立求到可见地面站卫星的最短跳数，不使用未来时间扩展路径。
    min_hops = np.full((len(offsets), n), n + 1, dtype=np.int16)
    reached = any_ground.copy()
    min_hops[reached] = 0
    for hops in range(1, n):
        expanded = np.any(adjacency & reached[:, None, :], axis=2) | reached
        newly = expanded & ~reached
        min_hops[newly] = hops
        reached = expanded
        if not newly.any():
            break
    relay_limit = cfg.get("relay_max_hops", 8)
    limited_reach = min_hops <= relay_limit
    per_satellite = [{"sat_id": sid,
                      "first_any_isl_contact_s": first_opportunity(degree[:, sid] > 0, 0, sample_s, horizon_s),
                      "first_direct_ground_contact_s": first_opportunity(any_ground[:, sid], 0, sample_s, horizon_s),
                      "first_current_network_ground_reach_s": first_opportunity(reached[:, sid], 0, sample_s, horizon_s),
                      "first_current_network_ground_reach_with_hop_limit_s": first_opportunity(limited_reach[:, sid], 0, sample_s, horizon_s),
                      "zero_neighbor_fraction": float(np.sum(weights[degree[:, sid] == 0]) / horizon_s),
                      "direct_ground_visible_fraction": float(np.sum(weights[any_ground[:, sid]]) / horizon_s),
                      "current_network_ground_reach_fraction": float(np.sum(weights[reached[:, sid]]) / horizon_s)}
                     for sid in range(n)]
    # 额外查看源星下一次直达机会，不扩展实验窗口/容量统计或引入新任务。
    future_horizon_s = horizon_s + 86400
    future_offsets = np.arange(0, future_horizon_s, sample_s)
    future_times = time_grid(t0, future_offsets)
    future_source_ground = {}
    for sid in cfg["task_source_sat_ids"]:
        visible = np.zeros(len(future_offsets), dtype=bool)
        for station_cfg in cfg["ground_stations"]:
            station = build_ground_station(station_cfg["lat_deg"], station_cfg["lon_deg"], station_cfg.get("alt_m", 0))
            visible |= (satellites[sid] - station).at(future_times).altaz()[0].degrees >= station_cfg["min_elevation_deg"]
        future_source_ground[sid] = visible
    acquisitions = []
    for event in cfg["scheduled_task_arrivals"]:
        release_s = event["release_step"] * dt
        sid = event["source_sat_id"]
        direct = first_opportunity(any_ground[:, sid], release_s, sample_s, horizon_s)
        connected = first_opportunity(reached[:, sid], release_s, sample_s, horizon_s)
        bounded = first_opportunity(limited_reach[:, sid], release_s, sample_s, horizon_s)
        future_direct = first_opportunity(future_source_ground[sid], release_s, sample_s, future_horizon_s)
        record = {"release_step": event["release_step"], "release_s": release_s, "source_sat_id": sid,
                  "target_id": event["target_id"],
                  "next_direct_ground_contact_s": direct,
                  "direct_wait_s": direct - release_s if direct is not None else None,
                  "first_current_network_ground_reach_s": connected,
                  "current_network_wait_s": connected - release_s if connected is not None else None,
                  "first_current_network_ground_reach_with_hop_limit_s": bounded,
                  "hop_limited_network_wait_s": bounded - release_s if bounded is not None else None,
                  "extended_geometry_next_direct_contact_s": future_direct,
                  "extended_geometry_direct_wait_s": future_direct - release_s if future_direct is not None else None,
                  "hops_at_first_bounded_opportunity": int(min_hops[int(bounded // sample_s), sid]) if bounded is not None else None}
        acquisitions.append(record)
    return {
        "config_path": str(path), "config_sha256": hashlib.sha256(raw).hexdigest(),
        "start_time_utc": t0.isoformat(), "end_time_utc": (t0 + timedelta(seconds=horizon_s)).isoformat(),
        "timeline_s": horizon_s, "sample_s": sample_s, "num_samples": len(offsets),
        "snapshot_equivalence_validation": {"scalar_checks": len(checked_indices), "all_passed": True},
        "sampling_and_scope": [
            "Same 10-second sample-and-hold visibility and distance-ranked bilateral terminal matching as frozen simulation",
            "Boundary times are sampled; each resolved crossing can shift by up to one sample and sub-sample contacts can be missed",
            "Full-window fit assumes bytes ready at contact start, no competing traffic and constant nominal bandwidth",
            "Two-sample subtraction is a conservative sampling margin, not a terminal acquisition-time model",
            "No acquisition/tracking/pointing setup delay, packet framing, retransmission or weather model",
            "Instantaneous graph reach is an opportunity bound, not proof a packet can traverse before topology changes",
            "Snapshot reach does not use store-carry-forward temporal paths; lack of snapshot path is not proof delivery is impossible",
            "Station union capacity honors receive-channel count; individual visibility capacities must not be summed as delivered service",
            "No contact observed within the episode does not imply the satellite never passes the station; a separate source-only +24h audit is included",
        ],
        "workload": {"unique_image_mb": workload.image_size_mb, "num_packets": cfg["num_tiles"],
                     "all_packet_input_mb": packet_image_mb, "maximum_packet_mb": largest_packet_mb,
                     "compression_ratio_assumption": workload.transport_compression_ratio},
        "isl": {"nominal_mbps": isl_rate, "max_range_km": topo["max_range_km"],
                "max_neighbors": topo["max_neighbors"], "observed_max_degree": int(degree.max()),
                "summary": isl_summary, "windows": isl_windows},
        "ground_stations": station_reports, "relay_max_hops": relay_limit,
        "satellites": per_satellite, "acquisitions": acquisitions,
        "extended_source_direct_geometry": {
            "end_s": future_horizon_s, "extension_after_episode_s": 86400,
            "included_in_episode_statistics": False,
            "first_direct_contact_s_by_source": {
                str(sid): first_opportunity(visible, 0, sample_s, future_horizon_s)
                for sid, visible in future_source_ground.items()},
        },
        "acquisition_wait_summary_s": {
            key: {**distribution([a[key] for a in acquisitions if a[key] is not None]),
                  "no_opportunity_within_audit_horizon_count": sum(a[key] is None for a in acquisitions)}
            for key in ("direct_wait_s", "current_network_wait_s", "hop_limited_network_wait_s", "extended_geometry_direct_wait_s")},
    }


def markdown(report):
    def seconds(value):
        return "未观测到" if value is None else f"{value:.0f} s"

    isl = report["isl"]["summary"]
    rate = report["isl"]["nominal_mbps"]
    workload = report["workload"]
    lines = ["# 冻结高分场景通信窗口审计", "",
             f"配置：`{report['config_path']}`，SHA256 `{report['config_sha256']}`。",
             f"时段：{report['start_time_utc']} 至 {report['end_time_utc']}，共 {report['timeline_s']:.0f} 秒，{report['sample_s']:.0f} 秒采样。", "",
             f"每幅图 {workload['num_packets']} 个调度包，总计 {workload['all_packet_input_mb']:.6f} MB；最大包 {workload['maximum_packet_mb']:.6f} MB。",
             f"星间 {rate:.0f} Mbps 下，最大包纯序列化 {workload['maximum_packet_mb'] * 8 / rate * 1000:.3f} ms，整幅分片合计 {workload['all_packet_input_mb'] * 8 / rate:.3f} s。", "",
             "| 链路 | 完整窗口数 | 最短(s) | 中位数(s) | P95(s) | 单包可容纳比例 | 整幅分片可容纳比例 |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    rows = [("星间", isl)] + [(g["station"]["id"], g["summary"]) for g in report["ground_stations"]]
    for name, summary in rows:
        stats, fits = summary["complete_duration_s"], summary["fit_if_entire_window_uncontended"]
        lines.append(f"| {name} | {stats['count']} | {stats['min']:.0f} | {stats['median']:.0f} | {stats['p95']:.0f} | "
                     f"{fits['one_max_packet']['complete_window_fraction']:.1%} | {fits['all_image_packets']['complete_window_fraction']:.1%} |")
    lines += ["", f"星间窗口另有左截断 {isl['left_censored_count']} 个、右截断 {isl['right_censored_count']} 个，不并入完整窗口分布。"]
    for station in report["ground_stations"]:
        cfg = station["station"]
        lines += [f"地面站 {cfg['id']}：最大包纯序列化 {workload['maximum_packet_mb'] * 8 / cfg['bandwidth_mbps'] * 1000:.3f} ms；"
                  f"唯一原图 {workload['unique_image_mb'] * 8 / cfg['bandwidth_mbps']:.3f} s；全部重叠分片 {workload['all_packet_input_mb'] * 8 / cfg['bandwidth_mbps']:.3f} s。",
                  f"可见时间并集 {station['visibility_union_s']:.0f} s，按 {cfg.get('receive_channels', 1)} 个接收通道的理想容量上界 "
                  f"{station['receive_channel_capacity_upper_bound_mb'] / 1000:.3f} GB；同时可见最多 {station['simultaneously_visible_satellites_max']} 颗星。"]
    waits = report["acquisition_wait_summary_s"]
    lines += ["", f"{len(report['acquisitions'])} 次成像：源星直达地面机会的中位等待 {seconds(waits['direct_wait_s']['median'])}；"
              f"经当前星间图且至多 {report['relay_max_hops']} 跳可达地面机会的中位等待 "
              f"{seconds(waits['hop_limited_network_wait_s']['median'])}。逐星首窗口、逐次成像机会保存在 JSON。",
              f"源星直接窗口在实验结束后另延长24小时查找，中位等待 {seconds(waits['extended_geometry_direct_wait_s']['median'])}；"
              "这些后续机会不计入本实验的窗口或容量统计。", "",
              "这些是几何和空载名义带宽审计，不是实际交付时延。可容纳比例假设窗口开始时数据已备齐，未计竞争和建链。",
              "采样分辨率为10秒；窗口边界可能偏移，短窗口可能漏检。JSON另给出扣除两个采样间隔后的保守比例。",
              "当前模型未包含真实激光链路捕获、指向、跟踪、帧开销和天气；瞬时多跳可达也不能保证转发期间路径持续存在。", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="results/highres_prepared_20261015/day01_config.json")
    parser.add_argument("--output", default="results/highres_contact_audit_20261015.json")
    args = parser.parse_args()
    result = audit(Path(args.config))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    output.with_suffix(".md").write_text(markdown(result), encoding="utf-8")
    print(json.dumps({"output": str(output), "markdown": str(output.with_suffix('.md')),
                      "isl_complete": result["isl"]["summary"]["complete_duration_s"],
                      "acquisition_wait_summary_s": result["acquisition_wait_summary_s"]}, indent=2))


if __name__ == "__main__":
    main()
