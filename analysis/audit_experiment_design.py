"""审计归档场景参数及采样覆盖；仅重建几何，不运行任务、调度或训练。"""

import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sim.orbit import build_ground_station, load_satellites, parse_start_time_utc, time_grid


PANELS = (
    ("D1", "固定开发组", "literature_existing_20261014"),
    ("D2", "分布内采样开发组", "literature_fresh_20261014"),
    ("C1", "机制确认组（分布内采样）", "ground_opportunity_confirmation_20261014"),
)
TRAINING_RUN = "checkpoints/relay_greedy_dagger_mappo_7_20261010/run.json"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def no_pipeline(config):
    return {k: v for k, v in config.items() if k != "pipeline_mode"}


def task_inputs(record):
    return [{k: task[k] for k in ("task_id", "source_sat_id", "release_seconds", "deadline_seconds")}
            for task in record["metrics"]["task_outcomes"]]


def summary(values):
    if not values:
        return {"count": 0, "min": None, "max": None, "mean": None, "median": None, "p90": None}
    return {"count": len(values), "min": min(values), "max": max(values),
            "mean": statistics.mean(values), "median": statistics.median(values),
            "p90": float(np.quantile(values, .9))}


def windows(mask, dt):
    edges = np.diff(np.r_[False, mask, False].astype(np.int8))
    starts, ends = np.flatnonzero(edges == 1), np.flatnonzero(edges == -1)
    return [[float(a * dt), float(b * dt)] for a, b in zip(starts, ends)]


def geometry(config):
    """按归档的执行采样时刻重建可见性，窗口仅是离散网格区间。"""
    topo, dt, steps = config["topology"], config["dt"], config["sim_steps"]
    sats = load_satellites(topo["tle_lines"])
    assert len(sats) == config["num_sats"]
    t0 = parse_start_time_utc(topo["start_time_utc"])
    sampled_steps = (np.arange(steps) // config["ground_visibility_update_steps"]
                     * config["ground_visibility_update_steps"])
    times = time_grid(t0, sampled_steps * dt)
    visible = np.empty((len(sats), len(config["ground_stations"]), steps), dtype=bool)
    stations = []
    for gi, station in enumerate(config["ground_stations"]):
        observer = build_ground_station(station["lat_deg"], station["lon_deg"], station["alt_m"])
        for sid, satellite in enumerate(sats):
            elevations = (satellite - observer).at(times).altaz()[0].degrees
            visible[sid, gi] = elevations >= station["min_elevation_deg"]
        counts = visible[:, gi].sum(axis=0)
        stations.append({"id": station["id"], "any_satellite_visible_seconds": float(np.count_nonzero(counts) * dt),
                         "satellite_pair_visible_seconds": float(counts.sum() * dt),
                         "over_channel_count_seconds": float(np.count_nonzero(counts > station["receive_channels"]) * dt),
                         "max_simultaneously_visible_satellites": int(counts.max()),
                         "clipped_window_count": sum(len(windows(visible[sid, gi], dt)) for sid in range(len(sats)))})
    parsed = []
    for sid, satellite in enumerate(sats):
        model = satellite.model
        # 与合成星座生成器同用 WGS72 常数，仅反解输入平均运动，不是瞬时高度。
        altitude = (398600.8 / (model.no_kozai / 60) ** 2) ** (1 / 3) - 6378.135
        parsed.append({"sat_id": sid, "inclination_deg": math.degrees(model.inclo),
                       "raan_deg": math.degrees(model.nodeo), "mean_anomaly_deg": math.degrees(model.mo),
                       "mean_motion_rev_per_day": model.no_kozai * 1440 / (2 * math.pi),
                       "nominal_altitude_from_mean_motion_km": altitude})
    return visible, stations, parsed


def contact_metrics(config, tasks, visible, stations):
    dt = config["dt"]
    sources = config["task_source_sat_ids"]
    per_sat = []
    for sid in range(config["num_sats"]):
        any_ground = visible[sid].any(axis=0)
        per_sat.append({"sat_id": sid, "source": sid in sources,
                        "any_ground_visible_seconds": float(any_ground.sum() * dt),
                        "by_station_windows_seconds": {
                            station["id"]: windows(visible[sid, gi], dt)
                            for gi, station in enumerate(config["ground_stations"])}})
    waits, missing = [], []
    for task in tasks:
        index = int(round(task["release_seconds"] / dt))
        later = np.flatnonzero(visible[task["source_sat_id"], :, index:].any(axis=0))
        if len(later):
            waits.append(float(later[0] * dt))
        else:
            missing.append(task["task_id"])
    return {"basis": "archived TLE and effective configuration; geometry reconstructed only; discrete step starts",
            "sample_dt_seconds": dt, "observation_seconds": config["sim_steps"] * dt,
            "boundary_note": "first/last windows are clipped at the six-hour observation boundary",
            "satellite_station_visible_seconds": float(visible.sum() * dt),
            "source_station_visible_seconds": float(visible[sources].sum() * dt),
            "mean_source_any_ground_visible_seconds": float(visible[sources].any(axis=1).sum(axis=1).mean() * dt),
            "sources_without_contact": [r["sat_id"] for r in per_sat if r["source"] and r["any_ground_visible_seconds"] == 0],
            "task_source_next_contact_wait_seconds_conditional": summary(waits),
            "tasks_without_source_contact_before_end": missing,
            "stations": stations, "satellites": per_sat}


def parameters(case, cfg, parsed):
    sources = cfg["task_source_sat_ids"]
    rates = cfg["satellite_compute_rates"]
    source_rates = sorted(set(rates[sid] for sid in sources))
    helper_rates = sorted(set(rates[sid] for sid in range(cfg["num_sats"]) if sid not in sources))
    assert len(source_rates) == len(helper_rates) == 1
    topo, orbit = cfg["topology"], case["walker"]
    assert abs(parsed[0]["inclination_deg"] - orbit["inclination_deg"]) < .0001
    assert abs(parsed[0]["nominal_altitude_from_mean_motion_km"] - orbit["altitude_km"]) < .001
    assert len(set(round(s["raan_deg"], 4) for s in parsed)) == orbit["planes"]
    return {"num_sats": cfg["num_sats"], "role_profile": case["role_profile"], "source_count": len(sources),
            "helper_count": cfg["num_sats"] - len(sources), "source_sat_ids": sources,
            "planes": orbit["planes"], "satellites_per_plane": orbit["satellites_per_plane"],
            "phasing": orbit["phasing"], **{k: orbit[k] for k in
                ("altitude_km", "inclination_deg", "raan_offset_deg", "phase_offset_deg")},
            "orbit_offset_seconds": case["orbit_offset_seconds"], "start_time_utc": topo["start_time_utc"],
            "base_arrival_rate_per_second": case["arrival_rate_per_second"],
            "effective_arrival_rate_per_second": cfg["task_arrival_rate"] / cfg["dt"],
            "per_source_arrival_rate_per_second": cfg["task_arrival_rate"] / cfg["dt"] / len(sources),
            "source_compute_rate": source_rates[0], "helper_compute_rate": helper_rates[0],
            "total_satellite_compute_rate": sum(rates), "compute_cost_per_tile": cfg["compute_cost_per_tile"],
            "source_unqueued_tile_compute_seconds": cfg["compute_cost_per_tile"] / source_rates[0],
            "helper_unqueued_tile_compute_seconds": cfg["compute_cost_per_tile"] / helper_rates[0],
            "ground_compute_rates": {s["id"]: s["compute_rate"] for s in cfg["ground_stations"]},
            "isl_bandwidth_scale": case["isl_bandwidth_scale"], "ground_bandwidth_scale": case["ground_bandwidth_scale"],
            "isl_min_mbps_before_distance": topo["bandwidth_mbps_min"],
            "isl_max_mbps_before_distance": topo["bandwidth_mbps_max"], "isl_noise_mbps": topo["bandwidth_noise"],
            "ground_mbps_per_station": {s["id"]: s["bandwidth_mbps"] for s in cfg["ground_stations"]},
            "ground_receive_channels": {s["id"]: s["receive_channels"] for s in cfg["ground_stations"]},
            "mem_capacity_gb": cfg["mem_capacity_gb"], "vram_capacity_gb": cfg["vram_capacity_gb"],
            "deadline_steps": cfg["deadline_steps"], "image_size_mb": cfg["image_size_mb"],
            "num_tiles": cfg["num_tiles"], "result_size_mb_per_tile": cfg["result_size_mb"],
            "dt_seconds": cfg["dt"], "duration_seconds": cfg["sim_steps"] * cfg["dt"],
            "arrivals_duration_seconds": cfg["task_arrival_end_step"] * cfg["dt"],
            "prediction": {k: topo[k] for k in ("predict_contacts", "contact_scan_s", "contact_update_s", "window_lookahead_s")},
            "max_neighbors": topo["max_neighbors"], "max_isl_range_km": topo["max_range_km"]}


def support_checks(case, spec):
    outside = []
    for key, bounds in spec["training"].items():
        if not bounds[0] <= case[key] <= bounds[1]:
            outside.append(key)
    for key in ("altitude_km", "inclination_deg", "raan_offset_deg", "phase_offset_deg"):
        if not spec["walker"][key][0] <= case["walker"][key] <= spec["walker"][key][1]:
            outside.append("walker." + key)
    layout = {k: case["walker"][k] for k in ("planes", "satellites_per_plane", "phasing")}
    if layout not in spec["walker"]["layouts"]:
        outside.append("walker.layout")
    if case["role_profile"] not in [p["id"] for p in spec["roles"]["profiles"]]:
        outside.append("role_profile")
    return outside


def unique_values(rows, field):
    def nominal(value):
        if isinstance(value, float):
            return round(value, 12)
        if isinstance(value, dict):
            return {key: nominal(item) for key, item in value.items()}
        if isinstance(value, list):
            return [nominal(item) for item in value]
        return value

    # 0.7 与算术产生的 0.7000000000000001 不算两个实验水平，原始精度仍保留在案例记录。
    encoded = {json.dumps(nominal(row["parameters"][field]), sort_keys=True, ensure_ascii=False) for row in rows}
    return [json.loads(value) for value in sorted(encoded)]


def audit(repo):
    training_path = repo / TRAINING_RUN
    training = load_json(training_path)
    spec = training["scenario_distribution"]["spec"]
    assert digest(spec) == training["scenario_distribution"]["sha256"]
    artifact_hashes = {TRAINING_RUN: sha(training_path)}
    rows, panels, geo_cache = [], [], {}
    for panel_id, panel_name, folder in PANELS:
        directory = repo / "results" / folder
        manifest = load_json(directory / "summary.json")
        artifact_hashes[str((directory / "summary.json").relative_to(repo))] = sha(directory / "summary.json")
        assert manifest["distribution_sha256"] == digest(spec)
        grouped = defaultdict(list)
        for name in manifest["records"]:
            path = directory / name
            record = load_json(path)
            grouped[record["case"]["id"]].append((record, path))
            artifact_hashes[str(path.relative_to(repo))] = sha(path)
        for number, case in enumerate(manifest["cases"], 1):
            group = grouped[case["id"]]
            methods = manifest.get("methods", manifest["args"].get("methods"))
            assert len(group) == len(methods)
            assert {record["method"] for record, _ in group} == set(methods)
            primary, path = next((record, path) for record, path in group if record["method"] == "cgs_compute_adapted")
            exp = primary["metrics"]["experiment"]
            cfg = exp["config"]
            tasks = task_inputs(primary)
            assert primary["case"] == case
            assert cfg["seed"] == case["seed"]
            assert exp["source_sha256"] == manifest["source_sha256"]
            assert len(tasks) == primary["metrics"]["overall"]["total_tasks"]
            for record, _ in group:
                other = record["metrics"]["experiment"]
                assert no_pipeline(cfg) == no_pipeline(other["config"])
                assert task_inputs(record) == tasks
                assert all(other[key] == exp[key] for key in ("task_trace_sha256", "tle_sha256", "source_sha256"))
                assert record["case"] == case
            fingerprint = digest({"topology": {k: cfg["topology"][k] for k in ("tle_lines", "start_time_utc")}, "stations": [
                {k: s[k] for k in ("id", "lat_deg", "lon_deg", "alt_m", "min_elevation_deg", "receive_channels")}
                for s in cfg["ground_stations"]], "dt": cfg["dt"], "steps": cfg["sim_steps"],
                "ground_visibility_update_steps": cfg["ground_visibility_update_steps"]})
            if fingerprint not in geo_cache:
                geo_cache[fingerprint] = geometry(cfg)
            visible, stations, parsed = geo_cache[fingerprint]
            params = parameters(case, cfg, parsed)
            contact = contact_metrics(cfg, tasks, visible, stations)
            network = primary["metrics"]["network"]
            rows.append({"panel": panel_id, "label": f"{panel_id}-{number:02d}", "case_id": case["id"],
                         "seed": cfg["seed"], "archive": str(path.relative_to(repo)),
                         "paired_methods_checked": sorted(r["method"] for r, _ in group),
                         "parameters": params, "effective_config": cfg, "case_metadata": case,
                         "parsed_saved_tle": parsed, "geometry_fingerprint": fingerprint,
                         "config_without_seed_or_pipeline_sha256": digest({k: v for k, v in no_pipeline(cfg).items() if k != "seed"}),
                         "task_count": len(tasks), "tasks": tasks,
                         "task_counts_by_source": dict(Counter(str(t["source_sat_id"]) for t in tasks)),
                         "expected_task_count": cfg["task_arrival_rate"] * cfg["task_arrival_end_step"],
                         "trace_sha256": exp["task_trace_sha256"], "tle_sha256": exp["tle_sha256"],
                         "source_sha256": exp["source_sha256"], "outside_training_support_fields": support_checks(case, spec),
                         "contacts": contact,
                         "archived_network": {"isl_directed_capacity_mb_sum": sum(v for k, v in network["capacity_mb"].items() if k.startswith("isl:")),
                                              "ground_nominal_capacity_mb": {k: v for k, v in network["capacity_mb"].items() if k.startswith("ground:")},
                                              "note": "ground capacity includes non-visible timesteps; not contact capacity; ISL sum double-counts directions"}})
            print(f"{panel_id} {case['id']}: {len(tasks)} tasks; geometry checked", flush=True)
        panel_rows = [row for row in rows if row["panel"] == panel_id]
        coverage = Counter(f"{r['parameters']['num_sats']}/{r['parameters']['role_profile']}" for r in panel_rows)
        panels.append({"id": panel_id, "name": panel_name, "archive": str(directory.relative_to(repo)),
                       "cases": len(panel_rows), "method_runs": len(manifest["records"]),
                       "total_tasks": sum(r["task_count"] for r in panel_rows),
                       "unique_configs_without_seed": len({r["config_without_seed_or_pipeline_sha256"] for r in panel_rows}),
                       "unique_geometry": len({r["geometry_fingerprint"] for r in panel_rows}),
                       "layout_role_coverage": dict(sorted(coverage.items())),
                       "source_sha256": manifest["source_sha256"],
                       "variation": {key: unique_values(panel_rows, key) for key in panel_rows[0]["parameters"]}})
    fixed_keys = ("sim_steps", "dt", "task_arrival_end_step", "decision_interval_steps", "topology_update_steps",
                  "ground_visibility_update_steps", "image_size_mb", "num_tiles", "result_size_mb", "deadline_steps",
                  "mem_capacity_gb", "vram_capacity_gb", "vram_base_gb", "vram_alpha_per_mb", "vram_policy",
                  "ground_relay_delay_s", "aggregation_delay_s", "result_buffering", "result_relay",
                  "relay_max_hops", "relay_contact_guard", "execution_commitment")
    fixed = {key: rows[0]["effective_config"][key] for key in fixed_keys}
    assert all(all(row["effective_config"][key] == value for key, value in fixed.items()) for row in rows)
    assert all(task["deadline_seconds"] is None for row in rows for task in row["tasks"])
    return {"schema_version": 1, "scope": "read-only archive audit and geometric reconstruction; no scheduling or training run",
            "training_run": TRAINING_RUN, "training_distribution": spec,
            "training_selection": training["selection"], "training_validation_seeds": training["validation_seeds"],
            "panels": panels, "cases": rows, "fixed_config_fields": fixed, "input_sha256": artifact_hashes,
            "audit_source_sha256": {name: sha(repo / name) for name in (
                "analysis/audit_experiment_design.py", "sim/orbit.py", "sim/marl/scenarios.py",
                "sim/topology.py", "sim/network.py", "sim/env.py", "sim/constellation.py")}}


def f(value, digits=3):
    if not isinstance(value, float):
        return str(value)
    formatted = f"{value:.{digits}f}"
    return formatted.rstrip("0").rstrip(".") if "." in formatted else formatted


def render_markdown(data):
    lines = ["# 实验设计与场景参数审计", "", "本表由 `analysis/audit_experiment_design.py` 从已保存的有效配置、TLE、任务结果和训练分布记录生成；不以当前默认 YAML 代替运行配置。完整精度、逐星轨道、逐任务来源与全部输入 SHA256 见 `results/experiment_design_audit_20261014/audit.json`。", "",
             "## 分组与证据边界", "", "| 标签 | 用途 | 案例 / 不含种子的不同配置 | 方法运行 | 任务数 |", "|---|---|---:|---:|---:|"]
    for p in data["panels"]:
        lines.append(f"| {p['id']} | {p['name']} | {p['cases']} / {p['unique_configs_without_seed']} | {p['method_runs']} | {p['total_tasks']} |")
    lines += ["", "- D1 是训练过程已使用的固定开发/选择面板：四套参数各两个环境种子，不是八种独立物理场景。训练档案明确记录其八个种子为 validation，并按该面板评分选择模型。",
              "- D2 是另一个种子生成的分布内采样开发面板，九个案例完整覆盖三种规模与三种角色的九种组合，每种组合只有一次连续参数抽样。",
              "- C1 是新增种子下的机制确认面板，使用与 D2 相同采样分布，也只覆盖九种组合各一次；这里只比较三个规划/机制方法，没有 MARL 记录。",
              "- D2、C1 的所有采样参数落在已保存训练分布支持集内。D1 除轨道起始时间偏移在训练区间之外，其余参数也在支持集内；这属于原先的时间段隔离，不能当作未见规模、布局或物理模型的 OOD 证据。",
              "- 三组已用于开发、诊断或方案确认，均不能再称最终独立测试集。旧/新只是执行先后，论文建议统一使用 D1、D2、C1 及上述用途名称。",
              "- D1/D2 同源代码指纹；C1 的代码指纹不同，包含后续候选机制版本。脚本逐个案例验证方法间配置（仅排除 pipeline_mode）、任务输入、轨道与源码指纹一致，但不会把跨版本跨面板成绩当同场景消融。", "", "## 每个案例的规模和轨道", "",
              "轨道高度、倾角和全局相位来自运行档案中的案例参数，并与保存 TLE 的平均根数核对。TLE 的有限小数精度导致细微舍入；完整逐星 RAAN、平均近点角保存在 JSON。RAAN 和相位均为度，时间偏移相对生成器基准历元，单位小时。", "",
              "| 标签 | 原始 case ID | 环境种子 | 星数 / 源星 / 计算星 | 角色 | 轨道面×每面 | 高度 km | 倾角 | RAAN偏移 | 相位 | 起始偏移 h |",
              "|---|---|---:|---:|---|---|---:|---:|---:|---:|---:|"]
    for r in data["cases"]:
        p = r["parameters"]
        lines.append(f"| {r['label']} | `{r['case_id']}` | {r['seed']} | {p['num_sats']}/{p['source_count']}/{p['helper_count']} | {p['role_profile']} | {p['planes']}×{p['satellites_per_plane']} | {f(p['altitude_km'])} | {f(p['inclination_deg'])} | {f(p['raan_offset_deg'])} | {f(p['phase_offset_deg'])} | {f(p['orbit_offset_seconds']/3600)} |")
    lines += ["", "角色不是只改名字：uniform 每组三星相位均匀、一个源星；near 同组沿轨偏移为 0/−2/+2 度、一个源星；shared 沿轨偏移同 near，但前两个成员都是源星。所有面间 phasing=1。源星算力均为 0.1，uniform/near 辅助计算星为 0.7，shared 辅助计算星为 1.3，总星上算力始终为 0.5×星数。源星也保留较低计算能力。", "", "## 每个案例的负载、计算和通信", "",
              "到达率是全星座总任务率，不是单颗星到达率。有效到达率 = 抽样基础到达率 × 源星数 / 8。算力和计算成本为仿真抽象单位；单分片无排队耗时 = 计算成本 / 执行节点算力。ISL 上下界是距离衰减前参数，不是恒定实际速率；执行时还有带宽相位、噪声和距离缩放。两地面站下行速率相同。", "",
              "| 标签 | 实际任务 / 期望 | 基础率 /s | 有效率 /s | tile成本 | 源星/辅助算力 | 源星/辅助单片耗时 s | ISL范围 Mb/s | 下行 Mb/s/站 |",
              "|---|---:|---:|---:|---:|---|---|---|---:|"]
    for r in data["cases"]:
        p = r["parameters"]
        lines.append(f"| {r['label']} | {r['task_count']}/{f(r['expected_task_count'],2)} | {f(p['base_arrival_rate_per_second'],6)} | {f(p['effective_arrival_rate_per_second'],6)} | {f(p['compute_cost_per_tile'])} | {f(p['source_compute_rate'])}/{f(p['helper_compute_rate'])} | {f(p['source_unqueued_tile_compute_seconds'],2)}/{f(p['helper_unqueued_tile_compute_seconds'],2)} | {f(p['isl_min_mbps_before_distance'],2)}–{f(p['isl_max_mbps_before_distance'],2)} | {f(next(iter(p['ground_mbps_per_station'].values())),2)} |")
    lines += ["", "## 固定参数及没有被覆盖的维度", "", "| 维度 | 26 个案例实际共同配置 |", "|---|---|",
              "| 时间协议 | dt=10 s；总时长21600 s；仅前7200 s产生任务；每步决策、链路与地面可见性更新 |",
              "| 图像与任务 | 每图128 MB、8片、每片结果0.5 MB；片大小带2%正态扰动后归一；每片计算成本在同一案例固定 |",
              "| 截止时间 | deadline_steps=0，所有任务硬截止时间为空；30/60/120/240分钟是评价时限，不是执行器超时限制 |",
              "| 存储 | 每星内存8 GB、显存1 GB；显存需求0.1+0.001×片大小MB；显存不足等待 |",
              "| 地面资源 | 北京39.9042°N/116.4074°E/50 m；乌鲁木齐43.8256°N/87.6168°E/800 m；最低仰角均15°；每站1接收通道；算力分别8与7 |",
              "| 星间约束 | 最大距离4500 km；每星最多4邻星；距离带宽尺度2000 km；名义延迟20 ms；周期60个仿真步 |",
              "| 预测 | 开启接触预测；扫描30 s；contact_update_s=60 s；地面预测前视7200 s；实际地面可见性每10 s刷新 |",
              "| 结果服务 | 结果缓存、中继和接触守护均开启；最多8跳；固定地面转发10 s+聚合10 s；执行承诺开启 |",
              "| 信息 | 公共中继使用即时全网链路/队列与预测地面窗口，无控制平面成本；不等于所有学习特征具有相同信息权限 |", "",
              "上述项目没有参数扫描。因此现有结果不足以支持不同缓存容量、地面站数/位置/接收通道、硬截止时间、信息时延、预测误差、图像规模和压缩比下的稳健性。", "", "## 维度实际变化统计", "",
              "单元格为不同取值数；唯一值不是独立重复次数。统计水平时合并浮点尾差（如0.7与0.7000000000000001），完整原值保留在案例记录。完整值列表在 JSON 的 panels[].variation。", "",
              "| 参数 | D1 | D2 | C1 |", "|---|---:|---:|---:|"]
    fields = ["num_sats", "role_profile", "source_count", "planes", "altitude_km", "inclination_deg",
              "raan_offset_deg", "phase_offset_deg", "orbit_offset_seconds", "base_arrival_rate_per_second",
              "effective_arrival_rate_per_second", "compute_cost_per_tile", "helper_compute_rate",
              "isl_bandwidth_scale", "ground_bandwidth_scale", "ground_compute_rates", "mem_capacity_gb",
              "vram_capacity_gb", "deadline_steps", "prediction", "ground_receive_channels"]
    for field in fields:
        lines.append("| " + field + " | " + " | ".join(str(len(p["variation"][field])) for p in data["panels"]) + " |")
    lines += ["", "## 地面接触机会审计", "",
              "以下是保存 TLE 和站点配置在实际10秒执行网格上的只读几何重建。窗口是观测区间截断后的连续可见采样段，不是连续时间精确过境边界；没有运行任务或策略。源星可见秒数对每颗源星取均值，并把同一时刻能看见多个地面站只计一次。任务等待指释放后源星首次具有任一地面可见机会的等待，不含数据传输、地面竞争和计算，也不代表经中继的最早交付时间。", "",
              "| 标签 | 源星平均可见 s/6h | 全星-站窗口数 | 北京/乌站超通道可见 s | 释放后源星接触均等候 s | 无后续源星接触任务数 |",
              "|---|---:|---:|---|---:|---:|"]
    for r in data["cases"]:
        c = r["contacts"]
        wait = c["task_source_next_contact_wait_seconds_conditional"]["mean"]
        lines.append(f"| {r['label']} | {f(c['mean_source_any_ground_visible_seconds'],2)} | {sum(s['clipped_window_count'] for s in c['stations'])} | "
                     + "/".join(f(s["over_channel_count_seconds"],0) for s in c["stations"])
                     + f" | {f(wait,2) if wait is not None else 'NA'} | {len(c['tasks_without_source_contact_before_end'])} |")
    lines += ["", "超通道可见时间仅表示几何上同时可见卫星数超过接收通道数，是竞争潜力，不代表这些卫星实际都有待下传数据。档案中 network.capacity_mb 的地面容量累计包含不可见时间，不能据此报告有效窗口容量；实际发送量同样受策略影响，不能当作外生接触机会。ISL可见时间、连通分量与逐任务可行多跳路径尚未在此审计重建，不从发送量或容量之和伪推。", "", "## 混淆、种子和样本充分性", "",
              "1. D1 中 uniform 只对应12星、near只对应24星、shared对应24或36星；36星同时使用19.2倍地面带宽，其余为1倍，各组还使用不同起始时间。规模、角色、负载、算力总预算、地面能力与几何时段共变，不能从跨组成绩推出单一规模效应或角色效应。",
              "2. 三组均固定每面6星，所以轨道面数与总星数完全共线。shared 在同星数下同时改变源星比例（1/3→2/3）、辅助星算力（0.7→1.3）和有效总到达率（倍增）；保持平均总算力不代表这些条件已经控制。",
              "3. D2、C1 的九种离散组合覆盖均衡，但每个组合只有一套同时随机变化的高度、倾角、RAAN、相位、时间、基础负载、成本和两种带宽参数。连续维度没有重复，也没有单因素配对，九个案例不能估计这些高维交互。",
              "4. training_case 的 run_seed 和 episode 用 SeedSequence 独立抽样连续参数，并按九种组合洗牌循环；环境 seed=run_seed+episode。D1 的 a/b 只改环境种子，改变泊松到达、来源卫星、片大小噪声及链路带宽随机相位/噪声；轨道和地面几何不变。它们不是两个训练种子。",
              "5. 197 次方法运行是8×10+9×10+9×3，不是197个独立场景或模型训练重复；三组共有26条任务轨迹、22套不含环境种子的配置。方法间配对保证横向比较公平，但不提供跨训练重复的方差。",
              "6. 任务数量仅随采样负载和来源数变化。同场景任务共享链路、队列和接收竞争，不能将每个任务当作统计独立样本来代替场景或训练重复。",
              "7. 目前足以展示可重放的开发阶段方法比较与竞争机制反例，不足以支持稳定泛化、统计显著优越、真实星座普适性或MARL已胜出。接触机会数据可解释难度差异，但不能消除上述混淆。", "", "## 后续实验最小修订", "",
              "先固定遥感算子、参考资源和信息权限，再设置明确的实验因子。规模实验应控制每源负载与每星算力并交代总资源变化；角色实验应分别控制源星比例与集群相位，避免把二者合为一个结论。以相同任务/轨道种子做配对方法比较，每个离散条件增加多个环境种子；学习方法另做多个独立训练种子。最终方案冻结后生成封存分布内面板，再单列未见规模、有限通信预测或资源变化等明确OOD轴，不根据已查看结果重定义测试集。", "", "## 复核入口", "", "```sh", ".venv/bin/python analysis/audit_experiment_design.py", "```", "", "运行只覆盖本审计 JSON 与 Markdown，不改动原始实验档案、仿真器或训练模型。逐记录的来源路径、完整参数、配对检查结果与 SHA256 均写入 JSON。", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--out-dir", type=Path, default=Path("results/experiment_design_audit_20261014"))
    parser.add_argument("--markdown", type=Path, default=Path("results/experiment_design_audit_20261014/README.md"))
    args = parser.parse_args()
    repo = args.repo.resolve()
    result = audit(repo)
    output = args.out_dir if args.out_dir.is_absolute() else repo / args.out_dir
    markdown = args.markdown if args.markdown.is_absolute() else repo / args.markdown
    output.mkdir(parents=True, exist_ok=True)
    markdown.parent.mkdir(parents=True, exist_ok=True)
    (output / "audit.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    markdown.write_text(render_markdown(result), encoding="utf-8")
    print(json.dumps({"cases": len(result["cases"]), "method_runs": sum(p["method_runs"] for p in result["panels"]),
                      "panels": [{k: p[k] for k in ("id", "cases", "total_tasks", "unique_configs_without_seed", "unique_geometry")} for p in result["panels"]]}, indent=2))


if __name__ == "__main__":
    main()
