"""固定SAR开发前缀的单因素扫描；不修改仿真核心或公共路由。"""

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
import csv
from dataclasses import asdict
from datetime import datetime, timezone
import json
import multiprocessing
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import run_sar_frozen as runner
from analysis.report_sar_task_choice import compare
from sim.config import config_from_dict
from sim.entities import TileState
from sim.reproducibility import runtime_metadata

SOURCE = Path("results/sar_loopfree_20261017/main_config.json")
BASE_PANEL = SOURCE.parent / "prefix_21600"
OLD_PANEL = Path("results/sar_temporal_study_20261017/choice_prefix_21600")
CONDITIONS = {
    "base": (1, 1, 2), "compute_4": (4, 1, 2), "compute_10": (10, 1, 2),
    "load_2": (1, 2, 2), "load_4": (1, 4, 2), "isl_1": (1, 1, 1), "isl_5": (1, 1, 5),
}
METHODS = {
    "local": "local", "ground_8": "ground_unique_relay", "ground_loopfree": "ground_unique_relay",
    "cgs": "cgs_compute_adapted", "heft": "heft_adapted", "choice": "cgs_task_choice",
}
LABELS = {"base": "基准", "compute_4": "计算量 x4", "compute_10": "计算量 x10",
          "load_2": "到达负载 x2", "load_4": "到达负载 x4", "isl_1": "ISL 1 Gbps", "isl_5": "ISL 5 Gbps"}


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def file_hash(path):
    return runner.sha256_bytes(path.read_bytes())


def derive(base, compute=1, load=1, bandwidth=2, compute_scope="all"):
    if (compute not in (1, 4, 10) or load not in (1, 2, 4) or bandwidth not in (1, 2, 5)
            or compute_scope not in ("all", "onboard")):
        raise ValueError("Only preregistered factor levels and compute scopes are allowed")
    if sum((compute != 1, load != 1, bandwidth != 2)) > 1:
        raise ValueError("Change one factor at a time")
    cfg = deepcopy(base)
    if compute != 1:
        # 单位仍是一个完整推理窗；执行组时长与规划服务率必须同步缩放。
        profile = cfg["onboard_compute_profile"]
        profile["service_s"] = {k: seconds * compute for k, seconds in profile["service_s"].items()}
        cfg["compute_rate"] = 1 / profile["service_s"]["1"]
        if compute_scope == "all":
            for gs in cfg["ground_stations"]:
                gs["compute_rate"] /= compute
    if load != 1:
        cfg["scheduled_task_arrivals"] = [deepcopy(event) for event in base["scheduled_task_arrivals"]
                                          for _ in range(load)]
    if bandwidth != 2:
        cfg["topology"]["bandwidth_mbps_min"] = float(bandwidth * 1000)
        cfg["topology"]["bandwidth_mbps_max"] = float(bandwidth * 1000)
    return asdict(config_from_dict(cfg))


def strong_ground(records):
    """选择完整面板记录，禁止逐任务拼接或另取另一个协议的更低R300。"""
    a, b = records["ground_8"], records["ground_loopfree"]
    compare(a, b)
    return max(("ground_8", "ground_loopfree"),
               key=lambda name: (records[name]["P300"], -records[name]["R300_seconds"]))


def validate_record(record, cfg, source_sha, method):
    expected = [(f"task_{i}", e["source_sat_id"], e["release_step"] * cfg["dt"])
                for i, e in enumerate(sorted(cfg["scheduled_task_arrivals"], key=lambda e: e["release_step"]))]
    actual = [(r["task_id"], r["source_sat_id"], r["release_seconds"])
              for r in record["metrics"]["task_outcomes"]]
    if (record["method"] != method or expected != actual or not record["invariants"]["passed"]
            or record["derived_config_sha256"] != runner.digest(cfg)
            or record["metrics"]["experiment"]["source_sha256"] != source_sha):
        raise ValueError("Record identity, configuration, source or execution audit mismatch")


def prepare(out, compute_scope):
    if out.exists():
        raise ValueError("Use a new scan directory")
    source_manifest = json.loads(SOURCE.with_name("manifest.json").read_text())
    if source_manifest["config_sha256"][SOURCE.name] != file_hash(SOURCE):
        raise ValueError("Frozen source changed")
    base = asdict(config_from_dict(runner.derive_config(json.loads(SOURCE.read_text()), 0.01, 21600)))
    if base != json.loads((BASE_PANEL / "derived_config.json").read_text()):
        raise ValueError("Base no longer matches the completed 42-image prefix")
    meta = runtime_metadata()
    out.mkdir(parents=True)
    (out / "configs").mkdir()
    (out / "records").mkdir()
    manifest = {"prepared_at_utc": datetime.now(timezone.utc).isoformat(), "timebox_hours": 48,
                "scope": "one-factor development-prefix scan; no held-out dates, training or thesis changes",
                "source": str(SOURCE), "source_config_sha256": file_hash(SOURCE), **meta,
                "runner_sha256": file_hash(Path(runner.__file__)), "scan_sha256": file_hash(Path(__file__)),
                "compute_scope": compute_scope, "stop_seconds": 21600,
                "ground_selection": "whole-panel P300 descending then R300 ascending; exact ties select 8-hop",
                "load_rule": "same imaging opportunities, source and timestamps; 1/2/4 independent images per event; cloned output fixture",
                "bandwidth_rule": "net payload rate, not terminal nominal rate; ground rate and efficiency convention unchanged",
                "hypothesis": "When isolated local image service approaches/exceeds 300s, local P300 falls and cooperative scheduling gains P300.",
                "falsification": "At x10 local P300 drops by >=5 percentage points vs x1, but none of CGS/HEFT/choice exceeds local P300; latency-only gain does not support completion-rate claim. Gains solely from ground execution do not establish multi-satellite compute benefit.",
                "conditions": {}, "jobs": []}
    for name, factors in CONDITIONS.items():
        cfg = derive(base, *factors, compute_scope)
        old = deepcopy(cfg)
        old["relay_max_hops"] = 8
        config_paths = {}
        for protocol, configuration in (("loopfree", cfg), ("8hop", old)):
            path = out / "configs" / f"{name}_{protocol}.json"
            write_json(path, configuration)
            config_paths[protocol] = {"path": str(path), "sha256": file_hash(path)}
        manifest["conditions"][name] = {
            "compute_multiplier": factors[0], "arrival_multiplier": factors[1], "net_isl_gbps": factors[2],
            "tasks": len(cfg["scheduled_task_arrivals"]), "tasks_per_hour": len(cfg["scheduled_task_arrivals"]) / 6,
            "isolated_local_image_service_s": 32 * cfg["onboard_compute_profile"]["service_s"]["2"],
            "isolated_ground_image_service_s": 64 / cfg["ground_stations"][0]["compute_rate"],
            "changed_top_level_fields": [k for k in base if base[k] != cfg[k]], "configs": config_paths}
        for variant, method in METHODS.items():
            protocol = "8hop" if variant == "ground_8" else "loopfree"
            entry = {"condition": name, "variant": variant, "method": method, "protocol": protocol,
                     "config": config_paths[protocol], "path": str(out / "records" / f"{name}_{variant}.json")}
            if name == "base":
                path = (OLD_PANEL if variant == "ground_8" else BASE_PANEL) / f"{method}.json"
                record = json.loads(path.read_text())
                validate_record(record, old if protocol == "8hop" else cfg, meta["source_sha256"], method)
                entry.update(path=str(path), reused=True, record_sha256=file_hash(path))
            manifest["jobs"].append(entry)
    write_json(out / "manifest.json", manifest)
    base_records = {j["variant"]: json.loads(Path(j["path"]).read_text()) for j in manifest["jobs"] if j["condition"] == "base"}
    selected = strong_ground(base_records)
    write_json(out / "ground_fairness.json", {"selected": selected, "rule": manifest["ground_selection"],
              "versions": {k: {f: base_records[k][f] for f in ("P300", "R300_seconds", "on_time_tasks", "total_tasks")}
                           for k in ("ground_8", "ground_loopfree")},
              "paired": compare(base_records["ground_8"], base_records["ground_loopfree"])})
    print(json.dumps({"prepared": str(out), "jobs": len(manifest["jobs"]), "reused": 6,
                      "ground_selection": selected, "compute_scope": compute_scope}), flush=True)


def measured_job(cfg, method, source_sha, runner_sha, memory_limit):
    observers = []
    original = runner.Observer

    class AllocationObserver(original):
        def on_start(self, env):
            super().on_start(env)
            observers.append(self)

    runner.Observer = AllocationObserver
    try:
        record = runner.run_one(cfg, method, source_sha, runner_sha, memory_limit)
    finally:
        runner.Observer = original
    env = observers[0].env
    placements = {tid: Counter() for tid in env.tasks}
    sources = set(env.cfg.task_source_sat_ids)
    for cohort in env.compute_cohort_records.values():
        sid = cohort["sat_id"]
        for tile_id in cohort.get("completed_window_tile_ids", []):
            task_id = env.tiles[tile_id].parent_task_id
            owner = env.tasks[task_id].source_sat_id
            kind = "local_windows" if sid == owner else ("peer_source_windows" if sid in sources else "helper_windows")
            placements[task_id][kind] += 1
    for tile in env.tiles.values():
        if tile.state == TileState.DONE and not tile.result_path:
            placements[tile.parent_task_id]["ground_windows"] += tile.compute_cost
    for task_id in record["on_time_task_ids"]:
        if sum(placements[task_id].values()) != 64:
            raise ValueError("On-time task compute placement does not sum to 64 windows")
    record["compute_placement_windows"] = {tid: dict(counts) for tid, counts in placements.items()}
    return record


def load_manifest(out):
    manifest = json.loads((out / "manifest.json").read_text())
    if (manifest["source_sha256"] != runtime_metadata()["source_sha256"]
            or manifest["runner_sha256"] != file_hash(Path(runner.__file__))
            or manifest["scan_sha256"] != file_hash(Path(__file__))):
        raise ValueError("Frozen experiment source changed")
    for condition in manifest["conditions"].values():
        for entry in condition["configs"].values():
            if file_hash(Path(entry["path"])) != entry["sha256"]:
                raise ValueError("Frozen condition configuration changed")
    return manifest


def job_key(job, manifest):
    return runner.digest({"config": job["config"]["sha256"], "method": job["method"],
                          "source": manifest["source_sha256"], "runner": manifest["runner_sha256"],
                          "scan": manifest["scan_sha256"]})


def existing_record(job, manifest):
    path = Path(job["path"])
    if not path.exists():
        return None
    record = json.loads(path.read_text())
    if job.get("reused"):
        if file_hash(path) != job["record_sha256"]:
            raise ValueError("Previously measured record changed")
    elif record.get("scan_key") != job_key(job, manifest):
        raise ValueError("Existing scan record belongs to another experiment")
    cfg = json.loads(Path(job["config"]["path"]).read_text())
    validate_record(record, cfg, manifest["source_sha256"], job["method"])
    return record


def run(out, workers):
    manifest = load_manifest(out)
    prepared = datetime.fromisoformat(manifest["prepared_at_utc"])
    if (datetime.now(timezone.utc) - prepared).total_seconds() > 48 * 3600:
        raise ValueError("48-hour timebox exceeded; report and stop")
    todo = [j for j in manifest["jobs"] if existing_record(j, manifest) is None]
    done = len(manifest["jobs"]) - len(todo)
    start = time.monotonic()
    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        pending = {pool.submit(measured_job, json.loads(Path(j["config"]["path"]).read_text()),
                               j["method"], manifest["source_sha256"], manifest["runner_sha256"], 24 / workers): j
                   for j in todo}
        for future in as_completed(pending):
            job = pending[future]
            record = future.result()
            cfg = json.loads(Path(job["config"]["path"]).read_text())
            record.update(derived_config_sha256=runner.digest(cfg), scan_key=job_key(job, manifest),
                          scan_manifest_sha256=file_hash(out / "manifest.json"), scan_condition=job["condition"],
                          scan_variant=job["variant"], scan_workers=workers, worker_rss_limit_gb=24 / workers)
            validate_record(record, cfg, manifest["source_sha256"], job["method"])
            path = Path(job["path"])
            temporary = path.with_suffix(".json.tmp")
            write_json(temporary, record)
            temporary.replace(path)
            done += 1
            progress = {"done": done, "total": len(manifest["jobs"]), "condition": job["condition"],
                        "variant": job["variant"], "on_time": record["on_time_tasks"], "tasks": record["total_tasks"],
                        "P300": record["P300"], "R300": record["R300_seconds"],
                        "helper_utilization": record["resources"]["helper"]["utilization_full_horizon"],
                        "wall_seconds": record["wall_seconds"], "elapsed_seconds": time.monotonic() - start}
            write_json(out / "progress.json", progress)
            print(json.dumps(progress), flush=True)


def report(out):
    manifest = load_manifest(out)
    rows, pairs, classifications = [], [], {}
    lines = ["# SAR条件扫描汇总", "", "7个单因素条件，固定开发日前6小时；不是独立测试或全天排名。",
             "表内每格依次为 **P300百分比 / R300秒 / 辅助星计算利用率百分比**。",
             "辅助星利用率分母固定为22颗辅助星乘21600秒，包含空闲；不把源星之间的计算互助计入该列。",
             "强地面按整套面板P300优先、R300次之选择，P和R来自同一协议；不是逐任务择优的可部署策略。", "",
             "| 条件 | 图数 | 本地+中继 | 地面8跳 | 地面无环 | 强地面 | CGS | HEFT | 整图复核 |",
             "|---|---:|---|---|---|---|---|---|---|"]
    cell = lambda r: f"{100*r['P300']:.2f} / {r['R300_seconds']:.2f} / {100*r['resources']['helper']['utilization_full_horizon']:.4f}"
    for condition in CONDITIONS:
        records = {}
        for job in manifest["jobs"]:
            if job["condition"] != condition:
                continue
            record = existing_record(job, manifest)
            if record is None:
                raise ValueError(f"Missing result {condition}/{job['variant']}")
            records[job["variant"]] = record
        selected = strong_ground(records)
        records["strong_ground"] = records[selected]
        cfg = manifest["conditions"][condition]
        for variant, record in records.items():
            compare(records["local"], record)
            row = {"condition": condition, **{k: cfg[k] for k in ("compute_multiplier", "arrival_multiplier", "net_isl_gbps")},
                   "variant": variant, "ground_selected": selected, "tasks": record["total_tasks"],
                   "on_time_tasks": record["on_time_tasks"], "P300": record["P300"], "R300_seconds": record["R300_seconds"],
                   "helper_utilization": record["resources"]["helper"]["utilization_full_horizon"],
                   "helper_busy_seconds": record["resources"]["helper"]["busy_seconds"],
                   "ground_busy_seconds": sum(g["busy_seconds"] for g in record["resources"]["ground"].values())}
            rows.append(row)
        for variant in ("cgs", "heft", "choice"):
            for reference in ("local", "strong_ground"):
                pair = {"condition": condition, "variant": variant, "reference_variant": reference,
                        **compare(records[reference], records[variant])}
                placement = records[variant].get("compute_placement_windows", {})
                pair["gained_compute_placement"] = {tid: placement.get(tid) for tid in pair["gained"]}
                pairs.append(pair)
        local = records["local"]
        classifications[condition] = {
            "P300_leaders_vs_local": [v for v in ("cgs", "heft", "choice") if records[v]["P300"] > local["P300"]],
            "P300_ties_vs_local": [v for v in ("cgs", "heft", "choice") if records[v]["P300"] == local["P300"]],
            "choice_gain_vs_local": compare(local, records["choice"])["gained"],
            "choice_loss_vs_local": compare(local, records["choice"])["lost"], "strong_ground": selected}
        lines.append(f"| {LABELS[condition]} | {cfg['tasks']} | {cell(local)} | {cell(records['ground_8'])} | "
                     f"{cell(records['ground_loopfree'])} | {cell(records[selected])} ({'8跳' if selected=='ground_8' else '无环'}) | "
                     f"{cell(records['cgs'])} | {cell(records['heft'])} | {cell(records['choice'])} |")
    lines.extend(["", "## 配对与解释", "", "逐任务新增/丢失、截断时延变化及新增任务的计算落点见summary.json。",
                  "运行中间状态不进入排名，原始记录均通过工时、字节、推理窗数、时间戳与无回访审计。",
                  "compute_placement只记录已完成推理窗；原始基准复用记录没有该诊断，明确记为缺失。",
                  "CGS/HEFT为计算任务适配，整图复核不是MARL。辅助星被使用不等于已证明其因果贡献。",
                  "计算倍率、同机会批量到达、净带宽及否定条件见README.md；不将扫描倍率称为硬件实测。", ""])
    for condition, result in classifications.items():
        lines.append(f"- {LABELS[condition]}：P300超过本地={result['P300_leaders_vs_local']}；"
                     f"持平={result['P300_ties_vs_local']}；新候选新增={result['choice_gain_vs_local']}，丢失={result['choice_loss_vs_local']}。")
    for name in ("summary.json", "summary.csv", "REPORT.md"):
        if (out / name).exists():
            raise ValueError("Completed reports are immutable")
    write_json(out / "summary.json", {"manifest_sha256": file_hash(out / "manifest.json"),
               "rows": rows, "paired": pairs, "classifications": classifications,
               "record_hashes": {j["path"]: file_hash(Path(j["path"])) for j in manifest["jobs"]}})
    with (out / "summary.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (out / "REPORT.md").write_text("\n".join(lines) + "\n")
    print(json.dumps(classifications, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "run", "report"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--compute-scope", choices=("all", "onboard"), default="all")
    parser.add_argument("--workers", type=int, choices=(1, 2, 3, 4), default=4)
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare(args.out, args.compute_scope)
    elif args.phase == "run":
        run(args.out, args.workers)
    else:
        report(args.out)


if __name__ == "__main__":
    main()
