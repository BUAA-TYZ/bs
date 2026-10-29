"""完整固定压力面板的配对报告；缺记录、配置失配或观察不足时拒绝汇总。"""

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from analysis.evaluate_literature import common_context
from analysis.report_short_deadline import compare_on_time
from sim.evaluation import task_age_score


LEVELS = ("s3_gap30", "s6_gap30", "s6_gap20", "s6_gap10")
DATES = ("2026-10-15", "2026-10-16", "2026-10-17")
CGS = "cgs_compute_adapted"
REQUIRED = {CGS, "heft_adapted", "dts_tmax_hybrid", "ground_relay",
            "task_delivery", "task_delivery_fcfs", "arrival_calendar"}


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def without_mode(cfg):
    return {key: value for key, value in cfg.items() if key != "pipeline_mode"}


def finite(value, label):
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid {label}: {value}")
    return value


def score_record(record):
    experiment = record["metrics"]["experiment"]
    cfg, outcomes = experiment["config"], record["metrics"]["task_outcomes"]
    end = finite(experiment["elapsed_sim_seconds"], "actual observation end")
    if not math.isclose(end, cfg["sim_steps"] * cfg["dt"], abs_tol=1e-7):
        raise ValueError("Run ended before or after its configured observation endpoint")
    if (cfg.get("ground_input_relay") is not True or cfg["dt"] != .1
            or cfg["input_relay_max_hops"] != 2 or cfg["deadline_steps"] != 0):
        raise ValueError("Actual config requires ground-input relay, dt=.1, two input hops and no hard expiry")
    for key, period in (("decision_interval_steps", 1), ("topology_update_steps", 10),
                        ("ground_visibility_update_steps", 10)):
        if not math.isclose(cfg[key] * cfg["dt"], period):
            raise ValueError(f"Changed physical cadence: {key}")
    if not outcomes or cfg["task_arrival_rate"] != 0:
        raise ValueError("This fixed nonempty panel requires all scheduled acquisitions")
    expected = Counter((e["source_sat_id"], round(e["release_step"] * cfg["dt"], 8))
                       for e in cfg["scheduled_task_arrivals"])
    actual = Counter((e["source_sat_id"], round(e["release_seconds"], 8)) for e in outcomes)
    if actual != expected:
        raise ValueError("Task outcomes omit or change scheduled acquisitions")
    for row in outcomes:
        if row["deadline_seconds"] is not None:
            raise ValueError("Hard deadline in a no-expiry panel")
        age = finite(row["observed_age_seconds"], "observed task age")
        if age < 300 or not math.isclose(age, end - row["release_seconds"], abs_tol=1e-7):
            raise ValueError("Task lacks a consistent full 300-second follow-up")
    score = task_age_score(outcomes, [300], end)
    recorded = record["task_age"]
    for key in ("delivery_score", "restricted_mean_latency_seconds"):
        if not math.isclose(score[key], recorded[key], abs_tol=1e-9):
            raise ValueError(f"Saved task-age score disagrees with outcomes: {key}")
    timing = record["decision_timing"]
    total = finite(timing["total_ms"], "scheduler total ms")
    p95 = timing["active_p95_ms"]
    if p95 is not None:
        finite(p95, "scheduler active p95 ms")
    return {"case": record["case"]["id"], "method": record["method"],
            "calendar_day": record["case"]["calendar_day"], "level": record["case"]["level"],
            "total_tasks": len(outcomes), "on_time_tasks": score["delivery_curve"][0]["delivered_within_horizon"],
            "p300": score["delivery_score"], "r300_seconds": score["restricted_mean_latency_seconds"],
            "scheduler_seconds": total / 1000, "active_p95_ms": p95,
            "elapsed_seconds": finite(record["elapsed_s"], "host run seconds")}


def load_panel(directory):
    if (directory / "INTERRUPTED.md").exists() or (directory / "INVALID_MIXED.md").exists():
        raise ValueError("Interrupted or invalid run cannot form a complete pressure report")
    manifest = read(directory / "run.json")
    methods = list(manifest["method_specs"])
    if not REQUIRED <= set(methods):
        raise ValueError(f"Missing required baselines or ablations: {sorted(REQUIRED - set(methods))}")
    task_spec, fcfs_spec = (manifest["method_specs"][name] for name in ("task_delivery", "task_delivery_fcfs"))
    task_args, fcfs_args = dict(task_spec["kwargs"]), dict(fcfs_spec["kwargs"])
    if (task_spec["policy"] != "task_delivery" or fcfs_spec["policy"] != "task_delivery"
            or fcfs_args.pop("max_orders", 3) != 1 or task_args.pop("max_orders", 3) <= 1
            or task_args != fcfs_args):
        raise ValueError("FCFS ablation must differ from task_delivery only in max_orders=1")
    expected_cases = {f"day{day:02d}_{level}" for level in LEVELS for day in range(1, 4)}
    infos = {info["id"]: info for info in manifest["case_info"]}
    if len(manifest["case_info"]) != 12 or set(infos) != expected_cases:
        raise ValueError("Complete pressure report requires all 12 preselected cases")
    for case_id, info in infos.items():
        day = int(case_id[3:5])
        if (info["calendar_day"] != DATES[day - 1] or info["level"] not in LEVELS
                or case_id != f"day{day:02d}_{info['level']}" or info["split"] != "development"
                or info.get("all_qualifying_events_retained") is not True):
            raise ValueError("Changed date, level, split or acquisition selection")
    names = {f"{case}_{method}.json" for case in expected_cases for method in methods}
    missing = sorted(name for name in names if not (directory / name).is_file())
    if missing:
        raise ValueError(f"Incomplete pressure panel: missing {len(missing)}/{len(names)} records; first={missing[0]}")
    sources = {str(directory / "run.json"): sha(directory / "run.json")}
    summary_path = directory / "summary.json"
    if summary_path.exists():
        summary = read(summary_path)
        if set(summary["records"]) != names or len(summary["records"]) != len(names):
            raise ValueError("Summary record index is incomplete or duplicated")
        for key in ("method_specs", "case_info", "source_sha256", "selection_sha256"):
            if summary[key] != manifest[key]:
                raise ValueError(f"Summary and run manifest disagree: {key}")
        sources[str(summary_path)] = sha(summary_path)
    prepared = Path(manifest["args"]["prepared_dir"])
    if not prepared.is_absolute():
        prepared = ROOT / prepared
    prepared_manifest = read(prepared / "run.json")
    if (sha(prepared / "run.json") != manifest["prepared_manifest_sha256"] or
            sha(prepared / "selection.json") != manifest["selection_sha256"] or
            prepared_manifest["selection_sha256"] != manifest["selection_sha256"]):
        raise ValueError("Prepared selection or configuration manifest changed")
    if infos != {info["id"]: info for info in prepared_manifest["case_info"]}:
        raise ValueError("Result case metadata differs from the preselected pressure panel")
    sources[str(prepared / "run.json")] = sha(prepared / "run.json")
    sources[str(prepared / "selection.json")] = sha(prepared / "selection.json")
    configs = {}
    for case in expected_cases:
        path = prepared / f"{case}_config.json"
        if sha(path) != prepared_manifest["config_sha256"][path.name]:
            raise ValueError(f"Prepared configuration changed: {case}")
        sources[str(path)] = sha(path)
        configs[case] = read(path)
    records, daily, by_case = [], [], {}
    for name in sorted(names):
        path = directory / name
        row = read(path)
        case, method = row["case"]["id"], row["method"]
        if name != f"{case}_{method}.json" or row["case"] != infos[case]:
            raise ValueError("Record filename or case metadata mismatch")
        if row["method_spec"] != manifest["method_specs"][method]:
            raise ValueError("Method parameters differ from the frozen manifest")
        experiment = row["metrics"]["experiment"]
        if experiment["source_sha256"] != manifest["source_sha256"]:
            raise ValueError("Experiment used a different core source")
        if without_mode(experiment["config"]) != without_mode(configs[case]):
            raise ValueError(f"Actual configuration differs from prepared case: {case}/{method}")
        peers = by_case.setdefault(case, {})
        if peers and common_context(row) != common_context(next(iter(peers.values()))):
            raise ValueError(f"Unpaired task trace, orbit or configuration: {case}")
        peers[method] = row
        daily.append(score_record(row))
        records.append(row)
        sources[str(path)] = sha(path)
    return manifest, methods, records, daily, by_case, sources


def paired_rows(by_case, daily, reference, candidate, cases):
    scores = {(row["case"], row["method"]): row for row in daily}
    result = []
    for case in sorted(cases):
        row = compare_on_time(by_case[case][reference], by_case[case][candidate])
        ref, cand = scores[case, reference], scores[case, candidate]
        delta = cand["on_time_tasks"] - ref["on_time_tasks"]
        row.update(p300_difference=cand["p300"] - ref["p300"],
                   r300_difference_seconds=cand["r300_seconds"] - ref["r300_seconds"],
                   outcome="win" if delta > 0 else "loss" if delta < 0 else "tie")
        result.append(row)
    return result


def aggregate(rows, methods):
    result = []
    for method in methods:
        selected = sorted((r for r in rows if r["method"] == method), key=lambda r: (r["calendar_day"], r["level"]))
        result.append({"method": method, "cases": len(selected),
                       "on_time_tasks": sum(r["on_time_tasks"] for r in selected),
                       "total_tasks": sum(r["total_tasks"] for r in selected),
                       "p300": statistics.mean(r["p300"] for r in selected),
                       "r300_seconds": statistics.mean(r["r300_seconds"] for r in selected),
                       "scheduler_seconds": sum(r["scheduler_seconds"] for r in selected),
                       "maximum_case_active_p95_ms": max((r["active_p95_ms"] for r in selected if r["active_p95_ms"] is not None), default=None),
                       "daily_counts": [{"case": r["case"], "on_time": r["on_time_tasks"], "total": r["total_tasks"]} for r in selected]})
    return result


def diagnostics(records, methods):
    result = []
    for level in LEVELS:
        for method in methods:
            selected = [r for r in records if r["method"] == method and r["case"]["level"] == level]
            raw = {r["case"]["id"]: r["metrics"].get("scheduler_diagnostics") for r in selected}
            available = all(isinstance(stats, dict) for stats in raw.values())
            stats = Counter()
            if available:
                for case_stats in raw.values():
                    stats.update(case_stats)
            calendar = method == "arrival_calendar"
            task = method in ("task_delivery", "task_delivery_fcfs")
            gap = stats["calendar_release_gap_observations"] if calendar and available else None
            orders = ({name.removeprefix("selected_"): value for name, value in stats.items()
                       if name.startswith("selected_")} if task and available else None)
            non_fcfs = sum(value for name, value in (orders or {}).items() if name != "fcfs")
            if calendar:
                status = ("diagnostics_missing" if not available else "no_release_gap_trigger_no_evidence_of_mechanism_gain"
                          if gap == 0 else "release_gap_triggered_gain_requires_paired_comparison")
            elif task:
                status = ("diagnostics_missing" if not available else "no_non_fcfs_order_selected_no_evidence_of_reordering_gain"
                          if non_fcfs == 0 else "non_fcfs_order_selected_gain_requires_fcfs_ablation")
            else:
                status = "baseline"
            result.append({"level": level, "method": method, "calendar_gap_observations": gap,
                           "calendar_gap_seconds_sum": stats["calendar_release_gap_seconds_sum"] if calendar and available else None,
                           "accepted_ground_actions": stats["accepted_ground"] if calendar and available else None,
                           "selected_order": orders, "non_fcfs_selections": non_fcfs if task and available else None,
                           "interpretation": status, "raw_by_case": raw})
    return result


def render(report):
    lines = ["# 固定负载与成像压力面板", "",
             "P300 为全部图像任务释放后 300 秒内完整结果在地面聚合完成的比例；R300 为含未完成任务的 300 秒截断平均时延，越低越好。迟到任务继续执行。",
             "比例和 R300 按三日等权；按时计数单独汇总。四档共享三个相关日期，是开发数据，不是 12 次独立重复；不做显著性或领域最优声明。",
             "原 s3_gap30 是主结果，压力档不能替代它。10/20 秒间隔属于未校准成像占用敏感性，不能称为真实载荷实测。", ""]
    for level in LEVELS:
        data = report["levels"][level]
        pairs = {m: [r for r in data["paired_against_cgs"] if r["candidate"] == m] for m in report["methods"]}
        lines += [f"## {'原负载主结果' if level == LEVELS[0] else '压力敏感性'}：{level}", "",
                  "| 方法 | 三日按时/全部（15/16/17日） | P300 | R300/s | 对CGS新增/丢失 | 三日胜平负 | 调度总时/s | 最大场景P95/ms |",
                  "|---|---|---:|---:|---:|---|---:|---:|"]
        for row in data["aggregate"]:
            counts = "; ".join(f"{d['on_time']}/{d['total']}" for d in row["daily_counts"])
            paired = pairs[row["method"]]
            gains = sum(r["candidate_only_on_time"] for r in paired)
            losses = sum(r["reference_only_on_time"] for r in paired)
            outcomes = "/".join({"win": "胜", "tie": "平", "loss": "负"}[r["outcome"]] for r in paired)
            p95 = "N/A" if row["maximum_case_active_p95_ms"] is None else f"{row['maximum_case_active_p95_ms']:.2f}"
            lines.append(f"| {row['method']} | {counts} | {row['p300']:.2%} | {row['r300_seconds']:.2f} | {gains}/{losses} | {outcomes} | {row['scheduler_seconds']:.2f} | {p95} |")
        ablation = data["task_delivery_vs_fcfs"]
        lines += ["", "整图重排对 FCFS 消融：新增按时 " + str(sum(r["candidate_only_on_time"] for r in ablation)) +
                  "，丢失按时 " + str(sum(r["reference_only_on_time"] for r in ablation)) +
                  f"；日期等权 R300 差 {statistics.mean(r['r300_difference_seconds'] for r in ablation):+.2f} 秒。", ""]
    lines += ["## 机制触发证据", "",
              "| 档位 | 方法 | 日历空档观测数 | 空档秒累计 | 地面接纳动作数 | 选中顺序计数 | 解释 |",
              "|---|---|---:|---:|---:|---|---|"]
    for row in report["mechanisms"]:
        if row["method"] not in ("arrival_calendar", "task_delivery", "task_delivery_fcfs"):
            continue
        display = lambda value: "N/A" if value is None else f"{value:.2f}" if isinstance(value, float) else str(value)
        order = ", ".join(f"{k}:{v}" for k, v in (row["selected_order"] or {}).items()) or "N/A"
        note = ("诊断缺失" if row["interpretation"] == "diagnostics_missing" else
                "未触发，不能归因机制收益" if "no_evidence" in row["interpretation"] else "已触发，须结合配对/消融判断")
        lines.append(f"| {row['level']} | {row['method']} | {display(row['calendar_gap_observations'])} | {display(row['calendar_gap_seconds_sum'])} | {display(row['accepted_ground_actions'])} | {order} | {note} |")
    lines += ["", "计数是调度器观测或提议动作，存在跨轮重复；空档秒累计不是实际空闲时间，地面接纳动作数也不是已成功地面计算的分片数。机制触发本身不证明收益。",
              "调度计时仅包含本机 select_actions，受并行负载影响；最大场景 P95 不是全体调用的合并 P95，也不是星载测量，未计入仿真任务时延。", "",
              "## 全面板描述性汇总", "", "| 方法 | 汇总按时/全部 | 场景等权P300 | 场景等权R300/s |", "|---|---:|---:|---:|"]
    for row in report["all_cases_descriptive"]:
        lines.append(f"| {row['method']} | {row['on_time_tasks']}/{row['total_tasks']} | {row['p300']:.2%} | {row['r300_seconds']:.2f} |")
    lines += ["", "此表重复使用相同日期的不同负载，只作描述，不能替代原负载或当作 12 个独立样本。逐场景得失、日期内平均差、原始诊断和文件哈希见 summary.json。",
              "结果比较规则候选和文献方法适配，不是 MARL 训练结果或原论文完整复现。算力与结果量代理、合成轨道、瞬时成像等原协议边界继续成立。", ""]
    return "\n".join(lines)


def build_report(directory):
    manifest, methods, records, daily, by_case, sources = load_panel(directory)
    levels = {}
    for level in LEVELS:
        selected = [r for r in daily if r["level"] == level]
        cases = sorted({r["case"] for r in selected})
        levels[level] = {"aggregate": aggregate(selected, methods),
                         "paired_against_cgs": [r for method in methods for r in paired_rows(by_case, daily, CGS, method, cases)],
                         "task_delivery_vs_fcfs": paired_rows(by_case, daily, "task_delivery_fcfs", "task_delivery", cases)}
    date_means = {date: {method: statistics.mean(
        r["p300"] - next(x["p300"] for x in daily if x["case"] == r["case"] and x["method"] == CGS)
        for r in daily if r["calendar_day"] == date and r["method"] == method)
        for method in methods} for date in DATES}
    return {"scope": "Complete fixed development pressure panel; three correlated dates, not 12 independent replications",
            "result_directory": str(directory.resolve()), "primary_seconds": 300, "methods": methods,
            "method_specs": manifest["method_specs"], "core_source_sha256": manifest["source_sha256"],
            "source_files_sha256": sources, "reporter_sha256": sha(Path(__file__)),
            "levels": levels, "daily": daily, "mechanisms": diagnostics(records, methods),
            "date_mean_p300_differences_against_cgs": date_means,
            "all_cases_descriptive": aggregate(daily, methods)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-dir", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()
    report = build_report(args.result_dir)
    args.out_dir.mkdir(parents=True, exist_ok=False)
    (args.out_dir / "summary.json").write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    (args.out_dir / "REPORT.md").write_text(render(report), encoding="utf-8")
    print(json.dumps({"report": str(args.out_dir / "REPORT.md"), "cases": 12, "methods": report["methods"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
