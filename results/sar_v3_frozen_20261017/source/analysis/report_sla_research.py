"""汇总冻结候选的开发和确认结果，保留逐任务配对得失与执行开销。"""

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.report_short_deadline import aggregate, compare_on_time, load_panel


def panel(directory, candidate):
    manifest = json.loads((directory / "run.json").read_text())
    methods = list(manifest["method_specs"])
    if candidate not in methods:
        raise ValueError("Candidate is absent")
    records, sources = load_panel([directory], methods)
    rows, daily = aggregate(records, methods, horizons=[300])
    pairs = []
    by_case = defaultdict(dict)
    for record in records:
        by_case[record["case"]["id"]][record["method"]] = record
    for case_records in by_case.values():
        for reference in methods:
            if reference != candidate:
                pairs.append(compare_on_time(case_records[reference], case_records[candidate]))
    comparisons = []
    for method in methods:
        if method == candidate:
            continue
        selected = [row for row in pairs if row["reference"] == method]
        differences = [row["candidate_only_on_time"] - row["reference_only_on_time"] for row in selected]
        comparisons.append({"reference": method,
            "gained_on_time": sum(row["candidate_only_on_time"] for row in selected),
            "lost_on_time": sum(row["reference_only_on_time"] for row in selected),
            "wins": sum(d > 0 for d in differences), "ties": differences.count(0),
            "losses": sum(d < 0 for d in differences)})
    costs = []
    for method in methods:
        selected = [r for r in records if r["method"] == method]
        costs.append({"method": method,
            "completed_tasks": sum(r["metrics"]["overall"]["completed_tasks"] for r in selected),
            "scheduler_total_seconds": sum(r["decision_timing"]["total_ms"] for r in selected) / 1000,
            "max_case_p95_ms": max(r["decision_timing"]["active_p95_ms"] or 0 for r in selected),
            "compute_satellites_by_case": {r["case"]["id"]: sum(v > 0 for v in
                r["metrics"]["resource"]["compute_busy_time"].values()) for r in selected}})
    date_differences = defaultdict(list)
    for case, case_records in by_case.items():
        if "cgs_compute_adapted" not in case_records:
            continue
        candidate_record = case_records[candidate]
        date = candidate_record["case"].get("calendar_day", case)
        date_differences[date].append(candidate_record["task_age"]["delivery_score"] -
            case_records["cgs_compute_adapted"]["task_age"]["delivery_score"])
    return {"manifest": manifest, "source_files_sha256": sources, "aggregate": rows,
            "daily": daily, "paired": pairs, "comparisons": comparisons, "costs": costs,
            "date_mean_differences_against_cgs": {d: statistics.mean(v) for d, v in date_differences.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--development-dir", required=True, type=Path)
    parser.add_argument("--confirmation-dir", type=Path)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()
    panels = {"开发": panel(args.development_dir, args.candidate)}
    if args.confirmation_dir:
        panels["确认"] = panel(args.confirmation_dir, args.candidate)
        development, confirmation = (p["manifest"] for p in panels.values())
        if (confirmation["args"]["split"] != "confirmation" or not confirmation.get("selection_sha256") or
                development["method_specs"][args.candidate] != confirmation["method_specs"][args.candidate]):
            raise ValueError("Confirmation must retain a preselected panel and identical candidate parameters")
        dev_dates = {row["calendar_day"] for row in development["case_info"]}
        if dev_dates & {row["calendar_day"] for row in confirmation["case_info"]}:
            raise ValueError("Development and confirmation dates overlap")
        if development["source_sha256"] != confirmation["source_sha256"]:
            raise ValueError("Candidate core changed between development and confirmation")
    args.out_dir.mkdir(parents=True, exist_ok=False)
    report = {"candidate": args.candidate, "primary_seconds": 300, "panels": panels,
              "reporter_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    (args.out_dir / "summary.json").write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    lines = ["# 短SLA候选配对研究", "", f"候选：`{args.candidate}`。主指标为全部图像任务释放后300秒内完整结果在地面聚合的比例；迟到任务继续运行。", "",
             "主列对场景等权，汇总计数另列。确认面板两种负载共享三个日期，因此不把每个任务或六个场景视为独立轨道重复。", ""]
    for title, data in panels.items():
        lines += [f"## {title}面板", "", "| 方法 | 按时/全部 | P300 | R300/s |", "|---|---:|---:|---:|"]
        for row in data["aggregate"]:
            lines.append(f"| {row['method']} | {row['on_time_tasks']}/{row['total_tasks']} | {row['on_time_rate']:.2%} | {row['capped_mean_latency_300s']:.2f} |")
        lines += ["", "| 相对基线 | 新增按时 | 丢失按时 | 场景胜/平/负 |", "|---|---:|---:|---:|"]
        for row in data["comparisons"]:
            lines.append(f"| {row['reference']} | {row['gained_on_time']} | {row['lost_on_time']} | {row['wins']}/{row['ties']}/{row['losses']} |")
        lines += ["", "| 方法 | 观测结束前完成 | 调度总耗时/s | 各场景P95最大值/ms |", "|---|---:|---:|---:|"]
        for row in data["costs"]:
            lines.append(f"| {row['method']} | {row['completed_tasks']} | {row['scheduler_total_seconds']:.2f} | {row['max_case_p95_ms']:.2f} |")
        lines += ["", "观测结束前完成不等同最终排空完成；只比较完整300秒观察的P300/R300。计时只包括本机select_actions，受并行负载影响；不是星载硬件测量，也未加入仿真中的任务时延。逐日、日期簇差值和源码哈希见summary.json。", ""]
    lines += ["## 结论边界", "",
              "这是规则候选与论文方法适配的比较，不是新MARL训练结果，也不是原论文完整复现或领域最优证明。CGS适配不具有原MSR的计划路径执行；DTS路径版将TMAX报价扩展到共同有界路径，不等同原文PropagateSearch。输入跳数以配置为准；不同公共执行能力间的收益不能单独归因于候选算法。", "",
              "现有场景采用公开标称带宽和受控算力代理；真实设备吞吐、控制消息、激光建链、地面去除重复overlap及更广星座规模仍需单独检验。研究过程与负结果见docs/thesis/experiment_log.md。", ""]
    (args.out_dir / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps({"report": str(args.out_dir / "REPORT.md"), "candidate": args.candidate}, ensure_ascii=False))


if __name__ == "__main__":
    main()
