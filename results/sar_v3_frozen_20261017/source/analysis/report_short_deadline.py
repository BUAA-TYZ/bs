"""按固定短时限重新评分开发轨迹；不改变任务执行和超时清理规则。"""

import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.evaluate_literature import common_context
from analysis.report_highres_scene import LABELS, ORDER
from sim.evaluation import paired_task_metrics, task_age_score


PRIMARY_SECONDS = 300
HORIZONS = [60, 120, 180, 300, 600]
DAILY_METHODS = ["ground_relay", "greedy", "heft_adapted", "cgs_compute_adapted"]


def score_record(record, horizons=None):
    horizons = HORIZONS if horizons is None else horizons
    cfg = record["metrics"]["experiment"]["config"]
    if cfg["deadline_steps"] != 0:
        raise ValueError("This report requires continue-after-SLA execution, not hard expiry")
    outcomes = record["metrics"]["task_outcomes"]
    end = cfg["sim_steps"] * cfg["dt"]
    if not outcomes:
        raise ValueError("Empty cases must be reported separately, not averaged as successes")
    # 主指标截断到300秒；不能误用敏感性曲线最大值600秒作时延上限。
    primary = task_age_score(outcomes, [PRIMARY_SECONDS], end)
    curve = task_age_score(outcomes, horizons, end)["delivery_curve"]
    return {"case": record["case"]["id"], "method": record["method"],
            "total_tasks": len(outcomes),
            "on_time_tasks": primary["delivery_curve"][0]["delivered_within_horizon"],
            "on_time_rate": primary["delivery_score"],
            "capped_mean_latency_300s": primary["restricted_mean_latency_seconds"],
            "curve": curve}


def aggregate(records, methods, horizons=None):
    horizons = HORIZONS if horizons is None else horizons
    rows = [score_record(record, horizons) for record in records]
    result = []
    for method in methods:
        selected = [row for row in rows if row["method"] == method]
        if not selected or len({r["case"] for r in selected}) != len(selected):
            raise ValueError("Each method requires distinct nonempty cases")
        result.append({"method": method, "cases": len(selected),
                       "on_time_tasks": sum(r["on_time_tasks"] for r in selected),
                       "total_tasks": sum(r["total_tasks"] for r in selected),
                       "on_time_rate": statistics.mean(r["on_time_rate"] for r in selected),
                       "capped_mean_latency_300s": statistics.mean(r["capped_mean_latency_300s"] for r in selected),
                       "delivery_rates": [statistics.mean(r["curve"][i]["delivery_rate"] for r in selected)
                                          for i in range(len(horizons))]})
    return result, rows


def compare_on_time(reference, candidate):
    ref = reference["metrics"]["task_outcomes"]
    cand = candidate["metrics"]["task_outcomes"]
    paired_task_metrics(ref, cand)
    def completed(rows):
        return {r["task_id"] for r in rows if r["status"] == "completed"
                and r["latency_seconds"] <= PRIMARY_SECONDS}
    ref_ids, cand_ids = completed(ref), completed(cand)
    return {"case": reference["case"]["id"], "reference": reference["method"],
            "candidate": candidate["method"], "total_tasks": len(ref),
            "both_on_time": len(ref_ids & cand_ids), "reference_only_on_time": len(ref_ids - cand_ids),
            "candidate_only_on_time": len(cand_ids - ref_ids),
            "neither_on_time": len(ref) - len(ref_ids | cand_ids)}


def load_panel(directories, expected_methods):
    records, sources = [], {}
    for directory in directories:
        summary = json.loads((directory / "summary.json").read_text())
        for name in summary["records"]:
            path = directory / name
            sources[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
            records.append(json.loads(path.read_text()))
    by_case = {}
    for record in records:
        by_case.setdefault(record["case"]["id"], []).append(record)
    for rows in by_case.values():
        if len(rows) != len(expected_methods) or {r["method"] for r in rows} != set(expected_methods):
            raise ValueError("Incomplete or duplicated comparison panel")
        if any(common_context(r) != common_context(rows[0]) for r in rows):
            raise ValueError("Unpaired task, source, orbit or configuration")
    return records, sources


def table(rows):
    lines = ["| 方法 | 5分钟按时任务/全部任务 | 1分钟 | 2分钟 | 3分钟 | **5分钟主指标** | 10分钟 | 300秒截断均值(s) |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        rates = " | ".join(f"{100 * value:.2f}%" for value in row["delivery_rates"])
        lines.append(f"| {LABELS[row['method']]} | {row['on_time_tasks']}/{row['total_tasks']} | {rates} | "
                     f"{row['capped_mean_latency_300s']:.2f} |")
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="results/highres_short_sla_20261015")
    args = parser.parse_args()
    root = Path("results")
    nominal, nominal_sources = load_panel([root / f"highres_fixed_nominal_{group}_20261015"
                                          for group in ("ground", "star", "literature")], ORDER)
    daily, daily_sources = load_panel([root / "highres_fixed_three_days_20261015",
                                      root / "highres_short_cgs_20261015"], DAILY_METHODS)
    main_rows, daily_rows = aggregate(daily, DAILY_METHODS)
    first_rows, _ = aggregate(nominal, ORDER)
    comparisons = []
    for case in sorted({r["case"]["id"] for r in daily}):
        ref = next(r for r in daily if r["case"]["id"] == case and r["method"] == "heft_adapted")
        for method in ("greedy", "cgs_compute_adapted"):
            cand = next(r for r in daily if r["case"]["id"] == case and r["method"] == method)
            comparisons.append(compare_on_time(ref, cand))
    data = {"scope": "development re-scoring; no hard expiry, no new learned policy, not an independent test",
            "primary_seconds": PRIMARY_SECONDS, "sensitivity_seconds": HORIZONS,
            "aggregation": "case equal for rates and capped latency; pooled counts also disclosed",
            "deadline_disposition": "late tasks continue and retain physical resource competition",
            "source_files_sha256": {**nominal_sources, **daily_sources},
            "reporter_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "first_day": first_rows, "three_days": main_rows, "daily": daily_rows,
            "paired_on_time_against_heft": comparisons}
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "summary.json").write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    lines = ["# 高分任务短时限开发评估", "",
             "主指标为任务释放后300秒内完整结果在地面聚合完成的比例；1、2、3、10分钟只作敏感性，不再平均成一个长短时限混合J。", "",
             "这是已查看开发数据的重新评分。三天任务轨迹不变，CGS补跑同一源码与配置；没有训练新模型，也没有启用300秒硬超时取消。迟到任务继续运行并占用资源，四小时只用于诊断最终交付。主时限由研究业务要求选定，不是遥感行业统一标准。", "",
             "## 连续三个日期", ""]
    lines += table(main_rows)
    lines += ["", "比例和截断时延为日期等权，计数为三日汇总，因此计数相除可能不同于主列。未交付、失败以及超过300秒才交付的任务均计为不按时；其300秒截断时延均为300秒。", "",
              "## 首日全部对照", ""]
    lines += table(first_rows)
    lines += ["", "HEFT与CGS为明确标注的论文方法适配，不是原文整套实验复现。greedy、window_aware是工程对照，不能替代文献对照。不能将任何基线的最好数字当作本文新方法成绩。", "",
              "## 解释与验收", "",
              "- 5分钟是本轮起拟定的主业务SLA；已经查看的三天仍归开发集，不能作为预注册后的封存测试。",
              "- 本报告的1/2/3/10分钟曲线来自同一执行轨迹，不是分别为各期限训练或运行的策略。",
              "- 硬截止取消会释放队列、链路和缓存，改变后续任务结果；若采用该机制，必须让全部方法在相同取消规则下重新运行。",
              "- 正式验收需要冻结时限、方法配置和未见日期/相位/负载，按独立场景进行配对比较；三天不足以支持显著性或领域最优结论。",
              "- 算力、输出量、聚合、零建链时间和地面重复overlap等限制继续适用，见[原实验报告](../highres_fixed_study_20261015/REPORT.md)。", "",
              "研究协议与文献复现要求：[短时限协议](../../docs/thesis/short_deadline_protocol.md)。逐日和按时任务新增/丢失计数见同目录summary.json。", ""]
    (out / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps({"report": str(out / "REPORT.md"), "three_days": main_rows}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
