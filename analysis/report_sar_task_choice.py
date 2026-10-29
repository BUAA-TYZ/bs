"""同源码SAR面板的整图配对报告；不混合不同物理配置或只筛成功任务。"""

import argparse
import hashlib
import json
from pathlib import Path
import statistics


def compare(reference, candidate):
    left = reference["metrics"]["task_outcomes"]
    right = candidate["metrics"]["task_outcomes"]
    identity = lambda rows: [(r["task_id"], r["source_sat_id"], r["release_seconds"]) for r in rows]
    if identity(left) != identity(right):
        raise ValueError("Different task identity or release trace")
    clipped = lambda row: min(row["latency_seconds"], 300) if row["status"] == "completed" else 300
    changes = [{"task_id": a["task_id"], "reference_R300": clipped(a),
                "candidate_R300": clipped(b), "delta_seconds": clipped(b) - clipped(a)}
               for a, b in zip(left, right)]
    old, new = set(reference["on_time_task_ids"]), set(candidate["on_time_task_ids"])
    return {"reference": reference["method"], "candidate": candidate["method"],
            "gained": sorted(new - old), "lost": sorted(old - new),
            "faster": sum(r["delta_seconds"] < -1e-8 for r in changes),
            "slower": sum(r["delta_seconds"] > 1e-8 for r in changes),
            "equal": sum(abs(r["delta_seconds"]) <= 1e-8 for r in changes),
            "mean_delta_seconds": statistics.mean(r["delta_seconds"] for r in changes),
            "tasks": changes}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("panel", type=Path)
    parser.add_argument("--bound", type=Path)
    parser.add_argument("--previous-panel", type=Path, help="same-code control differing only in relay_max_hops")
    args = parser.parse_args()
    summary = json.loads((args.panel / "summary.json").read_text())
    records = {row["method"]: json.loads((args.panel / (row["method"] + ".json")).read_text())
               for row in summary["rows"]}
    for record in records.values():
        if record["metrics"]["experiment"]["source_sha256"] != summary["source_sha256"]:
            raise ValueError("Mixed source code")
        if record["derived_config_sha256"] != summary["derived_config_sha256"] or not record["invariants"]["passed"]:
            raise ValueError("Mixed config or failed execution audit")
    candidate = records["cgs_task_choice"]
    bound = None
    if args.bound:
        bound = json.loads(args.bound.read_text())
        if bound["config_sha256"] != hashlib.sha256((args.panel / "derived_config.json").read_bytes()).hexdigest():
            raise ValueError("Bound does not match the exact panel config")
        identity = [(r["task_id"], r["source_sat_id"], r["release_seconds"]) for r in bound["tasks"]]
        if identity != [(r["task_id"], r["source_sat_id"], r["release_seconds"])
                        for r in candidate["metrics"]["task_outcomes"]]:
            raise ValueError("Bound task trace differs")
        for name, record in records.items():
            mode = "local_or_ground" if name in ("local", "ground_unique_relay") else "one_onboard_placement"
            possible = {r["task_id"] for r in bound["tasks"] if r[mode]["potentially_on_time"]}
            if set(record["on_time_task_ids"]) - possible:
                raise ValueError(f"Execution contradicts the bound: {name}")
    pairs = [compare(record, candidate) for name, record in records.items() if name != "cgs_task_choice"]
    protocol_pairs = []
    if args.previous_panel:
        old_cfg = json.loads((args.previous_panel / "derived_config.json").read_text())
        new_cfg = json.loads((args.panel / "derived_config.json").read_text())
        if {k for k in old_cfg.keys() | new_cfg.keys() if old_cfg.get(k) != new_cfg.get(k)} != {"relay_max_hops"}:
            raise ValueError("Protocol comparison may change only relay_max_hops")
        for name, record in records.items():
            old = json.loads((args.previous_panel / (name + ".json")).read_text())
            if old["metrics"]["experiment"]["source_sha256"] != summary["source_sha256"]:
                raise ValueError("Protocol comparison requires identical simulator code")
            protocol_pairs.append({**compare(old, record), "old_hops": old_cfg["relay_max_hops"],
                                   "new_hops": new_cfg["relay_max_hops"], "old_on_time": old["on_time_tasks"],
                                   "new_on_time": record["on_time_tasks"], "old_R300": old["R300_seconds"],
                                   "new_R300": record["R300_seconds"],
                                   "old_record_sha256": hashlib.sha256((args.previous_panel / (name + ".json")).read_bytes()).hexdigest()})
    report = {"scope": summary["scope"], "tasks": summary["tasks"],
              "source_sha256": summary["source_sha256"], "paired": pairs,
              "input_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                               for p in [args.panel / (name + ".json") for name in records]},
              "scheduler_diagnostics": candidate["metrics"].get("scheduler_diagnostics", {})}
    if protocol_pairs:
        report["public_protocol_comparison"] = protocol_pairs
    if bound:
        report["hop_bound"] = {"path": str(args.bound), "sha256": hashlib.sha256(args.bound.read_bytes()).hexdigest(),
                               "budgets": bound["budgets"], "upper_counts": bound["upper_counts"]}
    lines = ["# 整图方案复核开发对比", "", f"范围：{summary['stop_seconds']}秒、{summary['tasks']}图。"
             "固定开发日，非独立测试；整图300秒交付率优先。", "",
             "全部方法共用temporal中继和公共窗口修复，源代码及派生配置哈希相同，执行审计通过。",
             "地面对照发送唯一像素，输入块字节与星上不同；任务ID、源星、就绪时刻逐项核验。", "",
             "| 方法 | 按时图数 | P300 | R300/秒 | 活跃决策P95/ms | 计算星全时段利用率 |",
             "|---|---:|---:|---:|---:|---:|"]
    for name, record in records.items():
        p95 = record["timing"]["active_select_p95_ms"]
        p95_text = "N/A" if p95 is None else f"{p95:.3f}"
        util = record["resources"]["helper"]["utilization_full_horizon"]
        lines.append(f"| {name} | {record['on_time_tasks']}/{record['total_tasks']} | {record['P300']:.2%} | "
                     f"{record['R300_seconds']:.4f} | {p95_text} | {util:.4%} |")
    lines.extend(["", "R300对未交付及超过300秒的任务均计300秒；没有仅平均成功任务。",
                  "决策耗时为本机并行运行测量，不是Jetson数据；利用率包含无任务空闲时间。", "",
                  "## 新候选逐任务增减", ""])
    for pair in pairs:
        lines.append(f"- 相对{pair['reference']}：新增按时{pair['gained']}；丢失按时{pair['lost']}；"
                     f"截断时延更快/更慢/相同={pair['faster']}/{pair['slower']}/{pair['equal']}，"
                     f"平均差{pair['mean_delta_seconds']:.4f}秒。")
    lines.extend(["", "完整逐任务差值见paired_report.json。没有从开发样本推断统计显著性或全天泛化。",
                  "CGS/HEFT是计算任务适配，不是原论文最强版本完整复现；当前候选也不是MARL。",
                  "候选只复核全本地与CGS两套分配，输入整批估计保守，输出仍非精确预约日历。",
                  "scheduler_diagnostics的requests/accepted记录CGS备选生成，不是最终执行计数；"
                  "choice_cgs_tasks也不保证每图实际使用外部计算星。"])
    if bound:
        lines.extend(["", "## 有限跳数上界", "",
                      f"本地/地面最多{bound['budgets']['local_or_ground']}次星间跳转的零代价上界为"
                      f"{bound['upper_counts']['local_or_ground']}图；一次星上卸载+结果中继的总跳数"
                      f"松弛上界为{bound['upper_counts']['one_onboard_placement']}图。全部执行见证未违反上界。",
                      "这里允许完美未来、零计算、零传输、无限资源、多副本保留，且放松输入等待和计算落点；"
                      "仅为必要条件，不能把上界以内的失败全部视作算法一定可修复。",
                      "该跳数预算取自面板配置；N-1用于覆盖无环路径，旧8跳为数值/搜索截断。该上界是协议约束下的结果，"
                      "不是卫星网络普遍的物理极限。"])
    if protocol_pairs:
        lines.extend(["", "## 公共协议变化", "", "以下比较同一方法在旧/新跳数预算下的变化，不能归为新方法收益。", "",
                      "| 方法 | 旧/新按时图数 | 旧/新R300（秒） | 新增按时 | 丢失按时 |",
                      "|---|---:|---:|---|---|"])
        for row in protocol_pairs:
            lines.append(f"| {row['candidate']} | {row['old_on_time']} / {row['new_on_time']} | "
                         f"{row['old_R300']:.4f} / {row['new_R300']:.4f} | {row['gained']} | {row['lost']} |")
        lines.extend(["", "配置差异严格仅relay_max_hops，仿真核心源码相同；批量脚本新增路径无回访/跳数审计。",
                      "实际执行的最大路径与超过8跳的路径数见各原始记录invariants.relay_hops；"
                      "允许更长路径不会预先改变每跳字节、带宽或计算成本。"])
    for name in ("paired_report.json", "REPORT.md"):
        if (args.panel / name).exists():
            raise ValueError("Refusing to overwrite an existing report")
    (args.panel / "paired_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False))
    (args.panel / "REPORT.md").write_text("\n".join(lines) + "\n")
    print(json.dumps([{k: p[k] for k in ("reference", "gained", "lost", "mean_delta_seconds")} for p in pairs]))


if __name__ == "__main__":
    main()
