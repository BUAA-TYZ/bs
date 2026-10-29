"""只读已完成试点，核验封存信息，汇报标签和选错时延损失。"""

from collections import Counter
import ast
import hashlib
import json
import math
from pathlib import Path
import statistics

PROJECT = Path(__file__).resolve().parents[1]
OUT = PROJECT / "results/sar_selector_pilot_20261019"
FROZEN = PROJECT / "results/sar_v3_frozen_20261017"
PLANS = ("local", "cgs", "ground")


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def latency_regret(state):
    if state["label"] is None:
        return None
    branches = state["branches"]
    best = branches[state["label"]]["completion_seconds"]
    chosen = branches[state["rule_choice"]]
    finish = chosen["completion_seconds"]
    if finish is None:
        return {"exact_seconds": None, "strict_lower_bound_seconds": chosen["censor_seconds"] - best}
    assert finish >= best, "Selected fastest label is inconsistent with observed completion times"
    return {"exact_seconds": max(0., finish - best), "strict_lower_bound_seconds": None}


def regret_summary(states):
    wrong = [s for s in states if s["label"] is not None and not s["agrees"]]
    rows = [latency_regret(s) for s in wrong]
    exact = [r["exact_seconds"] for r in rows if r["exact_seconds"] is not None]
    censored = [r["strict_lower_bound_seconds"] for r in rows if r["strict_lower_bound_seconds"] is not None]
    return {"wrong_labels": len(wrong), "exact_count": len(exact), "censored_count": len(censored),
            "mean_exact_seconds": statistics.mean(exact) if exact else None,
            "median_exact_seconds": statistics.median(exact) if exact else None,
            "max_exact_seconds": max(exact) if exact else None,
            "all_wrong_mean_lower_bound_seconds": math.fsum(exact + censored) / len(wrong) if wrong else 0.,
            "all_wrong_mean_is_strict_lower_bound": bool(censored)}


def main():
    result = read(OUT / "summary.json")
    manifest = read(OUT / "manifest.json")
    assert result["status"] in ("STOP", "PILOT_PASS_AWAIT_CONFIRMATION", "BLOCKED")
    for registry in ("registration_hashes.json", "approved_registration_hashes.json"):
        for filename, expected in read(OUT / registry).items():
            assert sha(OUT / filename) == expected, filename
    assert sha(PROJECT / "analysis/sar_selector_pilot.py") == manifest["runner_sha256"]
    files = read(FROZEN / "freeze.json")["files"]
    for filename, expected in files.items():
        assert sha(FROZEN / "source" / filename) == expected, filename
        assert sha(PROJECT / filename) == expected, filename
    for job in manifest["jobs"]:
        assert sha(Path(job["config"])) == job["config_sha256"]
    states = result["states"]
    trace_controls = []
    for condition in result["conditions"]:
        if condition["status"] == "PASS":
            continue
        trace = condition.get("traceback", "")
        # 完整性断言同时保存了已取得状态的控制核验；读取证据，不修改失败状态。
        marker = "AssertionError: "
        if marker in trace:
            literal = trace.rsplit(marker, 1)[1].strip()
            try:
                payload = ast.literal_eval(literal)
            except (ValueError, SyntaxError):
                continue
            if isinstance(payload, list) and all(isinstance(a, dict) and "passed" in a for a in payload):
                trace_controls.extend({"condition": condition["condition"], **a, "evidence": "failed condition traceback"}
                                      for a in payload)
    expected = {(j["condition"], s["task_id"]) for j in manifest["jobs"] for s in j["samples"]}
    actual = {(s["condition"], s["task_id"]) for s in states}
    assert len(actual) == len(states) and actual <= expected
    if result["status"] != "BLOCKED":
        assert actual == expected and len(states) == 40
        assert all(c["status"] == "PASS" and len(c["control_audits"]) == 8
                   and all(a["passed"] for a in c["control_audits"]) for c in result["conditions"])
    grouped = []
    for job in manifest["jobs"]:
        group = [s for s in states if s["condition"] == job["condition"]]
        counts = Counter(s["label"] for s in group if s["label"] is not None)
        grouped.append({"condition": job["condition"], "observed": len(group), "determinate": sum(counts.values()),
                        "indeterminate": sum(s["status"] == "INDETERMINATE" for s in group),
                        "labels": {p: counts[p] for p in PLANS}, "agreements": sum(s["agrees"] is True for s in group),
                        "regret": regret_summary(group)})
    counts = Counter(s["label"] for s in states if s["label"] is not None)
    details = []
    for s in states:
        meta = read(OUT / "states" / s["condition"] / s["task_id"] / "state.json")
        for p, r in s["branches"].items():
            assert r["state_fingerprint"] == meta["state_fingerprint"] and r["parent_pid"] == meta["parent_pid"]
            assert r["task_id"] == s["task_id"] and r["plan"] == p
            assert r["status"] == "PASS"
        complete = [p for p in PLANS if s["branches"][p]["completion_seconds"] is not None]
        if s["label"] is not None:
            winner = min(complete, key=lambda p: s["branches"][p]["completion_seconds"])
            assert s["label"] == winner
            finish = s["branches"][winner]["completion_seconds"]
            assert all(r["completion_seconds"] is not None or r["censor_seconds"] > finish
                       for r in s["branches"].values())
        details.append({"condition": s["condition"], "task_id": s["task_id"], "status": s["status"],
                        "rule_choice": s["rule_choice"], "label": s["label"], "agrees": s["agrees"], "ties": s["ties"],
                        "regret": latency_regret(s), "quotes_seconds": meta["quotes_seconds"],
                        "actual_latencies_seconds": {p: r["latency_seconds"] for p, r in s["branches"].items()},
                        "censored_latency_lower_bounds_seconds": {p: r["censor_seconds"] - r["release_seconds"]
                            for p, r in s["branches"].items() if r["completion_seconds"] is None}})
    metrics = {"status": result["status"], "expected_states": 40, "observed_states": len(states),
               "missing_states": [{"condition": c, "task_id": t} for c, t in sorted(expected - actual)],
               "agreement": result["agreement"], "tie_aware_agreement": result["tie_aware_agreement"],
               "fastest_tie_states": sum(len(s["ties"]) > 1 for s in states),
               "indeterminate_fraction": result["indeterminate_fraction"],
               "labels": {p: counts[p] for p in PLANS}, "regret": regret_summary(states),
               "per_condition": grouped, "details": details,
               "audit": {"frozen_files_snapshot_and_live_unchanged": len(files), "configurations_unchanged": 5,
                         "matched_control_branches": sum(sum(a["passed"] for a in c.get("control_audits", [])) for c in result["conditions"]),
                         "matched_controls_in_failure_evidence": sum(a["passed"] for a in trace_controls),
                         "failure_evidence_controls": trace_controls,
                         "unforced_decisions_matched": sum(c.get("unforced_selector_audit_calls", 0) for c in result["conditions"]),
                         "registration_hashes_verified": True, "branch_records": sum(len(s["branches"]) for s in states)},
               "cost": {"condition_wall_seconds": {c["condition"]: c.get("wall_seconds") for c in result["conditions"]},
                        "branch_worker_seconds": result["branch_worker_seconds"],
                        "recorded_cpu_hours": (sum(c.get("parent_cpu_seconds", 0) for c in result["conditions"]) + result["branch_cpu_seconds"]) / 3600,
                        "cpu_accounting_complete": all("parent_cpu_seconds" in c for c in result["conditions"])},
               "source_summary_sha256": sha(OUT / "summary.json"), "blind_dates_sha256": sha(OUT / "blind_dates.json"),
               "blind_methods_amendment_sha256": sha(OUT / "blind_methods_amendment.json"),
               "report_script_sha256": sha(Path(__file__)), "full_labels_started": False, "training_started": False}
    (OUT / "pilot_metrics.json").write_text(json.dumps(metrics, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    agreement = "NA" if metrics["agreement"] is None else f"{metrics['agreement']:.2%}"
    lines = ["# 监督式方案选择器：40状态试点", "", f"结论：**{metrics['status']}**。试点后停止，未生成全量标签、未训练、未运行新盲测。", "",
             f"训练日2026-10-15；五条件，每格8个预先抽取状态，共观察{len(states)}/40状态、{metrics['audit']['branch_records']}/120条分支。",
             f"缺失抽样状态：{metrics['missing_states']}。若缺失非空，以下比例只描述已取得记录，不能判为完整试点通过。",
             f"一致率 **{agreement}**（可判定状态中v3等于最快标签）；不可判定率 **{metrics['indeterminate_fraction']:.2%}**（分母固定40）。",
             f"并列最快状态{metrics['fastest_tie_states']}个；将选中任意并列最快均计为正确时，一致率为{metrics['tie_aware_agreement']}，只作补充，不替代预登记标签判定。",
             f"可判定最快标签：本地{counts['local']}、CGS{counts['cgs']}、地面{counts['ground']}。",
             "停止规则：一致率≥90%或不可判定率>40%则停止；否则也必须先汇报、等待用户确认才能全量。执行/审计错误单列BLOCKED，不冒充不可判定或P300=0。", "",
             "## 分条件结果", "", "|条件|完成状态|可判定|本地标签|CGS标签|地面标签|一致|不可判定|选错损失均值 s（准确子集）|",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in grouped:
        regret = r["regret"]
        loss = "NA" if regret["mean_exact_seconds"] is None else f"{regret['mean_exact_seconds']:.4f} (n={regret['exact_count']})"
        lines.append(f"|{r['condition']}|{r['observed']}/8|{r['determinate']}|{r['labels']['local']}|{r['labels']['cgs']}|"
                     f"{r['labels']['ground']}|{r['agreements']}/{r['determinate']}|{r['indeterminate']}/8|{loss}|")
    regret = metrics["regret"]
    lines += ["", "## 选错方案的时延损失", "",
              "损失=同状态v3所选方案实际完整交付时延减最快方案实际时延，不截断到300秒。若v3分支未完成，只报告删失下界，不把7200秒当作精确时延。",
              f"选错{regret['wrong_labels']}状态：准确观测{regret['exact_count']}、删失{regret['censored_count']}。",
              f"准确观测子集：均值{regret['mean_exact_seconds']}秒，中位数{regret['median_exact_seconds']}秒，最大{regret['max_exact_seconds']}秒。",
              f"全部选错状态均值{'严格下界' if regret['all_wrong_mean_is_strict_lower_bound'] else ''}：{regret['all_wrong_mean_lower_bound_seconds']}秒。",
              "精确平局按本地、CGS、地面顺序；其他并列最快选项可能计为标签不一致，但损失为0；逐状态保留并列信息。", "",
              "## 审计与成本", "",
              f"冻结源码159份（快照及工作区）、5份试点配置、抽样清单、日期/方法哈希核对通过。已完成条件报告确认控制分支通过{metrics['audit']['matched_control_branches']}/40；失败完整性断言另保留{metrics['audit']['matched_controls_in_failure_evidence']}条通过的控制记录。无干预逐决策已完整记录{metrics['audit']['unforced_decisions_matched']}次，未含失败条件母进程计数。",
              "fork复制完整环境、在途传输、影子日历、调用栈与RNG；逐分支核验继承指纹，原选择对应分支与母轨迹比较完成时间。Python报告原生线程fork兼容性警告，实际退出及重放审计均核验；不据此保证跨机器恢复能力，未永久序列化完整环境。",
              "每分支最长7200仿真秒，已完成赢家须早于其他删失下界。报价为None不代表物理不可行，仍强制原计划执行。当日到达固定，只延长末图观察，未生成次日任务。私有真值和真实未来仅用于离线回放。",
              f"已有计时记录CPU合计{metrics['cost']['recorded_cpu_hours']:.3f}核小时；计时完整性={metrics['cost']['cpu_accounting_complete']}，若为False则未含失败条件母进程完整耗时，只是下界。分支合计{metrics['cost']['branch_worker_seconds']:.1f} worker秒，非面板墙钟。", "",
              "## 新盲测封存", "",
              "2026-12-15至12-17，五条件×v3/冻结学习选择器/旧两方案，共45次全天；只登记，未生成、查看或运行。须模型冻结且获准后再启封。",
              f"日期SHA256：{metrics['blind_dates_sha256']}。",
              f"批准的方法补充登记SHA256：{metrics['blind_methods_amendment_sha256']}。",
              "修改版见../sar_v3_frozen_20261017/LEARNING_PROPOSAL.md，最多分类/回归两个变体，同输入同数据，验证P300优先、R300次之。",
              "定位为监督式方案选择器，不称多智能体强化学习；局部反事实最快标签不构成整日全局最优，也不保证学习后P300提高。", "",
              "## 逐状态记录", "", "单位秒。NA为未完成；删失下界及原报价详见pilot_metrics.json。", "",
              "|条件|图像|v3|最快标签|本地实际|CGS实际|地面实际|v3损失|", "|---|---|---|---|---:|---:|---:|---:|"]
    for r in details:
        values = ["NA" if r["actual_latencies_seconds"][p] is None else f"{r['actual_latencies_seconds'][p]:.2f}" for p in PLANS]
        loss = r["regret"]
        loss_text = "NA" if loss is None else (f"{loss['exact_seconds']:.2f}" if loss["exact_seconds"] is not None else f">{loss['strict_lower_bound_seconds']:.2f}")
        lines.append(f"|{r['condition']}|{r['task_id']}|{r['rule_choice']}|{r['label'] or r['status']}|" + "|".join(values) + f"|{loss_text}|")
    if result["status"] == "BLOCKED":
        lines += ["", "## 抽样完整性异常", "",
                  "本次误把到达图像作为抽样母体；load_4/task_535未进入完整图首次选择，因而没有该状态的三分支。已运行117条分支均正常，不属于仿真算法失败。",
                  "这不是不可判定标签，而是未取得决策状态。不能把0/40不可判定解释为完整40状态覆盖，也不能以39状态替代验收。本轮不替换样本、不重跑；停止等待用户确认如何补齐。",
                  "建议后续只修正抽样母体为实际可干预的首次整图决策状态，在查看补充标签前固定抽取规则和哈希；不动方法或场景。",
                  "以下保留失败条件的原始完整性断言和配置：", "",
                  json.dumps([c for c in result["conditions"] if c["status"] != "PASS"], ensure_ascii=False, indent=2)]
    (OUT / "REPORT.md").write_text("\n".join(lines) + "\n")
    final_status = {"status": result["status"], "completed_at_utc": result["completed_at_utc"],
                    "observed_states": len(states), "missing_states": metrics["missing_states"],
                    "branch_records": metrics["audit"]["branch_records"],
                    "full_labels_started": False, "training_started": False,
                    "next_action": "STOP; wait for user decision on correcting sampling and filling the missing pilot state",
                    "note": "progress/load_4.json is the last in-run snapshot; terminal status is in conditions/load_4.json and this file"}
    (OUT / "final_status.json").write_text(json.dumps(final_status, indent=2, ensure_ascii=False) + "\n")
    hashes = {str(p.relative_to(OUT)): sha(p) for p in sorted(OUT.rglob("*.json"))
              if p.name != "artifact_hashes.json"}
    hashes["REPORT.md"] = sha(OUT / "REPORT.md")
    (OUT / "artifact_hashes.json").write_text(json.dumps(hashes, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({k: metrics[k] for k in ("status", "agreement", "indeterminate_fraction", "labels", "regret")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
