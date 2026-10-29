"""三方案候选验收、逐任务增减、诊断及未运行测试的工时估算。"""

import argparse
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.sar_condition_scan import file_hash, write_json
from analysis.run_sar_three_plan import ROOT, SCAN
from analysis.report_sar_task_choice import compare
from analysis import run_sar_frozen as runner
from sim.reproducibility import runtime_metadata


def diagnostic(manifest):
    records = {name: json.loads((ROOT / "diagnostic" / name / "record.json").read_text())
               for name in ("replay", "force_lost_cgs", "force_baseline_cgs", "force_predecessors_cgs")}
    old = records["replay"]
    cgs_path = next(Path(j["path"]) for j in manifest["jobs"]
                    if j["condition"] == "compute_10" and j["variant"] == "cgs")
    cgs = json.loads(cgs_path.read_text())
    lost = sorted(set(cgs["on_time_task_ids"]) - set(old["on_time_task_ids"]), key=lambda t: int(t.split("_")[1]))
    rows = []
    for task in lost:
        d = next(d for d in old["decisions"] if task in d["assignments"])
        o = next(o for o in old["metrics"]["task_outcomes"] if o["task_id"] == task)
        quoted = {name: None if q is None else q[task] for name, q in d["prediction"]["quotes"].items()}
        actual = {name: next(o["latency_seconds"] for o in r["metrics"]["task_outcomes"] if o["task_id"] == task)
                  for name, r in {**records, "pure_cgs": cgs}.items()}
        rows.append({"task_id": task, "source_sat_id": o["source_sat_id"], "release_seconds": o["release_seconds"],
                     "decision_seconds": d["time_seconds"], "selected": d["prediction"]["selected"],
                     "predicted_latency_seconds": quoted,
                     "predicted_completion_seconds": {k: None if v is None else v + o["release_seconds"] for k, v in quoted.items()},
                     "actual_latency_seconds": actual, "actual_completion_seconds": o["completion_seconds"],
                     "selection_failure": "false_infeasible_cgs_quote" if quoted["cgs"] is None else "clipped_over_300_tie_prefers_local",
                     "predecessor_intervention_recovers_remaining_loss": task in ("task_2", "task_11")})
    write_json(ROOT / "diagnostic" / "summary.json", {
        "lost_tasks": rows, "readonly_replay_exact": old["outcomes_identical_to_reference"],
        "group_interventions": {name: {"on_time": r["on_time_tasks"], "paired": compare(old, r)}
                                for name, r in records.items() if name != "replay"},
        "limitation": "Joint interventions change later histories; pure CGS is a separate trajectory, not same-state ground truth."})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("variant", choices=("v1", "v2", "v3"))
    args = parser.parse_args()
    manifest = json.loads((SCAN / "manifest.json").read_text())
    old_audit = json.loads((SCAN / "audit.json").read_text())
    assert old_audit["passed"] and old_audit["manifest_sha256"] == file_hash(SCAN / "manifest.json")
    diagnostic(manifest)
    panel = ROOT / args.variant
    selected = json.loads((panel / "summary.json").read_text())
    source = runtime_metadata()["source_sha256"]
    assert source == manifest["source_sha256"]
    for condition in manifest["conditions"].values():
        for entry in condition["configs"].values():
            assert file_hash(Path(entry["path"])) == entry["sha256"]
    inputs = {}
    worker_seconds = 0
    old_candidate_seconds = 0
    for job in manifest["jobs"]:
        path = Path(job["path"])
        r = json.loads(path.read_text())
        assert r["invariants"]["passed"] and r["metrics"]["experiment"]["source_sha256"] == source
        worker_seconds += r["wall_seconds"]
        old_candidate_seconds += r["wall_seconds"] if job["variant"] == "choice" else 0
        inputs[str(path)] = file_hash(path)
    new_seconds = sum(json.loads((panel / f"{r['condition']}.json").read_text())["wall_seconds"] for r in selected["rows"])
    hours = (worker_seconds + new_seconds) * 4 * 6 / 3600
    replace_hours = (worker_seconds - old_candidate_seconds + new_seconds) * 4 * 6 / 3600
    geometry_path = Path("results/sar_coastal_coverage_20261015/coverage.json")
    geometry = json.loads(geometry_path.read_text())["verification"]
    geometry_seconds = geometry["geometry_elapsed_s"] * 53 * 24 / geometry["source_trajectories_propagated"]
    estimate = {"executed": False, "dates": 6, "conditions": 7, "methods_with_old_candidate_and_two_ground_protocols": 7,
                "runs": 294, "simulation_worker_hours_linear": hours, "four_workers_wall_hours_linear": hours / 4,
                "planning_worker_hours_range": [hours * 1.5 + 1, hours * 2 + 1],
                "four_workers_planning_wall_hours_range": [hours * 1.5 / 4 + 1, hours * 2 / 4 + 1],
                "acquisition_geometry_estimate": {"development_record": str(geometry_path),
                    "development_elapsed_s": geometry["geometry_elapsed_s"], "continuous_days": 53,
                    "frozen_sources": 24, "linear_seconds": geometry_seconds,
                    "one_time_preparation_allowance_hours": 1,
                    "caveat": "Linear source-days scaling only; one hour is an explicit planning allowance for generation, serialization and checks, not a measurement."},
                "if_new_replaces_old_candidate": {"runs": 252, "worker_hours": replace_hours, "four_workers_wall_hours": replace_hours / 4},
                "measured_old_seven_conditions_worker_seconds": worker_seconds,
                "measured_new_seven_conditions_worker_seconds": new_seconds,
                "assumptions": ["6-hour observed wall times scale by 4 to a day, then by 6 held-out dates.",
                                "Worker hours sum per-worker wall time, not measured aggregate CPU seconds.",
                                "1.5-2 planning multiplier is a declared allowance, not a statistical interval.",
                                "Four CPU workers on same host; no GPU training or device measurement.",
                                "Unknown held-out task counts and accumulated queues motivate 1.5-2 simulation allowance.",
                                "Acquisition geometry extrapolated from old development metadata; one-time preparation budget is serial.",
                                "No held-out configuration, geometry or arrivals generated/read; only preregistration text read."]}
    write_json(ROOT / "test_runtime_estimate.json", estimate)
    lines = ["# 三方案整图候选修正", "", f"最终汇报版本：{args.variant}；验收：{'通过' if selected['passed'] else '未通过'}。",
             "开发日6小时，7个单因素条件；测试封存。P300为整图300秒内完整交付，R300包含失败/超时300秒惩罚。",
             "公共仿真核心与路由哈希与旧记录一致；候选扩展单独归档和哈希，不把新增地面分支冒充旧策略。", "",
             "## 第一步诊断", "", "旧候选相对CGS丢失的9图全部选本地。5图CGS报价为不可行；4图两方案均超过300秒，被截断为平局后选本地。",
             "仅对这9图强制内部CGS：19→24，无丢失；改用基线CGS：19→26，无丢失。task_2/11仍迟到。",
             "再将前序task_1/9/10也改用CGS，28/42且无新增丢失；task_2从455.37降到275.34秒、task_11从1516.18降到142.54秒。",
             "因此当前误判与历史资源竞争均有执行干预支持；仍不能将组干预当作9次独立因果分摊，28/42不是新算法成绩。",
             "完整9行预测/实际表见README；带绝对时刻的数据见diagnostic/summary.json。诊断已在实现前向用户汇报。", "",
             "## 七条件结果", "", "单元格为P300（按时数/总数） / R300秒 / 辅助星全时段计算利用率%。", "",
             "|条件|新候选|旧候选|CGS|强地面|门槛|验收|", "|---|---|---|---|---|---:|---|"]
    def cell(record):
        return f"{record['on_time_tasks']}/{record['total_tasks']} / {record['R300_seconds']:.2f} / {record['resources']['helper']['utilization_full_horizon']*100:.4f}"
    residual = []
    for row in selected["rows"]:
        condition = row["condition"]
        path = panel / f"{condition}.json"
        new = json.loads(path.read_text())
        assert new["invariants"]["passed"] and new["unique_ground_audit"]["passed"]
        assert new["metrics"]["experiment"]["source_sha256"] == source
        frozen = json.loads((panel / "manifest.json").read_text())
        assert new["extension_sha256"] == frozen["extension_sha256"] == file_hash(panel / "candidate_source.py")
        assert new["derived_config_sha256"] == runner.digest(json.loads(Path(row["config"]).read_text()))
        for task in new["unique_ground_tasks"]:
            placement = new["compute_placement_windows"][task]
            assert not sum(v for k, v in placement.items() if k != "ground_windows")
        outcomes = new["metrics"]["task_outcomes"]
        on_time = {o["task_id"] for o in outcomes if o["status"] == "completed" and o["latency_seconds"] <= 300}
        r300 = sum(min(o["latency_seconds"], 300) if o["status"] == "completed" else 300 for o in outcomes) / len(outcomes)
        assert on_time == set(new["on_time_task_ids"]) and math.isclose(new["P300"], len(on_time) / len(outcomes))
        assert math.isclose(r300, new["R300_seconds"])
        helper = new["resources"]["helper"]
        assert math.isclose(helper["utilization_full_horizon"], helper["busy_seconds"] / (helper["satellites"] * new["observation_end_seconds"]))
        for task in row["paired"]["choice"]["lost"]:
            residual.append({"condition": condition, "task_id": task,
                             "candidate_outcome": next(o for o in outcomes if o["task_id"] == task),
                             "decisions": [d for d in new["decisions"] if d["task_id"] == task]})
        inputs[str(path)] = file_hash(path)
        old = {j["variant"]: json.loads(Path(j["path"]).read_text()) for j in manifest["jobs"] if j["condition"] == condition}
        entries = [new, old["choice"], old["cgs"], old[row["strong_ground"]]]
        lines.append(f"|{condition}|" + "|".join(cell(r) for r in entries) + f"|{row['threshold']}|{'通过' if row['passed'] else '未通过'}|")
    lines += ["", "变体1因整图执行审计失败已停止，无有效算法成绩；失败原因和源码保存在v1目录。"]
    if (ROOT / "v2" / "summary.json").exists():
        prior = json.loads((ROOT / "v2" / "summary.json").read_text())
        lines += ["变体2完整结果：" + "；".join(f"{r['condition']} {r['on_time_tasks']}/{r['total_tasks']}"
                  for r in prior["rows"]) + "。其完整逐任务增减保存在v2/summary.json。"]
    lines += ["", "强地面按原条件扫描预先选定整套协议，不能逐任务拼接。候选的原图地面分支按对应条件采用同一协议；",
              "本地/CGS结果中继仍保持45跳无环，星上输入仍两跳。800MB唯一像素到齐后计算64窗，星地服务倍率不变。", "",
              "## 逐任务增减", "", "任务编号均属于各自条件，负载倍率条件不能跨条件配对；增减只按是否300秒内交付。", ""]
    for row in selected["rows"]:
        for name, pair in row["paired"].items():
            lines.append(f"- {row['condition']} 对 {name}：新增 {pair['gained']}；丢失 {pair['lost']}。")
    heavy = json.loads((panel / "compute_10.json").read_text())
    raw_on_time = len(set(heavy["unique_ground_tasks"]) & set(heavy["on_time_task_ids"]))
    lines += ["", "各图R300差值和完整增减见版本summary.json；新候选每次三方案报价、选择、实际结果见各条件JSON。", "",
              "## 验收结论", "",
              "通过条件：" + ", ".join(r["condition"] for r in selected["rows"] if r["passed"]) + "。",
              "未通过条件：" + ", ".join(r["condition"] for r in selected["rows"] if not r["passed"]) + "。",
              "三方案能改善重计算条件，但跨条件不退化假设未获支持；不将候选替换为默认方法。",
              "剩余失败仍有明显报价低估，细目见residual_failures.json；不继续调常数或尝试第四个变体。",
              f"x10的{heavy['on_time_tasks']}张按时图中{raw_on_time}张采用完整原图地面方案，不能把全部增益解释为多星计算的贡献。", "",
              "## 未运行测试的机器时间估算", "",
              f"六个预登记测试日，7条件×7原始方法（本地、两种纯地面、CGS、HEFT、旧候选、新候选）×6日=294次全天运行。",
              f"以实测开发工时线性外推：{hours:.1f} worker小时；4进程理想墙钟{hours/4:.1f}小时。",
              f"仿真预留1.5–2倍、另加一次性准备1小时：{hours*1.5+1:.1f}–{hours*2+1:.1f} worker小时，4进程墙钟{hours*1.5/4+1:.1f}–{hours*2/4+1:.1f}小时。",
              f"若正式面板新候选替代旧候选，则252次，线性{replace_hours:.1f} worker小时、4进程{replace_hours/4:.1f}小时。",
              f"开发记录48源星一天几何生成{geometry['geometry_elapsed_s']:.3f}秒；按24源星连续53天粗线性外推约{geometry_seconds:.1f}秒。",
              "该外推未验证长序列内存/排序和序列化成本，因此另列1小时生成、存储与核验预算；它是规划余量，不冒充实测。",
              "这是同机CPU仿真估算，不是GPU训练时长。测试任务数未知；未运行连续测试轨道/采集生成。",
              "余量是规划假设而非置信区间；详见test_runtime_estimate.json。没有生成或运行任何测试日期。", "",
              "## 限制与停止", "", "仅固定开发面板，不能声称独立测试有效或普遍最优。公开状态FIFO报价不等于实际共享链路的精确时隙预约。",
              "完整变体记录保留；本轮已停止，等待下一条命令。未改公共路由、主配置、论文，未训练MARL、未提交推送。"]
    write_json(ROOT / "residual_failures.json", {"variant": args.variant, "lost_vs_old_candidate": residual})
    (ROOT / "REPORT.md").write_text("\n".join(lines) + "\n")
    write_json(ROOT / "audit.json", {"source_sha256": source, "core_matches_frozen_scan": True,
               "execution_audit_passed": True, "metrics_independently_recomputed": True,
               "test_dates_executed": False, "variants_attempted": 3,
               "selected_variant": args.variant, "acceptance_passed": selected["passed"], "record_sha256": inputs})
    print(json.dumps({"variant": args.variant, "passed": selected["passed"], "worker_hours": hours}))


if __name__ == "__main__":
    main()
