"""只审计已完成的验证轨迹，区分动作提议、实际执行与因果证据。"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.report_overlap_study import HORIZONS, aggregate, context, load_directory, resource_metrics, validate_record
from analysis.export_overlap_validation import same_weights
from sim.marl.checkpoint import validate_checkpoint_protocol


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def quantile(values, fraction):
    values = sorted(values)
    if not values:
        return None
    position = (len(values) - 1) * fraction
    lower = int(position)
    upper = min(len(values) - 1, lower + 1)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def execution(record):
    metrics = record["metrics"]
    cfg, work = metrics["experiment"]["config"], metrics["experiment"]["workload"]
    resources = resource_metrics(record)
    sources = set(cfg["task_source_sat_ids"])
    rates = cfg["satellite_compute_rates"] or [cfg["compute_rate"]] * cfg["num_sats"]
    satellite_work = {int(sid): busy * rates[int(sid)] for sid, busy in metrics["resource"]["compute_busy_time"].items()}
    ground_rates = {station["id"]: station["compute_rate"] for station in cfg["ground_stations"]}
    ground_work = sum(busy * ground_rates[gid] for gid, busy in metrics["resource"]["ground_compute_busy_time"].items())
    total_tasks = metrics["overall"]["total_tasks"]
    image_work = work["total_compute_cost"]
    source_work = sum(value for sid, value in satellite_work.items() if sid in sources)
    non_source_work = sum(value for sid, value in satellite_work.items() if sid not in sources)
    expected = image_work * total_tasks
    if metrics["overall"]["completed_tasks"] == total_tasks and not math.isclose(
            source_work + non_source_work + ground_work, expected, rel_tol=1e-8, abs_tol=1e-8):
        raise ValueError("Completed population violates compute-work accounting")
    actions = record.get("action_counts")
    action_groups = None
    if actions is not None:
        action_groups = {"wait": sum(n for k, n in actions.items() if int(k) == 0),
                         "local": sum(n for k, n in actions.items() if int(k) == 1),
                         "neighbor": sum(n for k, n in actions.items() if 2 <= int(k) < 10),
                         "ground": sum(n for k, n in actions.items() if int(k) >= 10)}
    image_equivalents = {"source": source_work / image_work, "non_source": non_source_work / image_work,
                        "ground": ground_work / image_work}
    predicted_downlink = ((image_equivalents["source"] + image_equivalents["non_source"]) * work["unmerged_result_mb"]
                          + image_equivalents["ground"] * work["packet_input_mb"])
    return {**resources, "source_work_U": source_work, "non_source_work_U": non_source_work,
            "ground_work_U": ground_work, "total_expected_work_U": expected,
            "image_compute_equivalents": image_equivalents,
            "ground_work_fraction": ground_work / expected,
            "ground_bytes_from_work_mb": predicted_downlink,
            "ground_byte_accounting_residual_mb": resources["ground_sent_mb"] - predicted_downlink,
            "satellite_queue_tile_seconds": sum(metrics["resource"]["avg_queue_len"].values()) * cfg["dt"] * cfg["sim_steps"],
            "action_counts": actions, "action_groups": action_groups,
            "proposal_count": sum(actions.values()) if actions is not None else None,
            "active_decision_steps": (record.get("decision_timing") or {}).get("active_calls"),
            "total_tasks": total_tasks, "completed_tasks": metrics["overall"]["completed_tasks"],
            "total_tiles": metrics["overall"]["total_tiles"]}


def compare_tasks(record, reference):
    actual = {row["task_id"]: row for row in record["metrics"]["task_outcomes"]}
    baseline = {row["task_id"]: row for row in reference["metrics"]["task_outcomes"]}
    if actual.keys() != baseline.keys():
        raise ValueError("Paired task set differs")
    delays, losses, gains, examples = [], [0] * len(HORIZONS), [0] * len(HORIZONS), []
    for tid, row in actual.items():
        other = baseline[tid]
        if any(row[key] != other[key] for key in ("source_sat_id", "release_seconds", "deadline_seconds")):
            raise ValueError("Paired task definition differs")
        latency = row["latency_seconds"] if row["status"] == "completed" else math.inf
        prior = other["latency_seconds"] if other["status"] == "completed" else math.inf
        for index, horizon in enumerate(HORIZONS):
            losses[index] += int(prior <= horizon < latency)
            gains[index] += int(latency <= horizon < prior)
        if math.isfinite(latency) and math.isfinite(prior):
            delta = latency - prior
            delays.append(delta)
            examples.append({"task_id": tid, "source_sat_id": row["source_sat_id"],
                             "release_seconds": row["release_seconds"], "latency_seconds": latency,
                             "reference_latency_seconds": prior, "additional_seconds": delta,
                             "completion_seconds": row["completion_seconds"],
                             "reference_completion_seconds": other["completion_seconds"]})
    return {"reference": reference["method"],
            "delivery_score_difference": record["task_age"]["delivery_score"] - reference["task_age"]["delivery_score"],
            "slower_tasks": sum(value > 1e-8 for value in delays), "faster_tasks": sum(value < -1e-8 for value in delays),
            "equal_tasks": sum(abs(value) <= 1e-8 for value in delays), "both_completed": len(delays),
            "mean_additional_seconds_both_completed": statistics.mean(delays) if delays else None,
            "p95_additional_seconds_both_completed": quantile(delays, .95),
            "lost_age_thresholds": losses, "gained_age_thresholds": gains,
            "largest_delays": sorted(examples, key=lambda row: row["additional_seconds"], reverse=True)[:5]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baselines", default="results/overlap_proxy_baselines_20261014")
    parser.add_argument("--balanced", default="results/overlap_proxy_validation_balanced_bc_20261014")
    parser.add_argument("--training-dir", default="checkpoints/overlap_proxy_mappo_1701_20261014")
    parser.add_argument("--balanced-training-dir", default="checkpoints/overlap_proxy_balanced_bc_1701_20261014")
    parser.add_argument("--out-dir", default="results/overlap_learning_audit_20261014")
    parser.add_argument("--overwrite", action="store_true", help="Replace this derived audit's summary files")
    args = parser.parse_args(argv)
    baseline, balanced = load_directory(args.baselines), load_directory(args.balanced)
    if any(panel["manifest"]["args"]["split"] != "validation" for panel in (baseline, balanced)):
        raise ValueError("This diagnostic accepts validation only")
    if baseline["references"] != balanced["references"] or balanced["methods"] != ["bc_balanced"]:
        raise ValueError("Validation case configuration/source/task trace differs")
    evidence = {}
    def track(path):
        path = Path(path).resolve()
        evidence[str(path)] = digest(path)
        return path
    for panel in (baseline, balanced):
        for name in ("run.json", "summary.json", *panel["manifest"]["records"]):
            track(Path(panel["directory"]) / name)
    root = Path(args.training_dir).resolve()
    warm = json.loads(track(root / "warmstart.json").read_text())
    import torch
    teacher = torch.load(track(root / "teacher_bc.pt"), map_location="cpu", weights_only=True)
    validated = torch.load(track(root / "warmstart.pt"), map_location="cpu", weights_only=True)
    for checkpoint in (teacher, validated):
        validate_checkpoint_protocol(checkpoint)
        if checkpoint["episode"] != 0 or checkpoint["source_sha256"] != baseline["manifest"]["source_sha256"]:
            raise ValueError("Original BC checkpoint identity differs")
    # JSON 将动作计数的整数键规范化为字符串。
    if (not same_weights(teacher["model_state"], validated["model_state"])
            or json.loads(json.dumps(validated["validation"], allow_nan=False)) != warm["validation"]):
        raise ValueError("Original BC validation/checkpoint differs")
    equal_root = Path(args.balanced_training_dir)
    equal_fit = json.loads(track(equal_root / "summary.json").read_text())
    replay_check = json.loads(track(equal_root / "original_teacher_comparison.json").read_text())
    if (equal_fit["checkpoint"]["sha256"] != balanced["manifest"]["checkpoint_sha256"]["bc_balanced"]
            or equal_fit["source_sha256"] != baseline["manifest"]["source_sha256"]
            or replay_check["sha256"] != digest(root / "warmstart.json")
            or replay_check["cache_index_sha256"] != equal_fit["demonstration_cache"]["index_sha256"]
            or replay_check.get("samples_actions_and_full_environment_summaries_equal") is not True):
        raise ValueError("Balanced fit or post-fit demonstration equality evidence differs")
    specs = {row["case"]["id"]: row for row in baseline["manifest"]["cases"]}
    rows = warm["validation"]["cases"]
    if len(rows) != len(specs) or {row["id"] for row in rows} != specs.keys():
        raise ValueError("Original BC validation case set differs")
    original = {}
    for row in rows:
        spec = specs[row["id"]]
        if any(row.get(key) != value for key, value in spec["case"].items()):
            raise ValueError("Original BC case identity differs")
        record = {"case": spec["case"], "method": "bc", "metrics": row["metrics"], "task_age": row["task_age"],
                  "action_counts": row["action_counts"], "checkpoint_sha256": digest(root / "teacher_bc.pt")}
        validate_record(record, {**baseline["manifest"], "checkpoint_sha256": {"bc": record["checkpoint_sha256"]}}, spec)
        if context(record) != baseline["references"][row["id"]]:
            raise ValueError("Original BC task trace or execution protocol differs")
        original[(row["id"], "bc")] = record
    methods = ("local", "cgs_compute_adapted", "bc", "bc_balanced")
    records = {key: row for key, row in baseline["records"].items() if key[1] in methods}
    records.update(original)
    records.update(balanced["records"])
    details = []
    for (case_id, method), record in sorted(records.items()):
        details.append({"case": case_id, "method": method, "score": record["task_age"]["delivery_score"],
                        "delivery_curve": record["task_age"]["delivery_curve"],
                        "restricted_latency_s": record["task_age"]["restricted_mean_latency_seconds"],
                        "execution": execution(record),
                        "versus_local": compare_tasks(record, records[(case_id, "local")]),
                        "versus_teacher": compare_tasks(record, records[(case_id, "cgs_compute_adapted")])})
    overview = [aggregate([row for (_, name), row in records.items() if name == method], method) for method in methods]
    for row in overview:
        items = [detail for detail in details if detail["method"] == row["method"]]
        row["image_compute_equivalents"] = {key: sum(item["execution"]["image_compute_equivalents"][key] for item in items)
                                           for key in ("source", "non_source", "ground")}
        row["satellite_queue_tile_seconds"] = sum(item["execution"]["satellite_queue_tile_seconds"] for item in items)
        row["lost_age_thresholds_vs_local"] = [sum(item["versus_local"]["lost_age_thresholds"][i] for item in items) for i in range(4)]
        row["gained_age_thresholds_vs_local"] = [sum(item["versus_local"]["gained_age_thresholds"][i] for item in items) for i in range(4)]
        row["task_latency_counts_vs_local"] = {key: sum(item["versus_local"][key] for item in items)
                                               for key in ("slower_tasks", "faster_tasks", "equal_tasks")}
        row["maximum_absolute_ground_byte_accounting_residual_mb"] = max(abs(item["execution"]["ground_byte_accounting_residual_mb"]) for item in items)
        if all(item["execution"]["action_groups"] is not None for item in items):
            row["action_groups"] = {key: sum(item["execution"]["action_groups"][key] for item in items)
                                    for key in ("wait", "local", "neighbor", "ground")}
        else:
            row["action_groups"] = None
    groups = []
    for fast in (False, True):
        for method in methods:
            selected = [row for row in details if row["method"] == method and ("fast_ground" in row["case"]) == fast]
            groups.append({"group": "fast_ground" if fast else "other_validation_cases", "method": method,
                           "cases": len(selected), "delivery_score": statistics.mean(row["score"] for row in selected),
                           "completed_tasks": sum(row["execution"]["completed_tasks"] for row in selected),
                           "total_tasks": sum(row["execution"]["total_tasks"] for row in selected),
                           "ground_image_equivalents": sum(row["execution"]["image_compute_equivalents"]["ground"] for row in selected),
                           "tasks_identical_completion_to_local": sum(row["versus_local"]["equal_tasks"] for row in selected)})
    training_counts = dict(sum((Counter(row["action_counts"]) for row in warm["episodes"]), Counter()))
    if training_counts != equal_fit["fit"]["class_counts"]:
        raise ValueError("Teacher action population differs between BC controls")
    if any(digest(path) != checksum for path, checksum in evidence.items()):
        raise ValueError("Validation evidence changed during audit")
    result = {"scope": "Validation diagnostic only; no new simulation or training; no test results read",
              "horizons_seconds": HORIZONS, "cases": len(specs), "records": len(records),
              "source_sha256": baseline["manifest"]["source_sha256"], "evidence_sha256": evidence,
              "audit_script_sha256": digest(__file__), "original_bc_fit": warm["pooled_fits"],
              "balanced_bc_fit": equal_fit["fit"], "original_teacher_replay_check": replay_check,
              "training_action_counts": training_counts,
              "overview": overview, "groups": groups, "cases_by_method": details,
              "limits": ["Only original BC validation stores per-action proposal counts; balanced BC and rules do not.",
                         "Proposal counts include repeated pending decisions and are not executed task counts.",
                         "Source/non-source identifies node role, not the originating node of each task.",
                         "Work divided by per-image cost is an image-equivalent quantity, not a per-task executor trace.",
                         "Queue occupancy covers all items in satellite.queue, not exclusively WAIT actions.",
                         "Paired timing/traffic evidence does not isolate a causal contribution of WAIT or ground offload.",
                         "No decision-level logits, teacher labels on learner states or tile event trace were recorded."]}
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=args.overwrite)
    (out / "summary.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    lines = ["# 固定验证集学习退化审计", "", result["scope"], "",
             "12 个相同验证 case，按 case 等权统计 J；完成任务数跨 case 汇总。全部比较已核验任务轨迹、配置、源码与年龄指标。", "",
             "| 方法 | J | 受限时延/s | 完成任务 | 源星/辅助星/地面计算图像当量 | 地面发送/MB | 卫星队列占用/tile-s |", "|---|---:|---:|---:|---|---:|---:|"]
    for row in overview:
        work = row["image_compute_equivalents"]
        lines.append(f"| {row['method']} | {row['delivery_score']:.9f} | {row['restricted_mean_latency_seconds']:.3f} | "
                     f"{row['completed_tasks']}/{row['total_tasks']} | {work['source']:.3f}/{work['non_source']:.3f}/{work['ground']:.3f} | "
                     f"{row['resource']['ground_sent_mb']:.6f} | {row['satellite_queue_tile_seconds']:.1f} |")
    lines += ["", "## 逐场景定位", "", "| case | 方法 | J | 相对 local 的 J 差 | WAIT 提议 | 地面提议 | 地面计算图像当量 | 地面发送/MB |", "|---|---|---:|---:|---:|---:|---:|---:|"]
    for row in details:
        action = row["execution"]["action_groups"]
        lines.append(f"| {row['case']} | {row['method']} | {row['score']:.6f} | {row['versus_local']['delivery_score_difference']:+.6f} | "
                     f"{action['wait'] if action is not None else '未记录'} | {action['ground'] if action is not None else '未记录'} | "
                     f"{row['execution']['image_compute_equivalents']['ground']:.3f} | {row['execution']['ground_sent_mb']:.6f} |")
    lines += ["", "## 证据边界", "", *[f"- {note}" for note in result["limits"]], "",
              "每方法每场景的五个最大延迟任务、跨年龄门槛的损失/收益数、实际资源与网络量保存在 summary.json；不将同时出现的现象写成干预已证实的原因。", ""]
    (out / "summary.md").write_text("\n".join(lines))
    print(json.dumps({"output": str(out.resolve()), "overview": overview}, ensure_ascii=False))


if __name__ == "__main__":
    main()
