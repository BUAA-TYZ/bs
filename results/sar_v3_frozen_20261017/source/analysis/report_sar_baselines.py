"""只读归档结果并重算指标/事件归因，不再次运行全天仿真。"""

import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from diagnose_sar_failures import classify
from run_sar_frozen import digest
from sim.evaluation import paired_task_metrics, task_age_score


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(condition, message):
    if not condition:
        raise ValueError(message)


def correct_diagnosis(path, panel_path, bound_path):
    old = json.loads(path.read_bytes())
    verify(old["replay_identical_to_panel"], "Replay was not identical")
    verify(sha(panel_path) == old["panel_record_sha256"], "Panel hash mismatch")
    verify(sha(bound_path) == old["bound_sha256"], "Bound hash mismatch")
    timeline = path.parent / old["timeline_file"]
    verify(sha(timeline) == old["timeline_sha256"], "Timeline hash mismatch")
    traces = json.loads(gzip.decompress(timeline.read_bytes()))
    bound = {r["task_id"]: r for r in json.loads(bound_path.read_bytes())["tasks"]}
    panel = json.loads(panel_path.read_bytes())
    dt = panel["dt"]
    rows = []
    for row in old["tasks"]:
        task = SimpleNamespace(release_time=row["release_seconds"] / dt)
        label, evidence = classify(task, traces[row["task_id"]], bound[row["task_id"]], None, dt,
                                   unique_image=old["method"] == "ground_unique_relay")
        # 旧占用比例对应旧尾片区间，不能冒充重算后区间的遥测。
        if "receiver_busy_share" in row["evidence"]:
            evidence["archived_tail_interval_receiver_busy_share"] = row["evidence"]["receiver_busy_share"]
        rows.append({**row, "cause": label, "evidence": evidence, "previous_cause": row["cause"]})
    return {"method": old["method"], "original_report_sha256": sha(path),
            "timeline_sha256": sha(timeline), "panel_record_sha256": sha(panel_path),
            "bound_sha256": sha(bound_path), "replay_identical_to_panel": True,
            "scope": "offline correction; original replay and timelines preserved; no channel trace available",
            "cause_counts": dict(Counter(r["cause"] for r in rows)), "tasks": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", type=Path, default=Path("results/sar_baseline_b10_dt0p01_20261016"))
    parser.add_argument("--diagnosis", type=Path, default=Path("results/sar_failure_attribution_20261016"))
    parser.add_argument("--bound", type=Path, default=Path("results/sar_reachability_bound_20261015/bound.json"))
    args = parser.parse_args()
    summary = json.loads((args.panel / "summary.json").read_bytes())
    bound = json.loads(args.bound.read_bytes())
    cfg_hash = digest(json.loads((args.panel / "derived_config.json").read_bytes()))
    verify(cfg_hash == summary["derived_config_sha256"], "Config mismatch")
    reference = json.loads((args.panel / "ground_unique_relay.json").read_bytes())
    reference_ids = set(reference["on_time_task_ids"])
    impossible = set(bound["relay_infeasible_task_ids"])
    records = []
    for row in summary["rows"]:
        path = args.panel / f'{row["method"]}.json'
        panel = json.loads(path.read_bytes())
        outcomes = panel["metrics"]["task_outcomes"]
        verify(panel["derived_config_sha256"] == cfg_hash, "Method config mismatch")
        verify(panel["metrics"]["experiment"]["source_sha256"] == summary["source_sha256"], "Source mismatch")
        verify(panel["runner_sha256"] == summary["runner_sha256"], "Runner mismatch")
        paired_task_metrics(reference["metrics"]["task_outcomes"], outcomes)
        score = task_age_score(outcomes, [300], panel["observation_end_seconds"])
        verify(abs(score["delivery_score"] - panel["P300"]) < 1e-12, "P300 mismatch")
        verify(abs(score["restricted_mean_latency_seconds"] - panel["R300_seconds"]) < 1e-9, "R300 mismatch")
        verify(row["P300"] == panel["P300"] and row["R300_seconds"] == panel["R300_seconds"],
               "Summary metrics differ from method record")
        ids = {r["task_id"] for r in outcomes if r["status"] == "completed" and r["latency_seconds"] <= 300}
        verify(ids == set(panel["on_time_task_ids"]), "On-time IDs mismatch")
        verify(len(ids) == panel["on_time_tasks"] == row["on_time_tasks"], "On-time count mismatch")
        verify(len(outcomes) == panel["total_tasks"] == summary["tasks"] == bound["total_tasks"],
               "Task count mismatch")
        verify(not ids & impossible, "Feasible result contradicts bound")
        records.append({"method": row["method"], "record_sha256": sha(path),
                        "on_time": len(ids), "P300": panel["P300"], "R300_s": panel["R300_seconds"],
                        "gained": sorted(ids - reference_ids), "lost": sorted(reference_ids - ids)})
    corrections = {}
    for method in ("cgs_compute_adapted", "ground_unique_relay"):
        correction = correct_diagnosis(args.diagnosis / f"{method}_failures.json",
                                       args.panel / f"{method}.json", args.bound)
        output = args.diagnosis / f"{method}_failures_corrected.json"
        output.write_text(json.dumps(correction, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
        corrections[method] = {"path": str(output), "sha256": sha(output), "counts": correction["cause_counts"]}
    report = {"summary_sha256": sha(args.panel / "summary.json"), "bound_sha256": sha(args.bound),
              "reporter_sha256": sha(Path(__file__)),
              "diagnostic_script_sha256": sha(Path(__file__).with_name("diagnose_sar_failures.py")),
              "source_sha256": summary["source_sha256"], "derived_config_sha256": cfg_hash,
              "verified": True, "rows": records, "corrected_diagnostics": corrections,
              "relaxed_upper_bound_tasks": bound["relay_upper_bound_tasks"],
              "best_observed_tasks": max(r["on_time"] for r in records)}
    (args.panel / "audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    print(json.dumps({"verified": True, "methods": len(records), "corrections": corrections}, ensure_ascii=False))


if __name__ == "__main__":
    main()
