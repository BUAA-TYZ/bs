"""Build paired development results from finished runs without rerunning validation."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys

import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.evaluation import paired_task_metrics


def metrics(rows):
    total = sum(row["overall"]["total_tasks"] for row in rows)
    completed = sum(row["overall"]["completed_tasks"] for row in rows)
    return {"total_tasks": total, "completed_tasks": completed,
            "mean_scenario_completion_rate": sum(row["overall"]["task_completion_rate"] for row in rows) / len(rows)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="append", required=True, help="LABEL=checkpoint directory")
    parser.add_argument("--baselines", required=True)
    parser.add_argument("--test")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    baseline = json.loads(Path(args.baselines).read_text())
    reference = {label: {r["seed"]: r["metrics"] for r in baseline["runs"] if r["label"] == label}
                 for label in baseline["aggregate"]}
    report = {"scope": "Fixed-budget development; insufficient independent repeats for a thesis superiority claim",
              "baseline_validation": {k: metrics(list(v.values())) for k, v in reference.items()}, "runs": {}}
    sources = set()
    for item in args.run:
        label, directory = item.split("=", 1)
        directory = Path(directory)
        metadata = json.loads((directory / "run.json").read_text())
        records = [json.loads(line) for line in (directory / "train_log.jsonl").read_text().splitlines()]
        budget = metadata["training_args"]["episodes"]
        if [r["episode"] for r in records] != list(range(1, budget + 1)):
            raise ValueError(f"{label}: incomplete or duplicated training records")
        sources.add(metadata["source_sha256"])
        model = torch.load(directory / "best.pt", map_location="cpu", weights_only=True)
        warm = json.loads((directory / "warmstart.json").read_text())["validation"]
        coverage = Counter(f'{r["scenario"]["num_sats"]}:{r["scenario"]["role_profile"]}' for r in records)
        stages = {}
        for stage, validation in (("warmstart", warm), ("best", model["validation"]), ("last", records[-1]["validation"])):
            rows = [case["metrics"] for case in validation["cases"]]
            comparisons = {}
            for ref_label in ("window_aware", "ground_compute"):
                pairs = []
                for case in validation["cases"]:
                    ref = reference[ref_label][case["seed"]]
                    if ref["experiment"]["task_trace_sha256"] != case["metrics"]["experiment"]["task_trace_sha256"]:
                        raise ValueError("Different task traces in paired validation")
                    if ref["experiment"]["source_sha256"] != metadata["source_sha256"]:
                        raise ValueError("Simulation source changed between training and baseline")
                    pairs.append({"id": case["id"], **paired_task_metrics(ref["task_outcomes"], case["metrics"]["task_outcomes"])})
                common = sum(p["common_completed_tasks"] for p in pairs)
                comparisons[ref_label] = {
                    "common_completed": common,
                    "candidate_only_completed": sum(p["candidate_only_completed_tasks"] for p in pairs),
                    "reference_only_completed": sum(p["reference_only_completed_tasks"] for p in pairs),
                    "common_task_mean_latency_delta_seconds": sum(
                        (p["candidate_minus_reference_latency_mean_seconds"] or 0) * p["common_completed_tasks"]
                        for p in pairs) / common if common else None,
                    "cases": pairs}
            stages[stage] = {**metrics(rows), "paired": comparisons,
                             "cases": [{"id": c["id"], **c["metrics"]["overall"]} for c in validation["cases"]]}
        report["runs"][label] = {
            "directory": str(directory), "training_seed": metadata["seed"], "algorithm": metadata["algorithm"],
            "best_episode": model["episode"], "ppo_improved_selection_score": model["episode"] > 0,
            "best_checkpoint_sha256": hashlib.sha256((directory / "best.pt").read_bytes()).hexdigest(),
            "episodes": budget, "environment_steps": metadata["environment_steps"],
            "demonstration_steps": metadata["warmstart_environment_steps"], "coverage": dict(coverage),
            "unique_training_configs": len({r["scenario"]["config_sha256"] for r in records}),
            "training_tasks": sum(r["total_tasks"] for r in records),
            "sampled_training_completed_tasks": sum(r["completed_tasks"] for r in records),
            "decision_agents_per_episode_min": min(len(r["decision_agent_steps"]) for r in records),
            "decision_agents_per_episode_max": max(len(r["decision_agent_steps"]) for r in records),
            "rl_and_validation_elapsed_seconds": sum(r["elapsed_s"] for r in records),
            "validation": stages}
    if len(sources) != 1:
        raise ValueError("Training runs used different source versions")
    report["source_sha256"] = sources.pop()
    report["analysis_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if args.test:
        test = json.loads(Path(args.test).read_text())
        report["test_report"] = args.test
        report["test_summary"] = {label: metrics([r["metrics"] for r in test["runs"] if r["label"] == label])
                                  for label in test["aggregate"]}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
    print(json.dumps({k: {"best_episode": v["best_episode"],
                         "validation_rate": v["validation"]["best"]["mean_scenario_completion_rate"]}
                      for k, v in report["runs"].items()}, indent=2))


if __name__ == "__main__":
    main()
