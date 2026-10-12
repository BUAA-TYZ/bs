"""Merge independent policy evaluation shards after checking their paired task traces."""

import argparse
import json
from pathlib import Path
import sys

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.evaluation import paired_task_metrics


def merge(reports, reference):
    first = reports[0]
    labels, checkpoints, runs = [], {}, []
    traces, sources = {}, set()
    for report in reports:
        for key in ("evaluation_schema_version", "seeds", "split", "scenario_distribution_sha256"):
            if report[key] != first[key]:
                raise ValueError(f"Evaluation shard differs: {key}")
        for label in report["aggregate"]:
            if label in labels:
                raise ValueError(f"Duplicate policy label: {label}")
            labels.append(label)
        checkpoints.update(report["checkpoints"])
        for run in report["runs"]:
            key = run["seed"]
            trace = run["metrics"]["experiment"]["task_trace_sha256"]
            if key in traces and traces[key] != trace:
                raise ValueError("Different task traces across shards")
            traces[key] = trace
            sources.add(run["metrics"]["experiment"]["source_sha256"])
            runs.append(run)
    if len(sources) != 1 or reference not in labels:
        raise ValueError("Sources differ or reference is absent")
    reference_runs = {r["seed"]: r for r in runs if r["label"] == reference}
    aggregate, pairs = {}, []
    for label in labels:
        selected = [r for r in runs if r["label"] == label]
        if [r["seed"] for r in selected] != first["seeds"]:
            raise ValueError("Missing, duplicated or reordered evaluation cases")
        rates = np.array([r["metrics"]["overall"]["task_completion_rate"] for r in selected])
        reference_rates = np.array([reference_runs[r["seed"]]["metrics"]["overall"]["task_completion_rate"] for r in selected])
        completed = sum(r["metrics"]["overall"]["completed_tasks"] for r in selected)
        latency = sum((r["metrics"]["latency"]["task_mean"] or 0) * r["metrics"]["overall"]["completed_tasks"] for r in selected)
        aggregate[label] = {"task_completion_rate_mean": float(rates.mean()),
                            "task_completion_rate_std": float(rates.std(ddof=1)) if len(rates) > 1 else None,
                            "completed_tasks": completed,
                            "total_tasks": sum(r["metrics"]["overall"]["total_tasks"] for r in selected),
                            "completed_task_latency_seconds_pooled": latency / completed if completed else None,
                            "paired_completion_rate_delta_vs": reference,
                            "paired_delta_mean": float((rates - reference_rates).mean()),
                            "paired_bootstrap_95_ci": None}
        if label == reference:
            continue
        for run in selected:
            ref = reference_runs[run["seed"]]
            if ref["scenario"] != run["scenario"]:
                raise ValueError("Scenario definitions differ across shards")
            pairs.append({"label": label, "reference": reference, "seed": run["seed"], "scenario": run["scenario"],
                          **paired_task_metrics(ref["metrics"]["task_outcomes"], run["metrics"]["task_outcomes"])})
    return {"model_version": "2.0", "evaluation_schema_version": 2, "reference": reference,
            "seeds": first["seeds"], "split": first["split"], "checkpoints": checkpoints,
            "scenario_distribution_sha256": first["scenario_distribution_sha256"],
            "source_sha256": sources.pop(), "aggregate": aggregate, "runs": runs,
            "paired_task_comparisons": pairs,
            "uncertainty_scope": f"Fixed {first['split']} panel; descriptive case variation only, no population or training-run confidence interval"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", required=True)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = merge([json.loads(Path(p).read_text()) for p in args.input], args.reference)
    result["evaluation_shards"] = args.input
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
    print(json.dumps(result["aggregate"], indent=2))


if __name__ == "__main__":
    main()
