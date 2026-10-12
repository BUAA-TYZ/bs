"""从已有逐任务记录重算统一交付时限；保留原选模与原始指标。"""

import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.evaluation import task_age_delivery_curve


def summarize(report, horizons):
    curves, traces, scenarios, sources = {}, {}, {}, set()
    labels = list(report["aggregate"])
    expected_seeds = set(report["seeds"])
    if not labels or not expected_seeds or len(expected_seeds) != len(report["seeds"]):
        raise ValueError("Expected unique, nonempty cases and policies")
    for run in report["runs"]:
        label, seed, metrics = run["label"], run["seed"], run["metrics"]
        if label not in labels or seed not in expected_seeds:
            raise ValueError("Unexpected policy or evaluation seed")
        key = (label, seed)
        if key in curves:
            raise ValueError("Duplicated policy/case")
        experiment = metrics["experiment"]
        trace = experiment["task_trace_sha256"]
        if seed in traces and (traces[seed] != trace or scenarios[seed] != run["scenario"]):
            raise ValueError("Policies do not share the same task trace and scenario")
        traces[seed], scenarios[seed] = trace, run["scenario"]
        sources.add(experiment["source_sha256"])
        if len(metrics["task_outcomes"]) != metrics["overall"]["total_tasks"]:
            raise ValueError("Task outcomes do not cover all arrivals")
        curves[key] = task_age_delivery_curve(metrics["task_outcomes"], horizons,
                                               experiment["elapsed_sim_seconds"])
    if len(sources) != 1 or len(curves) != len(labels) * len(expected_seeds):
        raise ValueError("Incomplete panel or mixed simulation versions")
    aggregate = {}
    for label in labels:
        rows = []
        for index, horizon in enumerate(horizons):
            cases = [curves[label, seed][index] for seed in report["seeds"]]
            total = sum(case["total_tasks"] for case in cases)
            delivered = sum(case["delivered_within_horizon"] for case in cases)
            nonempty = [case for case in cases if case["total_tasks"]]
            rows.append({"horizon_seconds": horizon, "total_tasks": total,
                         "delivered_within_horizon": delivered,
                         "mean_scenario_delivery_rate":
                             sum(case["delivery_rate"] for case in nonempty) / len(nonempty) if nonempty else None,
                         "pooled_delivery_rate": delivered / total if total else None,
                         "nonempty_cases": len(nonempty), "empty_cases": len(cases) - len(nonempty)})
        aggregate[label] = rows
    return {"schema_version": 1, "split": report["split"], "checkpoints": report["checkpoints"],
            "simulation_source_sha256": sources.pop(),
            "definition": "All generated tasks; all results delivered and aggregated within task age <= horizon",
            "selection_scope": "Retrospective metric analysis of fixed checkpoints; no model reselection",
            "follow_up": "Every task observed for at least the largest reported horizon",
            "aggregate": aggregate,
            "cases": [{"label": label, "seed": seed, "scenario": scenarios[seed],
                       "curve": curves[label, seed]} for label in labels for seed in report["seeds"]]}


def plot(report, labels, directory):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    directory.mkdir(parents=True, exist_ok=False)
    fig, ax = plt.subplots(figsize=(8, 5.4))
    for label in labels:
        rows = report["aggregate"][label]
        ax.plot([r["horizon_seconds"] / 60 for r in rows],
                [100 * r["mean_scenario_delivery_rate"]
                 if r["mean_scenario_delivery_rate"] is not None else float("nan") for r in rows],
                marker="o", label=label)
    ax.set(xlabel="Time since task release (minutes)", ylabel="Mean scenario delivery rate (%)",
           title="Fixed checkpoints: task-age delivery", ylim=(0, 100))
    ax.set_xticks([r["horizon_seconds"] / 60 for r in report["aggregate"][labels[0]]])
    ax.grid(axis="y", alpha=.2)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(loc="upper center", bbox_to_anchor=(.5, -.17), ncol=2, fontsize=9, frameon=False)
    fig.subplots_adjust(bottom=.25, top=.89, left=.12, right=.97)
    fig.savefig(directory / "task_age_delivery.png", dpi=180)
    fig.savefig(directory / "task_age_delivery.pdf")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--horizons-minutes", default="30,60,120,240")
    parser.add_argument("--plot-dir")
    parser.add_argument("--plot-labels", help="Comma-separated subset; JSON always contains every policy")
    args = parser.parse_args()
    output = Path(args.output)
    if output.exists() or (args.plot_dir and Path(args.plot_dir).exists()):
        raise FileExistsError("Refusing to overwrite an existing analysis")
    horizons = [float(value) * 60 for value in args.horizons_minutes.split(",")]
    source = Path(args.input)
    report = summarize(json.loads(source.read_text()), horizons)
    labels = args.plot_labels.split(",") if args.plot_labels else list(report["aggregate"])
    if any(label not in report["aggregate"] for label in labels):
        raise ValueError("Unknown plot policy label")
    report.update(input_path=str(source), input_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                  metric_source_sha256=hashlib.sha256(
                      (Path(__file__).resolve().parents[1] / "sim/evaluation.py").read_bytes()).hexdigest(),
                  analysis_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
    if args.plot_dir:
        plot(report, labels, Path(args.plot_dir))
    print(json.dumps({label: [round(row["mean_scenario_delivery_rate"] * 100, 2)
                              if row["mean_scenario_delivery_rate"] is not None else None
                              for row in rows] for label, rows in report["aggregate"].items()}, indent=2))


if __name__ == "__main__":
    main()
