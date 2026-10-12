"""汇总固定开发协议，核对轨迹和源码后比较 BC、PPO 与规则对照。"""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.evaluation import task_age_score, paired_task_metrics


def summarize(baselines, training_dir):
    metadata = json.loads((training_dir / "run.json").read_text())
    warm = json.loads((training_dir / "warmstart.json").read_text())["validation"]
    logs = [json.loads(line) for line in (training_dir / "train_log.jsonl").read_text().splitlines()]
    horizons = metadata["delivery_horizons_seconds"]
    runs = list(baselines["runs"])
    validations = [("bc", 0, warm)] + [(f"ppo_{row['episode']}", row["episode"], row["validation"])
                                      for row in logs if row["validation"] is not None]
    for label, episode, validation in validations:
        for case in validation["cases"]:
            runs.append({"label": label, "seed": case["seed"], "episode": episode,
                         "scenario": {"id": case["id"], "seed": case["seed"]}, "metrics": case["metrics"]})
    reference = {row["seed"]: row for row in runs if row["label"] == "window_aware"}
    summaries, paired = [], []
    for label in dict.fromkeys(row["label"] for row in runs):
        records = [row for row in runs if row["label"] == label]
        if len(records) != len(reference) or {row["seed"] for row in records} != set(reference):
            raise ValueError("Methods must share exactly the same validation cases")
        age = []
        for row in records:
            metrics, ref = row["metrics"], reference[row["seed"]]["metrics"]
            for field in ("source_sha256", "task_trace_sha256", "tle_sha256"):
                if metrics["experiment"][field] != ref["experiment"][field]:
                    raise ValueError(f"Mismatched {field}: {label}/{row['seed']}")
            if metrics["experiment"]["source_sha256"] != metadata["source_sha256"]:
                raise ValueError("Training and evaluation source mismatch")
            age.append(task_age_score(metrics["task_outcomes"], horizons,
                                      metrics["experiment"]["elapsed_sim_seconds"]))
            paired.append({"label": label, "reference": "window_aware", "seed": row["seed"],
                           **paired_task_metrics(ref["task_outcomes"], metrics["task_outcomes"])})
        if any(row["delivery_score"] is None for row in age):
            raise ValueError("Empty task case")
        summaries.append({"label": label,
                          "delivery_score": sum(row["delivery_score"] for row in age) / len(age),
                          "delivery_rates": [sum(row["delivery_curve"][i]["delivery_rate"] for row in age) / len(age)
                                             for i in range(len(horizons))],
                          "restricted_mean_latency_seconds": sum(row["restricted_mean_latency_seconds"] for row in age) / len(age),
                          "completed_tasks": sum(row["metrics"]["overall"]["completed_tasks"] for row in records),
                          "total_tasks": sum(row["metrics"]["overall"]["total_tasks"] for row in records)})
    best = max(validations, key=lambda row: (row[2]["delivery_score"], -row[2]["restricted_mean_latency_seconds"]))
    bc_cases = {case["seed"]: case for case in warm["cases"]}
    learning_pairs = []
    for label, episode, validation in validations[1:]:
        for case in validation["cases"]:
            before = bc_cases[case["seed"]]
            learning_pairs.append({
                "label": label, "seed": case["seed"], "case": case["id"],
                "delivery_score_delta": case["task_age"]["delivery_score"] - before["task_age"]["delivery_score"],
                **paired_task_metrics(before["metrics"]["task_outcomes"], case["metrics"]["task_outcomes"]),
            })
    return {"scope": "single training seed, existing development validation; not held-out test",
            "source_sha256": metadata["source_sha256"], "horizons_seconds": horizons,
            "selected_label": best[0], "selected_episode": best[1],
            "summaries": summaries, "paired_vs_window": paired,
            "paired_ppo_vs_bc": learning_pairs,
            "training_cases": [row["scenario"] for row in logs],
            "validations": [{"label": label, "episode": episode,
                             "delivery_score": validation["delivery_score"]}
                            for label, episode, validation in validations]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baselines", required=True)
    parser.add_argument("--training-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    report = summarize(json.loads(Path(args.baselines).read_text()), Path(args.training_dir))
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=False)
    (out / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False))
    lines = ["# Relay Task-Age Development Results", "",
             "One training seed; existing validation cases. Not an independent test.", "",
             "| Method | Mean delivery score | 30 min | 60 min | 120 min | 240 min | Eventually delivered |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for row in report["summaries"]:
        values = " | ".join(f"{100 * value:.2f}%" for value in row["delivery_rates"])
        lines.append(f"| {row['label']} | {100 * row['delivery_score']:.2f}% | {values} | "
                     f"{row['completed_tasks']}/{row['total_tasks']} |")
    lines += ["", f"Selected by fixed validation rule: {report['selected_label']}."]
    (out / "summary.md").write_text("\n".join(lines) + "\n")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout="constrained")
    selected = {"ground_compute", "ground_tiles", "ground_relay", "greedy", "window_aware", "bc", report["summaries"][-1]["label"]}
    for row in report["summaries"]:
        if row["label"] in selected:
            axes[0].plot([h / 60 for h in report["horizons_seconds"]],
                         [100 * value for value in row["delivery_rates"]], marker="o", label=row["label"])
    axes[0].set(xlabel="Task age (minutes)", ylabel="Complete delivery (%)", ylim=(0, 105))
    axes[0].legend(fontsize=8)
    axes[1].plot([row["episode"] for row in report["validations"]],
                 [100 * row["delivery_score"] for row in report["validations"]], marker="o", label="BC then MAPPO")
    teacher = next(row for row in report["summaries"] if row["label"] == "window_aware")
    axes[1].axhline(100 * teacher["delivery_score"], linestyle="--", color="black", label="window_aware")
    greedy = next(row for row in report["summaries"] if row["label"] == "greedy")
    axes[1].axhline(100 * greedy["delivery_score"], linestyle=":", color="gray", label="greedy")
    axes[1].set(xlabel="PPO episodes (0 = after BC)", ylabel="Validation delivery score (%)", ylim=(0, 105))
    axes[1].legend(fontsize=8)
    fig.suptitle("Result relay development: one training seed, eight validation cases")
    fig.savefig(out / "delivery_and_training.png", dpi=160)
    plt.close(fig)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
