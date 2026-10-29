"""Plot recorded validation curves without smoothing or selecting a favorable run."""

import argparse
import hashlib
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="append", required=True, help="LABEL=checkpoint directory")
    parser.add_argument("--baselines", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=False)
    baseline = json.loads(Path(args.baselines).read_text())
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.3), constrained_layout=True)
    series = {}
    for item in args.run:
        label, directory = item.split("=", 1)
        directory = Path(directory)
        warm = json.loads((directory / "warmstart.json").read_text())["validation"]
        records = [json.loads(line) for line in (directory / "train_log.jsonl").read_text().splitlines()]
        points = [(0, warm)] + [(r["episode"], r["validation"]) for r in records if r["validation"]]
        xs = [p[0] for p in points]
        rates = [100 * p[1]["task_completion_rate"] for p in points]
        completed = [p[1]["completed_tasks"] for p in points]
        axes[0].plot(xs, rates, marker="o", label=label, linewidth=1.7)
        axes[1].plot(xs, completed, marker="o", label=label, linewidth=1.7)
        series[label] = {"episodes": xs, "validation_rate_percent": rates, "completed_tasks": completed}
    for name, color in (("window_aware", "#555555"), ("ground_compute", "#a35a28")):
        rate = baseline["aggregate"][name]["task_completion_rate_mean"] * 100
        completed = sum(r["metrics"]["overall"]["completed_tasks"] for r in baseline["runs"] if r["label"] == name)
        axes[0].axhline(rate, color=color, linestyle="--", label=name)
        axes[1].axhline(completed, color=color, linestyle="--", label=name)
    axes[0].set_ylabel("Mean scenario task completion (%)")
    axes[1].set_ylabel("Completed tasks across validation cases")
    for ax in axes:
        ax.set_xlabel("RL episodes (0 = behavior cloning)")
        ax.set_xticks(sorted({x for run in series.values() for x in run["episodes"]}))
        ax.grid(axis="y", alpha=.2)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(fontsize=8)
    fig.suptitle("Dedicated imaging roles: fixed validation panel", fontsize=13)
    fig.savefig(output / "validation_curves.png", dpi=180)
    fig.savefig(output / "validation_curves.pdf")
    plt.close(fig)
    (output / "series.json").write_text(json.dumps({
        "series": series, "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scope": "Development validation curves; individual runs, no confidence interval or convergence claim"
    }, indent=2))


if __name__ == "__main__":
    main()
