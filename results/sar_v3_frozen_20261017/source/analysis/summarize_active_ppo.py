"""校验同初始化 PPO 分批对照，保留更新诊断与全部开发评估。"""

import argparse
import json
from pathlib import Path
import statistics
import sys

import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.evaluation import paired_task_metrics, task_age_score


def read(path):
    return json.loads(Path(path).read_text())


def load_run(path):
    root = Path(path)
    return {"path": str(root), "metadata": read(root / "run.json"),
            "initial": torch.load(root / "initial.pt", weights_only=True, map_location="cpu"),
            "best": torch.load(root / "best.pt", weights_only=True, map_location="cpu"),
            "last": torch.load(root / "last.pt", weights_only=True, map_location="cpu"),
            "warm": read(root / "warmstart.json")["validation"],
            "logs": [json.loads(line) for line in (root / "train_log.jsonl").read_text().splitlines()]}


def same_weights(left, right):
    return left.keys() == right.keys() and all(torch.equal(value, right[key]) for key, value in left.items())


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runs", nargs="+", required=True, help="label=checkpoint-directory")
    p.add_argument("--baselines", default="results/relay_age_baselines_20261010.json")
    p.add_argument("--historical-control", default="checkpoints/relay_idlefix_mappo_7_20261010")
    p.add_argument("--output-dir", required=True)
    args = p.parse_args()
    runs = {label: load_run(path) for label, path in (item.split("=", 1) for item in args.runs)}
    first = next(iter(runs.values()))
    if first["metadata"]["training_args"]["actor_batching"] != "mixed":
        raise ValueError("List the mixed-batching control first")
    horizons = first["metadata"]["delivery_horizons_seconds"]
    expected_seeds = set(first["metadata"]["validation_seeds"])
    initial_cases = {c["seed"]: c for c in first["warm"]["cases"]}
    allowed_differences = {"out_dir", "actor_batching", "actor_epochs", "target_kl"}
    results, diagnoses, checks = [], {}, []

    def summarize(label, episode, cases, source=None):
        if len(cases) != len(expected_seeds) or {c["seed"] for c in cases} != expected_seeds:
            raise ValueError("Incomplete validation panel")
        rows = []
        for case in cases:
            experiment = case["metrics"]["experiment"]
            reference = initial_cases[case["seed"]]["metrics"]["experiment"]
            for key in ("task_trace_sha256", "tle_sha256", "config", "elapsed_sim_seconds"):
                if experiment[key] != reference[key]:
                    raise ValueError(f"Environment mismatch: {label}/{case['seed']}/{key}")
            if source and experiment["source_sha256"] != source:
                raise ValueError("Evaluation source differs from training")
            age = task_age_score(case["metrics"]["task_outcomes"], horizons, experiment["elapsed_sim_seconds"])
            base = initial_cases[case["seed"]]["task_age"]["delivery_score"]
            rows.append({"case": case.get("id", str(case["seed"])), "seed": case["seed"], **age,
                         "delta_vs_bc_pp": 100 * (age["delivery_score"] - base),
                         "paired_vs_initial": paired_task_metrics(
                             initial_cases[case["seed"]]["metrics"]["task_outcomes"], case["metrics"]["task_outcomes"])})
        record = {"label": label, "episode": episode, "cases": rows,
                  "delivery_score": statistics.mean(r["delivery_score"] for r in rows),
                  "restricted_mean_latency_seconds": statistics.mean(r["restricted_mean_latency_seconds"] for r in rows),
                  "completed_tasks": sum(c["metrics"]["overall"]["completed_tasks"] for c in cases),
                  "total_tasks": sum(c["metrics"]["overall"]["total_tasks"] for c in cases)}
        results.append(record)
        return record

    summarize("initial_bc", 0, first["warm"]["cases"], first["metadata"]["source_sha256"])
    greedy = [{"seed": r["seed"], "metrics": r["metrics"]} for r in read(args.baselines)["runs"]
              if r["label"] == "greedy"]
    summarize("greedy_archived", None, greedy)
    for label, run in runs.items():
        meta = run["metadata"]
        for key in ("source_sha256", "model_config", "config", "seed", "initialization", "scenario_distribution"):
            if meta[key] != first["metadata"][key]:
                raise ValueError(f"Run differs: {label}/{key}")
        differing_args = {k for k, v in meta["training_args"].items()
                          if v != first["metadata"]["training_args"][k]}
        if differing_args - allowed_differences:
            raise ValueError(f"Unexpected treatment: {differing_args}")
        if not same_weights(run["initial"]["model_state"], first["initial"]["model_state"]):
            raise ValueError("Initial weights differ")
        for case in run["warm"]["cases"]:
            if case["metrics"]["task_outcomes"] != initial_cases[case["seed"]]["metrics"]["task_outcomes"]:
                raise ValueError("Initial evaluation outcomes differ")
        for key in ("scenario", "ep_reward", "task_age", "action_counts", "decision_agent_steps"):
            if run["logs"][0][key] != first["logs"][0][key]:
                raise ValueError(f"First rollout differs before treatment: {key}")
        diagnostics = []
        for record in run["logs"]:
            if abs(record["ep_reward"] - record["task_age"]["delivery_score"] * record["total_tasks"]) > 1e-8:
                raise ValueError("Training reward does not match task-age objective")
            diagnostics.extend({"episode": record["episode"], **d} for d in record["update_diagnostics"])
            if record["validation"]:
                summarize(label, record["episode"], record["validation"]["cases"], meta["source_sha256"])
        if len(run["logs"]) != meta["training_args"]["episodes"]:
            raise ValueError("Training budget not complete")
        batches = [size for d in diagnostics for size in d["actor_batch_sizes"]]
        diagnoses[label] = {"path": run["path"], "training_args": meta["training_args"],
                            "selected_episode": run["best"]["episode"],
                            "selected_score": run["best"]["validation"]["delivery_score"],
                            "active_rows": sum(d["active_rows"] for d in diagnostics),
                            "total_rows": sum(d["rows"] for d in diagnostics),
                            "actor_updates": len(batches), "actor_batch_size_median": statistics.median(batches),
                            "mean_joint_kl": statistics.mean(d["joint_kl_mean"] for d in diagnostics),
                            "max_joint_kl_mean": max(d["joint_kl_mean"] for d in diagnostics),
                            "kl_stop_count": sum(d["kl_early_stop"] for d in diagnostics), "updates": diagnostics}
        checks.append({"label": label, "same_initial_weights": True, "same_initial_outcomes": True,
                       "same_first_rollout_summary": True, "reward_identity": True, "differing_args": sorted(differing_args)})
    historical = load_run(args.historical_control)
    historical_check = {"archived_source_sha256": historical["metadata"]["source_sha256"],
                        "identical_mixed_final_weights": same_weights(first["last"]["model_state"], historical["last"]["model_state"]),
                        "identical_mixed_final_task_outcomes": all(
                            a["metrics"]["task_outcomes"] == b["metrics"]["task_outcomes"]
                            for a, b in zip(first["last"]["validation"]["cases"], historical["last"]["validation"]["cases"]))}
    report = {"scope": "single-seed paired development comparison; initial checkpoint is imitation, not RL",
              "source_sha256": first["metadata"]["source_sha256"], "checks": checks,
              "historical_control_check": historical_check, "results": results, "diagnostics": diagnoses,
              "limitations": ["not independent test results", "not multiple independent training seeds",
                              "minibatch shuffling consumes different RNG draws after the first rollout",
                              "KL is a post-step early-stop criterion, not a hard trust-region constraint",
                              "active16kl changes both actor reuse count and KL stopping relative to active2",
                              "greedy is an archived measured baseline, not an optimal or SOTA claim"]}
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=False)
    (out / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False))
    lines = ["# Active Decision PPO Comparison", "", report["scope"], "",
             "| Method | Episode | Delivery score | Completed tasks | Restricted mean (s) |",
             "|---|---:|---:|---:|---:|"]
    for r in results:
        lines.append(f"| {r['label']} | {r['episode']} | {100*r['delivery_score']:.4f}% | {r['completed_tasks']}/{r['total_tasks']} | {r['restricted_mean_latency_seconds']:.1f} |")
    lines += ["", "| Method | Actor updates | Median active rows/batch | Mean joint KL | KL stops | Best episode |",
              "|---|---:|---:|---:|---:|---:|"]
    for label, d in diagnoses.items():
        lines.append(f"| {label} | {d['actor_updates']} | {d['actor_batch_size_median']} | {d['mean_joint_kl']:.6f} | {d['kl_stop_count']} | {d['selected_episode']} |")
    lines += ["", "All runs have identical initial weights, initial evaluation outcomes and first training rollout summaries.",
              "Full per-case results, source checks and per-update diagnostics are retained in summary.json.",
              ""]
    (out / "summary.md").write_text("\n".join(lines))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout="constrained")
    colors = {"mixed": "#b74b43", "active2": "#287f68", "active16kl": "#427cb4"}
    base_score = results[0]["delivery_score"]
    compared = results[1:]
    axes[0].bar([r["label"] for r in compared],
                [100 * (r["delivery_score"] - base_score) for r in compared],
                color=[colors.get(r["label"], "#777777") for r in compared])
    axes[0].axhline(0, color="#333333", linewidth=0.8)
    axes[0].set(ylabel="Delivery score change vs BC (percentage points)", title="Development evaluation after 9 episodes")
    axes[0].tick_params(axis="x", labelrotation=20)
    for label, d in diagnoses.items():
        axes[1].plot([r["episode"] for r in d["updates"]],
                     [max(1e-8, r["joint_kl_mean"]) for r in d["updates"]],
                     marker="o", label=label, color=colors.get(label))
    axes[1].axhline(0.01, linestyle="--", color="#777777", label="KL stop threshold")
    axes[1].set(yscale="log", xlabel="Training episode", ylabel="Mean joint KL (active rows)", title="Policy movement per rollout")
    axes[1].legend(fontsize=8)
    fig.savefig(out / "comparison.png", dpi=180)
    plt.close(fig)
    print(json.dumps({"results": [{k: v for k, v in r.items() if k != "cases"} for r in results],
                      "historical_control_check": historical_check}, indent=2))


if __name__ == "__main__":
    main()
