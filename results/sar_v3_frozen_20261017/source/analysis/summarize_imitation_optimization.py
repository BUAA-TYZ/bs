"""核对旧实验逐任务交付重放后，汇总教师纠偏训练的开发结果。"""

import argparse
from collections import Counter
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.evaluation import paired_task_metrics, task_age_score


HORIZONS = [1800, 3600, 7200, 14400]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--audit-dir", required=True)
    p.add_argument("--bc-audit-dir", required=True)
    p.add_argument("--training-dir", required=True)
    p.add_argument("--finetuning-dir")
    p.add_argument("--old-dir", default="checkpoints/relay_age_mappo_7_20261010")
    p.add_argument("--baselines", default="results/relay_age_baselines_20261010.json")
    p.add_argument("--output-dir", required=True)
    args = p.parse_args()
    read = lambda path: json.loads(Path(path).read_text())
    audit = read(Path(args.audit_dir) / "summary.json")
    bc = read(Path(args.bc_audit_dir) / "summary.json")
    old_logs = [json.loads(s) for s in (Path(args.old_dir) / "train_log.jsonl").read_text().splitlines()]
    old_val = max((r["validation"] for r in old_logs if r["validation"]),
                  key=lambda r: (r["delivery_score"], -r["restricted_mean_latency_seconds"]))
    old = {r["seed"]: r["metrics"] for r in old_val["cases"]}
    baseline_runs = read(args.baselines)["runs"]
    greedy = {r["seed"]: r["metrics"] for r in baseline_runs if r["label"] == "greedy"}
    groups = {"old_mappo": [], "greedy": []}
    replay_checks = []
    for r in audit["records"]:
        reference = (old if r["rollout"] == "learner" else greedy)[r["seed"]]
        actual = r["metrics"]
        route_differences = []
        actual_tasks = {t["task_id"]: t for t in actual["task_outcomes"]}
        reference_tasks = {t["task_id"]: t for t in reference["task_outcomes"]}
        if set(actual_tasks) != set(reference_tasks):
            raise ValueError("Replay task identities differ")
        for tid, task in actual_tasks.items():
            differences = {k: [reference_tasks[tid][k], task[k]] for k in task if task[k] != reference_tasks[tid][k]}
            if set(differences) - {"result_relay_hops", "raw_relay_hops"}:
                raise ValueError(f"Archived task delivery changed: {r['case']}/{r['rollout']}/{tid}")
            if differences:
                route_differences.append({"task_id": tid, "differences": differences})
        for field in ("task_trace_sha256", "tle_sha256", "config"):
            if actual["experiment"][field] != reference["experiment"][field]:
                raise ValueError(f"Replay mismatch: {field}")
        replay_checks.append({"case": r["case"], "rollout": r["rollout"], "exact_task_delivery": True,
                              "route_counter_differences": route_differences,
                              "original_source": reference["experiment"]["source_sha256"],
                              "replay_source": actual["experiment"]["source_sha256"]})
        groups["old_mappo" if r["rollout"] == "learner" else "greedy"].append(r)
    root = Path(args.training_dir)
    metadata, warm = read(root / "run.json"), read(root / "warmstart.json")
    logs = [json.loads(s) for s in (root / "train_log.jsonl").read_text().splitlines()]
    groups["pooled_greedy_bc"] = bc["records"]
    groups["dagger"] = [{"seed": r["seed"], "case": r["id"], "metrics": r["metrics"],
                         "task_age": r["task_age"]} for r in warm["validation"]["cases"]]
    validations = [(0, warm["validation"])]
    for r in logs:
        if r["validation"]:
            validations.append((r["episode"], r["validation"]))
            groups[f"mappo_{r['episode']}"] = [{"seed": c["seed"], "case": c["id"],
                "metrics": c["metrics"], "task_age": c["task_age"]} for c in r["validation"]["cases"]]
    finetuning_metadata = None
    if args.finetuning_dir:
        fine_root = Path(args.finetuning_dir)
        finetuning_metadata = read(fine_root / "run.json")
        fine_logs = [json.loads(s) for s in (fine_root / "train_log.jsonl").read_text().splitlines()]
        fine_warm = read(fine_root / "warmstart.json")["validation"]
        for episode, validation in [(0, fine_warm)] + [(r["episode"], r["validation"]) for r in fine_logs if r["validation"]]:
            groups[f"fixed_ppo_{episode}"] = [{"seed": c["seed"], "case": c["id"],
                "metrics": c["metrics"], "task_age": c["task_age"]} for c in validation["cases"]]
        bc_tasks = {r["seed"]: r["metrics"]["task_outcomes"] for r in groups["pooled_greedy_bc"]}
        if any(r["metrics"]["task_outcomes"] != bc_tasks[r["seed"]] for r in groups["fixed_ppo_0"]):
            raise ValueError("Fine-tuning initialization does not exactly reproduce pooled BC outcomes")
    summaries, cases = [], []
    for label, records in groups.items():
        if len(records) != len(old) or {r["seed"] for r in records} != set(old):
            raise ValueError("Incomplete or duplicate validation panel")
        for r in records:
            metrics = r["metrics"]
            for field in ("task_trace_sha256", "tle_sha256", "config"):
                if metrics["experiment"][field] != old[r["seed"]]["experiment"][field]:
                    raise ValueError(f"Task or environment mismatch: {label}/{r['case']}/{field}")
            expected_source = (finetuning_metadata if label.startswith("fixed_ppo_") else metadata)["source_sha256"]
            if label not in ("old_mappo", "greedy") and metrics["experiment"]["source_sha256"] != expected_source:
                raise ValueError("New evaluation/training source mismatch")
            r["task_age"] = task_age_score(metrics["task_outcomes"], HORIZONS,
                                           metrics["experiment"]["elapsed_sim_seconds"])
            cases.append({"label": label, "case": r["case"], **r["task_age"],
                          "paired_vs_old": paired_task_metrics(old[r["seed"]]["task_outcomes"], metrics["task_outcomes"]),
                          "paired_vs_greedy": paired_task_metrics(greedy[r["seed"]]["task_outcomes"], metrics["task_outcomes"])})
        summaries.append({"label": label,
            "delivery_score": sum(r["task_age"]["delivery_score"] for r in records) / len(records),
            "restricted_mean_latency_seconds": sum(r["task_age"]["restricted_mean_latency_seconds"] for r in records) / len(records),
            "completed_tasks": sum(r["metrics"]["overall"]["completed_tasks"] for r in records),
            "total_tasks": sum(r["metrics"]["overall"]["total_tasks"] for r in records)})
    confusion = Counter()
    for r in groups["old_mappo"]:
        confusion.update(r["confusion"])
    disagreements = Counter()
    for pair, n in confusion.items():
        teacher, learner = map(int, pair.split(":"))
        if teacher != learner:
            kind = "wait" if teacher == 0 else "local" if teacher == 1 else "neighbor" if teacher < 10 else "ground"
            disagreements[kind] += n
    selected_ep, _ = max(validations, key=lambda x: (x[1]["delivery_score"], -x[1]["restricted_mean_latency_seconds"]))
    best_learned = max((r for r in summaries if r["label"] not in ("old_mappo", "greedy")),
                       key=lambda r: (r["delivery_score"], -r["restricted_mean_latency_seconds"]))
    report = {"scope": "single-seed development optimization; no independent test or isolated attribution",
              "training_source_sha256": metadata["source_sha256"], "replay_checks": replay_checks,
              "summaries": summaries, "cases": cases, "old_confusion": dict(confusion),
              "old_disagreements_by_teacher_type": dict(disagreements),
              "selected_episode": selected_ep, "pooled_fits": warm["pooled_fits"],
              "best_learned_label": best_learned["label"],
              "training_args": metadata["training_args"],
              "finetuning_metadata": finetuning_metadata,
              "limitations": ["BC and PPO benefits reported separately", "extra demonstrations and optimizer budget",
                              "greedy is strongest measured local baseline, not global optimum or literature SOTA",
                              "teacher estimates may disagree with commitment and result-relay execution semantics"]}
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=False)
    (out / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False))
    lines = ["# Imitation Optimization Development Results", "", report["scope"], "",
             "| Method | Delivery score | Completed tasks | Restricted mean latency (s) |",
             "|---|---:|---:|---:|"]
    for r in summaries:
        lines.append(f"| {r['label']} | {100*r['delivery_score']:.2f}% | {r['completed_tasks']}/{r['total_tasks']} | {r['restricted_mean_latency_seconds']:.1f} |")
    lines += ["", f"Selected PPO episode: {selected_ep} (0 means DAgger initialization).",
              f"Best learned stage including the separately evaluated pooled BC: {best_learned['label']}.",
              "Historical source hashes differ due to training edits; all archived greedy and old-policy task delivery statuses and times were replayed exactly.",
              "Relay-hop counter differences are retained in summary.json; this is not full trajectory equivalence."]
    (out / "summary.md").write_text("\n".join(lines) + "\n")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout="constrained")
    labels = [r["label"] for r in summaries]
    axes[0].barh(labels, [100 * r["delivery_score"] for r in summaries], color=[
        "#686868" if label == "greedy" else "#32936f" if label.startswith("mappo_") else "#3973ac"
        for label in labels])
    axes[0].set(xlabel="Mean task-age delivery score (%)", xlim=(0, 100))
    axes[0].invert_yaxis()
    plot_labels = ["old_mappo", "greedy", "pooled_greedy_bc", "dagger", f"mappo_{logs[-1]['episode']}"]
    if finetuning_metadata:
        plot_labels.append(f"fixed_ppo_{fine_logs[-1]['episode']}")
    for label in plot_labels:
        records = groups[label]
        rates = [100 * sum(r["task_age"]["delivery_curve"][i]["delivery_rate"] for r in records) / len(records)
                 for i in range(len(HORIZONS))]
        axes[1].plot([h / 60 for h in HORIZONS], rates, marker="o", label=label)
    axes[1].set(xlabel="Task age (minutes)", ylabel="Complete delivery (%)", ylim=(0, 105))
    axes[1].legend(fontsize=8)
    fig.suptitle("Training optimization: one seed, eight development cases")
    fig.savefig(out / "comparison.png", dpi=160)
    plt.close(fig)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
