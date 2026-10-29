"""核对 critic 校准及同 actor、同 PPO 随机种子的后续开发对照。"""

import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys

import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.evaluation import paired_task_metrics, task_age_score


def read(path):
    return json.loads(Path(path).read_text())


def load_checkpoint(path):
    return torch.load(path, weights_only=True, map_location="cpu")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--calibration-dir", required=True)
    p.add_argument("--control-dir", required=True)
    p.add_argument("--calibrated-dir")
    p.add_argument("--sequential-dir")
    p.add_argument("--output-dir", required=True)
    args = p.parse_args()
    root = Path(args.calibration_dir)
    calibration = read(root / "summary.json")
    base_path = Path(calibration["args"]["checkpoint"])
    if hashlib.sha256(base_path.read_bytes()).hexdigest() != calibration["actor_checkpoint_sha256"]:
        raise ValueError("Actor initialization checkpoint changed")
    base = load_checkpoint(base_path)
    pooled = load_checkpoint(root / "pooled.pt")
    active = {k: v["probe"]["active"] for k, v in calibration["stages"].items()}
    gate = {"probe_mse_below_initial": active["pooled"]["mse"] < active["initial"]["mse"],
            "probe_mse_below_sequential": active["pooled"]["mse"] < active["sequential"]["mse"],
            "probe_normalized_mse_below_initial": active["pooled"]["normalized_mse"] < active["initial"]["normalized_mse"]}
    for key, value in base["model_state"].items():
        if not key.startswith("central_critic.") and not torch.equal(value, pooled["model_state"][key]):
            raise ValueError("Critic calibration changed the actor")
    runs = {"control": Path(args.control_dir)}
    if args.calibrated_dir:
        if not all(gate.values()):
            raise ValueError("Calibrated PPO was run despite failing the declared gate")
        runs["pooled"] = Path(args.calibrated_dir)
    if args.sequential_dir:
        runs["sequential"] = Path(args.sequential_dir)
    source = calibration["source_sha256"]
    results, diagnostics, metadata = [], {}, {}
    initial_cases = None
    first_log = None
    reference_args = None
    for label, run_path in runs.items():
        run = read(run_path / "run.json")
        if run["source_sha256"] != source:
            raise ValueError("Calibration and PPO source differ")
        metadata[label] = run
        training_args = run["training_args"]
        if reference_args is None:
            reference_args = training_args
        elif any(v != reference_args[k] for k, v in training_args.items() if k not in ("out_dir", "initialize_checkpoint")):
            raise ValueError("PPO settings differ beyond critic initialization")
        if set(run["training_seeds"]) & (set(calibration["fit_seeds"]) | set(calibration["probe_seeds"])):
            raise ValueError("PPO repeats calibration/probe scenarios")
        initial = load_checkpoint(run_path / "initial.pt")
        expected = base if label == "control" else load_checkpoint(root / f"{label}.pt")
        if any(not torch.equal(v, expected["model_state"][k]) for k, v in initial["model_state"].items()):
            raise ValueError("PPO initialization differs from designated checkpoint")
        warm = read(run_path / "warmstart.json")["validation"]
        logs = [json.loads(line) for line in (run_path / "train_log.jsonl").read_text().splitlines()]
        if len(logs) != training_args["episodes"]:
            raise ValueError("PPO budget incomplete")
        if initial_cases is None:
            initial_cases = {r["seed"]: r for r in warm["cases"]}
            first_log = logs[0]
        else:
            for key in ("scenario", "action_counts", "decision_agent_steps", "ep_reward", "task_age"):
                if logs[0][key] != first_log[key]:
                    raise ValueError(f"First rollout summary changed before policy updates: {key}")
        for case in warm["cases"]:
            if case["metrics"]["task_outcomes"] != initial_cases[case["seed"]]["metrics"]["task_outcomes"]:
                raise ValueError("Critic-only initialization changed deterministic task outcomes")
        horizons = run["delivery_horizons_seconds"]
        for log in logs:
            if abs(log["ep_reward"] - log["total_tasks"] * log["task_age"]["delivery_score"]) > 1e-8:
                raise ValueError("Reward/objective mismatch")
        checkpoints = [(0, warm)] + [(r["episode"], r["validation"]) for r in logs if r["validation"]]
        for episode, validation in checkpoints:
            rows = []
            if len(validation["cases"]) != len(initial_cases) or {c["seed"] for c in validation["cases"]} != set(initial_cases):
                raise ValueError("Incomplete validation panel")
            for case in validation["cases"]:
                baseline = initial_cases[case["seed"]]["metrics"]
                metrics = case["metrics"]
                if metrics["experiment"]["source_sha256"] != source:
                    raise ValueError("Evaluation source changed")
                for field in ("task_trace_sha256", "tle_sha256", "config"):
                    if baseline["experiment"][field] != metrics["experiment"][field]:
                        raise ValueError("Validation environment changed")
                age = task_age_score(metrics["task_outcomes"], horizons, metrics["experiment"]["elapsed_sim_seconds"])
                rows.append({"id": case["id"], "seed": case["seed"], **age,
                             "paired_vs_initial": paired_task_metrics(baseline["task_outcomes"], metrics["task_outcomes"])})
            results.append({"label": label, "episode": episode, "cases": rows,
                            "delivery_score": statistics.mean(r["delivery_score"] for r in rows),
                            "restricted_mean_latency_seconds": statistics.mean(r["restricted_mean_latency_seconds"] for r in rows),
                            "completed_tasks": validation["completed_tasks"]})
        best = load_checkpoint(run_path / "best.pt")
        updates = [{"episode": r["episode"], **d} for r in logs for d in r["update_diagnostics"]]
        diagnostics[label] = {"best_episode": best["episode"], "best_score": best["validation"]["delivery_score"],
                              "updates": updates, "actor_updates": sum(d["actor_updates"] for d in updates)}
    report = {"calibration": calibration, "gate": gate, "results": results, "diagnostics": diagnostics,
              "run_metadata": metadata, "checks": {"actor_initialization_identical": True,
                "initial_task_outcomes_identical": True, "ppo_settings_matched": True,
                "first_rollout_summaries_matched": True, "calibration_and_ppo_seeds_disjoint": True},
              "limitations": ["shared BC actor initialization, not independent end-to-end training repeats",
                              "single PPO seed comparison", "extra calibration data/compute for calibrated arm",
                              "probe scenarios from training distribution; eight-case development selection, not a new final test",
                              "Monte Carlo value targets are noisy; lower prediction error need not improve policy gradient"]}
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=False)
    (out / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False))
    lines = ["# Value Calibration and PPO", "", "Same actor initialization; same PPO seed and interaction budget.",
             "Each calibrated arm additionally uses the same 18 calibration scenarios; 9 probe scenarios are not fitted.", "",
             "| Critic | Probe active MSE | Probe normalized MSE | Probe explained variance |", "|---|---:|---:|---:|"]
    for label, metric in active.items():
        lines.append(f"| {label} | {metric['mse']:.6f} | {metric['normalized_mse']:.4f} | {metric['explained_variance']:.4f} |")
    lines += ["", "| Policy | Episode | Delivery score | Complete tasks | Restricted mean (s) |", "|---|---:|---:|---:|---:|"]
    for r in results:
        lines.append(f"| {r['label']} | {r['episode']} | {100*r['delivery_score']:.4f}% | {r['completed_tasks']} | {r['restricted_mean_latency_seconds']:.1f} |")
    lines += ["", "Critic accuracy and scheduling improvement are separate claims. Full limitations and checks are in summary.json.", ""]
    (out / "summary.md").write_text("\n".join(lines))
    print(json.dumps({"gate": gate, "results": [{k: v for k, v in r.items() if k != "cases"} for r in results],
                      "best": {k: {f: d[f] for f in ("best_episode", "best_score")} for k, d in diagnostics.items()}}, indent=2))


if __name__ == "__main__":
    main()
