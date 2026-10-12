"""运行配对的多训练种子实验，保存原始数据与交叉重采样区间。"""

import argparse
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import torch

from analyze_reachability import analyze
from main import run_config
from sim.config import load_config
from sim.reproducibility import runtime_metadata
from sim.marl.scenarios import ScenarioDistribution

VARIANTS = {
    "comappo_adapted": ["--algorithm", "comappo_adapted"],
    "ippo": ["--algorithm", "ippo"],
    "mappo": ["--algorithm", "mappo"],
    "feature_mlp": ["--algorithm", "mappo", "--features", "task_window"],
    "task_attention": ["--algorithm", "mappo", "--features", "task_window", "--encoder", "task_attention"],
    "no_task": ["--algorithm", "mappo", "--features", "task_window", "--encoder", "task_attention", "--no-task-features"],
    "no_window": ["--algorithm", "mappo", "--features", "task_window", "--encoder", "task_attention", "--no-window-features"],
}


def seeds(text):
    values = [int(value) for value in text.split(",")]
    if not values or len(set(values)) != len(values):
        raise ValueError("Seeds must be nonempty and unique")
    return values


def crossed_interval(matrix):
    """同一测试场景被所有训练重复共享，因此分别重采样行和列。"""
    matrix = np.asarray(matrix, dtype=float)
    rows, columns = matrix.shape
    if rows < 2 or columns < 2:
        return None
    rng = np.random.default_rng(0)
    row_idx = rng.integers(rows, size=(5000, rows, 1))
    col_idx = rng.integers(columns, size=(5000, 1, columns))
    means = matrix[row_idx, col_idx].mean(axis=(1, 2))
    return np.quantile(means, [0.025, 0.975]).tolist()


def aggregate(records, labels, train_seeds, test_seeds, reference="greedy"):
    index = {(r["label"], r["training_seed"], r["test_seed"]): r["metrics"] for r in records}

    def matrix_for(label):
        return np.array([[index[(label, seed if label in VARIANTS else None, case)]["overall"]["task_completion_rate"]
                          for case in test_seeds] for seed in train_seeds])

    baseline = matrix_for(reference)
    result = {}
    for label in labels:
        matrix = matrix_for(label)
        metrics = [r["metrics"] for r in records if r["label"] == label]
        complete = sum(m["overall"]["completed_tasks"] for m in metrics)
        latency = sum((m["latency"]["task_mean"] or 0.0) * m["overall"]["completed_tasks"] for m in metrics)
        result[label] = {
            "completion_rate_mean": float(matrix.mean()),
            "completion_rate_by_training_seed": matrix.mean(axis=1).tolist() if label in VARIANTS else None,
            "paired_delta_vs": reference,
            "paired_delta_mean": float((matrix - baseline).mean()),
            "paired_crossed_bootstrap_95_ci": crossed_interval(matrix - baseline),
            "completed_task_latency_seconds_pooled": latency / complete if complete else None,
        }
    if "task_attention" in labels and "feature_mlp" in labels:
        difference = matrix_for("task_attention") - matrix_for("feature_mlp")
        result["task_attention"]["structure_delta_vs_feature_mlp"] = {
            "mean": float(difference.mean()), "crossed_bootstrap_95_ci": crossed_interval(difference)}
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", required=True)
    p.add_argument("--scenario-distribution")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--resume", action="store_true", help="Continue completed training runs and missing evaluations")
    p.add_argument("--variants", default=",".join(VARIANTS))
    p.add_argument("--train-seeds", default="7,107,207,307,407")
    p.add_argument("--validation-seeds", default="10001,10002,10003")
    p.add_argument("--test-seeds", default="30001,30002,30003,30004,30005")
    p.add_argument("--episodes", type=int, default=20)
    p.add_argument("--eval-every", type=int, default=5)
    p.add_argument("--hidden", type=int, default=64)
    p.add_argument("--rollout-steps", type=int, default=128)
    p.add_argument("--ppo-epochs", type=int, default=4)
    p.add_argument("--shaping-coef", type=float, default=0.0)
    p.add_argument("--min-validation-reachable-rate", type=float, default=0.1)
    args = p.parse_args()
    train_seeds, validation_seeds, test_seeds = map(seeds, (args.train_seeds, args.validation_seeds, args.test_seeds))
    variants = args.variants.split(",")
    if len(set(variants)) != len(variants) or not set(variants) <= set(VARIANTS):
        raise ValueError("Select unique known variants")
    if min(args.episodes, args.eval_every, args.hidden, args.rollout_steps, args.ppo_epochs) <= 0:
        raise ValueError("Counts must be positive")
    if not 0 <= args.min_validation_reachable_rate <= 1:
        raise ValueError("Reachability threshold must be in [0, 1]")
    training_episodes = set().union(*(set(range(seed + 1, seed + args.episodes + 1)) for seed in train_seeds))
    cfg = load_config(args.config)
    distribution = ScenarioDistribution(args.scenario_distribution, cfg) if args.scenario_distribution else None
    if distribution:
        validation_seeds = [case["seed"] for case in distribution.spec["validation"]]
        test_seeds = [case["seed"] for case in distribution.spec["test"]]
    if (training_episodes & (set(validation_seeds) | set(test_seeds))) or set(validation_seeds) & set(test_seeds):
        raise ValueError("Training, validation, and test scenario seeds must be disjoint")
    test_cases = distribution.cases("test") if distribution else [
        (replace(cfg, seed=seed, tile_lifecycle_log=""), {"seed": seed}) for seed in test_seeds]
    if cfg.pipeline_mode != "distributed" or cfg.decision_interval_steps != 1:
        raise ValueError("Study requires distributed mode and one decision per step")
    out = Path(args.out_dir).resolve()
    out.mkdir(parents=True, exist_ok=args.resume)
    manifest = {"status": "running", "args": vars(args), "config": asdict(cfg), **runtime_metadata(),
                "scenario_distribution_sha256": distribution.sha256 if distribution else None,
                "scenario_distribution": distribution.spec if distribution else None,
                "budget_per_training_run": cfg.sim_steps * args.episodes,
                "reward": "completed tasks + optional terminal-corrected potential shaping; gamma=1",
                "uncertainty": "paired crossed bootstrap of training seeds and shared test scenarios; exploratory intervals"}
    if args.resume:
        saved = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
        expected = {key: value for key, value in vars(args).items() if key != "resume"}
        previous = {key: value for key, value in saved["args"].items() if key != "resume"}
        if expected != previous or saved["config"] != asdict(cfg):
            raise ValueError("Resume requires identical study arguments and config")
        if saved.get("scenario_distribution_sha256") != manifest["scenario_distribution_sha256"]:
            raise ValueError("Scenario distribution changed since original study")
        manifest["previous_executions"] = saved.get("previous_executions", []) + [
            {key: saved[key] for key in ("source_sha256", "python_version", "packages")}]
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    torch.set_num_threads(1)
    if args.min_validation_reachable_rate:
        print("checking validation reachability", flush=True)
        if distribution:
            reachability = {"scope": "validation cases, optimistic capacity-free upper bound", "runs": []}
            for case_cfg, case_info in distribution.cases("validation"):
                for run in analyze(case_cfg, [case_info["seed"]])["runs"]:
                    reachability["runs"].append({**run, "scenario": case_info})
        else:
            reachability = analyze(cfg, validation_seeds)
        (out / "validation_reachability.json").write_text(json.dumps(reachability, indent=2), encoding="utf-8")
        mean_upper = float(np.mean([run["completion_rate_upper_bound"] for run in reachability["runs"]]))
        if mean_upper < args.min_validation_reachable_rate:
            manifest.update(status="rejected", reason="validation reachability upper bound below threshold")
            (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            raise ValueError(f"Uninformative validation scenario: optimistic completion upper bound {mean_upper:.3%}")
    records, traces = [], {}
    if args.resume and (out / "evaluations.jsonl").exists():
        records = [json.loads(line) for line in (out / "evaluations.jsonl").read_text(encoding="utf-8").splitlines()]
        for item in records:
            seed = item["test_seed"]
            trace = item["metrics"]["experiment"]["task_trace_sha256"]
            if seed in traces and traces[seed] != trace:
                raise ValueError("Existing records contain mismatched task traces")
            traces[seed] = trace
    completed_cases = {(r["label"], r["training_seed"], r["test_seed"]) for r in records}
    if len(completed_cases) != len(records):
        raise ValueError("Existing evaluations contain duplicate cases")

    def record(label, training_seed, test_seed, metrics, checkpoint=None):
        trace = metrics["experiment"]["task_trace_sha256"]
        if test_seed in traces and traces[test_seed] != trace:
            raise RuntimeError("Mismatched paired task trace")
        traces[test_seed] = trace
        item = dict(label=label, training_seed=training_seed, test_seed=test_seed, metrics=metrics,
                    checkpoint_sha256=hashlib.sha256(checkpoint.read_bytes()).hexdigest() if checkpoint else None)
        records.append(item)
        with (out / "evaluations.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(item, allow_nan=False) + "\n")

    for label in ("local", "greedy", "window_aware"):
        for case_cfg, case_info in test_cases:
            test_seed = case_info["seed"]
            if (label, None, test_seed) not in completed_cases:
                record(label, None, test_seed, run_config(case_cfg, label))
        print(f"evaluated {label}", flush=True)
    for label in variants:
        for seed in train_seeds:
            run_dir = out / label / str(seed)
            run_dir.mkdir(parents=True, exist_ok=args.resume)
            command = [sys.executable, str(Path(__file__).with_name("train_marl.py")),
                       "--config", args.config, "--out-dir", str(run_dir), "--seed", str(seed),
                       "--episodes", str(args.episodes), "--eval-every", str(args.eval_every),
                       "--hidden", str(args.hidden), "--rollout-steps", str(args.rollout_steps),
                       "--ppo-epochs", str(args.ppo_epochs), "--validation-seeds", args.validation_seeds,
                       "--reward-mode", "task", "--gamma", "1", "--shaping-coef", str(args.shaping_coef),
                       *VARIANTS[label]]
            if args.scenario_distribution:
                command.extend(["--scenario-distribution", args.scenario_distribution])
            if args.resume and (run_dir / "run.json").exists():
                last = torch.load(run_dir / "last.pt", map_location="cpu", weights_only=True)
                if last["episode"] != args.episodes or last["config"] != asdict(cfg):
                    raise ValueError(f"Cannot resume incomplete or changed training run: {run_dir}")
                if last.get("scenario_distribution", {}).get("sha256") != manifest["scenario_distribution_sha256"]:
                    raise ValueError(f"Changed training scenario distribution: {run_dir}")
                print(f"reusing completed {label} seed={seed}", flush=True)
            else:
                print(f"training {label} seed={seed}", flush=True)
                with (run_dir / "console.log").open("w", encoding="utf-8") as log:
                    subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT)
            checkpoint = run_dir / "best.pt"
            digest = hashlib.sha256(checkpoint.read_bytes()).hexdigest()
            if any(r["checkpoint_sha256"] != digest for r in records
                   if r["label"] == label and r["training_seed"] == seed):
                raise ValueError(f"Checkpoint changed after evaluation: {checkpoint}")
            for case_cfg, case_info in test_cases:
                test_seed = case_info["seed"]
                if (label, seed, test_seed) not in completed_cases:
                    metrics = run_config(case_cfg, "marl", str(checkpoint))
                    record(label, seed, test_seed, metrics, checkpoint)
            print(f"evaluated {label} seed={seed}", flush=True)
    summaries = aggregate(records, ["local", "greedy", "window_aware", *variants], train_seeds, test_seeds)
    manifest["status"] = "complete"
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (out / "summary.json").write_text(json.dumps(summaries, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps(summaries, indent=2, allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
