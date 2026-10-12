"""在相同测试任务轨迹上评估启发式策略与已训练的 MARL checkpoint。"""

import argparse
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import numpy as np
import torch

from main import run_config
from sim.config import load_config
from sim.evaluation import paired_task_metrics, task_age_delivery_curve, task_age_score
from sim.scheduler.marl_policy import MARLPolicy
from sim.marl.scenarios import ScenarioDistribution


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--policies", default="ground_compute,local,greedy,window_aware")
    parser.add_argument("--reference", help="Paired comparison reference label; defaults to first policy")
    parser.add_argument("--drain-steps", type=int, default=0,
                        help="Append steps with no new arrivals; identical for all methods")
    parser.add_argument("--ground-relay-delay-s", type=float)
    parser.add_argument("--aggregation-delay-s", type=float)
    parser.add_argument("--delivery-horizons-minutes",
                        help="Comma-separated task ages; requires sufficient follow-up for every arrival")
    parser.add_argument("--checkpoint", action="append", default=[], metavar="LABEL=PATH")
    parser.add_argument("--seeds", default="20001,20002,20003,20004,20005")
    parser.add_argument("--output", required=True)
    parser.add_argument("--scenario-distribution")
    parser.add_argument("--split", choices=("validation", "test"), default="test")
    args = parser.parse_args()
    delivery_horizons = ([60 * float(value) for value in args.delivery_horizons_minutes.split(",")]
                         if args.delivery_horizons_minutes else None)
    if delivery_horizons is not None:
        task_age_delivery_curve([], delivery_horizons, 0)
    cfg = load_config(args.config)
    if args.drain_steps < 0:
        raise ValueError("drain-steps must be nonnegative")
    overrides = {key: getattr(args, key) for key in ("ground_relay_delay_s", "aggregation_delay_s")
                 if getattr(args, key) is not None}
    if args.drain_steps:
        overrides.update(sim_steps=cfg.sim_steps + args.drain_steps,
                         task_arrival_end_step=(cfg.task_arrival_end_step
                                                if cfg.task_arrival_end_step is not None else cfg.sim_steps))
    cfg = replace(cfg, **overrides)
    if cfg.decision_interval_steps != 1:
        raise ValueError("Comparable evaluation requires decision_interval_steps=1")
    seeds = [int(seed) for seed in args.seeds.split(",")]
    distribution = ScenarioDistribution(args.scenario_distribution, cfg) if args.scenario_distribution else None
    cases = distribution.cases(args.split) if distribution else [
        (replace(cfg, seed=seed, tile_lifecycle_log=""), {"id": f"seed_{seed}", "seed": seed}) for seed in seeds]
    seeds = [case["seed"] for _, case in cases]
    if len(seeds) != len(set(seeds)) or not seeds:
        raise ValueError("Test seeds must be nonempty and unique")
    output = Path(args.output)
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite experiment: {output}")
    policies = [(name, name, "") for name in args.policies.split(",") if name]
    checkpoints = {}
    torch.set_num_threads(1)
    for item in args.checkpoint:
        label, path = item.split("=", 1)
        policy = MARLPolicy(path)
        metadata = policy.metadata
        seen = set(metadata["training_seeds"])
        if args.split == "test":
            seen |= set(metadata["validation_seeds"])
        if seen & set(seeds):
            raise ValueError(f"{label}: evaluation seeds overlap a prohibited training/validation split")
        checkpoints[label] = {"path": path, "sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest(),
                              "algorithm": metadata["algorithm"], "episode": metadata["episode"],
                              "training_seed": metadata["seed"]}
        policies.append((label, "marl", path))
    if not policies or len({label for label, _, _ in policies}) != len(policies):
        raise ValueError("Policy labels must be nonempty and unique")
    reference_label = args.reference or policies[0][0]
    if reference_label not in {label for label, _, _ in policies}:
        raise ValueError("Reference must name an evaluated policy/checkpoint label")
    runs = []
    traces = {}
    for label, policy, path in policies:
        for case_cfg, case_info in cases:
            seed = case_info["seed"]
            mode = policy if policy in ("ground_compute", "ground_relay", "ground_tiles") else "distributed"
            result = run_config(replace(case_cfg, pipeline_mode=mode), policy, path)
            if delivery_horizons is not None:
                result["task_age_delivery_curve"] = task_age_delivery_curve(
                    result["task_outcomes"], delivery_horizons,
                    result["experiment"]["elapsed_sim_seconds"])
            trace = result["experiment"]["task_trace_sha256"]
            if seed in traces and traces[seed] != trace:
                raise RuntimeError("Policies were evaluated on different task arrival traces")
            traces[seed] = trace
            runs.append({"label": label, "seed": seed, "scenario": case_info, "metrics": result})
            overall = result["overall"]
            print(f"{label} seed={seed}: tasks={overall['completed_tasks']}/{overall['total_tasks']}", flush=True)
    aggregates = {}
    first_label = reference_label
    reference = np.array([r["metrics"]["overall"]["task_completion_rate"]
                          for r in runs if r["label"] == first_label])
    for label, _, _ in policies:
        records = [r["metrics"] for r in runs if r["label"] == label]
        rates = np.array([r["overall"]["task_completion_rate"] for r in records])
        completed = sum(r["overall"]["completed_tasks"] for r in records)
        pooled_latency = (sum((r["latency"]["task_mean"] or 0.0) * r["overall"]["completed_tasks"]
                              for r in records) / completed) if completed else None
        delta = rates - reference
        rng = np.random.default_rng(0)
        ci = np.quantile(rng.choice(delta, size=(5000, len(delta))).mean(axis=1), [0.025, 0.975]) if len(seeds) >= 2 else None
        aggregates[label] = {
            "task_completion_rate_mean": float(rates.mean()),
            "task_completion_rate_std": float(rates.std(ddof=1)) if len(seeds) > 1 else None,
            "completed_task_latency_seconds_pooled": pooled_latency,
            "paired_completion_rate_delta_vs": first_label,
            "paired_delta_mean": float(delta.mean()),
            "paired_bootstrap_95_ci": ci.tolist() if ci is not None else None,
        }
        if delivery_horizons:
            age_scores = [task_age_score(r["task_outcomes"], delivery_horizons,
                                         r["experiment"]["elapsed_sim_seconds"]) for r in records]
            if any(row["delivery_score"] is None for row in age_scores):
                raise ValueError("Cannot aggregate task-age scores of empty task cases")
            aggregates[label]["task_age"] = {
                "horizons_seconds": delivery_horizons,
                "delivery_score_mean": float(np.mean([r["delivery_score"] for r in age_scores])),
                "delivery_rate_mean_by_horizon": [float(np.mean([
                    r["delivery_curve"][i]["delivery_rate"] for r in age_scores]))
                    for i in range(len(delivery_horizons))],
                "restricted_mean_latency_seconds": float(np.mean([
                    r["restricted_mean_latency_seconds"] for r in age_scores])),
            }
    paired = []
    reference_runs = {r["seed"]: r for r in runs if r["label"] == reference_label}
    for run in runs:
        if run["label"] == reference_label:
            continue
        ref = reference_runs[run["seed"]]
        paired.append({"label": run["label"], "reference": reference_label,
                       "seed": run["seed"], "scenario": run["scenario"],
                       **paired_task_metrics(ref["metrics"]["task_outcomes"], run["metrics"]["task_outcomes"])})
    report = {"model_version": "2.0", "evaluation_schema_version": 2,
              "reference": reference_label, "paired_task_comparisons": paired,
              "seeds": seeds, "split": args.split, "checkpoints": checkpoints,
              "scenario_distribution_sha256": distribution.sha256 if distribution else None,
              "uncertainty_scope": "scenario cases only; does not measure variability across independent training runs",
              "aggregate": aggregates, "runs": runs}
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
    print(json.dumps(aggregates, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
