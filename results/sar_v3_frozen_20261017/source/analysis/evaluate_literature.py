"""文献适配方法、固定学习 checkpoint 和全地面对照的配对开发评估。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
import json
import multiprocessing
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main as simulator
import numpy as np
import torch
from sim.config import load_config
from sim.evaluation import paired_task_metrics, task_age_score
from sim.marl.scenarios import ScenarioDistribution
from sim.reproducibility import runtime_metadata

METHODS = ("ground_compute", "ground_tiles", "ground_relay", "local", "greedy", "window_aware",
           "reservation_calendar", "heft_adapted", "cgs_compute_adapted", "marl_bc")
CHECKPOINT = "checkpoints/relay_greedy_dagger_mappo_7_20261010/teacher_bc.pt"


def evaluate(cfg, case, method, checkpoint, delivery_horizons=None):
    torch.set_num_threads(1)
    calls, active = [], []
    original_factory = simulator.make_policy

    def measured_factory(*args, **kwargs):
        policy = original_factory(*args, **kwargs)
        select = policy.select_actions

        def measured(state):
            start = time.perf_counter_ns()
            actions = select(state)
            elapsed = (time.perf_counter_ns() - start) / 1e6
            calls.append(elapsed)
            if state.tiles:
                active.append(elapsed)
            return actions

        policy.select_actions = measured
        return policy

    policy_name = "marl" if method == "marl_bc" else method
    effective = simulator.apply_execution_mode(cfg, policy_name)
    simulator.make_policy = measured_factory
    start = time.monotonic()
    try:
        metrics = simulator.run_config(effective, policy_name, checkpoint if method == "marl_bc" else "")
    finally:
        simulator.make_policy = original_factory
    elapsed = time.monotonic() - start
    return {"case": case, "method": method, "metrics": metrics,
            "checkpoint_sha256": hashlib.sha256(Path(checkpoint).read_bytes()).hexdigest()
                if method == "marl_bc" else None,
            "task_age": task_age_score(metrics["task_outcomes"],
                                       delivery_horizons or [1800, 3600, 7200, 14400], cfg.sim_steps * cfg.dt),
            "elapsed_s": elapsed,
            "decision_timing": {"scope": "select_actions only; excludes export_state and geometry; host CPU",
                                "calls": len(calls), "total_ms": sum(calls), "active_calls": len(active),
                                "active_median_ms": float(np.median(active)) if active else None,
                                "active_p95_ms": float(np.quantile(active, .95)) if active else None,
                                "active_max_ms": max(active) if active else None}}


def common_context(record):
    experiment = record["metrics"]["experiment"]
    config = json.loads(json.dumps(experiment["config"]))
    config.pop("pipeline_mode")
    return {"config_except_pipeline_mode": config,
            **{key: experiment[key] for key in ("source_sha256", "task_trace_sha256", "tle_sha256")}}


def summarize(records, methods):
    aggregate = []
    for method in methods:
        rows = [r for r in records if r["method"] == method]
        aggregate.append({"method": method, "cases": len(rows),
                          "delivery_score": statistics.mean(r["task_age"]["delivery_score"] for r in rows),
                          "restricted_mean_latency_seconds": statistics.mean(
                              r["task_age"]["restricted_mean_latency_seconds"] for r in rows),
                          "completed_tasks": sum(r["metrics"]["overall"]["completed_tasks"] for r in rows),
                          "total_tasks": sum(r["metrics"]["overall"]["total_tasks"] for r in rows),
                          "simulation_seconds_sum": sum(r["elapsed_s"] for r in rows),
                          "scheduler_seconds_sum": sum(r["decision_timing"]["total_ms"] / 1000 for r in rows)})
    comparisons = []
    keys = ("common_completed_tasks", "reference_only_completed_tasks", "candidate_only_completed_tasks",
            "candidate_faster_tasks", "candidate_slower_tasks", "equal_latency_tasks")
    for reference in ("greedy", "ground_compute", "ground_tiles", "ground_relay", "reservation_calendar"):
        if reference not in methods:
            continue
        baseline = {r["case"]["id"]: r for r in records if r["method"] == reference}
        for method in methods:
            if method == reference:
                continue
            differences, cases, totals = [], [], {k: 0 for k in keys}
            for row in sorted((r for r in records if r["method"] == method), key=lambda r: r["case"]["id"]):
                ref = baseline[row["case"]["id"]]
                if common_context(row) != common_context(ref):
                    raise ValueError("Unmatched experiment context")
                diff = row["task_age"]["delivery_score"] - ref["task_age"]["delivery_score"]
                pair = paired_task_metrics(ref["metrics"]["task_outcomes"], row["metrics"]["task_outcomes"])
                for key in keys:
                    totals[key] += pair[key]
                differences.append(diff)
                cases.append({"case": row["case"]["id"], "score_difference": diff, **pair})
            comparisons.append({"reference": reference, "method": method,
                                "mean_score_difference": statistics.mean(differences),
                                "wins": sum(d > 1e-12 for d in differences),
                                "ties": sum(abs(d) <= 1e-12 for d in differences),
                                "losses": sum(d < -1e-12 for d in differences), **totals, "cases": cases})
    return aggregate, comparisons


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="examples/config_relay.yaml")
    parser.add_argument("--distribution", default="examples/scenario_roles.json")
    parser.add_argument("--panel", choices=("existing", "fresh"), required=True)
    parser.add_argument("--fresh-seed", type=int, default=2026101400)
    parser.add_argument("--fresh-count", type=int, default=9)
    parser.add_argument("--methods", nargs="+", choices=METHODS, default=list(METHODS))
    parser.add_argument("--checkpoint", default=CHECKPOINT)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    if args.workers < 1 or args.fresh_count < 1 or len(set(args.methods)) != len(args.methods):
        raise ValueError("Invalid workers, case count, or duplicate method")
    distribution = ScenarioDistribution(args.distribution, load_config(args.config))
    cases = distribution.cases("validation") if args.panel == "existing" else [
        distribution.training_case(args.fresh_seed, i + 1) for i in range(args.fresh_count)]
    output = Path(args.out_dir)
    output.mkdir(parents=True, exist_ok=False)
    source = runtime_metadata()
    checkpoint_hash = hashlib.sha256(Path(args.checkpoint).read_bytes()).hexdigest() if "marl_bc" in args.methods else None
    manifest = {"args": vars(args), **source, "checkpoint_sha256": checkpoint_hash,
                "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "distribution_sha256": distribution.sha256, "cases": [case for _, case in cases],
                "scope": "paired development comparison of literature adaptations; NOT original-paper numerical reproduction or final test",
                "protocol": "docs/thesis/literature_baselines.md"}
    (output / "run.json").write_text(json.dumps(manifest, indent=2, allow_nan=False))
    records, started = [], time.monotonic()
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as executor:
        futures = [executor.submit(evaluate, cfg, case, method, args.checkpoint)
                   for cfg, case in cases for method in args.methods]
        for future in as_completed(futures):
            record = future.result()
            if (record["metrics"]["experiment"]["source_sha256"] != source["source_sha256"] or
                    runtime_metadata()["source_sha256"] != source["source_sha256"]):
                raise ValueError("Core source changed during experiment")
            if record["method"] == "marl_bc" and record["checkpoint_sha256"] != checkpoint_hash:
                raise ValueError("Checkpoint changed during experiment")
            for reference in records:
                if reference["case"]["id"] == record["case"]["id"] and common_context(reference) != common_context(record):
                    raise ValueError("Unmatched task/geometry/config across methods")
            filename = f"{record['case']['id']}_{record['method']}.json"
            (output / filename).write_text(json.dumps(record, indent=2, allow_nan=False))
            records.append(record)
            print(json.dumps({"completed": len(records), "total": len(futures), "case": record["case"]["id"],
                              "method": record["method"], "score": record["task_age"]["delivery_score"]}), flush=True)
    aggregate, comparisons = summarize(records, args.methods)
    summary = {**manifest, "elapsed_s": time.monotonic() - started, "aggregate": aggregate,
               "comparisons": comparisons,
               "records": [f"{r['case']['id']}_{r['method']}.json" for r in sorted(records, key=lambda r: (r["case"]["id"], r["method"]))]}
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False))
    lines = ["# Literature Adaptation Development Comparison", "", manifest["scope"], "",
             "| Method | Delivery score | Complete | Restricted mean (s) | Scheduler elapsed time (s) |",
             "|---|---:|---:|---:|---:|"]
    for row in aggregate:
        lines.append(f"| {row['method']} | {100*row['delivery_score']:.4f}% | "
                     f"{row['completed_tasks']}/{row['total_tasks']} | "
                     f"{row['restricted_mean_latency_seconds']:.2f} | {row['scheduler_seconds_sum']:.2f} |")
    lines += ["", "Scores/latencies are case-weighted. Completes are pooled. Timing is select_actions elapsed time under concurrent host load, not spacecraft inference latency.", ""]
    (output / "summary.md").write_text("\n".join(lines))
    print(json.dumps(aggregate, indent=2))


if __name__ == "__main__":
    main()
