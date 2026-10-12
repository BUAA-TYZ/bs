"""在固定开发面板对比预约、相同预测无预约及现有强启发式。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import hashlib
import multiprocessing
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from main import run_config
from sim.config import load_config
from sim.evaluation import paired_task_metrics, task_age_score
from sim.marl.scenarios import ScenarioDistribution
from sim.reproducibility import runtime_metadata


def evaluate(cfg, case, policy):
    started = time.monotonic()
    metrics = run_config(cfg, policy)
    age = task_age_score(metrics["task_outcomes"], [1800, 3600, 7200, 14400], cfg.dt * cfg.sim_steps)
    return {"policy": policy, "case": case, "metrics": metrics, "task_age": age,
            "elapsed_s": time.monotonic() - started}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="examples/config_relay.yaml")
    parser.add_argument("--distribution", default="examples/scenario_roles.json")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--policies", nargs="+", default=["greedy", "window_aware", "reservation_independent", "reservation"])
    parser.add_argument("--fresh-seed", type=int, help="Sample a new diagnostic panel from the training distribution")
    parser.add_argument("--fresh-count", type=int, default=9)
    parser.add_argument("--reference-dir", action="append", default=[], help="Verify matching archived outcomes across source revisions")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    if args.workers < 1 or args.fresh_count < 1:
        raise ValueError("workers and fresh-count must be positive")
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    runtime = runtime_metadata()
    distribution = ScenarioDistribution(args.distribution, load_config(args.config))
    cases = (distribution.cases("validation") if args.fresh_seed is None else
             [distribution.training_case(args.fresh_seed, i + 1) for i in range(args.fresh_count)])
    policies = args.policies
    if "greedy" not in policies or len(set(policies)) != len(policies):
        raise ValueError("policies must include greedy and contain no duplicates")
    manifest = {"args": vars(args), **runtime, "distribution_sha256": distribution.sha256,
                "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "policies": policies, "seeds": [case["seed"] for _, case in cases],
                "cases": [case for _, case in cases],
                "scope": ("existing development panel" if args.fresh_seed is None else "new random development diagnostic panel")
                         + "; not final test or a literature-method comparison"}
    (out / "run.json").write_text(json.dumps(manifest, indent=2))
    started, records = time.monotonic(), []
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as executor:
        futures = [executor.submit(evaluate, cfg, case, policy)
                   for cfg, case in cases for policy in policies]
        for future in as_completed(futures):
            record = future.result()
            if record["metrics"]["experiment"]["source_sha256"] != runtime["source_sha256"]:
                raise ValueError("Core source changed during experiment")
            records.append(record)
            name = f"{record['case']['id']}_{record['policy']}.json"
            record["reference_outcomes_reproduced"] = []
            for directory in args.reference_dir:
                archive = Path(directory) / name
                if not archive.exists():
                    continue
                reference = json.loads(archive.read_text())
                if reference["metrics"]["task_outcomes"] != record["metrics"]["task_outcomes"]:
                    print(f"ERROR: Changed archived outcomes: {archive}", flush=True)
                    raise ValueError(f"Changed archived outcomes: {archive}")
                for field in ("task_trace_sha256", "tle_sha256", "config"):
                    actual = json.loads(json.dumps(record["metrics"]["experiment"][field], allow_nan=False))
                    if reference["metrics"]["experiment"][field] != actual:
                        print(f"ERROR: Changed archive context {field}: {archive}", flush=True)
                        raise ValueError(f"Changed archive context {field}: {archive}")
                record["reference_outcomes_reproduced"].append(str(archive))
            (out / name).write_text(json.dumps(record, indent=2, allow_nan=False))
            print(json.dumps({"completed": len(records), "policy": record["policy"],
                              "case": record["case"]["id"], "score": record["task_age"]["delivery_score"]}), flush=True)
    comparisons = []
    for _, case in cases:
        rows = {r["policy"]: r for r in records if r["case"]["seed"] == case["seed"]}
        baseline = rows["greedy"]["metrics"]
        for policy, row in rows.items():
            for field in ("source_sha256", "task_trace_sha256", "tle_sha256", "config"):
                if row["metrics"]["experiment"][field] != baseline["experiment"][field]:
                    raise ValueError(f"Unmatched experiment: {field}")
            if policy != "greedy":
                comparisons.append({"case": case["id"], "policy": policy,
                                    **paired_task_metrics(baseline["task_outcomes"], row["metrics"]["task_outcomes"])})
    aggregate = []
    for policy in policies:
        rows = [r for r in records if r["policy"] == policy]
        aggregate.append({"policy": policy,
                          "delivery_score": statistics.mean(r["task_age"]["delivery_score"] for r in rows),
                          "restricted_mean_latency_seconds": statistics.mean(r["task_age"]["restricted_mean_latency_seconds"] for r in rows),
                          "completed_tasks": sum(r["metrics"]["overall"]["completed_tasks"] for r in rows),
                          "total_tasks": sum(r["metrics"]["overall"]["total_tasks"] for r in rows)})
    summary = {**manifest, "elapsed_s": time.monotonic() - started, "aggregate": aggregate,
               "comparisons_vs_greedy": comparisons,
               "records": sorted(records, key=lambda r: (r["case"]["seed"], r["policy"]))}
    (out / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False))
    lines = ["# Reservation Development Comparison", "",
             "| Policy | Delivery score | Complete tasks | Restricted mean (s) |", "|---|---:|---:|---:|"]
    for row in aggregate:
        lines.append(f"| {row['policy']} | {100*row['delivery_score']:.4f}% | {row['completed_tasks']}/{row['total_tasks']} | {row['restricted_mean_latency_seconds']:.1f} |")
    lines += ["", manifest["scope"], "", "Identical environment and candidate limits. Reservation policies additionally consume committed-work broadcasts and coordinate synchronously. No signaling overhead or guaranteed execution slots are modeled.", ""]
    (out / "summary.md").write_text("\n".join(lines))
    print(json.dumps(aggregate, indent=2))


if __name__ == "__main__":
    main()
