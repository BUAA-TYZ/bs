"""固定重叠工作负载的配对评估；分别保留启发式、BC 与 PPO 结果。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
import hashlib
import json
import multiprocessing
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.evaluate_literature import METHODS, common_context, evaluate, summarize
from sim.config import load_config
from sim.marl.scenarios import ScenarioDistribution
from sim.reproducibility import runtime_metadata


def evaluate_one(cfg, case, name, checkpoint, horizons):
    record = evaluate(cfg, case, "marl_bc" if checkpoint else name, checkpoint or "", horizons)
    record["method"] = name
    return record


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--distribution", required=True)
    parser.add_argument("--split", choices=("validation", "test", "training"), default="validation")
    parser.add_argument("--training-seed", type=int, default=202610140)
    parser.add_argument("--training-count", type=int, default=6)
    parser.add_argument("--case-ids", nargs="+", help="Optional explicit subset, retained in the manifest")
    parser.add_argument("--methods", nargs="+", choices=[m for m in METHODS if m != "marl_bc"],
                        default=[m for m in METHODS if m != "marl_bc"])
    parser.add_argument("--checkpoint", action="append", default=[], metavar="NAME=PATH")
    parser.add_argument("--horizons-minutes", default="30,60,120,240")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    horizons = [60 * float(x) for x in args.horizons_minutes.split(",")]
    if args.workers < 1 or args.training_count < 1 or horizons != sorted(set(horizons)) or min(horizons) <= 0:
        raise ValueError("Invalid workers, training count or horizons")
    checkpoints = {}
    for item in args.checkpoint:
        name, path = item.split("=", 1)
        if not name or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for c in name):
            raise ValueError("Checkpoint names must use lowercase letters, digits, '_' or '-'")
        if name in checkpoints or name in args.methods:
            raise ValueError("Method names must be unique")
        checkpoints[name] = path
    methods = list(args.methods) + list(checkpoints)
    if not methods or len(methods) != len(set(methods)):
        raise ValueError("Select distinct methods")
    base = load_config(args.config)
    distribution = ScenarioDistribution(args.distribution, base)
    if base.image_tiling is None or distribution.spec["version"] != 3:
        raise ValueError("This runner requires explicit image_tiling and scenario schema v3")
    cases = (distribution.cases(args.split) if args.split != "training" else
             [distribution.training_case(args.training_seed, i + 1) for i in range(args.training_count)])
    if args.case_ids:
        known = {info["id"] for _, info in cases}
        if not set(args.case_ids) <= known:
            raise ValueError("Unknown case IDs")
        cases = [(cfg, info) for cfg, info in cases if info["id"] in args.case_ids]
    for cfg, _ in cases:
        end = cfg.task_arrival_end_step if cfg.task_arrival_end_step is not None else cfg.sim_steps
        if (cfg.sim_steps - end) * cfg.dt < max(horizons):
            raise ValueError("Every task requires complete follow-up through the largest horizon")
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    source = runtime_metadata()
    checkpoint_hashes = {name: hashlib.sha256(Path(path).read_bytes()).hexdigest()
                         for name, path in checkpoints.items()}
    manifest = {
        "args": vars(args), **source, "delivery_horizons_seconds": horizons,
        "checkpoint_sha256": checkpoint_hashes, "distribution_sha256": distribution.sha256,
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "cases": [{"case": info, "config": asdict(cfg)} for cfg, info in cases],
        "scope": "paired controlled-proxy simulation; not flight hardware validation or detection generalization",
        "protocol": "docs/thesis/fixed_overlap_protocol.md",
    }
    write_json(out / "run.json", manifest)
    started, records = time.monotonic(), []
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        pending = [pool.submit(evaluate_one, cfg, info, method, checkpoints.get(method), horizons)
                   for cfg, info in cases for method in methods]
        for future in as_completed(pending):
            record = future.result()
            if (record["metrics"]["experiment"]["source_sha256"] != source["source_sha256"] or
                    runtime_metadata()["source_sha256"] != source["source_sha256"]):
                raise RuntimeError("Source changed during evaluation")
            method = record["method"]
            if method in checkpoints and record["checkpoint_sha256"] != checkpoint_hashes[method]:
                raise RuntimeError("Checkpoint changed during evaluation")
            for previous in records:
                if previous["case"]["id"] == record["case"]["id"] and common_context(previous) != common_context(record):
                    raise RuntimeError("Methods did not receive the same configuration, orbit and task trace")
            if record["task_age"]["delivery_score"] is None:
                raise RuntimeError("Empty case cannot supply a delivery rate")
            records.append(record)
            write_json(out / f"{record['case']['id']}_{method}.json", record)
            print(json.dumps({"finished": len(records), "total": len(pending), "case": record["case"]["id"],
                              "method": method, "score": record["task_age"]["delivery_score"]}), flush=True)
    aggregate, comparisons = summarize(records, methods)
    summary = {**manifest, "elapsed_s": time.monotonic() - started, "aggregate": aggregate,
               "comparisons": comparisons,
               "records": [f"{r['case']['id']}_{r['method']}.json" for r in records]}
    write_json(out / "summary.json", summary)
    lines = ["# Fixed-Overlap Paired Evaluation", "", manifest["scope"], "",
             "| Method | Age delivery score | Completed / arrived | Restricted latency (s) |",
             "|---|---:|---:|---:|"]
    for row in aggregate:
        lines.append(f"| {row['method']} | {100 * row['delivery_score']:.3f}% | "
                     f"{row['completed_tasks']} / {row['total_tasks']} | {row['restricted_mean_latency_seconds']:.2f} |")
    lines += ["", "Scores and restricted latencies use equal case weights. Completed counts are pooled.",
              "BC, PPO and heuristic results are separate. A single training seed does not establish robustness.", ""]
    (out / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(aggregate, indent=2), flush=True)


if __name__ == "__main__":
    main()
