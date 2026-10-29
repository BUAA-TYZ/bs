"""复用冻结时间步诊断，仅补跑一个更细时间步并与已有结果配对。"""

import argparse
from dataclasses import asdict
import gzip
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis import check_twohop_window_timestep as diagnostic
from sim.evaluation import paired_task_metrics
from sim.reproducibility import runtime_metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--compare", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--dt", type=float, default=0.05)
    args = parser.parse_args()
    original_path, comparison_path = Path(args.reference), Path(args.compare)
    original = json.loads(original_path.read_bytes())
    comparison = json.loads(comparison_path.read_bytes())
    source = runtime_metadata()
    source_sha = source["source_sha256"]
    if (original["metrics"]["experiment"]["source_sha256"] != source_sha
            or comparison["source_after"]["source_sha256"] != source_sha):
        raise ValueError("Original, comparison and current core must match")
    if args.dt >= comparison["dt"] or original["method"] != "cgs_compute_adapted":
        raise ValueError("Requires CGS and a strictly finer timestep")
    stop = comparison["metrics"]["experiment"]["elapsed_sim_seconds"]
    cfg = diagnostic.prefix_config(original["metrics"]["experiment"]["config"], args.dt, stop)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    helper_path = Path(diagnostic.__file__)
    helper_sha = hashlib.sha256(helper_path.read_bytes()).hexdigest()
    wrapper_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    manifest = {"scope": "additional numerical convergence diagnostic; no new performance panel",
                "args": vars(args), "observation_end_seconds": stop,
                "source_before": source, "wrapper_sha256": wrapper_sha,
                "helper_sha256": helper_sha,
                "reference_sha256": hashlib.sha256(original_path.read_bytes()).hexdigest(),
                "comparison_sha256": hashlib.sha256(comparison_path.read_bytes()).hexdigest()}
    diagnostic.save(out / "run.json", manifest)
    diagnostic.save(out / f"dt_{str(args.dt).replace('.', 'p')}_config.json", asdict(cfg))
    record = diagnostic.run_diagnostic(asdict(cfg), str(out), source_sha, helper_sha)
    for key in ("physical_task_trace_sha256", "geometry_context_sha256"):
        if record[key] != comparison[key]:
            raise RuntimeError(f"Physical invariance check failed: {key}")
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != wrapper_sha:
        raise RuntimeError("Wrapper changed during execution")
    old = comparison["metrics"]["task_outcomes"]
    new = record["metrics"]["task_outcomes"]
    pairing = paired_task_metrics(old, new)
    outcomes = [{"task_id": a["task_id"], "reference_latency_seconds": a["latency_seconds"],
                 "candidate_latency_seconds": b["latency_seconds"],
                 "difference_seconds": b["latency_seconds"] - a["latency_seconds"]
                 if a["latency_seconds"] is not None and b["latency_seconds"] is not None else None,
                 "reference_on_time": a["latency_seconds"] is not None and a["latency_seconds"] <= 300,
                 "candidate_on_time": b["latency_seconds"] is not None and b["latency_seconds"] <= 300}
                for a, b in zip(old, new)]
    with gzip.open(comparison_path.parent / comparison["evidence_file"], "rt", encoding="utf-8") as handle:
        reference_evidence = json.load(handle)
    with gzip.open(out / record["evidence_file"], "rt", encoding="utf-8") as handle:
        candidate_evidence = json.load(handle)
    snapshots = {}
    for label, evidence in (("reference", reference_evidence), ("candidate", candidate_evidence)):
        snapshots[label] = [{"seconds": row["seconds"], "tasks": [task for task in row["tasks"]
                             if task["task_id"] in ("task_3", "task_4", "task_5")]}
                            for row in evidence["snapshots"] if row["seconds"] in (5830, 6040)]
    maximum = max(abs(row["difference_seconds"]) for row in outcomes if row["difference_seconds"] is not None)
    summary = {**manifest, "source_after": runtime_metadata(),
               "reference_dt": comparison["dt"], "candidate_dt": record["dt"],
               "wall_seconds": record["wall_seconds"], "physical_invariance_passed": True,
               "physical_task_trace_sha256": record["physical_task_trace_sha256"],
               "geometry_context_sha256": record["geometry_context_sha256"],
               "reference_task_age": comparison["task_age"], "candidate_task_age": record["task_age"],
               "paired_task_metrics": pairing, "per_task": outcomes,
               "max_absolute_latency_difference_seconds": maximum,
               "on_time_classification_changes": sum(row["reference_on_time"] != row["candidate_on_time"] for row in outcomes),
               "reference_task_tails": comparison["task_tails"], "candidate_task_tails": record["task_tails"],
               "critical_window_snapshots": snapshots}
    if summary["source_after"]["source_sha256"] != source_sha:
        raise RuntimeError("Core changed before comparison finished")
    diagnostic.save(out / "summary.json", summary)
    print(json.dumps({"reference_dt": comparison["dt"], "candidate_dt": record["dt"],
                      "wall_seconds": record["wall_seconds"], "per_task": outcomes,
                      "reference_task_age": comparison["task_age"], "candidate_task_age": record["task_age"],
                      "maximum_latency_difference_seconds": maximum,
                      "on_time_classification_changes": summary["on_time_classification_changes"]}, indent=2), flush=True)


if __name__ == "__main__":
    main()
