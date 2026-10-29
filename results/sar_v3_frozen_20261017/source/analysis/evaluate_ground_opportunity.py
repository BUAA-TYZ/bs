"""只增加未来地面计算机会的配对消融；不修改原 CGS 基线或公共执行器。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
import json
import multiprocessing
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.evaluate_literature import evaluate, common_context, summarize
import main as simulator
from sim.config import load_config
from sim.marl.scenarios import ScenarioDistribution
from sim.reproducibility import runtime_metadata
from sim.scheduler.literature import CGSComputeAdapted

METHODS = ("reservation_calendar", "cgs_compute_adapted", "cgs_ground_wait")


def run(cfg, case, method):
    original = simulator.make_policy

    def factory(name, *args, **kwargs):
        return CGSComputeAdapted(future_ground=True) if name == "cgs_ground_wait" else original(name, *args, **kwargs)

    simulator.make_policy = factory
    try:
        return evaluate(cfg, case, method, "")
    finally:
        simulator.make_policy = original


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", choices=("probe", "development", "confirmation"), required=True)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    dist = ScenarioDistribution("examples/scenario_roles.json", load_config("examples/config_relay.yaml"))
    fresh = [dist.training_case(2026101400, i) for i in range(1, 10)]
    cases = ([fresh[2], fresh[7]] if args.panel == "probe" else dist.cases("validation") + fresh
             if args.panel == "development" else [dist.training_case(2026101500, i) for i in range(1, 10)])
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    manifest = {"args": vars(args), **runtime_metadata(), "cases": [c for _, c in cases],
                "methods": METHODS, "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "shared_runner_sha256": hashlib.sha256(Path("analysis/evaluate_literature.py").read_bytes()).hexdigest(),
                "distribution_sha256": dist.sha256,
                "protocol": "docs/thesis/experiment_log.md"}
    (out / "run.json").write_text(json.dumps(manifest, indent=2, allow_nan=False))
    records = []
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        jobs = [pool.submit(run, cfg, case, method) for cfg, case in cases for method in METHODS]
        for job in as_completed(jobs):
            row = job.result()
            if runtime_metadata()["source_sha256"] != manifest["source_sha256"]:
                raise ValueError("Source changed during experiment")
            if row["metrics"]["experiment"]["source_sha256"] != manifest["source_sha256"]:
                raise ValueError("Worker source differs from manifest")
            for other in records:
                if other["case"]["id"] == row["case"]["id"] and common_context(row) != common_context(other):
                    raise ValueError("Unmatched paired context")
            if args.panel != "confirmation" and row["method"] != "cgs_ground_wait":
                panel = "fresh" if row["case"]["id"].startswith("train_") else "existing"
                old = json.loads(Path(f"results/literature_{panel}_20261014/{row['case']['id']}_{row['method']}.json").read_text())
                if old["metrics"]["task_outcomes"] != row["metrics"]["task_outcomes"]:
                    raise ValueError("Baseline no longer reproduces archived outcomes")
                row["archived_outcomes_reproduced"] = True
            records.append(row)
            (out / f"{row['case']['id']}_{row['method']}.json").write_text(json.dumps(row, indent=2, allow_nan=False))
            print(json.dumps({"completed": len(records), "total": len(jobs), "case": row["case"]["id"],
                              "method": row["method"], "score": row["task_age"]["delivery_score"]}), flush=True)
    aggregate, comparisons = summarize(records, METHODS)
    # 共用统计器以 CGS 别名作为参考，保留逐任务新增/丢失和配对时延。
    aliased = [{**r, "method": "greedy" if r["method"] == "cgs_compute_adapted" else r["method"]} for r in records]
    _, extra = summarize(aliased, ("greedy", "cgs_ground_wait"))
    comparisons += [{**r, "reference": "cgs_compute_adapted"} for r in extra]
    summary = {**manifest, "aggregate": aggregate, "comparisons": comparisons,
               "records": sorted(f"{r['case']['id']}_{r['method']}.json" for r in records)}
    (out / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False))
    print(json.dumps(aggregate, indent=2))


if __name__ == "__main__":
    main()
