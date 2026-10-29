"""仅运行三方案候选的冻结七条件开发面板，不读取测试日期数据。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import multiprocessing
from pathlib import Path
import shutil
import traceback
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import run_sar_frozen as runner
from analysis import sar_three_plan as candidate
from analysis.report_sar_task_choice import compare
from analysis.sar_condition_scan import strong_ground, write_json, file_hash, measured_job
from sim.reproducibility import runtime_metadata

ROOT = Path("results/sar_choice_revision_20261017")
SCAN = Path("results/sar_condition_scan_20261017")


def run_job(condition, hops, cfg, source_sha, extension_sha, variant="v2"):
    assert file_hash(Path(candidate.__file__)) == extension_sha
    factory, environment, observer = runner.simulator.make_policy, runner.simulator.SimulationEnv, runner.Observer
    policies, envs = [], []

    class Environment(candidate.CandidateEnv):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            envs.append(self)

    def make_policy(*a, **kw):
        policy = candidate.ThreePlanChoice(hops)
        policies.append(policy)
        return policy

    class Progress(observer):
        def on_step(self, env, step_idx, actions):
            super().on_step(env, step_idx, actions)
            if (step_idx + 1) % 600000 == 0:
                print(json.dumps({"condition": condition, "sim_seconds": env.time * env.cfg.dt}), flush=True)

    runner.simulator.make_policy = make_policy
    runner.simulator.SimulationEnv = Environment
    runner.Observer = Progress
    try:
        record = measured_job(cfg, "three_plan_" + variant, source_sha, file_hash(Path(runner.__file__)), 6)
    finally:
        runner.simulator.make_policy, runner.simulator.SimulationEnv = factory, environment
        runner.Observer = observer
    env = envs[0]
    errors = []
    for task_id, budget in env._dist_pipeline.raw_tasks.items():
        tiles = [env.tiles[tid] for tid in env.tasks[task_id].tile_ids]
        started = [t for t in tiles if t.timestamps.start_compute is not None]
        if (abs(sum(t.data_size_mb for t in tiles) - env.tasks[task_id].image_size_mb) > 1e-6 or
                any(len(t.raw_relay_path) - 1 > budget for t in tiles)):
            errors.append(task_id)
        if started and (any(t.timestamps.end_tx is None for t in tiles) or
                        min(t.timestamps.start_compute for t in started) < max(t.timestamps.end_tx for t in tiles)):
            errors.append(task_id)
    assert extension_sha == file_hash(Path(candidate.__file__))
    record.update(condition=condition, raw_ground_hops=hops, extension_sha256=extension_sha,
                  derived_config_sha256=runner.digest(cfg), decisions=policies[0].decisions,
                  selection_counts=dict(policies[0].stats), unique_ground_tasks=env._dist_pipeline.raw_tasks,
                  unique_ground_audit={"passed": not errors, "errors": errors})
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", default="v1", choices=("v1", "v2", "v3"))
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    out = ROOT / args.variant
    assert not out.exists(), "Use a fresh registered variant"
    manifest = json.loads((SCAN / "manifest.json").read_text())
    source = runtime_metadata()["source_sha256"]
    assert source == manifest["source_sha256"]
    extension = file_hash(Path(candidate.__file__))
    jobs = []
    for condition in manifest["conditions"]:
        records = {j["variant"]: json.loads(Path(j["path"]).read_text())
                   for j in manifest["jobs"] if j["condition"] == condition}
        ground = strong_ground(records)
        hops = 8 if ground == "ground_8" else 45
        config_path = SCAN / "configs" / f"{condition}_loopfree.json"
        jobs.append({"condition": condition, "raw_hops": hops, "strong_ground": ground,
                     "config": str(config_path), "config_sha256": file_hash(config_path)})
    out.mkdir()
    shutil.copyfile(candidate.__file__, out / "candidate_source.py")
    shutil.copyfile(__file__, out / "runner_source.py")
    write_json(out / "manifest.json", {"variant": args.variant, "source_sha256": source,
               "extension_sha256": extension, "runner_sha256": file_hash(Path(__file__)), "jobs": jobs})
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = {pool.submit(run_job, j["condition"], j["raw_hops"], json.loads(Path(j["config"]).read_text()),
                               source, extension, args.variant): j for j in jobs}
        for future in as_completed(futures):
            job = futures[future]
            try:
                record = future.result()
            except Exception:
                write_json(out / f"{job['condition']}_error.json", {"traceback": traceback.format_exc()})
                print(f"FAILED {job['condition']}: {traceback.format_exc()}", flush=True)
                continue
            record["variant"] = args.variant
            record["method"] = "three_plan_" + args.variant
            write_json(out / f"{job['condition']}.json", record)
            print(json.dumps({k: record[k] for k in ("condition", "on_time_tasks", "total_tasks", "R300_seconds", "wall_seconds")}), flush=True)
    rows = []
    for job in jobs:
        condition = job["condition"]
        path = out / f"{condition}.json"
        if not path.exists():
            raise RuntimeError(f"Incomplete panel: {condition}; see error files")
        record = json.loads(path.read_text())
        baselines = {j["variant"]: json.loads(Path(j["path"]).read_text())
                     for j in manifest["jobs"] if j["condition"] == condition}
        threshold = (max(baselines["cgs"]["on_time_tasks"], baselines[job["strong_ground"]]["on_time_tasks"]) - 2
                     if condition == "compute_10" else baselines["choice"]["on_time_tasks"])
        rows.append({**job, "on_time_tasks": record["on_time_tasks"], "total_tasks": record["total_tasks"],
                     "P300": record["P300"], "R300_seconds": record["R300_seconds"],
                     "helper_utilization": record["resources"]["helper"]["utilization_full_horizon"],
                     "threshold": threshold, "passed": record["on_time_tasks"] >= threshold and record["unique_ground_audit"]["passed"],
                     "paired": {name: compare(baselines[name], record) for name in ("choice", "cgs", job["strong_ground"])}})
    write_json(out / "summary.json", {"rows": rows, "passed": all(r["passed"] for r in rows)})
    print(json.dumps({"variant": args.variant, "passed": all(r["passed"] for r in rows)}), flush=True)


if __name__ == "__main__":
    main()
