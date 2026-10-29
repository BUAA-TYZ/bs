"""固定物理周期和任务前缀，诊断两跳窗口边界对仿真时间步的敏感性。"""

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import asdict
import gzip
import hashlib
import json
import math
import multiprocessing
from pathlib import Path
import sys
import time

import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import run_config
from sim.config import config_from_dict
from sim.evaluation import paired_task_metrics, task_age_score
from sim.reproducibility import runtime_metadata


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def steps(seconds, dt):
    count = seconds / dt
    if not math.isfinite(count) or count < 0 or not math.isclose(count, round(count), abs_tol=1e-9):
        raise ValueError(f"Cannot represent {seconds} seconds at dt={dt}")
    return round(count)


def prefix_config(base, dt, stop_seconds):
    cfg = deepcopy(base)
    old_dt = base["dt"]
    if not math.isfinite(dt) or dt <= 0 or base["task_arrival_rate"] != 0:
        raise ValueError("Requires positive dt and scheduled deterministic arrivals")
    if base["topology"].get("bandwidth_noise", 0) != 0:
        raise ValueError("Numerical comparison requires deterministic bandwidth")
    if base["tile_lifecycle_log"]:
        raise ValueError("Use read-only observer evidence instead of a shared lifecycle log")
    periods = {"decision_interval_steps": 1, "topology_update_steps": 10,
               "ground_visibility_update_steps": 10}
    for name, seconds in periods.items():
        if not math.isclose(base[name] * old_dt, seconds):
            raise ValueError(f"Unexpected base period for {name}")
        cfg[name] = steps(seconds, dt)
    cfg["dt"] = dt
    cfg["sim_steps"] = steps(stop_seconds, dt)
    cfg["deadline_steps"] = steps(base["deadline_steps"] * old_dt, dt)
    if base["task_arrival_end_step"] is not None:
        cfg["task_arrival_end_step"] = steps(min(stop_seconds, base["task_arrival_end_step"] * old_dt), dt)
    period = base["topology"].get("bandwidth_period", 0)
    cfg["topology"]["bandwidth_period"] = steps(period * old_dt, dt) if period > 0 else period
    cfg["scheduled_task_arrivals"] = []
    for event in base["scheduled_task_arrivals"]:
        release = event["release_step"] * old_dt
        if release > stop_seconds:
            continue
        if release == stop_seconds:
            raise ValueError("An arrival exactly at the observation endpoint needs another simulation step")
        cfg["scheduled_task_arrivals"].append({**event, "release_step": steps(release, dt)})
    # 保留所有秒制参数，只有步数制参数和数值积分步长允许改变。
    allowed = set(periods) | {"dt", "sim_steps", "deadline_steps", "task_arrival_end_step",
                              "scheduled_task_arrivals", "topology"}
    if any(cfg[key] != value for key, value in base.items() if key not in allowed):
        raise AssertionError("A non-temporal configuration changed")
    if {k: v for k, v in cfg["topology"].items() if k != "bandwidth_period"} != {
            k: v for k, v in base["topology"].items() if k != "bandwidth_period"}:
        raise AssertionError("A physical topology parameter changed")
    return config_from_dict(cfg)


class Observer:
    def on_start(self, env):
        self.env = env
        self.context = []
        self.snapshots = []
        self.watched_steps = {steps(second, env.cfg.dt) for second in (5830, 6040)}
        self.watched_steps |= {event["release_step"] + steps(300, env.cfg.dt)
                               for event in env.cfg.scheduled_task_arrivals}

    def on_step(self, env, step_idx, actions):
        if step_idx % env.cfg.topology_update_steps == 0:
            self.context.append({"seconds": step_idx * env.cfg.dt,
                "links": {key: [link.up, link.bandwidth_mbps] for key, link in sorted(env._links_cache.items())},
                "visible_ground_stations": {str(sid): sorted(env.vis_cache.visible_gs_by_sat.get(sid, []))
                                            for sid in env.satellites}})
        if env.time in self.watched_steps:
            rows = []
            for task in env.tasks.values():
                tiles = [env.tiles[tid] for tid in task.tile_ids]
                rows.append({"task_id": task.task_id,
                             "states": dict(Counter(tile.state.value for tile in tiles)),
                             "waiting_result_locations": dict(Counter(str(tile.location) for tile in tiles
                                 if tile.state.value in ("COMPUTED", "RESULT_TRANSFERRING", "DOWNLINKING")))})
            self.snapshots.append({"seconds": env.time * env.cfg.dt, "tasks": rows})

    def evidence(self):
        env = self.env
        physical_tasks, tile_trace, tails = [], [], []
        for task in env.tasks.values():
            tiles = [env.tiles[tid] for tid in task.tile_ids]
            definition = [{"data_size_mb": tile.data_size_mb, "compute_cost": tile.compute_cost,
                           "result_size_mb": tile.result_size_mb, "windows_xyxy": tile.windows_xyxy}
                          for tile in tiles]
            physical_tasks.append({"task_id": task.task_id, "source_sat_id": task.source_sat_id,
                                   "release_seconds": task.release_time * env.cfg.dt,
                                   "image_size_mb": task.image_size_mb, "tile_count": len(tiles),
                                   "tile_definition_sha256": digest(definition)})
            last_compute = last_delivery = None
            for tile, tile_definition in zip(tiles, definition):
                timestamps = {key: None if value is None else value * env.cfg.dt
                              for key, value in asdict(tile.timestamps).items()}
                tile_trace.append({"task_id": task.task_id, "tile_id": tile.tile_id,
                                   "state": tile.state.value, "timestamps_seconds": timestamps,
                                   "input_route": tile.input_route, "result_path": tile.result_path,
                                   "raw_relay_path": tile.raw_relay_path, **tile_definition})
                if timestamps["end_compute"] is not None:
                    last_compute = max(last_compute or 0, timestamps["end_compute"])
                if timestamps["end_downlink"] is not None:
                    last_delivery = max(last_delivery or 0, timestamps["end_downlink"])
            tails.append({"task_id": task.task_id, "last_compute_seconds": last_compute,
                          "last_delivery_seconds": last_delivery,
                          "compute_distribution": dict(Counter(str(tile.result_path[0])
                              for tile in tiles if tile.result_path))})
        return {"physical_tasks": physical_tasks, "tile_trace": tile_trace, "tails": tails,
                "snapshots": self.snapshots, "geometry_context": self.context}


def run_diagnostic(cfg_dict, out_dir, source_sha, script_sha):
    torch.set_num_threads(1)
    cfg = config_from_dict(cfg_dict)
    before = runtime_metadata()
    if before["source_sha256"] != source_sha:
        raise RuntimeError("Core changed before diagnostic")
    observer = Observer()
    start = time.monotonic()
    metrics = run_config(cfg, "cgs_compute_adapted", observer=observer)
    after = runtime_metadata()
    if after["source_sha256"] != source_sha or metrics["experiment"]["source_sha256"] != source_sha:
        raise RuntimeError("Core changed during diagnostic")
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != script_sha:
        raise RuntimeError("Diagnostic script changed during execution")
    evidence = observer.evidence()
    label = str(cfg.dt).replace(".", "p")
    out = Path(out_dir)
    evidence_path = out / f"dt_{label}_evidence.json.gz"
    with gzip.open(evidence_path, "wt", encoding="utf-8") as handle:
        json.dump(evidence, handle, ensure_ascii=False, allow_nan=False)
    record = {"scope": "numerical prefix diagnostic only; not a new scenario or a statistical test",
              "method": "cgs_compute_adapted", "dt": cfg.dt, "wall_seconds": time.monotonic() - start,
              "source_before": before, "source_after": after, "script_sha256": script_sha,
              "physical_task_trace": evidence["physical_tasks"],
              "physical_task_trace_sha256": digest(evidence["physical_tasks"]),
              "geometry_context_sha256": digest(evidence["geometry_context"]),
              "evidence_file": evidence_path.name,
              "evidence_sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
              "task_tails": evidence["tails"], "metrics": metrics,
              "task_age": task_age_score(metrics["task_outcomes"], [300], cfg.sim_steps * cfg.dt)}
    save(out / f"dt_{label}.json", record)
    return record


def prefix_comparison(reference, candidate, stop_seconds):
    expected = [row for row in reference["metrics"]["task_outcomes"] if row["release_seconds"] <= stop_seconds]
    actual = candidate["metrics"]["task_outcomes"]
    clean = lambda row: {key: value for key, value in row.items() if key != "observed_age_seconds"}
    differences = []
    for index in range(max(len(expected), len(actual))):
        old = clean(expected[index]) if index < len(expected) else None
        new = clean(actual[index]) if index < len(actual) else None
        if old != new:
            differences.append({"index": index, "full_run": old, "prefix_run": new})
    return {"matched": not differences, "expected_count": len(expected), "actual_count": len(actual),
            "ignored_fields": ["observed_age_seconds"], "differences": differences}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--stop-seconds", type=float, default=6600)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if args.workers not in (1, 2):
        raise ValueError("This diagnostic uses at most two workers")
    path = Path(args.reference)
    reference = json.loads(path.read_bytes())
    if reference["method"] != "cgs_compute_adapted":
        raise ValueError("Requires a CGS reference")
    base = reference["metrics"]["experiment"]["config"]
    runtime = runtime_metadata()
    if runtime["source_sha256"] != reference["metrics"]["experiment"]["source_sha256"]:
        raise ValueError("Reference must use the currently frozen core")
    script_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    configs = {dt: prefix_config(base, dt, args.stop_seconds) for dt in (1.0, 0.25, 0.1)}
    manifest = {"scope": "numerical timestep convergence diagnostic, not a performance panel",
                "reference": str(path), "reference_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "original_case": reference["case"], "args": vars(args), "source_before": runtime,
                "script_sha256": script_sha, "dts": list(configs),
                "physical_periods_seconds": {"decision": 1, "geometry": 10, "ground_visibility": 10,
                    "contact_scan": base["topology"]["contact_scan_s"],
                    "contact_update": base["topology"]["contact_update_s"]},
                "arrival_filter": "All original arrivals through stop_seconds retained; no arrivals inside the prefix are removed",
                "denominator": "All tasks born inside this numerical prefix, including late or pending tasks"}
    save(out / "run.json", manifest)
    for dt, cfg in configs.items():
        save(out / f"dt_{str(dt).replace('.', 'p')}_config.json", asdict(cfg))
    prefix = run_diagnostic(asdict(configs[1.0]), str(out), runtime["source_sha256"], script_sha)
    check = prefix_comparison(reference, prefix, args.stop_seconds)
    save(out / "prefix_validation.json", check)
    print(json.dumps({"dt": 1, "prefix_validation": check,
                      "task_age": prefix["task_age"], "wall_seconds": prefix["wall_seconds"]}), flush=True)
    if not check["matched"]:
        raise RuntimeError("Truncation changed outcomes; finer timestep runs were not started")
    records = [prefix]
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        jobs = [pool.submit(run_diagnostic, asdict(configs[dt]), str(out), runtime["source_sha256"], script_sha)
                for dt in (0.25, 0.1)]
        for job in as_completed(jobs):
            record = job.result()
            if (record["physical_task_trace_sha256"] != prefix["physical_task_trace_sha256"]
                    or record["geometry_context_sha256"] != prefix["geometry_context_sha256"]):
                raise RuntimeError("Physical task or geometry trace changed across timesteps")
            records.append(record)
            print(json.dumps({"dt": record["dt"], "task_age": record["task_age"],
                              "wall_seconds": record["wall_seconds"],
                              "task5": record["metrics"]["task_outcomes"][5]}), flush=True)
    source_after = runtime_metadata()
    if source_after["source_sha256"] != runtime["source_sha256"]:
        raise RuntimeError("Core changed before comparison finished")
    rows = []
    for record in sorted(records, key=lambda row: row["dt"], reverse=True):
        rows.append({"dt": record["dt"], "wall_seconds": record["wall_seconds"],
                     "task_age": record["task_age"], "task_outcomes": record["metrics"]["task_outcomes"],
                     "task_tails": record["task_tails"],
                     "paired_to_dt1": paired_task_metrics(prefix["metrics"]["task_outcomes"],
                                                          record["metrics"]["task_outcomes"])})
    save(out / "summary.json", {**manifest, "source_after": source_after,
                                 "prefix_validation": check, "results": rows})


if __name__ == "__main__":
    main()
