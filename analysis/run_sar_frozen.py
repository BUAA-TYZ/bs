"""冻结SAR配置的批量入口：直接读取冻结JSON，按全部哈希复用记录，固定P300/R300口径。

只派生时间离散参数（dt、前缀截止和对应步数），物理秒制参数与冻结配置一致。
前缀运行只作估时与数值诊断，不作主场景成绩。
"""

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import platform
import resource
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main as simulator
from sim.config import config_from_dict
from sim.entities import TileState
from sim.env import SimulationEnv
from sim.evaluation import task_age_score
from sim.reproducibility import runtime_metadata

FROZEN = Path("results/sar_loopfree_20261017")
HORIZON_S = 300.0
METHODS = (*simulator.GROUND_MODES, *simulator.POLICY_NAMES)
# 秒制周期必须保持；仅步数随 dt 换算。
PERIODS_S = {"decision_interval_steps": 1.0, "topology_update_steps": 10.0,
             "ground_visibility_update_steps": 10.0}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest(value) -> str:
    return sha256_bytes(json.dumps(value, sort_keys=True, allow_nan=False).encode())


def to_steps(seconds: float, dt: float) -> int:
    count = seconds / dt
    if not math.isfinite(count) or count < 0 or not math.isclose(count, round(count), abs_tol=1e-9):
        raise ValueError(f"Cannot represent {seconds} seconds at dt={dt}")
    return round(count)


def derive_config(base: dict, dt: float, stop_seconds: float | None) -> dict:
    """返回只改时间离散的派生配置；任何非时间字段变化都会报错。"""
    old_dt = base["dt"]
    if base["task_arrival_rate"] != 0 or base["topology"].get("bandwidth_noise", 0) != 0:
        raise ValueError("Requires deterministic scheduled arrivals and bandwidth")
    if base["deadline_steps"] != 0 or base["topology"].get("bandwidth_period", 0) != 0:
        raise ValueError("Frozen SAR protocol has no hard deadline or bandwidth period")
    if base["tile_lifecycle_log"]:
        raise ValueError("Shared lifecycle logs are not allowed in batch runs")
    end_s = base["sim_steps"] * old_dt
    stop = end_s if stop_seconds is None else float(stop_seconds)
    if not 0 < stop <= end_s:
        raise ValueError("stop_seconds must lie inside the frozen horizon")
    cfg = deepcopy(base)
    for name, seconds in PERIODS_S.items():
        if not math.isclose(base[name] * old_dt, seconds):
            raise ValueError(f"Unexpected frozen period {name}")
        cfg[name] = to_steps(seconds, dt)
    cfg["dt"] = dt
    cfg["sim_steps"] = to_steps(stop, dt)
    arrival_end = min(stop, base["task_arrival_end_step"] * old_dt)
    cfg["task_arrival_end_step"] = to_steps(arrival_end, dt)
    cfg["scheduled_task_arrivals"] = []
    for event in base["scheduled_task_arrivals"]:
        release = event["release_step"] * old_dt
        if release >= arrival_end:
            continue
        cfg["scheduled_task_arrivals"].append({**event, "release_step": to_steps(release, dt)})
    for event in cfg["scheduled_task_arrivals"]:
        if stop - event["release_step"] * dt < HORIZON_S:
            raise ValueError("Every retained task needs at least 300 seconds of follow-up")
    allowed = set(PERIODS_S) | {"dt", "sim_steps", "task_arrival_end_step", "scheduled_task_arrivals"}
    if any(cfg[key] != value for key, value in base.items() if key not in allowed):
        raise AssertionError("A non-temporal configuration field changed")
    return cfg


def peak_rss_gb() -> float:
    # macOS 的 ru_maxrss 单位是字节，Linux 是 KiB。
    scale = 1 if sys.platform == "darwin" else 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * scale / 1e9


class Observer:
    """只读记录资源轨迹与分段墙钟；不修改环境。超过 worker 内存预算时抛错中止本次运行。"""

    def __init__(self, max_rss_gb: float = math.inf):
        self.step_ns = 0
        self.last_ns = None
        self.max_rss_gb = max_rss_gb

    def on_start(self, env):
        self.env = env
        n = env.cfg.num_sats
        self.queue_peak = [0] * n
        self.pending_steps = [0] * n
        self.pending_busy = [0.0] * n
        self.busy_before = [0.0] * n
        self.ground_queue_peak = {gid: 0 for gid in env.ground_stations}
        self.ground_pending_steps = {gid: 0 for gid in env.ground_stations}
        self.ground_pending_busy = {gid: 0.0 for gid in env.ground_stations}
        self.ground_busy_before = {gid: 0.0 for gid in env.ground_stations}
        self.any_task_open_steps = 0
        self.last_ns = time.perf_counter_ns()

    def _snapshot(self, env):
        # 步首待处理：本星有已就绪/运行中的计算工作（不含仅存放原始数据或结果）。
        pending = [bool(sat.executing) or any(env.tiles[tid].state == TileState.READY for tid in sat.queue)
                   for sat in env.satellites.values()]
        ground = {gid: bool(env.ground_tile_queues.get(gid)) or gid in env.ground_tile_running
                  or bool(env.ground_compute_queue.get(gid)) or gid in env.ground_task_running
                  for gid in env.ground_stations}
        return pending, ground

    def before_step(self, env):
        self._pending, self._ground_pending = self._snapshot(env)
        busy = env.metrics.compute_busy_time
        self.busy_before = [busy.get(i, 0.0) for i in range(env.cfg.num_sats)]
        self.ground_busy_before = {gid: env.ground_compute_busy_time.get(gid, 0.0) for gid in env.ground_stations}
        self.step_start = time.perf_counter_ns()

    def on_step(self, env, step_idx, actions):
        self.step_ns += time.perf_counter_ns() - self.step_start
        if step_idx % 1000 == 0 and peak_rss_gb() > self.max_rss_gb:
            raise MemoryError(f"Worker peak RSS {peak_rss_gb():.2f} GB exceeded {self.max_rss_gb} GB "
                              f"at simulated {step_idx * env.cfg.dt:.0f} s")
        busy = env.metrics.compute_busy_time
        for i, sat in env.satellites.items():
            self.queue_peak[i] = max(self.queue_peak[i], len(sat.queue))
            if self._pending[i]:
                self.pending_steps[i] += 1
                self.pending_busy[i] += busy.get(i, 0.0) - self.busy_before[i]
        for gid in env.ground_stations:
            queued = len(env.ground_tile_queues.get(gid, [])) + len(env.ground_compute_queue.get(gid, []))
            self.ground_queue_peak[gid] = max(self.ground_queue_peak[gid], queued)
            if self._ground_pending[gid]:
                self.ground_pending_steps[gid] += 1
                self.ground_pending_busy[gid] += env.ground_compute_busy_time.get(gid, 0.0) - self.ground_busy_before[gid]
        if len(env._completed_task_ids) < len(env.tasks):
            self.any_task_open_steps += 1

    def resources(self, env, sources):
        dt = env.cfg.dt
        horizon = env.time * dt
        busy = env.metrics.compute_busy_time
        rows = []
        for i in range(env.cfg.num_sats):
            rows.append({"sat_id": i, "role": "source" if i in sources else "helper",
                         "busy_seconds": busy.get(i, 0.0), "queue_peak": self.queue_peak[i],
                         "pending_seconds": self.pending_steps[i] * dt,
                         "busy_while_pending_seconds": self.pending_busy[i]})

        def role(name):
            group = [r for r in rows if r["role"] == name]
            total_busy = math.fsum(r["busy_seconds"] for r in group)
            pending = math.fsum(r["pending_seconds"] for r in group)
            return {"satellites": len(group), "busy_seconds": total_busy,
                    "utilization_full_horizon": total_busy / (len(group) * horizon) if group else None,
                    "pending_seconds": pending,
                    "utilization_while_pending": math.fsum(r["busy_while_pending_seconds"] for r in group) / pending
                    if pending > 0 else None,
                    "queue_peak_max": max((r["queue_peak"] for r in group), default=0)}

        ground = {gid: {"busy_seconds": env.ground_compute_busy_time.get(gid, 0.0),
                        "utilization_full_horizon": env.ground_compute_busy_time.get(gid, 0.0) / horizon,
                        "pending_seconds": self.ground_pending_steps[gid] * dt,
                        "utilization_while_pending": self.ground_pending_busy[gid] / (self.ground_pending_steps[gid] * dt)
                        if self.ground_pending_steps[gid] else None,
                        "queue_peak_tiles_or_tasks": self.ground_queue_peak[gid]}
                  for gid in env.ground_stations}
        return {"denominators": {"utilization_full_horizon": "busy / (satellites * simulated seconds)",
                                 "utilization_while_pending": "busy during steps that began with READY/RUNNING work "
                                                              "on that node / those steps' seconds"},
                "simulated_seconds": horizon, "any_task_open_seconds": self.any_task_open_steps * dt,
                "source": role("source"), "helper": role("helper"), "ground": ground, "per_satellite": rows}


def invariants(env) -> dict:
    """运行结束时核对工作量、地面字节与时间戳次序；任何一项不成立即视为执行错误。"""
    cfg, dt = env.cfg, env.cfg.dt
    computed = (TileState.COMPUTED, TileState.RESULT_TRANSFERRING, TileState.DOWNLINKING, TileState.DONE)
    expected_sat = expected_ground = 0.0
    landed_mb = results_mb = 0.0
    order_errors, early = [], []
    ground_tasks_landed = set()
    window_errors = []
    relay_errors = []
    relay_hops = {"result": [], "raw": []}
    for tile in env.tiles.values():
        for kind, path in (("result", tile.result_path), ("raw", tile.raw_relay_path)):
            hops = max(0, len(path) - 1)
            relay_hops[kind].append(hops)
            if len(set(path)) != len(path) or hops > cfg.relay_max_hops:
                relay_errors.append({"tile_id": tile.tile_id, "path_kind": kind, "path": path})
        on_board = bool(tile.result_path)
        if tile.state in computed and on_board and cfg.onboard_compute_profile is None:
            expected_sat += tile.compute_cost / env.satellites[tile.result_path[0]].compute_rate
        elif tile.state == TileState.DONE and not on_board:
            expected_ground += tile.compute_cost / env.ground_stations[next(iter(env.ground_stations))].compute_rate
        elif tile.state in (TileState.RUNNING, TileState.GROUND_RUNNING) and tile.remaining_compute is not None:
            rate = (env.satellites[tile.location].compute_rate if tile.state == TileState.RUNNING
                    else env.ground_stations[next(iter(env.ground_stations))].compute_rate)
            done = tile.compute_cost / rate - float(tile.remaining_compute)
            expected_sat += done if tile.state == TileState.RUNNING and cfg.onboard_compute_profile is None else 0.0
            expected_ground += done if tile.state == TileState.GROUND_RUNNING else 0.0
        if on_board and tile.state == TileState.DONE:
            results_mb += env.tile_result_size_mb(tile)
        if not on_board and tile.state in (TileState.GROUND_QUEUED, TileState.GROUND_RUNNING, TileState.DONE):
            if env.pipeline_mode == "ground_compute":
                ground_tasks_landed.add(tile.parent_task_id)
            else:
                landed_mb += tile.data_size_mb
        ts = tile.timestamps
        chain = [("created", ts.created), ("start_compute", ts.start_compute),
                 ("end_compute", ts.end_compute), ("end_downlink", ts.end_downlink)]
        present = [(name, value) for name, value in chain if value is not None]
        if any(a[1] > b[1] for a, b in zip(present, present[1:])):
            order_errors.append(tile.tile_id)
        if (not on_board and tile.state in (TileState.GROUND_RUNNING, TileState.DONE)
                and ts.start_compute is not None and (ts.end_tx is None or ts.start_compute < ts.end_tx)):
            order_errors.append(tile.tile_id)
    if cfg.onboard_compute_profile is not None:
        # 并发组共享执行时间，包含取消前的已消耗服务，不能逐片累加串行工时。
        expected_sat = math.fsum(record["service_s"] - record["remaining_s"]
                                 for record in env.compute_cohort_records.values())
        completed_windows = Counter(tid for record in env.compute_cohort_records.values()
                                    for tid in record.get("completed_window_tile_ids", []))
        for tile in env.tiles.values():
            if completed_windows[tile.tile_id] != tile.completed_compute_windows:
                window_errors.append(tile.tile_id)
            if tile.result_path and tile.completed_compute_windows != tile.compute_cost:
                window_errors.append(tile.tile_id)
    landed_mb += sum(env.tasks[tid].image_size_mb for tid in ground_tasks_landed)
    for comp in env.ground_task_running.values():
        # 整图地面模式的运行进度记在任务对象上，而不是分片上。
        task = env.tasks[comp.task_id]
        total = sum(env.tiles[tid].compute_cost for tid in task.tile_ids if env.tiles[tid].state != TileState.DONE)
        expected_ground += total / env.ground_stations[comp.gs_id].compute_rate - comp.remaining_compute
    inflight = (sum(env.tiles[tr.tile_id].data_size_mb - tr.remaining_mb for tr in env.ground_tile_transfers)
                + sum(env.tile_result_size_mb(env.tiles[tr.tile_id]) - tr.remaining_mb for tr in env.downlink_transfers)
                + sum(env.tasks[tr.task_id].image_size_mb - tr.remaining_mb for tr in env.ground_task_transfers))
    sent_ground = math.fsum(v for k, v in env.metrics.network_sent_mb.items() if k.startswith("ground:"))
    busy_sat = math.fsum(env.metrics.compute_busy_time.values())
    busy_ground = math.fsum(env.ground_compute_busy_time.values())
    post = env._postprocessing_steps()
    for task in env.tasks.values():
        if task.completion_time is None:
            continue
        tiles = [env.tiles[tid] for tid in task.tile_ids]
        last = max((t.timestamps.end_downlink or t.timestamps.end_compute or 0) for t in tiles)
        task_post = (math.ceil((cfg.ground_relay_delay_s + task.aggregation_delay_s) / dt)
                     if task.aggregation_delay_s is not None else post)
        if not all(t.state == TileState.DONE for t in tiles) or task.completion_time < last + task_post:
            early.append(task.task_id)
    checks = {"satellite_work_seconds": {"busy": busy_sat, "expected": expected_sat},
              "ground_work_seconds": {"busy": busy_ground, "expected": expected_ground},
              "ground_bytes_mb": {"sent": sent_ground, "landed_raw": landed_mb, "results": results_mb,
                                  "inflight_sent": inflight},
              "timestamp_order_errors": order_errors[:20], "early_completion_tasks": early,
              "compute_window_errors": window_errors[:20], "relay_path_errors": relay_errors[:20],
              "relay_hops": {kind: {"max": max(hops, default=0), "paths_over_eight": sum(h > 8 for h in hops)}
                             for kind, hops in relay_hops.items()}}
    tol = 1e-6 * max(1.0, busy_sat + busy_ground + sent_ground)
    checks["passed"] = (abs(busy_sat - expected_sat) <= tol and abs(busy_ground - expected_ground) <= tol
                        and abs(sent_ground - landed_mb - results_mb - inflight) <= tol
                        and not order_errors and not early and not window_errors and not relay_errors)
    checks["step_seconds"] = dt
    checks["compute_service_mode"] = cfg.compute_service_mode
    return checks


def run_one(cfg_dict: dict, method: str, source_sha: str, script_sha: str, max_rss_gb: float = math.inf) -> dict:
    import torch
    torch.set_num_threads(1)
    cfg = simulator.apply_execution_mode(config_from_dict(cfg_dict), method)
    if runtime_metadata()["source_sha256"] != source_sha:
        raise RuntimeError("Core changed before run")
    timing = {"export_state_ns": 0, "decision_state_ns": 0, "select_actions_ns": 0, "calls": 0, "active_calls": 0}
    active = []
    observer = Observer(max_rss_gb)
    original_export, original_decision = SimulationEnv.export_state, simulator.decision_state
    original_factory, original_step = simulator.make_policy, SimulationEnv.step

    def export_state(env):
        start = time.perf_counter_ns()
        try:
            return original_export(env)
        finally:
            timing["export_state_ns"] += time.perf_counter_ns() - start

    def decision_state(state, limit):
        start = time.perf_counter_ns()
        try:
            return original_decision(state, limit)
        finally:
            timing["decision_state_ns"] += time.perf_counter_ns() - start

    def factory(*args, **kwargs):
        policy = original_factory(*args, **kwargs)
        select = policy.select_actions

        def measured(state):
            start = time.perf_counter_ns()
            actions = select(state)
            elapsed = time.perf_counter_ns() - start
            timing["select_actions_ns"] += elapsed
            timing["calls"] += 1
            if state.tiles:
                timing["active_calls"] += 1
                active.append(elapsed / 1e6)
            return actions

        policy.select_actions = measured
        return policy

    def step(env, actions):
        observer.before_step(env)
        return original_step(env, actions)

    SimulationEnv.export_state, simulator.decision_state = export_state, decision_state
    simulator.make_policy, SimulationEnv.step = factory, step
    start = time.monotonic()
    try:
        metrics = simulator.run_config(cfg, method, observer=observer)
    finally:
        SimulationEnv.export_state, simulator.decision_state = original_export, original_decision
        simulator.make_policy, SimulationEnv.step = original_factory, original_step
    wall = time.monotonic() - start
    checks = invariants(observer.env)
    if not checks["passed"]:
        raise RuntimeError(f"Execution invariant failed for {method}: {json.dumps(checks)[:2000]}")
    after = runtime_metadata()["source_sha256"]
    if after != source_sha or metrics["experiment"]["source_sha256"] != source_sha:
        raise RuntimeError("Core changed during run")
    if sha256_bytes(Path(__file__).read_bytes()) != script_sha:
        raise RuntimeError("Runner changed during run")
    observation_end = cfg.sim_steps * cfg.dt
    outcomes = metrics["task_outcomes"]
    score = task_age_score(outcomes, [HORIZON_S], observation_end)
    on_time = sorted(row["task_id"] for row in outcomes
                     if row["status"] == "completed" and row["latency_seconds"] <= HORIZON_S)
    env_for_resources = observer.env
    active.sort()
    pick = lambda q: active[min(len(active) - 1, math.ceil(q * len(active)) - 1)] if active else None
    return {"method": method, "dt": cfg.dt, "observation_end_seconds": observation_end,
            "P300": score["delivery_curve"][0]["delivery_rate"],
            "on_time_tasks": len(on_time), "total_tasks": len(outcomes),
            "R300_seconds": score["restricted_mean_latency_seconds"], "on_time_task_ids": on_time,
            "task_age": score, "wall_seconds": wall,
            "timing": {"scope": "host CPU wall time inside this worker; not spacecraft latency",
                       "export_state_s": timing["export_state_ns"] / 1e9,
                       "decision_state_s": timing["decision_state_ns"] / 1e9,
                       "select_actions_s": timing["select_actions_ns"] / 1e9,
                       "env_step_s": observer.step_ns / 1e9,
                       "decision_calls": timing["calls"], "active_decision_calls": timing["active_calls"],
                       "active_select_median_ms": pick(.5), "active_select_p95_ms": pick(.95),
                       "active_select_max_ms": active[-1] if active else None},
            "resources": observer.resources(env_for_resources, set(cfg.task_source_sat_ids or [])),
            "invariants": checks,
            "worker_peak_rss_gb": peak_rss_gb(),
            "failure_counts": dict(Counter(row["failure_reason"] for row in outcomes if row["failure_reason"])),
            "metrics": metrics}


def paired_on_time(reference: dict, candidate: dict) -> dict:
    ref, cand = set(reference["on_time_task_ids"]), set(candidate["on_time_task_ids"])
    ref_ids = [row["task_id"] for row in reference["metrics"]["task_outcomes"]]
    cand_ids = [row["task_id"] for row in candidate["metrics"]["task_outcomes"]]
    if ref_ids != cand_ids:
        raise ValueError("Paired comparison requires identical task lists")
    return {"reference": reference["method"], "candidate": candidate["method"],
            "gained_on_time": sorted(cand - ref), "lost_on_time": sorted(ref - cand),
            "P300_difference": candidate["P300"] - reference["P300"],
            "R300_difference_seconds": candidate["R300_seconds"] - reference["R300_seconds"]}


def record_key(cfg_sha: str, method: str, source_sha: str, script_sha: str, frozen_sha: str) -> str:
    return digest({"config": cfg_sha, "method": method, "source": source_sha, "runner": script_sha,
                   "frozen_config": frozen_sha})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=FROZEN / "main_config.json")
    parser.add_argument("--manifest", type=Path, default=FROZEN / "manifest.json")
    parser.add_argument("--methods", nargs="+", choices=METHODS, required=True)
    parser.add_argument("--dt", type=float, default=None, help="default: frozen dt")
    parser.add_argument("--stop-seconds", type=float, default=None, help="prefix end; default: full frozen horizon")
    parser.add_argument("--reference", default=None, help="method used for gained/lost on-time task IDs")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--max-total-rss-gb", type=float, default=24.0,
                        help="summed worker memory budget; split evenly, each worker aborts itself on its share")
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.workers not in (1, 2) or len(set(args.methods)) != len(args.methods):
        raise ValueError("Use one or two workers and unique methods")
    if not 0 < args.max_total_rss_gb <= 24:
        raise ValueError("Total worker memory budget must be positive and at most 24 GB")
    if args.reference is not None and args.reference not in args.methods:
        raise ValueError("Reference must be one of the evaluated methods")
    if "marl" in args.methods:
        raise ValueError("MARL checkpoints are not part of the frozen SAR baseline panel")

    frozen_bytes = args.config.read_bytes()
    frozen_sha = sha256_bytes(frozen_bytes)
    manifest = json.loads(args.manifest.read_bytes())
    if manifest["config_sha256"].get(args.config.name) != frozen_sha:
        raise ValueError("Config hash does not match the frozen manifest")
    base = json.loads(frozen_bytes)
    derived = derive_config(base, args.dt if args.dt is not None else base["dt"], args.stop_seconds)
    cfg_obj = config_from_dict(derived)
    cfg_dict = asdict(cfg_obj)
    cfg_sha = digest(cfg_dict)
    runtime = runtime_metadata()
    script_sha = sha256_bytes(Path(__file__).read_bytes())
    tle_sha = digest(cfg_dict["topology"]["tle_lines"])
    out = args.out_dir
    out.mkdir(parents=True, exist_ok=True)
    run_info = {"scope": ("full frozen development day" if args.stop_seconds is None else
                          "numerical/timing prefix only; not a main-scenario result"),
                "frozen_config": str(args.config), "frozen_config_sha256": frozen_sha,
                "frozen_manifest_sha256": sha256_bytes(args.manifest.read_bytes()),
                "calibration_inputs": manifest["inputs"], "derived_config_sha256": cfg_sha,
                "dt": cfg_obj.dt, "stop_seconds": cfg_obj.sim_steps * cfg_obj.dt,
                "tasks": len(cfg_obj.scheduled_task_arrivals), "tle_sha256": tle_sha,
                "runner_sha256": script_sha, **runtime, "platform": platform.platform(),
                "cpu_count": os.cpu_count(), "argv": sys.argv, "metric": {
                    "P300": "tasks fully aggregated on the ground within 300 s of readiness / all arrived tasks",
                    "R300": "mean of min(latency, 300) with undelivered tasks counted as 300 s"}}
    (out / "derived_config.json").write_text(json.dumps(cfg_dict, indent=2, allow_nan=False))
    (out / "run.json").write_text(json.dumps(run_info, indent=2, ensure_ascii=False, allow_nan=False))

    records, todo = {}, []
    for method in args.methods:
        key = record_key(cfg_sha, method, runtime["source_sha256"], script_sha, frozen_sha)
        path = out / f"{method}.json"
        if path.exists():
            old = json.loads(path.read_bytes())
            if old.get("record_key") == key:
                records[method] = old
                print(json.dumps({"reused": method}), flush=True)
                continue
            raise ValueError(f"{path} exists with a different hash context; use a new out-dir")
        todo.append((method, key))
    started = time.monotonic()
    worker_limit = args.max_total_rss_gb / args.workers
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        jobs = {pool.submit(run_one, cfg_dict, method, runtime["source_sha256"], script_sha, worker_limit):
                (method, key) for method, key in todo}
        for job in as_completed(jobs):
            method, key = jobs[job]
            try:
                record = job.result()
            except MemoryError:
                for other in jobs:
                    other.cancel()
                raise
            experiment = record["metrics"]["experiment"]
            if experiment["tle_sha256"] != hashlib.sha256(json.dumps(cfg_obj.topology["tle_lines"]).encode()).hexdigest():
                raise ValueError("TLE changed inside worker")
            record.update(record_key=key, derived_config_sha256=cfg_sha, frozen_config_sha256=frozen_sha,
                          runner_sha256=script_sha, task_trace_sha256=experiment["task_trace_sha256"],
                          worker_rss_limit_gb=worker_limit)
            tmp = out / f".{method}.json.tmp"
            tmp.write_text(json.dumps(record, indent=2, allow_nan=False))
            tmp.replace(out / f"{method}.json")
            records[method] = record
            print(json.dumps({"method": method, "P300": record["P300"], "on_time": record["on_time_tasks"],
                              "tasks": record["total_tasks"], "R300": record["R300_seconds"],
                              "wall_s": round(record["wall_seconds"], 1),
                              "worker_peak_rss_gb": round(record["worker_peak_rss_gb"], 2)}), flush=True)
    rows = [{key: records[m][key] for key in ("method", "P300", "on_time_tasks", "total_tasks", "R300_seconds",
                                              "wall_seconds", "timing", "failure_counts")}
            | {"source_utilization": records[m]["resources"]["source"],
               "helper_utilization": records[m]["resources"]["helper"],
               "ground": records[m]["resources"]["ground"],
               "network_sent_mb": records[m]["metrics"]["network"]["sent_mb"]}
            for m in args.methods]
    pairs = ([paired_on_time(records[args.reference], records[m]) for m in args.methods if m != args.reference]
             if args.reference else [])
    summary = {**run_info, "elapsed_s": time.monotonic() - started, "rows": rows, "paired_on_time": pairs}
    (out / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False, allow_nan=False))
    print(json.dumps([{k: r[k] for k in ("method", "P300", "on_time_tasks", "R300_seconds", "wall_seconds")}
                      for r in rows], indent=1))


if __name__ == "__main__":
    main()
