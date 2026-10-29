"""验证现有动作空间的协同机会；固定任务目的地对照不是论文创新算法。"""

import argparse
from collections import Counter
from dataclasses import asdict, replace
import json
import math
from pathlib import Path
from unittest.mock import patch

from audit_policy_behavior import summarize_events
from main import make_policy
from sim.config import load_config
from sim.entities import Action, ActionType, Link, Task, Tile, TileState, TileTimestamps
from sim.env import SimulationEnv
from sim.marl.decision import candidate_neighbors, decision_state
from sim.topology import link_key


class CommittedWindowDiagnostic:
    """当前窗口内保守估时，优先保留可交付的本地任务；目的地按 task 固定。"""

    def __init__(self, horizon_seconds):
        self.horizon_seconds = horizon_seconds
        self.plans = {}

    def finish_estimate(self, state, tile, destination, transfer_seconds):
        sat = state.satellites[destination]
        dt = state.config["dt"]
        service = math.ceil(tile["compute_cost"] / sat["compute_rate"] / dt) * dt
        # 整个任务串行计算的保守估计；源星队列可能已包含本任务，允许高估。
        ready = transfer_seconds + sat["executing_remaining_s"] + (
            sat["queue_len"] + tile["task_progress"]["total"]) * service
        best = math.inf
        for gs_id, window in sat["next_gs_windows"].items():
            if window is None:
                continue
            gs = state.ground_stations[gs_id]
            result_mb = tile["task_progress"]["total"] * state.config["result_size_mb"]
            share = min(1.0, gs["receive_channels"] / max(1, gs["visible_contenders"]))
            downlink = max(dt, math.ceil(result_mb * 8 / gs["bandwidth_mbps"] / share / dt) * dt)
            start, duration = window
            finish = max(ready, start) + downlink
            if finish <= start + duration and state.time * dt + finish <= self.horizon_seconds:
                best = min(best, finish)
        return best

    def select_actions(self, state):
        actions = []
        # 同步决策时先扣除本批拟使用的缓存，实际资源仍由环境检查。
        available = {sid: sat["mem_remaining_gb"] for sid, sat in state.satellites.items()}
        for tid, tile in state.tiles.items():
            source = tile["location"]
            task_id = tile["parent_task_id"]
            if task_id not in self.plans:
                local_finish = self.finish_estimate(state, tile, source, 0)
                target, best = source, local_finish
                if not math.isfinite(local_finish):
                    for neighbor in candidate_neighbors(state, source):
                        if available[neighbor] < state.config["image_size_mb"] / 1024:
                            continue
                        bandwidth = state.links[link_key(source, neighbor)]["bandwidth_mbps"]
                        if bandwidth <= 0:
                            continue
                        transfer = math.ceil(state.config["image_size_mb"] * 8 / bandwidth /
                                             state.config["dt"]) * state.config["dt"]
                        finish = self.finish_estimate(state, tile, neighbor, transfer)
                        if finish < best:
                            target, best = neighbor, finish
                self.plans[task_id] = {
                    "decision_step": state.time, "source": source, "target": target,
                    "local_estimate_seconds": local_finish if math.isfinite(local_finish) else None,
                    "chosen_estimate_seconds": best if math.isfinite(best) else None,
                }
            target = self.plans[task_id]["target"]
            if target == source:
                kind = ActionType.LOCAL if tile["vram_req_gb"] <= state.config["vram_capacity_gb"] else ActionType.WAIT
                actions.append(Action(tid, kind))
            elif target in candidate_neighbors(state, source) and available[target] >= tile["data_size_gb"]:
                actions.append(Action(tid, ActionType.OFFLOAD, target_sat_id=target))
                available[target] -= tile["data_size_gb"]
            else:
                actions.append(Action(tid, ActionType.WAIT))
        return actions


def run_diagnostic(cfg, label, output_dir, env_factory=SimulationEnv, setup=None):
    output_dir.mkdir(parents=True, exist_ok=False)
    cfg = replace(cfg, tile_lifecycle_log=str(output_dir / "events.jsonl"))
    policy = CommittedWindowDiagnostic(cfg.sim_steps * cfg.dt) if label == "committed_window" else make_policy(label, cfg.seed)
    env = env_factory(cfg)
    counts = Counter()
    try:
        if setup:
            setup(env)
        with (output_dir / "actions.jsonl").open("x", encoding="utf-8") as stream:
            for step in range(cfg.sim_steps):
                actions = policy.select_actions(decision_state(env.export_state())) if step % cfg.decision_interval_steps == 0 else []
                for action in actions:
                    kind = action.action_type.value
                    if action.action_type == ActionType.OFFLOAD:
                        kind = "OFFLOAD_GROUND" if action.target_gs_id is not None else "OFFLOAD_ISL"
                    counts[kind] += 1
                    stream.write(json.dumps({"step": step, "kind": kind, **asdict(action)}) + "\n")
                env.step(actions)
        metrics = env.summary()
        completed = sorted(task.task_id for task in env.tasks.values()
                           if task.tile_ids and all(env.tiles[tid].state == TileState.DONE for tid in task.tile_ids))
        if len(completed) != metrics["overall"]["completed_tasks"]:
            raise RuntimeError("Task states disagree with metrics")
        tasks = {key: asdict(value) for key, value in env.tasks.items()}
        tiles = {key: {"task_id": tile.parent_task_id, "state": tile.state.value,
                       "location": tile.location, "timestamps_steps": asdict(tile.timestamps)}
                 for key, tile in env.tiles.items()}
        sources = {key: task.source_sat_id for key, task in env.tasks.items()}
    finally:
        env.close()
    with (output_dir / "events.jsonl").open(encoding="utf-8") as stream:
        behavior = summarize_events((json.loads(line) for line in stream), sources, completed)
    report = {"policy": label, "seed": cfg.seed, "metrics": metrics, "proposed_actions": dict(counts),
              "plans": getattr(policy, "plans", {}), "completed_task_ids": completed,
              "tasks": tasks, "tiles": tiles, **behavior}
    with (output_dir / "summary.json").open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
    return report


class SyntheticTopology:
    def __init__(self, cfg):
        self.cfg = cfg
        self.sat_recs = [None] * cfg.num_sats
        self.t0 = None
        self.links = {"0-1": Link(0, 1, True, cfg.bandwidth_mbps_max, 0)}

    def snapshot(self, t, dt=1):
        return self.links

    def close(self):
        pass


class SyntheticVisibility:
    """源星无地面窗口，邻星在 [10, 20) 秒可见；不替换传输与计算引擎。"""

    def __init__(self, **kwargs):
        self.dt = kwargs["dt"]
        self.lookahead_s = 20
        self.visible_gs_by_sat = {}
        self.best_gs_by_sat = {}

    def refresh(self, t, recs, t0, station_objs, stations):
        self.now = t * self.dt
        self.visible_gs_by_sat = {0: [], 1: ["gs"] if 10 <= self.now < 20 else []}
        self.best_gs_by_sat = {0: None, 1: "gs" if self.visible_gs_by_sat[1] else None}

    def windows_at(self, sat_id, t):
        now = t * self.dt
        return {"gs": (max(0, 10 - now), 20 - max(now, 10)) if sat_id == 1 and now < 20 else None}

    def is_visible(self, sat_id, gs_id, ground_stations):
        return gs_id in self.visible_gs_by_sat.get(sat_id, [])


def synthetic_env(cfg):
    with patch("sim.env.TopologyModel", SyntheticTopology), patch("sim.env.VisibilityCache", SyntheticVisibility):
        return SimulationEnv(cfg)


def inject_synthetic_task(env):
    task = Task("witness", 0, 0, 4, 2, None)
    env.tasks[task.task_id] = task
    env.metrics.total_tasks = 1
    for index in range(2):
        tid = f"witness_tile_{index}"
        tile = Tile(tid, task.task_id, 2, 2, 0.1, TileState.CREATED, 0, TileTimestamps(0))
        task.tile_ids.append(tid)
        env.tiles[tid] = tile
        env.metrics.total_tiles += 1
        env._enqueue_tile(0, tile)
    env._task_tile_index[task.task_id] = task.tile_ids


def synthetic_config(cfg, bandwidth):
    return replace(cfg, seed=0, num_sats=2, sim_steps=20, dt=1, decision_interval_steps=1,
                   topology_update_steps=1, ground_visibility_update_steps=1,
                   task_arrival_rate=0, image_size_mb=4, num_tiles=2, compute_cost_per_tile=2,
                   compute_rate=1, result_size_mb=0.25, mem_capacity_gb=1, vram_capacity_gb=1,
                   pipeline_mode="distributed", deadline_steps=0,
                   ground_stations=[{"id": "gs", "bandwidth_mbps": 8, "receive_channels": 1}],
                   topology={**cfg.topology, "bandwidth_mbps_min": bandwidth, "bandwidth_mbps_max": bandwidth})


def compare_runs(runs):
    local = runs[0]
    pairs = []
    for run in runs[1:]:
        if run["metrics"]["experiment"]["task_trace_sha256"] != local["metrics"]["experiment"]["task_trace_sha256"]:
            raise RuntimeError("Paired arrival traces differ")
        baseline_ids, ids = set(local["completed_task_ids"]), set(run["completed_task_ids"])
        pairs.append({"policy": run["policy"], "gained_vs_local": sorted(ids - baseline_ids),
                      "lost_vs_local": sorted(baseline_ids - ids)})
    return pairs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="examples/config_training.yaml")
    parser.add_argument("--seeds", default="10001,10002,10003")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--synthetic-only", action="store_true")
    args = parser.parse_args()
    cfg = load_config(args.config)
    if cfg.pipeline_mode != "distributed":
        raise ValueError("Requires distributed tile pipeline")
    seeds = [int(x) for x in args.seeds.split(",")]
    if not seeds or len(seeds) != len(set(seeds)):
        raise ValueError("Seeds must be nonempty and unique")
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=False)
    groups = []
    for bandwidth, name in [(8, "synthetic_fast"), (0.8, "synthetic_slow")]:
        runs = [run_diagnostic(synthetic_config(cfg, bandwidth), label, output / name / label,
                               synthetic_env, inject_synthetic_task)
                for label in ("local", "window_aware", "committed_window")]
        groups.append({"name": name, "scope": "synthetic contact schedule, real execution pipeline",
                       "paired": compare_runs(runs), "runs": runs})
        print(name, {r["policy"]: r["metrics"]["overall"]["completed_tasks"] for r in runs}, flush=True)
    if not args.synthetic_only:
        for seed in seeds:
            runs = [run_diagnostic(replace(cfg, seed=seed), label, output / f"seed_{seed}" / label)
                    for label in ("local", "window_aware", "committed_window")]
            groups.append({"name": f"seed_{seed}", "scope": "development validation only",
                           "paired": compare_runs(runs), "runs": runs})
            print(f"seed={seed}", {r["policy"]: r["metrics"]["overall"]["completed_tasks"] for r in runs}, flush=True)
    with (output / "summary.json").open("x", encoding="utf-8") as stream:
        json.dump({"scope": "feasibility diagnosis, not MARL or held-out performance evidence",
                   "information": "committed_window uses existing 1800s forecast in real config, no future arrivals",
                   "groups": groups}, stream, indent=2, allow_nan=False)


if __name__ == "__main__":
    main()
