"""只读回放开发结果，记录短窗口内相邻整图的计算竞争和实际中继轨迹。"""

import argparse
from collections import Counter
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.audit_short_sla_failures import Observer
from main import run_config
from sim.config import config_from_dict
from sim.reproducibility import runtime_metadata


class WindowObserver(Observer):
    def __init__(self, task_ids, start, end):
        self.task_ids = set(task_ids)
        self.start, self.end = start, end
        self.transitions = []
        self.snapshots = []
        self.actions = []
        self.service = []
        self.previous_tiles = {}
        self.previous_busy = {}

    def on_step(self, env, step_idx, actions):
        super().on_step(env, step_idx, actions)
        seconds = env.time * env.cfg.dt
        current_busy = env.metrics.compute_busy_time
        if self.start < seconds <= self.end:
            self.service.append({"start_seconds": step_idx * env.cfg.dt,
                "busy_seconds": {str(sid): used - self.previous_busy.get(sid, 0)
                                 for sid, used in current_busy.items()
                                 if used > self.previous_busy.get(sid, 0)},
                "ground_visible": [sid for sid in env.satellites
                                   if any(env.vis_cache.is_visible(sid, gid, env.ground_stations)
                                          for gid in env.ground_stations)]})
        self.previous_busy = dict(current_busy)
        if not self.start <= seconds <= self.end:
            return
        for action in actions:
            tile = env.tiles.get(action.tile_id)
            if tile is not None and tile.parent_task_id in self.task_ids:
                self.actions.append({"start_seconds": step_idx * env.cfg.dt, **asdict(action)})
        selected = [env.tiles[tid] for task_id in sorted(self.task_ids)
                    if task_id in env.tasks for tid in env.tasks[task_id].tile_ids]
        transfers = {tr.tile_id: tr for tr in env.transfers}
        downlinks = {tr.tile_id: tr for tr in env.downlink_transfers}
        for tile in selected:
            signature = (tile.state.value, tile.location, tuple(tile.raw_relay_path), tuple(tile.result_path))
            if self.previous_tiles.get(tile.tile_id) == signature:
                continue
            self.previous_tiles[tile.tile_id] = signature
            transfer = transfers.get(tile.tile_id)
            downlink = downlinks.get(tile.tile_id)
            self.transitions.append({"end_seconds": seconds, "tile_id": tile.tile_id,
                "state": tile.state.value, "location": tile.location,
                "input_target": tile.input_target_sat_id, "input_route": tile.input_route,
                "input_traversed": tile.raw_relay_path.copy(), "result_path": tile.result_path.copy(),
                "transfer": asdict(transfer) if transfer else None,
                "downlink": asdict(downlink) if downlink else None})
        if env.time % max(1, round(10 / env.cfg.dt)):
            return
        self.snapshots.append({"end_seconds": seconds,
            "tasks": {task_id: {
                "states": dict(Counter(tile.state.value for tile in selected
                                       if tile.parent_task_id == task_id)),
                "state_locations": dict(Counter(f"{tile.state.value}:{tile.location}" for tile in selected
                                                if tile.parent_task_id == task_id))}
                      for task_id in sorted(self.task_ids)},
            "satellites": {str(sid): {
                "queue_tasks": dict(Counter(env.tiles[tid].parent_task_id for tid in sat.queue)),
                "executing": sat.executing,
                "memory_gb": sat.mem_used_gb}
                           for sid, sat in env.satellites.items()},
            "input_transfers": [asdict(tr) for tr in env.transfers
                                if env.tiles[tr.tile_id].parent_task_id in self.task_ids and not tr.is_result]})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--task-ids", nargs="+", default=["task_3", "task_4", "task_5"])
    parser.add_argument("--start", type=float, default=5600)
    parser.add_argument("--end", type=float, default=6500)
    args = parser.parse_args()
    reference = json.loads(Path(args.reference).read_text())
    cfg = config_from_dict(reference["metrics"]["experiment"]["config"])
    source_before = runtime_metadata()
    if source_before["source_sha256"] != reference["metrics"]["experiment"]["source_sha256"]:
        raise RuntimeError("Current core differs from the frozen reference")
    observer = WindowObserver(args.task_ids, args.start, args.end)
    started = time.monotonic()
    summary = run_config(cfg, reference["method"], observer=observer)
    source_after = runtime_metadata()
    outcomes_equal = summary["task_outcomes"] == reference["metrics"]["task_outcomes"]
    tiles = []
    for tile in observer.env.tiles.values():
        if tile.parent_task_id not in observer.task_ids:
            continue
        tiles.append({"tile_id": tile.tile_id, "task_id": tile.parent_task_id,
            "data_size_mb": tile.data_size_mb, "compute_cost": tile.compute_cost,
            "result_size_mb": observer.env.tile_result_size_mb(tile),
            "input_route": tile.input_route, "input_traversed": tile.raw_relay_path,
            "input_target": tile.input_target_sat_id, "result_path": tile.result_path,
            "timestamps_seconds": {key: None if value is None else value * cfg.dt
                                   for key, value in asdict(tile.timestamps).items()}})
    record = {"scope": "development-only read-only observer; unchanged policy and execution",
        "reference": args.reference, "source_before": source_before, "source_after": source_after,
        "observer_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "reference_task_outcomes_equal": outcomes_equal,
        "elapsed_s": time.monotonic() - started, "metrics": summary,
        "sla_snapshots": observer.sla_snapshots, "snapshots": observer.snapshots,
        "transitions": observer.transitions, "actions": observer.actions,
        "service": observer.service, "tiles": tiles}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"output": str(output), "outcomes_equal": outcomes_equal,
        "source_equal": source_before["source_sha256"] == source_after["source_sha256"],
        "elapsed_s": record["elapsed_s"], "target_outcomes": [row for row in summary["task_outcomes"]
                                                               if row["task_id"] in observer.task_ids]}))
    if not outcomes_equal or source_before["source_sha256"] != source_after["source_sha256"]:
        raise RuntimeError("Replay mismatch; diagnostic must not be used as paired evidence")


if __name__ == "__main__":
    main()
