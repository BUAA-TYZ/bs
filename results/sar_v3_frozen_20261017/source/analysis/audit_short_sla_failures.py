"""只读回放开发日，拆解整图尾分片的计算与回传时延。"""

import argparse
from collections import Counter
from dataclasses import asdict
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import run_config
from sim.config import load_config
from sim.reproducibility import runtime_metadata


class Observer:
    def on_start(self, env):
        self.env = env
        self.sla_snapshots = {}

    def on_step(self, env, step_idx, actions):
        for task in env.tasks.values():
            if env.time != task.release_time + round(300 / env.cfg.dt):
                continue
            tiles = [env.tiles[tid] for tid in task.tile_ids]
            self.sla_snapshots[task.task_id] = {
                "states": dict(Counter(tile.state.value for tile in tiles)),
                "computed_waiting_locations": dict(Counter(
                    str(tile.location) for tile in tiles if tile.state.value in
                    ("COMPUTED", "RESULT_TRANSFERRING", "DOWNLINKING"))),
                "max_hops_waiting": max((len(tile.result_path) - 1 for tile in tiles
                                         if tile.state.value == "COMPUTED"), default=0),
            }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--policy", default="heft_adapted")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    cfg = load_config(args.config)
    observer = Observer()
    source = runtime_metadata()
    start = time.monotonic()
    summary = run_config(cfg, args.policy, observer=observer)
    env = observer.env
    tasks = []
    for outcome in summary["task_outcomes"]:
        task = env.tasks[outcome["task_id"]]
        tiles = [env.tiles[tid] for tid in task.tile_ids]
        def end(tile, field):
            value = getattr(tile.timestamps, field)
            return -1 if value is None else value * cfg.dt
        by_delivery = sorted(tiles, key=lambda tile: end(tile, "end_downlink"))
        by_compute = sorted(tiles, key=lambda tile: end(tile, "end_compute"))
        def describe(tile):
            return {"tile_id": tile.tile_id, "compute_location": tile.result_path[0] if tile.result_path else None,
                    "result_path": tile.result_path,
                    "timestamps_seconds": {key: None if value is None else value * cfg.dt
                                           for key, value in asdict(tile.timestamps).items()}}
        tasks.append({**outcome,
            "sla_snapshot": observer.sla_snapshots.get(task.task_id),
            "compute_distribution": dict(Counter(str(t.result_path[0]) for t in tiles if t.result_path)),
            "last_compute_seconds": end(by_compute[-1], "end_compute"),
            "first_delivery_seconds": end(by_delivery[0], "end_downlink"),
            "p50_delivery_seconds": end(by_delivery[len(tiles) // 2], "end_downlink"),
            "p95_delivery_seconds": end(by_delivery[int(len(tiles) * .95)], "end_downlink"),
            "last_compute_tiles": [describe(t) for t in by_compute[-5:]],
            "last_delivery_tiles": [describe(t) for t in by_delivery[-5:]],
        })
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    record = {"scope": "development diagnostic; exact original config and policy; observer is read-only",
              "config": args.config, "policy": args.policy, "source_before": source,
              "source_after": runtime_metadata(), "elapsed_s": time.monotonic() - start,
              "task_outcomes": tasks}
    output.write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({"output": str(output), "elapsed_s": record["elapsed_s"],
                      "tasks": [{k: row[k] for k in ("task_id", "latency_seconds", "last_compute_seconds",
                                                     "p95_delivery_seconds", "sla_snapshot")}
                                for row in tasks]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
