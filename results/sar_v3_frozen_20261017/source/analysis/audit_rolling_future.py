"""只读复现未来输入候选，保存指定整图的尾分片与阶段快照。"""

import argparse
from collections import Counter
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main as simulator
from analysis.audit_short_sla_failures import Observer
from sim.config import load_config
from sim.reproducibility import runtime_metadata
from sim.scheduler.rolling_admission import RollingAdmissionPolicy


class TraceObserver(Observer):
    def __init__(self, task_ids):
        self.task_ids = set(task_ids)
        self.snapshots = []
        self.actions = []

    def on_step(self, env, step_idx, actions):
        super().on_step(env, step_idx, actions)
        for action in actions:
            tile = env.tiles.get(action.tile_id)
            if tile is not None and tile.parent_task_id in self.task_ids:
                self.actions.append({"seconds": step_idx * env.cfg.dt, **asdict(action)})
        if env.time % round(10 / env.cfg.dt):
            return
        for tid in self.task_ids:
            task = env.tasks.get(tid)
            if task is None or task.completion_time is not None:
                continue
            tiles = [env.tiles[t] for t in task.tile_ids]
            self.snapshots.append({"task_id": tid, "seconds": env.time * env.cfg.dt,
                "states": dict(Counter(t.state.value for t in tiles)),
                "locations_by_state": {state: dict(Counter(str(t.location) for t in tiles if t.state.value == state))
                                       for state in {t.state.value for t in tiles}},
                "ground_windows": {str(sid): env.vis_cache.windows_at(sid, env.time)
                                   for sid in {t.location for t in tiles if t.state.value == "COMPUTED"}},
            })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--task-ids", nargs="+", default=["task_10", "task_17"])
    args = parser.parse_args()
    reference = json.loads(Path(args.reference).read_text())
    from sim.config import config_from_dict
    cfg = config_from_dict(reference["metrics"]["experiment"]["config"])
    kwargs = reference["method_spec"]["kwargs"]
    source = runtime_metadata()
    observer = TraceObserver(args.task_ids)
    original = simulator.make_policy
    policy = RollingAdmissionPolicy(**kwargs)
    def factory(name, seed, marl_checkpoint=""):
        return policy if name == reference["method"] else original(name, seed, marl_checkpoint)
    started = time.monotonic()
    simulator.make_policy = factory
    try:
        summary = simulator.run_config(cfg, reference["method"], observer=observer)
    finally:
        simulator.make_policy = original
    same_outcomes = summary["task_outcomes"] == reference["metrics"]["task_outcomes"]
    env = observer.env
    tiles = [{"tile_id": t.tile_id, "task_id": t.parent_task_id, "result_path": t.result_path,
              "state": t.state.value, "location": t.location,
              "timestamps_seconds": {k: None if v is None else v * cfg.dt for k, v in asdict(t.timestamps).items()}}
             for t in env.tiles.values() if t.parent_task_id in observer.task_ids]
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    record = {"scope": "read-only observer on development run; all runtime actions unchanged",
              "reference": args.reference, "method_spec": reference["method_spec"],
              "source_before": source, "source_after": runtime_metadata(),
              "observer_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "reference_task_outcomes_equal": same_outcomes,
              "elapsed_s": time.monotonic() - started,
              "task_outcomes": summary["task_outcomes"], "sla_snapshots": observer.sla_snapshots,
              "snapshots": observer.snapshots, "actions": observer.actions, "tiles": tiles,
              "scheduler_stats": dict(policy.stats)}
    output.write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({"output": str(output), "outcomes_match": same_outcomes, "elapsed_s": record["elapsed_s"],
                      "target_outcomes": [r for r in summary["task_outcomes"] if r["task_id"] in observer.task_ids],
                      "source_equal": source["source_sha256"] == record["source_after"]["source_sha256"]}))


if __name__ == "__main__":
    main()
