"""在开发场景审计卸载与计算路径，不将行为相关性解释为协作的因果收益。"""

import argparse
from collections import Counter, defaultdict
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import torch

from main import make_policy
from sim.config import load_config
from sim.entities import ActionType, TileState
from sim.env import SimulationEnv
from sim.marl.decision import decision_state


def summarize_events(events, task_sources, completed_task_ids):
    counts = Counter()
    computed = defaultdict(set)
    isl_tasks, ground_tasks, remote_tasks = set(), set(), set()
    for event in events:
        kind, task = event["event"], event["task_id"]
        counts[kind] += 1
        if kind == "tx_end":
            isl_tasks.add(task)
        if kind == "computed":
            remote = event["sat_to"] != task_sources[task]
            computed["remote_satellite" if remote else "source_satellite"].add(event["tile_id"])
            if remote:
                remote_tasks.add(task)
        if kind == "ground_compute_end":
            computed["ground"].add(event["tile_id"])
            ground_tasks.add(task)
    completed = set(completed_task_ids)
    return {
        "event_counts": dict(sorted(counts.items())),
        "computed_unique_tiles": {k: len(computed[k]) for k in
                                  ("source_satellite", "remote_satellite", "ground")},
        "completed_tasks_with_finished_isl_hop": sorted(completed & isl_tasks),
        "completed_tasks_with_remote_satellite_compute": sorted(completed & remote_tasks),
        "completed_tasks_with_ground_compute": sorted(completed & ground_tasks),
    }


def signature(action):
    return (action.action_type.value, action.target_sat_id, action.target_gs_id)


def audit_run(cfg, policy_name, checkpoint, output_dir):
    output_dir.mkdir(parents=True, exist_ok=False)
    cfg = replace(cfg, tile_lifecycle_log=str(output_dir / "events.jsonl"))
    policy = make_policy(policy_name, cfg.seed, checkpoint)
    local = make_policy("local", cfg.seed)
    env = SimulationEnv(cfg)
    proposed = Counter()
    compared, differing = 0, 0
    try:
        with (output_dir / "actions.jsonl").open("x", encoding="utf-8") as stream:
            for step in range(cfg.sim_steps):
                actions = []
                if step % max(1, cfg.decision_interval_steps) == 0:
                    state = decision_state(env.export_state())
                    actions = policy.select_actions(state)
                    # 在同一状态查询本地策略，避免将不同轨迹上的动作直接逐步比较。
                    reference = {a.tile_id: signature(a) for a in local.select_actions(state)}
                    actual = {a.tile_id: signature(a) for a in actions}
                    compared += len(reference.keys() | actual.keys())
                    differing += sum(reference.get(tid) != actual.get(tid)
                                     for tid in reference.keys() | actual.keys())
                    for action in actions:
                        kind = action.action_type.value
                        if action.action_type == ActionType.OFFLOAD:
                            kind = "OFFLOAD_GROUND" if action.target_gs_id is not None else "OFFLOAD_ISL"
                        proposed[kind] += 1
                        stream.write(json.dumps({"step": step, "tile_id": action.tile_id,
                                                 "kind": kind, "target_sat": action.target_sat_id,
                                                 "target_gs": action.target_gs_id}) + "\n")
                env.step(actions)
        metrics = env.summary()
        sources = {task.task_id: task.source_sat_id for task in env.tasks.values()}
        completed = sorted(task.task_id for task in env.tasks.values()
                           if task.completion_time is not None)
        if len(completed) != metrics["overall"]["completed_tasks"]:
            raise RuntimeError("Final tile states disagree with task completion metrics")
    finally:
        env.close()
    with (output_dir / "events.jsonl").open(encoding="utf-8") as stream:
        behavior = summarize_events((json.loads(line) for line in stream), sources, completed)
    report = {
        "seed": cfg.seed, "policy": policy_name, "checkpoint": checkpoint or None,
        "proposed_actions": dict(sorted(proposed.items())),
        "same_state_local_comparison": {"tile_decisions": compared, "different_decisions": differing},
        "completed_task_ids": completed,
        "isl_sent_mb": sum(v for k, v in metrics["network"]["sent_mb"].items() if k.startswith("isl:")),
        **behavior, "metrics": metrics,
    }
    with (output_dir / "summary.json").open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--seeds", default="10001,10002,10003")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    cfg = load_config(args.config)
    if cfg.pipeline_mode != "distributed":
        raise ValueError("This audit requires the distributed tile pipeline")
    seeds = [int(x) for x in args.seeds.split(",")]
    if not seeds or len(seeds) != len(set(seeds)):
        raise ValueError("Seeds must be nonempty and unique")
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(1)
    runs, paired = [], []
    for seed in seeds:
        baseline = audit_run(replace(cfg, seed=seed), "local", "", output / f"local_{seed}")
        candidate = audit_run(replace(cfg, seed=seed), "marl", args.checkpoint, output / f"marl_{seed}")
        if baseline["metrics"]["experiment"]["task_trace_sha256"] != candidate["metrics"]["experiment"]["task_trace_sha256"]:
            raise RuntimeError("Task traces differ across paired policies")
        runs.extend([baseline, candidate])
        local_ids, candidate_ids = set(baseline["completed_task_ids"]), set(candidate["completed_task_ids"])
        paired.append({"seed": seed, "gained_vs_local": sorted(candidate_ids - local_ids),
                       "lost_vs_local": sorted(local_ids - candidate_ids)})
        print(json.dumps({"seed": seed, "candidate_actions": candidate["proposed_actions"],
                          "candidate_compute": candidate["computed_unique_tiles"],
                          "local_completed": len(local_ids), "candidate_completed": len(candidate_ids)}), flush=True)
    report = {
        "scope": "development behavior audit; not held-out performance evidence",
        "checkpoint_sha256": hashlib.sha256(Path(args.checkpoint).read_bytes()).hexdigest(),
        "audit_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "notes": ["Proposals may repeat or be rejected; event counts measure actual transitions.",
                  "Completed ISL hops do not by themselves demonstrate a causal benefit.",
                  "Event time_seconds is the step start; latency comes from simulator metrics."],
        "paired": paired, "runs": runs,
    }
    with (output / "summary.json").open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)


if __name__ == "__main__":
    main()
