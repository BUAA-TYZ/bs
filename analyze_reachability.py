"""离线乐观可达性诊断：忽略容量、计算/传播时延和资源限制，只检查未来拓扑。"""

import argparse
from dataclasses import replace
import json
from pathlib import Path

import numpy as np

from sim.config import load_config
from sim.env import SimulationEnv


def optimistic_reachability(edges, ground_visible, num_sats):
    """允许同一步无限快多跳与计算；仅作上界，不是可执行调度器。"""
    reachable = np.zeros((len(edges) + 1, num_sats), dtype=bool)
    for t in reversed(range(len(edges))):
        reachable[t] = reachable[t + 1]
        reachable[t, list(ground_visible[t])] = True
        adjacency = [set() for _ in range(num_sats)]
        for i, j in edges[t]:
            adjacency[i].add(j)
            adjacency[j].add(i)
        stack = list(np.flatnonzero(reachable[t]))
        while stack:
            node = stack.pop()
            for neighbor in adjacency[node]:
                if not reachable[t, neighbor]:
                    reachable[t, neighbor] = True
                    stack.append(neighbor)
    return reachable


def analyze(cfg, seeds):
    runs = []
    for seed in seeds:
        env = SimulationEnv(replace(cfg, seed=seed, tile_lifecycle_log=""))
        edges, visible = [], []
        try:
            for _ in range(cfg.sim_steps):
                state = env.export_state()
                edges.append([(sid, nb) for sid, neighbors in state.neighbors.items() for nb in neighbors if sid < nb])
                visible.append({sid for sid, options in state.ground_options.items() if options})
                env.step([])
            reachable = optimistic_reachability(edges, visible, cfg.num_sats)
            count = sum(bool(reachable[task.release_time, task.source_sat_id]) for task in env.tasks.values())
            runs.append({"seed": seed, "total_tasks": len(env.tasks), "optimistically_reachable_tasks": count,
                         "completion_rate_upper_bound": count / len(env.tasks) if env.tasks else 0.0,
                         "ground_visible_seconds_per_sat": [sum(sid in step for step in visible) * cfg.dt
                                                            for sid in range(cfg.num_sats)],
                         "experiment": env.summary()["experiment"]})
        finally:
            env.close()
    return {"scope": "offline optimistic upper bound only; zero transfer/compute delay and unlimited capacity/storage",
            "runs": runs}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", required=True)
    p.add_argument("--seeds", default="10001,10002,10003,20001,20002,20003")
    p.add_argument("--output", required=True)
    args = p.parse_args()
    output = Path(args.output)
    if output.exists():
        raise FileExistsError(output)
    report = analyze(load_config(args.config), [int(seed) for seed in args.seeds.split(",")])
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
    for run in report["runs"]:
        print(f"seed={run['seed']}: optimistic upper bound {run['optimistically_reachable_tasks']}/{run['total_tasks']}")


if __name__ == "__main__":
    main()
