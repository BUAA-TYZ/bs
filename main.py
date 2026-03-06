from __future__ import annotations

import argparse
import json
from typing import Dict

from sim.config import load_config
from sim.env import SimulationEnv
from sim.scheduler.greedy import GreedyEarliestFinish
from sim.scheduler.load_aware import LoadAwareResourceFit
from sim.scheduler.random_stub import RandomPolicy


def make_policy(name: str, seed: int):
    if name == "greedy":
        return GreedyEarliestFinish()
    if name == "load_aware":
        return LoadAwareResourceFit()
    if name == "random":
        return RandomPolicy(seed=seed)
    raise ValueError(f"Unknown policy: {name}")


def run_once(cfg_path: str, policy_name: str) -> Dict:
    cfg = load_config(cfg_path)
    env = SimulationEnv(cfg)
    policy = make_policy(policy_name, cfg.seed)

    for _ in range(cfg.sim_steps):
        state = env.export_state()
        actions = policy.select_actions(state)
        env.step(actions)

    summary = env.metrics.summary()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML/JSON config")
    parser.add_argument("--policy", default="greedy", help="greedy|load_aware|random")
    parser.add_argument("--output", default="metrics.json")
    args = parser.parse_args()

    summary = run_once(args.config, args.policy)

    print(json.dumps(summary, indent=2))
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


if __name__ == "__main__":
    main()
