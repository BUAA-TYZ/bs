from __future__ import annotations

import argparse
import json
from typing import Dict

from tqdm import tqdm

from sim.config import load_config
from sim.env import SimulationEnv
from sim.scheduler.greedy import GreedyEarliestFinish
from sim.scheduler.load_aware import LoadAwareResourceFit
from sim.scheduler.random_stub import RandomPolicy
from sim.scheduler.window_aware import WindowAwareGreedy


def make_policy(name: str, seed: int, marl_checkpoint: str = ""):
    if name == "greedy":
        return GreedyEarliestFinish()
    if name == "load_aware":
        return LoadAwareResourceFit()
    if name == "random":
        return RandomPolicy(seed=seed)
    if name == "window_aware":
        return WindowAwareGreedy()
    if name == "marl":
        from sim.scheduler.marl_policy import MARLPolicy

        if not marl_checkpoint:
            raise ValueError("--marl-checkpoint must be specified for marl policy")
        return MARLPolicy(checkpoint_path=marl_checkpoint)
    raise ValueError(f"Unknown policy: {name}")


def run_once(cfg_path: str, policy_name: str, marl_checkpoint: str = "") -> Dict:
    cfg = load_config(cfg_path)
    env = SimulationEnv(cfg)
    policy = make_policy(policy_name, cfg.seed, marl_checkpoint=marl_checkpoint)
    decision_interval = max(1, cfg.decision_interval_steps)
    cached_actions = []

    total_sim_s = cfg.sim_steps * cfg.dt
    try:
        with tqdm(total=cfg.sim_steps, unit="step", desc=policy_name) as pbar:
            for step_idx in range(cfg.sim_steps):
                if step_idx % decision_interval == 0:
                    state = env.export_state()
                    cached_actions = policy.select_actions(state)
                env.step(cached_actions)
                pbar.set_postfix_str(f"t={step_idx * cfg.dt:.0f}/{total_sim_s:.0f}s")
                pbar.update(1)
        summary = env.metrics.summary()
        return summary
    finally:
        env.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML/JSON config")
    parser.add_argument(
        "--policy", default="greedy", help="greedy|load_aware|random|window_aware|marl"
    )
    parser.add_argument("--output", default="metrics.json")
    parser.add_argument(
        "--marl-checkpoint",
        default="",
        help="Path to MARL checkpoint (.pt) when --policy=marl",
    )
    args = parser.parse_args()

    summary = run_once(args.config, args.policy)

    print(json.dumps(summary, indent=2))
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


if __name__ == "__main__":
    main()
