from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict

from main import make_policy
from sim.config import load_config
from sim.env import SimulationEnv


def run_with_mode(config_path: str, policy_name: str, mode: str) -> Dict:
    cfg = load_config(config_path)
    cfg.pipeline_mode = mode
    env = SimulationEnv(cfg)
    policy = make_policy(policy_name, cfg.seed)
    try:
        for _ in range(cfg.sim_steps):
            state = env.export_state()
            actions = policy.select_actions(state)
            env.step(actions)
        return env.metrics.summary()
    finally:
        env.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare completed tasks between pipeline modes.")
    parser.add_argument("--config", required=True, help="Path to config YAML/JSON.")
    parser.add_argument("--policy", default="greedy", help="greedy|load_aware|random")
    parser.add_argument(
        "--out-dir",
        default="results",
        help="Output directory for metrics JSON files.",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    distributed = run_with_mode(args.config, args.policy, "distributed")
    ground_compute = run_with_mode(args.config, args.policy, "ground_compute")

    (out_dir / "metrics_distributed.json").write_text(
        json.dumps(distributed, indent=2), encoding="utf-8"
    )
    (out_dir / "metrics_ground_compute.json").write_text(
        json.dumps(ground_compute, indent=2), encoding="utf-8"
    )

    d_overall = distributed["overall"]
    g_overall = ground_compute["overall"]
    print("Mode Comparison")
    print(
        f"distributed   completed_tasks={d_overall['completed_tasks']}/{d_overall['total_tasks']} "
        f"completed_tiles={d_overall['completed_tiles']}/{d_overall['total_tiles']}"
    )
    print(
        f"ground_compute completed_tasks={g_overall['completed_tasks']}/{g_overall['total_tasks']} "
        f"completed_tiles={g_overall['completed_tiles']}/{g_overall['total_tiles']}"
    )
    print(f"Saved: {out_dir / 'metrics_distributed.json'}")
    print(f"Saved: {out_dir / 'metrics_ground_compute.json'}")


if __name__ == "__main__":
    main()

