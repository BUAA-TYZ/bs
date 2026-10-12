"""Compare stochastic execution on one development case with fixed policy RNG seeds."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys

import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.config import load_config
from sim.env import SimulationEnv
from sim.marl.scenarios import ScenarioDistribution
from sim.scheduler.marl_policy import MARLPolicy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--distribution", required=True)
    parser.add_argument("--case", required=True)
    parser.add_argument("--policy-seeds", default="101,202,303")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    if output.exists():
        raise FileExistsError(output)
    distribution = ScenarioDistribution(args.distribution, load_config(args.config))
    cfg, info = next((cfg, info) for cfg, info in distribution.cases("validation") if info["id"] == args.case)
    torch.set_num_threads(1)
    policy = MARLPolicy(args.checkpoint, deterministic=False)
    records = []
    for seed in map(int, args.policy_seeds.split(",")):
        torch.manual_seed(seed)
        env = SimulationEnv(cfg)
        actions = Counter()
        try:
            for _ in range(cfg.sim_steps):
                chosen = policy.select_actions(env.export_state())
                actions.update(action.action_type.value for action in chosen)
                env.step(chosen)
            result = env.summary()
        finally:
            env.close()
        records.append({"policy_seed": seed, "action_counts": dict(actions), "metrics": result})
        print(json.dumps({"policy_seed": seed, **result["overall"]}), flush=True)
    if len({r["metrics"]["experiment"]["task_trace_sha256"] for r in records}) != 1:
        raise ValueError("Policy RNG changed the task arrival trace")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as stream:
        json.dump({"scope": "Validation-only sampling diagnostic; does not replace deterministic model selection or test",
                   "checkpoint": args.checkpoint, "checkpoint_episode": policy.metadata["episode"],
                   "checkpoint_sha256": hashlib.sha256(Path(args.checkpoint).read_bytes()).hexdigest(),
                   "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   "case": info, "runs": records}, stream, indent=2, allow_nan=False)


if __name__ == "__main__":
    main()
