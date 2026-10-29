"""诊断同一训练轨迹的团队回报与待调度任务结果；相关性不等同于因果贡献。"""

import argparse
from collections import Counter
import hashlib
import io
import json
from pathlib import Path
import random
import sys

import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.config import load_config
from sim.env import SimulationEnv
from sim.marl.actor import SatActorCritic
from sim.marl.checkpoint import validate_checkpoint_protocol
from sim.marl.decision import build_batch, decode_batch
from sim.marl.reward import delivered_task_reward
from sim.marl.scenarios import ScenarioDistribution
from sim.reproducibility import runtime_metadata
from train_marl import critic_observations


def correlation(x, y):
    if np.std(x) < 1e-10 or np.std(y) < 1e-10:
        return None
    return float(np.corrcoef(x, y)[0, 1])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--critic-checkpoint", help="Use only this checkpoint's critic; keep actor and rollout RNG fixed")
    p.add_argument("--config", default="examples/config_relay.yaml")
    p.add_argument("--distribution", default="examples/scenario_roles.json")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--episode", type=int, default=1)
    p.add_argument("--output", required=True)
    args = p.parse_args()
    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)
    torch.set_num_threads(1)
    saved = torch.load(args.checkpoint, weights_only=True, map_location="cpu")
    validate_checkpoint_protocol(saved)
    model = SatActorCritic(**saved["model_config"])
    model.load_state_dict(saved["model_state"])
    critic_source = None
    if args.critic_checkpoint:
        critic_path = Path(args.critic_checkpoint)
        critic_bytes = critic_path.read_bytes()
        critic_saved = torch.load(io.BytesIO(critic_bytes), weights_only=True, map_location="cpu")
        validate_checkpoint_protocol(critic_saved)
        if critic_saved["model_config"] != saved["model_config"]:
            raise ValueError("Critic checkpoint must have the same model configuration")
        model.central_critic.load_state_dict({k.removeprefix("central_critic."): v
                                             for k, v in critic_saved["model_state"].items()
                                             if k.startswith("central_critic.")})
        critic_source = {"path": str(critic_path), "episode": critic_saved["episode"],
                         "sha256": hashlib.sha256(critic_bytes).hexdigest()}
    model.eval()
    distribution = ScenarioDistribution(args.distribution, load_config(args.config))
    cfg, scenario = distribution.training_case(args.seed, args.episode)
    horizons = saved["delivery_horizons_seconds"]
    env = SimulationEnv(cfg)
    decisions, rewards, counts = [], [], Counter()
    try:
        for step in range(cfg.sim_steps):
            state = env.export_state()
            obs, tiles, masks, tile_ids = build_batch(state, **model.feature_options)
            obs, tiles, masks = map(torch.as_tensor, (obs, tiles, masks))
            central = critic_observations(obs, "mappo", pooling=True)
            with torch.no_grad():
                action, _, _, values = model.get_action_and_value(obs, tiles, action_mask=masks, critic_obs=central)
            for row, sid in enumerate(sorted(state.satellites)):
                if tile_ids[sid]:
                    tasks = sorted({state.tiles[tid]["parent_task_id"] for tid in tile_ids[sid]})
                    decisions.append({"step": step, "satellite": sid, "tasks": tasks, "value": float(values[row, 0])})
            counts.update(action[tiles].tolist())
            env.step(decode_batch(action.numpy(), state, tile_ids))
            rewards.append(delivered_task_reward(env, horizons) / cfg.num_sats)
        summary = env.summary()
        task_scores = {tid: (sum((task.completion_time - task.release_time) * cfg.dt <= h for h in horizons) / len(horizons)
                            if task.completion_time is not None else 0.0) for tid, task in env.tasks.items()}
        future = np.asarray(rewards)[::-1].cumsum()[::-1]
        for row in decisions:
            row["team_return"] = float(future[row["step"]])
            row["advantage"] = row["team_return"] - row["value"]
            row["own_task_score_mean"] = float(np.mean([task_scores[tid] for tid in row["tasks"]]))
            row["own_task_share_of_return"] = (sum(task_scores[tid] for tid in row["tasks"])
                                               / (cfg.num_sats * row["team_return"]) if row["team_return"] > 0 else None)
        target = np.array([r["team_return"] for r in decisions])
        residual = target - np.array([r["value"] for r in decisions])
        report = {"args": vars(args), **runtime_metadata(), "scenario": scenario, "critic_source": critic_source,
                  "scope": "training-only credit diagnostic; correlation is not causal credit",
                  "action_counts": dict(counts), "total_reward": sum(rewards) * cfg.num_sats,
                  "active_rows": len(decisions),
                  "advantage_time_correlation": correlation([r["step"] for r in decisions], residual),
                  "advantage_own_task_correlation": correlation([r["own_task_score_mean"] for r in decisions], residual),
                  "critic_explained_variance_active": float(1 - np.var(residual) / np.var(target)) if np.var(target) > 1e-10 else None,
                  "own_task_return_share_median": float(np.median([r["own_task_share_of_return"] for r in decisions
                                                                                    if r["own_task_share_of_return"] is not None])),
                  "decisions": decisions, "metrics": summary,
                  "interpretation_limit": "Other tasks may be affected by resource contention; their rewards cannot simply be discarded."}
    finally:
        env.close()
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, allow_nan=False))
    print(json.dumps({k: v for k, v in report.items() if k not in ("decisions", "metrics", "args", "scenario")}, indent=2))


if __name__ == "__main__":
    main()
