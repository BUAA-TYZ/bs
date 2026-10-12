"""固定策略下的多场景价值校准；采集与拟合随机流独立。"""

import time

import numpy as np
import torch

from sim.env import SimulationEnv
from sim.marl.actor import SatActorCritic
from sim.marl.decision import build_batch, decode_batch
from sim.marl.reward import delivered_task_reward


def prediction_metrics(prediction, target):
    if not len(target):
        raise ValueError("Value metrics require nonempty targets")
    prediction, target = prediction.detach().double(), target.detach().double()
    residual = target - prediction
    variance = float(target.var(unbiased=False))
    mse = float(residual.square().mean())
    return {"rows": len(target), "mse": mse, "rmse": mse ** 0.5,
            "bias": float((prediction - target).mean()), "target_mean": float(target.mean()),
            "target_std": variance ** 0.5,
            "normalized_mse": mse / variance if variance > 1e-12 else None,
            "explained_variance": 1 - float(residual.var(unbiased=False)) / variance if variance > 1e-12 else None}


def collect_value_case(cfg, model_config, model_state, horizons, uniform_rows=512):
    """仅保留有效决策与均匀时间样本；回报由完整终止轨迹计算。"""
    torch.set_num_threads(1)
    model = SatActorCritic(**model_config)
    model.load_state_dict(model_state)
    model.eval()
    if model_config.get("critic_kind", "value") != "value" or model.critic_dim != 2 * model.in_dim:
        raise ValueError("Value calibration requires pooled MAPPO value critic")
    torch.manual_seed(cfg.seed + 10_000_000)
    rng = np.random.default_rng(cfg.seed + 20_000_000)
    total = cfg.sim_steps * cfg.num_sats
    sampled = set(rng.choice(total, size=min(uniform_rows, total), replace=False).tolist())
    rows = {"active": [], "uniform": []}
    times = {"active": [], "uniform": []}
    rewards = []
    started = time.monotonic()
    env = SimulationEnv(cfg)
    try:
        for step in range(cfg.sim_steps):
            state = env.export_state()
            obs, tiles, masks, tile_ids = build_batch(state, **model.feature_options)
            obs, tiles, masks = map(torch.as_tensor, (obs, tiles, masks))
            central = torch.cat((obs, obs.mean(0, keepdim=True).expand_as(obs)), -1)
            active_indices = tiles.any(-1).nonzero().flatten()
            uniform_indices = torch.tensor([i for i in range(cfg.num_sats)
                                            if step * cfg.num_sats + i in sampled], dtype=torch.long)
            for kind, indices in (("active", active_indices), ("uniform", uniform_indices)):
                if len(indices):
                    rows[kind].append(central[indices])
                    times[kind].extend([step] * len(indices))
            with torch.no_grad():
                action, _, _, _ = model.get_action_and_value(obs, tiles, action_mask=masks)
            env.step(decode_batch(action.numpy(), state, tile_ids))
            rewards.append(delivered_task_reward(env, horizons) / cfg.num_sats)
        metrics = env.summary()
    finally:
        env.close()
    future = torch.tensor(np.asarray(rewards, dtype=np.float64)[::-1].cumsum()[::-1].copy(), dtype=torch.float32)
    data = {kind: {"obs": torch.cat(rows[kind]) if rows[kind] else torch.empty(0, model.critic_dim),
                   "target": future[times[kind]]} for kind in rows}
    return {"data": data, "metrics": metrics, "elapsed_s": time.monotonic() - started,
            "seed": cfg.seed, "reward_sum": sum(rewards) * cfg.num_sats,
            "sampling": {"policy_seed": cfg.seed + 10_000_000, "time_sample_seed": cfg.seed + 20_000_000,
                         "active_rows": len(times["active"]), "uniform_rows": len(times["uniform"])}}


def balanced_value_dataset(cases, rows_per_case, seed):
    if rows_per_case < 2 or rows_per_case % 2:
        raise ValueError("Rows per case must be positive and even")
    generator = torch.Generator().manual_seed(seed)
    datasets = []
    for case in cases:
        chunks = []
        for kind in ("active", "uniform"):
            group = case["data"][kind]
            if not len(group["target"]):
                raise ValueError(f"Calibration case lacks {kind} rows")
            # 两种训练方式共用相同有放回抽样，保持场景和决策时刻的权重一致。
            indices = torch.randint(len(group["target"]), (rows_per_case // 2,), generator=generator)
            chunks.append({key: value[indices] for key, value in group.items()})
        datasets.append({key: torch.cat([chunk[key] for chunk in chunks]) for key in chunks[0]})
    return datasets


def fit_value_critic(model, datasets, mode, epochs=32, minibatch=256, lr=3e-4, seed=7):
    if mode not in ("sequential", "pooled") or model.central_critic is None:
        raise ValueError("Require a separate critic and sequential/pooled fitting")
    if min(epochs, minibatch) <= 0 or lr <= 0:
        raise ValueError("Invalid calibration optimizer parameters")
    unchanged = {k: v.clone() for k, v in model.state_dict().items() if not k.startswith("central_critic.")}
    groups = datasets if mode == "sequential" else [{key: torch.cat([d[key] for d in datasets]) for key in datasets[0]}]
    optimizer = torch.optim.Adam(model.central_critic.parameters(), lr=lr, eps=1e-5)
    generator = torch.Generator().manual_seed(seed)
    losses = []
    started = time.monotonic()
    model.train()
    for group in groups:
        for _ in range(epochs):
            for indices in torch.randperm(len(group["target"]), generator=generator).split(minibatch):
                prediction = model.central_critic(group["obs"][indices]).squeeze(-1)
                loss = 0.5 * (prediction - group["target"][indices]).square().mean()
                if not torch.isfinite(loss):
                    raise RuntimeError("Non-finite calibration loss")
                optimizer.zero_grad(set_to_none=True)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.central_critic.parameters(), 0.5)
                optimizer.step()
                losses.append(float(loss.detach()))
    model.eval()
    if any(not torch.equal(value, model.state_dict()[key]) for key, value in unchanged.items()):
        raise RuntimeError("Calibration changed non-critic parameters")
    return {"mode": mode, "updates": len(losses), "mean_loss": float(np.mean(losses)),
            "elapsed_s": time.monotonic() - started, "actor_unchanged": True}


@torch.no_grad()
def evaluate_value_critic(model, cases):
    records = []
    model.eval()
    for case in cases:
        record = {"seed": case["seed"], "scenario": case.get("scenario")}
        for kind in ("active", "uniform"):
            group = case["data"][kind]
            prediction = model.central_critic(group["obs"]).squeeze(-1)
            record[kind] = prediction_metrics(prediction, group["target"])
        records.append(record)
    aggregates = {}
    for kind in ("active", "uniform"):
        aggregates[kind] = {}
        for key in ("mse", "rmse", "normalized_mse", "explained_variance"):
            values = [r[kind][key] for r in records if r[kind][key] is not None]
            aggregates[kind][key] = float(np.mean(values)) if values else None
    return {"cases": records, **aggregates}
