"""参数共享 IPPO / MAPPO 基线训练；以完整 task 完成率选择验证集 checkpoint。"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import random
import time

import numpy as np
import torch
from torch import nn

from sim.config import load_config
from sim.env import SimulationEnv
from sim.marl.actor import SatActorCritic
from sim.marl.actor import act_dim
from sim.marl.attention_q import joint_q_observations, counterfactual_baseline
from sim.marl.decision import OBSERVATION_VERSION, build_batch, decode_batch
from sim.marl.features import feature_dim
from sim.marl.imitation import (pretrain_episode, collect_demonstrations,
                                merge_demonstrations, fit_demonstrations)
from sim.marl.observation import MAX_TILES_PER_SAT
from sim.marl.reward import task_potential, shaped_task_reward, delivered_task_reward
from sim.evaluation import task_age_score, task_age_delivery_curve
from sim.marl.scenarios import ScenarioDistribution, config_digest
from sim.marl.value_calibration import prediction_metrics
from sim.reproducibility import runtime_metadata


def compute_gae(rewards, values, last_value, dones, gamma=0.99, lam=0.95):
    """沿时间轴递推，保留 agent 轴；兼容单 agent 的一维输入。"""
    advantages = torch.zeros_like(values)
    next_value = torch.as_tensor(last_value, device=values.device, dtype=values.dtype)
    next_advantage = torch.zeros_like(next_value)
    done_t = torch.as_tensor(dones, device=values.device, dtype=torch.bool)
    for t in reversed(range(len(rewards))):
        alive = (~done_t[t]).to(values.dtype)
        delta = rewards[t] + gamma * next_value * alive - values[t]
        next_advantage = delta + gamma * lam * alive * next_advantage
        advantages[t] = next_advantage
        next_value = values[t]
    return advantages, advantages + values


def critic_observations(obs: torch.Tensor, algorithm: str, max_agents=None, pooling=False):
    if algorithm in ("ippo", "comappo_adapted"):
        return None
    if pooling:
        return torch.cat((obs, obs.mean(dim=0, keepdim=True).expand_as(obs)), dim=-1)
    # CTDE：全体局部状态拼接，仅供训练 critic；actor 始终只看本星状态。
    max_agents = len(obs) if max_agents is None else max_agents
    if len(obs) > max_agents:
        raise ValueError("Training constellation exceeds critic capacity")
    padded = torch.nn.functional.pad(obs, (0, 0, 0, max_agents - len(obs)))
    global_obs = padded.reshape(1, -1).expand(len(obs), -1)
    return torch.cat((obs, global_obs), dim=-1)


@torch.no_grad()
def decision_distribution(model, flat, indices):
    logits, _ = model(flat["obs"][indices])
    logits = logits.masked_fill(~flat["action_mask"][indices], torch.finfo(logits.dtype).min)
    return torch.distributions.Categorical(logits=logits)


@torch.no_grad()
def decision_drift(model, flat, indices, old_distribution, clip_eps):
    current = decision_distribution(model, flat, indices)
    tiles = flat["tile_mask"][indices]
    joint_kl = (torch.distributions.kl_divergence(old_distribution, current) * tiles).sum(-1)
    log_prob = (current.log_prob(flat["action"][indices]) * tiles).sum(-1)
    ratio = (log_prob - flat["log_prob"][indices]).exp()
    return {"joint_kl_mean": float(joint_kl.mean()), "joint_kl_max": float(joint_kl.max()),
            "clip_fraction": float(((ratio - 1).abs() > clip_eps).float().mean()),
            "greedy_action_change_fraction": float(
                ((old_distribution.probs.argmax(-1) != current.probs.argmax(-1)) & tiles).sum() / tiles.sum())}


def active_ppo_update(model, optimizer, flat, returns, normalized, active_indices, args, device, old_distribution):
    if model.central_critic is None:
        raise ValueError("Active actor batching requires a separate centralized critic")
    losses, actor_batch_sizes = [], []
    critic_updates = 0
    # GAE 已在完整时间轴上计算；只有 actor 采样过滤空闲行，critic 保留全部轨迹。
    for _ in range(args.ppo_epochs):
        for indices in torch.randperm(len(returns), device=device).split(args.minibatch):
            value = model.get_value(flat["obs"][indices], flat["critic_obs"][indices]).squeeze(-1)
            loss = args.vf_coef * 0.5 * (value - returns[indices]).square().mean()
            if not torch.isfinite(loss):
                raise RuntimeError("Non-finite critic loss")
            optimizer.zero_grad(set_to_none=True)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), args.max_grad_norm)
            optimizer.step()
            losses.append(float(loss.detach()))
            critic_updates += 1
    stopped = False
    for _ in range(args.actor_epochs or args.ppo_epochs):
        for offsets in torch.randperm(len(active_indices), device=device).split(args.minibatch):
            if not len(offsets):
                continue
            indices = active_indices[offsets]
            _, log_prob, entropy, _ = model.get_action_and_value(
                flat["obs"][indices], flat["tile_mask"][indices], flat["action"][indices],
                flat["action_mask"][indices])
            ratio = (log_prob - flat["log_prob"][indices]).exp()
            advantage = normalized[indices]
            loss = -torch.minimum(ratio * advantage,
                                  ratio.clamp(1 - args.clip_eps, 1 + args.clip_eps) * advantage).mean()
            loss -= args.ent_coef * entropy.mean()
            if not torch.isfinite(loss):
                raise RuntimeError("Non-finite actor loss")
            optimizer.zero_grad(set_to_none=True)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), args.max_grad_norm)
            optimizer.step()
            losses.append(float(loss.detach()))
            actor_batch_sizes.append(len(indices))
            # 全部有效行上的精确联合 KL，仅停止后续更新，不宣称硬约束。
            if args.target_kl and decision_drift(
                    model, flat, active_indices, old_distribution, args.clip_eps)["joint_kl_mean"] > args.target_kl:
                stopped = True
                break
        if stopped:
            break
    return losses, actor_batch_sizes, critic_updates, stopped


def ppo_update(model, optimizer, buffer, last_value, args, device, diagnostics=None):
    batch = {key: torch.stack([step[key] for step in buffer]).to(device)
             for key in buffer[0]}
    advantages, returns = compute_gae(batch["reward"], batch["value"], last_value,
                                      batch["done"], args.gamma, args.lam)
    if "counterfactual" in batch:
        # Q-lambda 回报用于 critic；actor 减去固定旧策略下的反事实 baseline。
        advantages = returns - batch["counterfactual"]
    shape = batch["value"].shape
    n = shape[0] * shape[1]
    flat = {key: value.reshape(n, *value.shape[2:]) for key, value in batch.items()}
    advantages = advantages.reshape(n)
    returns = returns.reshape(n)
    active = flat["tile_mask"].any(dim=-1)
    normalized = torch.zeros_like(advantages)
    if active.any():
        actor_adv = advantages[active]
        normalized[active] = (actor_adv - actor_adv.mean()) / actor_adv.std(unbiased=False).clamp(min=1e-8)
    active_indices = active.nonzero().flatten()
    old_distribution = (decision_distribution(model, flat, active_indices)
                        if len(active_indices) and (diagnostics is not None or args.target_kl) else None)
    losses = []
    actor_batch_sizes = []
    critic_updates = 0
    stopped = False
    model.train()
    if args.actor_batching == "active":
        losses, actor_batch_sizes, critic_updates, stopped = active_ppo_update(
            model, optimizer, flat, returns, normalized, active_indices, args, device, old_distribution)
    for _ in range(args.ppo_epochs if args.actor_batching == "mixed" else 0):
        for indices in torch.randperm(n, device=device).split(args.minibatch):
            central = flat.get("critic_obs")
            _, log_prob, entropy, value = model.get_action_and_value(
                flat["obs"][indices], flat["tile_mask"][indices], flat["action"][indices],
                flat["action_mask"][indices], None if central is None else central[indices])
            ratio = (log_prob - flat["log_prob"][indices]).exp()
            advantage = normalized[indices]
            surrogate = torch.minimum(ratio * advantage,
                                       ratio.clamp(1 - args.clip_eps, 1 + args.clip_eps) * advantage)
            actor_weight = active[indices].float()
            pg_loss = -(surrogate * actor_weight).sum() / actor_weight.sum().clamp(min=1)
            vf_loss = 0.5 * (value.squeeze(-1) - returns[indices]).square().mean()
            entropy_mean = (entropy * actor_weight).sum() / actor_weight.sum().clamp(min=1)
            loss = pg_loss + args.vf_coef * vf_loss - args.ent_coef * entropy_mean
            if not torch.isfinite(loss):
                raise RuntimeError("Non-finite PPO loss")
            optimizer.zero_grad()
            loss.backward()
            if not active[indices].any():
                # 零梯度仍会触发 Adam 动量更新；无决策批次跳过 actor 独占参数。
                actor_modules = [model.task_actor] if model.task_actor is not None else [model.policy_head]
                if model.task_actor is None and model.central_critic is not None:
                    actor_modules.append(model.trunk)
                for module in actor_modules:
                    for parameter in module.parameters():
                        parameter.grad = None
            nn.utils.clip_grad_norm_(model.parameters(), args.max_grad_norm)
            optimizer.step()
            losses.append(float(loss.detach()))
            critic_updates += 1
            count = int(active[indices].sum())
            if count:
                actor_batch_sizes.append(count)
    model.eval()
    if diagnostics is not None:
        diagnostics.update({"rows": n, "active_rows": len(active_indices), "active_fraction": float(active.float().mean()),
                            "actor_updates": len(actor_batch_sizes), "critic_updates": critic_updates,
                            "actor_batch_sizes": actor_batch_sizes, "kl_early_stop": stopped,
                            "active_advantage_std": float(advantages[active].std(unbiased=False)) if active.any() else None})
        if old_distribution is not None:
            diagnostics.update(decision_drift(model, flat, active_indices, old_distribution, args.clip_eps))
        if len(active_indices):
            with torch.no_grad():
                central = flat.get("critic_obs")
                new_value = model.get_value(flat["obs"][active_indices],
                                            None if central is None else central[active_indices]).squeeze(-1)
            diagnostics["critic_before_active"] = prediction_metrics(flat["value"][active_indices], returns[active_indices])
            diagnostics["critic_after_active"] = prediction_metrics(new_value, returns[active_indices])
    return float(np.mean(losses))


def rng_state():
    state = np.random.get_state()
    return {"python": random.getstate(), "torch": torch.get_rng_state(),
            "numpy": [state[0], state[1].tolist(), *state[2:]]}


def restore_rng(state):
    random.setstate(state["python"])
    torch.set_rng_state(state["torch"].cpu())
    numpy_state = state["numpy"]
    np.random.set_state((numpy_state[0], np.asarray(numpy_state[1], dtype=np.uint32), *numpy_state[2:]))


def evaluate(model, cfg, seeds, device, cases=None, delivery_horizons=None):
    records = []
    model.eval()
    cases = cases if cases is not None else [(replace(cfg, seed=seed, tile_lifecycle_log=""),
                                             {"id": f"seed_{seed}", "seed": seed}) for seed in seeds]
    case_records = []
    for case_cfg, case_info in cases:
        env = SimulationEnv(case_cfg)
        action_counts = Counter()
        try:
            for _ in range(case_cfg.sim_steps):
                state = env.export_state()
                obs, _, masks, tile_ids = build_batch(state, **model.feature_options)
                with torch.no_grad():
                    logits, _ = model(torch.as_tensor(obs, device=device))
                    logits = logits.masked_fill(~torch.as_tensor(masks, device=device), torch.finfo(logits.dtype).min)
                    actions = logits.argmax(dim=-1).cpu().numpy()
                env.step(decode_batch(actions, state, tile_ids))
                for row, sid in enumerate(sorted(state.satellites)):
                    for slot in range(len(tile_ids[sid])):
                        action_counts[int(actions[row, slot])] += 1
            records.append(env.metrics.summary())
            case_records.append({**case_info, "config_sha256": config_digest(case_cfg),
                                 "action_counts": dict(action_counts), "metrics": env.summary()})
        finally:
            env.close()
    rate = float(np.mean([r["overall"]["task_completion_rate"] for r in records]))
    completed = sum(r["overall"]["completed_tasks"] for r in records)
    latency = sum((r["latency"]["task_mean"] or 0.0) * r["overall"]["completed_tasks"]
                  for r in records) / completed if completed else None
    age_metrics = {}
    if delivery_horizons:
        for record in case_records:
            record["task_age"] = task_age_score(record["metrics"]["task_outcomes"], delivery_horizons,
                                                 record["metrics"]["experiment"]["elapsed_sim_seconds"])
        if any(record["task_age"]["delivery_score"] is None for record in case_records):
            raise ValueError("Validation cases must contain tasks for task-age selection")
        age_metrics = {key: float(np.mean([r["task_age"][key] for r in case_records]))
                       for key in ("delivery_score", "restricted_mean_latency_seconds")}
    return {"task_completion_rate": rate, "task_mean_seconds": latency, **age_metrics,
            "completed_tasks": completed, "seeds": [case["seed"] for case in case_records],
            "cases": case_records}


def validation_score(validation, delivery_horizons=None):
    if delivery_horizons:
        return (validation["delivery_score"], -validation["restricted_mean_latency_seconds"])
    latency = validation["task_mean_seconds"]
    return (validation["task_completion_rate"], -latency if latency is not None else float("-inf"))


def train(args):
    cfg = load_config(args.config)
    delivery_horizons = ([60 * float(value) for value in args.delivery_horizons_minutes.split(",")]
                         if args.delivery_horizons_minutes else None)
    if delivery_horizons:
        task_age_delivery_curve([], delivery_horizons, 0)
        if args.reward_mode != "task" or args.latency_bonus or args.gamma != 1:
            raise ValueError("Task-age objective requires task reward, gamma=1 and no latency bonus")
        arrival_end = cfg.task_arrival_end_step if cfg.task_arrival_end_step is not None else cfg.sim_steps
        if (cfg.sim_steps - arrival_end) * cfg.dt < max(delivery_horizons):
            raise ValueError("Task-age training requires complete follow-up after arrivals")
    if cfg.pipeline_mode != "distributed" or cfg.decision_interval_steps != 1:
        raise ValueError("Training requires distributed mode and decision_interval_steps=1")
    if min(args.episodes, args.rollout_steps, args.ppo_epochs, args.minibatch, args.eval_every) <= 0:
        raise ValueError("Training counts must be positive")
    if not 0 < args.gamma <= 1 or args.shaping_coef < 0:
        raise ValueError("Require 0 < gamma <= 1 and nonnegative shaping coefficient")
    if args.reward_mode != "task" and args.shaping_coef:
        raise ValueError("Potential shaping requires --reward-mode task")
    if args.counterfactual_samples < 1:
        raise ValueError("Counterfactual sample count must be positive")
    if args.warmstart_episodes < 0 or args.warmstart_epochs < 1 or not 0 <= args.latency_bonus <= 1:
        raise ValueError("Invalid warmstart counts or latency bonus")
    if args.dagger_episodes < 0 or (args.dagger_episodes and (
            args.warmstart_mode != "pooled" or not args.warmstart_episodes)):
        raise ValueError("DAgger requires a nonempty pooled warmstart")
    if args.warmstart_lr is not None and args.warmstart_lr <= 0:
        raise ValueError("Warmstart learning rate must be positive")
    if args.resume and args.device != "cpu":
        raise ValueError("Exact resumable training currently requires CPU RNG state")
    if args.initialize_checkpoint and (args.resume or args.warmstart_episodes or args.dagger_episodes):
        raise ValueError("Checkpoint initialization cannot be combined with resume or imitation collection")
    if args.actor_batching == "active" and args.algorithm == "ippo":
        raise ValueError("Active actor batching requires a separate centralized critic")
    if args.target_kl < 0 or (args.actor_epochs is not None and args.actor_epochs < 1):
        raise ValueError("Require nonnegative target KL and positive actor epochs")
    if args.actor_batching == "mixed" and (args.target_kl or args.actor_epochs is not None):
        raise ValueError("Actor epochs and target KL require active actor batching")
    seed = cfg.seed if args.seed is None else args.seed
    training_end = max(args.episodes, args.warmstart_episodes + args.dagger_episodes)
    distribution = ScenarioDistribution(args.scenario_distribution, cfg) if args.scenario_distribution else None
    validation_seeds = [int(s) for s in args.validation_seeds.split(",")]
    if distribution:
        validation_seeds = [case["seed"] for case in distribution.spec["validation"]]
        held_out = {case["seed"] for split in ("validation", "test") for case in distribution.spec[split]}
        if held_out & set(range(seed + 1, seed + training_end + 1)):
            raise ValueError("Training seeds overlap scenario-distribution held-out seeds")
    if not validation_seeds or set(validation_seeds) & set(range(seed + 1, seed + training_end + 1)):
        raise ValueError("Validation seeds must be disjoint from training episode seeds")
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.set_num_threads(args.torch_threads)
    device = torch.device(args.device)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    if not args.resume and any((out / name).exists() for name in ("run.json", "train_log.jsonl", "initial.pt", "best.pt", "last.pt")):
        raise FileExistsError("Output contains a previous run; select a fresh --out-dir")
    max_agents = distribution.max_training_sats if distribution else cfg.num_sats
    critic_dim = (max_agents + 1) * feature_dim(len(cfg.ground_stations), args.features) if args.algorithm == "mappo" else 0
    if args.algorithm == "mappo" and args.critic_pooling:
        critic_dim = 2 * feature_dim(len(cfg.ground_stations), args.features)
    if args.algorithm == "comappo_adapted":
        critic_dim = max_agents * (feature_dim(len(cfg.ground_stations), args.features)
                                    + MAX_TILES_PER_SAT * act_dim(len(cfg.ground_stations)))
    model_config = {"num_gs": len(cfg.ground_stations), "hidden": args.hidden, "critic_dim": critic_dim,
                    "feature_set": args.features, "encoder": args.encoder,
                    "task_features": not args.no_task_features, "window_features": not args.no_window_features,
                    "critic_kind": "attention_q" if args.algorithm == "comappo_adapted" else "value"}
    model = SatActorCritic(**model_config).to(device)
    initialization = None
    if args.initialize_checkpoint:
        source = torch.load(args.initialize_checkpoint, map_location=device, weights_only=True)
        if source["model_config"] != model_config or source["config"] != asdict(cfg):
            raise ValueError("Checkpoint initialization requires identical model and base environment configuration")
        if source.get("delivery_horizons_seconds") != delivery_horizons:
            raise ValueError("Checkpoint initialization requires identical delivery objective")
        model.load_state_dict(source["model_state"])
        initialization = {"checkpoint": args.initialize_checkpoint,
                          "checkpoint_sha256": hashlib.sha256(Path(args.initialize_checkpoint).read_bytes()).hexdigest(),
                          "source_sha256": source["source_sha256"], "episode": source["episode"],
                          "optimizer": "fresh Adam", "rng": "new run seed; not exact continuation"}
    optimizer = torch.optim.Adam(model.parameters(), lr=args.lr, eps=1e-5)
    metadata = {"algorithm": args.algorithm, "model_version": "2.0",
                **runtime_metadata(),
                "observation_version": OBSERVATION_VERSION, "model_config": model_config,
                "ground_station_ids": [g["id"] for g in cfg.ground_stations],
                "config": asdict(cfg), "training_args": vars(args), "seed": seed,
                "parameter_count": sum(p.numel() for p in model.parameters()),
                "environment_steps": cfg.sim_steps * args.episodes,
                "warmstart_environment_steps": cfg.sim_steps * (args.warmstart_episodes + args.dagger_episodes),
                "information_assumption": "instantaneous task summaries and one-hop telemetry; no control-plane cost",
                "routing_information_assumption": ("shared rule service: current global graph/queues and predicted contacts"
                                                   if cfg.result_relay else "direct result downlink"),
                "training_seeds": list(range(seed + 1, seed + training_end + 1)),
                "validation_seeds": validation_seeds,
                "delivery_horizons_seconds": delivery_horizons,
                "initialization": initialization,
                "selection": ("validation equal-weight task-age delivery score, then restricted mean latency"
                              if delivery_horizons else "validation task completion rate, then completed-task mean latency")}
    if distribution:
        metadata["scenario_distribution"] = {"sha256": distribution.sha256, "spec": distribution.spec,
                                             "max_training_agents": max_agents,
                                             "scope": "continuous training randomization; disjoint orbital time blocks"}
    if args.algorithm == "comappo_adapted":
        metadata["paper_adaptation"] = {
            "source": "https://arxiv.org/html/2212.05757v2",
            "scope": "attention action-value critic and counterfactual PPO learning component only",
            "differences": ["shared parameters", "Monte Carlo marginalization of per-satellite joint tile actions",
                            "fixed simulator resource allocation", "remote-sensing task model and common reward",
                            "local network widths and common PPO budget"],
            "counterfactual_samples": args.counterfactual_samples,
        }
    best_score = (-1.0, float("-inf"))
    first_episode = 1
    if args.resume:
        saved = torch.load(out / "last.pt", map_location=device, weights_only=True)
        excluded = {"episodes", "resume"}
        if saved["config"] != asdict(cfg) or saved["model_config"] != model_config or any(
                saved["training_args"].get(k) != v for k, v in vars(args).items() if k not in excluded):
            raise ValueError("Resume requires identical config and training arguments except episode budget")
        if saved["source_sha256"] != metadata["source_sha256"] or saved.get("scenario_distribution") != metadata.get("scenario_distribution"):
            raise ValueError("Resume source or scenario distribution changed")
        model.load_state_dict(saved["model_state"])
        optimizer.load_state_dict(saved["optimizer_state"])
        restore_rng(saved["rng_state"])
        best_score = tuple(saved["best_score"])
        first_episode = saved["episode"] + 1
        if first_episode > args.episodes:
            raise ValueError("Episode budget must exceed last completed episode")
    else:
        torch.save({**metadata, "episode": 0, "validation": None, "model_state": model.state_dict()}, out / "initial.pt")
        warm_records = []
        datasets, fits = [], []
        if args.warmstart_lr is not None:
            optimizer = torch.optim.Adam(model.parameters(), lr=args.warmstart_lr, eps=1e-5)
        for warm in range(1, args.warmstart_episodes + 1):
            warm_cfg = distribution.training_case(seed, warm)[0] if distribution else replace(cfg, seed=seed + warm)
            if args.warmstart_mode == "pooled":
                data, warm_record = collect_demonstrations(model, warm_cfg, device, args.warmstart_teacher)
                datasets.append(data)
            else:
                warm_record = pretrain_episode(model, optimizer, warm_cfg, device, args.warmstart_epochs,
                                              args.minibatch, args.warmstart_teacher)
            warm_record["seed"] = warm_cfg.seed
            warm_records.append(warm_record)
            print(json.dumps({"warmstart_episode": warm, "samples": warm_record["samples"],
                              "action_counts": warm_record["action_counts"]}), flush=True)
        if args.warmstart_mode == "pooled":
            fits.append(fit_demonstrations(model, optimizer, merge_demonstrations(datasets), device,
                                          args.warmstart_epochs, args.minibatch))
            print(json.dumps({"pooled_fit": fits[-1]}), flush=True)
            torch.save({**metadata, "episode": 0, "validation": None,
                        "model_state": model.state_dict()}, out / "teacher_bc.pt")
            for offset in range(1, args.dagger_episodes + 1):
                warm = args.warmstart_episodes + offset
                warm_cfg = distribution.training_case(seed, warm)[0] if distribution else replace(cfg, seed=seed + warm)
                data, record = collect_demonstrations(model, warm_cfg, device, args.warmstart_teacher,
                                                      learner_rollout=True)
                datasets.append(data)
                record["seed"] = warm_cfg.seed
                warm_records.append(record)
                print(json.dumps({"dagger_episode": offset, "samples": record["samples"],
                                  "teacher_agreement": record["agreement"]}), flush=True)
            if args.dagger_episodes:
                fits.append(fit_demonstrations(model, optimizer, merge_demonstrations(datasets), device,
                                              args.warmstart_epochs, args.minibatch))
                print(json.dumps({"dagger_fit": fits[-1]}), flush=True)
            del datasets
        if args.warmstart_lr is not None:
            # 监督学习与 PPO 的步长和动量独立，避免继承较大的示范更新。
            optimizer = torch.optim.Adam(model.parameters(), lr=args.lr, eps=1e-5)
        initial_validation = evaluate(model, cfg, validation_seeds, device,
                                      distribution.cases("validation") if distribution else None, delivery_horizons)
        best_score = validation_score(initial_validation, delivery_horizons)
        initial = {**metadata, "episode": 0, "validation": initial_validation, "model_state": model.state_dict()}
        torch.save(initial, out / "warmstart.pt")
        torch.save(initial, out / "best.pt")
        (out / "warmstart.json").write_text(json.dumps({"episodes": warm_records, "pooled_fits": fits,
                                                       "validation": initial_validation}, indent=2))
        print(json.dumps({"initial_validation": {k: v for k, v in initial_validation.items() if k != "cases"}}), flush=True)
    (out / "run.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    for episode in range(first_episode, args.episodes + 1):
        started = time.monotonic()
        if distribution:
            episode_cfg, episode_scenario = distribution.training_case(seed, episode)
        else:
            episode_cfg = replace(cfg, seed=seed + episode, tile_lifecycle_log="")
            episode_scenario = {"id": f"train_{seed}_{episode}", "seed": seed + episode,
                                "config_sha256": config_digest(episode_cfg)}
        env = SimulationEnv(episode_cfg)
        action_counts = Counter()
        decision_agents = Counter()
        entropy_sum, active_agent_steps = 0.0, 0
        buffer = []
        reward_sum = 0.0
        environment_reward_sum = 0.0
        updates = 0
        losses = []
        update_diagnostics = []
        model.eval()
        try:
            for step in range(cfg.sim_steps):
                state = env.export_state()
                obs, tile_masks, masks, tile_ids = build_batch(state, **model.feature_options)
                obs_t = torch.as_tensor(obs, device=device)
                tile_t = torch.as_tensor(tile_masks, device=device)
                mask_t = torch.as_tensor(masks, device=device)
                central = critic_observations(obs_t, args.algorithm, max_agents, args.critic_pooling)
                with torch.no_grad():
                    action, log_prob, entropy, value = model.get_action_and_value(
                        obs_t, tile_t, action_mask=mask_t, critic_obs=central)
                    cf_baseline = None
                    if args.algorithm == "comappo_adapted":
                        central = joint_q_observations(obs_t, action, model.act_dim)
                        value = model.get_value(obs_t, central)
                        cf_baseline = counterfactual_baseline(model, obs_t, mask_t, central, args.counterfactual_samples)
                active = tile_t.any(dim=-1)
                entropy_sum += float(entropy[active].sum())
                active_agent_steps += int(active.sum())
                for sid in active.nonzero().flatten().cpu().tolist():
                    decision_agents[sid] += 1
                for index in action[tile_t].cpu().tolist():
                    action_counts[index] += 1
                before_completed = env.metrics.completed_tasks
                previous_potential = task_potential(env, delivery_horizons) if args.shaping_coef else 0.0
                result = env.step(decode_batch(action.cpu().numpy(), state, tile_ids))
                terminal = step + 1 == cfg.sim_steps
                training_reward = result.reward
                if args.reward_mode == "task":
                    training_reward = shaped_task_reward(
                        (delivered_task_reward(env, delivery_horizons) if delivery_horizons else
                         env.metrics.completed_tasks - before_completed), previous_potential,
                        task_potential(env, delivery_horizons) if args.shaping_coef else 0.0,
                        args.gamma, args.shaping_coef, terminal)
                    if args.latency_bonus:
                        training_reward += args.latency_bonus * sum(
                            1 - (task.completion_time - task.release_time) / cfg.sim_steps
                            for task in env.tasks.values() if task.completion_time == env.time)
                reward_sum += training_reward
                environment_reward_sum += result.reward
                entry = {"obs": obs_t, "tile_mask": tile_t, "action_mask": mask_t,
                         "action": action, "log_prob": log_prob, "value": value.squeeze(-1),
                         "reward": torch.full((episode_cfg.num_sats,), training_reward / episode_cfg.num_sats, device=device),
                         "done": torch.full((episode_cfg.num_sats,), terminal, device=device, dtype=torch.bool)}
                if central is not None:
                    entry["critic_obs"] = central
                if cf_baseline is not None:
                    entry["counterfactual"] = cf_baseline
                buffer.append({key: value.detach().cpu() for key, value in entry.items()})
                if len(buffer) == args.rollout_steps or terminal:
                    with torch.no_grad():
                        if terminal:
                            bootstrap = torch.zeros(episode_cfg.num_sats, device=device)
                        else:
                            next_obs, next_tiles, next_masks, _ = build_batch(env.export_state(), **model.feature_options)
                            next_t = torch.as_tensor(next_obs, device=device)
                            next_central = critic_observations(next_t, args.algorithm, max_agents, args.critic_pooling)
                            if args.algorithm == "comappo_adapted":
                                next_actions, _, _, _ = model.get_action_and_value(
                                    next_t, torch.as_tensor(next_tiles, device=device),
                                    action_mask=torch.as_tensor(next_masks, device=device))
                                next_central = joint_q_observations(next_t, next_actions, model.act_dim)
                            bootstrap = model.get_value(next_t, next_central).squeeze(-1)
                    diagnostic = {}
                    losses.append(ppo_update(model, optimizer, buffer, bootstrap, args, device, diagnostic))
                    update_diagnostics.append(diagnostic)
                    updates += 1
                    buffer.clear()
            metrics = env.metrics.summary()
            if delivery_horizons:
                metrics["task_age"] = task_age_score(env.summary()["task_outcomes"], delivery_horizons,
                                                     env.time * env.cfg.dt)
        finally:
            env.close()
        validation = None
        is_best = False
        if episode % args.eval_every == 0 or episode == args.episodes:
            validation = evaluate(model, cfg, validation_seeds, device,
                                  distribution.cases("validation") if distribution else None, delivery_horizons)
            score = validation_score(validation, delivery_horizons)
            if score > best_score:
                best_score = score
                is_best = True
        checkpoint = {**metadata, "episode": episode, "validation": validation,
                      "model_state": model.state_dict(), "optimizer_state": optimizer.state_dict(),
                      "rng_state": rng_state(), "best_score": best_score}
        torch.save(checkpoint, out / "last.pt")
        if is_best:
            torch.save(checkpoint, out / "best.pt")
        record = {"episode": episode, "ep_reward": reward_sum,
                  "scenario": episode_scenario, "action_counts": dict(action_counts),
                  "decision_agent_steps": dict(decision_agents),
                  "active_agent_entropy_mean": entropy_sum / active_agent_steps if active_agent_steps else None,
                  "environment_reward": environment_reward_sum,
                  "task_age": metrics.get("task_age"),
                  **metrics["overall"], "failures": metrics["failures"],
                  "updates": updates, "mean_loss": float(np.mean(losses)),
                  "update_diagnostics": update_diagnostics,
                  "elapsed_s": time.monotonic() - started, "validation": validation, "is_best": is_best}
        with (out / "train_log.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, allow_nan=False) + "\n")
        console = {**record, "validation": None if validation is None else
                   {key: value for key, value in validation.items() if key != "cases"}}
        print(json.dumps(console, allow_nan=False), flush=True)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--algorithm", choices=("ippo", "mappo", "comappo_adapted"), default="ippo")
    p.add_argument("--counterfactual-samples", type=int, default=16)
    p.add_argument("--features", choices=("base", "task_window", "contact_window", "role_contact"), default="base")
    p.add_argument("--critic-pooling", action="store_true", help="MAPPO critic uses self plus mean agent observation")
    p.add_argument("--warmstart-episodes", type=int, default=0)
    p.add_argument("--warmstart-epochs", type=int, default=4)
    p.add_argument("--warmstart-lr", type=float, help="Separate BC learning rate; resets optimizer before PPO")
    p.add_argument("--warmstart-teacher", choices=("predictive", "window_aware", "greedy"), default="predictive")
    p.add_argument("--warmstart-mode", choices=("sequential", "pooled"), default="sequential")
    p.add_argument("--dagger-episodes", type=int, default=0,
                   help="Additional learner rollouts labeled by teacher and fitted with the retained demonstration pool")
    p.add_argument("--latency-bonus", type=float, default=0.0)
    p.add_argument("--delivery-horizons-minutes", help="Fixed equal-weight task ages for reward and checkpoint selection")
    p.add_argument("--resume", action="store_true", help="Continue last.pt including optimizer and CPU RNG states")
    p.add_argument("--initialize-checkpoint", help="Load weights for a fresh run with a new optimizer; not exact resume")
    p.add_argument("--encoder", choices=("mlp", "task_attention"), default="mlp")
    p.add_argument("--no-task-features", action="store_true")
    p.add_argument("--no-window-features", action="store_true")
    p.add_argument("--reward-mode", choices=("legacy", "task"), default="legacy")
    p.add_argument("--shaping-coef", type=float, default=0.0)
    p.add_argument("--episodes", type=int, default=50)
    p.add_argument("--rollout-steps", type=int, default=128)
    p.add_argument("--ppo-epochs", type=int, default=4)
    p.add_argument("--actor-batching", choices=("mixed", "active"), default="mixed")
    p.add_argument("--actor-epochs", type=int, help="Active batching only; critic still uses ppo-epochs")
    p.add_argument("--target-kl", type=float, default=0.0,
                   help="Active batching only; stop actor updates after full active-row joint KL exceeds this value")
    p.add_argument("--minibatch", type=int, default=256)
    p.add_argument("--hidden", type=int, default=256)
    p.add_argument("--lr", type=float, default=3e-4)
    p.add_argument("--gamma", type=float, default=0.99)
    p.add_argument("--lam", type=float, default=0.95)
    p.add_argument("--clip-eps", type=float, default=0.2)
    p.add_argument("--vf-coef", type=float, default=0.5)
    p.add_argument("--ent-coef", type=float, default=0.01)
    p.add_argument("--max-grad-norm", type=float, default=0.5)
    p.add_argument("--device", default="cpu")
    p.add_argument("--seed", type=int)
    p.add_argument("--torch-threads", type=int, default=1)
    p.add_argument("--eval-every", type=int, default=5)
    p.add_argument("--validation-seeds", default="10001,10002,10003")
    p.add_argument("--scenario-distribution", help="JSON train distribution and fixed validation/test cases; overrides validation seeds")
    return p


if __name__ == "__main__":
    train(parser().parse_args())
