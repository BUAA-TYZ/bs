"""IPPO (Independent PPO) 训练脚本，Parameter Sharing 版本。

每颗卫星视为独立 Agent，共享同一套网络权重（SatActorCritic）。
合作式设置：所有 Agent 共享同一个全局 reward（env.step() 返回值）。

训练流程：
  1. 环境重置
  2. 收集 rollout_steps 步经验（每步所有卫星并行决策）
  3. 计算 GAE 优势估计
  4. 多轮 minibatch PPO-clip 更新
  5. 按 episode 保存 checkpoint（highest total reward）
  6. 循环直到 total_episodes 结束

运行方式：
  python train_marl.py --config examples/config.yaml --out-dir checkpoints/
  python train_marl.py --config examples/config.yaml --out-dir checkpoints/ --episodes 100

产出：
  checkpoints/best.pt     — 历史最高 episode reward 的权重
  checkpoints/last.pt     — 最近一次 episode 结束时的权重
  checkpoints/train_log.jsonl — 每 episode 的训练统计
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim

from sim.config import SimConfig, load_config
from sim.entities import Action, ActionType, EnvState
from sim.env import SimulationEnv
from sim.marl.actor import SatActorCritic, act_dim
from sim.marl.observation import MAX_NEIGHBORS, MAX_TILES_PER_SAT, build_obs, obs_dim
from sim.topology import link_key

# ────────────────────────────────────────────────────────────────────── #
# 动作解码：把网络输出的整数动作 → Action 对象                            #
# ────────────────────────────────────────────────────────────────────── #


def decode_action(
    act_idx: int,
    tile_id: str,
    env_state: EnvState,
    sat_id: int,
    num_gs: int,
) -> Action:
    """把 [0, act_dim) 的整数动作解码为 Action 对象。

    动作索引约定（与 actor.py 一致）：
      0        = WAIT
      1        = LOCAL
      2..9     = OFFLOAD 到邻居 slot 0..7（按 ISL 带宽降序，与 obs 一致）
      10..11   = OFFLOAD 到地面站 slot 0..1
    """
    if act_idx == 0:
        return Action(tile_id=tile_id, action_type=ActionType.WAIT)
    if act_idx == 1:
        return Action(tile_id=tile_id, action_type=ActionType.LOCAL)

    nb_offset = 2
    gs_offset = 2 + MAX_NEIGHBORS

    if nb_offset <= act_idx < gs_offset:
        # OFFLOAD 到邻居
        nb_slot = act_idx - nb_offset
        neighbors = env_state.neighbors.get(sat_id, [])

        def nb_bw(nb: int) -> float:
            lk = env_state.links.get(link_key(sat_id, nb))
            return lk["bandwidth_mbps"] if lk and lk["up"] else 0.0

        sorted_nbs = sorted(neighbors, key=nb_bw, reverse=True)
        if nb_slot < len(sorted_nbs):
            target_sat = sorted_nbs[nb_slot]
            return Action(
                tile_id=tile_id,
                action_type=ActionType.OFFLOAD,
                target_sat_id=target_sat,
            )
        # 无效邻居 slot → WAIT
        return Action(tile_id=tile_id, action_type=ActionType.WAIT)

    if act_idx >= gs_offset:
        # OFFLOAD 到地面站
        gs_slot = act_idx - gs_offset
        gs_ids = list(env_state.ground_stations.keys())
        if gs_slot < len(gs_ids):
            return Action(
                tile_id=tile_id,
                action_type=ActionType.OFFLOAD,
                target_gs_id=gs_ids[gs_slot],
            )
        return Action(tile_id=tile_id, action_type=ActionType.WAIT)

    return Action(tile_id=tile_id, action_type=ActionType.WAIT)


def collect_actions(
    env_state: EnvState,
    sat_actions: Dict[int, np.ndarray],  # {sat_id: [tile_slot → act_idx]}
    num_gs: int,
) -> List[Action]:
    """把所有卫星的动作数组转换为 Action 列表（传给 env.step）。"""
    actions: List[Action] = []
    # 找每颗卫星上的待调度 tile（按等待时间降序，与 obs 对齐）
    sat_tiles: Dict[int, List[str]] = {i: [] for i in env_state.satellites.keys()}
    for tile_id, tile in env_state.tiles.items():
        if tile["in_transfer"]:
            continue
        loc = tile["location"]
        sat_tiles.setdefault(loc, []).append(tile_id)

    # 按等待时间降序排序（与 build_obs 中 my_tiles 排序保持一致）
    for sat_id, tile_ids in sat_tiles.items():
        tile_ids.sort(
            key=lambda tid: env_state.tiles[tid]["waiting_time"], reverse=True
        )
        sat_tiles[sat_id] = tile_ids[:MAX_TILES_PER_SAT]

    for sat_id, act_arr in sat_actions.items():
        tile_ids = sat_tiles.get(sat_id, [])
        for slot, act_idx in enumerate(act_arr):
            if slot >= len(tile_ids):
                break
            tile_id = tile_ids[slot]
            action = decode_action(act_idx, tile_id, env_state, sat_id, num_gs)
            actions.append(action)
    return actions


# ────────────────────────────────────────────────────────────────────── #
# Rollout 缓冲区                                                          #
# ────────────────────────────────────────────────────────────────────── #


class RolloutBuffer:
    """存储一次 rollout 的经验，用于 PPO 更新。

    每条经验对应一颗卫星在一步的观测/动作/奖励/done/value。
    由于所有卫星共享同一个全局 reward，因此 reward 对所有卫星相同。
    """

    def __init__(self) -> None:
        self.obs: List[np.ndarray] = []
        self.actions: List[np.ndarray] = []  # shape (num_tiles,)
        self.log_probs: List[float] = []
        self.rewards: List[float] = []
        self.values: List[float] = []
        self.dones: List[bool] = []
        self.tile_masks: List[np.ndarray] = []  # shape (num_tiles,)

    def add(
        self,
        obs: np.ndarray,
        action: np.ndarray,
        log_prob: float,
        reward: float,
        value: float,
        done: bool,
        tile_mask: np.ndarray,
    ) -> None:
        self.obs.append(obs)
        self.actions.append(action)
        self.log_probs.append(log_prob)
        self.rewards.append(reward)
        self.values.append(value)
        self.dones.append(done)
        self.tile_masks.append(tile_mask)

    def __len__(self) -> int:
        return len(self.obs)

    def as_tensors(
        self, device: torch.device
    ) -> Tuple[
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
    ]:
        obs_t = torch.tensor(np.stack(self.obs), dtype=torch.float32, device=device)
        act_t = torch.tensor(np.stack(self.actions), dtype=torch.long, device=device)
        lp_t = torch.tensor(self.log_probs, dtype=torch.float32, device=device)
        rew_t = torch.tensor(self.rewards, dtype=torch.float32, device=device)
        val_t = torch.tensor(self.values, dtype=torch.float32, device=device)
        mask_t = torch.tensor(
            np.stack(self.tile_masks), dtype=torch.bool, device=device
        )
        return obs_t, act_t, lp_t, rew_t, val_t, mask_t


def compute_gae(
    rewards: torch.Tensor,  # (T,)
    values: torch.Tensor,  # (T,)
    last_value: float,
    dones: List[bool],
    gamma: float = 0.99,
    lam: float = 0.95,
) -> Tuple[torch.Tensor, torch.Tensor]:
    """计算 GAE(λ) 优势估计和 returns。

    Returns: (advantages, returns), 均为 shape (T,)。
    """
    T = len(rewards)
    advantages = torch.zeros(T, dtype=torch.float32)
    last_adv = 0.0
    next_val = last_value

    for t in reversed(range(T)):
        mask = 0.0 if dones[t] else 1.0
        delta = rewards[t].item() + gamma * next_val * mask - values[t].item()
        last_adv = delta + gamma * lam * mask * last_adv
        advantages[t] = last_adv
        next_val = values[t].item()

    returns = advantages + values
    return advantages, returns


# ────────────────────────────────────────────────────────────────────── #
# 主训练循环                                                              #
# ────────────────────────────────────────────────────────────────────── #


def train(
    config_path: str,
    out_dir: str,
    total_episodes: int = 50,
    rollout_steps: int = 128,
    ppo_epochs: int = 4,
    minibatch_size: int = 256,
    lr: float = 3e-4,
    gamma: float = 0.99,
    lam: float = 0.95,
    clip_eps: float = 0.2,
    vf_coef: float = 0.5,
    ent_coef: float = 0.01,
    max_grad_norm: float = 0.5,
    device_str: str = "cpu",
    seed: Optional[int] = None,
) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    log_path = out_path / "train_log.jsonl"

    device = torch.device(device_str)
    cfg = load_config(config_path)
    if seed is not None:
        cfg.seed = seed

    num_gs = len(cfg.ground_stations)
    in_dim = obs_dim(num_gs)
    a_dim = act_dim(num_gs)

    model = SatActorCritic(num_gs=num_gs, hidden=256).to(device)
    optimizer = optim.Adam(model.parameters(), lr=lr, eps=1e-5)

    best_ep_reward = float("-inf")
    log_records = []

    print(f"IPPO Training: obs_dim={in_dim}, act_dim={a_dim}, num_gs={num_gs}")
    print(
        f"Episodes={total_episodes}, rollout_steps={rollout_steps}, "
        f"ppo_epochs={ppo_epochs}, device={device_str}"
    )

    for episode in range(1, total_episodes + 1):
        ep_start = time.time()
        cfg_ep = load_config(config_path)
        cfg_ep.seed = cfg.seed + episode  # 每 episode 用不同种子，增加多样性
        env = SimulationEnv(cfg_ep)

        buffer = RolloutBuffer()
        ep_reward = 0.0
        step_count = 0
        done = False

        model.eval()
        for global_step in range(cfg_ep.sim_steps):
            state = env.export_state()
            num_sats = cfg_ep.num_sats

            # 为每颗卫星构建 obs 并推断动作（no_grad：仅推断，不建计算图）
            obs_list: List[np.ndarray] = []
            tile_mask_list: List[np.ndarray] = []
            for sat_id in range(num_sats):
                o = build_obs(state, sat_id)
                obs_list.append(o)
                my_tiles = [
                    tid
                    for tid, t in state.tiles.items()
                    if t["location"] == sat_id and not t["in_transfer"]
                ]
                n_valid = min(len(my_tiles), MAX_TILES_PER_SAT)
                mask = np.zeros(MAX_TILES_PER_SAT, dtype=bool)
                mask[:n_valid] = True
                tile_mask_list.append(mask)

            obs_batch = torch.tensor(
                np.stack(obs_list), dtype=torch.float32, device=device
            )
            mask_batch = torch.tensor(
                np.stack(tile_mask_list), dtype=torch.bool, device=device
            )

            # Rollout 推断：禁用梯度（节省内存）
            with torch.no_grad():
                actions_t, log_probs_t, _, values_t = model.get_action_and_value(
                    obs_batch, mask_batch
                )

            # 解码 Action 列表
            sat_actions_dict = {
                sat_id: actions_t[sat_id].cpu().numpy() for sat_id in range(num_sats)
            }
            env_actions = collect_actions(state, sat_actions_dict, num_gs)

            # 执行 step
            step_result = env.step(env_actions)
            reward = step_result.reward
            ep_reward += reward
            step_count += 1

            # 存入 buffer（每步、每颗卫星）
            for sat_id in range(num_sats):
                buffer.add(
                    obs=obs_list[sat_id],
                    action=actions_t[sat_id].cpu().numpy(),
                    log_prob=log_probs_t[sat_id].item(),
                    reward=reward / num_sats,
                    value=values_t[sat_id].item(),
                    done=False,
                    tile_mask=tile_mask_list[sat_id],
                )

            # 每 rollout_steps 步做一次 PPO 更新（在 no_grad 上下文外）
            if len(buffer) >= rollout_steps * num_sats:
                with torch.no_grad():
                    next_state = env.export_state()
                    next_obs = build_obs(next_state, 0)
                    next_obs_t = torch.tensor(
                        next_obs[None], dtype=torch.float32, device=device
                    )
                    last_value = model.get_value(next_obs_t).item()

                _ppo_update(
                    model=model,
                    optimizer=optimizer,
                    buffer=buffer,
                    last_value=last_value,
                    gamma=gamma,
                    lam=lam,
                    ppo_epochs=ppo_epochs,
                    minibatch_size=minibatch_size,
                    clip_eps=clip_eps,
                    vf_coef=vf_coef,
                    ent_coef=ent_coef,
                    max_grad_norm=max_grad_norm,
                    device=device,
                )
                buffer = RolloutBuffer()  # 清空 buffer

        env.close()
        ep_elapsed = time.time() - ep_start
        metrics = env.metrics.summary()

        # 保存 checkpoint
        torch.save(model.state_dict(), out_path / "last.pt")
        if ep_reward > best_ep_reward:
            best_ep_reward = ep_reward
            torch.save(model.state_dict(), out_path / "best.pt")
            best_marker = "*"
        else:
            best_marker = " "

        record = {
            "episode": episode,
            "ep_reward": round(ep_reward, 3),
            "best_ep_reward": round(best_ep_reward, 3),
            "completed_tiles": metrics["overall"]["completed_tiles"],
            "total_tiles": metrics["overall"]["total_tiles"],
            "completed_tasks": metrics["overall"]["completed_tasks"],
            "elapsed_s": round(ep_elapsed, 1),
        }
        log_records.append(record)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        print(
            f"[Ep {episode:3d}/{total_episodes}]{best_marker} "
            f"reward={ep_reward:8.1f} "
            f"tiles={record['completed_tiles']}/{record['total_tiles']} "
            f"tasks={record['completed_tasks']} "
            f"time={ep_elapsed:.1f}s"
        )

    print(f"\nTraining done. best.pt saved to {out_path / 'best.pt'}")


# ────────────────────────────────────────────────────────────────────── #
# PPO 更新                                                               #
# ────────────────────────────────────────────────────────────────────── #


def _ppo_update(
    model: SatActorCritic,
    optimizer: optim.Optimizer,
    buffer: RolloutBuffer,
    last_value: float,
    gamma: float,
    lam: float,
    ppo_epochs: int,
    minibatch_size: int,
    clip_eps: float,
    vf_coef: float,
    ent_coef: float,
    max_grad_norm: float,
    device: torch.device,
) -> None:
    obs_t, act_t, old_lp_t, rew_t, val_t, mask_t = buffer.as_tensors(device)

    advantages, returns = compute_gae(
        rewards=rew_t.cpu(),
        values=val_t.cpu(),
        last_value=last_value,
        dones=buffer.dones,
        gamma=gamma,
        lam=lam,
    )
    advantages = advantages.to(device)
    returns = returns.to(device)

    # 归一化优势
    adv_mean = advantages.mean()
    adv_std = advantages.std().clamp(min=1e-8)
    advantages = (advantages - adv_mean) / adv_std

    N = len(buffer)
    model.train()
    for _ in range(ppo_epochs):
        perm = torch.randperm(N, device=device)
        for start in range(0, N, minibatch_size):
            idx = perm[start : start + minibatch_size]
            mb_obs = obs_t[idx]
            mb_act = act_t[idx]
            mb_old_lp = old_lp_t[idx]
            mb_adv = advantages[idx]
            mb_ret = returns[idx]
            mb_mask = mask_t[idx]

            _, new_lp, entropy, new_val = model.get_action_and_value(
                mb_obs, mb_mask, action=mb_act
            )

            # PPO-clip policy loss
            ratio = torch.exp(new_lp - mb_old_lp)
            # 标准 PPO-clip：取 surrogate 的最小值（即损失的最大值）
            surr1 = ratio * mb_adv
            surr2 = ratio.clamp(1 - clip_eps, 1 + clip_eps) * mb_adv
            pg_loss = -torch.min(surr1, surr2).mean()

            # Value loss（MSE）
            vf_loss = 0.5 * ((new_val.squeeze() - mb_ret) ** 2).mean()

            # Entropy bonus（鼓励探索）
            ent_loss = -entropy.mean()

            loss = pg_loss + vf_coef * vf_loss + ent_coef * ent_loss

            optimizer.zero_grad()
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), max_grad_norm)
            optimizer.step()

    model.eval()


# ────────────────────────────────────────────────────────────────────── #
# CLI 入口                                                               #
# ────────────────────────────────────────────────────────────────────── #


def main() -> None:
    parser = argparse.ArgumentParser(description="Train MARL policy via IPPO.")
    parser.add_argument("--config", required=True, help="Path to config YAML/JSON.")
    parser.add_argument(
        "--out-dir", default="checkpoints", help="Output directory for checkpoints."
    )
    parser.add_argument(
        "--episodes", type=int, default=50, help="Total training episodes."
    )
    parser.add_argument("--rollout-steps", type=int, default=128)
    parser.add_argument("--ppo-epochs", type=int, default=4)
    parser.add_argument("--minibatch", type=int, default=256)
    parser.add_argument("--lr", type=float, default=3e-4)
    parser.add_argument("--gamma", type=float, default=0.99)
    parser.add_argument("--lam", type=float, default=0.95)
    parser.add_argument("--clip-eps", type=float, default=0.2)
    parser.add_argument("--vf-coef", type=float, default=0.5)
    parser.add_argument("--ent-coef", type=float, default=0.01)
    parser.add_argument("--device", default="cpu", help="cpu|cuda|mps")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    train(
        config_path=args.config,
        out_dir=args.out_dir,
        total_episodes=args.episodes,
        rollout_steps=args.rollout_steps,
        ppo_epochs=args.ppo_epochs,
        minibatch_size=args.minibatch,
        lr=args.lr,
        gamma=args.gamma,
        lam=args.lam,
        clip_eps=args.clip_eps,
        vf_coef=args.vf_coef,
        ent_coef=args.ent_coef,
        device_str=args.device,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
