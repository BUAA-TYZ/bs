"""actor.py — 轻量 MLP Actor-Critic 网络（用于 IPPO / Parameter Sharing MARL）。

动作空间（离散，per tile）：
  0       = WAIT（不动，等待更好时机）
  1       = LOCAL（在本卫星计算，完成后下传）
  2..9    = OFFLOAD 到邻居 slot 0..7（对应 obs 中邻居顺序，按带宽降序）
  10..11  = OFFLOAD 到地面站 slot 0..1（若 num_gs=2）

总动作数 = ACT_DIM = 1 + 1 + MAX_NEIGHBORS + num_gs

网络结构：
  Input:  obs_dim(num_gs) = 89 维（num_gs=2 时）
  ──────────────────────────────────
  Linear(obs_dim, hidden)  + LayerNorm + GELU
  Linear(hidden, hidden)   + LayerNorm + GELU
  ──────────────────────────────────
  Policy head: Linear(hidden, MAX_TILES_PER_SAT * ACT_DIM)  → reshape → softmax per tile
  Value  head: Linear(hidden, 1)                             → scalar baseline

所有卫星共享同一套权重（Parameter Sharing）。
"""

from __future__ import annotations

from typing import Tuple

import torch
import torch.nn as nn
import torch.nn.functional as F

from sim.marl.observation import MAX_NEIGHBORS, MAX_TILES_PER_SAT, obs_dim
from sim.marl.features import feature_dim
from sim.marl.task_actor import TaskDestinationActor
from sim.marl.attention_q import AttentionQCritic

# 动作维度常数
ACT_DIM_BASE = 1 + 1 + MAX_NEIGHBORS  # WAIT + LOCAL + 邻居


def act_dim(num_gs: int) -> int:
    """总动作维度（与 num_gs 有关）。"""
    return ACT_DIM_BASE + num_gs


class SatActorCritic(nn.Module):
    """共享权重的 Actor-Critic MLP。

    Parameters
    ----------
    num_gs:
        地面站数量，决定观测维度和动作维度。
    hidden:
        隐藏层宽度，默认 256。
    """

    def __init__(self, num_gs: int = 2, hidden: int = 256, critic_dim: int = 0,
                 feature_set: str = "base", encoder: str = "mlp",
                 task_features: bool = True, window_features: bool = True,
                 critic_kind: str = "value") -> None:
        super().__init__()
        self.num_gs = num_gs
        self.feature_options = dict(feature_set=feature_set, task_features=task_features,
                                    window_features=window_features)
        self.in_dim = feature_dim(num_gs, feature_set)
        if encoder not in ("mlp", "task_attention") or (encoder == "task_attention" and feature_set not in ("task_window", "contact_window", "role_contact", "overlap_contact")):
            raise ValueError("task_attention requires task_window features; supported encoders: mlp, task_attention")
        if feature_set == "overlap_contact":
            # 同维不同语义也不能直接互载 state_dict；旧模型的权重格式保持不变。
            self.register_buffer("workload_protocol_version", torch.tensor(3, dtype=torch.int64))
        self.act_dim = act_dim(num_gs)
        self.num_tiles = MAX_TILES_PER_SAT
        self.critic_dim = critic_dim
        self.hidden = hidden

        # 共享 trunk
        self.trunk = nn.Sequential(
            nn.Linear(self.in_dim, hidden),
            nn.LayerNorm(hidden),
            nn.GELU(),
            nn.Linear(hidden, hidden),
            nn.LayerNorm(hidden),
            nn.GELU(),
        )

        # Policy head：为每个 tile slot 输出 act_dim 个 logit
        self.policy_head = nn.Linear(hidden, self.num_tiles * self.act_dim)

        # Value head：标量 baseline（全局 reward 的估计）
        self.value_head = nn.Linear(hidden, 1)
        self.central_critic = nn.Sequential(
            nn.Linear(critic_dim, hidden), nn.LayerNorm(hidden), nn.GELU(),
            nn.Linear(hidden, hidden), nn.LayerNorm(hidden), nn.GELU(), nn.Linear(hidden, 1),
        ) if critic_dim else None
        if critic_kind == "attention_q":
            token_dim = self.in_dim + self.num_tiles * self.act_dim
            if not critic_dim or critic_dim % token_dim:
                raise ValueError("Attention Q input must contain whole observation/action tokens")
            self.central_critic = AttentionQCritic(token_dim, hidden)
        elif critic_kind != "value":
            raise ValueError(f"Unknown critic kind: {critic_kind}")

        # 权重初始化（正交初始化 + policy head 缩小尺度）
        self._init_weights()
        self.task_actor = TaskDestinationActor(num_gs, hidden, feature_set) if encoder == "task_attention" else None

    def _init_weights(self) -> None:
        for m in self.trunk.modules():
            if isinstance(m, nn.Linear):
                nn.init.orthogonal_(m.weight, gain=1.0)
                nn.init.zeros_(m.bias)
        nn.init.orthogonal_(self.policy_head.weight, gain=0.01)
        nn.init.zeros_(self.policy_head.bias)
        nn.init.orthogonal_(self.value_head.weight, gain=1.0)
        nn.init.zeros_(self.value_head.bias)

    def forward(self, obs: torch.Tensor, critic_obs: torch.Tensor | None = None) -> Tuple[torch.Tensor, torch.Tensor]:
        """前向传播。

        Parameters
        ----------
        obs:
            shape (batch, obs_dim) 的观测张量。

        Returns
        -------
        logits:
            shape (batch, num_tiles, act_dim)，per-tile 动作 logit。
        value:
            shape (batch, 1)，状态价值估计。
        """
        feat = self.trunk(obs)
        logits = (self.task_actor(obs) if self.task_actor is not None else
                  self.policy_head(feat).view(-1, self.num_tiles, self.act_dim))
        if self.central_critic is not None and critic_obs is not None:
            value = self.central_critic(critic_obs)
        else:
            value = self.value_head(feat)
        return logits, value

    def get_action_and_value(
        self,
        obs: torch.Tensor,
        tile_mask: torch.Tensor,
        action: torch.Tensor | None = None,
        action_mask: torch.Tensor | None = None,
        critic_obs: torch.Tensor | None = None,
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        """采样动作并计算 log_prob 和 entropy（用于 PPO 训练）。

        Parameters
        ----------
        obs:
            shape (batch, obs_dim)。
        tile_mask:
            shape (batch, num_tiles)，bool 张量，True 表示该 tile slot 有效。
        action:
            若不为 None，计算给定动作的 log_prob（用于 PPO old policy 评估）；
            否则从分布中采样。

        Returns
        -------
        action:   shape (batch, num_tiles)，整数动作
        log_prob: shape (batch,)，per-sample 联合 log_prob（有效 tile 求和）
        entropy:  shape (batch,)，per-sample 联合 entropy（有效 tile 均值）
        value:    shape (batch, 1)
        """
        logits, value = self.forward(obs, critic_obs)
        # logits: (batch, num_tiles, act_dim)

        # 对无效 tile slot，将所有 logit 置 0（均匀分布），避免全 -inf NaN
        # 无效 slot 的 log_prob / entropy 不计入最终统计
        invalid_mask = ~tile_mask.unsqueeze(-1).expand_as(logits)  # (B, T, A)
        logits_safe = logits.masked_fill(invalid_mask, 0.0)
        if action_mask is not None:
            if not action_mask.any(dim=-1).all():
                raise ValueError("Every action slot must have at least one feasible action")
            logits_safe = logits_safe.masked_fill(~action_mask, torch.finfo(logits.dtype).min)

        # 对每个 tile slot 独立建 Categorical 分布
        dist = torch.distributions.Categorical(logits=logits_safe)  # (batch, num_tiles)

        if action is None:
            action = dist.sample()  # (batch, num_tiles)

        log_prob_per_tile = dist.log_prob(action)  # (batch, num_tiles)
        entropy_per_tile = dist.entropy()  # (batch, num_tiles)

        # 只对有效 tile 求和 / 均值
        tile_mask_f = tile_mask.float()
        valid_count = tile_mask_f.sum(dim=-1).clamp(min=1)  # (batch,)
        log_prob = (log_prob_per_tile * tile_mask_f).sum(dim=-1)
        entropy = (entropy_per_tile * tile_mask_f).sum(dim=-1) / valid_count

        return action, log_prob, entropy, value

    def get_value(self, obs: torch.Tensor, critic_obs: torch.Tensor | None = None) -> torch.Tensor:
        """仅计算状态价值（GAE 计算时用）。

        Returns shape (batch, 1)。
        """
        if self.central_critic is not None:
            if critic_obs is None:
                raise ValueError("MAPPO value estimation requires centralized training observations")
            return self.central_critic(critic_obs)
        feat = self.trunk(obs)
        return self.value_head(feat)
