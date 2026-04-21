"""MARL 推理接口，实现与 GreedyEarliestFinish 相同的 select_actions 接口。

加载训练好的 SatActorCritic 权重，对每颗卫星进行推断，返回 Action 列表。
可直接代入 run_mode_compare.py 和 main.py 进行对比实验。

用法：
    from sim.scheduler.marl_policy import MARLPolicy

    policy = MARLPolicy(checkpoint_path="checkpoints/best.pt", num_gs=2)
    actions = policy.select_actions(env_state)
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import torch

from sim.entities import Action, ActionType, EnvState
from sim.marl.actor import SatActorCritic, act_dim
from sim.marl.observation import MAX_NEIGHBORS, MAX_TILES_PER_SAT, build_obs, obs_dim
from sim.scheduler.base import SchedulerPolicy
from sim.topology import link_key


class MARLPolicy(SchedulerPolicy):
    """加载训练好权重的 MARL 推理策略。

    Parameters
    ----------
    checkpoint_path:
        `best.pt` 或 `last.pt` 的路径。
    num_gs:
        地面站数量（必须与训练时一致，决定 obs_dim 和 act_dim）。
    hidden:
        网络隐藏层宽度（必须与训练时一致，默认 256）。
    device:
        推断设备，默认 "cpu"。
    deterministic:
        True → 取 argmax（贪心推断）；False → 从分布中采样（随机推断）。
        对比实验时建议用 True。
    """

    def __init__(
        self,
        checkpoint_path: str,
        num_gs: int = 2,
        hidden: int = 256,
        device: str = "cpu",
        deterministic: bool = True,
    ) -> None:
        self.num_gs = num_gs
        self.device = torch.device(device)
        self.deterministic = deterministic

        self.model = SatActorCritic(num_gs=num_gs, hidden=hidden)
        ckpt = Path(checkpoint_path)
        if not ckpt.exists():
            raise FileNotFoundError(f"Checkpoint not found: {ckpt}")
        state_dict = torch.load(str(ckpt), map_location=self.device)
        self.model.load_state_dict(state_dict)
        self.model.to(self.device)
        self.model.eval()

    def select_actions(self, env_state: EnvState) -> List[Action]:
        """为当前状态的所有待调度 tile 生成 Action 列表。

        与 GreedyEarliestFinish.select_actions 接口完全兼容。
        """
        num_sats = len(env_state.satellites)

        # 构建所有卫星的 obs 和 tile_mask
        obs_list: List[np.ndarray] = []
        mask_list: List[np.ndarray] = []
        sat_tile_ids: Dict[int, List[str]] = {}

        for sat_id in range(num_sats):
            obs = build_obs(env_state, sat_id)
            obs_list.append(obs)

            my_tiles = [
                tid
                for tid, t in env_state.tiles.items()
                if t["location"] == sat_id and not t["in_transfer"]
            ]
            my_tiles.sort(
                key=lambda tid: env_state.tiles[tid]["waiting_time"], reverse=True
            )
            my_tiles = my_tiles[:MAX_TILES_PER_SAT]
            sat_tile_ids[sat_id] = my_tiles

            n_valid = len(my_tiles)
            mask = np.zeros(MAX_TILES_PER_SAT, dtype=bool)
            mask[:n_valid] = True
            mask_list.append(mask)

        obs_batch = torch.tensor(
            np.stack(obs_list), dtype=torch.float32, device=self.device
        )
        mask_batch = torch.tensor(
            np.stack(mask_list), dtype=torch.bool, device=self.device
        )

        with torch.no_grad():
            logits, _ = self.model(obs_batch)  # (num_sats, num_tiles, act_dim)
            if self.deterministic:
                actions_t = logits.argmax(dim=-1)  # (num_sats, num_tiles)
            else:
                dist = torch.distributions.Categorical(logits=logits)
                actions_t = dist.sample()  # (num_sats, num_tiles)

        actions: List[Action] = []
        for sat_id in range(num_sats):
            tile_ids = sat_tile_ids[sat_id]
            for slot, tile_id in enumerate(tile_ids):
                act_idx = actions_t[sat_id, slot].item()
                action = _decode_action(
                    act_idx, tile_id, env_state, sat_id, self.num_gs
                )
                actions.append(action)

        return actions


# ────────────────────────────────────────────────────────────────────── #
# 动作解码（与 train_marl.py 保持一致）                                    #
# ────────────────────────────────────────────────────────────────────── #


def _decode_action(
    act_idx: int,
    tile_id: str,
    env_state: EnvState,
    sat_id: int,
    num_gs: int,
) -> Action:
    """把 [0, act_dim) 的整数动作解码为 Action 对象。

    动作索引约定：
      0        = WAIT
      1        = LOCAL
      2..9     = OFFLOAD 到邻居 slot 0..7（按 ISL 带宽降序）
      10..11   = OFFLOAD 到地面站 slot 0..1
    """
    if act_idx == 0:
        return Action(tile_id=tile_id, action_type=ActionType.WAIT)
    if act_idx == 1:
        return Action(tile_id=tile_id, action_type=ActionType.LOCAL)

    nb_offset = 2
    gs_offset = 2 + MAX_NEIGHBORS

    if nb_offset <= act_idx < gs_offset:
        nb_slot = act_idx - nb_offset
        neighbors = env_state.neighbors.get(sat_id, [])

        def nb_bw(nb: int) -> float:
            lk = env_state.links.get(link_key(sat_id, nb))
            return lk["bandwidth_mbps"] if lk and lk["up"] else 0.0

        sorted_nbs = sorted(neighbors, key=nb_bw, reverse=True)
        if nb_slot < len(sorted_nbs):
            return Action(
                tile_id=tile_id,
                action_type=ActionType.OFFLOAD,
                target_sat_id=sorted_nbs[nb_slot],
            )
        return Action(tile_id=tile_id, action_type=ActionType.WAIT)

    if act_idx >= gs_offset:
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
