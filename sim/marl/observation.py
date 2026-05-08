"""observation.py — 把 EnvState 转换为每颗卫星的局部观测向量。

设计原则：
1. 每颗卫星只能看到自身 + 1跳邻居的信息（模拟真实分布式部署）
2. 输出固定长度向量，便于 MLP 直接消费
3. 所有数值归一化到合理范围，避免梯度爆炸
4. 邻居 pad 到 MAX_NEIGHBORS 上限（不足补零，超出截断，按带宽降序）

向量结构（共 OBS_DIM 维）：
  [0]      time_norm              当前时刻归一化（/ sim_total_s）
  [1]      queue_len_norm         自身队列长度（/ MAX_QUEUE）
  [2]      mem_remaining_norm     自身内存剩余（/ mem_capacity_gb）
  [3]      vram_remaining_norm    自身显存剩余（/ vram_capacity_gb）
  [4]      executing_remaining    当前计算 tile 的剩余比例（0=空闲）

  per GS（num_gs × 4）：
    [5+i*4+0] visible              当前是否可见（0/1）
    [5+i*4+1] window_starts_norm  下次窗口开始时间归一化（/ lookahead_s，1=无窗口）
    [5+i*4+2] window_dur_norm     下次窗口持续时长归一化（/ lookahead_s，0=无窗口）
    [5+i*4+3] gs_bw_norm          地面站带宽归一化（/ MAX_BW）

  per 邻居 slot（MAX_NEIGHBORS × 6）：
    [5+num_gs*4 + k*6+0] valid            是否有效邻居（0/1）
    [5+num_gs*4 + k*6+1] isl_bw_norm      ISL 带宽归一化
    [5+num_gs*4 + k*6+2] nb_queue_norm    邻居队列长度归一化
    [5+num_gs*4 + k*6+3] nb_mem_norm      邻居内存剩余归一化
    [5+num_gs*4 + k*6+4] nb_gs_visible    邻居是否可见任意地面站（0/1）
    [5+num_gs*4 + k*6+5] nb_window_min    邻居最近窗口开始时间归一化（最小值）

  per tile（每颗卫星最多调度 MAX_TILES_PER_SAT 个，每个 tile 7 维）：
    [base + j*7+0] has_tile           是否有待调度 tile（0/1）
    [base + j*7+1] data_size_norm     tile 大小归一化（/ image_size_mb）
    [base + j*7+2] compute_cost_norm  计算量归一化（/ max_compute_cost）
    [base + j*7+3] vram_req_norm      显存需求归一化（/ vram_capacity_gb）
    [base + j*7+4] waiting_norm       等待时间归一化（/ lookahead_s）
    [base + j*7+5] task_pending_norm  同任务未完成 tile 数（/ num_tiles）
    [base + j*7+6] cohesion_score     同任务 tile 在本卫星的集中度（0~1）
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import numpy as np

from sim.entities import EnvState
from sim.topology import link_key

# ── 超参数（固定，决定向量维度）──────────────────────────────────────────────
MAX_NEIGHBORS = 8  # 邻居 slot 上限
MAX_TILES_PER_SAT = 4  # 每颗卫星最多暴露给 obs 的 tile 数（按等待时间降序）
MAX_QUEUE = 16.0  # 队列长度归一化上限
MAX_BW = 300.0  # 带宽归一化上限（Mbps）
MAX_COMPUTE_COST = 20.0  # 计算量归一化上限


def obs_dim(num_gs: int) -> int:
    """返回观测向量的维度（依赖 num_gs）。"""
    self_dim = 5
    gs_dim = num_gs * 4
    nb_dim = MAX_NEIGHBORS * 6
    tile_dim = MAX_TILES_PER_SAT * 7
    return self_dim + gs_dim + nb_dim + tile_dim


def build_obs(env_state: EnvState, sat_id: int) -> np.ndarray:
    """为卫星 sat_id 构建局部观测向量。

    返回 shape (obs_dim(num_gs),) 的 float32 向量，所有值在 [0, 1] 或合理范围内。
    """
    cfg = env_state.config
    num_sats: int = cfg["num_sats"]
    num_gs: int = len(env_state.ground_stations)
    lookahead_s: float = cfg["gs_window_lookahead_s"]
    mem_cap: float = max(1.0, cfg["mem_capacity_gb"])
    vram_cap: float = max(1.0, cfg["vram_capacity_gb"])
    num_tiles_per_task: int = cfg["num_tiles"]

    dim = obs_dim(num_gs)
    obs = np.zeros(dim, dtype=np.float32)
    ptr = 0

    sat = env_state.satellites.get(sat_id, {})

    # ── 自身状态（5 维）────────────────────────────────────────────────────────
    # [0] 时间归一化（用 lookahead 做尺度，让时间感知有意义）
    obs[ptr] = min(1.0, env_state.time / max(1.0, lookahead_s * 10))
    ptr += 1
    # [1] 队列长度
    obs[ptr] = min(1.0, sat.get("queue_len", 0) / MAX_QUEUE)
    ptr += 1
    # [2] 内存剩余
    obs[ptr] = sat.get("mem_remaining_gb", mem_cap) / mem_cap
    ptr += 1
    # [3] 显存剩余
    obs[ptr] = sat.get("vram_remaining_gb", vram_cap) / vram_cap
    ptr += 1
    # [4] 当前执行进度（0=空闲）
    obs[ptr] = float(sat.get("executing_remaining", 0.0))
    ptr += 1

    # ── 地面站可见性 + 过境窗口（num_gs × 4）──────────────────────────────────
    gs_windows = sat.get("next_gs_windows", {})
    visible_gs = {opt["gs_id"] for opt in env_state.ground_options.get(sat_id, [])}

    for gs_id, gs_info in env_state.ground_stations.items():
        # [0] 当前是否可见
        obs[ptr] = 1.0 if gs_id in visible_gs else 0.0
        ptr += 1
        # [1~2] 窗口预测
        window = gs_windows.get(gs_id)
        if window is not None:
            starts_in_s, duration_s = window
            obs[ptr] = min(1.0, starts_in_s / lookahead_s)
            ptr += 1
            obs[ptr] = min(1.0, duration_s / lookahead_s)
            ptr += 1
        else:
            obs[ptr] = 1.0  # 无窗口：starts_in = lookahead（最远）
            ptr += 1
            obs[ptr] = 0.0  # 无窗口：duration = 0
            ptr += 1
        # [3] 地面站带宽归一化
        obs[ptr] = min(1.0, gs_info.get("bandwidth_mbps", 0.0) / MAX_BW)
        ptr += 1

    # ── 邻居信息（MAX_NEIGHBORS × 6）────────────────────────────────────────────
    neighbors = env_state.neighbors.get(sat_id, [])

    # 按 ISL 带宽降序，取前 MAX_NEIGHBORS 个
    def nb_bw(nb: int) -> float:
        lk = env_state.links.get(link_key(sat_id, nb))
        return lk["bandwidth_mbps"] if lk and lk["up"] else 0.0

    sorted_nbs = sorted(neighbors, key=nb_bw, reverse=True)[:MAX_NEIGHBORS]

    for k in range(MAX_NEIGHBORS):
        if k < len(sorted_nbs):
            nb = sorted_nbs[k]
            lk = env_state.links.get(link_key(sat_id, nb))
            nb_sat = env_state.satellites.get(nb, {})
            nb_visible_any = len(env_state.ground_options.get(nb, [])) > 0
            nb_windows = nb_sat.get("next_gs_windows", {})
            nb_min_start = min(
                (w[0] for w in nb_windows.values() if w is not None),
                default=lookahead_s,
            )

            obs[ptr] = 1.0  # valid
            ptr += 1
            obs[ptr] = min(1.0, lk["bandwidth_mbps"] / MAX_BW) if lk else 0.0  # isl_bw
            ptr += 1
            obs[ptr] = min(1.0, nb_sat.get("queue_len", 0) / MAX_QUEUE)  # nb_queue
            ptr += 1
            obs[ptr] = nb_sat.get("mem_remaining_gb", mem_cap) / mem_cap  # nb_mem
            ptr += 1
            obs[ptr] = 1.0 if nb_visible_any else 0.0  # nb_gs_visible
            ptr += 1
            obs[ptr] = min(1.0, nb_min_start / lookahead_s)  # nb_window_min
            ptr += 1
        else:
            ptr += 6  # 全零 padding

    # ── 本卫星待调度的 tile（MAX_TILES_PER_SAT × 7）──────────────────────────
    # 找该卫星上 QUEUED/READY 的 tile，按等待时间降序
    my_tiles = [
        (tid, tinfo)
        for tid, tinfo in env_state.tiles.items()
        if tinfo["location"] == sat_id and not tinfo["in_transfer"]
    ]
    my_tiles.sort(key=lambda x: x[1]["waiting_time"], reverse=True)
    my_tiles = my_tiles[:MAX_TILES_PER_SAT]

    image_size = max(
        1.0, cfg.get("image_size_mb", 4096.0) if isinstance(cfg, dict) else 4096.0
    )

    for j in range(MAX_TILES_PER_SAT):
        if j < len(my_tiles):
            _, tinfo = my_tiles[j]
            # cohesion_score：同任务 tile 中有多少在本卫星（越高越集中）
            sibling_locs = tinfo.get("sibling_locations", {})
            total_siblings = sum(sibling_locs.values())
            on_self = sibling_locs.get(sat_id, 0)
            cohesion = on_self / max(1, total_siblings)

            obs[ptr] = 1.0  # has_tile
            ptr += 1
            obs[ptr] = min(1.0, tinfo["data_size_mb"] / image_size)  # data_size
            ptr += 1
            obs[ptr] = min(
                1.0, tinfo["compute_cost"] / MAX_COMPUTE_COST
            )  # compute_cost
            ptr += 1
            obs[ptr] = min(1.0, tinfo["vram_req_gb"] / vram_cap)  # vram_req
            ptr += 1
            obs[ptr] = min(1.0, tinfo["waiting_time"] / lookahead_s)  # waiting
            ptr += 1
            obs[ptr] = min(
                1.0, tinfo.get("task_pending_count", 1) / num_tiles_per_task
            )  # task_pending
            ptr += 1
            obs[ptr] = float(cohesion)  # cohesion
            ptr += 1
        else:
            ptr += 7  # 全零 padding

    assert ptr == dim, f"obs 维度不一致: ptr={ptr}, dim={dim}"
    return obs


def build_all_obs(env_state: EnvState) -> Dict[int, np.ndarray]:
    """为所有卫星构建观测向量，返回 {sat_id: obs_vector}。"""
    return {sat_id: build_obs(env_state, sat_id) for sat_id in env_state.satellites}
