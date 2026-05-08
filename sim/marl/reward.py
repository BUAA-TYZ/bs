"""reward.py — 为 MARL 训练定义 step-level reward shaping。

设计原则：
1. 每个 step 返回一个标量 reward，供所有 Agent 共享（合作式 MARL）
2. 奖励信号密集：不依赖任务完成这一稀疏事件，tile 完成即给奖励
3. 惩罚信号明确：失败有代价，长时间等待有轻微惩罚
4. 量级对齐：tile 完成 +1 是基准，任务完成 +10 是跳跃，失败 -2 是警示

Reward 组成：
  +1.0  × new_tiles_done       本步新完成的 tile 数
  +10.0 × new_tasks_done       本步新完成的任务数（32 tile 全部 DONE）
  -2.0  × new_failures         本步新失败的 tile 数（mem_full / link_down 等）
  -0.01 × num_waiting_tiles    本步仍在等待的 tile 数（稀疏鼓励，防止囤积）

使用方式：
    from sim.marl.reward import RewardConfig, compute_reward, StepEvents

    # 在 env.step() 前后记录 metrics 快照，再调用 compute_reward
    events = StepEvents(
        new_tiles_done=2,
        new_tasks_done=0,
        new_failures=1,
        num_waiting_tiles=15,
    )
    r = compute_reward(events, cfg=RewardConfig())
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class RewardConfig:
    """奖励权重配置，可在训练时调整。"""

    tile_done_reward: float = 1.0
    task_done_reward: float = 10.0
    failure_penalty: float = -2.0
    waiting_penalty_per_tile: float = -0.01


@dataclass
class StepEvents:
    """单步内发生的事件数量（由 env.step() 计算后填入）。"""

    new_tiles_done: int = 0
    new_tasks_done: int = 0
    new_failures: int = 0  # 所有失败原因之和
    num_waiting_tiles: int = 0  # 本步结束时仍处于 QUEUED/READY 的 tile 数


def compute_reward(events: StepEvents, cfg: RewardConfig = RewardConfig()) -> float:
    """根据本步事件计算标量 reward。"""
    r = 0.0
    r += cfg.tile_done_reward * events.new_tiles_done
    r += cfg.task_done_reward * events.new_tasks_done
    r += cfg.failure_penalty * events.new_failures
    r += cfg.waiting_penalty_per_tile * events.num_waiting_tiles
    return r


def diff_events(
    tiles_done_before: int,
    tasks_done_before: int,
    failures_before: Dict[str, int],
    tiles_done_after: int,
    tasks_done_after: int,
    failures_after: Dict[str, int],
    num_waiting_tiles: int,
) -> StepEvents:
    """从 step 前后的 metrics 快照计算本步事件。

    典型用法（在 env.step 里）：
        snap_before = _metrics_snapshot(self.metrics)
        ... 执行 step 逻辑 ...
        snap_after  = _metrics_snapshot(self.metrics)
        events = diff_events(*snap_before, *snap_after, waiting)
    """
    new_failures = sum(
        failures_after.get(k, 0) - failures_before.get(k, 0)
        for k in set(list(failures_before) + list(failures_after))
    )
    return StepEvents(
        new_tiles_done=tiles_done_after - tiles_done_before,
        new_tasks_done=tasks_done_after - tasks_done_before,
        new_failures=max(0, new_failures),
        num_waiting_tiles=num_waiting_tiles,
    )
