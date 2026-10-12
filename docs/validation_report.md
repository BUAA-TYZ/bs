# 模型 v2.0 运行验证记录

日期：2026-09-30。本文记录实现正确性与运行流程验证，不用于证明收敛或算法优越性。

## 验证范围

使用仓库 `.venv/bin/python`（Python 3.12.13）。`python -m unittest discover -s tests -v` 的 24 项测试全部通过，覆盖时间单位、容量共享、资源释放、断链续传、截止期清理、阶段时序、GAE、动作 mask、集中式 critic 和不足 rollout 的训练更新。`git diff --check` 通过。

真实 TLE 的 `examples/config_smoke.yaml` 场景为 8 星、80 步、每步 5 秒。IPPO 与 MAPPO 各训练 2 个 episode，rollout 为 32，PPO 更新轮数为 2，固定验证种子为 10001。每个 episode 实际执行 3 次更新，包含尾部不足 32 步的数据；训练损失均为有限值。

- IPPO 产物：`checkpoints/v2_verified/ippo/`。
- MAPPO 产物：`checkpoints/v2_verified/mappo/`。
- 每个目录包含 `run.json`、`train_log.jsonl`、`best.pt`、`last.pt`。
- 两种算法两轮验证的 task 完成率均为 0；按既定规则保留第一轮为 best。这验证了保存与选择流程，但尚无有效的验证集性能证据。

`main.py --policy marl --marl-checkpoint checkpoints/v2_verified/mappo/best.pt` 推理成功；`run_mode_compare.py` 的 distributed 与 ground_compute 两种模式均正常结束。

## 配对评估

最终评估命令：

```bash
.venv/bin/python evaluate_policies.py --config examples/config_smoke.yaml --policies greedy,window_aware --checkpoint ippo=checkpoints/v2_verified/ippo/best.pt --checkpoint mappo=checkpoints/v2_verified/mappo/best.pt --seeds 20001,20002,20003 --output results/v2_verified_comparison.json
```

输出文件已存在；重新执行需使用新输出路径。每个种子对应的任务轨迹 SHA-256 在四种策略之间一致，测试种子与训练、验证种子不重叠。启发式策略和 MARL 使用相同候选 tile 数，并检查单个动作的硬资源约束。

| 策略 | 种子 20001 | 种子 20002 | 种子 20003 | 完成率跨场景均值 | 已完成任务平均时延 |
|---|---:|---:|---:|---:|---:|
| greedy | 0/8 | 1/9 | 0/7 | 3.70% | 55.00 秒 |
| window_aware | 0/8 | 1/9 | 0/7 | 3.70% | 55.00 秒 |
| IPPO | 1/8 | 0/9 | 0/7 | 4.17% | 30.00 秒 |
| MAPPO | 2/8 | 1/9 | 0/7 | 12.04% | 48.33 秒 |

完成率为三个场景完成率的算术平均，时延按完成任务数加权。每种策略总共只测试 24 个任务，不能据此判断算法排名；不同策略完成的任务集合也不同。当前采用固定时域且不排空队列，大量任务在终点仍为 pending。

原始记录保留配置、代码和 TLE 哈希、checkpoint 哈希、任务轨迹哈希、依赖版本及逐场景指标。报告中的 bootstrap 区间只覆盖测试场景变动，不包含独立训练重复的不确定性。更早的 `results/v2_smoke_comparison.json` 是启发式硬约束修正前的验证产物，以本次 verified 文件为准。

## 尚未完成

目前实现的是参数共享 IPPO 与 MAPPO 风格基线，卫星领域论文方法尚未复现，任务级协作创新机制尚未实现。下一阶段按 [研究计划](research_plan.md) 完成领域基线方法映射、窗口和任务状态设计，以及至少 5 个独立训练种子的正式对比、消融和泛化实验。旧版历史实验数据不可与 v2.0 直接混用。
