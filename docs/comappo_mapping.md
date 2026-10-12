# Co-MAPPO 复现映射与取舍

核查日期：2026-09-30。依据 [作者公开全文 arXiv:2212.05757v2](https://arxiv.org/html/2212.05757v2)，重点核对 V-A、V-B 和 Algorithm 1。按论文编号、题名与作者检索，尚未确认对应作者代码仓库；这不等于作者没有公开代码。

## 原方法的必要组成

| 论文位置 | 必须保留的组成 | 当前仓库差距 |
|---|---|---|
| 式 25–26 | 联合观测/动作输入、对其他 agent 加权的注意力 Q critic | 已实现共享参数的适配版 |
| 式 36–38 | 固定其他 agent 动作，对自身动作边缘化的反事实 baseline | 已实现完整 tile 联合动作的 Monte Carlo 估计 |
| V-B、Algorithm 1 | 卸载后优化通信和计算资源 | 当前物理分配规则固定 |
| III、式 21 | ITS 来源任务、多层服务器、时延与租赁价格目标 | 当前为遥感切块、完整任务完成目标 |

因此，本仓库新加的目的地注意力 actor **不是 Co-MAPPO 复现**，也不能用它的结果代表该论文。以上区别均依据原文对应章节。

## 本项目的适配决策

优先保持同一任务到达、物理容量和评价指标，以比较学习调度部分。实现与命名分为两层：

1. `Co-MAPPO learning-component (adapted)`：保留动作条件化注意力 critic 和反事实 baseline，明确标注统一替换的奖励、服务器模型及固定资源分配。它只能支持学习组件的比较结论。
2. 完整 Co-MAPPO：需增加原任务模型、资源价格、连续分配变量和对应优化器，在匹配的论文设定下核验。当前场景缺少这些定义，暂不把完整复现列为可直接运行的基线。

不能为了得到一行性能数字，将原方法削减为普通 MAPPO 加注意力。最终论文至少要有一个经方法细节核验的领域方法；上述适配是否足够，需结合最终论文实验范围决定。

## 具体实现规格与验收点

以下是本项目的实现规格，不是对原文的逐字复述：

- critic 接收本步所有卫星观测与联合动作。训练 buffer 必须保留 `[time, agent, tile_slot]` 的联合动作对应关系，不能打散后丢失同一步其他 agent 的动作。
- 现在每星同时选择 4 个 tile 动作，每 tile 有 `10 + num_gs` 个选项。完整边缘化每星动作需处理笛卡尔积，2 个地面站时为 `12^4`；不能将独立逐 tile baseline 冒充完整联合 baseline。实现从旧策略采样整个本星动作向量，默认 16 次；用包含动作交互项的小型 Q 函数精确枚举检验估计值。
- 反事实候选改变自身动作时，需要重新计算依赖自身动作的 query 和 Q 值；其他 agent 的动作固定。所有概率使用 rollout 旧策略和相同动作 mask。
- bootstrap 使用下一时刻的联合观测及动作；Q 的 TD 目标与反事实优势分别存储。终点值归零，padding tile 只允许 WAIT。
- 单元验证已覆盖：联合状态行顺序、其他动作固定、采样均值接近精确 baseline、其他 agent 置换一致性、单 agent 无邻居、actor 不访问集中式输入、Q critic 梯度更新。完整轨迹训练和 checkpoint 推理纳入开发试验。
- 当前使用共享参数和本地网络宽度/训练预算；与作者原实现的一致性尚未验证。采样数对方差和运行开销的敏感性仍需实验。

## 运行

```bash
.venv/bin/python train_marl.py --config examples/config_training.yaml --algorithm comappo_adapted --counterfactual-samples 16 --reward-mode task --gamma 1 --episodes 20 --hidden 64 --out-dir checkpoints/comappo_adapted_run
```

代码位于 `sim/marl/attention_q.py` 和 `train_marl.py`；checkpoint 的 `paper_adaptation` 字段记录来源、组件范围和差异。训练先计算 Q-lambda 回报，actor 使用回报减旧策略反事实 baseline，critic 拟合回报；跨 rollout 的 Q bootstrap 使用旧策略在下一状态采样的联合动作，有限时域终点归零。

当前状态：全文方法映射及学习组件适配已实现；完整算法、原场景数值结果与作者实现一致性均未复现。结果表必须标为 `Co-MAPPO learning-component (adapted)`，不能简写为“Co-MAPPO 复现”。
