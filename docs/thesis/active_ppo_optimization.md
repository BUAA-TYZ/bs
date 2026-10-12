# 有效决策批次与 PPO 更新稳定性

## 问题与实现

此前强 greedy 示范初始化的开发评分为 69.2520%，修复空闲批次的 Adam 动量更新后，九回合 PPO 仍降至 68.4040%。本轮保持环境、任务、结果中继、观测、28417 参数 actor 和评价口径不变，检查策略更新本身。

旧分批方式将所有卫星时间步打乱，每批 256 行；有效决策仅占约 0.34%。虽然空闲行不贡献策略损失，但每批策略损失除以该批有效行数，导致很多梯度只来自一两个有效样本，同一份轨迹触发大量小样本 Adam 更新。

新增 `--actor-batching active`：先沿完整时间轴计算 GAE 和回报，再对有效行组成 actor minibatch。集中 critic 仍使用全部轨迹，独立执行原有两次 epoch。策略更新不会反向传播到 critic，critic 更新也不会推动 actor。当前此模式要求独立的集中 critic，不支持共享 trunk 的 IPPO。

新增 `--actor-epochs` 和 `--target-kl`：可以增加有效样本复用次数，同时逐次计算当前策略相对本次 rollout 策略的精确 KL。KL 在所有有效卫星决策行上计算，每行对有效分片求和，再对行取平均。阈值超过后停止后续 actor 更新；它是更新后早停，不是硬 KL 上界或回滚机制。

训练日志保留每次更新的有效样本数、actor/critic 更新次数、actor 批次大小、平均与最大联合 KL、样本动作裁剪比例和确定性动作改变比例。默认 mixed 行为保留，便于与既有训练核对。

## 固定开发对照

三组从同一 `teacher_bc.pt` 初始化，重新建立 Adam，训练种子均为 7，学习率 1e-4，九个相同随机场景，完整 rollout 2160 步。每组重新运行八场景初始化评估；最终在第九回合评估。所有实验仅使用开发面板，没有查看或训练新的测试数据。

| 组别 | actor 数据 | actor epoch | KL 早停阈值 | critic epoch |
|---|---|---:|---:|---:|
| mixed | 全时刻混合，每批只使用其中有效行 | 2 | 无 | 2 |
| active2 | 仅有效行 | 2 | 无 | 2 |
| active16kl | 仅有效行 | 最多 16 | 0.01 | 2 |

mixed 与 active2 检验有效数据分批和策略/价值分开更新这一组实现变化；相同交互与 epoch 预算不等于相同梯度更新次数。active16kl 同时增加 actor 复用次数和 KL 早停，不能单独归因于 KL。不同方法首次更新后访问的状态允许不同，但首条轨迹应相同。

策略和价值分开更新也去除了原来共同梯度裁剪的耦合。分批打乱会消耗不同数量的随机数，因此同一初始种子不表示首次更新后每个动作都使用逐次相同的随机数。性能的因果归因仍需独立重复，第一回合的同轨迹更新诊断则不受后续轨迹分歧影响。

复现命令的公共部分：

```bash
.venv/bin/python train_marl.py \
  --config examples/config_relay.yaml \
  --initialize-checkpoint checkpoints/relay_greedy_dagger_mappo_7_20261010/teacher_bc.pt \
  --algorithm mappo --features role_contact --encoder task_attention --hidden 64 --critic-pooling \
  --scenario-distribution examples/scenario_roles.json --seed 7 --episodes 9 \
  --rollout-steps 2160 --ppo-epochs 2 --minibatch 256 --lr 0.0001 \
  --gamma 1 --lam 1 --reward-mode task --delivery-horizons-minutes 30,60,120,240 \
  --ent-coef 0.002 --eval-every 9 --torch-threads 1 \
  --actor-batching active --actor-epochs 16 --target-kl 0.01 \
  --out-dir checkpoints/relay_active16kl_mappo_7_20261010
```

其他两组按表替换最后的分批参数，并使用独立输出目录。汇总脚本为 `analysis/summarize_active_ppo.py`，检查初始化权重、初始逐任务交付、首条采样轨迹摘要、环境与源码指纹、训练奖励和任务年龄评分的恒等关系。

## 完成结果

三组均已完成九回合训练及八案例最终评估。当前源码指纹为 `5db17731d2a0a91fdaefc79f254c69c94e02a534f1c5353fbc53e4758c79d66d`。三组初始化权重、初始逐任务结果和首回合采样摘要一致；mixed 最终模型的全部权重、最终逐任务交付与此前空更新修复实验逐项一致，说明新增诊断未改变对照行为。

| 方法 | 多时限交付评分 | 最终完整交付 | 四小时受限平均时延（秒） |
|---|---:|---:|---:|
| 示范初始化 | 69.2520% | 742/754 | 3850.0 |
| greedy 归档对照 | 68.9032% | 741/754 | 3874.8 |
| mixed / 9 回合 | 68.4040% | 741/754 | 3966.5 |
| active2 / 9 回合 | 69.1994% | 742/754 | 3844.4 |
| active16kl / 9 回合 | 68.8443% | 743/754 | 3867.8 |

最终完整交付是六小时排空后的计数，不等于四小时内交付。active16kl 虽多完成一个任务，但主指标低于初始化，因此不能因最终计数更高而改动预先约定的选模标准。

| 方法 | 有效行 / 全部行 | actor 更新次数 | 每批有效行中位数 | 平均联合 KL | 触发早停的回合数 |
|---|---:|---:|---:|---:|---:|
| mixed | 1550 / 466560 | 2021 | 1 | 0.025085 | 0 |
| active2 | 1550 / 466560 | 20 | 153 | 0.000288 | 0 |
| active16kl | 1550 / 466560 | 137 | 170 | 0.006576 | 3 |

第一回合的同一条轨迹含 38 个有效行。mixed 更新 65 次，active2 更新 2 次，平均联合 KL 分别为 0.006645 和 0.000738；active16kl 第 9 次更新后触发早停。这一同数据诊断直接说明批次组织会显著改变更新次数和策略偏移。

active2 相对 mixed 提高 0.7954 个百分点，但相对初始化仍低 0.0526 个百分点；相对初始化，八案例中一个提高、两个下降、五个评分相同。增加复用和早停没有进一步提高主评分。三组 `best.pt` 都选中第 0 回合，权重仍是示范初始化，不能把 69.25% 归因于本轮 PPO。

本轮获得的是减轻训练退化的开发证据，尚未得到超过强初始化的新模型。102 项测试通过，覆盖延迟奖励穿过空闲时刻、空闲批次动量、actor/critic 参数隔离和 KL 早停。开发结果不替代独立多种子结论。

```bash
.venv/bin/python analysis/summarize_active_ppo.py \
  --runs mixed=checkpoints/relay_mixed_mappo_7_20261010 \
         active2=checkpoints/relay_active2_mappo_7_20261010 \
         active16kl=checkpoints/relay_active16kl_mappo_7_20261010 \
  --output-dir results/relay_active_ppo_reproduce
```

完整汇总、逐案例配对、更新记录和图位于 `results/relay_active_ppo_20261010/`。三组的第九回合权重各保存在对应目录的 `last.pt`，便于后续延续开发；当前最佳仍保留原示范模型。

## 信用分配诊断

`analysis/audit_ppo_credit.py` 只在训练分布上执行固定 BC 策略，记录每次调度关联的任务、旧 critic 估值、后续团队回报和最终任务交付评分。没有使用教师标签，也没有用诊断数据额外拟合模型。

当前任务在后续团队回报中的占比、优势与发生时刻/任务结果的相关性，仅用于判断噪声来源，不是反事实因果贡献。调度通过共享队列影响其他任务，因此不能据此直接删除其他任务的奖励。示范阶段没有训练 critic，首次 PPO 的价值基线尚未校准；需要与后续训练阶段区分。

固定 BC 策略在三个训练案例上的结果如下。回报占比统计一次决策关联的全部任务，不等于单动作的因果贡献。

| 训练案例 | 有效决策行 | 优势与时刻相关性 | 优势与关联任务评分相关性 | 关联任务回报占比中位数 | critic 解释方差 |
|---|---:|---:|---:|---:|---:|
| near / 12 星 / episode 1 | 38 | -0.918 | 0.695 | 9.30% | 0.040 |
| uniform / 12 星 / episode 8 | 84 | -0.931 | -0.055 | 2.00% | 0.072 |
| shared / 24 星 / episode 7 | 334 | -0.965 | 0.154 | 0.71% | -0.037 |

对 near/12 星案例，固定 BC actor，仅替换为归档 mixed 九回合后的 critic，逐任务交付完全相同，但解释方差变为 -4.776。改为该九回合模型自身的 actor 重新采样，解释方差为 -4.732。负值表示 critic 对本条轨迹回报变化的解释差于常数基线。这提示跨场景价值拟合需要检查，不能把“critic 已经做了很多次更新”等同于价值估计可靠。一次轨迹的蒙特卡洛回报有噪声，前一项还存在 critic 与执行策略不同的限制，因此不据此独立判定全部退化原因。

原始记录位于 `results/relay_active_ppo_credit_20261010/`。表格使用 `initial_episode1.json`、`initial_episode7.json`、`initial_episode8.json`；稳定归档对照使用 `control9_critic_episode1.json` 和 `control9_on_policy_episode1.json`。运行中读取过一次第 2 回合 critic 的探索记录另存 `fitted_critic_episode1.json`，其原路径后来被训练覆盖，不用于本表或可复现结论。

这些诊断优先支持后续比较：在多场景数据上校准/归一化价值估计，检查场景间遗忘；以及保持共同任务目标的反事实或任务条件信用分配。不能仅凭相关性把其他任务奖励全部删除，也不能把 critic 校准当作已证明的算法创新。

## 验证边界

本轮是单训练种子的开发对照。即使超过初始化，也需要更多独立训练种子和新的未见测试面板；即使只抑制退化，也应如实归为训练稳定性改进，不能写作已证明的多智能体算法优势。领域论文基线、任务级信用分配、价值基线校准仍需后续独立研究。
