# 学习差距诊断与教师纠偏

## 本轮问题与边界

上一轮结果中继协议下，旧 MAPPO 的开发集多时限交付评分为 60.67%，greedy 为 68.90%，window_aware 为 66.80%。这里的 greedy 是当前已测规则中的最佳方法，不能称为领域最优算法或文献 SOTA。

本轮保持轨道、任务、带宽、结果中继、动作空间、观测和 28417 参数 actor 不变，只调整学习过程。开发集仍为八个案例，所有任务都有相同的 30/60/120/240 分钟观测口径。测试集不参与诊断、监督标签采集或选模。

## 已定位的问题

1. 旧模型的八场景确定性推理只选择邻星卸载，没有 LOCAL 或直接地面卸载。同状态共审计 6032 个分片决策，与 greedy 一致率 51.99%；2896 次差异中，2708 次是教师选择另一颗邻星，148 次教师选择地面，40 次教师选择本地。约 93.5% 的分歧属于邻星分配。需要结合任务结果判断错误，不能把与教师不一致直接等同于决策错误。
2. 原示范教师为 window_aware，而实测 greedy 更强。结果可多跳中继后，执行星自身的地面窗口不再等于结果必须等待的窗口；window_aware 的估计可能不匹配此语义。这是待单独消融的解释，不是已证实的全部原因。
3. 旧 BC 只有九个场景、1550 个有效卫星决策样本，逐场景训练后丢弃数据，没有跨场景回放。最后一个场景训练损失仍约 1.16。该机制存在遗忘风险，本轮没有把遗忘与样本量不足作独立因果分解。
4. 旧模型在自己形成的队列状态上与教师的差距，比只在教师轨迹上看到的更大。例如 shared24_a 的动作一致率分别约为 51.2% 和 65.4%，说明需要覆盖学生自己访问的状态。
5. 旧 PPO 的 18 回合共有 933120 个卫星时间步，但只有 3166 个卫星时间步存在待决策分片，占 0.339%。无任务时间步已屏蔽 actor 损失，并不是把它们错当作有效动作；不过有效经验量确实很少。PPO 同时面对稀疏终局交付奖励、多个星共同影响一个任务的信用分配问题，不能把“近百万卫星时间步”理解为同量级有效调度样本。

greedy 也不是完美教师：它保留了“邻星收到原始分片后继续转发到地面”的候选估计，而当前 execution_commitment 会让已接收分片进入计算。此处沿用已测教师，未悄悄修改基线；后续应另建执行语义一致的启发式对照。

## 已实现的调整

- 增加 greedy 教师选项；保留原有 sequential 模式。
- pooled 模式先收集九个随机训练场景，再对完整样本池打乱训练。
- 增加一轮 DAgger：在另外九个随机训练场景执行学生策略，逐状态查询教师标签，与原始示范合并再训练。验证集标签不写入训练池。
- BC 使用独立学习率；进入 PPO 前重建优化器，避免把监督学习动量带入强化学习。
- 保存 pooled BC、DAgger 初始化、PPO best/last；记录逐动作类别召回率、混淆计数和实际任务交付，以区别“学得更像”与“完成得更好”。

DAgger 的依据是 [Ross、Gordon、Bagnell 的原始论文](https://arxiv.org/abs/1011.0686)，用于缓解学生策略导致的状态分布偏移。本轮是一个教师初始化加一轮学生轨迹聚合的有限预算实现，不承诺模仿本身能超过教师。MAPPO 的实现与超参数仍需参考[原始论文](https://arxiv.org/abs/2103.01955)做后续信用分配、价值归一化和更新稳定性研究。

## 固定开发预算

训练种子 7；九个教师轨迹 + 九个学生轨迹；两阶段均 96 个监督 epoch、学习率 1e-3；随后 18 个 MAPPO 回合。PPO 学习率 1e-4、两次更新 epoch、minibatch 256，每九回合验证。其余目标参数与上一轮一致。

这不是等预算单因素消融：教师、数据聚合、示范学习率和优化步数同时变化，PPO minibatch 也从 512 调到 256。结果只能归因于这一组训练改进，不能单独宣称某一项的增益。另行评估 pooled BC 快照，区分 DAgger 与后续 PPO 的开发表现。

```bash
.venv/bin/python train_marl.py \
  --config examples/config_relay.yaml \
  --out-dir checkpoints/relay_greedy_dagger_mappo_7_20261010 \
  --algorithm mappo --features role_contact --encoder task_attention \
  --hidden 64 --critic-pooling \
  --scenario-distribution examples/scenario_roles.json --seed 7 \
  --warmstart-episodes 9 --warmstart-epochs 96 --warmstart-teacher greedy \
  --warmstart-mode pooled --warmstart-lr 0.001 --dagger-episodes 9 \
  --episodes 18 --rollout-steps 2160 --ppo-epochs 2 --minibatch 256 \
  --lr 0.0001 --gamma 1 --lam 1 --reward-mode task \
  --delivery-horizons-minutes 30,60,120,240 --ent-coef 0.002 \
  --eval-every 9 --torch-threads 1
```

## 复核入口

`analysis/audit_imitation.py` 对同一状态同时查询学生和教师，分别执行二者的完整轨迹。`analysis/summarize_imitation_optimization.py` 在比较新训练前，要求旧模型和 greedy 的八场景重放逐任务交付状态和时间与归档完全一致，并核对任务、轨道与环境配置。中继跳数计数的差异另外保留，因此不是全轨迹等价声明。训练代码的修改会改变全源码指纹，不能直接绕过旧汇总器的源码一致性校验。

重放检查中，只有 shared36_strong_a 的教师执行出现两项中继跳数计数差异（任务 142 为 56/57，任务 143 为 53/52），交付时间完全一致。补充实验按基线原有任务顺序提交动作后，全部逐任务字段恢复一致，确认差异来自教师动作按卫星重排后的处理顺序。新旧策略的交付评分比较可用，但不混用这两种顺序的逐任务跳数作为优化证据。

## 分阶段结果

混合示范 BC 的独立开发评估已完成：八场景等权多时限评分 69.2520%，完整排空交付 742/754；greedy 为 68.9032%、741/754，旧 MAPPO 为 60.6658%、687/754。四小时内交付与仿真最终排空交付分别记录，不能混用。

BC 相对旧 MAPPO 改善 8.59 个百分点；八场景中七个评分改善、一个下降。相对 greedy 为六个场景提高、两个下降，平均仅高 0.35 个百分点，不构成稳定超越证据。在学生自己产生的验证状态上，教师动作一致率从 51.99% 提升至 95.56%。这主要证明改进训练可以弥补原模型的模仿不足，不证明多智能体强化学习已经超过教师。

DAgger 后训练样本池为 3042 个有效卫星决策行，训练池动作一致率 97.47%。完整 18 回合 PPO 结果如下：

| 阶段 | 多时限交付评分 | 最终完整交付 |
|---|---:|---:|
| 旧 MAPPO | 60.67% | 687/754 |
| greedy | 68.90% | 741/754 |
| 混合 greedy 示范 | 69.25% | 742/754 |
| DAgger 初始化 | 68.87% | 743/754 |
| DAgger + MAPPO 9 回合 | 68.10% | 741/754 |
| DAgger + MAPPO 18 回合 | 68.06% | 742/754 |

本阶段最佳学习模型来自混合示范，不能把它的收益归为 PPO。保留全部结果和按任务配对记录于 `results/relay_imitation_optimization_20261010/`。模型仍为 28417 个 actor 参数，纯 actor 导出为 `checkpoints/relay_greedy_dagger_mappo_7_20261010/export_pooled_bc/actor.pt`，文件 139686 字节；本机单线程网络中位推理 0.069 ms，不含预处理和轨道预测，不代表星载硬件。

## PPO 空动作批次诊断

旧实现虽将无决策行的 actor loss 乘为零，但仍调用统一 Adam.step。对已积累动量的 actor 参数，零梯度仍会使参数变化。使用本轮第 13 回合检查点做一次全空决策批次，16 个 actor 参数张量发生变化，最大变化约 5.57e-5；第 14 回合检查点连续十次全空批次的最大变化约 3.58e-4。

仅在无有效决策时将 actor 独占参数的梯度设为 None，十次空批次的最大变化为零。MLP + 集中 critic 的 trunk 也是 actor 独占，应一起跳过；IPPO 共享 trunk 仍用于 critic，不应冻结。回归测试同时检查 actor 不变与 critic 继续更新。

这个机制问题可以确定，但它对最终交付评分的贡献需要修复后重新训练衡量，不能将此前所有退步都归因于它。

修复已合入 `train_marl.py::ppo_update`。追加九回合开发训练从较好的 `teacher_bc.pt` 初始化，输出 `checkpoints/relay_idlefix_mappo_7_20261010/`。PPO 参数同上；不再采集示范。初始化重新验证八个案例，用于核对与保存的 BC 交付结果完全一致。该运行重新初始化 Adam 和随机状态，且从 pooled BC 而不是 DAgger 开始，因此不是只改一个开关的配对消融；回归测试证明空更新问题消失，性能实验只检验这一修正后的训练路径。

```bash
.venv/bin/python train_marl.py \
  --config examples/config_relay.yaml --out-dir checkpoints/relay_idlefix_mappo_7_20261010 \
  --initialize-checkpoint checkpoints/relay_greedy_dagger_mappo_7_20261010/teacher_bc.pt \
  --algorithm mappo --features role_contact --encoder task_attention --hidden 64 --critic-pooling \
  --scenario-distribution examples/scenario_roles.json --seed 7 --episodes 9 \
  --rollout-steps 2160 --ppo-epochs 2 --minibatch 256 --lr 0.0001 \
  --gamma 1 --lam 1 --reward-mode task --delivery-horizons-minutes 30,60,120,240 \
  --ent-coef 0.002 --eval-every 9 --torch-threads 1
```

修复后九回合已全部完成。重新验证的初始化与原 BC 八场景逐任务字段完全一致；PPO 后评分为 68.4040%，最终完整交付 741/754，低于初始化 69.2520%、742/754。按固定评分选中的 `best.pt` 为第 0 回合，即原混合示范权重；`last.pt` 为第 9 回合。不能将“修复后比另一条运行稍高”归因于修复，因为初始化和随机状态也不同。

最终统一结果、逐任务配对与图位于 `results/relay_imitation_optimization_final_20261010/`。选中模型为 `checkpoints/relay_idlefix_mappo_7_20261010/best.pt`，纯 actor 为其 `export_selected/actor.pt`。两轮 PPO 的奖励累计均按任务年龄交付评分核对；完整测试 99 项通过。

```bash
.venv/bin/python analysis/summarize_imitation_optimization.py \
  --audit-dir results/relay_imitation_audit_20261010 \
  --bc-audit-dir results/relay_pooled_bc_audit_20261010 \
  --training-dir checkpoints/relay_greedy_dagger_mappo_7_20261010 \
  --finetuning-dir checkpoints/relay_idlefix_mappo_7_20261010 \
  --output-dir results/relay_imitation_optimization_reproduce
```

## 下一步优先级

当前证据已证明小网络可以学会大部分强规则决策；没有证据表明扩网络或继续加 BC 轮数能产生强化学习优势。下一轮应固定这份强初始化，控制随机状态和交互预算，优先比较有效决策样本单独组成 actor minibatch、更新 KL/裁剪率诊断及参考策略约束，检查 PPO 是否出现破坏性更新；再检验任务级信用分配与 critic 价值归一化。上述属于后续实验，不是本轮已实现模块。

后续更新：有效决策分批与 KL 早停的三组同初始化开发对照已完成，见 [PPO 稳定性记录](active_ppo_optimization.md)。九回合结果分别为 mixed 68.40%、active2 69.20%、active16kl 68.84%；仍未超过 69.25% 初始化。固定参考策略约束尚未实现，下一步优先验证跨场景价值校准与任务信用分配。

正式结论仍需多个独立训练种子、未查看的测试面板及已核验的领域论文基线。greedy 的执行语义一致版本和公平的地面服务规则也仍需独立对照。本轮结果只能支持“修正学习过程弥补了主要模仿差距”，不支持“MARL 已稳定优于现有方法”。
