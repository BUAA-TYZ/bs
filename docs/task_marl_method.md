# 任务条件化 MARL 第一版

状态：已实现可运行研究原型，尚未建立创新性或性能优势。物理仿真仍为 v2.0；基础观测和旧版 v2 checkpoint 保持兼容，新特征和网络类型写入 checkpoint 的 `model_config`。

## 要检验的问题

同一 task 的任一 tile 未交付都会阻止完整任务完成。当前原型检验：显式加入任务阶段与目的地上的同任务负载，是否比只有局部队列、带宽等信息更有利于完整任务交付；再检验按 tile 条件化的目的地编码是否比同输入 MLP 更有效。没有加入人为的“聚到同一颗卫星就奖励”规则。

## 实现

`sim/marl/features.py` 在原观测后追加星地传输积压与窗口容量估计、当前邻星方向的 ISL 积压、邻星星地积压，以及候选 tile 的任务阶段、deadline 和各目的地上的同任务 tile 比例。两个地面站时基础观测为 89 维，增强观测为 169 维。

窗口容量估计为 `持续时间 * 每通道带宽 / 8 * min(1, 接收通道数 / 当前可见卫星数)`。它是根据当前竞争状态估计的轮转份额，不是真实未来可用容量；不作为强制 action mask。当前只增加星地窗口容量特征，尚未实现 ISL 未来窗口预测。

`sim/marl/task_actor.py` 为当前 tile 编码 query，对 WAIT、本地计算、邻星和地面站分别编码候选目的地。邻星和地面站各自共享编码器，不使用卫星编号。目的地 token 包含对应同任务负载；注意力汇总目的地信息后，用共享打分器产生各动作 logit。无效邻星不参加注意力，最终动作仍使用公共可行性 mask。邻星顺序变化时相应动作分数等变。

此结构是单星 actor 内对目的地的注意力，尚未加入 agent 间可学习消息传递。协作依赖任务摘要和邻星资源信息，不能描述为已实现图消息通信。MAPPO critic 仍使用集中式训练观测，执行时不需要该 critic。

## 信息假设

同任务完成/失败状态与其他位置的 tile 数由仿真器即时提供；地面站竞争卫星数需视作站端广播。当前未建模状态同步时延、丢包或控制信令成本。基础观测本来也读取全局维护的同任务摘要；所有增强输入 MLP 与注意力对照共享同一信息权限。

`--no-task-features` 同时清除原有 pending/cohesion 和新增任务进度、目的地同任务比例，避免消融泄漏；保留本 tile 大小、计算量、等待时间和 deadline。`--no-window-features` 清除原有窗口起始/持续时间、邻星窗口起始及新增预测容量，保留当前可见性和积压。

## 奖励

新增 `--reward-mode task`，以本步新增完整任务数为基础奖励。对固定外生任务轨迹、`gamma=1` 的有限时域，它最大化完整任务数；跨到达数量不同的场景，完成数目标与场景完成率均值并不完全等价，评估仍以完成率为主、已完成任务时延为辅。

可选 `--shaping-coef c` 添加 `c * (gamma * Phi(next) - Phi(current))`。`Phi` 是所有无失败 task 的已交付比例平方之和，终点势函数强制为 0；已完成 task 的势保留至终点。折扣后的势奖励会望远镜消去，不额外偏好未完成 tile 的堆积。奖励总额与环境旧奖励分别记入日志，方便核对。正式消融需固定其他条件比较 `c=0` 与非零值。

## 可复现入口

训练单个候选模型：

```bash
.venv/bin/python train_marl.py --config examples/config_training.yaml --out-dir checkpoints/task_candidate_run --algorithm mappo --features task_window --encoder task_attention --reward-mode task --gamma 1 --episodes 20 --hidden 64
```

完整开发矩阵包含 Co-MAPPO 学习组件适配、IPPO、基础 MAPPO、增强输入 MLP、任务目的地 actor、去任务特征、去窗口特征。默认使用相同完整任务奖励，势函数系数为 0：

```bash
.venv/bin/python run_marl_study.py --config examples/config_training.yaml --out-dir results/task_study_run --episodes 20 --test-seeds 30001,30002,30003,30004,30005
```

每种策略使用相同交互步数、PPO 参数、候选数和验证集；不同网络参数量记录在 run.json，不能称为严格参数量匹配实验。保留全部独立训练和逐测试场景结果，按训练种子、共享测试场景两个轴进行配对交叉 bootstrap；样本少时区间仅作探索性参考。测试只改变任务随机种子，尚不包含轨道/星座规模泛化。

启发式参照包含全部本地计算 `local`、greedy 和 window_aware。每次训练保存 `initial.pt`、`best.pt`、`last.pt`，可区分初始化偏好与训练收益；`initial.pt` 的 episode 为 0，不参与 best 选择。

中断后可在完全相同参数后追加 `--resume`，复用已完整训练的 checkpoint 并补齐缺失评估；已有评估的 checkpoint 哈希、配置和任务轨迹均会核对。它不恢复训练中途的 optimizer 状态，不完整训练需使用新目录重跑。原运行及恢复运行的代码哈希均记录在 manifest 中。

批量入口默认检查验证集的离线乐观可达率上界，低于 10% 时拒绝训练。首轮 8 星短场景已因此被判定不适合算法比较，见 [开发试验与诊断](task_marl_pilot_report.md)。单独调用 `train_marl.py` 不自动进行此预检。
