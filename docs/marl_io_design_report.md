# MARL 输入输出设计报告

## 1. 背景与目标

本项目当前的 MARL 方案把每颗卫星视为一个独立 agent，并使用参数共享的 Actor-Critic 网络进行训练。每个 agent 在每个仿真步接收自身的局部观测，输出对本星若干个候选 tile 的调度动作。

目标不是让单颗卫星掌握全局最优解，而是让它基于局部信息做出合理的一步决策：

```text
对于本星当前最重要的 K 个 tile：
  是等待、本地计算、卸载给邻居，还是上传到地面站？
```

当前实现位于：

- `sim/marl/observation.py`：构造每颗卫星的输入观测向量。
- `sim/marl/actor.py`：定义 Actor-Critic 神经网络。
- `train_marl.py`：采样动作、执行环境、收集经验并 PPO 更新。
- `sim/scheduler/marl_policy.py`：加载 checkpoint 后用于推理评估。

## 2. 当前输入观测总体结构

当前每颗卫星的 observation 是一个固定长度一维向量。默认配置中地面站数量为 2，因此每颗卫星输入维度为 89。

维度公式：

```text
obs_dim = self_dim + ground_station_dim + neighbor_dim + tile_dim
        = 5 + num_gs * 4 + MAX_NEIGHBORS * 6 + MAX_TILES_PER_SAT * 7
```

当前常量：

```text
num_gs = 2
MAX_NEIGHBORS = 8
MAX_TILES_PER_SAT = 4
```

因此：

```text
obs_dim = 5 + 2 * 4 + 8 * 6 + 4 * 7
        = 5 + 8 + 48 + 28
        = 89
```

整体分块如下：

| 模块 | 维度 | 说明 |
|---|---:|---|
| 本卫星自身状态 | 5 | 队列、资源、当前执行状态 |
| 地面站窗口与带宽 | 8 | 2 个地面站，每个 4 维 |
| 邻居卫星状态 | 48 | 最多 8 个邻居，每个 6 维 |
| 本星候选 tile | 28 | 最多 4 个 tile，每个 7 维 |
| 合计 | 89 | 每颗卫星一个 observation |

## 3. 本卫星自身状态：5 维

| 索引 | 特征 | 维度 | 含义 | 当前归一化方式 |
|---:|---|---:|---|---|
| 0 | `time_norm` | 1 | 当前仿真时间 | `time / (lookahead_s * 10)`，截断到 1 |
| 1 | `queue_len_norm` | 1 | 本星队列长度 | `queue_len / MAX_QUEUE`，截断到 1 |
| 2 | `mem_remaining_norm` | 1 | 本星剩余存储比例 | `mem_remaining_gb / mem_capacity_gb` |
| 3 | `vram_remaining_norm` | 1 | 本星剩余显存比例 | `vram_remaining_gb / vram_capacity_gb` |
| 4 | `executing_remaining` | 1 | 当前执行 tile 的剩余比例 | 空闲为 0，否则为剩余计算占比 |

这一组特征回答的问题是：本卫星现在忙不忙，还有多少资源，是否适合继续接收或本地处理 tile。

## 4. 地面站信息：每站 4 维

当前默认有 2 个地面站，因此地面站部分共 8 维。

每个地面站的特征如下：

| 特征 | 维度 | 含义 | 当前归一化方式 |
|---|---:|---|---|
| `visible` | 1 | 当前本星是否可见该地面站 | 可见为 1，否则 0 |
| `window_starts_norm` | 1 | 下一次可见窗口多久后开始 | `starts_in_s / lookahead_s`，无窗口为 1 |
| `window_dur_norm` | 1 | 下一次窗口持续多久 | `duration_s / lookahead_s`，无窗口为 0 |
| `gs_bw_norm` | 1 | 地面站带宽 | `bandwidth_mbps / MAX_BW`，截断到 1 |

这组特征回答的问题是：本星是否应该现在上传结果或 tile，或者等待下一次窗口。

当前不足：

- 地面站计算队列长度没有进入 observation。
- 地面站是否正在计算没有进入 observation。
- 当前窗口剩余时间没有显式进入 observation。

如果允许 agent 做 `OFFLOAD_TO_GROUND_STATION`，这些信息会影响它判断“上传到地面站是否真的快”。

## 5. 邻居卫星信息：最多 8 个，每个 6 维

当前最多取 8 个邻居，按 ISL 带宽降序排序，不足 8 个时补零。因此邻居部分共 48 维。

每个邻居 slot 的特征如下：

| 特征 | 维度 | 含义 | 当前归一化方式 |
|---|---:|---|---|
| `valid` | 1 | 该邻居 slot 是否有效 | 有效为 1，否则 0 |
| `isl_bw_norm` | 1 | 到邻居的星间链路带宽 | `bandwidth_mbps / MAX_BW`，截断到 1 |
| `nb_queue_norm` | 1 | 邻居队列长度 | `queue_len / MAX_QUEUE`，截断到 1 |
| `nb_mem_norm` | 1 | 邻居剩余存储比例 | `mem_remaining_gb / mem_capacity_gb` |
| `nb_gs_visible` | 1 | 邻居当前是否可见任意地面站 | 可见为 1，否则 0 |
| `nb_window_min` | 1 | 邻居最近地面站窗口开始时间 | `min_start / lookahead_s`，截断到 1 |

这组特征回答的问题是：把 tile 传给哪个邻居更合适。

当前不足：

- 没有邻居显存剩余。
- 没有邻居当前执行剩余时间。
- 没有邻居到地面站的窗口持续时间。
- 邻居排序只按带宽，不一定等价于调度可行性。

例如，一个邻居带宽很高但队列很长、没有地面窗口，另一个邻居带宽略低但马上可见地面站，后者可能更优。当前特征里部分信息存在，但排序规则和特征表达还不够直接。

## 6. 本星候选 tile 信息：最多 4 个，每个 7 维

当前每颗卫星最多暴露 4 个 tile 给模型，按 `waiting_time` 降序排序。tile 部分共 28 维。

每个 tile slot 的特征如下：

| 特征 | 维度 | 含义 | 当前归一化方式 |
|---|---:|---|---|
| `has_tile` | 1 | 该 tile slot 是否有效 | 有效为 1，否则 0 |
| `data_size_norm` | 1 | tile 数据大小 | `data_size_mb / image_size_mb`，截断到 1 |
| `compute_cost_norm` | 1 | tile 计算量 | `compute_cost / MAX_COMPUTE_COST`，截断到 1 |
| `vram_req_norm` | 1 | tile 显存需求 | `vram_req_gb / vram_capacity_gb`，截断到 1 |
| `waiting_norm` | 1 | tile 已等待时间 | `waiting_time / lookahead_s`，截断到 1 |
| `task_pending_norm` | 1 | 所属 task 当前未完成 tile 数 | `task_pending_count / num_tiles`，截断到 1 |
| `cohesion_score` | 1 | 同任务 tile 在本星的集中度 | `on_self / total_siblings` |

这组特征回答的问题是：本星手上的哪些 tile 更值得优先调度。

当前不足：

- `MAX_TILES_PER_SAT = 4` 偏小。每个任务有 32 个 tile，队列一长，agent 只能看到很小一部分。
- 没有 deadline 剩余时间。当前配置未启用 deadline 时影响不大，但后续若启用 deadline，这会非常重要。
- 没有 task 已完成比例、task 最慢 tile、task 是否接近完成等任务级摘要。
- `cohesion_score` 的定义偏弱，不能充分表达“这个 task 是否值得集中完成”。

## 7. 当前输出动作结构

当前 Actor 对每颗卫星输出 `MAX_TILES_PER_SAT` 个动作分布。也就是说，每颗卫星每个 step 最多对 4 个 tile 做决策。

默认 2 个地面站时，单个 tile slot 的动作数为：

```text
act_dim = WAIT + LOCAL + MAX_NEIGHBORS + num_gs
        = 1 + 1 + 8 + 2
        = 12
```

动作含义：

| 动作编号 | 含义 |
|---:|---|
| 0 | `WAIT`，等待 |
| 1 | `LOCAL`，本地计算 |
| 2-9 | 卸载到邻居 slot 0-7 |
| 10-11 | 卸载到地面站 slot 0-1 |

每颗卫星总输出 logits 形状：

```text
(MAX_TILES_PER_SAT, act_dim) = (4, 12)
```

Actor 网络实际输出：

```text
policy_head: hidden -> MAX_TILES_PER_SAT * act_dim
            = 256 -> 48
```

同时 Critic 输出一个 scalar value：

```text
value_head: hidden -> 1
```

## 8. 当前输入输出设计的优点

当前设计已经具备一个可训练 MARL 原型的基本形态：

- 每颗卫星作为独立 agent，观测是局部的，符合分布式调度设定。
- 使用固定长度向量，便于 MLP 直接训练。
- 邻居和 tile 都采用 slot 化设计，可以处理动态数量。
- 已经包含地面站窗口信息，这是卫星调度里非常关键的特征。
- 输出动作为一步决策，避免直接生成复杂多跳路径，动作空间可控。
- 所有卫星参数共享，有利于样本效率，也符合卫星角色同质化的假设。

## 9. 当前主要风险

### 9.1 动作 mask 不完整

当前只对无效 tile slot 做 mask，没有对无效动作做 mask。

例如：

- 当前卫星只有 2 个邻居，但动作空间仍然允许选择邻居 slot 7。
- 当前不可见某地面站，但动作空间仍然允许上传到该地面站。
- 邻居内存不足时，动作空间仍然允许卸载给该邻居。

这些动作最后可能被 decode 成 `WAIT`，或在环境中失败。训练时会浪费大量探索样本。

建议增加 action mask，让不可行动作在采样前就被屏蔽。

### 9.2 每星候选 tile 数偏小

当前 `MAX_TILES_PER_SAT = 4`，而一个任务有 32 个 tile。启发式策略会遍历所有可调度 tile，但 MARL 每步只能处理每星前 4 个 tile。

这会带来两个问题：

- 训练策略的调度能力弱于启发式策略，比较时不完全公平。
- 模型可能无法有效感知 task 级完成进度。

建议训练版至少比较 `K=4/8/16`，其中 `K=8` 是较好的起点。

### 9.3 任务级目标表达不够强

当前 reward 里 tile 完成给 `+1`，task 完成给 `+10`。这能提供密集奖励，但可能让模型偏向“完成零散 tile”，而不是“集中完成完整 task”。

当前 observation 也缺少强任务级摘要。例如：

- 该 task 已完成多少 tile。
- 该 task 还差多少 tile 完成。
- 该 task 的剩余 tile 分散在哪些位置。
- 该 tile 是否属于接近完成的 task。

如果目标指标是 task completion rate，后续应加强这部分。

### 9.4 邻居排序规则偏简单

当前邻居按 ISL 带宽降序排序。带宽重要，但调度可行性还取决于：

- 邻居队列长度。
- 邻居显存/内存。
- 邻居是否很快可见地面站。
- 邻居当前是否正在执行长任务。

建议后续可以改为按“候选邻居可行性 score”排序，或保留带宽排序但增加更完整特征。

## 10. 建议的下一版输入设计

在保持 MLP + slot 结构不变的前提下，建议下一版做最小增强：

### 10.1 自身状态增强

保留当前 5 维，新增：

| 新特征 | 维度 | 说明 |
|---|---:|---|
| `compute_idle` | 1 | 本星是否空闲 |
| `current_window_remaining_norm` | 1 | 当前地面站可见窗口剩余时间 |

### 10.2 地面站特征增强

每个地面站从 4 维扩到 6 或 7 维：

| 特征 | 维度 |
|---|---:|
| `visible` | 1 |
| `window_starts_norm` | 1 |
| `window_dur_norm` | 1 |
| `window_remaining_norm` | 1 |
| `gs_bw_norm` | 1 |
| `gs_queue_norm` | 1 |
| `gs_running` | 1 |

### 10.3 邻居特征增强

每个邻居从 6 维扩到 9 或 10 维：

| 特征 | 维度 |
|---|---:|
| `valid` | 1 |
| `isl_bw_norm` | 1 |
| `nb_queue_norm` | 1 |
| `nb_mem_norm` | 1 |
| `nb_vram_norm` | 1 |
| `nb_executing_remaining` | 1 |
| `nb_gs_visible` | 1 |
| `nb_window_min_start_norm` | 1 |
| `nb_window_min_duration_norm` | 1 |

### 10.4 Tile 特征增强

每个 tile 从 7 维扩到 10 或 12 维：

| 特征 | 维度 |
|---|---:|
| `has_tile` | 1 |
| `data_size_norm` | 1 |
| `compute_cost_norm` | 1 |
| `vram_req_norm` | 1 |
| `waiting_norm` | 1 |
| `deadline_remaining_norm` | 1 |
| `task_pending_norm` | 1 |
| `task_completed_norm` | 1 |
| `task_on_self_norm` | 1 |
| `task_near_done` | 1 |

其中 `task_near_done` 可以表达“该任务是否接近完成”，帮助模型更重视完整 task。

## 11. 建议的下一版输出设计

输出仍建议采用当前结构：

```text
每颗卫星每 step 对前 K 个候选 tile 同时输出动作。
```

但建议做两点调整：

### 11.1 将 K 从 4 提升到 8

建议先使用：

```text
MAX_TILES_PER_SAT = 8
```

原因：

- 仍然保持动作空间可控。
- 比 4 更接近实际调度负载。
- 对 32 tile/task 的任务结构更友好。

默认动作数仍为 12 时，policy 输出维度变为：

```text
8 * 12 = 96
```

这对当前 MLP 来说仍然很轻。

### 11.2 增加 action mask

每个 tile slot 应该有一个对应的动作可行性 mask：

```text
action_mask shape = (MAX_TILES_PER_SAT, act_dim)
```

建议 mask 掉：

- 无效 tile slot 的全部动作。
- 不存在的邻居 slot。
- 当前链路不可用的邻居。
- 目标邻居内存不足。
- 不存在的地面站 slot。
- 当前不可见且 pipeline 不支持等待上传启动的地面站动作。

这样 PPO 采样时只会在可行动作中探索，训练效率会明显提高。

## 12. 一个推荐的近期版本

如果先做一个不太激进、容易实现和验证的版本，建议：

```text
MAX_TILES_PER_SAT = 8
MAX_NEIGHBORS = 8
num_gs = 2
```

输入维度可以先控制在约 130-150 维：

```text
self:      6-7 维
gs:        2 * 6 = 12 维
neighbor:  8 * 9 = 72 维
tile:      8 * 8 = 64 维
合计:      154 左右
```

输出维度：

```text
per tile act_dim = 12
policy logits = 8 * 12 = 96
value = 1
```

这个规模对 MLP 来说仍然非常小，训练资源主要仍然消耗在仿真环境，而不是神经网络。

## 13. 结论

当前 MARL 输入输出设计方向是合理的：每颗卫星基于局部信息，对本地候选 tile 做一步离散调度决策。它已经足够支撑 smoke test 和小规模训练。

但如果目标是认真训练并尝试超过 `greedy/window_aware`，建议优先改三件事：

1. 增加 action mask，减少无效探索。
2. 将 `MAX_TILES_PER_SAT` 从 4 提升到 8，并比较 4/8/16 的效果。
3. 增强任务级特征和奖励，让模型学会完成完整 task，而不只是完成零散 tile。

完成这三步后，再讨论 PPO 训练稳定性、GAE 计算和资源投入，会更有意义。
