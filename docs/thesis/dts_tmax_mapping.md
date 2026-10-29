# DTS-TMAX 一跳选择规则适配

日期：2026-10-15。方法名称为 **DTS-TMAX one-hop adapted**，计划注册名为 `dts_tmax_onehop`。它是公开选择规则的适配，不是原论文实验复现，也不是 OrbitAware 或动态优先级队列的复现。

**冻结SAR配置下的名称边界：** `main.make_policy` 中 `dts_tmax_onehop` 与 `dts_tmax_path` 构造同一个 `DTSTMAXOneHopAdapted()`。当 `input_relay_max_hops>1` 或 `ground_input_relay=true` 时，候选来自公共 `current_input_options`，会产生两跳星上输入路径，因此“onehop”只是历史名称，不代表严格单跳。`dts_tmax_hybrid` 另加 `include_ground=True`，允许地面候选与原图地面中继。别名关系由 `tests/test_input_route_planners.py::test_dts_onehop_name_is_two_hop_alias_under_frozen_relay_config` 固定。SAR 主基线只使用 `dts_tmax_hybrid`。

## 原文依据

来源：[Magliarisi et al., Orbit-aware task scheduling in satellite edge computing, Cluster Computing, 2025](https://link.springer.com/article/10.1007/s10586-025-05663-9)。已核验第3.5、5.2、5.3、5.4、6.2、7.1节及式(3)-(10)。

式(5)以服务时间和双优先级 M/M/1 等待估计构成响应时间；式(10)在远程候选上增加输入传输及链路传播时延。第5.2节的 SearchNode 先筛选资源和期限可行节点，再选择响应估计最小者。无可行节点时，原实现随机传播搜索并扣减传输消耗的剩余期限。第7.1节将离开用户可视范围前完成、但已经超过请求期限的任务仍计为成功。因此原成功率不同于本项目的整图 `P_300`。

## 本项目接口与公式

实现位置：`sim/scheduler/dts_tmax.py`。

```python
DTSTMAXOneHopAdapted(deadline_s=300.0, continue_late=True)
policy.select_actions(state)  # list[Action]
```

候选集合只包含源星和公共 `candidate_neighbors` 给出的当前邻星；不加入可见地面计算候选。对于分片 r 和候选卫星 j：

\[
\widehat R_{r,i,j}=\widehat W_j+\widehat c_{r,j}+\widehat T_{r,i,j}.
\]

- `W` 使用公开的 `committed_compute_s`，包含接收方已接单的在途输入。原文稳态 M/M/1 公式不直接适用于同一影像瞬时产生的数百个分片；本项目不虚构泊松到达率或优先级利用率。
- `c` 使用分片工作量除以候选算力，服从公共计算服务模式。工作守恒模式保留实际秒数；旧模式按 `dt` 取整。
- 本地 `T=0`；邻星 `T` 为原始分片字节数除以当前链路带宽，按公共步长至少占一个时间步，再加公开的 `latency_ms/1000`。保留原式加法，没有引入排队与输入并行的额外修正。当前执行器未单独推进毫秒级传播事件，此差异也存在于其他使用标称链路时延的估计方法。
- 资源筛选检查显存容量、接收方缓存、链路连通与正带宽。同轮连续接单后更新局部的计算承诺和接收缓存；不修改环境状态。
- 不预测结果回传、未来接触、整图尾片或整图联合接纳。实际结果仍通过公共中继执行器交付，按完整图像的统一指标计分。

## 期限与执行边界

软时限剩余量为 `deadline_s - waiting_time`，其中 `waiting_time` 来自分片原始生成时刻，跨调度轮不会重新获得300秒。环境已经提供硬期限时，先按 `deadline_remaining_s` 筛选；任何软时限延续都不放宽硬期限。

默认先在软期限可行候选中选取最小响应；没有这样的候选时，`continue_late=True` 在仍满足硬期限和资源条件的节点中继续选最小响应，并增加 `stats["late_admission"]`。这是公共“迟到仍继续执行”协议下的显式适配，不是原论文的随机传播搜索。若完全没有资源或硬期限可行节点，动作仍为 WAIT。

**这项延续不会带来一个新的优化目标。** 同一分片的所有候选使用同一个软截止阈值，且排序目标本来就是最小响应，所以默认模式和 `deadline_s=None` 在目的地选择上等价；软期限只增加按时/迟到接纳统计。不得将其宣称为期限优化贡献。`continue_late=False` 保留严格的一跳期限筛选供诊断，但在公共候选上无可行解时会等待，并可能阻塞后续候选，因此不以该严格退化版本替代主对照。

当前 `execution_commitment` 在原始输入到达后承担计算，不支持原文的逐跳继续搜索及再卸载；本实现不会把原始数据随机发送到无法接单的节点来假装实现传播。READY 任务保持 LOCAL，正在传输的分片不重复调度，失败任务不再接单。默认按等待时间、分片编号稳定排序，响应相同时优先本地，其后按卫星编号破同分。

原文动态双优先级队列、地面 AP 选择、OrbitAware 的用户可视域退出筛选均未实现。本研究允许结果中继、任务来自成像星，也没有同一地面用户的监听球模型，故不将源星当前无法看到北京地面站作为 OrbitAware 基线的失败条件。

## 验证范围

`tests/test_dts_tmax.py` 的14项测试检查本地/邻星选择、输入与传播开销、连续计算服务、链路共享、已承诺工作量、资源竞争、绝对任务年龄、硬期限、迟到延续与严格诊断分支、同分顺序、状态不变性及公共执行器合法动作。2026-10-15 已全部通过。测试是选择规则和适配契约的验证，不代表复现原论文数值。
