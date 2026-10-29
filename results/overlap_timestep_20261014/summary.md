# Overlap Timestep Sensitivity

one selected development case; timestep sensitivity, not statistical robustness

| Method | dt (s) | Age score | Completed / arrived | Restricted latency (s) | Actual ground Mbps | ISL sent (MB) |
|---|---:|---:|---:|---:|---:|---:|
| ground_compute | 1 | 0.347619 | 71 / 105 | 9146.581 | 2.580734 | 0.000000 |
| local | 1 | 0.859524 | 105 / 105 | 1841.076 | 0.000291 | 1.665048 |
| greedy | 1 | 0.854762 | 105 / 105 | 1888.210 | 0.000291 | 11326.122784 |
| cgs_compute_adapted | 1 | 0.859524 | 105 / 105 | 1841.076 | 0.000291 | 26.829376 |
| ground_compute | 0.5 | 0.347619 | 71 / 105 | 9146.000 | 2.580734 | 0.000000 |
| local | 0.5 | 0.859524 | 105 / 105 | 1839.810 | 0.000291 | 1.662804 |
| greedy | 0.5 | 0.854762 | 105 / 105 | 1886.895 | 0.000291 | 11326.122784 |
| cgs_compute_adapted | 0.5 | 0.859524 | 105 / 105 | 1839.805 | 0.000291 | 33.118214 |
| ground_compute | 0.25 | 0.347619 | 71 / 105 | 9145.750 | 2.580734 | 0.000000 |
| local | 0.25 | 0.859524 | 105 / 105 | 1839.176 | 0.000291 | 1.661308 |
| greedy | 0.25 | 0.854762 | 105 / 105 | 1886.245 | 0.000291 | 11326.122784 |
| cgs_compute_adapted | 0.25 | 0.859524 | 105 / 105 | 1839.181 | 0.000291 | 196.706916 |

Every run has the identical frozen physical task trace and sampled physical link context.

- Finest dt is a numerical reference, not an exact continuous-time solution.
- Ranking stability does not establish accuracy of individual service times.
- Compute timestamps, transfer completion, ground arbitration and fixed merge delay remain quantized.
- Actual link throughput counts transmitted bytes including relay hops, not unique useful payload.
- Network nominal capacity counters are not used as visibility-conditioned capacity evidence.
- Common-completion latency retains success-subset bias; complete-population age metrics are primary.
- Native task_trace_sha256 includes step indices; cross-dt equality uses physical_task_trace_sha256.

## 核验范围与口径

本次仅重放 `validation_near_high_1` 已生成并冻结的 105 个任务到达事件，覆盖 4 种方法和 3 个时间步，共 12 次运行。任务到达时间、源星、窗口几何、计算量、结果量和轨道文件完全相同；每 10 秒采样的全部 ISL 状态/带宽及源星地面站可见性记录一致。物理决策周期为 1 秒，轨道与地面可见性更新为 10 秒，接触预测扫描为 30 秒，带宽正弦变化周期为 600 秒。0.25 秒仅为本次更细的数值参考，未检验 10 秒轨道观测和 30 秒接触扫描本身的误差，不能视为连续时间真值或对所有场景的验证。

年龄评分 J 为 0.5、1、2、4 小时完整任务交付率的等权平均；受限平均时延以 4 小时截断、以全部 105 个任务为分母。完成数统计整个 6 小时仿真，任务只在前 2 小时到达。实际 Mbps 的分母为整个 6 小时（含排空期），ISL MB 逐跳计量，不等于唯一有效载荷或可见窗口利用率。四种方法的 1 秒结果与既有验证基线的完整 `task_outcomes`、`network` 和原生任务轨迹哈希逐项一致。

## 数值稳定性分层

| 方法 | 1 s 相对 0.25 s 的受限均值差 (s) | 共同完成任务最大时延差 (s) | 共同完成任务最大相对时延差 | 1 s ISL MB 相对 0.25 s 的偏差 |
|---|---:|---:|---:|---:|
| ground_compute | 0.830952 | 2.25 | 0.6258% | 不适用，两者为零 |
| local | 1.900000 | 3.75 | 71.4286% | +0.2251% |
| greedy | 1.964286 | 3.75 | 28.0000% | 约 0，浮点累加量级 |
| cgs_compute_adapted | 1.895238 | 3.75 | 71.4286% | -86.3607% |

三种步长下，四种方法的 J、最终完整任务数和每方法完成任务集合均相同。ground_compute 为 11.833333 个完整任务/小时，其余三种方法为 17.5 个/小时；后三者相对地面完成吞吐比均为 1.478873，跨步长偏差为零。这是当前 6 小时有限批次的吞吐，不是稳态容量证明。

小时级 J 的稳定不等于秒级时延收敛。以 local 的 `task_24` 为例，1、0.5、0.25 秒步长的时延分别为 9.00、6.50、5.25 秒；1 秒对更细参考仍高 71.43%，0.5 秒仍高 23.81%。总体受限均值差虽不超过 1.965 秒，但不能据此把短任务服务时间当作准确值。共同完成时延只比较成功子集，ground_compute 的 34 项未完成任务仍通过主年龄指标和受限均值计入。

CGS 的流量敏感性应独立保留：ISL 实传量随步长从 1 秒降至 0.5、0.25 秒，分别为 26.829376、33.118214、196.706916 MB，最细步长是 1 秒的 7.3318 倍。该差异发生在多条源星到邻星链路，显著大于浮点误差；具体边差见 `cgs_finest_traffic_diagnostic.json`。现有普通卸载路径未填充 `raw_relay_path`，因此 `raw_relay_hops=0` 不表示没有原始分片卸载。没有逐分片传输日志，无法精确确认每项额外载荷对应的任务及决策触发原因。仅能确认动作与通信资源使用尚未收敛，不能把完整任务吞吐稳定外推为流量收敛。

## 论文可用表述

在近邻协同、高到达率的一个开发场景中，本文固定 105 个任务的到达时间、源星及物理链路环境，对 1、0.5 和 0.25 秒仿真步长进行配对比较。四种方法的任务年龄评分和最终完整任务数均保持一致；以 0.25 秒为数值参考，1 秒步长的受限平均时延偏差不超过 1.965 秒。这一结果支持该场景中小时尺度交付指标对步长的稳定性，但不构成秒级时延或资源动作收敛的证据：本地处理的个别短任务时延相对偏差达到 71.43%，CGS 的星间流量则由 26.829376 MB 增至 196.706916 MB。因而后续涉及秒级响应、能耗或通信开销的结论仍需更细时间步与逐分片事件记录验证；本次结果亦不能外推到尚未开展时间步检验的其他场景。

以上中文解释和诊断索引为运行完成后依据原始记录追加，原始逐次结果及其源码哈希保持不变。
