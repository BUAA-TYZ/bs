# 局部状态通告的实际实现

结论：**仿真中的显式链路传输，不是决策时直接读取邻居最新动态再按TTL过滤。** 本轮仅核实并记录，不修改实现。

1. `sim/control.py:ControlPlane.enqueue` 每5模拟秒采集一次计算星的队列、内存、积压等状态，制作面向各源星的通告。通告大小为payload实际JSON序列化字节数。仿真器在这里读取发送者当前状态用于生成报文，不直接把这些值提供给接收方actor。
2. 用当时源星到计算星的至多两跳路径反向发送；不会让其他SAR源星中继。每条通告逐跳保留remaining_mb、创建时间和下一跳可发送步。无广播免费共享，同一发送星给不同源星的报文分别发送。
3. `sim/pipeline/distributed.py:DistributedPipeline.advance_transfers` 将通告和业务传输加入同一有向链路的`fair_bytes`带宽分配。通告字节占用ISL容量，并计入network_sent_mb；与业务竞争。
4. 只有up链路推进通告字节。断链时通告暂停在当前路径上，等待恢复，不主动重路由；创建后超过15秒则在途过期移除。不能将它描述为必达或瞬时广播。
5. 每跳传完最早下一模拟步继续转发/送达。最后一跳完成才写入接收方delivered缓存；`received_s=(step+1)*dt`。因此存在串行化、带宽竞争、断链等待和逐跳步进延迟。**控制报文没有额外应用`latency_ms`传播时延**，也没有完整MAC/ACK/重传协议；这是现有链路仿真抽象，不是硬件网络测量。
6. `analysis/local_information_baselines.py:LocalInformationPlane.records/view` 仅采用已送达、创建后年龄不超过15秒、发送路径不超过k跳且当前仍处于k跳范围内的远端通告。自身动态即时可见；未知计算节点使用明示零负载先验（history变体才用该源星合法历史均值）；地面队列没有通告，使用零负载先验。actor本轮k=2、zero估计。
7. 全网轨道、接触计划、可见窗口和链路静态属性按获批口径免费可知；这与远端计算队列等动态通告是两条不同的信息路径。集中critic读取全局状态，但不把这些输入交给actor。

建议论文表述：“局部动态观测由共享ISL带宽的显式状态通告提供，按已送达缓存及15秒生成时间TTL使用；链路中断可使通告延迟或失效。控制报文延迟包含传输及离散逐跳推进，未单独建模传播时延与可靠传输协议。全网静态接触计划视为先验可知。”

本轮运行入口`analysis/run_main_rl_stage1.py`为reference安装ControlPlane，为teacher/student安装其子类Plane；默认estimator=zero只改变视图，不改变enqueue/传输。reference与teacher动作/执行轨迹一致性由正式实测单独核验。
