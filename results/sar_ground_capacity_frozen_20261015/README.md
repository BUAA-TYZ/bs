# SAR地面容量倍率冻结配置

2026-10-15根据用户要求固定：卫星使用Jetson实测，地面按可扩容资源池的有效服务倍数建模，无需另测某台服务器。本目录只含配置，没有调度实验结果。

主入口为`main_config.json`，beta=10。1、5、20、100倍敏感性配置分别为
`ground_x1.0_config.json`、`ground_x5.0_config.json`、`ground_x20.0_config.json`、
`ground_x100.0_config.json`。`manifest.json`记录参数依据、原始标定与负载、
源文件哈希和每份配置哈希。此前`sar_spotfine_frozen_20261015`不被覆盖。

| 参数 | 固定值与依据 |
| --- | --- |
| 星座 | 24颗SAR源星，570 km、97.7度、12面×2；22颗计算中继星，550 km、53度、2面×11 |
| 到达与时限 | 单日180任务、24个UTC小时均有任务，最长间隔2685 s；图像就绪后300 s内全结果及预览在地面聚合 |
| 图像与分片 | 20000² uint16、800 MB；4096窗、1792重叠、2304步长、64窗；每窗33.554432 MB |
| 星上处理 | 每窗1 U，0.574 s/窗，1.7421602787456447 U/s，源星与计算星同档；Jetson固定代理实测 |
| 地面处理 | beta=10，17.421602787456447 U/s；等效每窗0.0574 s、整图纯服务3.6736 s，属于资源供给假设 |
| 输出 | 元数据与完整ROI的512² uint8预览，均值0.86857875 MB/窗、55.58904 MB/图 |
| 星上内存 | 8 GB缓存+8 GB执行预算，共用32 GB级RAM；其余为系统及预留 |
| 地面内存 | 随资源配套供给、容量充足，不模拟有限内存准入或实际GPU分配 |
| 通信 | 星间10 Gbps；北京参考站星地1.2 Gbps、一个接收服务，不随beta变化 |
| 聚合 | 独立Jetson CPU代理0.25091760235 s/图，不随beta变化；dt=0.1时事件延时0.3 s |

`mu_g=beta/t_J`表示完整每窗流水线的等效吞吐，含CPU、设备传输和GPU阶段，
不是GPU频率或卡数。10倍是在查看调度效果之前固定的一个数量级资源供给
假设，不是由L4峰值计算得到。公开单卡L4实例仅作硬件背景，不绑定本资源池。
不同beta用于检查结论是否依赖地面容量，全部调度方法及纯地面基线必须共用
同一beta；不只汇报有利条件。100倍仍是有限容量，不称无限计算上界。

执行器是一个FIFO、工作守恒的等效服务节点，步内复用剩余预算。实际增加
独立GPU主要增加并发，不能据卡数断言单窗物理时延同比下降；本模型不模拟
独立GPU工作槽、批处理效率及单窗物理时延下限。内存充足也是显式供给假设，
不是已证明固定24 GB显存可容纳全部负载。地面聚合没有共享竞争队列。

23项配置与计算预算/规划器检查通过。所有五档与旧1倍配置逐字段比较，仅
地面计算速率变化；整图和分片地面路径分别核验64窗服务守恒和步末完成误差。
主档每窗57.4 ms、高容量档5.74 ms，计算完成和逐跳通信仍受dt=0.1事件量化。
正式算法排名前须做同轨迹更细步长核查；本轮没有进行该实验。

复算（目标目录必须不存在）：

```bash
.venv/bin/python analysis/build_sar_frozen_config.py \
  --calibration-report results/sar_spotfine_calibration_20261015/calibration_summary.json \
  --workload-fixture results/sar_spotfine_calibration_20261015/workload_fixture.json \
  --ground-rate-multiplier 10 --ground-rate-sensitivity 1 5 20 100 \
  --output-dir /tmp/sar_ground_capacity_recomputed
```

完整依据见[计算参数证据](../../docs/thesis/compute_calibration_evidence.md)第0.2.1与0.8节，
论文Markdown第5.23节及LaTeX实验设计章节已同步该口径。
