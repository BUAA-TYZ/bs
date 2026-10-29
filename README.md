# 面向遥感图像处理的多星协同任务调度仿真

## 当前状态

更新：2026-10-17。研究目标是在通信窗口、星上算力和缓存约束下，
调度高分辨率图像分片，经星上或地面计算后完成整图结果交付。
当前保留业务负载，重新登记链路基准与分片变量；尚无新算法稳定优于最强可比基线的证据，未开展新场景MARL训练。

- [工作简报](docs/codex_brief.md)：唯一研究推进约束、任务顺序和停止条件。
- [当前方法对比](results/sar_loopfree_20261017/prefix_21600/REPORT.md)：新协议42图，新候选/本地34、CGS31、HEFT32、余量33、地面28；新候选R300较本地低9.05%，完成率仍持平。
- [公共路由变化与诊断](results/sar_loopfree_20261017/README.md)：用户批准取消8跳截断，采用无回访的N-1安全界；公共收益与地面对照退步单列，不归为新算法贡献。
- [旧8跳方法对比](results/sar_temporal_study_20261017/choice_prefix_21600/REPORT.md)：历史同源码42图，新整图复核33、本地33、强地面31、CGS29、HEFT28；不作为新协议成绩。
- [历史机制消融与上界](results/sar_temporal_study_20261017/README.md)：输入竞争消融、公共窗口修复及旧有限跳数上界。
- [当前SAR配置](examples/config_sar_current.yaml)：Jetson并发实测后的46星开发场景，前端默认入口。
- [当前分片实现与实验](results/sar_dispatch_20261016/README.md)：16块调度、块内去重、空目标确认、短跑结果与限制。
- [链路参数约定](results/sar_parameter_contract_20261016/README.md)：固定算力、链路变量及工程边界，分片待接入的描述已被新实现替代。
- [配置证据清单](results/sar_loopfree_20261017/manifest.json)：当前配置哈希及上游配置来源，物理参数与合成负载保持不变。
- [硬件并发测量](results/jetson_concurrency_20261016/summary.json)：单流、batch=2/4、双流三轮对照及持续确认。
- [Profiling与参数依据](results/jetson_concurrency_20261016/README.md)：注意力热点、共享内存预算、频率观测及适用边界。
- [历史串行主基线](results/sar_baseline_b10_dt0p01_20261016/REPORT.md)：地面164/180、CGS159/180；不能移植为新并发配置成绩。
- [统一实验日志](docs/thesis/experiment_log.md)：假设、正负结果、停止决定及历史摘要。
- [论文资料入口](docs/thesis/README.md)：参数证据、文献映射、初稿和中期待办。

## 当前开发基准

24颗570 km SAR源星、22颗550 km计算/中继星，美国沿海五港口成像、北京地面接收。
开发日180图，单图800 MB；4096像素窗口、2304步长、1792重叠，共64窗，2x2组合为16个调度块。
块内原生像素只传一次，整图分片输入约1.288 GB；推理仍执行64窗，不把16块当成16次推理。
星间标称2.5 Gbps、星地标称1.2 Gbps，均按显式假设的80%载荷效率折算为2 Gbps和960 Mbps；
每星最多两路同时邻接。速率在已接通链路内固定，接触窗口与共享竞争随时间变化；不使用无依据的正弦衰减。
星上计算采用Jetson AGX Orin地面代理测量，保持单窗0.597秒、双窗组1.056秒。
星上按实测并发组服务时间推进；地面是单星饱和吞吐10倍的资源供给假设，不是服务器实测。

当前能力包含本地计算、最多两跳星上输入卸载、原图中继到地面和结果中继。
公共中继使用temporal路由，禁止回访已访问卫星；46星对应N-1=45次星间跳转安全界。
积分步长0.01秒，决策周期1秒，几何采样10秒，每源每轮最多64个候选包。
所有比较方法共享执行规则，超过300秒仍继续竞争资源。

主指标P300是全部释放图像中300秒内在地面完成聚合的比例；次指标R300为全任务
300秒截断平均时延。日期等权比例和汇总计数分别报告，未完成任务不能从分母删除。
当前并发参数见[测量记录](results/jetson_concurrency_20261016/README.md)；
[计算标定协议](docs/thesis/compute_calibration_evidence.md)保留前期串行口径，论文暂未同步。
64/16/4个调度块均可执行，内部按实测并发组推进；部分执行的块不再次迁移，整块完成后回传结果。
无目标窗口仅回28字节完成头，有目标回元数据和预览。主负载在合成100目标几何中抽取30个，
另设10%全空图概率；这是输出量假设，不是真实港口密度。实际输出量在计算完成前对调度器隐藏。

## 方法与实现

- `cgs_compute_adapted`：逐分片比较输入、排队计算和结果回地时刻，选预计最早交付的方案；非强化学习，也非原CGS完整复现。
- `cgs_task_choice`：整图尚未派发时，复核全本地与CGS分配，计入同轮输入竞争和实测并发组服务。当前研究成绩使用`relay_routing=temporal`；不是MARL，也未证明原创性。
- `heft_adapted`、`dts_tmax_hybrid`：文献规则的明确适配，差异见[文献映射](docs/thesis/literature_baselines.md)。
- `task_delivery`：历史整图试排候选，旧三日场景与CGS完成率持平；不是当前42图成绩。
- `arrival_calendar`：跨轮保留输入预计到达与计算承诺；仅有前两日结果，不能作完整排名。
- IPPO/MAPPO等学习代码仍可运行；旧checkpoint、旧小时级评分不能直接作为当前五分钟任务的成绩。

模块入口：`main.py`负责运行，`sim/env.py`维护生命周期，
`sim/pipeline/`执行实际传输和计算，`sim/scheduler/`产生调度动作，
`sim/marl/`和`train_marl.py`实现学习，`analysis/`负责配对评估与审计。

## 本地使用

现有`.venv`为Python 3.12；`.python-version`历史值为3.14，尚未迁移，优先使用已有环境。
核心依赖见`requirements.txt`，图像测量另用`requirements-workload.txt`。

```sh
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python main.py --help
.venv/bin/python main.py --config examples/config_smoke.yaml --policy window_aware --output /tmp/bs-smoke.json
```

smoke配置仅验证程序闭环，不代表主实验。正式SAR批量入口为`analysis/run_sar_frozen.py`。
批量入口默认读取`results/sar_loopfree_20261017/`；64/4块、全空与密集负载的历史派生配置
仍在`results/sar_dispatch_20261016/`，不能直接与新公共路由面板混比。
新协议13/42任务对照与180任务几何上界已完成；前缀不代表全天排名。
测试面板尚未启封，不能直接恢复此前84组压力矩阵。

## 仿真工作台

```sh
.venv/bin/python -m workbench.server --port 8765
```

地址为http://127.0.0.1:8765/，支持单次运行、取消、过程回放和同配置对比，输出位于`runs/`。
策略列表来自`main.POLICY_NAMES`；地图默认使用Cesium CDN，可配置`WORKBENCH_CESIUM_URL`。
回放中点击详情面板的实体按钮会把视角直接切到该实体上方；点击地球只选中不移动，双击跟随。
卫星按配置分类显示：`task_source_sat_ids`内为任务源星（SAR载荷标`SAR{id}`，其余载荷标`IMG{id}`，未设置时全部为任务源），
其余为计算/中继星`C{id}`；编号即仿真`sat_id`，与配置和事件日志一致。
工作台的最终完成率和成功任务时延不是论文P300/R300，正式主表须用固定评分脚本。

## 证据边界

仿真按测得的服务时间与字节代理推进，不逐任务运行检测网络。
尚未建模控制广播代价、激光建链、能耗、辐射、随机故障或执行状态迁移；
地面聚合为固定无竞争延迟。标称带宽、同机算力代理与真实在轨性能须区分。

旧抽象任务、角色训练和overlap代理实验的原始`results/`、`checkpoints/`保留。
已过时专题已从文档目录移除，摘要统一进入实验日志，清理前原文见
[文档快照](results/document_cleanup_20261015.zip)。6个无代码依赖的旧探索配置已从examples
删除，恢复副本见[旧配置快照](results/obsolete_example_configs_20261016.zip)。仍被测试与复现
脚本使用的旧配置保留，历史测量与实验结果快照不删除。
