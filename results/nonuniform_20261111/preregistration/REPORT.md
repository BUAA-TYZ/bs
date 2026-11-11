# 阶段〇：只读接口调研与预登记

日期：2026-11-11。范围：现有源码/日期/历史验证证据检查，新增预登记和本报告；未实现研究算法，未清理文件，未生成新场景，未训练或运行新仿真。

## 结论

现有执行器可以按窗口累计服务并携带块内窗口坐标，但现有layout只支持规则网格，不支持任意分组。扩展至少涉及工作量描述、preview/repack、原图分包解耦、教师候选/日历核价、学习观测与动作统计五个边界，不能仅增加一个requested_num_tiles取值。
三验证日全局联合教师历史R600=70.64 s、P60=77.83%、P600=96.35%，已定位原报告及独立审计。该数值是关卡1历史对照参考，不是本轮新增测量。
新测试登记2026-12-28/29/30；已有文本日期最晚12-23，分别间隔5/6/7天。15289个已有文本文件完成日期与内容摘要核查，候选日期命中0；新增测试开启0、场景生成0。只证明仓库记录范围内的未使用，不外推未记录的实验。
用户已确认训练10-16～21、验证10-22～24，后者不参与梯度更新。种子1701/1702/1703，关卡标准和一次性测试要求已写入预登记。

## 当前接口与必要改动

|接口|已核实行为|任意分组需要的改动|
|---|---|---|
|sim/workload.py: ImageTiling.tiles / ImageTile|requested_num_tiles只接受1/4/16/64；选不超过请求且能容纳的q×q网格，按行列连续分窗；ImageTile已有windows_xyxy/输入/计算/结果量|增加显式窗口ID到块ID映射和合法性检查；保留旧路径；输出稳定的非空块描述，最多16块|
|sim/workload.py: 去重输入|规则矩形窗组用包围框，因连续网格才等价并集；原生分量向外映射后计字节|对任意组求真实矩形并集；不能把蛇形跨行产生的空洞计入；分量级去重、位打包、压缩口径必须一致|
|sim/workload.py: unique_transport_blocks|独立q×q原图像元分区；要求块数正平方数且总字节严格等于整图|与NU计算块分离；G保持旧唯一像素分区，不将任意k传进此函数，也不混淆窗口重叠与原图唯一字节|
|analysis/dynamic_granularity.py: layout|replace(requested_num_tiles)后检查窗口Counter与总计算；逐窗真实输出按(y,x)排序绑定坐标|增加grouping输入/版本及映射摘要；加强重复/漏窗/非法坐标/字节/计算守恒；结果真值只保留在执行状态|
|preview|构造候选tile公开字段、sibling/progress、源星队长和内存delta；调用unique_transport_blocks(n)|去除平方块数耦合，显式区分计算布局和G；与commit字段一致，不能就地更改真实env或暴露真实逐窗输出|
|GranularityEnv.repack|整图未承诺且全在源星时原子替换tiles、队列、task索引和内存，记录requested/actual|以完整mapping而非requested整数识别幂等；同数量不同分组不能误判相同；维护全部索引/指标/承诺，增加接收与中继输入配额核验|
|DynamicThreePlan / v2|枚举L4/L16/C4/C16/G，日历副本报价，CGSTaskChoice再复核，胜者commit；v2仅显式固定同分顺序|新增C_nu独立模块/候选，份额估计与真实共享日历核价分开；新候选同分放末尾；保存全部预测、份额与可行性失败证据|
|sar_three_plan.Calendar.quote|按tile_id完整覆盖检查，输入路径/接触volume/free、FIFO busy、内存、结果回传共同报价；不要求块数为平方数|可复用核价核心，但必须传正确块描述和动作；防止方案比较间预留污染；保持当前近似口径，不顺便修改旧教师|
|sar_three_plan.ThreePlanChoice / CandidatePipeline|原图RawImageAction触发唯一像素输入替换、到齐后地面处理；否则按块执行|G在选择前保留规则布局，禁止NU提交后再偷换G；继续唯一原图字节核验及整图合并|
|dynamic_learning_interface|图像头3模式4/16/G，两个preview；每块Categorical采样，logp逐块求和；软标签以窗口权重；repack固定4/16|NU用2模式图像头、位置计数Multinomial及组合项、确定性最大余数评估；新schema/缓冲/PPO统计/标签与commit记录，旧F/D保持冻结|
|main_bc_interface / main_rl_interface|K=8计算候选，加本地和单站地面共10位置；根据当前块大小建立可行掩码；ground_id单站断言|份额大小影响可达性和内存，不能用固定16块的掩码直接覆盖NU；仍沿局部2跳真实通告权限，记录候选截断与未表示教师比例|
|sim/env.py / sim/pipeline/distributed.py|实际结果按windows_xyxy汇总；实测profile按整数窗口服务；部分处理块不重新开放调度|验证任意块含窗数与服务一致、结果缓冲/内存/到齐一致；不改变任务真值、批服务率或将块数当窗口数|

现有测试入口可扩展：tests/test_dynamic_granularity.py、test_dynamic_granularity_v2.py、test_dynamic_learning_interface.py、test_workload_highres_splits.py、test_workload_overlap.py。已有测试覆盖几何/内存/preview一致性、原图总量、承诺后拒绝重打包和真实结果不可影响报价。任意非矩形并集、1～16非平方块数、重复/漏窗、同块数不同映射、异构份额和Multinomial统计需新增独立测试。此阶段没有把既有测试通过记录当成新功能验证。

## 不变量与实施风险

- 窗口唯一归属、计算量守恒、真实结果按坐标绑定、块内并集去重四项是硬约束；结果代理与执行真值不可混用。
- 任意分组不是改变成像窗口，只改变调度/传输打包。单块输入缓冲配额与单窗显存是不同资源。
- 独立目标份额会忽略共享接触竞争；二分估计需要离散可行性/单调性核查，再由共享日历检验，不构成最优性证明。
- 采样后裁剪份额会改变策略分布，不能继续用裁剪前logp训练；这一点须在学习实现登记中明确处理。
- 单站M是本次研究主口径，不将旧扩站报价近似问题混入新方法比较。连续合成轨道传播不证明真实远期轨道精度。

## 产物与停点

预登记：docs/thesis/NONUNIFORM_PREREGISTRATION_20261111.md。
日期核查及关键来源哈希：本目录DATE_AUDIT.json；登记摘要：REGISTRATION.json。
第一笔提交同时归档现有未跟踪analysis/tests/results产物，保留旧失败和交接证据；唯一新增忽略项为178553234字节的可重建input_bundle.tar.gz，不删除本地包。
提交前大文件与凭据核查记录见SUBMISSION_CHECK.json。当前仅文档/登记及归档，没有运行pytest或冒烟；用户要求的清理后完整pytest和M验证日冒烟留在下一阶段。
完成本阶段push后停止。下一阶段须用户确认，先确保快照并创建推送pre-cleanup-20261111，再做引用清单和清理；本阶段没有创建该标签或删除文件。
