# 硕士论文资料入口

更新：2026-10-17。题目：面向遥感图像处理的多星协同任务调度技术研究。
本目录只维护当前协议、证据和必要的参数溯源，研究过程统一写入实验日志。

## 先读这些

1. [工作简报](../codex_brief.md)：推进约束、T1至T6和停止条件。
2. [当前SAR冻结清单](../../results/sar_loopfree_20261017/manifest.json)：180任务、46星、实测并发服务、16调度块/64推理窗、地面10倍、dt=0.01秒；中继取消固定8跳截断。
3. [最新开发对比](../../results/sar_loopfree_20261017/prefix_21600/REPORT.md)：42图新候选/本地34、CGS31、HEFT32、余量33、地面28；新候选R300较本地低9.05%，完成率仍持平。
4. [统一实验日志](experiment_log.md)：实验假设、结果、负结果和清理后的历史摘要。
5. [中期待办](midterm_todo.md)：论文证据缺口与交付标准。

用户已批准去除8跳截断，公共中继采用禁止回访的N-1安全界；新42图松弛上界38/42。
旧42图结果见[历史面板](../../results/sar_temporal_study_20261017/choice_prefix_21600/REPORT.md)，
不能将公共协议变化记作新算法贡献；新协议地面对照退步需如实保留并诊断。
[路由回放与局限](../../results/sar_loopfree_20261017/README.md)记录了逐包报价、共享竞争与保守窗口问题。
SAR主场景尚未训练MARL，验证/测试保持未运行；本轮不修改论文正文。
[历史串行主表](../../results/sar_baseline_b10_dt0p01_20261016/REPORT.md)不代表当前配置。
[旧三日T1](../../results/sla_frozen_t1_20261015/REPORT.md)及到达日历只属于历史，不与当前主表混用。

## 当前问题与协议

- [研究边界](research_scope.md)：任务、信息、方法与论文表述边界。
- [P300/R300协议](short_deadline_protocol.md)：所有任务入分母、超时继续竞争、配对和测试规则。
- [高分历史场景](highres_scene_protocol.md)：上游轨道、通信研究；当前SAR数值以冻结清单和主报告为准。
- [文献方法映射](literature_baselines.md)、[DTS映射](dts_tmax_mapping.md)、[候选文献审查](strong_baseline_candidates.md)：区分适配与完整复现。

## 参数与实现依据

- [通信规格](highres_link_evidence.md)、[图像与字节](image_workload_evidence.md)、[算力标定](compute_calibration_evidence.md)。
- [几何审计](highres_geometry_audit.md)、[轨道布局依据](orbit_architecture.md)、[共享路由修正](highres_routing_correction.md)。
- [结果中继实现](result_relay.md)、[原图地面中继](ground_input_relay_design.md)、[整图候选实现](task_delivery_method.md)。

以下保留的是当前参数的上游测量或历史核算依据，不是当前实验配置：
[固定overlap代理](fixed_overlap_protocol.md)、[测量与旧代理复现](overlap_reproduction.md)、
[窗口与调度包核算](overlap_workload_design.md)、[历史参数审计](parameter_decisions.md)。
旧代理结果直接见[原始汇总](../../results/overlap_proxy_study_20261014/summary.md)。

## 论文与材料

[论文工作稿](draft.md)已补入当前SAR参数与开发日结果，但实验章节仍含大量历史协议，
尚需整体统一贡献和主线，不能直接提交。学院模板和LaTeX材料位于本机
`/Users/bytedance/Downloads/中期/`；学籍、签名、原计划节点等需本人核对。

旧专题不再作为当前状态入口。清理摘要见[日志](experiment_log.md)，
原文可从[单一压缩快照](../../results/document_cleanup_20261015.zip)追溯；
原始结果与模型保留，清理没有重算或改写实验成绩。
