# CGS报价差距接续（2026-11-11）

## 最终状态：完成，等待提交交付后停止

北京时间11-12 00:21:26报告和审计已自动完成。3重放、146枚举分片全部PASS，500498报价；98有效图，1原CGS无有限报价排除不替补，超上限0，不触发局部搜索。均值改善0.602041秒，中位数0.05秒，P90 1.3秒，最大5.3秒，仅报价搜索结果，16块非完整分配最优。当前回合已复核1519/1649源输入hash及8报告hash。详见REPORT.md、DELIVERY.md。下文运行中状态均为历史，不恢复任何worker，不再发布任务。提交推送后等待用户确认。

最新：新增持久接续入口 `analysis.continue_cgs_gap_20261111` 已启动，身份见 `continuation/LAUNCH.json`，每30秒检查。它等待原重放3PASS，自动做最终pilot/分片prepare/墙钟估计/三机部署核验/唯一发布/四机6槽启动/原字节收集/报告审计。状态见 `continuation/status.json`；异常写 `continuation/FAILURE.json` 后停止，不自动重启任务。不要手工并行调用prepare/publish/worker。完成输出 `REPORT_COMPLETE_PENDING_REVIEW_AND_PUSH` 时由当前任务核验、扫描、提交推送，不能把“已报告”误当“已推送”。

全套最终测试837 PASS（54.17秒），`FINAL_ENUM_TESTS_PASS.json`。三机1519hash及固定64报价一致性均PASS、最大误差0；同样16块快照单报价Mac0.084秒，devbox1/2约0.324/0.328秒，devbox3约0.183秒。估计以实际全候选量为准，尚不能无条件保证早晨完成。用户已收到超过2小时及分片方案告知。当前重放约三分之一，三进程健康，无科学失败。

当前用户授权本分析完成、审计、提交推送origin/master后停止，不开最终测试、不训练、不改论文、不删文件。用户不准备盯夜间运行，要求持续跟进至交付；当前任务内监测，不创建跟踪具体作业的自动化。

## 已启动

- M验证10-22～24三天同版本确定性重放，Mac3槽，HTTP实验`cgs_gap_replay_20261111`。入口`analysis.run_cgs_gap_20261111`；REGISTRATION SHA256 `f38fdb5f6c520021ed2eda949344e230e87b6f08e3c08a7661ac32a01fc0b706`，1519项hash。worker48621/caffeinate48622，身份以replay/LAUNCH和workers/local/launch核对，不重启。
- 744幅原CGS协同选择，按日期×9/64/100窗口九层各11幅，seed20261111，固定99样本；实际4块38幅、16块61幅。SAMPLES已冻结，不替补。源代码/原教师/Calendar不改。819全套测试PASS，后续分片17聚焦测试PASS；正式任务尚无失败。
- `analysis.cgs_gap_capture_20261111`保留原NonuniformThreePlan.select_actions代码对象，仅观察候选第2/4次Calendar.quote调用前真实state和资源日历。须全部动作hash、决策、结果、资源等与本日Gate1 baseline相同才能用于枚举。快照在snapshots，捕获进度capture_progress，重放进度replay/full_run/validation。

## 枚举尚未发布

- 原docs/thesis/CGS_GAP_PREREGISTRATION_20261111.md及补充CGS_GAP_SHARDING_20261111.md。16块只枚举计数向量、块编号递增连续映射，不是全分配最优。CGS作为incumbent，纯枚举最优另列。
- 单块最早报价排名前5，保留CGS所有位置；并集上限8优先保留CGS。CGS实际超过8按用户确认标OVER_CAP、不替补，报告总体及按窗口大小数量比例。若超过99样本10%（至少10幅），所有超上限图另做20轮上限移动/交换最陡改进搜索，单列改进下限，不与枚举合并。
- 首批16块报价约0.070～0.084秒/次；8位置245157组合单进程约5小时。已告知用户可能超过2小时，不承诺明早无条件完成。为避免拖尾，枚举按全局索引每5000向量一段，四机24槽（Mac6、各devbox6）共享HTTP队列。科学空间不变。用户不加机器。
- 分片入口`analysis.run_cgs_gap_shards_20261111`，prepare在完整重放PASS后计算候选并冻结enum登记。不要调用旧非分片`plan_cgs_gap_20261111 prepare`或旧run_cgs_gap enumeration worker。`plan_cgs_gap_20261111 pilot`可继续做预先规定的九层最早样本64均匀索引计时；PILOT_REG已冻结该源码，不修改。
- 三机SSH已通，Python3.12.13、各8CPU、当时空闲。用户已允许本项目SSH/kinit。token路径私密，不打印内容；队列仍devbox3:18765。新增分片结果内嵌原raw gzip base64，由服务器保存原字节，避免必须逐机取raw。

## 下一步

1. 完成报告器/独立审计和相关测试；重放当前源已冻结不能改。新分片及报告源在枚举登记前可完成并冻结。
2. 用replay manifest文件列表（绝对路径转repo相对路径）加新源/首次16块快照/对应timing记录，rsync到devbox1/2/3同一真实根`/Users/bytedance/Projects/bs`，不删远端文件、不碰.venv；`analysis.preflight_cgs_gap_20261111 <timing路径>`逐机验证1519hash及同快照64报价、记录耗时/平台。
3. 等三天重放全部PASS、final COMPLETE；运行新分片prepare计算所有候选，保存CANDIDATES和enumeration manifest/REGISTRATION。汇报实际候选总量和四机实测墙钟预估。部署最终所有hash文件、候选/快照/登记，逐机verify。prepare会保存replay/QUEUE_FINAL。
4. 只发布一次新分片实验`cgs_gap_enumeration_20261111`，再启动4个新分片worker（env Torch/BLAS1），Mac caffeinate。失败停止新领取，不重试、不回收租约。跨架构全空间逐位一致未证明，须披露。
5. 全部PASS后用`analysis.collect_cgs_gap_20261111`只读取回服务器所有主结果和内嵌raw，核验唯一领取/完成和hash；该collector只在最终完成时调用一次。暂断不当科学失败。
6. 生成REPORT/每图/按窗口和实际块分组/超上限/必要局部搜索单表；核验每图索引全覆盖、全部保存报价最小值和最好分配独立重报价、分组统计。主比较是Calendar预测，不是执行结果。
7. 记录实际墙钟，按用户find待提交>50MB、必要LFS、已知token精确匹配及模式扫描、内置扫描完整等待规则提交推送。当前本阶段新增文件均未提交。此前阶段HEAD 7af08c67已推送。最终报告commit/LFS/干净状态，停止。
