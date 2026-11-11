# 地面扩站代表基线补跑接续

## 2026-10-31 23:35 北京时间：45项及报告审计完成，交付并停止

全部45项PASS，四机worker/监督及Mac coordinator均已正常退出，不恢复。15:32:41Z的REPORT_COMPLETE为PASS；独立审计372独立记录、432逻辑行、2310区间、2656来源hash通过。交付时再次核对6报告文件hash、HASHES/INDEPENDENT_AUDIT与完成标记一致，以及45项唯一job_id/原字节结果hash；无需重复报告或审计。归属Mac26、devbox1/2各6、devbox3共7。

完整五指标主表见report/REPORT.md，已打开；代表局部差值见REPRESENTATIVE_GAPS.json。M扩站后F/D的P60相对表现变差、R600优势缩小；L的P60优势缩小而R600优势扩大。原图下传M的P60增加3.446个百分点、R600下降10.429秒、未完成106降至103；L分别增加10.888个百分点、下降46.275秒、143降至115。S缺六代表方法仍NA，完整代表比较不可判定。不能将运行/审计PASS表述为方法四项成功；原成功标准不变。

NOTIFIED.json登记交付核验，删除automation自动跟进后停止等待用户命令。原147与全部证据保留；教师报价近似不修，跨架构全长逐位一致未证明、三日bootstrap仅描述性等边界见报告。不改论文、不提交推送、不再启动仿真。

## 2026-10-31 13:35 北京时间：已冻结、发布并启动四机

用户明确批准新增代表方法评估及缺失L单站补齐。先读SUPPLEMENT.md。本目录与原147项独立，原实验已完成，不恢复、不覆盖。REGISTRATION.json SHA256 `6e9e370e278b87f338d2e7f5054f8ba53a345ce1e51403210480751a17f445b7`；原协议`cd1a3d97f70d0b60b33abaaf0931db616983c17e7aa6c3bb6e1794792b1207f1`、manifest`135b7b93c3348f249f46aea382a64aef0ed6cd9e7ac0715f1cdbae545a9df4c7`和模型/原TEST_OPENED不变。

45新任务：L_single_representatives中local_16/global_heft_16/global_cgs_16各3日（9）；L_scaled_representatives和M_scaled_representatives中ground_16/local_16/local_heft_16/local_cgs_16/global_heft_16/global_cgs_16各3日（36）。M单站18与L单站已存在9条代表记录已核验hash、协议配置/评分ID/窗口及600随访，直接复用。S六种代表方法无记录，本次范围没有S补跑，最终必须保留NA。

新入口analysis.run_ground_representatives_20261031；HTTP experiment ID为`ground_representatives_20261031`，仍用devbox3 `10.251.250.34:18765`服务、SQLite和原字节artifacts。凭据在各机用户目录`.local/state/bs-evaluation-queue/token`，不得打印或提交。只读检查：导入新入口为r，调用`r.queue.api('/status',dict(exp=r.EXPERIMENT),retry=False)`，或者读取本目录QUEUE_STATUS.json。

PUBLICATION.json记录首次publish请求少包spec层被HTTP409拒绝，未创建experiment、更没有仿真；随后使用`/experiments`的`dict(spec=已冻结EXPERIMENT)`正确包装，返回CREATED，/release返回RELEASED，45项已发布。冻结CLI中的publish便利函数保留原字节，不再使用、不修补、不再次发布。此发布前接口错误不是仿真失败或重试。所有任务输出开始前为空。

启动于05:31至05:32Z，逐角色launch_request防重复。Mac worker15944/监督15936，coordinator15984/监督15975；devbox1 worker1035290/监督1035141；devbox2 worker1029978/监督1029965；devbox3 worker2040408/监督2040404。目录分别local/devbox1/devbox2/devbox3/coordinator。必须核对launch.json命令/启动时间，不只PID。新模块launch/worker/coordinate，旧模块角色不恢复。真实部署根全是`/Users/bytedance/Projects/bs`。

Mac9槽，devbox各6槽，Torch/BLAS单线程，全部允许M/L任务，空闲槽即领，无同批PASS屏障、无预取。05:32Z四机合计27 RUNNING、18 PENDING，尚无结果和失败。队列RUNNING表示已领取，实际仿真推进须结合各机新阶段validation/*.progress.json或子进程；启动记录本身不当测量结果。

05:36Z复核：仍27 RUNNING/18 PENDING、blocked=null。Mac9项已有60至120秒实际仿真进度；devbox3原图/本地处理也已到60秒。devbox1/2尚未到首个60秒进度落盘，但各6个真实spawn子进程CPU约99%、累计CPU超过3分钟、RSS约0.4GB，父命令与启动时间匹配，无supervisor_failure。不能把首条进度尚未写出当失败或据此重启。

四机真实spawn子进程核验登记/全部9协议与原source_hashes/implementation_hashes，均PASS。新增4组测试在四机PASS：范围、原图第二站选择与完整到齐屏障、6方法单站一致性/多站玩具600随访、372独立记录/432逻辑行/2310区间合成报告及独立审计。验证日2026-10-22真实轨道18接口也PASS。见PREFLIGHT和DEPLOYMENT_CHECK，不重复测试。历史M/L峰值RSS约0.65/0.60GB，三机可用RAM14GB左右，6槽容量已核对；不是全长跨架构逐位一致证明。

## 执行约束

原图ground_unique_relay是自动入口，原20方法未覆盖。独立GroundOnlyPipeline已接入登记的公开几何最早整图到达选站、唯一原图字节路由、整图到齐后共享FIFO；单站走原入口，教师报价代码不改。源码已经冻结，运行中不编辑新旧源/模型/协议/清单。不能以旧SharedPipeline未适配自动入口为由在运行中再改或重跑。

FAILED在服务事务中停止所有新领取，已领取项收尾；无自动仿真重试。断联/180秒心跳过期只待确认，不回收重发，不当仿真失败，不直接重启worker。新失败核实具体traceback后通知，不能自行修复重跑。当前队列和本目录状态优先，原147/v1/v2的历史BLOCKED或18次路径启动失败不当新失败。

允许本任务SSH devbox1-3，认证过期用户已授权kinit后SSH；不训练、微调、选模、调参、不改论文、不提交推送。原协议站点/窗口/任务直接引用。M两站北京+喀什，L三站加三亚，各400Mbps，地面共享原总算力FIFO。S/M6小时，L1小时，2026-12-21至23，同日期同轨道场景变体，模型冻结，600暖机与真实随访。

48小时保守墙钟含6小时下载审计，只依据历史墙钟；硬截止11-04T16Z（11-05北京时间零点），监督杀在途组，不延期。预计超时提前通知，缺项如实NA，不补跑或缩包络。自动跟进ID `automation`、名称“扩站代表基线补跑跟进”，每小时。正常运行/等待/暂断静默，只完整完成、新失败、需行动或墙钟风险通知。

## 完成交付

全部45PASS后本地coordinator自动下载devbox3原字节结果，核验身份/hash，生成DISTRIBUTED_AUDIT（45项唯一机器归属），调用report_ground_representatives_20261031和verify_ground_representatives_20261031。REPORT_COMPLETE.json在本目录，报告在report/。检查REPORT_COMPLETE记录的HASHES/INDEPENDENT_AUDIT哈希以及HASHES逐项hash；已PASS不重复审计。

完整372独立记录/432逻辑方法日期行/2310配对区间，其中45新增327复用。M/L双设定共26方法完整，S原20方法60记录用于两栏，S缺少六代表方法不得填0或宣称完整代表包络。主表展示F/D三种子均值及原图下传、本地处理、最小负载2跳4块、局部HEFT/CGS、局部整图复核2跳16块、全局整图复核F/D、全局HEFT/CGS的P60/P600/交付均值/R600/未完成。先日等权，再种子等权；未完成为三日合计再种子均值。

重点汇报原图下传扩站变化、模型减最好代表局部方法的P60/R600差值变化，检查绝对差值符号再描述优势扩大/保持/缩小，不能把“落后减少”称“已有优势”。代表集合为ground_16/local_16/minload_2_4/local_heft_16/local_cgs_16/zero_2_16，逐指标日等权后跨方法取最好，全球排除；该补充比较不替代原六16/十二4+16四项成功标准，S集合缺项不可判定。

MODEL_PAIRS给逐种子和种子均值配对；SCENARIO_PAIRED给扩站减单站；REPRESENTATIVE_GAPS给最好来源和差值；RESOURCES给逐站主窗/全程利用率和分类资源。三日10000次paired-day bootstrap seed20261027仅描述性，不校正选择偏差；未完成区间单位每日计数，合计差另列。保存计数器不是逐包/观测重放。

全局教师接触边报价未联合预测站级通道与跨站卫星互斥，保持原近似，仅记局限，不修复、不重跑。披露Mac ARM/Linux x86全长轨迹逐位一致未证明、原路径错误及本次发布前HTTP包装错误。只陈述数据支持结论。

完整交付链接report/REPORT.md及明细，写NOTIFIED.json，更新本HANDOFF和上级状态最新顶部，删除automation自动跟进并停止等用户命令。若确认阻断，核实日志并报告，保持所有原证据；不得借收尾名义重试。若到截止缺项，只读汇总PASS和缺失清单，原状态不改。
