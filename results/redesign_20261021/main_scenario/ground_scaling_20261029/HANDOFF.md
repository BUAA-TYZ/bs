# 地面站扩展补充评估接续

## 2026-10-31 23:35 北京时间：代表基线补跑完整交付，停止跟进

representatives_20261031新增45项全部PASS，自动报告和独立审计完成；372独立记录/432逻辑行/2310区间/2656来源hash通过，6报告hash及完成标记与审计hash已核对，45项唯一归属和原字节hash再次核验通过。四机worker/监督与coordinator正常退出，不恢复。完整主表和结论见representatives_20261031/report/REPORT.md，NOTIFIED.json已登记，删除automation并停止等待命令。

M扩站后F/D相对最好代表局部的P60表现变差、R600优势缩小；L的P60优势缩小、R600优势扩大。原图下传在M/L均改善，L的R600下降46.275秒、未完成143降至115。S缺六代表不在补跑范围，保持NA和代表比较不可判定。原147/原成功标准不变；教师报价近似仅记局限，不改论文、不训练调参、不提交推送。

## 2026-10-31 13:35 北京时间：用户授权代表方法补跑，独立45项已启动

本段优先于下方“完整停止”的原147项记录。原147仍全部完成并保留；新增45项在representatives_20261031，必须先读其HANDOFF.md和SUPPLEMENT.md。新登记SHA256 `6e9e370e278b87f338d2e7f5054f8ba53a345ce1e51403210480751a17f445b7`，HTTP experiment ID `ground_representatives_20261031`，沿用devbox3服务。M/L扩站六代表各三日共36，L单站缺local_16/global_heft_16/global_cgs_16共9。四机Mac9/各devbox6按空闲槽领M/L，失败停发，无仿真重试；硬截止11-04T16Z。首次发布HTTP包装错误在创建任务前被拒绝，正确spec包装已成功发布，详见PUBLICATION，不重发或恢复旧调度。

四机冻结输入/真实spawn九协议校验与新增测试PASS；05:31至05:32Z已启动worker及coordinator，详情看新目录launch，不重复启动。自动跟进automation已创建，正常静默。完成自动新报告和独立审计，目标372独立记录/432逻辑行/2310区间，突出原图下传变化及完整代表局部差值。S缺六代表不在本次补跑范围，保留NA。教师报价近似不修复，原六16/十二4+16成功标准不改。不改论文，不训练调参，不提交推送。

## 2026-10-31 13:04 北京时间：完整交付，停止跟进

147新增仿真全部PASS；SSH认证曾阻止下载，用户提示后执行kinit成功，现有coordinator自动收齐原字节结果并完成审计，没有重启或重跑仿真。REPORT_COMPLETE于05:02:36Z记录PASS/COMPLETE；http_queue_20261030/DISTRIBUTED_AUDIT核验147项唯一归属，report/INDEPENDENT_AUDIT核验300独立记录/360逻辑行/2820区间/2563来源hash/10报告hash均PASS。跟进已核对10报告hash、完成标记及独立审计hash并目视curves.png，无需重复审计。

结果见report/REPORT.md：S仍为同一单站记录复用；M扩站后六模型交付均值均下降，L多数模型改善，但L全局v3固定/动态的P600下降、R600上升。三档两设定完整六16/十二4+16包络均可判定，所有模型种子均未满足全部四项。不能将仿真与审计PASS当方法成功。逐模型配对、完整局部与两全局参考差值、各站主窗/全程利用率均在报告内。

S/M为6小时、L为1小时；同日期同轨道任务场景变体，模型冻结。三日bootstrap仅描述性，保存计数器非逐包重放；Mac ARM/Linux x86完整轨迹逐位一致未证明，启动前18项路径失败和授权修正及队列交接证据保留。论文未改，不提交推送。NOTIFIED.json登记本次交付；删除automation自动跟进，停止等待用户命令，不启动任何旧任务。

## 2026-10-30 14:40 北京时间：HTTP队列已部署，逐机收尾交接优先

用户明确要求devbox3运行HTTP统一队列，Mac及现有devbox1至3按实际并发领取，不增加机器；后续讨论确认按空闲槽续领，不再等待同批全部完成。新入口analysis.run_ground_queue_20261030，通用服务analysis.evaluation_queue。先读http_queue_20261030/QUEUE_HANDOFF_AMENDMENT.md。新REGISTRATION SHA256 be27584b65895de0ab44cfd269187b5c1398892c7b3bc1f9586e20e21ad34d78；147项任务规范及新调度源码/测试冻结，原429hash、协议、manifest、模型、输入、唯一TEST_OPENED和科学统计不变。

17调度检查PASS，覆盖并发排他、按空闲槽领取、主机并发上限、重复请求/结果幂等、失败停发、断联不回收、数据库重启、交接拒绝非PASS，以及真实HTTP鉴权/结果上传。四台均实测可直连10.251.250.34:18765并通过Bearer鉴权，三机新登记及全部旧冻结哈希检查PASS。凭据只在各机用户目录.local/state/bs-evaluation-queue/token（0600），不能打印或提交。SQLite与artifacts在devbox3的http_queue_20261030/server，不能只拷活动数据库就当完整备份。

服务及worker于06:34至06:38Z启动：server1801804/监督1801794；Mac worker41082/监督41070，coordinator41152/监督41136；devbox1 worker516761/监督516724，devbox2 worker514647/监督514633，devbox3 worker1802673/监督1802667。以各角色launch.json的完整命令/启动时间核对，不仅PID。角色目录是http_queue_20261030/{server,local,devbox1,devbox2,devbox3,coordinator}。各新worker均已进入DRAINING_LEGACY_BATCH，无新失败，当前不创建额外仿真池。新旧部署根都是真实/Users/bytedance/Projects/bs。

旧v2四机已收到STOP_AFTER_BATCH，STOP_DELIVERY记录四机成功，PRE_HANDOFF保存命令与启动时间核对证据。旧在途仿真继续；旧协调器及监督退出后，新worker核验该机所有已启动批次PASS/哈希、无孤立或RUNNING/FAILED文件，再写LEGACY_RELEASE并解除中央RESERVED。旧STOP造成BLOCKED是预期退出，但只有逐项证据通过才能认定正常交接，不恢复旧进程。此前v1路径失败18项及v2归档证据也全部保留。

初始147 RESERVED包括已有PASS但尚未交接的任务，不代表147项都尚未运行。每机可独立交接，较快机器先领已释放任务，无需等慢机。Mac9、每台devbox6个spawn仿真进程，Torch/OMP/MKL/OpenBLAS1线程。L仍由Mac优先完成，之后Mac自动帮M；远端M不再固定每机20，按空闲槽领取，禁止预取。同一领取请求幂等，同一结果哈希幂等。失败提交在事务内阻止所有后续新领取，已领取项收尾；180秒心跳超时只标待确认，不回收重发，断联不能当仿真失败或直接重启worker。

最新总状态在http_queue_20261030/QUEUE_STATUS.json，或只读调用适配器api('/status',dict(exp=EXPERIMENT),retry=False)。旧根progress.json和v2状态可能只代表交接前，不可单独据此判断新队列。所有147 PASS后coordinator自动下载原字节并核验147项唯一归属/hash，生成新http_queue_20261030/DISTRIBUTED_AUDIT，然后调用冻结的原报告器和独立审计器，REPORT_COMPLETE仍在原根。原本v2每机20项的验收已由此替代，旧源码不改。

硬截止11-04T16Z、完整300独立记录/360逻辑行/2820区间及下方全部科学审计边界保持不变。Mac ARM/Linux x86全长逐位一致未经证明仍需披露。只本任务允许SSH devbox1-3；不训练调参、不改论文、不提交。自动跟进automation已更新为“扩站队列评估跟进”，正常运行/交接/临时断联静默，只完整完成、新失败、需行动或墙钟风险通知。不要重复启动任何角色。

## 2026-10-30：用户明确批准路径修正及续跑，v2登记优先

v2已于03:58:15Z启动，本地监督95851/协调95862；devbox1监督446058/协调446079，03:58:35Z；devbox2监督442660/协调442699，03:58:45Z；devbox3监督1767001/协调1767026，03:58:51Z。各6工作进程已核对命令及启动时间，首批已实际产生RUNNING进度（60至180秒及以上），不再是仅启动检查。三台M共18进程，同时本地原L9工作者继续推进；本地v2正常WAITING_PREVIOUS_L_BATCH，状态同步无传输错误。注册源文件保持冻结，后续请以当前launch/进度核验，不只PID。

首次三机各首6项共18项在创建仿真环境前触发旧冻结字典绝对路径键不一致，三台均BLOCKED并停止；源码hash完全相同，非性能失败。用户已明确回答允许修正部署路径、保留证据并续跑。原失败保留在distributed_20261030/devbox*/final_status.json及旧远端/home/tianyuzhe/bs_ground_scaling_20261030。不能把这些旧BLOCKED当v2失败，也不恢复旧远端。

修正登记distributed_20261030_v2/REGISTRATION.json SHA256 7c39067d74bc221bfb528e9c406cb854e9f31099c388ecea453c8d6734ec862a，PATH_CORRECTION_AMENDMENT说明授权与原因，26项补充文件冻结。远端改为真实/Users/bytedance/Projects/bs目录，绝非符号链接；原check_frozen、原协议及全部模型/源/物理/统计未改。三机真实spawn子进程完整三协议校验PASS，source_hashes/implementation_hashes字典完全一致，7项v2调度检查PASS。旧跨平台20方法几何/玩具核验仍有效。

新入口analysis.run_ground_distributed_v2_20261030，读v2/local和devbox1-3的launch/status/final_status/supervisor_failure，先核对launch_request防重复。三机仍各20项、6单线程工作进程，MacL9。v2先启动独立目录M；旧本地v1监督93893/协调93906仅收原L当前批，原88943/88955仍停止派发、旧9工作者继续。v1批次收完后退休旧组，因STOP_AFTER_BATCH故意退出BLOCKED，属于已知收尾过程。v2必须等v1 TAKEOVER_COMPLETE及父/监督退出，才归档本地18个旧FAILED progress（RETIRED_FAILURES保留逐字节hash）并同步新M结果、开新L池接续未开始项。等待时只同步远端状态，避免覆盖v1失败证据。不得手工恢复旧协调器或删除失败文件。

v2的WAITING_PREVIOUS_L_BATCH正常，M此时可在运行；本地M目录暂保留旧FAILED不能据此认定v2失败，需看v2状态或SSH真实新目录。后续失败仍无自动重试。硬截止11-04T16Z不变，旧和新监督均保留截止控制。全部任务完成后v2自动DISTRIBUTED_AUDIT和原报告/独立审计，披露跨架构及启动前路径修正，不改论文、不提交。自动跟进仍automation，正常运行静默。

## 2026-10-30 11:45 北京时间：用户批准三台开发机并行，已冻结跨机登记

本段优先于下方单机单池/不SSH历史。用户明确要求 devbox1-3 均多进程跑 M，现已完成独立环境部署与三机验证日预检；不需要第四台才能启动。新登记 distributed_20261030/REGISTRATION.json SHA256 2853bde863c710b91f4b041d68fffc4a783671b5b789bf7242925b082b133229；EXECUTION_PLACEMENT_AMENDMENT.md 与新协调器等18文件一并冻结。原协议/manifest/429hash/唯一TEST_OPENED及模型物理统计均不改。三机各20完整方法乘日期任务、6工作进程/Torch1线程，Mac保留L9进程。Python3.12.13/Torch2.12.0（Linux+cpu）/NumPy2.4.4一致；实际float32观测/掩码/离散动作/玩具轨迹一致，概率原容差通过。每机20真实验证几何+20玩具600随访通过，PREFLIGHT_AUDIT和6新增调度测试PASS。全程轨迹跨架构逐位一致未经证明，最终报告披露平台差异。

启动入口改为 analysis.run_ground_distributed_20261030；启动前先检查 distributed_20261030/local/launch_request.json 和 local/launch.json，存在就绝不重复启动。本条登记时尚未启动新池。新协调器核对旧88955/88943身份后SIGSTOP派发和旧监督，保留原L工作者运行；M无已启动记录时独占M_OWNERSHIP_ACQUIRED并自动依次启动三机。当前旧L批次收齐PASS且CPU时间稳定后退休旧组，TAKEOVER_COMPLETE落盘后新本地池只续未开始L。旧进程人为停止/退出不是失败，不SIGCONT恢复，不另开旧池。

跟进读取 distributed_20261030/local、devbox1、devbox2、devbox3 的 launch/status/final_status/supervisor_failure、SYNC_STATUS、TAKEOVER_*。用各launch的PID/命令/启动时间核对；允许仅为本补充任务SSH devbox1-3。远端目录均 /home/tianyuzhe/bs_ground_scaling_20261030。本地新协调器自动rsync远端状态及M结果，断联不等于测量失败，不重启；失败同批收尾BLOCKED，无重试。失败控制传播有网络延迟。每机独立硬截止仍11-04T16Z。旧报告和输出路径不变，追加DISTRIBUTED_AUDIT核验60项唯一归属后调用原报告/独立审计；完整147新任务/300独立记录/360行/2820区间。先看远端与本地全量状态，不能只读旧progress或旧PID。正常静默，只通知完成、新失败/需行动/墙钟风险。不训练调参、不改论文、不提交推送。

## 2026-10-29 21:10 北京时间：已登记并启动

用户已明确批准开发后直接实验，无需再次确认。新增147任务已启动，首批为L单站缺失局部规则；不要重复启动。旧S/M/L/METHOD-COST和机制诊断均已结束，旧结果不改，不开旧池。不训练、不微调、不选模、不调参，不SSH，不修改论文目录，不提交推送。

工作目录 /Users/bytedance/Projects/bs。协议为上级 GROUND_SCALING_FORMAL_TEST_PROTOCOL_20261029.md，SHA256 cd1a3d97f70d0b60b33abaaf0931db616983c17e7aa6c3bb6e1794792b1207f1。manifest.json SHA256 135b7b93c3348f249f46aea382a64aef0ed6cd9e7ac0715f1cdbae545a9df4c7；PREGEN_REGISTRATION.json SHA256 5042f382f4b5f09cb8b4abc3f55da81c82b4ac79980b432eb124030736bec5f6。429项登记hash在生成派生配置前通过；新旧57单元/集成检查和验证日真实轨道60接口检查PASS。不要重复运行测试。

启动2026-10-29T13:07:23.944804Z，协调器88955、监督88943、防休眠88956。命令 analysis.run_ground_scaling_20261029 panel/supervise；以launch.json命令和启动时间核对身份，不仅PID。TEST_OPENED.json在13:07:25.545251Z排他落盘，run_manifest已生成。运行中源码/清单/模型/协议冻结；只能只读进度、运行清单、结果状态、日志、失败标记、进程。

首批9个worker 89031/89032/89033/89034/89035/89036/89037/89038/89039已产出RUNNING进度并持续推进，无失败标记。每小时自动跟进ID为automation，完成或确认阻断后删除此ID；正常运行静默，仅完成、失败、需要行动或墙钟风险通知。

## 口径和队列

保留原北京模拟点39.9042N/116.4074E/50m；M加喀什39.5051N/75.99287E/1307m，L再加三亚18.3121N/109.3099E/22m。每站400Mbps、1接收通道、10度仰角，地面处理共享原总算力单FIFO。2025文献表2坐标预先固定，喀什与2019文献经度差异已披露，非独立测绘。任一站收到结果即交付，站间互联额外时延假设0。地面槽映射公开几何最早可达站，不改变actor形状和模型；实际资源竞争仍执行。

同日期12-21至23，原轨道/任务/评分ID/窗口直接复用，是同日期的场景变体，非新独立日期。S/M为6小时，L为1小时，600暖机回看与真实随访。每档20方法：固定/动态各三种子、六局部规则各4/16共12、global_v3_16/global_v3_dynamic参考。

单9CPU spawn池/Torch1线程，9项批次全PASS才下一批，无预取重试。顺序L_single_completion27（补齐原L缺失9规则）、L_scaled60、M_scaled60。S场景仍同一单站，引用原60记录填两栏，零差异不是独立重复证据。引用原S60/M60/L33，共300独立记录、360逻辑行。旧L历史精简报告不可判定不改写，本次补齐后新的完整比较独立出表。

保守88小时预算：L补齐12、L扩站40、M扩站30、审计6。阶段开始前分别要求剩余58/46/36小时；不足按墙钟取舍，WALLCLOCK_SCOPE_REDUCTION落盘，保留L优先、缺档NA，不根据性能取舍。硬截止11-05零点北京时间即11-04T16Z，监督终止在途进程组、不延期。预计赶不上须提前通知。

正常健康运行静默，不报零散成绩。进度在progress.json及各阶段validation/YYYY-MM-DD/*.progress.json，首次汇总可能等首条完成才出现。失败收完同批后BLOCKED；核实具体traceback后通知，不手工修复重试或恢复旧池。

## 收尾

入口完成后自动调用 report_ground_scaling_20261029 和 verify_ground_scaling_20261029；不要重复审计已完成输出。检查REPORT_COMPLETE、report/REPORT_HASHES与INDEPENDENT_AUDIT的hash、完整包络、分母和曲线，打开curves.png目视。完整时独立复算300记录/360逻辑行，504逐基线配对乘5指标加60场景配对乘5，共2820区间。若按墙钟省略M，则输出完整L与S及M具体缺项，不能缩方法包络；部分输出同样独立复核。

主表逐日后等权P60/P600/交付均值/R600，未完成三日合计；扩站减单站差值、模型减完整最强局部包络及两全局参考差值、逐站主窗/全程利用率。P600相等为平但满足不下降。阈值必须每档每设定完整六16/十二4+16分别计算，全球参考排除。只有3日bootstrap区间为描述性，不校正选择偏差；未完成区间按每日计数，合计差另列。审计保存计数器不冒充逐包/观测重放。

如果硬截止导致监督杀组但未生成报告，保留全部原状态/错误；可在新的只读汇总目录从progress复制已PASS记录作PARTIAL输入，使用冻结报告器的ROOT上下文适配生成已完整档位报告。不得修改模拟入口、manifest、旧final_status或填补缺失任务；不完整档位只给缺项NA。若没有完整L/M，仅如实说明未完成，S重复引用不是本次新证据。

报告完成后仅陈述数据支持的结论，交付配对主表/基线差值/各站利用率和完整报告链接，写NOTIFIED标记并更新本HANDOFF，删除本次自动跟进，停止等用户指令。
