# 动态阶段状态（优先于10-25状态文件）

## 2026-10-29 17:00 北京时间更新（S完成，全计划停止）

S正式60/60 PASS，自动报告及独立复核于08:20:56Z完成。837项溯源hash、3协议、评分ID与600随访、资源及候选OOD保存计数器核验PASS；60结果、84组配对比较/336指标bootstrap区间、完整曲线/两套包络/99行跨规模对照独立复核PASS。本次核验10份报告hash及REPORT_COMPLETE两个hash，协议/manifest未变，曲线已目视检查，原协调器/监督/防休眠及池全部退出，无失败标记。

S完整六16/十二4+16包络均可判定，但无模型四项全过。固定1701仅P60/P600通过，固定1702/1703仅P60通过；动态三个种子四项均未通过。不根据测试改模型。S主表与S/M/L六模型逐种子对照随最终通知交付，完整报告在s_formal_test_20261029/independent_audit_20261029。S/M为6小时、L为1小时；同日期、不同场景、模型冻结，未完成为三日合计，缺项NA不当0。L原缺完整基线的两套判定仍不可判定。

S_NOTIFIED已登记，删除自动跟进s-s-m-l，全部授权工作收尾，等待用户命令。不再启动实验/训练，不修改论文目录，未提交。

## 2026-10-29 11:54 北京时间更新（用户恢复S，优先于下方全计划结束/S取消历史）

用户明确恢复S零样本正式测试，不改论文。S_FORMAL_TEST_PROTOCOL_20261029.md SHA256 021b59e513b61c5a321312bfb59d6db0171ef5e12872fba7d52c4a748e9396e6已先登记冻结，再生成12SAR+72计算星模板和三日新任务。manifest SHA256 7cc7c90241ba2d8e0a90904a1088f3c73242baa912b25ab589c80c82dbf13c99；S_TEST_OPENED在03:52:13Z唯一落盘。同日期12-21至23、不同场景、模型冻结。每日最忙原始字节6小时，600暖机回看/随访；墙钟主估计19.2小时、压力69.7小时，均在11-05前完成预算内，未降为1小时。

20方法60任务：固定动态各3冻结模型、六局部规则各4/16共12、全局v3固定16/动态2。完整六16/十二4+16包络，S基线先三日等权再跨指标重算四项标准，全局不入包络。不微调、不选模、不调参。5新增测试已PASS，含20真实玩具执行、60合成报告和336配对区间独立复核。协调器25260/监督25249/防休眠25261在03:52:11Z启动，analysis.run_s_formal_20261029 panel；先核对命令和启动时间，不重复启动。三日数据生成46.53秒，首9固定模型任务健康运行，单9CPU spawn池/Torch1线程，每批收齐PASS才下一批，无预取重试。

入口自动在60PASS后生成并独立复核S报告与S_M_L_COMPARISON。完成后核验REPORT_COMPLETE和报告hash、目视曲线，汇报S主表及三档P60/P600/R600/未完成（标L1小时、S/M6小时），然后停止自动跟进s-s-m-l。S保守截止11-05零点北京时间即11-04T16Z，不延期；预计超时提前通知，缺失不当0或缩包络。M/L/METHOD-COST已完成，不恢复。详细状态和接续要求见s_formal_test_20261029/HANDOFF.md。未提交。

## 2026-10-29 11:23 北京时间更新（全计划收尾，优先于下方历史）

METHOD-COST已完成：连接恢复后取回四组GPU识别代理负载下CPU测量，连同八组无负载测量独立审计通过；406项溯源hash、48240个原始分项计时、分位数、概率一致性、功耗时钟及完整负载时间覆盖已核验。报告位于results/method_cost_20261028/final_report_20261029，完成记录为METHOD_COST_COMPLETE.json。真实论文chap04.tex第4.5节已移除METHOD-COST并填入实测，chap05.tex第5.11节保留四块实测边界并补充计时局限。临时章节校样编译及目视检查通过，非整篇论文构建。

动态方法Orin CPU单线程观测重建加前向P95约1197ms，已超过1秒；不包含完整端到端动作提交等流程，不能宣称已满足1秒星载部署要求。M/L既有结论不变，不依据测试选模调参。全部授权实验与本次论文收尾已完成，停止自动跟进，等待用户命令；不再启动仿真、训练或重复测量，未提交。

## 2026-10-29 08:00 北京时间更新（优先于下方历史）

L正式测试42/42 PASS，协调器53400、监督53396、防休眠53401及原池已正常退出，不恢复或重跑。analysis.report_l_formal_followup_20261028独立审计PASS：272项源/模型/输入/结果hash、3协议、评分ID与真实600秒随访、三日等权P全阶梯/时延/R600、资源分类及分段守恒、候选直方图/OOD保存计数器。报告位于l_formal_test_20261028/independent_audit_20261028。另独立复核8个报告文件hash、48组配对比较的192项胜平负/不等式/逐日差/10000次日期bootstrap区间，曲线已查看。审计仍限保存计数器，非逐包或逐观测重放。

六MAPPO种子P60为57.193%至64.134%，R600为94.333至118.152秒；均优于保留minload规则的P60与R600，但P600为90.123%至95.464%，低于minload_2_4的98.412%及minload_2_16的95.489%。精简集合缺原六16/十二4+16完整基线，两套成功口径均不可判定，不能缩集合宣告成功。测试不触发微调、不选模调参。完整结果随本次通知交付，L_FORMAL_NOTIFIED.json登记，后续不重复报告。

仅METHOD-COST尚待收尾：Orin SSH仍在认证前断开，未取回负载组最终状态，不能认定测量失败或完成，不重启。无负载8组与四块限定配置内存已审计完成。保留自动跟进以在连接恢复后下载四组并发测量、审计完整GPU负载覆盖并更新授权论文4.5；详见METHOD-COST HANDOFF。未提交。

## 2026-10-28 20:07 北京时间更新（优先于下方历史）

L精简验证42/42完成（9复用、33新），旧28766/28758及池正常退出。独立审计analysis/audit_l_reduced_20261028.py PASS，核验244项hash、3协议、评分ID/600随访、逐图日等权全曲线、资源分段及分类守恒、粒度、候选直方图/越界计数器。报告在l_reduced_validation_20261028/independent_audit_20261028；曲线已查看，L_VALIDATION_NOTIFIED.json已登记。六模型R600为101.949至118.542秒，均低于保留局部规则最低146.578911秒（minload_2_4），无微调触发；checkpoint保持冻结。原六/十二完整集合缺失，两套成功口径均不可判定，不缩集合。

Mac METHOD-COST单进程固定/动态录制已全部完成，各67决策；209登记hash与录制hash通过。Orin四块特定窗口/缓冲配置内存实测已完成并已通知，论文5.11已按实测边界更新；不能外推全队列或实机在轨可行性。用户授权此任务ssh gpu-vpn及论文4.5/5.11修改，覆盖旧对应禁令；详见results/method_cost_20261028/HANDOFF.md。Orin actor重放持久守护768227正在串行测量，不能重复启动。

L正式测试已按用户授权另登记并唯一开启。L_FORMAL_TEST_PROTOCOL_20261028.md SHA256 9cb35319a2fa5d0e8807e219cd58ab6bf0243c9ff18da924715b8a8ebf5d2920；l_formal_test_20261028/manifest.json SHA256 3e5459ab51ffb04ede1db63f6f0c1b6064e3875361abe0a3abdc22648533184d。新增4测试PASS，含开启门控/拒绝二次开启/失败批收尾/全部策略族真实玩具执行及600随访。12:04:07Z协调器53400、监督53396、防休眠53401，命令analysis.run_l_formal_20261028 panel，以launch.json命令和启动时间核对。L_TEST_OPENED.json于12:04:09Z排他落盘；当前L测试已开启，历史SEALED文字不是当前状态。

L48+288、三日12-21至23各首个最忙原始字节一小时、14方法42全新任务，模型/范围/物理/两套比较口径冻结；单9 CPU spawn池，Torch1线程，同池生成48源星窗口后按最多9项一批执行，无预取/重试。正常静默，失败同批结束后BLOCKED。新入口/manifest不得运行中修改。完成后运行analysis.report_l_formal_followup_20261028，调用已冻结audit_l_reduced_20261028.main(formal=True)；适配器仅从run_manifest补充生成后的protocol路径，不改磁盘manifest及hash校验。正式报告不计算微调触发，配对差与10000次日期bootstrap seed20261027。结果不得选模/调参。硬截止11-06零点北京时间不变。

## 2026-10-28 08:46 北京时间更新（优先于下方历史）

M正式测试99/99 PASS，旧43609/24087/43610及池已正常退出，不恢复。自动FINAL_AUDIT与独立跟进analysis/verify_m_report_20261028.py均PASS；FOLLOWUP_AUDIT_20261028.json核验316项hash、99结果、3协议、评分ID/真实600随访、完整阶梯/日等权统计、资源分类/分段守恒/利用率、粒度/尾部、两套完整包络、162组逐基线对比和648个bootstrap指标区间及局部前沿。三张图已打开检查；资源独立性限保存计数器，不是逐包重放。

M完整结果已首先通知，m_formal_test_20261027/M_NOTIFIED.json已登记。正式fixed_six_local_16与dynamic_twelve_local_4_16均无种子四项全过；六种子各自P60与R600通过，交付均值与P600失败。测试包络固定P60=49.82134%、均值188.01146s、P600=96.96201%、R600149.22680s；动态P60=51.66639%、均值157.81190s、P600=99.21674%、R600122.99615s。P600-P60局部前沿为fixed1701/fixed1703/dynamic1703/minload_2_4，R600-P60局部前沿为dynamic1703/fixed1701。完整报告/逐基线比较/全曲线/尾部和两张权衡图在M目录，不重复测试或按测试改模型。

已按最新授权登记并启动l_reduced_validation_20261028。L_REDUCED_VALIDATION_PROTOCOL_20261028.md SHA256 7b92477aefdbf4dce00088c8d9ddfcdabdbc86f1841cea5f110d1cb8f0093b5a；manifest SHA256 a65d9d4722540ac733ed50ceb2a75955ee59ace09720d02826f8caef52610c42。2026-10-28T00:46:09Z协调器28766、监督28758、防休眠28767，命令analysis.run_l_reduced_validation_20261028 panel，以launch.json启动身份核对。新入口6项测试PASS，含旧drain字段转换、精确剩余33任务、批内失败收尾且不预取、拒绝正式日期、强地面worker复用重置、各保留策略族真实玩具执行/600随访/资源守恒。登记前发现旧暂停记录使用output字段，已修正新入口适配并重测；无实验失败、无重跑旧记录。

14方法与用户清单一致，沿用原L10-22/23/24三日一小时protocol/真值/评分ID，L48+288。旧固定三种子三日9条已核验hash/配置/模型/评分/600随访后引用复用，原记录未改；只补33项。首批为动态三种子三日，单9 CPU spawn池/Torch1线程，每批收齐才开下批，无预取/重试。冻结源/协议/模型/范围/manifest不修改，不恢复旧66队列，不另开池。预计约10至14小时完成仿真是基于探针的粗估，不是保证。

只读新manifest/run_manifest/launch/progress/validation/*.progress/final_status/supervisor_failure与进程。完整42条后COMPLETION_CHECK仅运行及语义初检，仍需独立报告：hash与3协议、日等权P全阶梯/交付均值分位数/R600/未完成、全部保存资源守恒利用率、请求实际粒度、候选直方图及特征越界计数器分母/类别/未知/饱和。缺完整六/十二规则，原两套成功判定均不可判定，不能缩集合。微调需求只对保留局部规则minload_2_4/minload_2_16/zero_2_16的最低三日等权R600诊断，不重选冻结模型。微调未安排，若实际进行须另登记且在L测试前冻结，不能依据M成绩。L_TEST_OPENED仍不存在，后续另登记同14方法测试并唯一开启。

L验证截止10-31零点北京时间，总截止11-06零点；M优先已完成，L按实际完成报告。S/无教师取消，不SSH、不改论文、不提交；正常运行静默，异常/完成/需行动才通知。Orin4块峰值内存仍未验证，电脑RSS不证明实机可行。

## 2026-10-27 13:02 北京时间核验（优先于下方历史）

L首9项全部PASS，PAUSED_BY_USER.json于04:52:20Z落盘，unexpected_progress为空；9份输出hash、评分ID及600随访复核通过。旧协调器10808、监督10803、worker10815至10823及drain守护22317已退出，不恢复旧66项队列，保留全部完成记录。

M正式测试已自动启动：launch.json记录协调器43609、监督24087、防休眠43610，启动04:52:20Z；M_TEST_OPENED.json于04:52:24Z唯一开启M12-21至23。M manifest、全部run_manifest输入hash与三个协议核验PASS，33方法共99项；首批9个worker43632至43640正常运行，尚无完整阶段结果。以命令及启动时间核对身份；supervisor.json可能仍保留WAITING_FOR_L_DRAIN旧状态，应结合launch和worker进度判断。L_TEST_OPENED不存在，L测试仍封存。

当前只跟进M，不再等待已结束的drain，不启动额外池；M完整报告先汇报后再继续精简L。所有冻结入口、模型、manifest及比较口径保持原登记，后续计划和硬截止见下方M优先修订。

## 2026-10-27 M正式测试优先修订（优先于下方所有历史）

用户要求当前L首9项完成后暂停，M正式测试先行并首先汇报，再按精简清单继续L。本次补充登记M_TEST_PRIORITY_AMENDMENT_20261027.md SHA256 1241624a1bae950d897d8b37c8142ba97602d630edd9ebb9517c5c28435e1050；M运行manifest m_formal_test_20261027/manifest.json SHA256 00acb98d55c3c01e3005159907c99980ce02bdd125774256eea51f6ea88f3597。旧固定、动态模型与选择点均不变。

L旧协调器10808已按用户指令SIGSTOP，9个worker仍完成当前固定三种子三日任务。独立drain守护22317，启动2026-10-27T03:10:57Z，入口analysis.drain_scale_batch_20261027 guard。旧入口一次性预排66项，不可直接恢复；守护每项输出落盘后停对应worker，收齐9项终止旧队列及监督10803，写PAUSED_BY_USER.json。DRAIN_REQUEST/GUARD与drain.log为收尾证据；核查unexpected_progress，存在时必须复核，不声称无竞态。旧progress停在acquisition不代表worker空转。守护新增代码不修改旧冻结入口和manifest。

M独立入口analysis.run_m_formal_20261027、报告入口analysis.report_m_formal_20261027已冻结。5项新增测试PASS，含全部33方法真实玩具执行器、600随访/资源守恒、原任务生成算法一致、FIFO不可见远端内存、胜平负/严格判定/两个方向帕累托/日期bootstrap及99条合成报告端到端生成。合成图已查看，不是测试成绩，未访问真实测试数据。

M监督等待L PAUSED_BY_USER=9 PASS，核验9结果hash/评分/600随访及旧池退出，才排他写入M_TEST_OPENED.json并自动生成M12-21至23任务。launch_request/supervisor/launch记录新身份，不能复用旧PID。单9CPU spawn池/Torch1线程，M六小时33方法三日99项。明确9项一批，失败只收完当批，不再预取后续批，无自动重试。测试不选模不训练。M正式日期唯一开启后L仍SEALED，旧date_split的历史封存文字不改，当前授权状态以唯一开启标记为准。

M报告新增P600-P60及R600-P60权衡图，固定/动态MAPPO种子各点及三种子均值，局部实测方法帕累托前沿，全局和展示均值点不参与前沿。两套成功口径分别列表；逐MAPPO对全部非MAPPO方法列四指标胜平负/原不等式判定/逐日差/日期配对bootstrap95%区间（10000次，seed20261027）。三日等权、基线测试阈值重算、四项不等式不变；只有三日期，区间描述性，不据此调参。FIFO原实现直接读取真实远端内存，新同信息适配保留排序与公共几何，只用源星2跳TTL视图的剩余内存，执行器物理不变；具体已登记且测试。

完整M99PASS后自动生成FINAL_AUDIT/REPORT/PAIRWISE/PAIRED_COMPARISONS/full_curves/TAIL_RECORDS/两张tradeoff与曲线。跟进时仍需核验manifest/开启标记/全部模型与输入hash、审计和图，首先汇报M并写M_NOTIFIED标记，然后才继续L，不等用户确认。正常进度静默；失败或监督异常核实具体日志通知，不重试。不恢复旧阶段，不开第二池。

后续L精简为14方法：固定/动态MAPPO各3、全局v3固定16/动态、minload_2的4/16、局部v3=zero_2_16、局部CGS16/HEFT16、强地面16。保持48+288三日1小时，首9项经完整语义/hash审计可复用；续跑须新登记新入口，不启动旧66任务队列。L验证仅判断微调需求、不重选现有冻结checkpoint；时间优先M，规模微调尚未安排，不因M测试表现改变L模型或超参；需要实际安排88片段时先登记且只能用L训练/验证，在L测试前冻结。L测试另写唯一L_TEST_OPENED才开启，方法同14项。

11-06零点北京时间总截止不变，优先M测试和报告，L按实际完成报告缺失，不把部分面板说完整。预计超时提前通知。S/无教师仍取消。原M输入范围hash和两套比较口径仍有效；L精简不能冒称原6/12完整包络，缺失基线只能标不可判定。Orin4块峰值内存未验证列最终局限。禁止SSH、论文修改、提交。

## 2026-10-27 10:09 北京时间更新（优先于下方历史）

L资源探针scale_probe_20261027已9/9 PASS，83270/83271及worker全部退出。独立核验入口analysis/audit_scale_probe_20261027.py，产物independent_audit_20261027/{AUDIT.json,RESOURCE_DECISION.json,REPORT.md,HASHES.json}；登记/源码/模型/协议/真值hash及保存不变量、600随访检查一致。未读取探针P/R600，不将探针当泛化成绩。单项墙钟2.312至2.560小时，整批2.565小时，峰值RSS约0.524至0.555GB/进程，含此前生成任务高水位。Orin4块内存仍未实测。

资源决策已冻结：L保持48+288，改为三日各首个最忙原始字节1小时；取消S、规模微调、无教师消融，优先M正式测试和L零样本。22方法×3日=66任务/面板，9并发8波。按实测最大墙钟，六小时验证+测试名义245.76小时，已超过当时剩余约230小时，尚未含M和审计；一小时两面板名义40.96小时，含2倍未知方法/日期密度不确定性、M测试35h、准备审计12h及20%余量共154.71小时。该系数不是保证，不能单任务墙钟再除9，不能按成绩改变规模。本次通知后不重复报告探针。

M训练输入范围在L验证生成/观察前已冻结：m_training_ranges_20261027/ranges.json SHA256 e967486e4acb9992effb7c07c59bbfed1a4aa807b4b111b9c5f5b4083a77939c。BC固定/动态各841评分观测；固定所选谱系1701/1702/1703分别24676/12242/12303条PPO观测；动态各6155/6132/12303条，并合并各自固定父谱系。逐项更新rollout_hash核对，不含未更新或选中之后片段。按实际block_count分组节点角色/known状态，padding排除；候选统计另按请求4/16分组。源文件、更新清单、模型和范围hash见registration.json/HASHES.json。历史截断前数值不可复原，编码空间统计不证明物理值未越界。

已启动scale_validation_20261027。登记SCALE_VALIDATION_PROTOCOL_20261027.md SHA256 6889e166427332f025a1b1a6a8f37c05444ae722720ac1e1d34b3b428ede1ad3；manifest SHA256 fa47bba78bcf777bb758505a6736e244652fc8a5eadab7f6c2b0467d300ba497。2026-10-27T02:08:37Z协调器10808、监督10803、防休眠10809，命令analysis.run_scale_validation_20261027 panel；核对launch.json启动身份。单9 CPU spawn池、Torch1线程，先同池生成10-22至24 L任务，冻结run_manifest/protocol/真值hash，再运行22方法×3日66项。不能另开池。新增4项特征统计测试和5项规模入口测试PASS，包含固定/动态旁路动作hash与outcomes一致、五类策略真实玩具执行器资源分段与600随访，不重复旧测试。

只读manifest/launch/progress、run_manifest、validation各*.progress.json、final_status与supervisor_failure。入口/登记/范围均冻结，不改运行中代码。无重试，单项失败保留同batch其余结束后BLOCKED。监督只处理硬截止与异常，不恢复。10-31零点北京时间终止在途组，不延长11-06总截止。正常运行静默，不报零散成绩。

全部66PASS后独立核验三日协议/评分ID/600随访、全部hash、逐图P全阶梯/均值/P50/P90/R600/未完成、资源守恒和利用率、请求/实际粒度；复算候选直方图与P50/P90、每维越界/总体单元与决策分母、类别新值/未知占比/饱和。固定六16与动态十二4/16包络分开，CGS/HEFT另列。报告完整L零样本验证后继续统一正式测试冻结，无需确认；规模微调已因资源取消，不由验证再启。正式12-21至23和旧测试继续SEALED，唯一开启前冻结全部M/L方法、模型、时长和配对bootstrap统计清单，最迟11-01开启，11-06零点完成。后续不恢复任何旧阶段。

## 2026-10-27 07:12 北京时间更新（优先于下方历史）

local_comparison_20261027已COMPLETE，21项新任务PASS加21份复用共42条完整记录；38102/38103已退出。analysis/audit_local_comparison_20261027.py独立审计PASS，42配置hash另逐项核对一致。冻结hash/协议/评分ID/600随访、P全阶梯/交付均值及分位数/R600/未完成、资源计数器分类与分段守恒、粒度、两套包络与选中模型hash均通过；已打开curves.png核对。产物local_comparison_20261027/independent_audit_20261027/{AUDIT.json,REPORT.md,full_curves.json,TAIL_RECORDS.json,curves.png,HASHES.json}，LOCAL_COMPARISON_NOTIFIED.json登记本次通知。

fixed_six_local_16阈值：P60 49.76016%、均值183.62504s、P600 96.34690%、R600 133.65739s。固定1701只失败P600；1702失败均值/P600；1703四项通过。dynamic_twelve_local_4_16阈值：P60 50.23733%、均值155.82689s、P600 98.81554%、R600 117.41127s。动态1701/1702失败均值和P600；1703只失败P600；无动态种子四项全部通过。局部CGS P60/P600/R600=57.4437%/86.5641%/140.4764s，局部HEFT=68.1107%/89.8241%/118.5887s；两者单列更广参考，不加入包络。均为验证结果，正式测试阈值重新计算，测试仍SEALED。

下一阶段scale_probe_20261027已登记启动。SCALE_PROBE_PROTOCOL_20261027.md SHA256 a994e5652749ce751fdeada12a8843187ab43a4d1fbc9b875927ebd9e9f29149；registration.json SHA256 e10f2ad1c60d024c144dc0741b86fd2106c9fef8262843c6a025c7dad355fe87。2026-10-26T23:11:37Z协调器83270、防休眠83271，命令analysis.run_scale_probe_20261027 panel，核对launch命令/启动时间。4项新增测试PASS，M整个配置精确复原，L只改已登记规模字段。入口/登记冻结，无重试或运行中修改。

探针只生成L48+288在10-16的首高峰1小时，600秒暖机回看/随访，非测试。单个9-worker spawn池先生成48源星成像窗口，再复用该池同时测9种方法：固定/动态三个种子、全局v3固定16/动态、局部HEFT16；Torch1线程。不报告/比较P或R600，不按探针成绩选模；只统计生成、环境初始化、逐步墙钟、进程峰值RSS（同池高水位可能含先前生成任务）。数据生成后先冻结run_manifest/protocol/真值hash再仿真。progress.stage=acquisition或simulation，runs/*.progress.json及final_status.json；只读跟进，不开第二池。

全部9PASS后核验登记/输入/模型/protocol hash，依实测墙钟/RSS保守外推全方法三日L6小时面板，包含数据准备审计与20%余量；不能将单任务墙钟再除9或把一小时线性外推当保证。CGS/其他规则成本未被逐一探针测量，注明不确定性。按资源而非成绩决定是否L1小时/36+2161小时及取消S/微调/无教师，必要时提前汇报。探针10-30零点截止；正式测试最迟11-01开启，全计划11-06零点结束不变。

后续仍需仅用M实际训练输入建立/冻结特征范围，再生成S/L验证与必要条件微调（只由同档验证触发，禁止测试后触发），并统一冻结全部主测试/规模方法、模型、时长、统计清单与唯一开启标记。新旧测试继续SEALED。当前没有正式测试任务，不把资源探针当零样本性能结论。Orin4块峰值内存仍未实测。既有完成阶段不重复通知。

## 2026-10-27 03:17 北京时间更新（优先于下方历史）

动态学习dynamic_learning_20261026已COMPLETE。旧协调器16363/监督16355/防休眠16364正常退出，不再跟踪或恢复。三个种子各176实际更新片段、176完整采样，27项验证全PASS；动态新增预算100%（原703的25.04%），固定父阶段352/703另列。按原选模规则冻结1701=88、1702=88、1703=176；pure_imitation各自独立保留。独立入口analysis/audit_dynamic_learning_20261027.py已核验verify_run(False)、15协议、全部输入/FINAL_AUDIT哈希、183 checkpoint父链与528采样来源、三日评分ID/600随访、完整P阶梯曲线、均值/分位数/R600/未完成、日等权已记录KL熵、资源守恒与请求/实际粒度、选模和退化计数。产物dynamic_learning_20261026/independent_audit_20261027/{AUDIT.json,REPORT.md,TAIL_RECORDS.json,curves.png,HASHES.json}。原validation_curves.png与独立图已打开检查。DYNAMIC_NOTIFIED.json登记本次完整阶段通知，避免重复。

三个选中动态模型P600分别96.048%/95.264%/97.923%，R600分别84.672/84.927/79.843秒；相对自身动态BC初始化改善。仍不能按原REPORT六项口径宣告动态模型成功。完整12项口径待补齐，且已知minload_2@4的P600约98.82%高于三个选中模型，已知必要条件不满足。正式测试阈值必须用测试结果重算，不能沿用验证阈值。

当前启动local_comparison_20261027：登记LOCAL_COMPARISON_PROTOCOL_20261027.md SHA256 2e47f619a9d8d113fde2c80c33655b52163912109452a6b55628aaa0e00d50e5，manifest SHA256 fdc69a7b77eb9943d3eba62df344a728f2bea5a4f1116cae5d0948207160066a。2026-10-26T19:17:12Z协调器38102、防休眠38103；命令analysis.run_local_comparison_20261027 panel，以launch.json身份和启动时间核对，不只PID。单池9 CPU spawn worker、Torch1线程；4项新增接口测试PASS。入口和登记已冻结，不在运行中修改。

复用21份记录（原六局部16各三日18份，minload_2@4三日3份），已核对冻结代码/配置/方法/评分ID/随访/P；新增21项（其余五局部4块三日15份，局部CGS/HEFT16块三日6份）。CGS/HEFT逻辑不改，只通过2跳、15s TTL、未知零先验、公共计划的局部视图调度。旧16资源计数器缺失为N/A，非零；不可虚构或将缺失当失败。读progress、validation/*.progress.json、final_status；单项失败保留，同batch其他任务结束，不重试。无旧训练池或第二池。

全部新增21 PASS及42条完整结果后独立复算新12包络与局部CGS/HEFT指标、哈希和全曲线，再汇报/继续。随后登记L训练日耗时探针，统一冻结全部正式方法/模型/规模/统计后唯一开启正式12-21至23测试（最迟11-01）；当前新旧测试继续SEALED。规模1:6及条件88微调仅由本档验证触发、不能测试后触发的要求不变。动态冻结已按时完成；全部11-06零点截止，资源不足按登记优先L零样本，取消低优先级，不自行SSH/改论文/提交。不重复已完成阶段通知。

## 2026-10-26 比较口径补充（优先于下方旧口径）

用户已批准 COMPARISON_SCOPE_AMENDMENT_20261026.md，SHA256 97e4963ef0d40381be2f58d5c270bf904f1be57c0fb414b2e659a263fb399616。固定MAPPO沿用六项局部16块包络；动态MAPPO改用 zero_1/2、history_1/2、minload_1/2 各4/16块共12项跨指标包络。四项不等式不变，正式报告分表标明 fixed_six_local_16 / dynamic_twelve_local_4_16；同信息组更广基线比较另列。12项验证未齐时动态新口径只能写待补齐，不能沿用当前冻结入口自动REPORT里的六项判定宣告成功。

当前训练保持运行，入口/协议/manifest不改，选模和防退化不变。后续单池补齐4块局部规则验证并审计可复用记录；正式测试唯一开启之前将全部12项及本补充hash纳入统一冻结清单。测试仍SEALED。最终报告局限明确Orin上4块峰值内存尚未实测。后续自动跟进以本补充优先，无需用户再次确认。

## 2026-10-26 03:19 北京时间更新（优先于下方历史记录）

启动后更新：三个BC均100epoch PASS，各841样本，实测各约31.8秒；checkpoint/来源/100epoch/三模式/初始化权重一致性独立核验见dynamic_learning_20261026/BC_AUDIT.json。当前已自动进入0点三日九项验证，9worker正常；MAPPO首次梯度更新须待完整初始化验证完成。progress.json在此阶段仍可能显示pure_imitation，应同时看validation/*进度和当前进程。

动态教师12/12已COMPLETE，原4006协调器正常结束。独立审计analysis/audit_dynamic_teachers_20261026.py通过全部冻结hash、12协议、首reference/teacher完整一致、逐图实际承诺/窗口权重/位置掩码/软分布复算。841图52656窗口全部represented，计算星Top-K覆盖100%，未承诺/未捕获/掩码外/Top-K外/unsupported均0；ground_block 4046为represented子集；非法image_mask 0。产物dynamic_teachers_20261026/independent_audit_20261026/AUDIT.json。无需再启动教师。

新三模式PPO入口analysis/dynamic_mappo.py、协调器analysis/run_dynamic_learning.py已实现；8项新增测试通过，包含真实玩具执行器的训练与验证输出、奖励归属、实际块数、行为logp、双条件熵、父checkpoint与预算、选模。未修改已冻结的dynamic_learning_interface.py或原执行器/物理。

登记DYNAMIC_LEARNING_PROTOCOL_20261026.md；运行dynamic_learning_20261026，manifest SHA256 bf38e564198ab5e095c65983b718e2d26af13dbc7ca525a50d2cc1ceebd95997。2026-10-25T19:18:56Z启动监督16355、协调器16363、防休眠16364，命令analysis.run_dynamic_learning panel；核对launch.json与进程启动身份，不只PID。当前三个纯模仿worker，100epoch最后checkpoint保留；完成后自动三日初始化验证，再每种子176 MAPPO，88/176三日验证，不需重复启动。BC最多3worker，仿真最多9worker单池、Torch1线程。

父模型固定1701=352、1702/1703=176，actor共享参数/16/raw行继承、新4行按种子随机，critic继承固定父模型。BC actor-only Adam；MAPPO新建原HP联合优化器，真实事件回报沿用原函数，3模式联合概率与概率加权熵用新入口。训练仅12冻结训练小时；0/88/176验证与原防退化/选模规则。进度progress.json、batch_progress.json、training/*_progress.json及rollouts/validation/*.progress.json；VALIDATION_0/88/176_READY.json为完整三日门控。阶段失败不重试，通知证据；不改运行中冻结文件。截止监督10-30T16Z终止进程组并只冻结完整三日验证模型；实际更新和已采样预算分开。supervisor只负责截止及异常报告，不自动恢复失败。

后续依优先级继续局部CGS/HEFT验证、统一测试清单冻结后一次开启、1:6规模泛化。当前测试仍SEALED。不得恢复旧固定训练或旧LaunchAgent。正常训练静默；阶段完成独立核验后报告，不报告零散成绩。

## 此前记录

启发式21/21已COMPLETE，协调器50032正常退出。独立复核入口analysis/audit_dynamic_heuristics_20261026.py，产物dynamic_heuristics_v2_20261025/independent_audit_20261026/AUDIT.json、REPORT.md、curves.png、HASHES.json。固定16三天完整动作hash/outcomes与旧v3一致；输入哈希、评分ID、600随访、全曲线、保存计数器守恒/分段/利用率都通过。无逐包事件流，不宣称独立逐包物理重放。HEURISTIC_NOTIFIED.json避免重复通知。

当前动态教师阶段：dynamic_teachers_20261026，manifest SHA256 7ab185640422280d1ac39bd5df887f630aea11bc4f44cc95f10f8b7edb0d4eb5。2026-10-25T18:13:19Z启动协调器4006/caffeinate4007，命令analysis.run_dynamic_teachers panel，身份见launch.json。先10-16首训练小时reference/teacher两任务，被动观测与标签必须不改变actions/outcomes/dynamics；REPLAY_GATE全PASS后自动跑其他11个教师（最大9worker/Torch1线程）。仅生成已冻结的12训练窗口，未开始BC/MAPPO更新。无需再启动其他教师进程。硬截止10-30T16:00Z。

动态接口文件已纳入教师manifest冻结，不可运行中修改。analysis/dynamic_learning_interface.py含三模式actor、实际块数logp、条件位置mask、精确分布KL/熵、教师软标签、执行承诺。新增4块行随机，16/raw行从固定checkpoint复制。地面标签ground_block_windows为represented子集，ground_block_committed_windows含所有地面承诺；不可加到互斥覆盖分母。当前接口/采集测试8项通过，不必重复无关测试。

下一步开发新文件：动态MAPPO update（原analysis.main_mappo.update硬编码二模式，不能直接使用）；动态BC/验证/176片段训练协调器。可复用原Critic、assign_returns、HP，动态joint_statistics与distribution_metrics用新接口。BC父模型为fixed_mappo_frozen_20261025/mappo_1701/1702/1703.pt，先检查其checkpoint键。保存新纯模仿checkpoint，验证初始化/88/176，不能从验证挑BCepoch。新增代码须测试、预登记哈希后才执行，教师全部PASS并独立覆盖核验为门控。状态/模型来源/更新预算和严格截止要落盘；仍为一个9worker CPU池。教师生成期间可开发PPO新入口，不能提前训练。

用户新第五项见SCALE_GENERALIZATION_AMENDMENT_20261025.md：S12SAR+72计算，M24+144，L48+288；旧比例作废。规模注册和耗时外推已完成，未生成任务，测试仍SEALED。条件88微调只看同档验证触发，必须在第四项测试一次开启之前冻结所有规模/模型。保持后续优先级和总截止，不SSH不改论文不提交。
