# 教师分析接续

## 最新：2026-11-11阶段完成

六任务全部PASS，HEFT72.54分钟、两层17.70分钟；报告与独立审计已完成，曲线已目视。809项全套测试、完整接触网格覆盖、1014图独立最短路复算、5070逐图支配检查及服务器六份原字节/唯一领取审计全部PASS。见REPORT.md、DELIVERY.md及各AUDIT文件。HEFT相对CGS的R600增加3.065011秒，P60降低2.4652个百分点。关卡1结论不变；参考非理论界，严格层保证范围见实现登记。科学工作到此停止，不重新运行、不训练、不开最终测试；提交推送信息见当前会话最终回复。

以下为运行中历史接续，不代表仍需运行。

2026-11-11：用户批准两层输出，并要求Mac并行。仅10-22～24 M验证日，最终12-28～30仍封存。不得恢复旧实验、训练、改论文或重复任务。

- HEFT协同消融3任务已经通过HTTP发布，Mac3槽。入口analysis.run_teacher_analysis_20261111；实验teacher_ablation_20261111；REGISTRATION SHA256 863c877a0192334c05da69cefe75ce5a7022feb4b051f075a53060da159fe2fc。worker17729/caffeinate17730，身份以LAUNCH与workers/local/launch核对。
- 两层无争用离线分析3任务也已发布，Mac3槽；总计Mac6。入口analysis.run_teacher_bounds_20261111；实验teacher_bounds_20261111；bound_queue/REGISTRATION SHA256 a9c052e31ce9848cf702566c3dfe5206582c6101d538ee55693def88dff118f2。worker19621/caffeinate19622，身份以bound_queue/LAUNCH核对。
- devbox3沿现有18765队列服务；devbox不跑本批计算。两个worker沿旧队列函数，owner中gate1字样仅为复用的命名标签，实验ID/任务ID分别唯一，不能按owner标签误认为旧关卡任务。
- 失败即停止本实验新领取；不自动重试、网络断联不当仿真失败。任务已全部领取，无后续任务。本轮没有自动化，当前对话持续跟进。
- 新源文件全部在各自manifest中冻结，运行中不能修改。新报告器及验证器另登记REPORTER_REGISTRATION，先用人工小样本测试、未读取新成绩后才执行。
- 空日历参考不是理论界。严格界使用完整执行网格接触、无建链延迟、无限带宽/零序列化、独立单窗、全部位置最短正服务和不量化合并项；不是有限带宽紧界。完整证明和限制见docs/thesis/TEACHER_BOUND_IMPLEMENTATION_20261111.md。
- 发布前793全套测试PASS；下界新增12项、消融6项合计18PASS；报告器4项PASS。下界预发布小型几何测试曾因缩短sim_steps未同时缩短task_arrival_end_step失败，已修正测试夹具并通过，科学任务未开始时发生，不算任务重试。

完成步骤：
1. 等两个workers/local/final_status COMPLETE及两个HTTP status均3PASS。读状态，不再启动。将各自最终队列状态保存QUEUE_FINAL.json。
2. 执行analysis.report_teacher_analysis_20261111，再执行analysis.verify_teacher_analysis_20261111；已存在报告则核验、不覆盖。核验服务器六份原字节artifact与唯一CLAIM/FINISH事件，保存补充审计。
3. 打开report/curves.png目视；检查REPORT_COMPLETE、REPORT_HASHES、INDEPENDENT_AUDIT。主表教师CGS/HEFT/F三种子均值/参考/下界，P60/P120/P600/R600/未完成；两不可执行层未完成NA。
4. 写DELIVERY实际与预估墙钟、简明结论和局限。按用户规则find所有待提交>50MB、扫描实际暂存blob与私有token原值匹配（不输出token）、内置扫描完整等候；必要大文件LFS，不能普通提交>100MB。不得删除文件。
5. 提交推送origin/master，报告commit/LFS/干净状态，停止等待用户。不要开启最终评估。
