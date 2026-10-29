# 动态分片执行状态

## 已完成

- 固定模型停止并冻结，1701取352，1702/1703取176；三个种子实际均更新352/703，纯模仿独立保留。见fixed_mappo_frozen_20261025/REPORT.md与manifest.json。旧LaunchAgent已卸载，不恢复旧训练。
- 总协议DYNAMIC_REVISION_PROTOCOL_20261025.md SHA256：6dc2eed60cb23d8517b9fa890ac0326d43ff8155483677677f67aa26fa5f73c6。
- 原dynamic_heuristics_20261025因固定4块派生配置冲突已停，首版证据和源码保留。v2修订登记见DYNAMIC_HEURISTIC_V2_CORRECTION_20261025.md。
- v2执行与配置测试9项通过；动态学习接口测试6项通过。命令：env PYTHONPATH=tests:. .venv/bin/python -B -m unittest test_dynamic_granularity_v2 test_dynamic_learning_interface -q。两组分别运行通过，不需重复长测试。

## 正在运行

- dynamic_heuristics_v2_20261025，21项，9CPU worker/Torch1线程。2026-10-25T14:52:38Z启动，协调器50032，caffeinate50033；必须核对launch身份，不能只认PID。
- manifest SHA256：256333abd75099a264a217745621e042a4d5fc2739a66fc4224f9cc05e78278a。
- 入口analysis/run_dynamic_heuristics_v2.py；执行扩展analysis/dynamic_granularity_v2.py。运行时不得修改这些冻结文件。首9项已进入仿真，尚无终态结果；不能报告性能。
- 全21项完成后，先独立核验动作/逐图结果与原固定16记录一致性、指标、字节守恒、利用率及粒度比例，再进行启发式阶段汇报。根final_status的COMPLETE只表示面板运行完整，仍需独立核验。

## 待继续，尚未执行

analysis/dynamic_learning_interface.py及tests/test_dynamic_learning_interface.py为已测试的开发接口，尚未冻结训练manifest，也未生成动态教师/训练样本。图像head为3行：新4块行随机，16块/原图地面行从固定冻结actor复制；共享GAT/位置head复用，分别对4/16合法预览形成条件位置分布。联合logp按选中模式实际块数求和，熵/KL按模式概率加权。Top-K仍按各粒度的公开接触可行到达时间排序，动态数据只取原2跳TTL局部视图；动态预览不得用逐窗未来输出真值。

下一步：完成动态PPO的三模式联合概率/熵适配和教师/训练/验证协调器；保持原HP与事件回报归属，不直接调用仍硬编码二模式的main_mappo.update。Critic及assign_returns可复用。对实际块数、合法mask、行为logp、初始化父hash做测试。新入口/初始化映射/输入/数据先登记SHA256；必须等启发式审计通过才启动动态教师与训练。12个原训练窗口，3种子BC100epoch后MAPPO176，验证初始化/88/176，纯模仿checkpoint保留。禁止另开第二个9worker池与当前面板争抢。

之后按用户优先级完成局部CGS/HEFT、一次正式测试、S/L、可选无教师。动态10-31零点、全部11-06零点北京时间硬截止；测试12-21至23和旧测试当前继续SEALED。自动跟进m已改为本轮任务，每小时检查并继续获授权工作，不再跟踪固定训练。
