# 当前 SAR 并发配置

`main_config.json` 根据 `../jetson_concurrency_20261016/summary.json` 独立派生，
历史冻结文件保持原样。参数依据及profiling限制见该测量目录README。
`manifest.json`记录配置及来源哈希，`examples/config_sar_current.yaml`为同值前端入口。

单片服务0.597 s，双片组1.056 s，执行预算8/14 GB，共享物理池32.1887 GB。
所有源星与计算星同档；地面容量为星上饱和吞吐10倍。场景、图像、任务到达、链路、
指标与0.01秒步长保持不变。短流程已验证，不代表全天实验已重新完成。

`smoke/`保留首次接入时的部分记录，当时旧校验器仍使用串行工时公式，不能作为有效
对比结果。`smoke_verified/`为修复按组工时校验后的完整三模式前缀验证：1040仿真秒、
4任务，local/ground_unique_relay/cgs_compute_adapted均通过执行不变量检查。
这四个任务不代表全天，不据此报告新算法更优。
