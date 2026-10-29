# 五分钟SLA配对实验

paired short-SLA heuristic research; not MARL or original-paper full reproduction

面板：development。所有到达任务均保留，超SLA不取消。

| 方法 | 300秒按时/全部 | 场景等权P300 | 300秒截断均值(s) |
|---|---:|---:|---:|
| cgs_compute_adapted | 19/21 | 90.4762% | 217.14 |
| rolling_h30 | 17/21 | 80.9524% | 216.19 |
| rolling_h60 | 17/21 | 80.9524% | 210.76 |
| rolling_h120 | 18/21 | 85.7143% | 213.86 |
| rolling_no_guard | 17/21 | 80.9524% | 216.19 |

此处比较规则候选和论文适配，不能称为新MARL训练结果。配置、完整任务结果、计时和源码哈希均保留。
