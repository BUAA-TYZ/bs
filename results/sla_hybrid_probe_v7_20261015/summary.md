# 五分钟SLA配对实验

paired short-SLA heuristic research; not MARL or original-paper full reproduction

面板：development。所有到达任务均保留，超SLA不取消。

| 方法 | 300秒按时/全部 | 场景等权P300 | 300秒截断均值(s) |
|---|---:|---:|---:|
| cgs_compute_adapted | 15/17 | 88.2353% | 160.76 |
| rolling_future | 15/17 | 88.2353% | 160.55 |
| ground_relay | 6/17 | 35.2941% | 257.82 |

此处比较规则候选和论文适配，不能称为新MARL训练结果。配置、完整任务结果、计时和源码哈希均保留。
