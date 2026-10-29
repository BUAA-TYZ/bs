# 五分钟SLA配对实验

paired short-SLA heuristic research; not MARL or original-paper full reproduction

面板：development。所有到达任务均保留，超SLA不取消。

| 方法 | 300秒按时/全部 | 场景等权P300 | 300秒截断均值(s) |
|---|---:|---:|---:|
| cgs_compute_adapted | 47/53 | 87.4510% | 139.83 |
| heft_adapted | 47/53 | 87.4510% | 140.65 |
| dts_tmax_path | 47/53 | 87.4510% | 140.62 |
| ground_relay | 12/53 | 22.5584% | 279.98 |
| rolling_future | 47/53 | 87.4510% | 138.68 |
| task_quota | 47/53 | 87.4510% | 139.25 |

此处比较规则候选和论文适配，不能称为新MARL训练结果。配置、完整任务结果、计时和源码哈希均保留。
