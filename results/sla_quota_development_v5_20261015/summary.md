# 五分钟SLA配对实验

paired short-SLA heuristic research; not MARL or original-paper full reproduction

面板：development。所有到达任务均保留，超SLA不取消。

| 方法 | 300秒按时/全部 | 场景等权P300 | 300秒截断均值(s) |
|---|---:|---:|---:|
| cgs_compute_adapted | 36/53 | 66.1064% | 224.39 |
| heft_adapted | 36/53 | 66.1064% | 224.59 |
| dts_tmax_onehop | 36/53 | 66.1064% | 223.06 |
| ground_relay | 12/53 | 22.5584% | 279.98 |
| rolling_future | 36/53 | 66.4799% | 214.10 |
| task_quota_current | 36/53 | 66.1064% | 224.39 |
| task_quota | 36/53 | 66.4799% | 217.22 |

此处比较规则候选和论文适配，不能称为新MARL训练结果。配置、完整任务结果、计时和源码哈希均保留。
