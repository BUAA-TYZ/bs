# MAPPO 最终报告

三验证日等权，训练仅10-16至21。纯模仿及六局部原记录复用；测试继续封存。
| 种子/片段预算 | P60/90/120/180/240/300/600 (%) | 均值/P50/P90(s) | R600 | 未完成 | KL/熵 |
|---|---|---|---:|---:|---|
| 1701/352 | 68.22/79.97/83.42/90.32/94.26/95.45/96.24 | 178.61/49.55/150.26 | 84.40 | 15 | 3.2255/19.4821 |
| 1702/176 | 70.00/79.77/82.63/89.33/93.38/94.56/95.45 | 198.82/43.77/155.58 | 85.97 | 21 | 3.1013/18.4809 |
| 1703/176 | 66.94/78.67/82.13/89.04/93.27/94.95/96.73 | 162.92/48.13/183.11 | 85.81 | 15 | 2.2723/19.4247 |

逐项成功判定及全部checkpoint、逐日原始数据见JSON；尾部诊断见TAIL_DIAGNOSIS.md。
验证选模不代表测试成绩；KL为当前访问观测上的精确KL(current||initial)。初始化模型可被选回；全局v3不是数学上界。
种子1701: {'P60': True, 'mean_latency': True, 'P600': False, 'R600': True}；停止原因 None。
种子1702: {'P60': True, 'mean_latency': False, 'P600': False, 'R600': True}；停止原因 None。
种子1703: {'P60': True, 'mean_latency': True, 'P600': True, 'R600': True}；停止原因 None。

续训说明：原 checkpoint 未保存全局 RNG；沿用片段编号派生采样与 PPO 洗牌种子。
保持 CPU、9 workers、每种子每3片段一次更新；每25个已完成采样保存模型/优化器及待更新batch，验证点另存。
停止原因：User requested early stop: mixed tradeoffs between 176 and 352; redirect compute to dynamic granularity。实际更新预算与已完成采样数：{'1701': {'updated_episodes': 352, 'updated_fraction': 0.5007112375533428, 'collected_pass': 353}, '1702': {'updated_episodes': 352, 'updated_fraction': 0.5007112375533428, 'collected_pass': 353}, '1703': {'updated_episodes': 352, 'updated_fraction': 0.5007112375533428, 'collected_pass': 355}}。
