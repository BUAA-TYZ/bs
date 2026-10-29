# 纯模仿1701尾部诊断

动作哈希及逐图outcomes均与冻结验证精确相同；以下是观察证据，非因果隔离。
同图v3/minload_2处理位置及逐块时间戳见TAIL_DIAGNOSIS.json和diagnostics目录。
类别可能重叠；主类别按地面争用、辅助星排队、分散尾块、其他顺序登记，不将相关性当唯一原因。

| 日期 | 图像 | 主类别 | v3 / minload2 / BC时延(s) |
|---|---|---|---|
| 2026-10-22 | task_7 | raw_ground_contention_observed | [5599.400000000001, 220.4, 5599.1] |
| 2026-10-22 | task_31 | helper_queue_observed | [41.5, 225.10000000000002, 5593.3] |
| 2026-10-22 | task_41 | raw_ground_contention_observed | [47.1, 225.4, 5624.5] |
| 2026-10-22 | task_47 | helper_queue_observed | [46.5, 219.70000000000002, 5923.200000000001] |
| 2026-10-22 | task_65 | helper_queue_observed | [41.300000000000004, 141.9, 5646.200000000001] |
| 2026-10-22 | task_143 | raw_ground_contention_observed | [None, 220.4, None] |
| 2026-10-22 | task_161 | raw_ground_contention_observed | [46.7, 220.0, 5622.700000000001] |
| 2026-10-22 | task_171 | helper_queue_observed | [327.40000000000003, 141.6, 5913.8] |
| 2026-10-22 | task_177 | raw_ground_contention_observed | [None, 449.40000000000003, None] |
| 2026-10-22 | task_179 | raw_ground_contention_observed | [None, 290.8, None] |
| 2026-10-22 | task_182 | raw_ground_contention_observed | [None, 180.9, None] |
| 2026-10-22 | task_190 | raw_ground_contention_observed | [None, 220.10000000000002, None] |
| 2026-10-22 | task_191 | raw_ground_contention_observed | [23.0, 26.700000000000003, None] |
| 2026-10-22 | task_197 | other_or_insufficient_causal_evidence | [48.0, 141.70000000000002, None] |
| 2026-10-22 | task_205 | raw_ground_contention_observed | [54.7, 220.20000000000002, 5874.6] |
| 2026-10-22 | task_218 | raw_ground_contention_observed | [5744.200000000001, 234.20000000000002, 5746.700000000001] |
| 2026-10-22 | task_220 | raw_ground_contention_observed | [59.6, 219.9, 5707.8] |
| 2026-10-22 | task_239 | helper_queue_observed | [136.9, 3157.8, 3035.7000000000003] |
| 2026-10-22 | task_247 | helper_queue_observed | [54.7, 219.9, None] |
| 2026-10-22 | task_256 | helper_queue_observed | [None, 220.10000000000002, None] |
| 2026-10-22 | task_257 | helper_queue_observed | [55.400000000000006, 225.20000000000002, None] |
| 2026-10-22 | task_310 | other_or_insufficient_causal_evidence | [35.5, 141.70000000000002, None] |
| 2026-10-22 | task_317 | other_or_insufficient_causal_evidence | [None, 229.0, None] |
| 2026-10-22 | task_318 | raw_ground_contention_observed | [59.300000000000004, 220.20000000000002, None] |
| 2026-10-22 | task_326 | raw_ground_contention_observed | [176.5, 141.8, None] |
| 2026-10-23 | task_3 | other_or_insufficient_causal_evidence | [5599.700000000001, 225.3, 5617.1] |
| 2026-10-23 | task_16 | raw_ground_contention_observed | [175.4, 223.9, 5782.1] |
| 2026-10-23 | task_25 | raw_ground_contention_observed | [5600.8, 156.8, 5620.6] |
| 2026-10-23 | task_50 | raw_ground_contention_observed | [328.3, None, 344.90000000000003] |
| 2026-10-23 | task_69 | raw_ground_contention_observed | [54.300000000000004, 219.8, None] |
| 2026-10-23 | task_70 | raw_ground_contention_observed | [35.6, 141.8, None] |
| 2026-10-23 | task_80 | other_or_insufficient_causal_evidence | [35.6, 141.70000000000002, None] |
| 2026-10-23 | task_133 | distributed_tail_block_observed | [331.1, 21.400000000000002, 5915.400000000001] |
| 2026-10-23 | task_140 | raw_ground_contention_observed | [5749.0, 315.70000000000005, 5749.6] |
| 2026-10-23 | task_169 | raw_ground_contention_observed | [49.0, 219.9, 5863.700000000001] |
| 2026-10-23 | task_175 | raw_ground_contention_observed | [None, 3434.2000000000003, None] |
| 2026-10-23 | task_176 | helper_queue_observed | [41.7, 141.6, 406.70000000000005] |
| 2026-10-23 | task_212 | raw_ground_contention_observed | [87.60000000000001, 235.0, None] |
| 2026-10-23 | task_213 | raw_ground_contention_observed | [54.300000000000004, 225.20000000000002, None] |
| 2026-10-23 | task_214 | raw_ground_contention_observed | [35.4, 141.6, None] |
| 2026-10-23 | task_215 | raw_ground_contention_observed | [64.9, 158.60000000000002, None] |
| 2026-10-23 | task_235 | other_or_insufficient_causal_evidence | [33.5, 141.8, None] |
| 2026-10-23 | task_245 | distributed_tail_block_observed | [50.300000000000004, None, None] |
| 2026-10-23 | task_248 | raw_ground_contention_observed | [41.2, 141.6, 5799.400000000001] |
| 2026-10-23 | task_299 | raw_ground_contention_observed | [35.4, 143.4, None] |
| 2026-10-23 | task_311 | other_or_insufficient_causal_evidence | [35.6, 143.6, None] |
| 2026-10-24 | task_51 | raw_ground_contention_observed | [5779.8, 320.90000000000003, 5840.700000000001] |
| 2026-10-24 | task_53 | raw_ground_contention_observed | [168.70000000000002, 235.70000000000002, 5758.900000000001] |
| 2026-10-24 | task_55 | raw_ground_contention_observed | [5740.900000000001, 264.7, 5759.6] |
| 2026-10-24 | task_56 | raw_ground_contention_observed | [102.2, 156.0, 5692.400000000001] |
| 2026-10-24 | task_57 | raw_ground_contention_observed | [5641.200000000001, 225.20000000000002, 5688.5] |
| 2026-10-24 | task_82 | raw_ground_contention_observed | [54.7, 225.3, None] |
| 2026-10-24 | task_85 | other_or_insufficient_causal_evidence | [35.5, 141.9, 5650.1] |
| 2026-10-24 | task_89 | raw_ground_contention_observed | [35.6, 143.5, 5590.900000000001] |
| 2026-10-24 | task_98 | raw_ground_contention_observed | [None, 279.40000000000003, None] |
| 2026-10-24 | task_100 | raw_ground_contention_observed | [56.2, 220.20000000000002, None] |
| 2026-10-24 | task_121 | other_or_insufficient_causal_evidence | [None, 225.10000000000002, None] |
| 2026-10-24 | task_136 | helper_queue_observed | [342.0, 2747.1000000000004, 2720.0] |
| 2026-10-24 | task_162 | raw_ground_contention_observed | [46.6, 220.0, 5689.1] |
| 2026-10-24 | task_181 | helper_queue_observed | [None, 141.6, None] |
| 2026-10-24 | task_184 | raw_ground_contention_observed | [5763.1, None, 5764.6] |
| 2026-10-24 | task_185 | helper_queue_observed | [54.400000000000006, 225.10000000000002, 5715.8] |
| 2026-10-24 | task_202 | helper_queue_observed | [50.0, 3147.8, 3026.0] |
| 2026-10-24 | task_246 | distributed_tail_block_observed | [54.400000000000006, None, None] |
| 2026-10-24 | task_264 | raw_ground_contention_observed | [35.4, 143.4, None] |
| 2026-10-24 | task_265 | raw_ground_contention_observed | [40.2, 143.5, None] |
| 2026-10-24 | task_274 | other_or_insufficient_causal_evidence | [54.400000000000006, 225.10000000000002, None] |
| 2026-10-24 | task_332 | raw_ground_contention_observed | [None, 293.90000000000003, None] |
| 2026-10-24 | task_342 | helper_queue_observed | [40.6, 141.9, None] |
