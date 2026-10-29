# Fixed-Overlap Paired Evaluation

paired controlled-proxy simulation; not flight hardware validation or detection generalization

| Method | Age delivery score | Completed / arrived | Restricted latency (s) |
|---|---:|---:|---:|
| ground_compute | 36.231% | 735 / 1009 | 9044.28 |
| ground_tiles | 30.099% | 587 / 1009 | 9980.64 |
| ground_relay | 49.775% | 672 / 1009 | 6811.00 |
| local | 90.189% | 1009 / 1009 | 1376.78 |
| greedy | 89.865% | 1009 / 1009 | 1417.72 |
| window_aware | 88.579% | 1009 / 1009 | 1526.67 |
| reservation_calendar | 89.180% | 1009 / 1009 | 1476.52 |
| heft_adapted | 90.189% | 1009 / 1009 | 1376.78 |
| cgs_compute_adapted | 90.189% | 1009 / 1009 | 1376.78 |

Scores and restricted latencies use equal case weights. Completed counts are pooled.
BC, PPO and heuristic results are separate. A single training seed does not establish robustness.
