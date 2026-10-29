# Fixed-Overlap Paired Evaluation

paired controlled-proxy simulation; not flight hardware validation or detection generalization

| Method | Age delivery score | Completed / arrived | Restricted latency (s) |
|---|---:|---:|---:|
| ground_compute | 38.554% | 836 / 1015 | 8789.35 |
| ground_tiles | 34.725% | 726 / 1015 | 9284.02 |
| ground_relay | 50.649% | 682 / 1015 | 6678.71 |
| local | 91.592% | 1015 / 1015 | 1279.36 |
| greedy | 91.435% | 1015 / 1015 | 1310.07 |
| window_aware | 90.565% | 1015 / 1015 | 1379.40 |
| reservation_calendar | 91.000% | 1015 / 1015 | 1347.59 |
| heft_adapted | 91.592% | 1015 / 1015 | 1279.36 |
| cgs_compute_adapted | 91.592% | 1015 / 1015 | 1278.83 |

Scores and restricted latencies use equal case weights. Completed counts are pooled.
BC, PPO and heuristic results are separate. A single training seed does not establish robustness.
