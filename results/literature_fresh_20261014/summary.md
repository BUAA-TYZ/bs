# Literature Adaptation Development Comparison

paired development comparison of literature adaptations; NOT original-paper numerical reproduction or final test

| Method | Delivery score | Complete | Restricted mean (s) | Scheduler elapsed time (s) |
|---|---:|---:|---:|---:|
| ground_compute | 28.1899% | 436/696 | 10167.98 | 0.00 |
| ground_tiles | 27.2747% | 428/696 | 10235.62 | 0.00 |
| ground_relay | 45.8838% | 618/696 | 6941.28 | 0.00 |
| local | 11.5947% | 278/696 | 12540.85 | 0.01 |
| greedy | 53.7221% | 642/696 | 6260.97 | 0.03 |
| window_aware | 52.5316% | 639/696 | 6408.84 | 0.05 |
| reservation_calendar | 54.9174% | 642/696 | 6127.13 | 3.87 |
| heft_adapted | 54.5952% | 645/696 | 6138.74 | 7.26 |
| cgs_compute_adapted | 55.4916% | 647/696 | 6013.45 | 77.38 |
| marl_bc | 53.7072% | 641/696 | 6255.11 | 28.26 |

Scores/latencies are case-weighted. Completes are pooled. Timing is select_actions elapsed time under concurrent host load, not spacecraft inference latency.
