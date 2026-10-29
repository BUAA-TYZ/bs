# Literature Adaptation Development Comparison

paired development comparison of literature adaptations; NOT original-paper numerical reproduction or final test

| Method | Delivery score | Complete | Restricted mean (s) | Scheduler elapsed time (s) |
|---|---:|---:|---:|---:|
| ground_compute | 31.8519% | 384/754 | 9493.91 | 0.00 |
| ground_tiles | 32.4851% | 399/754 | 9400.38 | 0.00 |
| ground_relay | 35.1445% | 452/754 | 8961.86 | 0.00 |
| local | 10.2087% | 316/754 | 12752.58 | 0.01 |
| greedy | 68.9032% | 741/754 | 3874.80 | 0.04 |
| window_aware | 66.8002% | 741/754 | 4125.89 | 0.06 |
| reservation_calendar | 74.2441% | 753/754 | 3119.55 | 5.62 |
| heft_adapted | 70.2083% | 745/754 | 3690.43 | 16.11 |
| cgs_compute_adapted | 74.1500% | 754/754 | 3116.13 | 22.50 |
| marl_bc | 69.2520% | 742/754 | 3849.98 | 25.55 |

Scores/latencies are case-weighted. Completes are pooled. Timing is select_actions elapsed time under concurrent host load, not spacecraft inference latency.
