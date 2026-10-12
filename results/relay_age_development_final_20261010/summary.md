# Relay Task-Age Development Results

One training seed; existing validation cases. Not an independent test.

| Method | Mean delivery score | 30 min | 60 min | 120 min | 240 min | Eventually delivered |
|---|---:|---:|---:|---:|---:|---:|
| ground_compute | 31.85% | 12.92% | 22.66% | 37.95% | 53.88% | 384/754 |
| ground_tiles | 32.49% | 13.19% | 22.83% | 38.23% | 55.68% | 399/754 |
| ground_relay | 35.14% | 15.99% | 25.62% | 42.52% | 56.46% | 452/754 |
| local | 10.21% | 0.00% | 0.00% | 8.08% | 32.75% | 316/754 |
| greedy | 68.90% | 30.35% | 61.01% | 89.37% | 94.87% | 741/754 |
| window_aware | 66.80% | 28.51% | 57.63% | 86.27% | 94.79% | 741/754 |
| bc | 52.72% | 23.27% | 41.76% | 65.87% | 80.00% | 586/754 |
| ppo_9 | 53.14% | 23.72% | 42.31% | 66.42% | 80.10% | 587/754 |
| ppo_18 | 60.67% | 26.04% | 49.82% | 76.94% | 89.87% | 687/754 |

Selected by fixed validation rule: ppo_18.
