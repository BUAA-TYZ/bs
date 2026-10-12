# Value Calibration and PPO

Same actor initialization; same PPO seed and interaction budget.
Each calibrated arm additionally uses the same 18 calibration scenarios; 9 probe scenarios are not fitted.

| Critic | Probe active MSE | Probe normalized MSE | Probe explained variance |
|---|---:|---:|---:|
| initial | 5.538989 | 23.4175 | -0.0240 |
| sequential | 1.844824 | 6.2932 | 0.5603 |
| pooled | 0.709737 | 3.1641 | 0.2373 |

| Policy | Episode | Delivery score | Complete tasks | Restricted mean (s) |
|---|---:|---:|---:|---:|
| control | 0 | 69.2520% | 742 | 3850.0 |
| control | 9 | 69.1184% | 742 | 3865.3 |
| pooled | 0 | 69.2520% | 742 | 3850.0 |
| pooled | 9 | 69.0004% | 741 | 3890.9 |
| sequential | 0 | 69.2520% | 742 | 3850.0 |
| sequential | 9 | 69.0004% | 741 | 3890.6 |

Critic accuracy and scheduling improvement are separate claims. Full limitations and checks are in summary.json.
