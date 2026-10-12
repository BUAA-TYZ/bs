# Reservation Development Comparison

| Policy | Delivery score | Complete tasks | Restricted mean (s) |
|---|---:|---:|---:|
| greedy | 68.9032% | 741/754 | 3874.8 |
| reservation | 73.9720% | 753/754 | 3176.3 |
| reservation_joint_sequential | 74.1218% | 753/754 | 3131.0 |
| reservation_joint | 74.1218% | 753/754 | 3131.0 |

Existing development cases; identical environment and candidate limits. Reservation policies additionally consume committed-work broadcasts and coordinate synchronously. No signaling overhead or guaranteed execution slots are modeled.
