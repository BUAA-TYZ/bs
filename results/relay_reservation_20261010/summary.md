# Reservation Development Comparison

| Policy | Delivery score | Complete tasks | Restricted mean (s) |
|---|---:|---:|---:|
| greedy | 68.9032% | 741/754 | 3874.8 |
| window_aware | 66.8002% | 741/754 | 4125.9 |
| reservation_independent | 73.0845% | 753/754 | 3324.0 |
| reservation | 73.9720% | 753/754 | 3176.3 |

Existing development cases; identical environment and candidate limits. Reservation policies additionally consume committed-work broadcasts and coordinate synchronously. No signaling overhead or guaranteed execution slots are modeled.
