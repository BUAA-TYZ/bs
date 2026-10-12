# Reservation Development Comparison

| Policy | Delivery score | Complete tasks | Restricted mean (s) |
|---|---:|---:|---:|
| greedy | 68.9032% | 741/754 | 3874.8 |
| reservation_joint_sequential | 74.1218% | 753/754 | 3131.0 |
| reservation_calendar | 74.2441% | 753/754 | 3119.6 |
| reservation_guarded | 73.5355% | 751/754 | 3191.9 |
| reservation_multihop | 73.8105% | 752/754 | 3128.5 |
| reservation_forecast | 72.9642% | 752/754 | 3238.1 |

Existing development cases; identical environment and candidate limits. Reservation policies additionally consume committed-work broadcasts and coordinate synchronously. No signaling overhead or guaranteed execution slots are modeled.
