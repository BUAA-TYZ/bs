# Imitation Optimization Development Results

single-seed development optimization; no independent test or isolated attribution

| Method | Delivery score | Completed tasks | Restricted mean latency (s) |
|---|---:|---:|---:|
| old_mappo | 60.67% | 687/754 | 5065.4 |
| greedy | 68.90% | 741/754 | 3874.8 |
| pooled_greedy_bc | 69.25% | 742/754 | 3850.0 |
| dagger | 68.87% | 743/754 | 3841.9 |
| mappo_9 | 68.10% | 741/754 | 3971.8 |
| mappo_18 | 68.06% | 742/754 | 3959.4 |
| fixed_ppo_0 | 69.25% | 742/754 | 3850.0 |
| fixed_ppo_9 | 68.40% | 741/754 | 3966.5 |

Selected PPO episode: 0 (0 means DAgger initialization).
Best learned stage including the separately evaluated pooled BC: pooled_greedy_bc.
Historical source hashes differ due to training edits; all archived greedy and old-policy task delivery statuses and times were replayed exactly.
Relay-hop counter differences are retained in summary.json; this is not full trajectory equivalence.
