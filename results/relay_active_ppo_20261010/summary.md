# Active Decision PPO Comparison

single-seed paired development comparison; initial checkpoint is imitation, not RL

| Method | Episode | Delivery score | Completed tasks | Restricted mean (s) |
|---|---:|---:|---:|---:|
| initial_bc | 0 | 69.2520% | 742/754 | 3850.0 |
| greedy_archived | None | 68.9032% | 741/754 | 3874.8 |
| mixed | 9 | 68.4040% | 741/754 | 3966.5 |
| active2 | 9 | 69.1994% | 742/754 | 3844.4 |
| active16kl | 9 | 68.8443% | 743/754 | 3867.8 |

| Method | Actor updates | Median active rows/batch | Mean joint KL | KL stops | Best episode |
|---|---:|---:|---:|---:|---:|
| mixed | 2021 | 1 | 0.025085 | 0 | 0 |
| active2 | 20 | 153.0 | 0.000288 | 0 | 0 |
| active16kl | 137 | 170 | 0.006576 | 3 | 0 |

All runs have identical initial weights, initial evaluation outcomes and first training rollout summaries.
Full per-case results, source checks and per-update diagnostics are retained in summary.json.
