# Frozen-Scenario Geometry Audit

post-freeze descriptive geometry audit; no policy outcomes read; no configuration changed

| Split | Layout | Source direct time fraction | Source instantaneous relay-exit fraction | Mean components | Mean largest component |
|---|---|---:|---:|---:|---:|
| validation | near | 0.027141 | 0.040220 | 5.377 | 6.521 |
| validation | uniform | 0.027141 | 0.092188 | 7.494 | 9.042 |
| test | near | 0.026852 | 0.041204 | 5.387 | 6.475 |
| test | uniform | 0.026852 | 0.090162 | 7.488 | 9.057 |

- Sample-and-hold geometry at the configured 10 second period; not continuous truth.
- Instant relay reachability ignores rates, queues, transfer duration and future store-carry-forward.
- Geometric paths and potential channel seconds do not prove task delivery feasibility.
- Different conditions use different task seeds and link phases; comparisons are not exact paired interventions.
