# Exported Training Validation

Existing training validation trajectories; paired controlled-proxy simulation, not a new evaluation run

Verified exact continuation after episode 2: only the resume flag changed; original model/log and validation audit hashes are retained.

BC: episode 0, teacher_bc.pt tensors equal validated warmstart.pt. PPO: last.pt episode 8.
Timing values are null because training validation did not measure them. Source artifact hashes and exact per-case action counts are retained.

| Method | F30 | F60 | F120 | F240 | J | Restricted latency (s) | Completed / total |
|---|---:|---:|---:|---:|---:|---:|---:|
| bc | 56.946% | 79.457% | 83.375% | 97.536% | 79.328% | 3002.03 | 1015 / 1015 |
| ppo | 55.008% | 76.392% | 80.892% | 97.536% | 77.457% | 3249.17 | 1015 / 1015 |
