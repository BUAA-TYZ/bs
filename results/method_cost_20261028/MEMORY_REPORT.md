# Orin four-block input and execution memory

Measured on 2026-10-28 using AGX Orin, MAXN mode 0, unchanged dynamic clocks, original service-calibration Python/Torch 2.3.0 environment. No clock or power settings were changed. CPU governor and frequency bounds are recorded in clock_snapshot.json; privileged jetson_clocks status is unavailable. cuDNN TF32 remained enabled and matmul TF32 disabled, matching prior calibration.

Four scheduling blocks are materialized from the largest raw-byte input geometry in the registered M test-day input: 30000 x 30000 uint16, 100 unique 4096 windows, stride 3200. Their combined input buffers are 1,909,125,632 bytes. This is four scheduling blocks, not a GPU batch of four. All four buffers remain resident during inference; windows are preprocessed on demand. No full original raster or unrelated full queue cache is retained.

Same frozen YOLO11n-OBB FP32 weights, controlled SAR output fixture, uint16-to-uint8 preprocessing, full network, CPU output, controlled NMS and SRP1 encoding as prior service calibration. This is a hardware workload proxy, not an SAR recognition-accuracy test. Each group uses 12 warmup windows and three complete 100-window passes. The two-stream group uses two model copies, two streams, batch 1 per stream, CPU internal thread count 1.

| Quantity | One window | Two streams |
|---|---:|---:|
| Preprocessing-stage process RSS highwater (GB) | 2.374 | 2.380 |
| Whole-process RSS highwater (GB) | 3.035 | 3.160 |
| CUDA allocated peak during measurement (GB) | 4.832 | 9.697 |
| CUDA reserved peak during measurement (GB) | 5.694 | 11.329 |
| System RAM maximum reported by tegrastats (MB) | 14091 | 19489 |
| Incremental swap-in / swap-out (pages) | 0 / 0 | 0 / 0 |
| Service cohort P50 / P95 / P99 (s) | 0.577 / 0.624 / 0.641 | 1.140 / 1.205 / 1.220 |
| Measured cohorts | 300 | 150 |

GB uses 10^9 bytes. tegrastats MB is retained as emitted by the tool. System RAM includes other processes. Shared-memory RSS, CUDA reservation and system RAM counters overlap and must not be added. Preprocessing-stage highwater includes runtime and resident input buffers; it is not an isolated allocation delta. No OOM occurred. Zero *incremental* swap does not mean the device had no pre-existing swapped pages.

Both groups passed; the 100 per-window output hashes are identical over all three passes. Captured tegrastats samples: 1765 / 1726, at requested 100 ms; /proc samples at requested 50 ms. Sampling can miss brief system peaks, complemented by process/CUDA highwater counters. This supports only the tested buffer/execution configuration, not full-queue or flight feasibility.

`MEMORY_STATUS.json` contains checked report/telemetry hashes. `MEMORY_REGISTRATION.json` and `deployment_memory_v1.tgz` preserve the exact measurement registration/code deployment. The later Mac recording refinement adds control-view timing before recording starts; it does not change these completed hardware runs. Section 5.11 was updated to this limited evidence. METHOD-COST remains pending until actual recorded-decision replay completes.
