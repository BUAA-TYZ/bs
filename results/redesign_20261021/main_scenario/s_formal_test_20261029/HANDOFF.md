# S formal test followup

## 2026-10-29 17:00 Beijing: COMPLETE, final notification

All 60/60 simulations passed; automatic reporting and independent verification completed at2026-10-29T08:20:56Z. FINAL_AUDIT PASS covers837 provenance hashes and3 protocols; FOLLOWUP_VERIFICATION PASS covers60 records,84 paired comparisons,336 metric intervals, complete curves/scopes and99 cross-scale rows. Followup verified all10 report hashes plus both REPORT_COMPLETE hashes, protocol and manifest hashes, and visually checked curves.png. Original coordinator/supervisor/caffeinate/pool have exited. No failure markers. Do not rerun.

Both S envelopes are complete and determinable. No model meets all four criteria. Fixed1701 passes P60/P600 only; fixed1702/1703 pass P60 only. All dynamic seeds fail all four envelope inequalities. These are experimental findings, not execution failures. No tuning or model selection follows.

Final delivery includes all20 S methods and the six frozen models individually across S/M/L, with full99-row union linked. S/M use6h, L1h; same dates, different scenarios, frozen models. Unfinished is a three-day sum and missing methods are NA, not zero. Three-day bootstrap intervals are descriptive; resource/OOD checks audit saved counters, not packet/observation replay.

S_NOTIFIED.json records this final delivery. Delete automation s-s-m-l and stop awaiting user instructions. No paper edits, SSH, commits, additional simulations or training.

## 2026-10-29 11:54 Beijing: RUNNING, supersedes earlier S cancellation

User explicitly restored S on2026-10-29. No paper directory edits, no SSH, no commits. All earlier M/L/METHOD-COST work is complete and notified; do not restart or repeat those reports. Only S remains active.

Protocol `../S_FORMAL_TEST_PROTOCOL_20261029.md` SHA256 `021b59e513b61c5a321312bfb59d6db0171ef5e12872fba7d52c4a748e9396e6`.
PREGEN_REGISTRATION SHA256 `7dae8ec3f4931b8e7b405d57a99953b8a5668afd1f5d3f90f60cd0d230317fd7`.
Manifest SHA256 `7cc7c90241ba2d8e0a90904a1088f3c73242baa912b25ab589c80c82dbf13c99`.
PREGEN registration recorded the protocol/source/model/range/estimate hashes before generating S orbit template. Manifest then froze template before exclusive S_TEST_OPENED at2026-10-29T03:52:13.849280Z. No repeat opening.

Supervisor25249 started03:52:09Z; coordinator25260 and caffeinate25261 started03:52:11Z. Command `analysis.run_s_formal_20261029 panel` under `.venv/bin/python`3.12.13. Verify command and lstart against launch.json, not PID alone. First nine workers25276..25284 are fixed three seeds by three dates. Main progress can remain acquisition12/12 until first result; consult validation/*.progress.json. No duplicate start, retry, extra pool or source/model/manifest edits while running.

Scope12SAR+72compute,1:6,12 planes per layer, original frozen scale formula/M physics. Dates2026-12-21/22/23, same dates different scenarios frozen models. Three new six-hour protocols generated in46.53s; scored image counts177/177/174, warmup625/615/605s after minute alignment, followup600s each. All methods share each day's protocol. These counts are task-generation metadata, not performance results.

20 methods60 records: six fixed/dynamic1701/1702/1703 models, six rules zero_1/zero_2/history_1/history_2/minload_1/minload_2 each4/16 (12), global_v3_16/global_v3_dynamic (2). Fixed checkpoints352/176/176, dynamic88/88/176 unchanged. Only zero-shot: no training, finetuning, checkpoint selection or tuning. Input ranges hash e967486e4acb9992effb7c07c59bbfed1a4aa807b4b111b9c5f5b4083a77939c unchanged. Local v3 already zero_2_16. CGS/HEFT/BC not added to S.

Single9CPU spawn pool/Torch1 thread, batches at most9, allPASS before next, no prefetch or retries. Failure drains same batch thenBLOCKED; inspect specific traceback before notification, do not fix/restart simulation. Deadline conservatively interprets user's "before11-05": November5 00:00 Beijing =2026-11-04T16Z. Supervisor terminates process group on deadline. No extension. Initial wall-only estimate19.2h including prep/audit and20% reserve;4xM-slowest stress69.7h versus156.3h remaining, so6h retained. Not a guarantee. If credible runtime projection exceeds cutoff, notify before deadline; missing records are not zero and cannot shrink envelope.

5 new testsPASS including20 real toy methods,60 synthetic report records and336 metric intervals independently verified; see../S_PRELAUNCH_CHECKS_20261029.json. Pre-registration test-fixture adjustments were not formal failures. Do not rerun tests during experiments.

## Completion path

After60PASS, panel automatically calls analysis.report_s_formal_20261029 and analysis.verify_s_formal_report_20261029, then writes REPORT_COMPLETE.json. final_status COMPLETE means simulations only; require REPORT_COMPLETE and independent_audit_20261029/FOLLOWUP_VERIFICATION PASS. report_failure.json preserves reporting error without erasing simulation completion; supervisor_failure reports deadline/abnormal process exit. Do not rerun successful audit. Check hashes before using artifacts.

Report artifacts: FINAL_AUDIT.json, REPORT.md, PAIRWISE.json, full_curves.json, FEATURE_AUDIT.json, GRANULARITY.json, TAIL_RECORDS.json, S_M_L_COMPARISON.json/md, curves.png, REPORT_HASHES.json, FOLLOWUP_VERIFICATION.json. Open curves.png for visual check after automatic nonblank pixel check.

Audit all source/model/input/truth hashes, three protocols/scored IDs/real600 followup, day-equal P60/90/120/180/240/300/600 and full staircase, completed mean/P50/P90, R600 and unfinished. Resource classification and warmup/main/followup conservation include redundant multihop control bytes, compute/ground utilization, requested/actual granularity. Candidate histograms/TopK and encoded OOD per dimension/role/known/block count with1e-6 and padding excluded; this audits saved counters, not per-observation or packet replay, historical pre-clipping values unavailable.

Full fixed_six_local_16 and dynamic_twelve_local_4_16 envelopes: first day-equal each method, then across baselines strongest P60/P600 maxima and latency/R600 minima, with ties/sources listed. Fixed models tested against6, dynamic against12. P60 strictly higher, mean strictly lower, P600 not decreased, R600 strictly lower. All4 required. P600 tie is tie but passes non-decrease. Global references excluded. Recompute S thresholds; do not borrow M/L thresholds or shrink missing sets. Each of6models versus14baselines yields84comparisons/336metric paired-day bootstrap intervals,10000 draws seed20261027;3days only, descriptive/discrete, no selection-bias correction.

## Notify and stop

Deliver S main table and S/M/L P60/P600/R600/unfinished comparison. Include all six models separately, full-method union in linked report; label L1h and S/M6h. Unfinished is three-day sum; unequal windows/loads make raw counts unsuitable for direct scale ranking. NA means method not registered, never zero. Say "同日期、不同场景，模型冻结". Keep L original incomplete6/12scope conclusions unchanged. Do not change paper directory.

Automation `s-s-m-l` runs hourly on this chat. Healthy running/waiting silent, notify only completion/new failure/action needed; do not send partial performance. After verified reports and visual check, notify once, write S_NOTIFIED.json and update this header/parent execution status, delete automation `s-s-m-l`, then stop awaiting user instructions. No further experiment, training, SSH or paper changes.
