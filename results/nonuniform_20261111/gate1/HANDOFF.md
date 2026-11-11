# Gate 1 handoff

2026-11-11 latest: six jobs PASS, independent report/audits PASS, Gate 1 NOT PASSED. All simulations and the Mac worker exited normally. Report, curves and negative mechanism analysis complete. Independent staged scan and full built-in scanner PASS (1.215 s, exit 0), no large or LFS additions. Commit/push is the final delivery step; verify the stage commit in origin/master before doing anything further. Do not publish, launch, train, or repeat the completed audits.

Final paired day-equal R600: baseline 70.637085 s, nu 69.026421 s (improvement 1.610664 s, below 2 s). P60 improvement 0.290610 percentage points, below 1 pp; P600 difference -0.005237 pp satisfies the protection threshold. Baseline matches historical raw R600. Six-concurrent Mac run took 80.894 minutes. See DELIVERY.md and NEGATIVE_MECHANISM.md; final dates remain sealed. Stop after push and wait for user confirmation.

- Registration SHA256: `9271cf47b080885c77f8c444ac10d84d9d6eb1bbebf9f18936b58e6dfc73f12a`.
- Manifest freezes 1503 source/input/config/test/document hashes. Do not modify frozen files during execution.
- Tests: 787 passed, including 19 new tests. `tests_final.log` and `TESTS_PASS.json` frozen. The explicit cleanup test-clock plugin affects only 17 legacy deadline tests; no production clock patch.
- Six jobs: baseline/nu x validation 2026-10-22, 23, 24, original M single-station six-hour windows. Same code and host. Final dates 2026-12-28/29/30 remain sealed.
- Worker PID 77422, caffeinate 77423; verify actual command and launch timestamps from LAUNCH.json and workers/local/launch.json. Six spawn processes, one Torch/BLAS thread each.
- Existing devbox3 HTTP queue restored only for this experiment; server PID 144873 at 2026-11-11T09:38:44Z, source hash verified equal to local. Old experiments in database were all PASS; no old workers restarted.
- New experiment: `nonuniform_gate1_20261111`. API: import `analysis.run_nonuniform_gate1_20261111 as r`; `r.api('/status',exp=r.EXPERIMENT)` is read-only. Tokens stay in user state directories, never print contents.
- Mac only executes; devbox1/2/3 do not receive simulation tasks. Source/hash preflight on Mac passed before publish. No simulation retry or ownership recycling on network interruption. FAILED blocks new claims; drain claimed work and report specific failure.
- Initial estimate: implementation/checks 1–2 h; six concurrent jobs 2–5 h. Implementation start precisely known as 09:17:14Z; earlier read-only exploration not timed. Record actual task and stage time.

## Completed Workflow and Remaining Submission

1. Wait in the current chat until all six finish; no new automation. Capture final HTTP status in QUEUE_FINAL.json only when complete. Retain original bytes and all failures.
2. Verify server artifacts byte hashes against local results and six unique job owners; write DISTRIBUTED_AUDIT.json. Server artifacts are named SHA256(experiment + '/' + job_id) under the existing queue server artifacts directory. Copy only this experiment's six artifacts into a new directory, never delete or overwrite previous evidence.
3. Run `.venv/bin/python -B -m analysis.report_nonuniform_gate1_20261111` once. It independently verifies registered source/input hashes, score IDs, metrics, arbitrary-group geometry/truth/bytes, resource segment conservation and result identities; computes paired metrics/bootstrap, mapping fractions and block prediction errors. Any report-only defect must be documented; never change/re-run scientific results to repair a report.
4. Check REPORT_COMPLETE/INDEPENDENT_AUDIT hashes and inspect curves.png. Report Gate 1 pass/fail only from this paired run, plus historical comparison. Prediction errors use predicted minus actual; both allocation and calendar estimates, completed block denominators and censor counts. Per-image spread requires every block completed.
5. Write DELIVERY.md with actual timing and submission scans. Find staged files >50 MB; necessary large files use LFS, no ordinary >100 MB; scan exact queue credential plus secret patterns without echoing matches. Preserve ignored runs/ and workbench/settings.json.
6. Run installed built-in scanner directly without the Git hook's 2-second wrapper timeout and retain status/digest, then normal git commit with hook enabled. Push origin/master, verify remote hash. Report commit, new LFS files/sizes, git status. Stop and await user confirmation before Gate 2 even if Gate 1 passes.

No files may be deleted in this stage. No paper changes, training, model selection, tuning, or new final test access. Clean-up and previous stages are complete and must not be resumed.
