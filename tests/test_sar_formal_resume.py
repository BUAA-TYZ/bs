from copy import deepcopy
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("formal_resume", Path("analysis/resume_sar_formal.py"))
resume = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(resume)


def record():
    return {"P300": .5, "on_time_tasks": 1, "total_tasks": 2, "on_time_task_ids": ["task_0"],
            "R300_seconds": 200.0, "observation_end_seconds": 1000,
            "metrics": {"task_outcomes": [
                {"task_id": "task_0", "source_sat_id": 0, "release_seconds": 0,
                 "status": "completed", "latency_seconds": 100.0},
                {"task_id": "task_1", "source_sat_id": 0, "release_seconds": 0,
                 "status": "pending", "latency_seconds": None}]}}


class ResumeTests(unittest.TestCase):
    def test_tolerance_and_exact_counts(self):
        r = record()
        for error, expected in ((1e-14, "PASS"), (0.9e-6, "PASS"), (1.1e-6, "AUDIT_MISMATCH")):
            r["R300_seconds"] = 200 + error
            self.assertEqual(resume.metric_audit(r)["status"], expected)
        r = record()
        r["on_time_tasks"] = 2
        self.assertEqual(resume.metric_audit(r)["status"], "AUDIT_MISMATCH")
        r = record()
        r["total_tasks"] = 3
        self.assertEqual(resume.metric_audit(r)["status"], "AUDIT_MISMATCH")
        r = record()
        r["on_time_task_ids"] = ["task_1"]
        self.assertEqual(resume.metric_audit(r)["status"], "AUDIT_MISMATCH")

    def test_zero_tasks_and_nonfinite_r300(self):
        r = record()
        r.update(P300=None, R300_seconds=None, on_time_tasks=0, total_tasks=0, on_time_task_ids=[])
        r["metrics"]["task_outcomes"] = []
        self.assertEqual(resume.metric_audit(r)["status"], "PASS")
        r = record()
        r["R300_seconds"] = float("nan")
        self.assertEqual(resume.metric_audit(r)["status"], "AUDIT_MISMATCH")

    def job(self, out, key):
        cfg = out / (key + "_config.json")
        resume.save(cfg, {"seed": 1701})
        return {"key": key, "config": str(cfg), "config_sha256": resume.sha(cfg),
                "record": str(out / (key + "_record.json")), "method": "local", "method_code": "local"}

    def test_mismatch_saved_and_next_job_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            resume.save(out / "freeze.json", {})
            first, second = self.job(out, "bad"), self.job(out, "good")
            bad, good = record(), record()
            bad["R300_seconds"] += .001
            bad["wall_seconds"] = good["wall_seconds"] = 1
            frozen = {"source_sha256": "core", "files": {"analysis/run_sar_frozen.py": "runner"}}
            with patch.object(resume, "verify", return_value=(frozen, {})), \
                 patch.object(resume.old, "measured_job", side_effect=[bad, good]), \
                 patch.object(resume, "audit", side_effect=lambda r, *args: resume.metric_audit(r)):
                self.assertEqual(resume.worker(out, first), "AUDIT_MISMATCH")
                self.assertEqual(resume.worker(out, second), "PASS")
            issue = resume.read(out / resume.REPAIR / "audit_mismatches/bad.json")
            self.assertEqual(issue["config"], {"seed": 1701})
            self.assertIn("record", issue)
            self.assertEqual(issue["audit"]["expected"]["R300_seconds"], 200)
            self.assertTrue(Path(first["record"]).exists())

    def test_exception_contains_traceback_config_and_does_not_abort_next_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            resume.save(out / "freeze.json", {})
            first, second = self.job(out, "crash"), self.job(out, "next")
            good = record()
            good["wall_seconds"] = 1
            frozen = {"source_sha256": "core", "files": {"analysis/run_sar_frozen.py": "runner"}}
            with patch.object(resume, "verify", return_value=(frozen, {})), \
                 patch.object(resume.old, "measured_job", side_effect=[RuntimeError("injected failure"), good]), \
                 patch.object(resume, "audit", side_effect=lambda r, *args: resume.metric_audit(r)):
                self.assertEqual(resume.worker(out, first), "FAILED")
                self.assertEqual(resume.worker(out, second), "PASS")
            failed = resume.read(out / resume.REPAIR / "failures/crash.json")
            self.assertIn("Traceback", failed["traceback"])
            self.assertIn("injected failure", failed["traceback"])
            self.assertEqual(failed["config"], {"seed": 1701})

    def test_inventory_reuses_saved_success_and_does_not_retry_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            a, b, c = [self.job(out, k) for k in ("saved", "failed", "pending")]
            resume.save(a["record"], record())
            resume.save(resume.result_path(out, b), {"status": "FAILED"})
            with patch.object(resume, "audit", return_value={"status": "PASS"}):
                statuses = resume.inventory(out, {"jobs": [a, b, c]}, {})
            self.assertEqual(statuses, {"saved": "PASS", "failed": "FAILED"})

    def test_report_keeps_exception_days_without_biased_mean(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            resume.save(out / "freeze.json", {})
            resume.save(out / resume.REPAIR / "repair.json", {})
            jobs, statuses = [], {}
            for condition in resume.old.GROUND:
                for date in resume.old.DATES:
                    for method in resume.old.METHODS:
                        key = f"{date}_{condition}_{method}"
                        j = self.job(out, key)
                        j.update(condition=condition, date=date, method=method)
                        jobs.append(j)
                        statuses[key] = "FAILED" if key == "2026-12-01_base_local" else "PASS"
                        resume.save(j["record"], record())
            with patch.object(resume, "verify", return_value=({"files": {}}, {"jobs": jobs})), \
                 patch.object(resume, "inventory", return_value=statuses):
                resume.summarize(out)
            report = resume.read(out / "summary.json")
            self.assertEqual(len(report["daily"]), 180)
            self.assertEqual(len(report["daily_pairs"]), 150)
            self.assertEqual(len(report["issues"]), 1)
            mean = next(r for r in report["day_equal_means"] if r["condition"] == "base" and r["method"] == "local")
            self.assertEqual(mean["valid_days"], 5)
            self.assertIsNone(mean["P300_day_equal"])
            wins = next(r for r in report["wins_losses"] if r["condition"] == "base" and r["baseline"] == "local")
            self.assertEqual(wins["unavailable"], 1)
            self.assertIsNone(wins["delta_P300_day_equal_pp"])


if __name__ == "__main__":
    unittest.main()
