import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analysis import run_sar_formal as formal


class FormalPanelTests(unittest.TestCase):
    def test_ready_day_partition_keeps_prior_day_capture_and_unchanged_tles(self):
        base = formal.read(formal.ROOT / "results/sar_loopfree_20261017/main_config.json")
        offset = 47 * 86400
        events = [{"release_s": offset + delta, "source_sat_id": 24,
                   "target_id": "la_harbor", "window_id": i}
                  for i, delta in enumerate((-65, -60, 0, 86400 - 65, 86400 - 60))]
        cfg = formal.daily_config(base, events, "2026-12-01")
        self.assertEqual([e["release_step"] * cfg["dt"] for e in cfg["scheduled_task_arrivals"]],
                         [0, 60, 86395])
        self.assertEqual(cfg["topology"]["tle_lines"], base["topology"]["tle_lines"])
        self.assertEqual(cfg["sim_steps"] * cfg["dt"], 86701)
        self.assertTrue(all(len(e["window_result_bytes"]) == 64 for e in cfg["scheduled_task_arrivals"]))

    def test_daily_macro_mean_and_zero_day_are_not_task_weighted(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            formal.save(out / "freeze.json", {})
            formal.save(out / "acquisition.json", {})
            jobs = []
            for condition in formal.GROUND:
                for day, date in enumerate(formal.DATES):
                    n = 0 if day == 5 else 10 if day == 0 else 1
                    for method in formal.METHODS:
                        on = n if method == "three_plan" and day == 0 else 0
                        outcomes = [{"task_id": f"task_{i}", "source_sat_id": 0, "release_seconds": 0}
                                    for i in range(n)]
                        key = f"{date}_{condition}_{method}"
                        path = out / f"{key}.json"
                        record = {"P300": on / n if n else None, "R300_seconds": (0 if on else 300) if n else None,
                                  "on_time_tasks": on, "total_tasks": n, "wall_seconds": 1,
                                  "on_time_task_ids": [f"task_{i}" for i in range(on)],
                                  "metrics": {"task_outcomes": outcomes}, "formal_job": key,
                                  "formal_freeze_sha256": formal.file_hash(out / "freeze.json")}
                        formal.save(path, record)
                        jobs.append({"key": key, "condition": condition, "date": date,
                                     "method": method, "record": str(path)})
            formal.save(out / "manifest.json", {"jobs": jobs,
                        "freeze_sha256": formal.file_hash(out / "freeze.json"),
                        "acquisition_sha256": formal.file_hash(out / "acquisition.json")})
            with patch.object(formal, "verify_freeze", return_value={}), patch.object(formal, "audit_record"):
                formal.report(out)
            summary = formal.read(out / "summary.json")
            mean = next(r for r in summary["day_equal_means"] if r["condition"] == "base" and r["method"] == "three_plan")
            self.assertEqual(mean["defined_days"], 5)
            self.assertEqual(mean["P300_day_equal"], .2)
            self.assertEqual(mean["R300_day_equal_s"], 240)
            self.assertEqual(len(summary["daily"]), 180)
            self.assertEqual(len(summary["daily_pairs"]), 150)
            wins = summary["wins_losses"][0]
            self.assertEqual((wins["win"], wins["tie"], wins["loss"], wins["undefined"]), (1, 4, 0, 1))


if __name__ == "__main__":
    unittest.main()
