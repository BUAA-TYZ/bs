import copy
import json
from pathlib import Path
import tempfile
import unittest

from analysis.report_overlap_study import build_panel, load_directory, main, training_report
from sim.evaluation import task_age_score


def write_json(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


def fixture(root, methods=("local", "cgs_compute_adapted"), trace="trace", counts=(1, 3)):
    root.mkdir()
    cfg = {"pipeline_mode": "distributed", "sim_steps": 21600, "dt": 1,
           "task_arrival_end_step": 7200, "num_sats": 2, "task_source_sat_ids": [0],
           "ground_stations": [{"id": "gs"}]}
    cases = [{"case": {"id": f"validation_near_light_{i}", "seed": i}, "config": cfg}
             for i in range(1, len(counts) + 1)]
    manifest = {"args": {"methods": list(methods), "split": "validation"},
                "cases": cases, "checkpoint_sha256": {}, "source_sha256": "source",
                "delivery_horizons_seconds": [1800, 3600, 7200, 14400]}
    names = []
    for i, (case, count) in enumerate(zip(cases, counts), 1):
        for method in methods:
            outcomes = [{"task_id": f"task_{j}", "source_sat_id": 0, "release_seconds": 0,
                         "deadline_seconds": None, "status": "completed" if i == 1 else "pending",
                         "latency_seconds": 60 if i == 1 else None,
                         "raw_relay_hops": 0, "result_relay_hops": 2} for j in range(count)]
            experiment = {"source_sha256": "source", "task_trace_sha256": f"{trace}{i}",
                          "tle_sha256": "tle", "workload_sha256": "workload", "config": cfg,
                          "elapsed_sim_seconds": 21600}
            record = {"case": case["case"], "method": method, "checkpoint_sha256": None,
                      "metrics": {"experiment": experiment,
                                  "overall": {"completed_tasks": count if i == 1 else 0, "total_tasks": count},
                                  "task_outcomes": outcomes,
                                  "resource": {"compute_busy_time": {"0": 216, "1": 0},
                                               "ground_compute_busy_time": {}},
                                  "network": {"sent_mb": {"isl:0->1": 2, "ground:gs": 1}}},
                      "task_age": task_age_score(outcomes, manifest["delivery_horizons_seconds"], 21600)}
            name = f"{case['case']['id']}_{method}.json"
            names.append(name)
            write_json(root / name, record)
    write_json(root / "run.json", manifest)
    write_json(root / "summary.json", {**manifest, "records": names})
    return root


class OverlapReportTests(unittest.TestCase):
    def test_equal_case_score_pooled_counts_full_duration_and_real_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = fixture(Path(tmp) / "panel")
            report = build_panel("validation", [root])
            row = report["aggregate"][0]
            self.assertEqual(row["delivery_score"], .5)
            self.assertEqual(row["completed_tasks"], 1)
            self.assertEqual(row["total_tasks"], 4)
            self.assertEqual(row["restricted_mean_latency_seconds"], 7230)
            self.assertEqual(row["resource"]["source_compute_s"], 432)
            self.assertEqual(row["resource"]["source_capacity_s"], 43200)
            self.assertEqual(row["resource"]["source_utilization"], .01)
            self.assertEqual(row["resource"]["non_source_utilization"], 0)
            self.assertEqual(row["resource"]["isl_sent_mb"], 4)
            self.assertEqual(row["resource"]["ground_sent_mb"], 2)
            self.assertEqual(report["conditions"][0]["condition"], "near_light")

    def test_same_label_merges_disjoint_methods_only_with_identical_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = fixture(root / "local", ("local",))
            second = fixture(root / "cgs", ("cgs_compute_adapted",))
            merged = build_panel("validation", [first, second])
            self.assertEqual(merged["record_count"], 4)
            self.assertEqual(merged["comparisons"][0]["ties"], 2)
            with self.assertRaisesRegex(ValueError, "duplicate case-method"):
                build_panel("validation", [first, first])
            mismatch = fixture(root / "mismatch", ("greedy",), trace="other")
            with self.assertRaisesRegex(ValueError, "task trace differs"):
                build_panel("validation", [first, mismatch])
            fewer = fixture(root / "fewer", ("greedy",), counts=(1,))
            with self.assertRaisesRegex(ValueError, "case set"):
                build_panel("validation", [first, fewer])

    def test_missing_summary_records_and_mutated_results_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = fixture(Path(tmp) / "panel")
            summary = json.loads((root / "summary.json").read_text())
            (root / "summary.json").unlink()
            with self.assertRaisesRegex(ValueError, "Completed summary"):
                load_directory(root)
            short = copy.deepcopy(summary)
            short["records"].pop()
            write_json(root / "summary.json", short)
            with self.assertRaisesRegex(ValueError, "record list"):
                load_directory(root)
            write_json(root / "summary.json", summary)
            path = root / summary["records"][0]
            record = json.loads(path.read_text())
            for key in ("source_sha256", "task_trace_sha256", "workload_sha256"):
                mutated = copy.deepcopy(record)
                mutated["metrics"]["experiment"][key] = "changed"
                write_json(path, mutated)
                with self.subTest(key=key), self.assertRaises(ValueError):
                    load_directory(root)
            mutated = copy.deepcopy(record)
            mutated["task_age"]["delivery_score"] = .3
            write_json(path, mutated)
            with self.assertRaisesRegex(ValueError, "disagree"):
                load_directory(root)

    def test_warmstart_only_selection_is_not_rl_gain(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "run.json", {"training_args": {"episodes": 1}})
            write_json(root / "warmstart.json", {"episodes": [{"samples": 10, "action_counts": {"1": 10}}],
                                                 "pooled_fits": [{"samples": 10, "agreement": 1}],
                                                 "validation": {"delivery_score": .8}})
            (root / "train_log.jsonl").write_text(json.dumps({"episode": 1, "is_best": False,
                                                              "validation": {"delivery_score": .7}}) + "\n")
            report = training_report(root)
            self.assertEqual(report["selected_episode_from_log"], 0)
            self.assertIn("no PPO improvement", report["selection_note"])
            self.assertEqual(report["status"], "complete")
            self.assertEqual(report["bc"]["teacher_action_counts"], {"1": 10})

    def test_cli_keeps_different_panels_separate_and_renders_png_pdf(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = fixture(root / "first", ("local",))
            second = fixture(root / "second", ("cgs_compute_adapted",), trace="different")
            out = root / "report"
            main(["--panel", f"one={first}", "--panel", f"two={second}", "--out-dir", str(out)])
            report = json.loads((out / "summary.json").read_text())
            self.assertEqual(len(report["panels"]), 2)
            self.assertNotIn("aggregate", report)
            self.assertEqual(len(report["figures"]), 12)
            self.assertTrue(all((out / name).stat().st_size > 1000 for name in report["figures"]))
            self.assertIn("Full observation: [6.0] hours", (out / "summary.md").read_text())


if __name__ == "__main__":
    unittest.main()
