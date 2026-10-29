import json
from pathlib import Path
import tempfile
import unittest

from analysis.audit_ground_sla_bound import attach_execution_witnesses, audit, classify_window_relaxation


def outcome(tid, release, latency=None):
    return {"task_id": tid, "source_sat_id": 0, "release_seconds": release,
            "latency_seconds": latency, "status": "completed" if latency is not None else "pending"}


class GroundWindowBoundTests(unittest.TestCase):
    def test_closed_deadline_open_window_end_and_release_inside_window(self):
        windows = [{"start_s": 10, "end_s": 20}, {"start_s": 320, "end_s": 330}]
        rows = [outcome("inside", 15), outcome("closed", 20), outcome("late", 19.5)]
        result = classify_window_relaxation(rows, windows, 700, 300)
        self.assertEqual([row["next_global_ground_open_seconds"] for row in result], [15, 320, 19.5])
        self.assertTrue(all(row["potentially_on_time"] for row in result))
        result = classify_window_relaxation([outcome("miss", 20)], windows, 700, 299.9)
        self.assertFalse(result[0]["potentially_on_time"])

    def test_no_future_contact_is_censored_not_infinite_physical_delay(self):
        result = classify_window_relaxation([outcome("none", 100)], [], 401, 300)[0]
        self.assertFalse(result["potentially_on_time"])
        self.assertIsNone(result["delivery_latency_lower_bound_seconds"])
        self.assertEqual(result["no_contact_through_seconds"], 401)

    def test_ground_work_may_finish_after_window_closes(self):
        result = classify_window_relaxation([outcome("ground", 0, 200)],
                                           [{"start_s": 0, "end_s": 10}], 301, 300)[0]
        self.assertTrue(result["potentially_on_time"])
        self.assertTrue(result["observed_on_time"])

    def test_requires_full_followup_and_rejects_counterexample(self):
        with self.assertRaisesRegex(ValueError, "coverage"):
            classify_window_relaxation([outcome("short", 10)], [], 310, 300)
        with self.assertRaisesRegex(AssertionError, "contradicts"):
            classify_window_relaxation([outcome("success", 0, 20)], [], 301, 300)

    def test_legacy_capacity_bound_cannot_be_used_for_mixed_ground_relay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "record.json"
            path.write_text(json.dumps({"metrics": {"experiment": {"config": {"ground_input_relay": True}}}}))
            with self.assertRaisesRegex(ValueError, "--window-only"):
                audit(path)

    def test_witnesses_do_not_stitch_successes_or_accept_different_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = []
            for name, rows in (("first", [outcome("a", 0, 20), outcome("b", 0)]),
                               ("second", [outcome("a", 0), outcome("b", 0, 20)])):
                record = {"method": name, "case": {"id": "day"}, "metrics": {
                    "experiment": {"config": {"pipeline_mode": "distributed"},
                                   "source_sha256": "source", "task_trace_sha256": "tasks", "tle_sha256": "tle"},
                    "task_outcomes": rows}}
                path = root / f"{name}.json"
                path.write_text(json.dumps(record))
                records.append((path, record))
            tasks = classify_window_relaxation(records[0][1]["metrics"]["task_outcomes"],
                                              [{"start_s": 0, "end_s": 100}], 301, 300)
            report = {"case": {"id": "day"}, "input": str(records[0][0]),
                      "tasks": tasks, "potentially_on_time_tasks": 2}
            attach_execution_witnesses([report], [records[1][0]], 300)
            self.assertFalse(report["offline_optimal_count_certified"])
            self.assertEqual(report["best_complete_execution_witness"]["on_time_tasks"], 1)
            records[1][1]["metrics"]["task_outcomes"][0] = outcome("a", 0, 30)
            records[1][0].write_text(json.dumps(records[1][1]))
            attach_execution_witnesses([report], [records[1][0]], 300)
            self.assertTrue(report["offline_optimal_count_certified"])
            records[1][1]["metrics"]["experiment"]["config"]["pipeline_mode"] = "ground_compute"
            records[1][0].write_text(json.dumps(records[1][1]))
            with self.assertRaisesRegex(ValueError, "execution context"):
                attach_execution_witnesses([report], [records[1][0]], 300)


if __name__ == "__main__":
    unittest.main()
