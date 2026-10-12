import copy
import unittest

from analysis.task_age_delivery import summarize


class TaskAgeAnalysisTests(unittest.TestCase):
    def panel(self):
        runs = []
        for label in ("reference", "candidate"):
            for seed, count in ((1, 1), (2, 3)):
                outcomes = [{"task_id": f"task_{i}", "release_seconds": 0,
                             "status": "completed" if seed == 1 else "pending",
                             "latency_seconds": 10 if seed == 1 else None} for i in range(count)]
                runs.append({"label": label, "seed": seed, "scenario": {"id": str(seed)},
                             "metrics": {"overall": {"total_tasks": count}, "task_outcomes": outcomes,
                                         "experiment": {"source_sha256": "version",
                                                        "task_trace_sha256": str(seed),
                                                        "elapsed_sim_seconds": 20}}})
        return {"aggregate": {"reference": {}, "candidate": {}}, "seeds": [1, 2],
                "split": "test", "checkpoints": {}, "runs": runs}

    def test_macro_and_pooled_denominators_remain_distinct(self):
        result = summarize(self.panel(), [5, 10])
        row = result["aggregate"]["candidate"][1]
        self.assertEqual(row["mean_scenario_delivery_rate"], .5)
        self.assertEqual(row["pooled_delivery_rate"], .25)
        self.assertEqual(row["delivered_within_horizon"], 1)
        self.assertEqual(row["total_tasks"], 4)
        self.assertEqual(len(result["cases"]), 4)

    def test_panel_rejects_missing_cases_and_inconsistent_evidence(self):
        panel = self.panel()
        bad = copy.deepcopy(panel)
        bad["runs"].pop()
        with self.assertRaises(ValueError):
            summarize(bad, [10])
        for key in ("task_trace_sha256", "source_sha256"):
            bad = copy.deepcopy(panel)
            bad["runs"][-1]["metrics"]["experiment"][key] = "different"
            with self.subTest(key=key), self.assertRaises(ValueError):
                summarize(bad, [10])
        bad = copy.deepcopy(panel)
        bad["runs"][-1]["metrics"]["overall"]["total_tasks"] += 1
        with self.assertRaises(ValueError):
            summarize(bad, [10])


if __name__ == "__main__":
    unittest.main()
