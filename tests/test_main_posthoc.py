import unittest

from analysis.report_main_posthoc import latency_stats, score


class PosthocTests(unittest.TestCase):
    def test_threshold_is_inclusive(self):
        rows = [{"task_id": "a", "status": "completed", "latency_seconds": 60,
                 "observed_age_seconds": 346}]
        self.assertEqual(score(rows, 60)["P_lower"], 1)
        self.assertEqual(score(rows, 59)["P_lower"], 0)

    def test_short_followup_keeps_denominator_and_bounds(self):
        rows = [{"task_id": "a", "status": "completed", "latency_seconds": 400,
                 "observed_age_seconds": 500},
                {"task_id": "b", "status": "pending", "latency_seconds": None,
                 "observed_age_seconds": 346},
                {"task_id": "c", "status": "pending", "latency_seconds": None,
                 "observed_age_seconds": 600}]
        s = score(rows, 600)
        self.assertEqual(s["unknown_ids"], ["b"])
        self.assertEqual((s["P_lower"], s["P_upper"]), (1/3, 2/3))
        self.assertEqual(score(rows, 300)["unknown_count"], 0)
        self.assertEqual(score(rows, 300)["on_time_count"], 0)

    def test_unfinished_excluded_only_from_conditional_latency(self):
        rows = [{"task_id": "a", "status": "completed", "latency_seconds": 1000,
                 "observed_age_seconds": 2000},
                {"task_id": "b", "status": "pending", "latency_seconds": None,
                 "observed_age_seconds": 400}]
        stats = latency_stats(rows)
        self.assertEqual(stats["mean_completed_s"], 1000)
        self.assertEqual(stats["unfinished"], 1)
        self.assertEqual(score(rows, 600)["total"], 2)


if __name__ == "__main__":
    unittest.main()
