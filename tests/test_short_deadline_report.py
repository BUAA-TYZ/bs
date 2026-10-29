import unittest

from analysis.report_short_deadline import aggregate, compare_on_time, score_record


def outcome(tid, latency=None, status="completed", release=0):
    return {"task_id": tid, "source_sat_id": 0, "release_seconds": release,
            "deadline_seconds": None, "latency_seconds": latency, "status": status}


def record(case, rows, method="greedy"):
    return {"case": {"id": case}, "method": method, "metrics": {
        "experiment": {"config": {"deadline_steps": 0, "sim_steps": 1000, "dt": 1}},
        "task_outcomes": rows}}


class ShortDeadlineTests(unittest.TestCase):
    def test_boundary_failures_and_primary_cap_not_sensitivity_maximum(self):
        result = score_record(record("a", [outcome("1", 300), outcome("2", 301),
                                             outcome("3", status="failed"), outcome("4", status="pending")]))
        self.assertEqual(result["on_time_tasks"], 1)
        self.assertEqual(result["on_time_rate"], .25)
        self.assertEqual(result["capped_mean_latency_300s"], 300)
        self.assertEqual(result["curve"][-1]["delivery_rate"], .5)

    def test_case_equal_primary_and_pooled_counts_remain_distinct(self):
        rows, _ = aggregate([record("a", [outcome("1", 10)]), record("b", [
            outcome("1", 301), outcome("2", 302), outcome("3", status="failed")])], ["greedy"])
        self.assertEqual(rows[0]["on_time_rate"], .5)
        self.assertEqual((rows[0]["on_time_tasks"], rows[0]["total_tasks"]), (1, 4))
        self.assertEqual(rows[0]["capped_mean_latency_300s"], 155)

    def test_on_time_pairing_does_not_confuse_eventual_completions(self):
        ref = record("a", [outcome("1", 300), outcome("2", 301)])
        cand = record("a", [outcome("1", 301), outcome("2", 300)])
        result = compare_on_time(ref, cand)
        self.assertEqual(result["both_on_time"], 0)
        self.assertEqual(result["reference_only_on_time"], 1)
        self.assertEqual(result["candidate_only_on_time"], 1)
        with self.assertRaises(ValueError):
            compare_on_time(ref, record("a", [outcome("1", 10, release=1), outcome("2", 10)]))

    def test_rejects_hard_expiry_and_inadequate_observation(self):
        row = record("a", [outcome("1", 10)])
        row["metrics"]["experiment"]["config"]["deadline_steps"] = 300
        with self.assertRaises(ValueError):
            score_record(row)
        with self.assertRaises(ValueError):
            score_record(record("a", [outcome("1", 10, release=401)]))

    def test_explicit_p300_horizon_keeps_pending_in_denominator(self):
        row = record("a", [outcome("1", 20), outcome("2", status="pending")])
        row["metrics"]["experiment"]["config"]["sim_steps"] = 301
        with self.assertRaises(ValueError):
            score_record(row)
        rows, _ = aggregate([row], ["greedy"], horizons=[300])
        self.assertEqual(rows[0]["delivery_rates"], [.5])
        self.assertEqual(rows[0]["total_tasks"], 2)
        self.assertEqual(rows[0]["capped_mean_latency_300s"], 160)
        row["metrics"]["experiment"]["config"]["sim_steps"] = 299
        with self.assertRaises(ValueError):
            aggregate([row], ["greedy"], horizons=[300])


if __name__ == "__main__":
    unittest.main()
