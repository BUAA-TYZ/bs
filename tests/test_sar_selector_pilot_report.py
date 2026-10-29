import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.report_sar_selector_pilot import latency_regret, regret_summary


class PilotReportTests(unittest.TestCase):
    def sample(self, finish):
        return {"label": "cgs", "rule_choice": "local", "agrees": False,
                "branches": {"cgs": {"completion_seconds": 120.},
                             "local": {"completion_seconds": finish, "censor_seconds": 7300.}}}

    def test_unclipped_actual_regret(self):
        self.assertEqual(latency_regret(self.sample(500.))["exact_seconds"], 380.)

    def test_censored_rule_has_only_lower_bound(self):
        r = latency_regret(self.sample(None))
        self.assertIsNone(r["exact_seconds"])
        self.assertEqual(r["strict_lower_bound_seconds"], 7180.)
        group = regret_summary([self.sample(500.), self.sample(None)])
        self.assertEqual(group["mean_exact_seconds"], 380.)
        self.assertEqual(group["all_wrong_mean_lower_bound_seconds"], 3780.)
        self.assertTrue(group["all_wrong_mean_is_strict_lower_bound"])

    def test_tie_label_mismatch_has_zero_regret(self):
        self.assertEqual(latency_regret(self.sample(120.))["exact_seconds"], 0.)


if __name__ == "__main__":
    unittest.main()
