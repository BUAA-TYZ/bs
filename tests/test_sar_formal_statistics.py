import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.sar_formal_statistics import bootstrap, exact_sign_test, holm


class FormalStatisticsTests(unittest.TestCase):
    def test_exact_sign_test_small_sample_and_ties(self):
        self.assertEqual(exact_sign_test([1] * 6)["p_two_sided"], .03125)
        self.assertEqual(exact_sign_test([1] * 5 + [-1])["p_two_sided"], .21875)
        self.assertEqual(exact_sign_test([1] * 3 + [-1] * 3)["p_two_sided"], 1.)
        self.assertEqual(exact_sign_test([1, 1, 1, 1e-8, 0, -1e-8], 1e-6)["nonzero_days"], 3)
        self.assertEqual(exact_sign_test([0] * 6)["p_two_sided"], 1.)

    def test_bootstrap_pairs_are_day_unit_and_enumerated(self):
        self.assertEqual(bootstrap([2.] * 6), [2., 2.])
        low, high = bootstrap([0., 1.])
        self.assertAlmostEqual(low, .0375)
        self.assertAlmostEqual(high, .9625)
        low, high = bootstrap([-3., -2., -1., 1., 2., 3.])
        self.assertAlmostEqual(low, -high)

    def test_holm_corrects_as_one_family(self):
        self.assertEqual(holm([.03125] * 50), [1.] * 50)
        self.assertEqual(holm([.04, .01, .03]), [.06, .03, .06])


if __name__ == "__main__":
    unittest.main()
