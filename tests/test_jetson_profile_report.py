import unittest

from analysis.report_jetson_profile import union_duration


class GpuIntervalTests(unittest.TestCase):
    def test_nested_and_cross_stream_intervals_count_once(self):
        self.assertEqual(union_duration([(5, 12), (0, 10), (2, 3), (12, 15), (20, 23)]), 18)

    def test_empty_and_negative_intervals(self):
        self.assertEqual(union_duration([]), 0)
        with self.assertRaises(ValueError):
            union_duration([(2, 1)])
