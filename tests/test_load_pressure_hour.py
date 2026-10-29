import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from analysis import run_load_pressure_hour as hour


class HourProtocolTests(unittest.TestCase):
    def test_constrained_window_does_not_follow_unrelated_peak(self):
        events = [{"ready_s": 20000, "raw_bytes": 5},
                  {"ready_s": 22595, "raw_bytes": 10},
                  {"ready_s": 30000, "raw_bytes": 1000}]
        start, end, origin = hour.select_window(events)
        self.assertLessEqual(start, 22595)
        self.assertGreaterEqual(end, 23195)
        self.assertEqual(start, 19595)
        self.assertGreaterEqual(start-origin, 600)
        self.assertEqual(origin % 60, 0)

    def bound(self, intervals, source=0):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"cache.npz"
            visible = np.zeros((32, 168, 1), dtype=bool)
            visible[:, 25, 0] = True
            np.savez(path, intervals=np.asarray(intervals, dtype=float).reshape(-1, 4),
                     times=np.arange(0, 320, 10), ground_visible=visible)
            with patch.object(hour, "CACHE", path):
                return hour.compute_upper([{"task_id": "task_0", "ready_s": 0, "source_sat_id": source}])

    def test_source_cannot_bridge_computing_components(self):
        result = self.bound([(0, 24, 0, 310), (1, 24, 0, 310), (1, 25, 0, 310)])
        self.assertEqual(result["reachable"], 0)
        own = self.bound([(1, 24, 0, 310), (1, 25, 0, 310)], source=1)
        self.assertEqual(own["tasks"][0]["latency_s"], 0)

    def test_helper_store_and_forward_and_exclusive_contact_end(self):
        result = self.bound([(0, 24, 0, 5), (24, 25, 100, 110)])
        self.assertEqual(result["tasks"][0]["latency_s"], 100)
        closed = self.bound([(0, 24, -10, 0), (24, 25, 100, 110)])
        self.assertEqual(closed["reachable"], 0)


if __name__ == "__main__":
    unittest.main()
