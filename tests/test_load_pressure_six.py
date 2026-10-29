import unittest
from unittest.mock import patch

from analysis import run_load_pressure_six as six


def row(sid, sources=(1, 2)):
    return {"sat_id": sid, "waiting_source_count": len(sources), "source_ids": list(sources),
            "task_ids": [f"task_{s}" for s in sources]}


class SixHourTests(unittest.TestCase):
    def test_overlapping_nodes_have_single_network_event(self):
        tracker = six.Episodes(10, 20)
        tracker.update(11, [row(24)])
        tracker.update(12, [row(24), row(25)])
        tracker.update(13, [row(25)])
        tracker.update(14, [])
        result = tracker.finish()
        self.assertEqual(result["node_distribution"]["count"], 2)
        self.assertEqual(result["node_distribution"]["total_s"], 4)
        self.assertEqual(result["network_distribution"]["count"], 1)
        self.assertEqual(result["network_distribution"]["total_s"], 3)

    def test_identity_change_does_not_split_and_boundary_is_censored(self):
        tracker = six.Episodes(10, 20)
        tracker.update(10, [row(24)])
        tracker.update(15, [row(24, (2, 3, 4))])
        result = tracker.finish()
        event = result["node_events"][0]
        self.assertEqual(event["duration_s"], 10)
        self.assertEqual(event["source_ids"], [1, 2, 3, 4])
        self.assertEqual(event["peak_sources"], 3)
        self.assertTrue(event["left_censored"] and event["right_censored"])

    def test_single_source_not_counted_and_gap_splits(self):
        tracker = six.Episodes(0, 10)
        tracker.update(1, [row(24, (1,))])
        tracker.update(2, [row(24)])
        tracker.update(2.1, [])
        tracker.update(3, [row(24)])
        tracker.update(3.5, [])
        result = tracker.finish()
        self.assertEqual(result["node_distribution"]["count"], 2)
        self.assertAlmostEqual(result["node_distribution"]["total_s"], .6)
        self.assertEqual(result["node_distribution"]["duration_bins"]["(0,1]"], 2)

    def test_failed_gate_cannot_launch_information_stage(self):
        first = {"methods": {}, "failures": [], "criteria": {"candidate": False}}
        with patch.object(six, "read", return_value={}), patch.object(six.short, "verify"), \
             patch.object(six, "stage_run", return_value=first) as run, \
             patch.object(six, "report"), patch.object(six, "write_json"), patch.object(six, "sha", return_value="hash"):
            six.panel()
        run.assert_called_once_with("neutral", {})

    def test_passed_gate_launches_exactly_one_information_stage(self):
        first = {"methods": {}, "failures": [], "criteria": {"candidate": True}}
        second = {"methods": {}, "failures": []}
        with patch.object(six, "read", return_value={}), patch.object(six.short, "verify"), \
             patch.object(six, "stage_run", side_effect=[first, second]) as run, \
             patch.object(six, "report"), patch.object(six, "write_json"), patch.object(six, "sha", return_value="hash"):
            six.panel()
        self.assertEqual([c.args[0] for c in run.call_args_list], ["neutral", "information"])


if __name__ == "__main__":
    unittest.main()
