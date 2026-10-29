import unittest
from types import SimpleNamespace

from analysis.diagnose_sar_failures import classify, stages


def tile(name, arrived, start, end):
    return {"tile_id": name, "created": 0, "start_tx": 1, "end_tx": arrived,
            "start_compute": start, "end_compute": end, "end_downlink": end,
            "processor": "ground", "failure_reason": None, "ground_relay_target": None}


class FailureAttributionTests(unittest.TestCase):
    def test_unique_image_barrier_is_not_a_compute_queue(self):
        rows = [tile("late_input", 5000, 5000, 5001), tile("compute_tail", 4, 5001, 5002)]
        label, evidence = classify(SimpleNamespace(release_time=0), rows, None, None, 1,
                                   unique_image=True)
        self.assertEqual(label, "input_delivery_wait")
        self.assertEqual(evidence["stage_seconds"]["input_assembly_wait"], 4996)
        self.assertEqual(evidence["stage_seconds"]["compute_queue"], 1)
        self.assertEqual(evidence["last_input_tile"]["tile_id"], "late_input")
        self.assertEqual(sum(evidence["stage_seconds"].values()), 5002)

    def test_independent_window_keeps_its_actual_queue(self):
        values = stages(tile("independent", 4, 100, 101))
        self.assertEqual(values["compute_queue"], 96)
        self.assertEqual(values["input_assembly_wait"], 0)

    def test_input_gate_cannot_follow_computation(self):
        with self.assertRaises(ValueError):
            stages(tile("bad", 4, 10, 11), input_ready_seconds=20)

    def test_receiver_activity_is_not_proof_of_contention(self):
        import numpy as np
        label, evidence = classify(SimpleNamespace(release_time=0), [tile("a", 500, 500, 501)],
                                   None, np.ones(600, dtype=bool), 1)
        self.assertEqual(label, "input_delivery_wait")
        self.assertEqual(evidence["receiver_busy_share"], 1)

    def test_physical_bound_takes_precedence(self):
        label, _ = classify(SimpleNamespace(release_time=0), [],
                            {"relay_potentially_on_time": False, "relay_ground_open_seconds": 500},
                            None, 1)
        self.assertEqual(label, "physical_no_window")
