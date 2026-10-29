import unittest

from analysis.audit_sar_hop_bound import hop_bound


class HopBoundTests(unittest.TestCase):
    def test_temporal_wait_retains_hop_budget(self):
        epochs = [(0, {0: {1}, 1: {0}, 2: set()}, set()),
                  (10, {0: set(), 1: {2}, 2: {1}}, {2})]
        self.assertFalse(hop_bound(0, 0, epochs, 10, 0, 1)["potentially_on_time"])
        row = hop_bound(0, 0, epochs, 10, 0.25, 2)
        self.assertTrue(row["potentially_on_time"])
        self.assertEqual(row["hops_at_first_ground"], 2)
        self.assertEqual(row["latency_lower_bound_s"], 10.25)

    def test_same_epoch_closure_and_deadline(self):
        graph = {0: {1}, 1: {0, 2}, 2: {1}}
        self.assertTrue(hop_bound(0, 0, [(0, graph, {2})], 10, 0, 2)["potentially_on_time"])
        self.assertFalse(hop_bound(0, 0, [(300, graph, {2})], 10, 0.25, 2)["potentially_on_time"])

    def test_later_shorter_route_keeps_remaining_hops(self):
        epochs = [(0, {0: {1}, 1: {0, 2}, 2: {1}, 3: set()}, set()),
                  (10, {0: {2}, 1: set(), 2: {0, 3}, 3: {2}}, {3})]
        self.assertTrue(hop_bound(0, 0, epochs, 10, 0, 2)["potentially_on_time"])


if __name__ == "__main__":
    unittest.main()
