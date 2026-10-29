import unittest

from analysis.audit_overlap_opportunity import graph_stats


class OpportunityTests(unittest.TestCase):
    def test_hop_limit_does_not_turn_component_membership_into_exit_reachability(self):
        distances, components = graph_stats(6, {(0, 1), (1, 2), (2, 3), (4, 5)}, [0], 2)
        self.assertEqual(distances, {0: 0, 1: 1, 2: 2})
        self.assertEqual(components, [4, 2])

    def test_multiple_exits_and_disconnected_singleton(self):
        distances, components = graph_stats(5, {(0, 1), (1, 2), (2, 3)}, [0, 3], 1)
        self.assertEqual(distances, {0: 0, 3: 0, 1: 1, 2: 1})
        self.assertEqual(components, [4, 1])
        self.assertEqual(graph_stats(5, {(0, 1)}, [], 8), ({}, [2, 1, 1, 1]))


if __name__ == "__main__":
    unittest.main()
