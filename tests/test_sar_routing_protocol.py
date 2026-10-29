from copy import deepcopy
import unittest

from analysis.prepare_sar_routing_study import derive
from analysis.run_sar_frozen import invariants
from test_simulation import add_task, make_env


class RoutingProtocolTests(unittest.TestCase):
    def test_simple_path_budget_changes_only_public_routing(self):
        base = {"num_sats": 46, "relay_max_hops": 8, "input_relay_max_hops": 2,
                "topology": {"predict_contacts": True}}
        before = deepcopy(base)
        cfg = derive(base, True)
        self.assertEqual(cfg["relay_max_hops"], 45)
        self.assertEqual(cfg["relay_routing"], "temporal")
        self.assertEqual(cfg["input_relay_max_hops"], 2)
        self.assertEqual(base, before)
        self.assertEqual(set(cfg) - set(base), {"relay_routing"})

    def test_budget_tracks_constellation_size_and_legacy_remains_explicit(self):
        base = {"num_sats": 12, "relay_max_hops": 8, "topology": {"predict_contacts": True}}
        self.assertEqual(derive(base, True)["relay_max_hops"], 11)
        self.assertEqual(derive(base)["relay_max_hops"], 8)

    def test_execution_audit_rejects_repeated_nodes(self):
        env = make_env()
        self.addCleanup(env.close)
        tile = add_task(env)[0]
        tile.raw_relay_path = [0, 1, 0]
        checks = invariants(env)
        self.assertFalse(checks["passed"])
        self.assertEqual(checks["relay_path_errors"][0]["path_kind"], "raw")


if __name__ == "__main__":
    unittest.main()
