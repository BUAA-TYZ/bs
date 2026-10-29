from dataclasses import replace
from types import SimpleNamespace
import unittest

from sim.entities import Link
from sim.routing import delivery_route
from sim.topology import link_key
from sim.cache.contacts import reconcile_windows
from test_bounded_routing import BoundedRoutingTests


class TemporalRoutingTests(unittest.TestCase):
    def test_down_snapshot_preserves_future_part_of_coarse_forecast(self):
        self.assertEqual(reconcile_windows([(0, 364), (500, 60)], False, 30),
                         [(30, 334), (500, 60)])
        self.assertEqual(reconcile_windows([(0, 20)], False, 30), [])
        self.assertEqual(reconcile_windows([(0, 364)], True, 30), [(0, 364)])

    def env(self, count, edges, windows, contacts, max_hops=8):
        helper = BoundedRoutingTests()
        self.addCleanup(helper.doCleanups)
        env = helper.env(count, edges, windows, max_hops)
        env.cfg = replace(env.cfg, relay_routing="temporal", relay_contact_guard=True,
                          topology={**env.cfg.topology, "predict_contacts": True})
        env.topology.cfg = SimpleNamespace(bandwidth_mbps_min=80)
        env.contact_cache = SimpleNamespace(scan_s=0.1, windows_at=lambda _: contacts)
        return env

    def route(self, env, source=0, visited=None, **kw):
        return delivery_route(env, source, 1, env.topology.links,
                              [source] if visited is None else visited, **kw)

    def test_waits_for_future_shortcut_instead_of_consuming_hops(self):
        env = self.env(4, [(0, 1, 80), (1, 3, 80)], {3: (50, 100), 2: (5, 10)},
                       {"0-1": [(0, 100)], "1-3": [(0, 100)], "0-2": [(5, 10)]}, 2)
        self.assertIsNone(self.route(env))
        env.cfg = replace(env.cfg, relay_routing="snapshot")
        self.assertEqual(self.route(env), ("sat", 1))

    def test_current_first_hop_can_lead_to_future_second_hop(self):
        env = self.env(3, [(0, 1, 80)], {2: (5, 20)},
                       {"0-1": [(0, 100)], "1-2": [(5, 10)]}, 2)
        self.assertEqual(self.route(env), ("sat", 1))
        self.assertIsNone(self.route(env, source=1, visited=[0, 1]))

    def test_never_uses_a_forecast_edge_that_is_currently_down(self):
        env = self.env(2, [], {1: (0, 20)}, {"0-1": [(0, 20)]})
        self.assertIsNone(self.route(env))

    def test_can_plan_through_currently_down_intermediate_future_contact(self):
        env = self.env(3, [(0, 1, 80)], {2: (5, 20)},
                       {"0-1": [(0, 100)], "1-2": [(0, 100)]}, 2)
        env.topology.links["1-2"] = Link(1, 2, False, 80, 0)
        self.assertEqual(self.route(env), ("sat", 1))
        self.assertIsNone(self.route(env, source=1, visited=[0, 1]))

    def test_hop_budget_history_and_memory_are_respected(self):
        env = self.env(4, [(0, 1, 80), (1, 2, 80)], {2: (0, 20)},
                       {"0-1": [(0, 20)], "1-2": [(0, 20)]}, 2)
        self.assertIsNone(self.route(env, visited=[3, 0]))
        self.assertIsNone(self.route(env, visited=[1, 0]))
        env.satellites[1].mem_used_gb = env.satellites[1].mem_capacity_gb
        self.assertIsNone(self.route(env))

    def test_ground_is_allowed_with_no_remaining_isl_hops(self):
        env = self.env(2, [], {0: (0, 20)}, {}, 1)
        self.assertEqual(self.route(env, visited=[1, 0]), ("ground", "gs"))
        self.assertIsNone(self.route(env, target_gs_id="missing"))

    def test_future_larger_contact_does_not_send_in_insufficient_current_contact(self):
        env = self.env(2, [(0, 1, 80)], {1: (0, 20)},
                       {"0-1": [(0, 0.15), (5, 10)]})
        self.assertIsNone(self.route(env))

    def test_simple_path_budget_reaches_beyond_eight_without_revisiting(self):
        edges = [(sid, sid + 1, 80) for sid in range(11)]
        contacts = {f"{sid}-{sid + 1}": [(0, 100)] for sid in range(11)}
        env = self.env(12, edges, {11: (0, 100)}, contacts, max_hops=8)
        self.assertIsNone(self.route(env))
        env.cfg = replace(env.cfg, relay_max_hops=11)
        for source in range(11):
            self.assertEqual(self.route(env, source, list(range(source + 1)), target_gs_id="gs"),
                             ("sat", source + 1))
        self.assertEqual(self.route(env, 11, list(range(12))), ("ground", "gs"))
        # 唯一地面出口已在访问历史中，也不能为了更长预算回访。
        self.assertIsNone(self.route(env, 10, [11, 10]))


if __name__ == "__main__":
    unittest.main()
