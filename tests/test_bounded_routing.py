from dataclasses import replace
from types import SimpleNamespace
import unittest

from sim.entities import Link
from sim.routing import delivery_route
from sim.topology import link_key
from test_result_relay import RelayVisibility
from test_simulation import make_env


class BoundedRoutingTests(unittest.TestCase):
    def env(self, count, edges, windows, max_hops=8, contact_ends=None):
        env = make_env(num_sats=count, dt=0.1, relay_max_hops=max_hops)
        self.addCleanup(env.close)
        env.topology.links = {link_key(a, b): Link(a, b, True, bandwidth, 0)
                              for a, b, bandwidth in edges}
        env.vis_cache = RelayVisibility(windows)
        if contact_ends is not None:
            env.cfg = replace(env.cfg, relay_contact_guard=True,
                              topology={**env.cfg.topology, "predict_contacts": True})
            env.topology.cfg = SimpleNamespace(bandwidth_mbps_min=1000)
            env.contact_cache = SimpleNamespace(scan_s=0.1, windows_at=lambda _: {
                key: [(0, contact_ends.get(key, 100) + 0.1)] for key in env.topology.links})
        return env

    def route(self, env, source=0, visited=None, ready_delay=0):
        return delivery_route(env, source, 1, env.topology.links,
                              [source] if visited is None else visited, ready_delay=ready_delay)

    def test_fast_eight_hop_path_does_not_hide_feasible_three_hop_path(self):
        edges = [(sid, sid + 1, 80) for sid in range(8)]
        edges += [(0, 9, 16), (9, 10, 16), (10, 8, 16)]
        env = self.env(11, edges, {8: (0, 100)}, max_hops=3)
        self.assertEqual(self.route(env), ("sat", 9))
        env.cfg = replace(env.cfg, relay_max_hops=8)
        self.assertEqual(self.route(env), ("sat", 1))

    def test_expired_shortest_path_does_not_hide_later_valid_path(self):
        env = self.env(5, [(0, 1, 16), (1, 4, 80), (0, 2, 16), (2, 4, 16)],
                       {4: (0, 100)}, max_hops=2, contact_ends={"1-4": 0.4})
        self.assertEqual(self.route(env), ("sat", 2))

    def test_ready_delay_counts_against_every_contact_deadline(self):
        env = self.env(5, [(0, 1, 16), (1, 4, 80), (0, 2, 8), (2, 4, 8)],
                       {4: (0, 100)}, max_hops=2, contact_ends={"1-4": 0.8})
        self.assertEqual(self.route(env), ("sat", 1))
        self.assertEqual(self.route(env, ready_delay=0.3), ("sat", 2))
        self.assertIsNone(self.route(env, ready_delay=101))

    def test_consumed_hops_and_visited_nodes_are_respected(self):
        env = self.env(5, [(0, 1, 80), (1, 2, 80), (0, 3, 16), (3, 2, 16)],
                       {2: (0, 100)}, max_hops=3)
        self.assertEqual(self.route(env, visited=[1, 0]), ("sat", 3))
        self.assertIsNone(self.route(env, visited=[4, 1, 0]))

    def test_source_can_downlink_with_exhausted_hop_budget(self):
        env = self.env(3, [(0, 1, 80)], {0: (0, 100)}, max_hops=1)
        self.assertEqual(self.route(env, visited=[2, 0]), ("ground", "gs"))
        env.vis_cache = RelayVisibility({0: (5, 100)})
        self.assertIsNone(self.route(env, visited=[2, 0]))

    def test_ground_window_can_make_fewer_hops_preferable_to_faster_route(self):
        env = self.env(4, [(0, 1, 80), (1, 2, 80), (0, 2, 8)],
                       {2: (10, 100)}, max_hops=2)
        # 等待同一个站窗时，两条路径交付时间相同，应选更少跳数。
        self.assertEqual(self.route(env), ("sat", 2))


if __name__ == "__main__":
    unittest.main()
