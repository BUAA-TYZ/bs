import copy
from graphlib import CycleError
import hashlib
import importlib.util
import math
from pathlib import Path
import random
import sys
import unittest

from sim.entities import ActionType
from sim.marl.decision import decision_state, candidate_neighbors
from sim.scheduler.contact_graph import Contact, ContactPlan, choose_opportunity
from sim.scheduler.heft import Placement, first_gap, schedule
from sim.scheduler.literature import CGSComputeAdapted, HEFTAdapted
from test_reservation import contention_state
from test_simulation import add_task, make_env


class HEFTReferenceTests(unittest.TestCase):
    def test_original_paper_figure_three_schedule_length_eighty(self):
        # 原文 Fig. 3 的完整十节点样例；Fig. 4(a) 给出的 makespan 为 80。
        matrix = [(14, 16, 9), (13, 19, 18), (11, 13, 19), (13, 8, 17),
                  (12, 13, 10), (13, 16, 9), (7, 15, 11), (5, 11, 14),
                  (18, 12, 20), (21, 7, 16)]
        costs = {i + 1: dict(enumerate(row)) for i, row in enumerate(matrix)}
        edges = {(1, 2): 18, (1, 3): 12, (1, 4): 9, (1, 5): 11, (1, 6): 14,
                 (2, 8): 19, (2, 9): 16, (3, 7): 23, (4, 8): 27, (4, 9): 23,
                 (5, 9): 13, (6, 8): 15, (7, 10): 17, (8, 10): 11, (9, 10): 13}
        successors = {node: [b for a, b in edges if a == node] for node in costs}
        placed, ranks = schedule(costs, successors,
                                 lambda a, b, p, q: edges[a, b] if p != q else 0,
                                 lambda a, b: edges[a, b])
        self.assertAlmostEqual(ranks[3], ranks[4])
        order = sorted(ranks, key=lambda n: -ranks[n])
        self.assertEqual(set(order[1:3]), {3, 4})
        self.assertEqual(order[:1] + order[3:], [1, 2, 5, 6, 9, 7, 8, 10])
        self.assertEqual(max(p.end for p in placed.values()), 80)
        self.assertEqual(placed[1], Placement(2, 0, 9))
        self.assertEqual(placed[4], Placement(1, 18, 26))
        self.assertEqual(placed[10], Placement(1, 73, 80))
        for (a, b), delay in edges.items():
            self.assertGreaterEqual(placed[b].start, placed[a].end +
                                    (delay if placed[a].processor != placed[b].processor else 0))
        for processor in range(3):
            events = sorted((p.start, p.end) for p in placed.values() if p.processor == processor)
            self.assertTrue(all(a[1] <= b[0] for a, b in zip(events, events[1:])))

    def test_insertion_initial_work_and_unreachable_successor(self):
        self.assertEqual(first_gap([(0, 3), (8, 10)], 2, 4), 3)
        self.assertEqual(first_gap([(0, 3), (8, 10)], 4, 5), 10)
        occupied = {0: [(0, 3), (8, 10)]}
        placed, _ = schedule({"a": {0: 4}, "b": {0: 1}}, {"a": ["b"]},
                             lambda *args: math.inf, lambda *args: 1, occupied=occupied)
        self.assertEqual(placed, {"a": Placement(0, 3, 7)})
        self.assertEqual(occupied, {0: [(0, 3), (8, 10)]})
        with self.assertRaises(CycleError):
            schedule({1: {0: 1}, 2: {0: 1}}, {1: [2], 2: [1]}, lambda *a: 0, lambda *a: 0)


class ContactGraphReferenceTests(unittest.TestCase):
    def test_paper_pickup_delivery_choice_and_capacity_update(self):
        plan = ContactPlan([Contact("early", "g", 50, 70, 1, 20),
                            Contact("later", "g", 20, 30, 1, 1)])
        opportunities = [(1, "early", "first_pickup"), (10, "later", "later_pickup")]
        choice = choose_opportunity(opportunities, plan, {"g"}, 1)
        self.assertEqual(choice, (21, "later_pickup", (1,)))
        plan.reserve(choice[2], 1)
        self.assertEqual(choose_opportunity(opportunities, plan, {"g"}, 1)[1], "first_pickup")
        before = copy.deepcopy(plan.contacts)
        with self.assertRaises(ValueError):
            plan.reserve((0, 0), 11)
        self.assertEqual(plan.contacts, before)

    def test_first_byte_paths_against_unmodified_author_kernel(self):
        path = Path(__file__).resolve().parents[1] / "third_party/cgs_reference/routing.py"
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),
                         "ee15b1535c6f6cf7454e73fa006a71e6bdc421188cdc8f1876f652b54a7aba36")
        spec = importlib.util.spec_from_file_location("cgs_upstream_reference", path)
        upstream = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = upstream
        spec.loader.exec_module(upstream)
        fixtures = [
            [(0, 1, 0, 20, 1), (1, 2, 5, 20, 1), (0, 2, 12, 20, 1)],
            [(0, 1, 0, 5, 1), (1, 2, 15, 20, 1), (0, 2, 10, 20, 1)],
            [(0, 1, 0, 5, 1)],
        ]
        for rows in fixtures:
            ours = ContactPlan([Contact(a, b, s, e, 1, e - s, d) for a, b, s, e, d in rows])
            theirs = [upstream.Contact(a, b, start=s, end=e, rate=1, owlt=d) for a, b, s, e, d in rows]
            root = upstream.Contact(0, 0)
            root.arrival_time = 0
            expected = upstream.cgr_dijkstra(root, 2, [root] + theirs)
            actual = ours.delivery(0, {2}, 0, 0)
            self.assertEqual(actual[0] if actual else None,
                             expected.best_delivery_time if expected else None)

    def test_complete_payload_search_matches_exhaustive_paths(self):
        rng = random.Random(20261014)
        for _ in range(50):
            contacts = [Contact(a, b, rng.randrange(10), 15 + rng.randrange(10),
                                rng.randrange(1, 4), rng.randrange(1, 20))
                        for a in range(4) for b in range(a + 1, 4) if rng.random() < 0.7]
            plan, size = ContactPlan(contacts), rng.randrange(1, 5)
            expected = []

            def enumerate_paths(node, ready, hops):
                if node == 3:
                    expected.append(ready)
                if hops == 3:
                    return
                for c in contacts:
                    if c.source != node or c.volume < size:
                        continue
                    finish = max(ready, c.start) + max(1, math.ceil(size / c.rate))
                    if finish <= c.end:
                        enumerate_paths(c.target, finish, hops + 1)

            enumerate_paths(0, 0, 0)
            actual = plan.delivery(0, {3}, 0, size, dt=1, max_hops=3)
            self.assertEqual(actual[0] if actual else None, min(expected) if expected else None)

    def test_future_wait_serialization_and_hop_limit(self):
        plan = ContactPlan([Contact(0, 1, 0, 10, 1, 10), Contact(1, 2, 8, 20, 1, 12)])
        self.assertEqual(plan.delivery(0, {2}, 0, 6, dt=1, max_hops=2), (14, (0, 1)))
        self.assertIsNone(plan.delivery(0, {2}, 0, 6, dt=1, max_hops=1))
        self.assertIsNone(plan.delivery(0, {2}, 0, 11))


class LiteratureAdapterTests(unittest.TestCase):
    def test_memory_contention_ready_commitment_and_state_immutability(self):
        for cls in (CGSComputeAdapted, HEFTAdapted):
            state = contention_state()
            state.satellites[2]["mem_remaining_gb"] = state.tiles["t0"]["data_size_gb"]
            for sat in state.satellites.values():
                sat["next_gs_windows"]["gs"] = (0, 100)
            before = copy.deepcopy(state)
            actions = cls().select_actions(state)
            self.assertEqual([a.target_sat_id for a in actions], [2, 3])
            self.assertEqual(state, before)
            state.tiles["t0"]["state"] = "READY"
            self.assertEqual(cls().select_actions(state)[0].action_type, ActionType.LOCAL)

    def test_common_candidate_limit_and_legal_dispatch_in_simulation(self):
        for cls in (CGSComputeAdapted, HEFTAdapted):
            env = make_env(execution_commitment=True)
            self.addCleanup(env.close)
            add_task(env, count=8)
            policy = cls()
            for _ in range(8):
                state = decision_state(env.export_state())
                before = copy.deepcopy(state)
                actions = policy.select_actions(state)
                self.assertLessEqual(len(actions), 4 * len(state.satellites))
                for a in actions:
                    src = state.tiles[a.tile_id]["location"]
                    if a.target_sat_id is not None:
                        self.assertIn(a.target_sat_id, candidate_neighbors(state, src))
                    if a.target_gs_id is not None:
                        self.assertIn(a.target_gs_id, [o["gs_id"] for o in state.ground_options[src]])
                self.assertEqual(state, before)
                env.step(actions)

    def test_cgs_deadline_and_relay_switch(self):
        state = contention_state(False)
        state.tiles = {"t0": state.tiles["t0"]}
        state.tiles["t0"]["deadline_remaining_s"] = 0
        self.assertEqual(CGSComputeAdapted().select_actions(state)[0].action_type, ActionType.WAIT)
        state.tiles["t0"]["deadline_remaining_s"] = None
        state.neighbors[0] = []
        state.config["result_relay"] = False
        self.assertEqual(CGSComputeAdapted().select_actions(state)[0].action_type, ActionType.WAIT)
        state.config.update(result_relay=True, relay_max_hops=1)
        state.satellites[2]["next_gs_windows"]["gs"] = (0, 100)
        self.assertEqual(CGSComputeAdapted().select_actions(state)[0].action_type, ActionType.LOCAL)


if __name__ == "__main__":
    unittest.main()
