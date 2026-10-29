import copy
from types import SimpleNamespace
import unittest

from sim.scheduler.execution_return import execution_delivery
from sim.entities import Action, ActionType, TileState
from sim.scheduler.contact_graph import Contact, ContactPlan
from sim.scheduler.literature import contact_plan
from test_result_relay import RelayVisibility, relay_env
from test_simulation import add_task


def fixture(contacts, count=6):
    return SimpleNamespace(
        config={"dt": 1, "result_relay": True, "relay_max_hops": 8,
                "contact_scan_s": 0, "memory_mb_per_gb": 1000},
        satellites={sid: {"mem_remaining_gb": 1} for sid in range(count)},
        ground_stations={"gs": {}},
    ), ContactPlan(contacts)


def edge(a, b, start=0, end=100, rate=1, volume=None):
    return Contact(a, b, start, end, rate, (end - start) * rate if volume is None else volume)


class ExecutionReturnTests(unittest.TestCase):
    def test_future_intermediate_edge_does_not_override_real_first_hop(self):
        # p=0,a=1,g=2,b=3,x=4,q=5；真实路由先去 x，错过 a 的未来捷径。
        state, plan = fixture([
            edge(0, 1, end=5), edge(1, 0, end=5), edge(1, 2, start=10, end=20),
            edge(0, 3), edge(3, 0), edge(3, 4), edge(4, 3),
            edge(2, ("ground", "gs"), 12, 15),
            edge(4, ("ground", "gs"), 100, 110),
            edge(5, ("ground", "gs"), 30, 40),
        ])
        before = copy.deepcopy(plan.contacts)
        self.assertEqual(plan.delivery(0, {("ground", "gs")}, 0, 1, dt=1)[0], 13)
        self.assertEqual(execution_delivery(state, plan, 0, 0, 1), (101, (3, 5, 8)))
        self.assertEqual(execution_delivery(state, plan, 5, 0, 1), (31, (9,)))
        self.assertEqual(plan.contacts, before)

    def test_waiting_without_an_alternative_can_use_later_contact(self):
        state, plan = fixture([edge(0, 1, start=10, end=20),
                               edge(1, ("ground", "gs"), 12, 20)])
        self.assertEqual(execution_delivery(state, plan, 0, 0, 1), (13, (0, 1)))

    def test_compute_ready_changes_current_topology_and_first_hop(self):
        state, plan = fixture([edge(0, 1, end=5), edge(0, 2, start=10, end=30),
                               edge(1, ("ground", "gs"), 100, 110),
                               edge(2, ("ground", "gs"), 12, 20)])
        self.assertEqual(execution_delivery(state, plan, 0, 0, 1), (101, (0, 2)))
        self.assertEqual(execution_delivery(state, plan, 0, 10, 1), (13, (1, 3)))

    def test_hops_visited_and_local_downlink_with_exhausted_isl_budget(self):
        state, plan = fixture([edge(0, 1), edge(1, 2), edge(2, ("ground", "gs"))])
        self.assertIsNone(execution_delivery(state, plan, 0, 0, 1, max_hops=2))
        self.assertEqual(execution_delivery(state, plan, 0, 0, 1, max_hops=3)[0], 3)
        self.assertIsNone(execution_delivery(state, plan, 0, 0, 1, visited=[1, 0]))
        self.assertEqual(execution_delivery(state, plan, 2, 0, 1, max_hops=3,
                                            visited=[0, 1, 2]), (1, (2,)))

    def test_guard_checks_cumulative_path_arrival_and_does_not_shorten_ground(self):
        state, plan = fixture([edge(0, 1, end=33), edge(1, 2, end=31),
                               edge(2, ("ground", "gs"), 0, 10)])
        self.assertEqual(execution_delivery(state, plan, 0, 0, 1, guard_s=0)[0], 3)
        self.assertIsNone(execution_delivery(state, plan, 0, 0, 1, guard_s=30))
        self.assertEqual(execution_delivery(state, plan, 2, 0, 1, guard_s=30), (1, (2,)))

    def test_rate_floor_memory_and_capacity_are_respected(self):
        state, plan = fixture([edge(0, 1, end=10, rate=10),
                               edge(1, ("ground", "gs"), 0, 100)])
        state.config["isl_bandwidth_floor_mbps"] = 0.8
        self.assertIsNone(execution_delivery(state, plan, 0, 0, 1, guard_s=1))
        state.config["isl_bandwidth_floor_mbps"] = 8
        state.satellites[1]["mem_remaining_gb"] = 0
        self.assertIsNone(execution_delivery(state, plan, 0, 0, 1))
        state.satellites[1]["mem_remaining_gb"] = 1
        plan.contacts[0].volume = 0.5
        self.assertIsNone(execution_delivery(state, plan, 0, 0, 1))

    def test_returned_indices_can_reserve_same_round_capacity(self):
        state, plan = fixture([edge(0, 1, end=10), edge(1, ("ground", "gs"), 3, 4)])
        finish, path = execution_delivery(state, plan, 0, 0, 1)
        self.assertEqual((finish, path), (4, (0, 1)))
        plan.reserve(path, 1)
        self.assertIsNone(execution_delivery(state, plan, 0, 0, 1))

    def test_no_known_path_and_ground_source(self):
        state, plan = fixture([edge(0, 1, start=20, end=30)])
        self.assertIsNone(execution_delivery(state, plan, 0, 0, 1))
        self.assertEqual(execution_delivery(state, plan, ("ground", "gs"), 1.1, 1), (2, ()))
        self.assertIsNone(execution_delivery(state, plan, 0, 0, 1, max_hops=0))
        with self.assertRaises(ValueError):
            execution_delivery(state, plan, 0, 0, 1, visited=[1])

    def test_matches_shared_executor_without_competing_traffic(self):
        for opening in (0, 10):
            with self.subTest(opening=opening):
                env = relay_env()
                self.addCleanup(env.close)
                env.vis_cache = RelayVisibility({2: (opening, 100)})
                env.contact_cache = SimpleNamespace(windows_at=lambda _: {
                    "0-1": [(0, 100)], "1-2": [(0, 100)]})
                tile = add_task(env)[0]
                state = env.export_state()
                prediction = execution_delivery(state, contact_plan(state), 0, 1, 1, guard_s=0)
                env.step([Action(tile.tile_id, ActionType.LOCAL)])
                for _ in range(20):
                    if tile.state == TileState.DONE:
                        break
                    env.step([])
                self.assertEqual(tile.state, TileState.DONE)
                self.assertEqual(env.metrics.task_latencies, [prediction[0]])


if __name__ == "__main__":
    unittest.main()
