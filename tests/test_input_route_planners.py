import copy
import unittest

from sim.entities import ActionType
from sim.marl.observation import MAX_NEIGHBORS
from sim.scheduler.dts_tmax import DTSTMAXOneHopAdapted
from sim.scheduler.input_routing import (
    current_input_options, input_path_arrival, input_route_nodes, reserve_input_memory,
)
from sim.scheduler.literature import CGSComputeAdapted, HEFTAdapted, contact_plan, input_options
from test_reservation import contention_state


def relay_state(count=1):
    state = contention_state(False)
    state.config.update(input_relay_max_hops=2, compute_service_mode="work_conserving")
    state.links = {"0-1": {"up": True, "bandwidth_mbps": 8, "contact_windows": [(0, 100)]},
                   "1-2": {"up": True, "bandwidth_mbps": 8, "contact_windows": [(0, 100)]}}
    state.neighbors = {0: [1], 1: [0, 2], 2: [1]}
    for sid, satellite in state.satellites.items():
        satellite["compute_rate"] = 100 if sid == 2 else 0.001
        satellite["next_gs_windows"] = {"gs": (0, 100) if sid == 2 else None}
    tile = {**state.tiles["t0"], "data_size_mb": 1, "data_size_gb": 1 / 1024}
    state.tiles = {f"t{index}": {**tile, "parent_task_id": f"task{index}"}
                   for index in range(count)}
    return state


def memory_for(state):
    return {sid: sat["mem_remaining_gb"] for sid, sat in state.satellites.items()}


class InputRoutePlannerTests(unittest.TestCase):
    def test_two_hops_use_store_forward_and_same_candidates_for_all_three(self):
        state = relay_state()
        before = copy.deepcopy(state)
        plan = contact_plan(state)
        options = current_input_options(state, plan, state.tiles["t0"], memory_for(state))
        self.assertEqual(options[2][0], 2)
        self.assertEqual(input_route_nodes(plan, options[2][1]), (0, 1, 2))
        for cls in (CGSComputeAdapted, HEFTAdapted, DTSTMAXOneHopAdapted):
            with self.subTest(policy=cls.__name__):
                action = cls().select_actions(state)[0]
                self.assertEqual(action.target_sat_id, 2)
                self.assertEqual(action.input_path, (0, 1, 2))
        self.assertEqual(state, before)

    def test_each_hop_needs_full_payload_capacity_and_remaining_window(self):
        for duration in (0.5, 1.5):
            state = relay_state()
            state.links["1-2"]["contact_windows"] = [(0, duration)]
            plan = contact_plan(state)
            options = current_input_options(state, plan, state.tiles["t0"], memory_for(state))
            self.assertNotIn(2, options)

    def test_intermediate_and_final_memory_are_both_required(self):
        for sid in (1, 2):
            state = relay_state()
            state.satellites[sid]["mem_remaining_gb"] = 0
            options = current_input_options(state, contact_plan(state), state.tiles["t0"], memory_for(state))
            self.assertNotIn(2, options)

    def test_memory_reservation_counts_distinct_remote_nodes_once(self):
        state = relay_state()
        plan, memory, tile = contact_plan(state), memory_for(state), state.tiles["t0"]
        path = current_input_options(state, plan, tile, memory)[2][1]
        reserve_input_memory(memory, plan, path, tile)
        self.assertEqual(memory[0], 1)
        self.assertAlmostEqual(memory[1], 1 - tile["data_size_gb"])
        self.assertAlmostEqual(memory[2], 1 - tile["data_size_gb"])
        reserve_input_memory(memory, plan, (), tile)
        self.assertEqual(memory[0], 1)

    def test_all_hops_are_reserved_and_revalidated(self):
        state = relay_state()
        for link in state.links.values():
            link["contact_windows"] = [(0, 2)]
        plan, memory, tile = contact_plan(state), memory_for(state), state.tiles["t0"]
        _, path = current_input_options(state, plan, tile, memory)[2]
        plan.reserve(path, tile["data_size_mb"])
        self.assertTrue(all(plan.contacts[index].volume == 1 for index in path))
        self.assertIsNone(input_path_arrival(state, plan, tile, memory, path))

    def test_existing_backlog_and_cumulative_reservations_delay_input(self):
        state = relay_state()
        for link in state.links.values():
            link["bandwidth_mbps"] = 80
        state.satellites[0]["isl_backlog_mb"] = {1: 25}
        tile = state.tiles["t0"]
        tile.update(data_size_mb=5, data_size_gb=5 / 1024)
        plan, memory = contact_plan(state), memory_for(state)
        first = current_input_options(state, plan, tile, memory)[2]
        self.assertEqual(first[0], 4)
        plan.reserve(first[1], 5)
        self.assertEqual(current_input_options(state, plan, tile, memory)[2][0], 5)

    def test_small_packets_do_not_each_occupy_an_exclusive_dt(self):
        state = relay_state()
        for link in state.links.values():
            link["bandwidth_mbps"] = 800
        plan, memory, tile = contact_plan(state), memory_for(state), state.tiles["t0"]
        for _ in range(3):
            time, path = current_input_options(state, plan, tile, memory)[2]
            self.assertEqual(time, 2)
            plan.reserve(path, tile["data_size_mb"])

    def test_only_current_up_links_and_no_future_ground_relay(self):
        for change in ("down", "future", "zero"):
            state = relay_state()
            if change == "down":
                state.links["1-2"]["up"] = False
            elif change == "future":
                state.links["1-2"]["contact_windows"] = [(1, 100)]
            else:
                state.links["1-2"]["bandwidth_mbps"] = 0
            options = current_input_options(state, contact_plan(state), state.tiles["t0"], memory_for(state))
            self.assertNotIn(2, options)
        state = relay_state()
        state.ground_options[2] = [{"gs_id": "gs"}]
        state.config["input_relay_max_hops"] = 3
        self.assertNotIn(("ground", "gs"), current_input_options(
            state, contact_plan(state), state.tiles["t0"], memory_for(state)))

    def test_direct_visible_ground_is_available_but_dts_keeps_satellite_scope(self):
        state = relay_state()
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 100)
        state.ground_options[0] = [{"gs_id": "gs"}]
        options = current_input_options(state, contact_plan(state), state.tiles["t0"], memory_for(state))
        self.assertIn(("ground", "gs"), options)
        self.assertIsNone(DTSTMAXOneHopAdapted().select_actions(state)[0].target_gs_id)
        state.tiles["t0"]["vram_req_gb"] = 2
        self.assertEqual(set(current_input_options(state, contact_plan(state), state.tiles["t0"],
                                                  memory_for(state))), {("ground", "gs")})

    def test_candidate_neighbor_bound_applies_at_intermediate_satellite(self):
        state = relay_state()
        for sid in range(3, MAX_NEIGHBORS + 2):
            state.satellites[sid] = copy.deepcopy(state.satellites[2])
            state.neighbors[sid] = [1]
            state.links[f"1-{sid}"] = copy.deepcopy(state.links["1-2"])
        state.neighbors[1] = [0, *range(2, MAX_NEIGHBORS + 2)]
        options = current_input_options(state, contact_plan(state), state.tiles["t0"], memory_for(state))
        self.assertIn(MAX_NEIGHBORS, options)
        self.assertNotIn(MAX_NEIGHBORS + 1, options)

    def test_path_validation_rejects_loops_and_excess_hops(self):
        state = relay_state()
        plan, memory, tile = contact_plan(state), memory_for(state), state.tiles["t0"]
        path = current_input_options(state, plan, tile, memory)[2][1]
        state.config["input_relay_max_hops"] = 1
        self.assertIsNone(input_path_arrival(state, plan, tile, memory, path))
        state.config["input_relay_max_hops"] = 3
        reverse = next(i for i, c in enumerate(plan.contacts) if c.source == 1 and c.target == 0)
        self.assertIsNone(input_path_arrival(state, plan, tile, memory, (path[0], reverse)))

    def test_default_one_hop_input_options_preserve_old_backlog_semantics(self):
        state = relay_state()
        state.config.pop("input_relay_max_hops")
        state.satellites[0]["isl_backlog_mb"] = {1: 10}
        plan = contact_plan(state)
        options = input_options(state, plan, state.tiles["t0"], memory_for(state))
        self.assertEqual(options[1][0], 1)
        self.assertNotIn(2, options)
        for cls in (CGSComputeAdapted, HEFTAdapted, DTSTMAXOneHopAdapted):
            action = cls().select_actions(state)[0]
            self.assertNotEqual(action.target_sat_id, 2)
            self.assertFalse(action.input_path)

    def test_all_policies_recheck_path_capacity_between_requests(self):
        state = relay_state(count=3)
        for link in state.links.values():
            link["contact_windows"] = [(0, 2)]
        for cls in (CGSComputeAdapted, HEFTAdapted, DTSTMAXOneHopAdapted):
            with self.subTest(policy=cls.__name__):
                actions = cls().select_actions(state)
                self.assertEqual(sum(action.target_sat_id == 2 for action in actions), 1)

    def test_all_policies_reserve_intermediate_memory_between_requests(self):
        state = relay_state(count=2)
        state.satellites[1]["mem_remaining_gb"] = state.tiles["t0"]["data_size_gb"]
        for cls in (CGSComputeAdapted, HEFTAdapted, DTSTMAXOneHopAdapted):
            with self.subTest(policy=cls.__name__):
                actions = cls().select_actions(state)
                self.assertEqual(sum(action.target_sat_id == 2 for action in actions), 1)

    def test_multihop_dts_preserves_hard_deadline_and_late_continuation(self):
        state = relay_state()
        tile = state.tiles["t0"]
        tile["deadline_remaining_s"] = 1.5
        self.assertEqual(DTSTMAXOneHopAdapted().select_actions(state)[0].action_type, ActionType.WAIT)
        tile["deadline_remaining_s"] = None
        tile["waiting_time"] = 400
        self.assertEqual(DTSTMAXOneHopAdapted().select_actions(state)[0].target_sat_id, 2)
        self.assertEqual(DTSTMAXOneHopAdapted(continue_late=False).select_actions(state)[0].action_type,
                         ActionType.WAIT)

    def test_dts_onehop_name_is_two_hop_alias_under_frozen_relay_config(self):
        # 冻结 SAR 配置 input_relay_max_hops=2 且开启 ground_input_relay：
        # dts_tmax_onehop 与 dts_tmax_path 构造相同，名字中的 onehop 不代表严格单跳。
        import main
        state = relay_state()
        state.config["ground_input_relay"] = True
        for name in ("dts_tmax_onehop", "dts_tmax_path"):
            with self.subTest(name=name):
                policy = main.make_policy(name, 0)
                self.assertFalse(policy.include_ground)
                action = policy.select_actions(copy.deepcopy(state))[0]
                self.assertEqual(action.input_path, (0, 1, 2))
        hybrid = main.make_policy("dts_tmax_hybrid", 0)
        self.assertTrue(hybrid.include_ground)


if __name__ == "__main__":
    unittest.main()
