import copy
import unittest

from sim.entities import ActionType
from sim.scheduler.dts_tmax import DTSTMAXOneHopAdapted
from sim.scheduler.input_routing import (
    current_input_options, ground_input_options, input_path_arrival,
    input_route_nodes, reserve_input_memory,
)
from sim.scheduler.literature import CGSComputeAdapted, HEFTAdapted, action_for, contact_plan
from test_input_route_planners import memory_for, relay_state


def ground_state(count=1):
    state = relay_state(count)
    state.config.update(ground_input_relay=True, relay_max_hops=8)
    for sat in state.satellites.values():
        sat["compute_rate"] = 0.001
    state.ground_stations["gs"]["compute_rate"] = 100
    return state


def policies():
    return [CGSComputeAdapted(), HEFTAdapted(), DTSTMAXOneHopAdapted(include_ground=True)]


class GroundInputPlannerTests(unittest.TestCase):
    def test_all_methods_share_fixed_ground_candidate_without_source_visibility(self):
        state = ground_state()
        before = copy.deepcopy(state)
        plan, memory = contact_plan(state), memory_for(state)
        options = current_input_options(state, plan, state.tiles["t0"], memory)
        self.assertEqual(options["ground", "gs"][0], 3)
        self.assertEqual(input_route_nodes(plan, options["ground", "gs"][1]),
                         (0, 1, 2, ("ground", "gs")))
        for policy in policies():
            with self.subTest(policy=type(policy).__name__):
                action = policy.select_actions(state)[0]
                self.assertEqual(action.action_type, ActionType.OFFLOAD)
                self.assertEqual(action.target_gs_id, "gs")
                self.assertTrue(action.relay_to_ground)
                self.assertIsNone(action.target_sat_id)
                self.assertFalse(action.input_path)
        self.assertEqual(state, before)

    def test_ground_relay_works_with_satellite_input_hop_limit_one(self):
        state = ground_state()
        state.config["input_relay_max_hops"] = 1
        options = current_input_options(state, contact_plan(state), state.tiles["t0"], memory_for(state))
        self.assertNotIn(2, options)
        self.assertIn(("ground", "gs"), options)
        for policy in policies():
            self.assertTrue(policy.select_actions(state)[0].relay_to_ground)

    def test_ground_computation_does_not_require_satellite_vram(self):
        state = ground_state()
        state.tiles["t0"]["vram_req_gb"] = 100
        options = current_input_options(state, contact_plan(state), state.tiles["t0"], memory_for(state))
        self.assertEqual(set(options), {("ground", "gs")})
        for policy in policies():
            self.assertEqual(policy.select_actions(state)[0].target_gs_id, "gs")

    def test_future_isl_is_not_a_ground_commitment_until_connected(self):
        state = ground_state()
        state.links["0-1"]["up"] = False
        state.links["0-1"]["contact_windows"] = [(10, 100)]
        plan, memory = contact_plan(state), memory_for(state)
        self.assertEqual(ground_input_options(state, plan, state.tiles["t0"], memory), {})
        for policy in policies():
            self.assertFalse(policy.select_actions(state)[0].relay_to_ground)

    def test_disjoint_isl_windows_do_not_create_an_executable_ground_route(self):
        state = ground_state()
        state.links["0-1"]["contact_windows"] = [(0, 2)]
        state.links["1-2"]["up"] = False
        state.links["1-2"]["contact_windows"] = [(3, 100)]
        plan = contact_plan(state)
        self.assertEqual(ground_input_options(state, plan, state.tiles["t0"], memory_for(state)), {})
        path = tuple(next(i for i, c in enumerate(plan.contacts) if c.source == a and c.target == b)
                     for a, b in ((0, 1), (1, 2), (2, ("ground", "gs"))))
        self.assertIsNone(input_path_arrival(state, plan, state.tiles["t0"], memory_for(state), path))

    def test_future_ground_window_uses_actual_window_start(self):
        state = ground_state()
        state.satellites[2]["next_gs_windows"]["gs"] = (20, 10)
        plan, memory = contact_plan(state), memory_for(state)
        time, path = ground_input_options(state, plan, state.tiles["t0"], memory)["ground", "gs"]
        self.assertEqual(time, 21)
        self.assertEqual(input_path_arrival(state, plan, state.tiles["t0"], memory, path), time)

    def test_ground_hop_limit_includes_final_downlink_only_once(self):
        state = ground_state()
        state.config["relay_max_hops"] = 1
        self.assertEqual(ground_input_options(state, contact_plan(state), state.tiles["t0"], memory_for(state)), {})
        state.config["relay_max_hops"] = 2
        self.assertIn(("ground", "gs"), ground_input_options(
            state, contact_plan(state), state.tiles["t0"], memory_for(state)))

    def test_every_hop_reserves_real_bytes_and_capacity_is_rechecked(self):
        state = ground_state()
        for link in state.links.values():
            link["contact_windows"] = [(0, 2)]
        plan, memory, tile = contact_plan(state), memory_for(state), state.tiles["t0"]
        time, path = ground_input_options(state, plan, tile, memory)["ground", "gs"]
        before = [plan.contacts[index].volume for index in path]
        plan.reserve(path, tile["data_size_mb"])
        self.assertEqual([plan.contacts[index].volume for index in path], [v - 1 for v in before])
        self.assertIsNone(input_path_arrival(state, plan, tile, memory, path))
        self.assertEqual(ground_input_options(state, plan, tile, memory), {})

    def test_backlog_and_prior_input_reservation_delay_ground_arrival(self):
        state = ground_state()
        state.satellites[0]["isl_backlog_mb"] = {1: 3}
        plan, memory, tile = contact_plan(state), memory_for(state), state.tiles["t0"]
        first = ground_input_options(state, plan, tile, memory)["ground", "gs"]
        self.assertEqual(first[0], 6)
        plan.reserve(first[1], 1)
        self.assertEqual(ground_input_options(state, plan, tile, memory)["ground", "gs"][0], 7)

    def test_intermediate_cache_is_checked_but_only_immediate_first_hop_reserved(self):
        state = ground_state()
        plan, tile, memory = contact_plan(state), state.tiles["t0"], memory_for(state)
        _, path = ground_input_options(state, plan, tile, memory)["ground", "gs"]
        reserve_input_memory(memory, plan, path, tile)
        self.assertEqual(memory[0], 1)
        self.assertAlmostEqual(memory[1], 1 - tile["data_size_gb"])
        self.assertEqual(memory[2], 1)
        for sid in (1, 2):
            insufficient = memory_for(state)
            insufficient[sid] = 0
            self.assertEqual(ground_input_options(state, plan, tile, insufficient), {})

    def test_each_ground_quote_has_its_own_fixed_destination(self):
        state = ground_state()
        state.ground_stations["other"] = copy.deepcopy(state.ground_stations["gs"])
        state.satellites[0]["next_gs_windows"]["other"] = (10, 100)
        plan, tile, memory = contact_plan(state), state.tiles["t0"], memory_for(state)
        options = ground_input_options(state, plan, tile, memory)
        self.assertEqual(options["ground", "gs"][0], 3)
        self.assertEqual(options["ground", "other"][0], 11)
        for target, (_, path) in options.items():
            self.assertEqual(plan.contacts[path[-1]].target, target)
            action = action_for("t0", 0, target, plan=plan, path=path, ground_relay=True)
            self.assertEqual(action.target_gs_id, target[1])
            self.assertTrue(action.relay_to_ground)

    def test_ground_flag_disabled_preserves_old_available_destinations(self):
        state = ground_state()
        state.config["ground_input_relay"] = False
        options = current_input_options(state, contact_plan(state), state.tiles["t0"], memory_for(state))
        self.assertNotIn(("ground", "gs"), options)
        self.assertEqual(ground_input_options(state, contact_plan(state), state.tiles["t0"], memory_for(state)), {})
        for policy in policies():
            self.assertFalse(policy.select_actions(state)[0].relay_to_ground)

    def test_dts_satellite_only_default_and_ground_commitment(self):
        state = ground_state()
        self.assertIsNone(DTSTMAXOneHopAdapted().select_actions(state)[0].target_gs_id)
        hybrid = DTSTMAXOneHopAdapted(include_ground=True)
        self.assertEqual(hybrid.select_actions(state)[0].target_gs_id, "gs")
        state.satellites[2]["compute_rate"] = 0.25
        state.ground_stations["gs"]["committed_compute_s"] = 100
        self.assertEqual(hybrid.select_actions(state)[0].target_sat_id, 2)

    def test_dts_mixed_processor_ties_are_stable(self):
        state = ground_state()
        state.satellites[1]["compute_rate"] = 1
        state.ground_stations["gs"]["compute_rate"] = 1
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 100)
        state.ground_options[0] = [{"gs_id": "gs"}]
        action = DTSTMAXOneHopAdapted(include_ground=True).select_actions(state)[0]
        self.assertEqual(action.target_sat_id, 1)

    def test_all_methods_recheck_remaining_ground_path_for_later_tiles(self):
        state = ground_state(count=3)
        for link in state.links.values():
            link["contact_windows"] = [(0, 2)]
        for policy in policies():
            with self.subTest(policy=type(policy).__name__):
                actions = policy.select_actions(state)
                self.assertEqual(sum(action.target_gs_id == "gs" for action in actions), 1)

    def test_dts_ground_hard_deadline_and_soft_continuation(self):
        state = ground_state()
        tile = state.tiles["t0"]
        tile["deadline_remaining_s"] = 2.5
        self.assertEqual(DTSTMAXOneHopAdapted(include_ground=True).select_actions(state)[0].action_type,
                         ActionType.WAIT)
        tile["deadline_remaining_s"] = None
        tile["waiting_time"] = 400
        self.assertTrue(DTSTMAXOneHopAdapted(include_ground=True).select_actions(state)[0].relay_to_ground)


if __name__ == "__main__":
    unittest.main()
