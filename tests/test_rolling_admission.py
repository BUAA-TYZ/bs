import copy
import unittest

from sim.entities import ActionType
from sim.marl.decision import candidate_neighbors, decision_state
from sim.scheduler.rolling_admission import RollingAdmissionPolicy
from test_reservation import contention_state
from test_simulation import add_task, make_env


def state_for_admission(count=1):
    state = contention_state(False)
    state.config.update(compute_service_mode="work_conserving", contact_scan_s=1,
                        result_relay=False, decision_tile_limit_per_sat=64)
    state.satellites[2]["next_gs_windows"]["gs"] = (0, 1000)
    tile = state.tiles["t0"]
    state.tiles = {f"tile{i:03d}": {**tile, "parent_task_id": "image"} for i in range(count)}
    state.pending_task_tiles = {"image": state.tiles.copy()}
    return state


class RollingAdmissionTests(unittest.TestCase):
    def test_horizon_is_start_time_inclusive_and_recovers_after_wait(self):
        state = state_for_admission()
        policy = RollingAdmissionPolicy(30)
        state.satellites[2]["committed_compute_s"] = 30.01
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        self.assertEqual(policy.stats["wait_commit_horizon"], 0)
        # 本地可开算但不能回传，邻居超过承诺范围，结果不得发往邻居。
        state.satellites[0]["committed_compute_s"] = 31
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        self.assertEqual(policy.stats["wait_commit_horizon"], 1)
        state.satellites[2]["committed_compute_s"] = 30
        self.assertEqual(policy.select_actions(state)[0].target_sat_id, 2)
        self.assertEqual(policy.stats["accepted"], 1)

    def test_same_round_compute_budget_is_shared_across_sources(self):
        state = state_for_admission(2)
        state.tiles["tile001"]["location"] = 1
        policy = RollingAdmissionPolicy(1)
        actions = policy.select_actions(state)
        self.assertEqual(actions[0].target_sat_id, 2)
        self.assertEqual(actions[1].action_type, ActionType.WAIT)

    def test_sixty_four_small_packets_do_not_occupy_sixty_four_seconds(self):
        state = state_for_admission(64)
        state.links["0-2"]["bandwidth_mbps"] = 10_000
        for tile in state.tiles.values():
            tile.update(data_size_mb=10, data_size_gb=0.01, compute_cost=0.01)
        policy = RollingAdmissionPolicy(2)
        actions = policy.select_actions(state)
        self.assertTrue(all(a.target_sat_id == 2 for a in actions))
        self.assertEqual(policy.stats["accepted"], 64)

    def test_cumulative_input_bytes_and_existing_backlog_bound_admission(self):
        state = state_for_admission(3)
        state.links["0-2"].update(bandwidth_mbps=8, contact_windows=[(0, 2)])
        state.satellites[0]["isl_backlog_mb"][2] = 0.5
        for tile in state.tiles.values():
            tile.update(data_size_mb=0.75, data_size_gb=0.75 / 1024, compute_cost=0.01)
        actions = RollingAdmissionPolicy(30).select_actions(state)
        self.assertEqual([a.target_sat_id for a in actions], [2, 2, None])
        self.assertEqual(actions[-1].action_type, ActionType.WAIT)

    def test_shared_memory_and_result_capacity_are_not_overbooked(self):
        state = state_for_admission(2)
        state.satellites[2]["mem_remaining_gb"] = state.tiles["tile000"]["data_size_gb"]
        actions = RollingAdmissionPolicy().select_actions(state)
        self.assertEqual([a.target_sat_id for a in actions], [2, None])
        state.satellites[2]["mem_remaining_gb"] = 1
        state.satellites[2]["next_gs_windows"]["gs"] = (10, 1)
        for tile in state.tiles.values():
            tile["result_size_mb"] = 0.75
        actions = RollingAdmissionPolicy(guard_returns=False).select_actions(state)
        self.assertEqual([a.target_sat_id for a in actions], [2, None])

    def test_return_guard_shrinks_isl_and_ground_windows_only_when_enabled(self):
        state = state_for_admission()
        state.satellites[2]["next_gs_windows"]["gs"] = (0, 3)
        self.assertEqual(RollingAdmissionPolicy(guard_returns=False).select_actions(state)[0].target_sat_id, 2)
        self.assertEqual(RollingAdmissionPolicy().select_actions(state)[0].action_type, ActionType.WAIT)
        state.config.update(result_relay=True, relay_max_hops=1)
        state.tiles["tile000"]["compute_cost"] = 0.1
        state.neighbors[0] = []
        state.links["0-2"]["contact_windows"] = [(0, 2)]
        state.satellites[2]["next_gs_windows"]["gs"] = (0, 100)
        self.assertEqual(RollingAdmissionPolicy(guard_returns=False).select_actions(state)[0].action_type,
                         ActionType.LOCAL)
        self.assertEqual(RollingAdmissionPolicy().select_actions(state)[0].action_type, ActionType.WAIT)

    def test_soft_sla_ready_failed_and_hard_deadline_have_distinct_effects(self):
        state = state_for_admission()
        state.tiles["tile000"]["waiting_time"] = 301
        policy = RollingAdmissionPolicy()
        self.assertEqual(policy.select_actions(state)[0].target_sat_id, 2)
        self.assertEqual(policy.stats["task_sla_risk_observations"], 1)
        self.assertEqual(policy.stats["accepted_sla_risk"], 1)
        state.tiles["tile000"]["deadline_remaining_s"] = 0
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        state.tiles["tile000"]["state"] = "READY"
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.LOCAL)
        state.tiles["tile000"]["state"] = "QUEUED"
        state.tiles["tile000"]["task_progress"] = {"failed": 1}
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)

    def test_future_contact_or_invisible_ground_never_emits_offload(self):
        state = state_for_admission()
        state.links["0-2"].update(up=False, contact_windows=[(10, 100)])
        self.assertEqual(RollingAdmissionPolicy().select_actions(state)[0].action_type, ActionType.WAIT)
        state.satellites[0]["next_gs_windows"]["gs"] = (10, 100)
        state.tiles["tile000"]["vram_req_gb"] = 2
        self.assertEqual(RollingAdmissionPolicy().select_actions(state)[0].action_type, ActionType.WAIT)
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 100)
        state.ground_options[0] = [{"gs_id": "gs", "bandwidth_mbps": 8}]
        self.assertEqual(RollingAdmissionPolicy().select_actions(state)[0].target_gs_id, "gs")

    def test_state_immutability_candidate_budget_and_execution_legality(self):
        env = make_env(execution_commitment=True)
        self.addCleanup(env.close)
        add_task(env, count=8)
        policy = RollingAdmissionPolicy()
        for _ in range(8):
            state = decision_state(env.export_state())
            before = copy.deepcopy(state)
            actions = policy.select_actions(state)
            self.assertLessEqual(len(actions), 4 * len(state.satellites))
            for action in actions:
                src = state.tiles[action.tile_id]["location"]
                if action.target_sat_id is not None:
                    self.assertIn(action.target_sat_id, candidate_neighbors(state, src))
                if action.target_gs_id is not None:
                    self.assertIn(action.target_gs_id, [o["gs_id"] for o in state.ground_options[src]])
            self.assertEqual(state, before)
            env.step(actions)

    def test_invalid_horizon_rejected_and_zero_allows_immediate_local_start(self):
        for value in (-1, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RollingAdmissionPolicy(value)
        state = state_for_admission()
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 100)
        self.assertEqual(RollingAdmissionPolicy(0).select_actions(state)[0].action_type, ActionType.LOCAL)

    def test_future_input_waits_then_dispatches_when_contact_opens(self):
        state = state_for_admission()
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 1000)
        state.satellites[0]["compute_rate"] = 0.01
        state.links["0-2"].update(up=False, contact_windows=[(10, 100)])
        state.neighbors[0] = []
        immediate = RollingAdmissionPolicy(14400, False)
        self.assertEqual(immediate.select_actions(state)[0].action_type, ActionType.LOCAL)
        policy = RollingAdmissionPolicy(14400, False, input_lookahead_s=300)
        before = copy.deepcopy(state)
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        self.assertEqual(policy.stats["wait_future_input"], 1)
        self.assertEqual(policy.stats["accepted"], 0)
        self.assertEqual(state, before)
        state.links["0-2"].update(up=True, contact_windows=[(0, 100)])
        state.neighbors[0] = [2]
        self.assertEqual(policy.select_actions(state)[0].target_sat_id, 2)

    def test_disappearing_future_input_and_shorter_lookahead_recompute_current_choice(self):
        state = state_for_admission()
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 1000)
        state.satellites[0]["compute_rate"] = 0.01
        state.links["0-2"].update(up=False, contact_windows=[(10, 100)])
        state.neighbors[0] = []
        policy = RollingAdmissionPolicy(14400, False, input_lookahead_s=300)
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        self.assertEqual(RollingAdmissionPolicy(14400, False, 9).select_actions(state)[0].action_type,
                         ActionType.LOCAL)
        state.links["0-2"]["contact_windows"] = []
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.LOCAL)

    def test_equal_ground_arrival_prefers_execution_over_future_wait(self):
        state = state_for_admission()
        state.satellites[0]["next_gs_windows"]["gs"] = (100, 100)
        state.satellites[2]["next_gs_windows"]["gs"] = (100, 100)
        state.links["0-2"].update(up=False, contact_windows=[(1, 100)])
        state.neighbors[0] = []
        policy = RollingAdmissionPolicy(14400, False, input_lookahead_s=300)
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.LOCAL)
        self.assertEqual(policy.stats["wait_future_input"], 0)

    def test_future_reservations_consume_shared_compute_memory_and_capacity(self):
        for constraint in ("compute", "memory", "capacity"):
            with self.subTest(constraint=constraint):
                state = state_for_admission(2)
                state.satellites[0]["next_gs_windows"]["gs"] = (0, 1000)
                state.satellites[0]["compute_rate"] = 1 / 15
                state.links["0-2"].update(up=False, contact_windows=[(10, 100)])
                state.neighbors[0] = []
                if constraint == "compute":
                    state.satellites[2]["compute_rate"] = 1 / 3
                elif constraint == "memory":
                    state.satellites[2]["mem_remaining_gb"] = state.tiles["tile000"]["data_size_gb"]
                else:
                    state.links["0-2"].update(bandwidth_mbps=1, contact_windows=[(10, 1)])
                actions = RollingAdmissionPolicy(14400, False, 300).select_actions(state)
                self.assertEqual([a.action_type for a in actions], [ActionType.WAIT, ActionType.LOCAL])

    def test_future_small_packets_use_cumulative_service_after_contact_start(self):
        state = state_for_admission(64)
        state.links["0-2"].update(up=False, bandwidth_mbps=10_000, contact_windows=[(10, 1)])
        state.neighbors[0] = []
        for tile in state.tiles.values():
            tile.update(data_size_mb=10, data_size_gb=0.01, compute_cost=0.01)
        policy = RollingAdmissionPolicy(12, False, 300)
        actions = policy.select_actions(state)
        self.assertTrue(all(a.action_type == ActionType.WAIT for a in actions))
        self.assertEqual(policy.stats["wait_future_input"], 64)
        # 传输至少在窗口打开后的步末完成，H=10 不能假装到窗口开头就已到达。
        too_early = RollingAdmissionPolicy(10, False, 300)
        too_early.select_actions(state)
        self.assertEqual(too_early.stats["wait_future_input"], 0)

    def test_current_non_candidate_is_not_enabled_by_future_input_search(self):
        state = state_for_admission()
        state.neighbors[0] = []
        self.assertEqual(RollingAdmissionPolicy(14400, False, 300).select_actions(state)[0].action_type,
                         ActionType.WAIT)
        for value in (-1, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RollingAdmissionPolicy(input_lookahead_s=value)

    def test_fractional_future_start_is_quantized_before_service(self):
        state = state_for_admission()
        state.links["0-2"].update(up=False, bandwidth_mbps=8, contact_windows=[(0.9, 2)])
        state.neighbors[0] = []
        state.tiles["tile000"].update(data_size_mb=1.1, data_size_gb=0.0011)
        policy = RollingAdmissionPolicy(14400, False, 300)
        policy.select_actions(state)
        self.assertEqual(policy.stats["wait_future_input"], 0)

    def test_execution_mode_uses_same_legal_current_input(self):
        state = state_for_admission()
        policy = RollingAdmissionPolicy(14400, False, return_mode="execution")
        self.assertEqual(policy.select_actions(state)[0].target_sat_id, 2)
        with self.assertRaises(ValueError):
            RollingAdmissionPolicy(return_mode="unknown")

    def test_sla_gate_waits_only_for_an_on_time_gain(self):
        # 即时在101秒交付，未来在13秒交付；均含最后一次下传。
        for age, expected in ((0, ActionType.LOCAL), (200, ActionType.WAIT),
                              (288, ActionType.LOCAL), (301, ActionType.LOCAL)):
            with self.subTest(age=age):
                state = state_for_admission()
                state.satellites[0]["next_gs_windows"]["gs"] = (0, 1000)
                state.satellites[0]["compute_rate"] = 0.01
                state.links["0-2"].update(up=False, contact_windows=[(10, 100)])
                state.neighbors[0] = []
                state.tiles["tile000"]["waiting_time"] = age
                policy = RollingAdmissionPolicy(14400, False, 300, wait_for_sla_only=True)
                self.assertEqual(policy.select_actions(state)[0].action_type, expected)

    def test_sla_gate_includes_postprocessing_and_inclusive_boundary(self):
        state = state_for_admission()
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 1000)
        state.satellites[0]["compute_rate"] = 0.01
        state.links["0-2"].update(up=False, contact_windows=[(10, 100)])
        state.neighbors[0] = []
        state.tiles["tile000"]["waiting_time"] = 286
        state.config["postprocessing_delay_s"] = 1
        policy = RollingAdmissionPolicy(14400, False, 300, wait_for_sla_only=True)
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        state.config["postprocessing_delay_s"] = 2
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.LOCAL)
        for value in (0, -1, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RollingAdmissionPolicy(sla_seconds=value)

    def test_sla_gate_does_not_create_an_impossible_current_delivery(self):
        state = state_for_admission()
        state.links["0-2"].update(up=False, contact_windows=[(10, 100)])
        state.neighbors[0] = []
        state.tiles["tile000"]["waiting_time"] = 301
        policy = RollingAdmissionPolicy(14400, False, 300, wait_for_sla_only=True)
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)


if __name__ == "__main__":
    unittest.main()
