import copy
import math
import unittest

from sim.entities import ActionType
from sim.marl.decision import candidate_neighbors, decision_state
from sim.scheduler.dts_tmax import DTSTMAXOneHopAdapted
from test_reservation import contention_state
from test_simulation import add_task, make_env


def single_request_state():
    state = contention_state()
    state.tiles = {"t0": state.tiles["t0"]}
    state.config["compute_service_mode"] = "work_conserving"
    for satellite in state.satellites.values():
        satellite["compute_rate"] = 1.0
    return state


class DTSTMAXAdaptationTests(unittest.TestCase):
    def test_local_when_transmission_outweighs_remote_service(self):
        state = single_request_state()
        state.satellites[2]["compute_rate"] = 2.0
        action = DTSTMAXOneHopAdapted().select_actions(state)[0]
        self.assertEqual(action.action_type, ActionType.LOCAL)

    def test_neighbor_when_source_has_committed_work(self):
        state = single_request_state()
        state.satellites[0]["committed_compute_s"] = 10.0
        state.satellites[3]["committed_compute_s"] = 5.0
        action = DTSTMAXOneHopAdapted().select_actions(state)[0]
        self.assertEqual(action.target_sat_id, 2)

    def test_input_serialization_and_propagation_affect_selection(self):
        state = single_request_state()
        state.satellites[0]["committed_compute_s"] = 1.5
        state.satellites[3]["committed_compute_s"] = 20.0
        policy = DTSTMAXOneHopAdapted()
        self.assertEqual(policy.select_actions(state)[0].target_sat_id, 2)
        state.links["0-2"]["latency_ms"] = 1000.0
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.LOCAL)
        state.links["0-2"]["latency_ms"] = 0.0
        state.tiles["t0"]["data_size_mb"] = 4.0
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.LOCAL)

    def test_remote_memory_and_compute_commitments_are_updated(self):
        state = contention_state()
        state.config["compute_service_mode"] = "work_conserving"
        state.satellites[2]["mem_remaining_gb"] = state.tiles["t0"]["data_size_gb"]
        before = copy.deepcopy(state)
        actions = DTSTMAXOneHopAdapted().select_actions(state)
        self.assertEqual([action.target_sat_id for action in actions], [2, 3])
        self.assertEqual(state, before)
        state.satellites[2]["mem_remaining_gb"] = 1.0
        actions = DTSTMAXOneHopAdapted().select_actions(state)
        self.assertEqual([action.target_sat_id for action in actions], [2, 3])

    def test_resources_and_hard_deadline_are_never_relaxed(self):
        state = single_request_state()
        state.tiles["t0"]["vram_req_gb"] = 2.0
        policy = DTSTMAXOneHopAdapted()
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        state.tiles["t0"]["vram_req_gb"] = 0.1
        state.tiles["t0"]["deadline_remaining_s"] = 0.5
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        state.tiles["t0"]["deadline_remaining_s"] = 1.0
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.LOCAL)

    def test_deadline_uses_original_age_and_late_modes_are_explicit(self):
        state = single_request_state()
        state.tiles["t0"]["waiting_time"] = 299.5
        strict = DTSTMAXOneHopAdapted(continue_late=False)
        late = DTSTMAXOneHopAdapted()
        self.assertEqual(strict.select_actions(state)[0].action_type, ActionType.WAIT)
        self.assertEqual(late.select_actions(state)[0].action_type, ActionType.LOCAL)
        self.assertEqual(late.stats["late_admission"], 1)
        state.tiles["t0"]["waiting_time"] = 350.0
        self.assertEqual(strict.select_actions(state)[0].action_type, ActionType.WAIT)
        unrestricted = DTSTMAXOneHopAdapted(deadline_s=None, continue_late=False)
        self.assertEqual(unrestricted.select_actions(state)[0].action_type, ActionType.LOCAL)

    def test_soft_continuation_matches_no_soft_filter(self):
        state = contention_state()
        for age in (0.0, 299.0, 350.0):
            for tile in state.tiles.values():
                tile["waiting_time"] = age
            self.assertEqual(DTSTMAXOneHopAdapted().select_actions(state),
                             DTSTMAXOneHopAdapted(deadline_s=None).select_actions(state))

    def test_ready_inflight_failed_and_ground_scope(self):
        state = single_request_state()
        state.tiles["t0"]["state"] = "READY"
        self.assertEqual(DTSTMAXOneHopAdapted().select_actions(state)[0].action_type,
                         ActionType.LOCAL)
        state.tiles["t0"]["in_transfer"] = True
        self.assertEqual(DTSTMAXOneHopAdapted().select_actions(state), [])
        state.tiles["t0"]["in_transfer"] = False
        state.tiles["t0"]["state"] = "QUEUED"
        state.tiles["t0"]["task_progress"]["failed"] = 1
        self.assertEqual(DTSTMAXOneHopAdapted().select_actions(state)[0].action_type,
                         ActionType.WAIT)
        state.tiles["t0"]["task_progress"]["failed"] = 0
        state.ground_options[0] = [{"gs_id": "gs", "bandwidth_mbps": 10000.0}]
        state.ground_stations["gs"]["compute_rate"] = 1000000.0
        self.assertIsNone(DTSTMAXOneHopAdapted().select_actions(state)[0].target_gs_id)

    def test_nonpositive_link_and_stable_tie_order(self):
        state = single_request_state()
        state.satellites[0]["committed_compute_s"] = 20.0
        state.neighbors[0] = [3, 2]
        policy = DTSTMAXOneHopAdapted()
        self.assertEqual(policy.select_actions(state)[0].target_sat_id, 2)
        state.links["0-2"]["bandwidth_mbps"] = 0.0
        self.assertEqual(policy.select_actions(state)[0].target_sat_id, 3)
        state.links["0-3"]["up"] = False
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.LOCAL)

    def test_estimate_is_additive_as_equation_ten(self):
        state = single_request_state()
        state.satellites[0]["committed_compute_s"] = 2.5
        state.satellites[2]["committed_compute_s"] = 1.0
        state.satellites[3]["committed_compute_s"] = 20.0
        state.tiles["t0"]["data_size_mb"] = 2.0
        # 远端为 2 秒传输 + 1 秒排队 + 1 秒服务；本地为 3.5 秒。
        self.assertEqual(DTSTMAXOneHopAdapted().select_actions(state)[0].action_type,
                         ActionType.LOCAL)

    def test_work_conserving_service_retains_substep_compute(self):
        state = single_request_state()
        state.tiles["t0"]["compute_cost"] = 0.25
        state.satellites[0]["committed_compute_s"] = 0.9
        state.satellites[2]["compute_rate"] = 4.0
        state.satellites[3]["committed_compute_s"] = 20.0
        self.assertEqual(DTSTMAXOneHopAdapted().select_actions(state)[0].target_sat_id, 2)
        state.config["compute_service_mode"] = "legacy"
        self.assertEqual(DTSTMAXOneHopAdapted().select_actions(state)[0].action_type,
                         ActionType.LOCAL)

    def test_shared_link_does_not_reserve_exclusive_dt_per_packet(self):
        state = single_request_state()
        state.satellites[0]["committed_compute_s"] = 2.0
        state.satellites[3]["committed_compute_s"] = 20.0
        template = {**state.tiles["t0"], "compute_cost": 0.01}
        state.tiles = {f"t{index}": dict(template) for index in range(3)}
        actions = DTSTMAXOneHopAdapted().select_actions(state)
        self.assertEqual([action.target_sat_id for action in actions], [2, 2, 2])

    def test_invalid_soft_deadline(self):
        for deadline in (0, -1, math.inf, math.nan):
            with self.assertRaises(ValueError):
                DTSTMAXOneHopAdapted(deadline_s=deadline)

    def test_common_candidate_limit_and_pipeline_legal_actions(self):
        env = make_env(execution_commitment=True)
        self.addCleanup(env.close)
        add_task(env, count=8)
        policy = DTSTMAXOneHopAdapted()
        for _ in range(8):
            state = decision_state(env.export_state())
            before = copy.deepcopy(state)
            actions = policy.select_actions(state)
            self.assertLessEqual(len(actions), 4 * len(state.satellites))
            for action in actions:
                if action.target_sat_id is not None:
                    source = state.tiles[action.tile_id]["location"]
                    self.assertIn(action.target_sat_id, candidate_neighbors(state, source))
                self.assertIsNone(action.target_gs_id)
            self.assertEqual(state, before)
            env.step(actions)


if __name__ == "__main__":
    unittest.main()
