import copy
import unittest

from sim.entities import ActionType
from sim.marl.decision import decision_state
from sim.scheduler.task_gate import CGSTaskGate, whole_service
from test_reservation import contention_state


def gate_state():
    state = contention_state()
    state.config.update(dt=0.1, result_relay=True, relay_max_hops=8, relay_contact_guard=True,
                        contact_scan_s=1, input_relay_max_hops=2, decision_tile_limit_per_sat=64)
    state.satellites[0]["next_gs_windows"] = {"gs": (0, 1000)}
    state.satellites[0]["compute_rate"] = 1
    state.tiles = {"t0": state.tiles["t0"]}
    state.pending_task_tiles = {"task0": dict(state.tiles)}
    return state


class TaskGateTests(unittest.TestCase):
    def test_prefers_whole_local_delivery_without_mutating_observation(self):
        state = gate_state()
        before = copy.deepcopy(state)
        policy = CGSTaskGate()
        actions = policy.select_actions(state)
        self.assertEqual(actions[0].action_type, ActionType.LOCAL)
        self.assertTrue(policy.last_prediction["task0"]["selected_local"])
        self.assertEqual(state, before)

    def test_falls_back_to_cgs_when_source_queue_misses_deadline(self):
        state = gate_state()
        state.satellites[0]["committed_compute_s"] = 400
        policy = CGSTaskGate()
        actions = policy.select_actions(state)
        self.assertEqual(actions[0].action_type, ActionType.OFFLOAD)
        self.assertFalse(policy.last_prediction["task0"]["selected_local"])

    def test_hidden_sibling_work_is_included_but_not_dispatched(self):
        state = gate_state()
        tile = state.tiles["t0"]
        state.tiles = {f"t{i}": {**tile, "compute_cost": 200} for i in range(2)}
        state.pending_task_tiles = {"task0": dict(state.tiles)}
        state.config["decision_tile_limit_per_sat"] = 1
        state = decision_state(state, 1)
        policy = CGSTaskGate()
        actions = policy.select_actions(state)
        self.assertEqual(len(actions), 1)
        self.assertFalse(policy.last_prediction["task0"]["selected_local"])
        self.assertGreater(policy.last_prediction["task0"]["local_latency_seconds"], 400)

    def test_cohort_service_uses_windows_not_dispatch_block_count(self):
        state = gate_state()
        state.config["onboard_compute_profile"] = {"service_s": {"1": 0.597, "2": 1.056}}
        self.assertAlmostEqual(whole_service(state, [{"compute_cost": 4}] * 16, 0), 33.792)
        self.assertAlmostEqual(whole_service(state, [{"compute_cost": 3}], 0), 1.653)


if __name__ == "__main__":
    unittest.main()
