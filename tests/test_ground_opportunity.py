import copy
import unittest

from sim.entities import ActionType
from sim.scheduler.literature import CGSComputeAdapted
from test_reservation import contention_state


class GroundOpportunityTests(unittest.TestCase):
    def state(self):
        state = contention_state()
        state.tiles = {"t0": state.tiles["t0"]}
        state.ground_options = {sid: [] for sid in state.satellites}
        state.ground_stations["gs"]["compute_rate"] = 100
        state.ground_stations["gs"]["committed_compute_s"] = 0
        state.satellites[0]["compute_rate"] = 0.0025
        state.satellites[0]["next_gs_windows"]["gs"] = (10, 100)
        for sid in (2, 3):
            state.satellites[sid]["next_gs_windows"]["gs"] = (80, 100)
        return state

    def test_wait_for_better_ground_opportunity_and_dispatch_only_when_visible(self):
        state = self.state()
        original = copy.deepcopy(state)
        baseline = CGSComputeAdapted().select_actions(state)[0]
        policy = CGSComputeAdapted(future_ground=True)
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        self.assertNotEqual(baseline.action_type, ActionType.WAIT)
        self.assertEqual(state, original)
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 100)
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        state.ground_options[0] = [{"gs_id": "gs"}]
        self.assertEqual(policy.select_actions(state)[0].target_gs_id, "gs")

    def test_ground_congestion_or_short_contact_does_not_force_wait(self):
        for reason in ("queue", "window"):
            state = self.state()
            if reason == "queue":
                state.ground_stations["gs"]["committed_compute_s"] = 1000
            else:
                state.satellites[0]["next_gs_windows"]["gs"] = (10, 0.001)
            action = CGSComputeAdapted(future_ground=True).select_actions(state)[0]
            self.assertEqual(action.action_type, ActionType.OFFLOAD)
            self.assertIsNotNone(action.target_sat_id)

    def test_deadline_and_shared_future_ground_compute_reservation(self):
        state = self.state()
        state.tiles["t0"]["deadline_remaining_s"] = 1
        self.assertEqual(CGSComputeAdapted(future_ground=True).select_actions(state)[0].action_type, ActionType.WAIT)
        state = self.state()
        tile = copy.deepcopy(state.tiles["t0"])
        state.ground_stations["gs"]["compute_rate"] = tile["compute_cost"] / 40
        for i in range(1, 5):
            state.tiles[f"t{i}"] = copy.deepcopy(tile)
        actions = CGSComputeAdapted(future_ground=True).select_actions(state)
        self.assertEqual(actions[0].action_type, ActionType.WAIT)
        self.assertTrue(any(a.target_sat_id is not None for a in actions[1:]))


if __name__ == "__main__":
    unittest.main()
