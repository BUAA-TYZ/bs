import copy
import unittest

from sim.entities import Action, ActionType
from sim.marl.decision import decision_state
from sim.scheduler.rolling_admission import RollingAdmissionPolicy
from sim.scheduler.task_quota import TaskQuotaPolicy
from test_rolling_admission import state_for_admission
from test_task_quota import FixedShadow


def chain_state(count=1):
    state = state_for_admission(count)
    state.config["input_relay_max_hops"] = 2
    state.neighbors = {0: [1], 1: [0, 2], 2: [1]}
    state.links = {
        "0-1": {"up": True, "bandwidth_mbps": 1000, "contact_windows": [(0, 100)]},
        "1-2": {"up": True, "bandwidth_mbps": 1000, "contact_windows": [(0, 100)]},
    }
    state.satellites[1]["next_gs_windows"] = {"gs": None}
    return state


class MultihopAdmissionTests(unittest.TestCase):
    def test_rolling_emits_shared_two_hop_path_and_preserves_one_hop_mode(self):
        state = chain_state()
        before = copy.deepcopy(state)
        action = RollingAdmissionPolicy(14400, False).select_actions(state)[0]
        self.assertEqual(action.target_sat_id, 2)
        self.assertEqual(tuple(action.input_path), (0, 1, 2))
        self.assertEqual(state, before)
        state.config["input_relay_max_hops"] = 1
        self.assertEqual(RollingAdmissionPolicy(14400, False).select_actions(state)[0].action_type,
                         ActionType.WAIT)

    def test_rolling_reserves_intermediate_and_final_memory_once(self):
        for bottleneck in (1, 2):
            with self.subTest(bottleneck=bottleneck):
                state = chain_state(2)
                state.satellites[bottleneck]["mem_remaining_gb"] = state.tiles["tile000"]["data_size_gb"]
                actions = RollingAdmissionPolicy(14400, False).select_actions(state)
                self.assertEqual(actions[0].target_sat_id, 2)
                self.assertEqual(actions[1].action_type, ActionType.WAIT)

    def test_rolling_counts_input_capacity_on_every_hop(self):
        state = chain_state(2)
        for link in state.links.values():
            link["bandwidth_mbps"] = 8
        state.links["1-2"]["contact_windows"] = [(0, 2)]
        for tile in state.tiles.values():
            tile.update(data_size_mb=0.75, data_size_gb=0.75 / 1024)
        actions = RollingAdmissionPolicy(14400, False).select_actions(state)
        self.assertEqual(actions[0].target_sat_id, 2)
        self.assertEqual(actions[1].action_type, ActionType.WAIT)

    def test_future_one_hop_still_waits_in_multihop_mode(self):
        state = chain_state()
        state.links = {"0-2": {"up": False, "bandwidth_mbps": 1000,
                                "contact_windows": [(10, 100)]}}
        state.neighbors = {0: [], 1: [], 2: []}
        policy = RollingAdmissionPolicy(14400, False, input_lookahead_s=300)
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        self.assertEqual(policy.stats["wait_future_input"], 1)

    def test_quota_revalidates_full_path_and_never_dispatches_hidden_tiles(self):
        state = chain_state(4)
        state.config["decision_tile_limit_per_sat"] = 2
        state.satellites[1]["mem_remaining_gb"] = state.tiles["tile000"]["data_size_gb"]
        shadow = FixedShadow([Action(tid, ActionType.OFFLOAD, target_sat_id=2)
                              for tid in ("tile002", "tile003")])
        policy = TaskQuotaPolicy(shadow_policy=shadow)
        active = decision_state(state, 2)
        before = copy.deepcopy(active)
        actions = policy.select_actions(active)
        self.assertEqual([a.tile_id for a in actions], ["tile000", "tile001"])
        self.assertEqual(actions[0].target_sat_id, 2)
        self.assertEqual(tuple(actions[0].input_path), (0, 1, 2))
        self.assertEqual(actions[1].action_type, ActionType.WAIT)
        self.assertEqual(active, before)

    def test_quota_one_hop_mode_cannot_spend_a_two_hop_destination_quota(self):
        state = chain_state(2)
        state.config.update(decision_tile_limit_per_sat=1, input_relay_max_hops=1)
        shadow = FixedShadow([Action("tile001", ActionType.OFFLOAD, target_sat_id=2)])
        self.assertEqual(TaskQuotaPolicy(shadow_policy=shadow).select_actions(state)[0].action_type,
                         ActionType.WAIT)


if __name__ == "__main__":
    unittest.main()
