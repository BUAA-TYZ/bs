import copy
from collections import Counter
import unittest

from sim.scheduler.task_quota import TaskQuotaPolicy
from sim.entities import Action, ActionType
from sim.marl.decision import decision_state
from test_rolling_admission import state_for_admission


class FixedShadow:
    def __init__(self, actions):
        self.actions = actions
        self.stats = Counter()

    def select_actions(self, state):
        self.stats["accepted"] += sum(a.action_type != ActionType.WAIT for a in self.actions)
        return self.actions


class TaskQuotaTests(unittest.TestCase):
    def test_real_shadow_moves_immediate_tail_quota_to_public_prefix(self):
        state = state_for_admission(8)
        state.config["decision_tile_limit_per_sat"] = 4
        state.satellites[0]["committed_compute_s"] = 100
        state.satellites[0]["compute_rate"] = 1
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 1000)
        state.satellites[2]["mem_remaining_gb"] = 4 * state.tiles["tile000"]["data_size_gb"]
        state.links["0-2"].update(up=False, contact_windows=[(10, 100)])
        state.neighbors[0] = []
        active = decision_state(state, 4)
        before = copy.deepcopy(active)
        policy = TaskQuotaPolicy()
        actions = policy.select_actions(active)
        self.assertEqual([a.tile_id for a in actions], [f"tile{i:03d}" for i in range(4)])
        self.assertTrue(all(a.action_type == ActionType.LOCAL for a in actions))
        self.assertEqual(policy.stats["shadow_wait_future_input"], 4)
        self.assertEqual(policy.stats["shadow_accepted"], 4)
        self.assertEqual(policy.stats["shadow_planned_tiles"], 8)
        self.assertEqual(policy.stats["accepted"], 4)
        self.assertEqual(policy.stats["reassigned_current_quota"], 4)
        self.assertEqual(active, before)

    def test_shared_memory_and_contact_bytes_checked_after_heterogeneous_remap(self):
        for constraint in ("memory", "contact"):
            with self.subTest(constraint=constraint):
                state = state_for_admission(4)
                state.config["decision_tile_limit_per_sat"] = 2
                for tid in ("tile000", "tile001"):
                    state.tiles[tid].update(data_size_mb=2, data_size_gb=2 / 1024)
                if constraint == "memory":
                    state.satellites[2]["mem_remaining_gb"] = 2 / 1024
                else:
                    state.links["0-2"]["contact_windows"] = [(0, 3)]
                shadow = FixedShadow([Action(tid, ActionType.OFFLOAD, target_sat_id=2)
                                      for tid in ("tile002", "tile003")])
                policy = TaskQuotaPolicy(shadow_policy=shadow)
                actions = policy.select_actions(decision_state(state, 2))
                self.assertEqual(actions[0].target_sat_id, 2)
                self.assertEqual(actions[1].action_type, ActionType.WAIT)
                self.assertEqual(policy.stats["accepted"], 1)
                self.assertEqual(policy.stats["shadow_accepted"], 2)

    def test_input_backlog_is_included_and_small_packets_are_not_serialized_by_steps(self):
        state = state_for_admission(64)
        state.links["0-2"].update(bandwidth_mbps=10_000, contact_windows=[(0, 1)])
        for tile in state.tiles.values():
            tile.update(data_size_mb=10, data_size_gb=0.01)
        shadow = FixedShadow([Action(tid, ActionType.OFFLOAD, target_sat_id=2) for tid in state.tiles])
        self.assertEqual(TaskQuotaPolicy(shadow_policy=shadow).select_actions(state)[-1].target_sat_id, 2)
        state.satellites[0]["isl_backlog_mb"][2] = 1000
        actions = TaskQuotaPolicy(shadow_policy=shadow).select_actions(state)
        self.assertEqual(sum(a.target_sat_id == 2 for a in actions), 25)

    def test_quotas_never_cross_task_or_source_boundaries(self):
        state = state_for_admission(4)
        state.config["decision_tile_limit_per_sat"] = 2
        state.tiles["tile002"]["location"] = 1
        state.tiles["tile003"]["parent_task_id"] = "other"
        shadow = FixedShadow([Action("tile002", ActionType.OFFLOAD, target_sat_id=2),
                              Action("tile003", ActionType.OFFLOAD, target_sat_id=2)])
        state.tiles = {tid: state.tiles[tid] for tid in ("tile000", "tile001")}
        actions = TaskQuotaPolicy(shadow_policy=shadow).select_actions(state)
        self.assertTrue(all(a.action_type == ActionType.WAIT for a in actions))

    def test_vram_and_current_neighbor_validation_reject_illegal_quota(self):
        for constraint in ("vram", "neighbor", "down"):
            with self.subTest(constraint=constraint):
                state = state_for_admission(2)
                state.config["decision_tile_limit_per_sat"] = 1
                if constraint == "vram":
                    state.tiles["tile000"]["vram_req_gb"] = 2
                elif constraint == "neighbor":
                    state.neighbors[0] = []
                else:
                    state.links["0-2"].update(up=False, contact_windows=[(10, 100)])
                shadow = FixedShadow([Action("tile001", ActionType.OFFLOAD, target_sat_id=2)])
                self.assertEqual(TaskQuotaPolicy(shadow_policy=shadow).select_actions(state)[0].action_type,
                                 ActionType.WAIT)

    def test_visible_ground_only_and_ground_compute_does_not_require_satellite_vram(self):
        state = state_for_admission(2)
        state.config["decision_tile_limit_per_sat"] = 1
        state.tiles["tile000"]["vram_req_gb"] = 2
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 100)
        shadow = FixedShadow([Action("tile001", ActionType.OFFLOAD, target_gs_id="gs")])
        policy = TaskQuotaPolicy(shadow_policy=shadow)
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        state.ground_options[0] = [{"gs_id": "gs", "bandwidth_mbps": 8}]
        self.assertEqual(policy.select_actions(state)[0].target_gs_id, "gs")

    def test_no_hidden_actions_even_when_input_state_contains_more_than_limit(self):
        state = state_for_admission(8)
        state.config["decision_tile_limit_per_sat"] = 4
        shadow = FixedShadow([Action(tid, ActionType.LOCAL) for tid in state.tiles])
        actions = TaskQuotaPolicy(shadow_policy=shadow).select_actions(state)
        self.assertEqual(len(actions), 4)
        self.assertEqual([a.tile_id for a in actions], [f"tile{i:03d}" for i in range(4)])

    def test_ready_and_expired_sla_do_not_get_dropped(self):
        state = state_for_admission(2)
        state.tiles["tile000"].update(state="READY", waiting_time=301)
        state.tiles["tile001"]["waiting_time"] = 301
        shadow = FixedShadow([Action("tile001", ActionType.LOCAL)])
        policy = TaskQuotaPolicy(shadow_policy=shadow)
        actions = policy.select_actions(state)
        self.assertEqual([a.action_type for a in actions], [ActionType.LOCAL, ActionType.LOCAL])
        self.assertEqual(policy.stats["ready_continue"], 1)
        self.assertEqual(policy.stats["accepted"], 1)


if __name__ == "__main__":
    unittest.main()
