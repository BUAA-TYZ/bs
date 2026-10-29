import copy
import unittest

from sim.entities import Action, ActionType
from sim.marl.decision import decision_state
from sim.scheduler.contact_graph import Contact, ContactPlan
from sim.scheduler.task_choice import CGSTaskChoice, batch_arrivals
from test_task_gate import gate_state


class TaskChoiceTests(unittest.TestCase):
    def test_shared_batch_cannot_give_each_flow_full_bandwidth(self):
        plan = ContactPlan([Contact(0, 1, 0, 100, 1, 100)])
        self.assertEqual(batch_arrivals(plan, {"a": (0,), "b": (0,)}, {"a": 4, "b": 4}, 0.1),
                         {"a": 8, "b": 8})
        self.assertEqual(plan.contacts[0].volume, 92)

    def test_aggregate_volume_and_multihop_window_are_checked(self):
        plan = ContactPlan([Contact(0, 1, 0, 6, 1, 6)])
        self.assertIsNone(batch_arrivals(plan, {"a": (0,), "b": (0,)}, {"a": 4, "b": 4}, 0.1))
        self.assertEqual(plan.contacts[0].volume, 6)
        plan = ContactPlan([Contact(0, 1, 0, 10, 1, 10), Contact(1, 2, 0, 6, 1, 6)])
        self.assertIsNone(batch_arrivals(plan, {"a": (0, 1)}, {"a": 4}, 0.1))
        self.assertEqual(plan.contacts[0].volume, 10)

    def test_selects_fast_helper_with_existing_source_queue(self):
        state = gate_state()
        state.satellites[0]["committed_compute_s"] = 100
        before = copy.deepcopy(state)
        policy = CGSTaskChoice()
        actions = policy.select_actions(state)
        self.assertEqual(policy.last_prediction["selected"], "cgs")
        self.assertEqual(actions[0].action_type, ActionType.OFFLOAD)
        self.assertEqual(state, before)

    def test_source_profile_is_measured_in_windows(self):
        state = gate_state()
        state.config["onboard_compute_profile"] = {"service_s": {"1": 0.597, "2": 1.056}}
        tile = state.tiles["t0"]
        state.tiles = {f"t{i}": {**tile, "compute_cost": 4, "result_size_mb": 0} for i in range(16)}
        quote = CGSTaskChoice().quote(state, [Action(tid, ActionType.LOCAL) for tid in state.tiles])
        self.assertAlmostEqual(quote["task0"], 33.9)

    def test_hidden_siblings_fall_back_without_dispatching_hidden_work(self):
        state = gate_state()
        state.tiles["t1"] = dict(state.tiles["t0"])
        state.pending_task_tiles = {"task0": dict(state.tiles)}
        state.config["decision_tile_limit_per_sat"] = 1
        limited = decision_state(state, 1)
        policy = CGSTaskChoice()
        actions = policy.select_actions(limited)
        self.assertEqual(policy.stats["choice_fallback"], 1)
        self.assertEqual(len(actions), 1)
        self.assertIn(actions[0].tile_id, limited.tiles)

    def test_already_running_sibling_does_not_get_whole_task_quote(self):
        state = gate_state()
        state.tiles["t0"]["task_progress"]["running"] = 1
        policy = CGSTaskChoice()
        policy.select_actions(state)
        self.assertEqual(policy.stats["choice_fallback"], 1)

    def test_quote_rejects_memory_overbooking(self):
        state = gate_state()
        state.tiles["t1"] = dict(state.tiles["t0"])
        state.satellites[2]["mem_remaining_gb"] = state.tiles["t0"]["data_size_gb"]
        actions = [Action(tid, ActionType.OFFLOAD, target_sat_id=2) for tid in state.tiles]
        self.assertIsNone(CGSTaskChoice().quote(state, actions))

    def test_inflight_sibling_missing_from_pending_is_not_a_complete_task(self):
        state = gate_state()
        state.tiles["t0"]["task_progress"]["total"] = 2
        policy = CGSTaskChoice()
        policy.select_actions(state)
        self.assertEqual(policy.stats["choice_fallback"], 1)


if __name__ == "__main__":
    unittest.main()
