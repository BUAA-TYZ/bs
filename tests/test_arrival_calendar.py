import copy
import unittest

from sim.entities import Action, ActionType, EnvState
from sim.marl.decision import decision_state
from sim.scheduler.arrival_calendar import ArrivalCalendarPolicy, Commitment
from sim.scheduler.literature import CGSComputeAdapted, ground_node
from test_reservation import contention_state
from test_simulation import add_task, make_env


def future_ground_state(time=0, tid="first", work=20, ground_work=0):
    tile = {"location": 0, "state": "QUEUED", "in_transfer": False,
            "data_size_mb": 1, "data_size_gb": 0.001, "compute_cost": work,
            "vram_req_gb": 0.1, "result_size_mb": 0.01, "waiting_time": 0,
            "parent_task_id": "image", "task_progress": {"failed": 0},
            "deadline_remaining_s": None}
    satellite = {"compute_rate": 1, "committed_compute_s": max(0, 69 - time),
                 "mem_remaining_gb": 1, "isl_backlog_mb": {}, "ground_backlog_mb": {},
                 "next_gs_windows": {"gs": (max(0, 50 - time), 1000)}}
    ground = {"compute_rate": 1, "committed_compute_s": ground_work,
              "bandwidth_mbps": 8000, "queue_len": 0, "running": 0}
    return EnvState(time, {0: satellite}, {"gs": ground}, {0: []}, {0: []}, {}, {tid: tile},
                    {"dt": 1, "vram_capacity_gb": 1, "compute_service_mode": "work_conserving",
                     "ground_input_relay": True, "input_relay_max_hops": 2,
                     "result_relay": True, "relay_max_hops": 8},
                    pending_task_tiles={"image": {tid: tile}})


class ArrivalCalendarTests(unittest.TestCase):
    def test_second_round_does_not_forget_future_ground_release(self):
        policy = ArrivalCalendarPolicy()
        first = future_ground_state()
        action = policy.select_actions(first)[0]
        self.assertEqual(action.target_gs_id, "gs")
        self.assertTrue(action.relay_to_ground)
        self.assertEqual(policy.stats["accepted_future_ground_window"], 1)
        second = future_ground_state(1, "second", ground_work=20)
        self.assertEqual(CGSComputeAdapted().select_actions(second)[0].target_gs_id, "gs")
        self.assertEqual(policy.select_actions(second)[0].action_type, ActionType.LOCAL)
        self.assertEqual(policy.stats["calendar_release_gap_observations"], 1)

    def test_wait_has_no_persistent_commitment(self):
        state = future_ground_state()
        state.tiles["first"]["deadline_remaining_s"] = 1
        policy = ArrivalCalendarPolicy()
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        self.assertFalse(any(entry.tile_id for entries in policy.calendar.values() for entry in entries))

    def test_zero_public_work_clears_future_reservation(self):
        policy = ArrivalCalendarPolicy()
        policy.select_actions(future_ground_state())
        state = future_ground_state(1, "new", ground_work=0)
        state.tiles = {}
        state.pending_task_tiles = {}
        policy.select_actions(state)
        self.assertNotIn(ground_node("gs"), policy.calendar)

    def test_public_progress_updates_remaining_service_without_double_charge(self):
        policy = ArrivalCalendarPolicy()
        policy.calendar[ground_node("gs")] = [Commitment("old", 50, 20)]
        state = future_ground_state(55, "new", ground_work=15)
        state.tiles = {}
        state.pending_task_tiles = {}
        _, busy = policy._processing_calendar(state)
        self.assertEqual(busy[ground_node("gs")], 15)
        self.assertEqual(policy.calendar[ground_node("gs")][0].remaining_service_seconds, 15)

    def test_unaccepted_action_and_clock_reset_do_not_leave_stale_work(self):
        policy = ArrivalCalendarPolicy()
        first = future_ground_state()
        policy.select_actions(first)
        second = future_ground_state(1, "first", ground_work=0)
        policy._processing_calendar(second)
        self.assertNotIn(ground_node("gs"), policy.calendar)
        self.assertEqual(policy.stats["calendar_unaccepted_removed"], 1)
        policy.calendar[ground_node("gs")] = [Commitment("stale", 500, 20)]
        policy._processing_calendar(future_ground_state(0))
        self.assertNotIn(ground_node("gs"), policy.calendar)
        self.assertEqual(policy.stats["calendar_resets"], 1)

    def test_same_round_ground_compute_budget_and_state_immutability(self):
        state = future_ground_state()
        state.tiles["second"] = {**state.tiles["first"]}
        state.pending_task_tiles["image"]["second"] = state.tiles["second"]
        before = copy.deepcopy(state)
        actions = ArrivalCalendarPolicy().select_actions(state)
        self.assertEqual(actions[0].target_gs_id, "gs")
        self.assertEqual(actions[1].action_type, ActionType.LOCAL)
        self.assertEqual(state, before)

    def test_same_round_input_capacity_is_not_expanded(self):
        state = future_ground_state()
        state.satellites[0]["next_gs_windows"]["gs"] = (50, 1)
        state.ground_stations["gs"]["bandwidth_mbps"] = 8
        state.tiles["first"].update(vram_req_gb=2, data_size_mb=0.75)
        state.tiles["second"] = {**state.tiles["first"]}
        state.pending_task_tiles["image"] = dict(state.tiles)
        actions = ArrivalCalendarPolicy().select_actions(state)
        self.assertEqual(actions[0].target_gs_id, "gs")
        self.assertEqual(actions[1].action_type, ActionType.WAIT)

    def test_shared_receiver_memory_is_not_overbooked(self):
        state = contention_state(False)
        state.config["compute_service_mode"] = "work_conserving"
        state.satellites[2]["next_gs_windows"]["gs"] = (0, 1000)
        state.satellites[2]["mem_remaining_gb"] = state.tiles["t0"]["data_size_gb"]
        actions = ArrivalCalendarPolicy().select_actions(state)
        self.assertEqual(actions[0].target_sat_id, 2)
        self.assertEqual(actions[1].action_type, ActionType.WAIT)

    def test_exact_policy_parity_without_release_gaps(self):
        state = future_ground_state()
        state.config["ground_input_relay"] = False
        state.satellites[0]["committed_compute_s"] = 0
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 1000)
        state.ground_options[0] = [{"gs_id": "gs", "bandwidth_mbps": 8000}]
        self.assertEqual(ArrivalCalendarPolicy().select_actions(state), CGSComputeAdapted().select_actions(state))

    def test_environment_actions_respect_public_limit_and_complete(self):
        env = make_env(execution_commitment=True)
        self.addCleanup(env.close)
        env.vis_cache.windows_at = lambda sid, time: {"gs": (0, 100)}
        add_task(env, count=8)
        policy = ArrivalCalendarPolicy()
        for _ in range(40):
            state = decision_state(env.export_state())
            before = copy.deepcopy(state)
            actions = policy.select_actions(state)
            self.assertLessEqual(len(actions), 4 * len(state.satellites))
            self.assertTrue(all(action.tile_id in state.tiles for action in actions))
            self.assertEqual(state, before)
            env.step(actions)
        self.assertEqual(env.summary()["overall"]["completed_tasks"], 1)

    def test_real_executor_future_window_competition(self):
        def replay(policy):
            env = make_env(execution_commitment=True, ground_input_relay=True,
                           compute_service_mode="work_conserving")
            self.addCleanup(env.close)
            original_refresh = env.vis_cache.refresh

            def refresh(t, *args):
                env.vis_cache.visible = t >= 50
                original_refresh(t, *args)

            env.vis_cache.refresh = refresh
            env.vis_cache.windows_at = lambda sid, t: {"gs": (max(0, 50 - t), 1000)}
            previous = add_task(env)[0]
            previous.compute_cost = 69
            env.step([Action(previous.tile_id, ActionType.LOCAL)])
            first = add_task(env)[0]
            first.compute_cost = 20
            action1 = policy.select_actions(decision_state(env.export_state()))[0]
            env.step([action1])
            second = add_task(env)[0]
            second.compute_cost = 20
            action2 = policy.select_actions(decision_state(env.export_state()))[0]
            env.step([action2])
            while env.time < 100:
                env.step(policy.select_actions(decision_state(env.export_state())))
            return action1, action2, env.summary()

        plain_first, plain_second, plain = replay(CGSComputeAdapted())
        aware_first, aware_second, aware = replay(ArrivalCalendarPolicy())
        self.assertTrue(plain_first.relay_to_ground and aware_first.relay_to_ground)
        self.assertTrue(plain_second.relay_to_ground)
        self.assertEqual(aware_second.action_type, ActionType.LOCAL)
        self.assertEqual(plain["overall"]["completed_tasks"], 3)
        self.assertEqual(aware["overall"]["completed_tasks"], 3)
        self.assertLess(aware["task_outcomes"][2]["completion_seconds"],
                        plain["task_outcomes"][2]["completion_seconds"])


if __name__ == "__main__":
    unittest.main()
