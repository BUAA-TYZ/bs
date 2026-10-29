import copy
import math
from types import SimpleNamespace
import unittest

from sim.entities import ActionType, Link
from sim.marl.decision import decision_state
from sim.scheduler.task_delivery import TaskDeliveryPolicy
from test_ground_input_planners import ground_state
from test_reservation import contention_state
from test_rolling_admission import state_for_admission
from test_simulation import add_task, make_env


def image_competition(window=4, second_size=2):
    state = contention_state(False)
    state.config.update(compute_service_mode="work_conserving", result_relay=False,
                        decision_tile_limit_per_sat=64)
    state.satellites[2]["next_gs_windows"]["gs"] = (0, window)
    first, second = state.tiles["t0"], state.tiles["t1"]
    state.tiles = {
        "t0": {**first, "parent_task_id": "a"},
        "t1": {**second, "parent_task_id": "b"},
        "t2": {**first, "parent_task_id": "a"},
    }
    if second_size == 2:
        state.tiles["t3"] = {**second, "parent_task_id": "b"}
    state.pending_task_tiles = {
        task: {tid: tile for tid, tile in state.tiles.items() if tile["parent_task_id"] == task}
        for task in ("a", "b")
    }
    return state


class TaskDeliveryTests(unittest.TestCase):
    def test_whole_image_admission_avoids_two_incomplete_images(self):
        state = image_competition()
        baseline, task_level = TaskDeliveryPolicy(4, max_orders=1), TaskDeliveryPolicy(4)
        first = baseline.select_actions(state)
        second = task_level.select_actions(state)
        self.assertEqual([a.tile_id for a in first if a.action_type != ActionType.WAIT], ["t0", "t1"])
        self.assertEqual([a.tile_id for a in second if a.action_type != ActionType.WAIT], ["t0", "t2"])
        self.assertEqual(baseline.last_prediction["on_time_tasks"], 0)
        self.assertEqual(task_level.last_prediction["on_time_tasks"], 1)
        self.assertEqual(task_level.last_prediction["selected"], "deadline")
        self.assertEqual(task_level.last_prediction["task_latency"]["a"], 4)

    def test_equal_completion_count_prefers_smaller_truncated_task_latency(self):
        state = image_competition(window=100, second_size=1)
        baseline, task_level = TaskDeliveryPolicy(10, max_orders=1), TaskDeliveryPolicy(10)
        baseline.select_actions(state)
        actions = task_level.select_actions(state)
        self.assertEqual(baseline.last_prediction["on_time_tasks"], 2)
        self.assertEqual(task_level.last_prediction["on_time_tasks"], 2)
        self.assertEqual(baseline.last_prediction["truncated_latency_sum"], 9)
        self.assertEqual(task_level.last_prediction["truncated_latency_sum"], 8)
        self.assertEqual(task_level.last_prediction["selected"], "scarcity")
        self.assertEqual([a.tile_id for a in actions], ["t1", "t0", "t2"])

    def test_shadow_siblings_never_bypass_public_action_limit(self):
        state = state_for_admission(8)
        state.config["decision_tile_limit_per_sat"] = 2
        visible = decision_state(state, 2)
        before = copy.deepcopy(visible)
        policy = TaskDeliveryPolicy()
        actions = policy.select_actions(visible)
        self.assertEqual({a.tile_id for a in actions}, set(visible.tiles))
        self.assertEqual(policy.last_prediction["pending_tiles"], 8)
        self.assertEqual(policy.last_prediction["active_tiles"], 2)
        self.assertEqual(visible, before)
        # 隐藏的最后一片无法计算时，整图不能只按可见前缀记成功。
        visible.pending_task_tiles["image"]["tile007"]["vram_req_gb"] = 2
        policy.select_actions(visible)
        self.assertEqual(policy.last_prediction["on_time_tasks"], 0)
        self.assertTrue(math.isinf(policy.last_prediction["task_latency"]["image"]))

    def test_hidden_siblings_wait_for_subsequent_decision_batches(self):
        state = state_for_admission(3)
        state.config["decision_tile_limit_per_sat"] = 1
        state.satellites[2]["compute_rate"] = 100
        state.links["0-2"]["bandwidth_mbps"] = 10_000
        policy = TaskDeliveryPolicy(decision_interval_s=10)
        actions = policy.select_actions(decision_state(state, 1))
        self.assertEqual([a.tile_id for a in actions], ["tile000"])
        self.assertGreaterEqual(policy.last_prediction["task_latency"]["image"], 22)

        state.config["decision_interval_seconds"] = 10
        from_public = TaskDeliveryPolicy()
        self.assertEqual(from_public.select_actions(decision_state(state, 1)), actions)
        self.assertEqual(from_public.last_prediction, policy.last_prediction)

        state = state_for_admission(5)
        state.config["decision_tile_limit_per_sat"] = 2
        state.satellites[2]["compute_rate"] = 100
        state.links["0-2"]["bandwidth_mbps"] = 10_000
        # 公共前缀与分片 ID 次序可以不同，后续每批仍不得超过两片。
        state.tiles = {tid: state.tiles[tid] for tid in ("tile000", "tile004")}
        policy = TaskDeliveryPolicy(decision_interval_s=10)
        self.assertEqual(len(policy.select_actions(state)), 2)
        self.assertGreaterEqual(policy.last_prediction["task_latency"]["image"], 22)

    def test_future_task_and_tasks_outside_current_action_prefix_are_not_considered(self):
        state = state_for_admission(2)
        reference = TaskDeliveryPolicy()
        expected = reference.select_actions(state)
        foreign = {**state.tiles["tile000"], "parent_task_id": "future", "waiting_time": -10}
        state.pending_task_tiles["future"] = {"future0": foreign}
        state.pending_task_tiles["image"]["future_sibling"] = {
            **foreign, "parent_task_id": "image", "vram_req_gb": 100,
        }
        state.pending_task_tiles["outside_prefix"] = {
            "outside": {**foreign, "parent_task_id": "outside_prefix", "waiting_time": 100},
        }
        policy = TaskDeliveryPolicy()
        self.assertEqual(policy.select_actions(state), expected)
        self.assertEqual(policy.last_prediction, reference.last_prediction)
        self.assertEqual(policy.last_prediction["pending_tiles"], 2)

    def test_single_task_reuses_fcfs_and_no_candidate_replaces_equal_score(self):
        state = state_for_admission(4)
        policy = TaskDeliveryPolicy()
        policy.select_actions(state)
        self.assertEqual(policy.stats["candidate_plans"], 1)
        self.assertEqual(policy.stats["single_task_or_fcfs_reuse"], 1)
        state = contention_state(False)
        state.satellites[2]["next_gs_windows"]["gs"] = (0, 100)
        policy = TaskDeliveryPolicy()
        policy.select_actions(state)
        self.assertEqual(policy.last_prediction["selected"], "fcfs")
        self.assertLessEqual(policy.stats["candidate_plans"], 3)

    def test_late_tasks_continue_and_hard_expiry_or_failed_images_do_not(self):
        state = state_for_admission()
        policy = TaskDeliveryPolicy()
        state.tiles["tile000"]["waiting_time"] = 301
        self.assertEqual(policy.select_actions(state)[0].target_sat_id, 2)
        self.assertEqual(policy.last_prediction["on_time_tasks"], 0)
        state.tiles["tile000"]["deadline_remaining_s"] = 0
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        state.tiles["tile000"]["state"] = "READY"
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.LOCAL)
        state.tiles["tile000"]["state"] = "QUEUED"
        state.tiles["tile000"]["task_progress"] = {"failed": 1}
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)

    def test_ground_relay_and_satellite_paths_use_shared_actions(self):
        state = ground_state()
        state.pending_task_tiles = {"task": state.tiles.copy()}
        for mode in ("contact_graph", "execution"):
            with self.subTest(mode=mode):
                before = copy.deepcopy(state)
                action = TaskDeliveryPolicy(return_mode=mode).select_actions(state)[0]
                self.assertEqual(action.target_gs_id, "gs")
                self.assertTrue(action.relay_to_ground)
                self.assertFalse(action.input_path)
                self.assertEqual(state, before)
        state.config["ground_input_relay"] = False
        state.satellites[2]["compute_rate"] = 100
        action = TaskDeliveryPolicy().select_actions(state)[0]
        self.assertEqual(action.input_path, (0, 1, 2))
        self.assertEqual(action.target_sat_id, 2)

    def test_cumulative_resources_and_sibling_failure_are_not_ignored(self):
        for resource in ("memory", "input", "result"):
            with self.subTest(resource=resource):
                state = state_for_admission(2)
                tile = state.tiles["tile000"]
                if resource == "memory":
                    state.satellites[2]["mem_remaining_gb"] = tile["data_size_gb"]
                elif resource == "input":
                    state.links["0-2"].update(bandwidth_mbps=1, contact_windows=[(0, 1)])
                else:
                    state.satellites[2]["next_gs_windows"]["gs"] = (10, 1)
                    for row in state.tiles.values():
                        row["result_size_mb"] = 0.75
                policy = TaskDeliveryPolicy()
                actions = policy.select_actions(state)
                self.assertEqual(sum(a.action_type != ActionType.WAIT for a in actions), 1)
                self.assertEqual(policy.last_prediction["on_time_tasks"], 0)

    def test_real_pipeline_completes_one_whole_image_instead_of_two_halves(self):
        def completed_by_deadline(max_orders, dt):
            window = 4 if dt == 1 else 2.5
            env = make_env(num_sats=3, sim_steps=6, execution_commitment=True,
                           compute_service_mode="work_conserving", result_relay=False,
                           result_size_mb=0.125, satellite_compute_rates=[0.1, 0.1, 1],
                           ground_relay_delay_s=0, aggregation_delay_s=0,
                           decision_tile_limit_per_sat=64, dt=dt)
            self.addCleanup(env.close)
            env.topology.links = {f"{src}-2": Link(src, 2, True, 8, 0) for src in (0, 1)}
            visibility = env.vis_cache

            def refresh(t, *args, visibility=visibility):
                visibility.visible_gs_by_sat = {sid: ["gs"] if sid == 2 and t * dt < window else []
                                               for sid in range(3)}
                visibility.best_gs_by_sat = {sid: "gs" if options else None
                                            for sid, options in visibility.visible_gs_by_sat.items()}

            visibility.refresh = refresh
            visibility.windows_at = lambda sid, t: {
                "gs": (0, window - t * dt) if sid == 2 and t * dt < window else None}
            visibility.is_visible = lambda sid, gid, stations, visibility=visibility: (
                gid in visibility.visible_gs_by_sat.get(sid, []))
            env.contact_cache = SimpleNamespace(windows_at=lambda seconds: {
                "0-2": [(0, 100)], "1-2": [(0, 100)]})
            first, second = add_task(env, src=0, count=2, size=0.125), add_task(env, src=1, count=2, size=0.125)
            names = {tile.tile_id: f"t{i}" for i, tile in enumerate((first[0], second[0], first[1], second[1]))}
            for tile in env.tiles.values():
                tile.tile_id = names[tile.tile_id]
            env.tiles = {tile.tile_id: tile for tile in (first[0], second[0], first[1], second[1])}
            for task in env.tasks.values():
                task.tile_ids[:] = [names[tid] for tid in task.tile_ids]
            for satellite in env.satellites.values():
                satellite.queue[:] = [names[tid] for tid in satellite.queue]
            policy = TaskDeliveryPolicy(window, max_orders=max_orders)
            for _ in range(math.ceil(window / dt) + 2):
                actions = policy.select_actions(env.export_state())
                env.step(actions)
            self.assertEqual(env.metrics.failed_tiles, 0)
            for link, sent in env.metrics.network_sent_mb.items():
                self.assertLessEqual(sent, env.metrics.network_capacity_mb[link] + 1e-9)
            return sum(task.completion_time is not None and task.completion_time * dt <= window + 1e-9
                       for task in env.tasks.values())

        for dt in (1, 0.1, 0.05):
            with self.subTest(dt=dt):
                self.assertEqual([completed_by_deadline(order, dt) for order in (1, 3)], [0, 1])

    def test_contiguous_task_ids_rollback_larger_image_for_completable_smaller_image(self):
        outcomes = []
        for max_orders in (1, 3):
            env = make_env(num_sats=2, execution_commitment=True,
                           compute_service_mode="work_conserving", result_relay=False,
                           result_size_mb=0.125, satellite_compute_rates=[0.1, 1],
                           ground_relay_delay_s=0, aggregation_delay_s=0,
                           decision_tile_limit_per_sat=64)
            self.addCleanup(env.close)
            visibility = env.vis_cache

            def refresh(t, *args, visibility=visibility):
                visibility.visible_gs_by_sat = {0: [], 1: ["gs"] if t < 4 else []}
                visibility.best_gs_by_sat = {sid: "gs" if options else None
                                            for sid, options in visibility.visible_gs_by_sat.items()}

            visibility.refresh = refresh
            visibility.windows_at = lambda sid, t: {"gs": (0, 4 - t) if sid == 1 and t < 4 else None}
            visibility.is_visible = lambda sid, gid, stations, visibility=visibility: (
                gid in visibility.visible_gs_by_sat.get(sid, []))
            env.contact_cache = SimpleNamespace(windows_at=lambda seconds: {"0-1": [(0, 100)]})
            # 两图剩余工作不同；保留生成器的连续任务 ID，不声称这是 315 片同质主场景。
            older = add_task(env, count=3, size=0.125)
            newer = add_task(env, count=2, size=0.125)
            self.assertEqual(list(env.tiles), sorted(env.tiles))
            policy = TaskDeliveryPolicy(4, max_orders=max_orders)
            for _ in range(6):
                env.step(policy.select_actions(env.export_state()))
            outcomes.append([task_id for task_id, task in env.tasks.items()
                             if task.completion_time is not None and task.completion_time <= 4])
            self.assertEqual(env.metrics.failed_tiles, 0)
            if max_orders == 1:
                self.assertEqual(sum(tile.state.value == "DONE" for tile in older), 2)
                self.assertEqual(sum(tile.state.value == "DONE" for tile in newer), 0)
            for link, sent in env.metrics.network_sent_mb.items():
                self.assertLessEqual(sent, env.metrics.network_capacity_mb[link] + 1e-9)
        self.assertEqual(outcomes, [[], ["task_1"]])

    def test_completed_sibling_does_not_keep_obsolete_tail_prediction(self):
        state = state_for_admission(2)
        state.config["decision_tile_limit_per_sat"] = 1
        state.satellites[2]["next_gs_windows"]["gs"] = (310, 1000)
        policy = TaskDeliveryPolicy()
        policy.select_actions(state)
        self.assertGreater(policy.last_prediction["task_latency"]["image"], 300)
        state.time = 10
        state.tiles = {"tile001": state.tiles["tile001"]}
        state.pending_task_tiles = {"image": state.tiles.copy()}
        state.tiles["tile001"].update(waiting_time=10, task_progress={
            "total": 2, "done": 0, "failed": 0, "running": 0, "awaiting_delivery": 1,
        })
        state.satellites[2]["next_gs_windows"]["gs"] = (0, 1000)
        policy.select_actions(state)
        self.assertEqual(policy.last_prediction["task_latency"]["image"], 311)
        self.assertEqual(policy.stats["cleared_completed_tail"], 0)
        state.tiles["tile001"]["task_progress"].update(done=1, awaiting_delivery=0)
        policy.select_actions(state)
        self.assertEqual(policy.last_prediction["task_latency"]["image"], 13)
        self.assertEqual(policy.stats["cleared_completed_tail"], 1)

    def test_local_fifo_reordering_is_rejected_in_real_pipeline(self):
        env = make_env(execution_commitment=True, compute_service_mode="work_conserving",
                       result_relay=False, result_size_mb=0.125, satellite_compute_rates=[1, 0.01],
                       ground_stations=[{"id": "gs", "bandwidth_mbps": 8, "compute_rate": 0.01}],
                       ground_relay_delay_s=0, aggregation_delay_s=0, decision_tile_limit_per_sat=64)
        self.addCleanup(env.close)
        env.vis_cache.windows_at = lambda sid, t: {"gs": (0, 100)}
        env.contact_cache = SimpleNamespace(windows_at=lambda seconds: {"0-1": [(0, 100)]})
        add_task(env, count=2, size=0.125)
        add_task(env, count=1, size=0.125)
        policy = TaskDeliveryPolicy(10)
        actions = policy.select_actions(env.export_state())
        self.assertEqual([action.tile_id for action in actions], env.satellites[0].queue)
        self.assertTrue(all(action.action_type == ActionType.LOCAL for action in actions))
        self.assertEqual(policy.last_prediction["selected"], "fcfs")
        self.assertEqual(policy.last_prediction["truncated_latency_sum"], 7)
        self.assertEqual(policy.stats["rejected_local_fifo_order"], 1)
        env.step(actions)
        for _ in range(5):
            env.step(policy.select_actions(env.export_state()))
        self.assertEqual({tid: task.completion_time for tid, task in env.tasks.items()},
                         {"task_0": 3, "task_1": 4})

    def test_invalid_parameters_rejected(self):
        for kwargs in ({"sla_seconds": 0}, {"sla_seconds": math.inf}, {"return_mode": "unknown"},
                       {"max_orders": 0}, {"max_orders": 4}, {"max_orders": True},
                       {"decision_interval_s": 0}, {"decision_interval_s": math.nan}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                TaskDeliveryPolicy(**kwargs)


if __name__ == "__main__":
    unittest.main()
