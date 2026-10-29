import copy
import unittest
from unittest.mock import patch

from analysis.sar_three_plan import Calendar, CandidateEnv, CandidatePipeline, RawImageAction, ThreePlanChoice
from sim.entities import Action, ActionType, Link, TileState
from sim.scheduler.contact_graph import Contact, ContactPlan
from sim.scheduler.literature import contact_plan
from test_ground_unique_relay import unique_env
from test_task_gate import gate_state


class ThreePlanTests(unittest.TestCase):
    def test_calendar_preserves_public_contact_guard(self):
        state = gate_state()
        state.config.update(relay_contact_guard=True, contact_scan_s=30)
        original = contact_plan(state)
        guarded = Calendar(state).plan
        for before, after in zip(original.contacts, guarded.contacts):
            if before.target in state.satellites:
                self.assertEqual(after.end, max(before.start, before.end - 30))
                self.assertLessEqual(after.volume, before.volume)
            else:
                self.assertEqual(after, before)

    def test_raw_packet_waiting_for_disconnected_next_hop_never_computes(self):
        env = unique_env("distributed")
        self.addCleanup(env.close)
        env._dist_pipeline = CandidatePipeline(env)
        commands = [RawImageAction(f"task_0_tile_{i}", ActionType.OFFLOAD, raw_hops=2) for i in range(4)]
        env.step(commands)
        for _ in range(3):
            env.step([])
        self.assertTrue(any(t.location == 1 for t in env.tiles.values()))
        env.topology.links["1-2"] = Link(1, 2, False, 2.88, 0)
        for _ in range(20):
            env.step([])
        self.assertFalse(env.metrics.compute_busy_time)
        self.assertTrue(all(t.timestamps.start_compute is None for t in env.tiles.values()))
        env.topology.links["1-2"] = Link(1, 2, True, 2.88, 0)
        for _ in range(40):
            env.step([])
        self.assertEqual(env.metrics.completed_tasks, 1)
        self.assertAlmostEqual(env.metrics.network_sent_mb["ground:gs"], 1.44)

    def test_raw_transit_is_not_satellite_compute_commitment(self):
        env = unique_env("distributed")
        self.addCleanup(env.close)
        env.__class__ = CandidateEnv
        env._dist_pipeline = CandidatePipeline(env)
        env.step([RawImageAction(f"task_0_tile_{i}", ActionType.OFFLOAD, raw_hops=2) for i in range(4)])
        state = env.export_state()
        self.assertTrue(all(s["committed_compute_s"] == 0 for s in state.satellites.values()))
        self.assertAlmostEqual(state.ground_stations["gs"]["committed_compute_s"], 1)
        self.assertFalse(state.tiles)

    def test_ground_adapter_matches_existing_ground_execution(self):
        baseline = unique_env(relay_max_hops=2)
        mixed = unique_env("distributed", relay_max_hops=8)
        self.addCleanup(baseline.close)
        self.addCleanup(mixed.close)
        mixed._dist_pipeline = CandidatePipeline(mixed)
        # 测试首步预置命令只隔离执行语义，实际策略仍在到达后下一决策步运行。
        commands = [RawImageAction(f"task_0_tile_{i}", ActionType.OFFLOAD, raw_hops=2) for i in range(4)]
        for step in range(40):
            baseline.step([])
            mixed.step(commands if step == 0 else [])
        self.assertEqual(baseline.summary()["task_outcomes"], mixed.summary()["task_outcomes"])
        self.assertEqual(baseline.metrics.network_sent_mb, mixed.metrics.network_sent_mb)
        self.assertEqual(baseline.ground_compute_busy_time, mixed.ground_compute_busy_time)
        self.assertEqual({tid: t.timestamps for tid, t in baseline.tiles.items()},
                         {tid: t.timestamps for tid, t in mixed.tiles.items()})

    def test_regular_actions_retain_original_pipeline_behavior(self):
        baseline, mixed = unique_env("distributed"), unique_env("distributed")
        self.addCleanup(baseline.close)
        self.addCleanup(mixed.close)
        mixed._dist_pipeline = CandidatePipeline(mixed)
        commands = [Action(f"task_0_tile_{i}", ActionType.LOCAL) for i in range(4)]
        for step in range(40):
            baseline.step(commands if step == 0 else [])
            mixed.step(commands if step == 0 else [])
        self.assertEqual(baseline.summary()["task_outcomes"], mixed.summary()["task_outcomes"])
        self.assertEqual(baseline.metrics.network_sent_mb, mixed.metrics.network_sent_mb)
        self.assertEqual(baseline.metrics.compute_busy_time, mixed.metrics.compute_busy_time)

    def test_fifo_packets_share_capacity_and_pipeline(self):
        state = gate_state()
        cal = Calendar(state)
        cal.plan = ContactPlan([Contact(0, 1, 0, 20, 1, 20), Contact(1, 2, 0, 20, 1, 20)])
        cal.free = {0: 0, 1: 0}
        self.assertEqual(cal.transport((0, 1), 4, 0, .1), 8)
        self.assertEqual(cal.transport((0, 1), 4, 0, .1), 12)
        self.assertIsNone(cal.transport((0, 1), 20, 0, .1))

    def test_unclipped_choice_prefers_350_to_900_and_does_not_mutate_state(self):
        state = gate_state()
        state.config["image_size_mb"] = 1
        before = copy.deepcopy(state)
        with patch.object(Calendar, "quote", side_effect=[900, 350, 400]):
            policy = ThreePlanChoice(8)
            policy.select_actions(state)
        self.assertEqual(policy.decisions[0]["selected"], "cgs")
        self.assertEqual(state, before)

    def test_ground_commits_unique_bytes_and_full_image_barrier(self):
        env = unique_env("distributed")
        self.addCleanup(env.close)
        env._dist_pipeline = CandidatePipeline(env)
        env.step([])
        commands = [RawImageAction(tid, ActionType.OFFLOAD, raw_hops=2) for tid in env.tasks["task_0"].tile_ids]
        for step in range(40):
            env.step(commands if step == 0 else [])
            for sid, sat in env.satellites.items():
                held = sum(env.memory_size_gb(t.data_size_mb) for t in env.tiles.values() if sid in t.memory_holders)
                self.assertAlmostEqual(sat.mem_used_gb, held)
            tiles = list(env.tiles.values())
            started = [t for t in tiles if t.timestamps.start_compute is not None]
            if started:
                self.assertTrue(all(t.timestamps.end_tx is not None for t in tiles))
                self.assertGreaterEqual(min(t.timestamps.start_compute for t in started), max(t.timestamps.end_tx for t in tiles))
        self.assertTrue(all(t.state == TileState.DONE for t in tiles))
        self.assertAlmostEqual(env.metrics.network_sent_mb["ground:gs"], 1.44)
        self.assertAlmostEqual(env.ground_compute_busy_time["gs"], 1)
        self.assertEqual(env.cfg.relay_max_hops, 8)
        self.assertTrue(all(len(t.raw_relay_path) <= 3 for t in tiles))

    def test_raw_hop_budget_does_not_change_global_result_protocol(self):
        env = unique_env("distributed")
        self.addCleanup(env.close)
        env._dist_pipeline = CandidatePipeline(env)
        env.step([])
        commands = [RawImageAction(tid, ActionType.OFFLOAD, raw_hops=1) for tid in env.tasks["task_0"].tile_ids]
        for step in range(10):
            env.step(commands if step == 0 else [])
        self.assertEqual(env.metrics.completed_tasks, 0)
        self.assertFalse(env.ground_compute_busy_time)
        self.assertEqual(env.cfg.relay_max_hops, 8)


if __name__ == "__main__":
    unittest.main()
