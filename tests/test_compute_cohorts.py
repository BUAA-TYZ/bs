from copy import deepcopy
from dataclasses import asdict
import unittest

from sim.config import config_from_dict
from sim.entities import FailureReason, TileState
from test_simulation import add_task, make_env
from analysis.run_sar_frozen import invariants


PROFILE = {"service_s": {"1": 0.6, "2": 1.0}, "workspace_gb": {"1": 1.0, "2": 1.5}}


class ComputeCohortTests(unittest.TestCase):
    def env(self, **overrides):
        values = dict(compute_service_mode="work_conserving", onboard_compute_profile=deepcopy(PROFILE),
                      compute_rate=1 / 0.6, compute_cost_per_tile=1, dt=0.5, vram_base_gb=1,
                      vram_alpha_per_mb=0, vram_capacity_gb=2)
        values.update(overrides)
        env = make_env(**values)
        self.addCleanup(env.close)
        return env

    def ready(self, env, count):
        tiles = add_task(env, count=count)
        for tile in tiles:
            tile.compute_cost = 1
            tile.state = TileState.READY
        return tiles

    def test_parallel_group_completion_is_not_linear_speedup(self):
        env = self.env(result_buffering=True, result_size_mb=0.25)
        tiles = self.ready(env, 5)
        for step in range(6):
            env.time = step
            env._dist_pipeline.advance_compute()
            if step == 0:
                self.assertEqual(len(env.satellites[0].executing_batch), 2)
                self.assertEqual(env.satellites[0].vram_used_gb, 1.5)
                self.assertTrue(all(tile.state == TileState.RUNNING for tile in tiles[:2]))
        self.assertEqual([tile.timestamps.end_compute for tile in tiles], [2, 2, 4, 4, 6])
        self.assertAlmostEqual(env.metrics.compute_busy_time[0], 2.6)
        self.assertEqual(env.satellites[0].vram_used_gb, 0)
        self.assertAlmostEqual(env.satellites[0].mem_used_gb, 5 * 0.25 / 1024)

    def test_singleton_does_not_wait_for_another_arrival(self):
        env = self.env(dt=1)
        tile = self.ready(env, 1)[0]
        env._dist_pipeline.advance_compute()
        self.assertEqual(tile.state, TileState.COMPUTED)
        self.assertAlmostEqual(env.metrics.compute_busy_time[0], 0.6)
        self.assertEqual(tile.result_available_time, 1)

    def test_input_readiness_barrier_and_cohort_commitment(self):
        env = self.env()
        tiles = self.ready(env, 3)
        tiles[2].timestamps.end_tx = 1
        env._dist_pipeline.advance_compute()
        self.assertIsNone(tiles[2].timestamps.start_compute)
        state = env.export_state()
        self.assertAlmostEqual(state.satellites[0]["committed_compute_s"], 1.1)
        self.assertEqual(state.satellites[0]["running_count"], 2)

    def test_cancel_one_member_preserves_other_service_and_workspace(self):
        env = self.env()
        a, b = self.ready(env, 2)
        env._dist_pipeline.advance_compute()
        env._fail_tile(a, FailureReason.VRAM_OOM)
        sat = env.satellites[0]
        self.assertEqual(sat.executing, b.tile_id)
        self.assertEqual(sat.vram_used_gb, 1.5)
        self.assertAlmostEqual(sat.batch_remaining_s, 0.5)
        env.time = 1
        env._dist_pipeline.advance_compute()
        self.assertEqual(b.state, TileState.COMPUTED)
        self.assertEqual(sat.vram_used_gb, 0)
        self.assertEqual(a.state, TileState.FAILED)

    def test_shared_memory_and_profile_validation(self):
        config = asdict(self.env().cfg)
        config.update(mem_capacity_gb=10, vram_capacity_gb=2, shared_memory_gb=16,
                      system_memory_reserve_gb=4)
        config_from_dict(config)
        config["mem_capacity_gb"] = 11
        with self.assertRaisesRegex(ValueError, "Shared memory"):
            config_from_dict(config)
        config["mem_capacity_gb"] = 10
        config["onboard_compute_profile"]["service_s"]["2"] = -1
        with self.assertRaisesRegex(ValueError, "finite and positive"):
            config_from_dict(config)

    def test_service_audit_counts_partial_cancelled_and_completed_cohorts_once(self):
        env = self.env()
        a, b = self.ready(env, 2)
        env._dist_pipeline.advance_compute()
        self.assertTrue(invariants(env)["passed"])
        env._fail_tile(a, FailureReason.VRAM_OOM)
        self.assertTrue(invariants(env)["passed"])
        env.time = 1
        env._dist_pipeline.advance_compute()
        self.assertEqual(b.state, TileState.COMPUTED)
        self.assertTrue(invariants(env)["passed"])
        env.metrics.compute_busy_time[0] += 0.1
        self.assertFalse(invariants(env)["passed"])


if __name__ == "__main__":
    unittest.main()
