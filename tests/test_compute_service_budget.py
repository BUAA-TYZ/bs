from dataclasses import asdict
import math
import unittest

from sim.config import config_from_dict
from sim.entities import FailureReason, TileState
from test_simulation import add_task, make_env


class ComputeServiceBudgetTests(unittest.TestCase):
    def env(self, **changes):
        env = make_env(compute_service_mode="work_conserving", **changes)
        self.addCleanup(env.close)
        return env

    def ready(self, env, costs):
        tiles = add_task(env, count=len(costs))
        for tile, cost in zip(tiles, costs):
            tile.compute_cost = cost
            tile.state = TileState.READY
        return tiles

    def ground_ready(self, env, costs):
        tiles = add_task(env, count=len(costs))
        for tile, cost in zip(tiles, costs):
            tile.compute_cost = cost
            if env.pipeline_mode != "ground_compute":
                env.satellites[tile.location].queue.remove(tile.tile_id)
                env._release_tile_memory(tile, tile.location)
                tile.state = TileState.GROUND_QUEUED
                tile.timestamps.end_tx = env.time
                env.ground_tile_queues["gs"].append(tile.tile_id)
        if env.pipeline_mode == "ground_compute":
            task = env.tasks[tiles[0].parent_task_id]
            env.ground_task_queued[task.source_sat_id].remove(task.task_id)
            held = env.task_source_mem_gb.pop(task.task_id)
            env.satellites[task.source_sat_id].mem_used_gb -= held
            for tile in tiles:
                tile.state = TileState.GROUND_QUEUED
                tile.timestamps.end_tx = env.time
            env._ground_pipeline._enqueue_ground_task_compute(task.task_id, "gs")
        return tiles

    def test_default_and_config_round_trip(self):
        self.assertEqual(config_from_dict({}).compute_service_mode, "step")
        cfg = config_from_dict({"compute_service_mode": "work_conserving"})
        self.assertEqual(config_from_dict(asdict(cfg)).compute_service_mode, "work_conserving")
        with self.assertRaisesRegex(ValueError, "compute_service_mode"):
            config_from_dict({"compute_service_mode": "unbounded"})

    def test_short_jobs_share_budget_and_result_memory_without_early_downlink(self):
        env = self.env(result_buffering=True, result_size_mb=0.25)
        tiles = self.ready(env, [0.1] * 8)
        env._dist_pipeline.advance_compute()
        self.assertTrue(all(tile.state == TileState.COMPUTED for tile in tiles))
        self.assertEqual([tile.timestamps.end_compute for tile in tiles], [1] * 8)
        self.assertAlmostEqual(env.metrics.compute_busy_time[0], 0.8)
        self.assertAlmostEqual(env.satellites[0].vram_used_gb, 0)
        self.assertAlmostEqual(env.satellites[0].mem_used_gb, 8 * 0.25 / 1024)
        self.assertFalse(env.satellites[0].queue)
        env._dist_pipeline.start_downlinks({})
        self.assertFalse(env.downlink_transfers)
        self.assertEqual(env.metrics.completed_tasks, 0)

    def test_fifo_partial_job_carries_over_without_exceeding_budget(self):
        env = self.env()
        tiles = self.ready(env, [0.6, 0.6, 0.6])
        env._dist_pipeline.advance_compute()
        self.assertEqual([tile.state for tile in tiles],
                         [TileState.COMPUTED, TileState.RUNNING, TileState.READY])
        self.assertAlmostEqual(tiles[1].remaining_compute, 0.2)
        self.assertAlmostEqual(env.metrics.compute_busy_time[0], 1)
        env.time = 1
        env._dist_pipeline.advance_compute()
        self.assertEqual([tile.timestamps.end_compute for tile in tiles], [1, 2, 2])
        self.assertAlmostEqual(env.metrics.compute_busy_time[0], 1.8)
        self.assertTrue(all(tile.remaining_compute >= 0 for tile in tiles))
        self.assertAlmostEqual(env.satellites[0].vram_used_gb, 0)

    def test_input_finishing_this_step_cannot_use_its_compute_budget(self):
        for ground in (False, True):
            with self.subTest(ground=ground):
                env = self.env()
                tile = (self.ground_ready(env, [0.1]) if ground else self.ready(env, [0.1]))[0]
                tile.timestamps.end_tx = 1
                advance = (env._dist_pipeline.advance_ground_tile_compute if ground else
                           env._dist_pipeline.advance_compute)
                advance()
                self.assertIsNone(tile.timestamps.start_compute)
                env.time = 1
                advance()
                self.assertEqual(tile.timestamps.start_compute, 1)
                self.assertEqual(tile.timestamps.end_compute, 2)

    def test_whole_upload_completion_obeys_same_readiness_barrier(self):
        env = self.env(pipeline_mode="ground_compute", compute_cost_per_tile=0.1)
        tiles = add_task(env, count=1)
        tiles[0].compute_cost = 0.1
        env.vis_cache.refresh(0, [None] * 2, None, {}, env.ground_stations)
        env._ground_pipeline.start_ground_task_uploads()
        env.ground_task_transfers[0].remaining_mb = 0
        env._ground_pipeline.advance_ground_task_uploads()
        self.assertEqual(tiles[0].timestamps.end_tx, 1)
        env._ground_pipeline.advance_ground_task_compute()
        self.assertIsNone(tiles[0].timestamps.start_compute)
        self.assertEqual(env.ground_compute_busy_time, {})
        env.time = 1
        env._ground_pipeline.advance_ground_task_compute()
        self.assertEqual(tiles[0].timestamps.end_compute, 2)
        self.assertEqual(env.metrics.completed_tasks, 0)
        env._advance_task_delivery(1)
        self.assertEqual(env.metrics.completed_tasks, 0)
        env._advance_task_delivery(2)
        self.assertEqual(env.metrics.completed_tasks, 1)

    def test_whole_and_tiled_ground_queues_preserve_same_total_service(self):
        outcomes = []
        for mode in ("ground_compute", "ground_tiles"):
            env = self.env(pipeline_mode=mode, aggregation_delay_s=2)
            tiles = []
            for _ in range(4):
                tiles.extend(self.ground_ready(env, [0.2, 0.2]))
            advance = (env._ground_pipeline.advance_ground_task_compute if mode == "ground_compute"
                       else env._dist_pipeline.advance_ground_tile_compute)
            advance()
            self.assertAlmostEqual(env.ground_compute_busy_time["gs"], 1)
            self.assertEqual(env.metrics.completed_tasks, 0)
            env.time = 1
            advance()
            self.assertTrue(all(tile.state == TileState.DONE for tile in tiles))
            self.assertAlmostEqual(env.ground_compute_busy_time["gs"], 1.6)
            self.assertFalse(env.ground_task_running)
            self.assertFalse(env.ground_tile_running)
            env._advance_task_delivery(2)
            self.assertEqual(env.metrics.completed_tasks, 0)
            env._advance_task_delivery(4)
            outcomes.append([task.completion_time for task in env.tasks.values()])
            self.assertEqual(env.metrics.completed_tasks, 4)
        self.assertEqual(outcomes[0], outcomes[1])

    def test_vram_reject_and_wait_preserve_fifo_and_release(self):
        for policy in ("reject", "wait"):
            env = self.env(vram_policy=policy, vram_capacity_gb=0.5)
            tiles = self.ready(env, [0.1, 0.1])
            tiles[0].vram_req_gb = 1
            env._dist_pipeline.advance_compute()
            if policy == "reject":
                self.assertEqual(tiles[0].state, TileState.FAILED)
                self.assertEqual(tiles[1].state, TileState.COMPUTED)
                self.assertAlmostEqual(env.metrics.compute_busy_time[0], 0.1)
                self.assertAlmostEqual(env.satellites[0].mem_used_gb, 0)
            else:
                self.assertTrue(all(tile.state == TileState.READY for tile in tiles))
                self.assertEqual(env.metrics.compute_busy_time, {})
            self.assertAlmostEqual(env.satellites[0].vram_used_gb, 0)

    def test_failed_running_job_releases_slot_for_next_job(self):
        env = self.env()
        tiles = self.ready(env, [2, 0.1])
        env._dist_pipeline.advance_compute()
        env._fail_tile(tiles[0], FailureReason.DEADLINE_MISS)
        env.time = 1
        env._dist_pipeline.advance_compute()
        self.assertEqual(tiles[0].state, TileState.FAILED)
        self.assertEqual(tiles[1].state, TileState.COMPUTED)
        self.assertAlmostEqual(env.metrics.compute_busy_time[0], 1.1)
        self.assertAlmostEqual(env.satellites[0].vram_used_gb, 0)

    def test_failed_ground_jobs_are_not_resurrected_or_charged(self):
        for mode in ("ground_compute", "ground_tiles"):
            env = self.env(pipeline_mode=mode)
            failed = self.ground_ready(env, [0.2])[0]
            pending = self.ground_ready(env, [0.2])[0]
            env._fail_tile(failed, FailureReason.DEADLINE_MISS)
            if mode == "ground_compute":
                env._ground_pipeline.advance_ground_task_compute()
            else:
                env._dist_pipeline.advance_ground_tile_compute()
            self.assertEqual(failed.state, TileState.FAILED)
            self.assertEqual(pending.state, TileState.DONE)
            self.assertAlmostEqual(env.ground_compute_busy_time["gs"], 0.2)

    def test_exported_committed_service_uses_seconds_in_new_mode(self):
        env = self.env()
        self.ready(env, [0.2, 0.3])
        self.ground_ready(env, [0.2, 0.3])
        state = env.export_state()
        self.assertEqual(state.config["compute_service_mode"], "work_conserving")
        self.assertAlmostEqual(state.satellites[0]["committed_compute_s"], 0.5)
        self.assertAlmostEqual(state.ground_stations["gs"]["committed_compute_s"], 0.5)
        env.cfg.compute_service_mode = "step"
        state = env.export_state()
        self.assertEqual(state.satellites[0]["committed_compute_s"], 2)
        self.assertEqual(state.ground_stations["gs"]["committed_compute_s"], 2)

    def test_whole_ground_state_includes_queued_running_and_incoming_work(self):
        env = self.env(pipeline_mode="ground_compute")
        self.ground_ready(env, [0.6, 0.6])
        self.ground_ready(env, [0.2])
        incoming = add_task(env, count=1)[0]
        incoming.compute_cost = 0.3
        env.vis_cache.refresh(0, [None] * 2, None, {}, env.ground_stations)
        env._ground_pipeline.start_ground_task_uploads()
        env._ground_pipeline.advance_ground_task_compute()
        state = env.export_state()
        self.assertEqual(state.ground_stations["gs"]["running"], 1)
        self.assertEqual(state.ground_stations["gs"]["queue_len"], 1)
        self.assertAlmostEqual(state.ground_stations["gs"]["committed_compute_s"], 0.7)

    def test_continuously_backlogged_throughput_is_not_limited_to_one_job_per_step(self):
        records = []
        for dt in (1.0, 0.5, 0.25):
            env = self.env(dt=dt)
            tiles = self.ready(env, [0.07] * 30)
            for step in range(round(3 / dt)):
                env.time = step
                env._dist_pipeline.advance_compute()
            self.assertTrue(all(tile.state == TileState.COMPUTED for tile in tiles))
            records.append(env.metrics.compute_busy_time[0])
            self.assertLessEqual(tiles[-1].timestamps.end_compute * dt, 2.1 + dt)
        for busy in records:
            self.assertAlmostEqual(busy, 2.1)

    def test_legacy_ground_modes_keep_one_job_per_step(self):
        for mode in ("ground_compute", "ground_tiles"):
            env = make_env(pipeline_mode=mode, compute_cost_per_tile=0.1)
            self.addCleanup(env.close)
            tiles = [self.ground_ready(env, [0.1])[0] for _ in range(2)]
            if mode == "ground_compute":
                env._ground_pipeline.advance_ground_task_compute()
            else:
                env._dist_pipeline.advance_ground_tile_compute()
            self.assertEqual([tile.state for tile in tiles], [TileState.DONE, TileState.GROUND_QUEUED])
            self.assertEqual(env.ground_compute_busy_time, {})

    def test_sar_ground_multipliers_preserve_service_in_both_upload_modes(self):
        for multiplier in (1, 5, 10, 20, 100):
            for mode in ("ground_compute", "ground_tiles"):
                with self.subTest(multiplier=multiplier, mode=mode):
                    env = self.env(pipeline_mode=mode, dt=0.1, ground_stations=[{
                        "id": "gs", "bandwidth_mbps": 1200, "compute_rate": multiplier / 0.574}])
                    tiles = self.ground_ready(env, [1] * 64)
                    advance = (env._ground_pipeline.advance_ground_task_compute
                               if mode == "ground_compute" else
                               env._dist_pipeline.advance_ground_tile_compute)
                    service_s = 64 * 0.574 / multiplier
                    for step in range(math.ceil(service_s / env.cfg.dt)):
                        env.time = step
                        advance()
                        if multiplier == 100 and mode == "ground_tiles" and step == 0:
                            self.assertGreater(sum(tile.state == TileState.DONE for tile in tiles), 1)
                    self.assertTrue(all(tile.state == TileState.DONE for tile in tiles))
                    self.assertAlmostEqual(env.ground_compute_busy_time["gs"], service_s)
                    end_s = max(tile.timestamps.end_compute for tile in tiles) * env.cfg.dt
                    self.assertGreaterEqual(end_s + 1e-9, service_s)
                    self.assertLess(end_s - service_s, env.cfg.dt + 1e-9)

    def test_legacy_step_keeps_one_short_job_per_step(self):
        env = make_env()
        self.addCleanup(env.close)
        tiles = self.ready(env, [0.1, 0.1])
        env._dist_pipeline.advance_compute()
        self.assertEqual([tile.state for tile in tiles], [TileState.COMPUTED, TileState.READY])
        self.assertAlmostEqual(env.metrics.compute_busy_time[0], 0.1)


if __name__ == "__main__":
    unittest.main()
