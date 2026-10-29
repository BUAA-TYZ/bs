from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import unittest

from analysis.prepare_sar_dispatch import derive
from analysis.run_sar_frozen import invariants
from sim.config import config_from_dict
from sim.entities import Action, ActionType, FailureReason, TileState
from sim.workload import ImageTiling
from test_compute_cohorts import PROFILE
from test_simulation import add_task, make_env


class DispatchPacketTests(unittest.TestCase):
    def env(self, **kwargs):
        values = dict(compute_service_mode="work_conserving", onboard_compute_profile=deepcopy(PROFILE),
                      compute_rate=1 / 0.6, dt=0.5, vram_base_gb=1, vram_alpha_per_mb=0,
                      vram_capacity_gb=2, result_buffering=True)
        values.update(kwargs)
        env = make_env(**values)
        self.addCleanup(env.close)
        return env

    def test_internal_windows_use_measured_cohorts_and_keep_input_until_done(self):
        env = self.env()
        tile = add_task(env)[0]
        tile.compute_cost, tile.state = 4, TileState.READY
        tile.actual_result_size_mb = 4 * 28 / 1e6
        input_memory = env.satellites[0].mem_used_gb
        env._dist_pipeline.advance_compute()
        self.assertEqual(env.satellites[0].executing_batch, [tile.tile_id] * 2)
        self.assertAlmostEqual(env.export_state().satellites[0]["committed_compute_s"], 1.5)
        env.time = 1
        env._dist_pipeline.advance_compute()
        self.assertEqual(tile.completed_compute_windows, 2)
        self.assertEqual(tile.state, TileState.READY)
        self.assertEqual(env.satellites[0].mem_used_gb, input_memory)
        self.assertIsNone(tile.timestamps.end_compute)
        self.assertNotIn(tile.tile_id, env.export_state().tiles)
        for step in (2, 3):
            env.time = step
            env._dist_pipeline.advance_compute()
        self.assertEqual(tile.state, TileState.COMPUTED)
        self.assertEqual(tile.completed_compute_windows, 4)
        self.assertEqual(tile.timestamps.start_compute, 0)
        self.assertEqual(tile.timestamps.end_compute, 4)
        self.assertAlmostEqual(env.metrics.compute_busy_time[0], 2)
        self.assertAlmostEqual(env.satellites[0].mem_used_gb, 112 / 1e6 / 1024)
        self.assertEqual(len(env.compute_cohort_records), 2)
        self.assertTrue(invariants(env)["passed"])

    def test_odd_packet_tail_can_share_cohort_with_next_packet(self):
        env = self.env(dt=2)
        a, b = add_task(env, count=2)
        a.compute_cost, b.compute_cost = 3, 1
        a.state = b.state = TileState.READY
        env._dist_pipeline.advance_compute()
        self.assertEqual((a.completed_compute_windows, b.completed_compute_windows), (3, 1))
        self.assertEqual(a.state, TileState.COMPUTED)
        self.assertEqual(b.state, TileState.COMPUTED)
        self.assertEqual(env.metrics.compute_busy_time[0], 2)
        self.assertTrue(invariants(env)["passed"])

    def test_cancel_packet_removes_all_its_running_windows(self):
        env = self.env()
        tile = add_task(env)[0]
        tile.compute_cost, tile.state = 4, TileState.READY
        env._dist_pipeline.advance_compute()
        env._fail_tile(tile, FailureReason.DEADLINE_MISS)
        self.assertEqual(env.satellites[0].executing_batch, [])
        self.assertEqual(env.satellites[0].vram_used_gb, 0)
        self.assertEqual(env.satellites[0].mem_used_gb, 0)
        self.assertTrue(invariants(env)["passed"])

    def test_empty_truth_is_hidden_until_compute_and_still_must_be_delivered(self):
        env = self.env(result_size_mb=0.5)
        tile = add_task(env)[0]
        tile.actual_result_size_mb = 28 / 1e6
        self.assertEqual(env.export_state().tiles[tile.tile_id]["result_size_mb"], 0.5)
        env.step([Action(tile.tile_id, ActionType.LOCAL)])
        self.assertEqual(env.metrics.completed_tasks, 0)
        env.step([])
        self.assertEqual(tile.state, TileState.COMPUTED)
        self.assertEqual(env.tile_result_size_mb(tile), 28 / 1e6)
        self.assertEqual(env.metrics.completed_tasks, 0)
        for _ in range(4):
            env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.assertEqual(env.metrics.completed_tasks, 1)
        self.assertAlmostEqual(sum(v for k, v in env.metrics.network_sent_mb.items()
                                   if k.startswith("ground:")), 28 / 1e6)
        self.assertTrue(invariants(env)["passed"])

    def test_group_sizes_preserve_workload_and_exact_native_pixel_union(self):
        base = json.loads(Path("results/sar_parameter_contract_20261016/main_config.json").read_text())
        traces = []
        for group, count, total in ((1, 64, 2147.483648), (2, 16, 1287.882752), (4, 4, 949.782528)):
            cfg, truth = derive(base, group)
            workload = ImageTiling.from_dict(cfg["image_tiling"])
            tiles = workload.tiles()
            self.assertEqual(len(tiles), count)
            self.assertEqual(sum(t.compute_cost for t in tiles), 64)
            # 用逐行区间合并独立核对包内原生像素并集。
            exact = 0
            for tile in tiles:
                ys = sorted({v for b in tile.windows_xyxy for v in (b[1], b[3])})
                for lo, hi in zip(ys, ys[1:]):
                    intervals = sorted((b[0], b[2]) for b in tile.windows_xyxy if b[1] <= lo < b[3])
                    start, end = intervals[0]
                    width = 0
                    for left, right in intervals[1:]:
                        if left > end:
                            width += end - start
                            start = left
                        end = max(end, right)
                    exact += (width + end - start) * (hi - lo) * 2
            self.assertAlmostEqual(sum(t.data_size_mb for t in tiles), exact / 1e6)
            self.assertAlmostEqual(sum(t.data_size_mb for t in tiles), total)
            traces.append(truth)
        self.assertEqual(traces[0], traces[1])
        self.assertEqual(traces[1], traces[2])

    def test_output_map_uses_global_window_coordinates_not_group_order(self):
        tiling = dict(width_px=8, height_px=8, bands=1, bits_per_sample=16,
                      window_px=4, stride_px=2, windows_per_tile_axis=2,
                      compute_cost_per_window=1, result_size_mb_per_window=0.2,
                      deduplicate_within_tile=True)
        outputs = list(range(28, 37))
        env = self.env(image_tiling=tiling, scheduled_task_arrivals=[
            dict(release_step=0, source_sat_id=0, window_result_bytes=outputs)])
        env._task_arrivals(0)
        actual = [tile.actual_result_size_mb * 1e6 for tile in env.tiles.values()]
        self.assertEqual(actual, [28+29+31+32, 30+33, 34+35, 36])
        cfg = asdict(env.cfg)
        cfg["scheduled_task_arrivals"][0]["window_result_bytes"] = [0] * 9
        with self.assertRaisesRegex(ValueError, "positive byte count"):
            config_from_dict(cfg)


if __name__ == "__main__":
    unittest.main()
