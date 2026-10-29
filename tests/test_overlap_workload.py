from dataclasses import asdict, replace
import math
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import numpy as np

from sim.config import config_from_dict
from sim.entities import Action, ActionType, TileState
from sim.marl.scenarios import ScenarioDistribution
from sim.workload import ImageTiling, window_starts
from test_simulation import make_env


def image_tiling(**changes):
    return asdict(replace(ImageTiling(4000, 4000, 3, 8, 1024, 824, 2, 1.0, 0.04), **changes))


def release_one(env):
    env.cfg.task_arrival_rate = 1.0
    with patch.object(env, "rng", SimpleNamespace(poisson=lambda _: 1)):
        env._task_arrivals(env.time)
    env.cfg.task_arrival_rate = 0.0
    return list(env.tiles.values())


class OverlapGeometryTests(unittest.TestCase):
    def test_published_candidate_geometry_and_bytes(self):
        workload = ImageTiling.from_dict(image_tiling())
        self.assertEqual(window_starts(4000, 1024, 824), (0, 824, 1648, 2472, 2976))
        manifest = workload.manifest()
        self.assertEqual(manifest["num_windows"], 25)
        self.assertEqual(manifest["num_tiles"], 9)
        self.assertEqual([len(tile.windows_xyxy) for tile in workload.tiles()], [4, 4, 2, 4, 4, 2, 2, 2, 1])
        self.assertEqual(manifest["image_size_mb"], 48)
        self.assertAlmostEqual(manifest["packet_input_mb"], 78.6432)
        self.assertAlmostEqual(manifest["coverage_multiplier"], 1.6384)
        self.assertAlmostEqual(manifest["unmerged_result_mb"], 1.0)

    def test_high_overlap_count_includes_edge_shift(self):
        manifest = ImageTiling.from_dict(image_tiling(stride_px=512)).manifest()
        self.assertEqual(manifest["num_windows"], 49)
        self.assertEqual(manifest["num_tiles"], 16)
        self.assertAlmostEqual(manifest["packet_input_mb"], 154.140672)

    def test_packaging_changes_no_windows_or_work(self):
        expected = None
        for group in (1, 2, 4, 9):
            workload = ImageTiling.from_dict(image_tiling(windows_per_tile_axis=group))
            tiles = workload.tiles()
            windows = [box for tile in tiles for box in tile.windows_xyxy]
            self.assertEqual(len(windows), len(set(windows)))
            values = (set(windows), math.fsum(t.data_size_mb for t in tiles),
                      math.fsum(t.compute_cost for t in tiles), math.fsum(t.result_size_mb for t in tiles))
            if expected is not None:
                self.assertEqual(values, expected)
            expected = values

    def test_rectangular_and_single_window_images_cover_every_pixel(self):
        for width, height, window, stride in ((31, 21, 8, 5), (8, 8, 8, 5), (16, 8, 8, 8)):
            workload = ImageTiling(width, height, 3, 8, window, stride, 2, 1, 0)
            coverage = np.zeros((height, width), dtype=int)
            for tile in workload.tiles():
                for x0, y0, x1, y1 in tile.windows_xyxy:
                    self.assertLessEqual(x1, width)
                    self.assertLessEqual(y1, height)
                    coverage[y0:y1, x0:x1] += 1
            self.assertGreaterEqual(coverage.min(), 1)

    def test_invalid_and_unsupported_inputs_fail_explicitly(self):
        for changes in ({"width_px": 100}, {"stride_px": 1025}, {"stride_px": 0},
                        {"bands": True}, {"bits_per_sample": 12}, {"windows_per_tile_axis": 0},
                        {"compute_cost_per_window": 0}, {"compute_cost_per_window": float("nan")},
                        {"result_size_mb_per_window": -1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                ImageTiling.from_dict({**image_tiling(), **changes})

    def test_derived_config_round_trip_and_conflict_detection(self):
        cfg = config_from_dict({"image_tiling": image_tiling()})
        self.assertEqual(cfg.image_size_mb, 48)
        self.assertEqual(cfg.num_tiles, 9)
        self.assertEqual(asdict(config_from_dict(asdict(cfg))), asdict(cfg))
        for name, value in (("image_size_mb", 128), ("num_tiles", 8),
                            ("compute_cost_per_tile", 60), ("result_size_mb", 0.5)):
            with self.subTest(name=name), self.assertRaises(ValueError):
                config_from_dict({"image_tiling": image_tiling(), name: value})

    def test_old_scenario_sampler_cannot_silently_override_workload(self):
        cfg = config_from_dict({"image_tiling": image_tiling()})
        with self.assertRaisesRegex(ValueError, "workload-aware"):
            ScenarioDistribution("examples/scenario_roles.json", cfg)


class OverlapExecutionTests(unittest.TestCase):
    def test_generated_tiles_and_memory_use_exact_packet_sizes(self):
        env = make_env(image_tiling=image_tiling())
        self.addCleanup(env.close)
        tiles = release_one(env)
        self.assertEqual(len(tiles), 9)
        source = next(iter(env.tasks.values())).source_sat_id
        self.assertAlmostEqual(env.satellites[source].mem_used_gb, 78.6432 / 1000)
        state = env.export_state()
        self.assertTrue(state.config["image_tiling"])
        self.assertEqual(state.config["memory_mb_per_gb"], 1000)
        self.assertEqual([t.compute_cost for t in tiles], [4, 4, 2, 4, 4, 2, 2, 2, 1])
        self.assertEqual([state.tiles[t.tile_id]["result_size_mb"] for t in tiles],
                         [t.result_size_mb for t in tiles])
        self.assertEqual(len({t.vram_req_gb for t in tiles}), 1)
        self.assertEqual(env.summary()["experiment"]["workload"]["num_windows"], 25)

    def test_arrival_trace_is_invariant_to_overlap_and_packaging(self):
        traces = []
        for stride, group in ((824, 1), (824, 4), (512, 2)):
            env = make_env(image_tiling=image_tiling(stride_px=stride, windows_per_tile_axis=group),
                           task_arrival_rate=0.3, mem_capacity_gb=1000)
            self.addCleanup(env.close)
            for t in range(100):
                env._task_arrivals(t)
            traces.append([(t.release_time, t.source_sat_id) for t in env.tasks.values()])
        self.assertGreater(len(traces[0]), 0)
        self.assertEqual(traces[0], traces[1])
        self.assertEqual(traces[0], traces[2])

    def test_whole_ground_uploads_raw_once_but_computes_all_windows(self):
        env = make_env(pipeline_mode="ground_compute", image_tiling=image_tiling())
        self.addCleanup(env.close)
        tiles = release_one(env)
        task = next(iter(env.tasks.values()))
        env.vis_cache.refresh(0, [None] * 2, None, {}, env.ground_stations)
        env._ground_pipeline.start_ground_task_uploads()
        self.assertEqual(env.ground_task_transfers[0].remaining_mb, 48)
        self.assertAlmostEqual(env.task_source_mem_gb[task.task_id], 0.048)
        env.ground_task_transfers[0].remaining_mb = 0
        env._ground_pipeline.advance_ground_task_uploads()
        env._ground_pipeline.advance_ground_task_compute()
        self.assertEqual(env.ground_task_running["gs"].remaining_compute, sum(t.compute_cost for t in tiles) - env.cfg.dt)

    def test_result_memory_downlink_and_delivery_respect_per_tile_bytes(self):
        workload = image_tiling(width_px=10, height_px=10, window_px=4, stride_px=3,
                                result_size_mb_per_window=2.0)
        env = make_env(image_tiling=workload, result_buffering=True, task_source_sat_ids=[0],
                       aggregation_delay_s=3, sim_steps=200)
        self.addCleanup(env.close)
        tiles = release_one(env)
        expected_mb = math.fsum(t.result_size_mb for t in tiles)
        self.assertEqual(expected_mb, 18)
        for _ in range(150):
            env.step([Action(t.tile_id, ActionType.LOCAL) for t in tiles if t.state == TileState.QUEUED])
            if env.metrics.completed_tasks:
                break
        self.assertEqual(env.metrics.completed_tasks, 1)
        self.assertAlmostEqual(env.metrics.network_sent_mb["ground:gs"], expected_mb)
        self.assertAlmostEqual(env.satellites[0].mem_used_gb, 0)
        task = next(iter(env.tasks.values()))
        self.assertEqual(task.completion_time - task.results_ready_time, 3)
        self.assertTrue(all(t.state == TileState.DONE for t in tiles))

    def test_raw_ground_packets_preserve_overlap_bytes(self):
        for mode in ("ground_tiles", "ground_relay"):
            with self.subTest(mode=mode):
                env = make_env(pipeline_mode=mode, image_tiling=image_tiling(),
                               task_source_sat_ids=[0], sim_steps=300)
                self.addCleanup(env.close)
                env.vis_cache.windows_at = lambda sid, t: {"gs": (0, 1000)}
                release_one(env)
                for _ in range(300):
                    env.step([])
                    if env.metrics.completed_tasks:
                        break
                self.assertEqual(env.metrics.completed_tasks, 1)
                self.assertAlmostEqual(env.metrics.network_sent_mb["ground:gs"], 78.6432)
                self.assertAlmostEqual(env.satellites[0].mem_used_gb, 0)


if __name__ == "__main__":
    unittest.main()
