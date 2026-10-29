import json
import math
import unittest

from sim.config import load_config
from sim.entities import FailureReason, Link, TileState
from sim.workload import ImageTiling
from test_result_relay import RelayVisibility
from test_simulation import make_env


# 1200² uint8 原图 1.44 MB；4 个 800² 重叠窗共 2.56 MB；2×2 唯一块各 0.36 MB。
TILING = {"width_px": 1200, "height_px": 1200, "bands": 1, "bits_per_sample": 8,
          "window_px": 800, "stride_px": 400, "windows_per_tile_axis": 1,
          "compute_cost_per_window": 1.0, "result_size_mb_per_window": 0.0}
BLOCK_MB = 0.36
BLOCK_GB = BLOCK_MB / 1000


def unique_env(mode="ground_unique_relay", **changes):
    options = dict(num_sats=3, pipeline_mode=mode, image_tiling=TILING, dt=1.0,
                   compute_service_mode="work_conserving", result_buffering=True, result_relay=True,
                   task_arrival_rate=0.0, scheduled_task_arrivals=[{"release_step": 0, "source_sat_id": 0}],
                   ground_stations=[{"id": "gs", "bandwidth_mbps": 2 * BLOCK_MB * 8, "compute_rate": 4}])
    options.update(changes)
    env = make_env(**options)
    # 星间每步一个唯一块；一个重叠窗约需两步。
    env.topology.links = {"0-1": Link(0, 1, True, BLOCK_MB * 8, 0),
                          "1-2": Link(1, 2, True, BLOCK_MB * 8, 0)}
    env.vis_cache = RelayVisibility({2: (0, 1000)})
    return env


class GroundUniqueRelayTests(unittest.TestCase):
    def run_env(self, env, steps, check=None):
        self.addCleanup(env.close)
        for _ in range(steps):
            env.step([])
            if check:
                check(env)
        return env

    def check_memory(self, env):
        for sid, sat in env.satellites.items():
            held = sum(env.memory_size_gb(t.data_size_mb) for t in env.tiles.values() if sid in t.memory_holders)
            self.assertAlmostEqual(sat.mem_used_gb, held)
            self.assertLessEqual(sat.mem_used_gb, sat.mem_capacity_gb + 1e-12)

    def check_no_early_compute(self, env):
        for task in env.tasks.values():
            tiles = [env.tiles[tid] for tid in task.tile_ids]
            started = [t for t in tiles if t.timestamps.start_compute is not None]
            if started:
                self.assertTrue(all(t.timestamps.end_tx is not None for t in tiles))
                last_arrival = max(t.timestamps.end_tx for t in tiles)
                self.assertTrue(all(t.timestamps.start_compute >= last_arrival for t in started))

    def test_blocks_partition_image_bytes_and_reject_invalid_counts(self):
        workload = ImageTiling.from_dict(TILING)
        blocks = workload.unique_transport_blocks(4)
        self.assertEqual([box for box, _ in blocks],
                         [(0, 0, 600, 600), (600, 0, 1200, 600), (0, 600, 600, 1200), (600, 600, 1200, 1200)])
        self.assertTrue(all(math.isclose(size, BLOCK_MB) for _, size in blocks))
        self.assertAlmostEqual(math.fsum(size for _, size in blocks), workload.image_size_mb)
        self.assertAlmostEqual(math.fsum(t.data_size_mb for t in workload.tiles()), 2.56)
        for count in (0, 3, 8, -4):
            with self.assertRaises(ValueError):
                workload.unique_transport_blocks(count)
        # 降采样组件在奇数边界切块会重复像元，不能称为唯一像素。
        odd = ImageTiling.from_dict({**TILING, "width_px": 14, "height_px": 12, "window_px": 8, "stride_px": 4,
                                     "transport_components": [
                                         {"name": "ms", "downsample": 2, "bands": 1, "bits_per_sample": 8}]})
        with self.assertRaises(ValueError):
            odd.unique_transport_blocks(4)

    def test_frozen_sar_config_uses_800_mb_in_64_blocks_of_12_5_mb(self):
        with open("results/sar_ground_capacity_frozen_20261015/main_config.json", encoding="utf-8") as handle:
            cfg = json.load(handle)
        workload = ImageTiling.from_dict(cfg["image_tiling"])
        blocks = workload.unique_transport_blocks(len(workload.tiles()))
        self.assertEqual(len(blocks), 64)
        self.assertTrue(all(math.isclose(size, 12.5) for _, size in blocks))
        self.assertTrue(all(x1 - x0 == 2500 and y1 - y0 == 2500 for (x0, y0, x1, y1), _ in blocks))
        self.assertAlmostEqual(math.fsum(size for _, size in blocks), 800.0)
        self.assertAlmostEqual(math.fsum(t.data_size_mb for t in workload.tiles()), 2147.483648)
        self.assertEqual(load_config("results/sar_ground_capacity_frozen_20261015/main_config.json").num_tiles, 64)

    def test_requires_image_tiling(self):
        with self.assertRaises(ValueError):
            make_env(pipeline_mode="ground_unique_relay")

    def test_relays_unique_bytes_and_computes_only_after_all_blocks_land(self):
        env = self.run_env(unique_env(), 30, lambda e: (self.check_memory(e), self.check_no_early_compute(e)))
        tiles = [env.tiles[tid] for tid in env.tasks["task_0"].tile_ids]
        self.assertTrue(all(t.state == TileState.DONE for t in tiles))
        self.assertTrue(all(t.raw_relay_path == [0, 1, 2] for t in tiles))
        # 每块两跳 ISL 加一次下行，网络计数只含唯一字节，没有星上计算。
        sent = env.metrics.network_sent_mb
        for key in ("isl:0->1", "isl:1->2", "ground:gs"):
            self.assertAlmostEqual(sent[key], 1.44)
        self.assertFalse(env.metrics.compute_busy_time)
        self.assertAlmostEqual(env.ground_compute_busy_time["gs"], 4 / 4)
        # 整图一次进入地面 FIFO：首窗开始计算不早于最后一块落地。
        self.assertEqual(min(t.timestamps.start_compute for t in tiles), max(t.timestamps.end_tx for t in tiles))
        self.assertEqual(env.metrics.completed_tasks, 1)
        self.assertAlmostEqual(env.summary()["experiment"]["unique_transport"]["image_mb"], 1.44)

    def test_unique_relay_is_faster_than_overlapping_window_relay(self):
        unique = self.run_env(unique_env(), 40)
        overlap = self.run_env(unique_env("ground_relay"), 40)
        self.assertEqual(unique.metrics.completed_tasks, overlap.metrics.completed_tasks)
        self.assertAlmostEqual(overlap.metrics.network_sent_mb["ground:gs"], 2.56)
        self.assertLess(unique.metrics.task_latencies[0], overlap.metrics.task_latencies[0])

    def test_cache_limit_waits_and_link_drop_pauses_without_loss(self):
        env = unique_env()
        self.addCleanup(env.close)
        for sid in (1, 2):
            env.satellites[sid].mem_capacity_gb = 1.5 * BLOCK_GB
        env.step([])
        env.topology.links["0-1"] = Link(0, 1, False, BLOCK_MB * 8, 0)
        for _ in range(5):
            env.step([])
            self.check_memory(env)
        self.assertFalse(any(t.state == TileState.FAILED for t in env.tiles.values()))
        self.assertFalse(any(t.timestamps.start_compute is not None for t in env.tiles.values()))
        env.topology.links["0-1"] = Link(0, 1, True, BLOCK_MB * 8, 0)
        for _ in range(40):
            env.step([])
            self.check_memory(env)
            self.check_no_early_compute(env)
        self.assertTrue(all(t.state == TileState.DONE for t in env.tiles.values()))
        self.assertLessEqual(env.metrics.mem_peak[1], BLOCK_GB + 1e-12)
        self.assertEqual(env.metrics.completed_tasks, 1)

    def test_two_images_share_one_receiver_and_never_mix_assembly(self):
        env = unique_env(scheduled_task_arrivals=[{"release_step": 0, "source_sat_id": 0},
                                                  {"release_step": 0, "source_sat_id": 1}])
        self.run_env(env, 60, lambda e: (self.check_memory(e), self.check_no_early_compute(e)))
        self.assertEqual(env.metrics.completed_tasks, 2)
        self.assertAlmostEqual(env.metrics.network_sent_mb["ground:gs"], 2 * 1.44)
        # 一个接收通道每步 0.72 MB，两图 2.88 MB 至少占 4 步下行。
        downlink_capacity = env.metrics.network_capacity_mb["ground:gs"] / env.metrics.queue_len_steps
        self.assertAlmostEqual(downlink_capacity, 2 * BLOCK_MB)
        self.assertFalse(env._dist_pipeline._unique_landed)

    def test_deadline_failure_releases_partial_assembly(self):
        env = unique_env(deadline_steps=2)
        self.run_env(env, 10, self.check_memory)
        self.assertTrue(any(t.failure_reason == FailureReason.DEADLINE_MISS for t in env.tiles.values()))
        self.assertFalse(env.ground_tile_queues["gs"])
        self.assertFalse(env.ground_tile_running)
        self.assertEqual(env.metrics.completed_tasks, 0)
        self.assertTrue(all(sat.mem_used_gb == 0 for sat in env.satellites.values()))


if __name__ == "__main__":
    unittest.main()
