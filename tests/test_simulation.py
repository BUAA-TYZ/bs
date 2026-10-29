import json
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from sim.cache.visibility import VisibilityCache
from sim.config import load_config
from sim.entities import (Action, ActionType, DownlinkTransfer, FailureReason,
                          GroundTileTransfer, Link, Task, Tile, TileState, TileTimestamps)
from sim.env import SimulationEnv
from sim.metrics import Metrics
from sim.network import advance_ground_transfers, fair_bytes
from sim.topology import TopologyModel
from sim.scheduler.random_stub import RandomPolicy


class FakeTopology:
    def __init__(self, cfg):
        self.cfg = cfg
        self.sat_recs = [None] * cfg.num_sats
        self.t0 = None
        self.links = {"0-1": Link(0, 1, True, 8.0, 0.0)}

    def snapshot(self, t, dt=1.0):
        return self.links

    def close(self):
        pass


class FakeVisibility:
    def __init__(self, **kwargs):
        self.lookahead_s = 60.0
        self.visible = True
        self.visible_gs_by_sat = {}
        self.best_gs_by_sat = {}

    def refresh(self, t, recs, t0, station_objs, stations):
        self.visible_gs_by_sat = {sid: list(stations) if self.visible else [] for sid in range(len(recs))}
        self.best_gs_by_sat = {sid: next(iter(stations)) if self.visible else None for sid in range(len(recs))}

    def windows_at(self, sat_id, t):
        return {}

    def is_visible(self, sat_id, gs_id, ground_stations):
        return self.visible


def make_env(**changes):
    cfg = replace(load_config("examples/config.yaml"), num_sats=2, dt=1.0,
                  num_tiles=2, image_size_mb=2.0, task_arrival_rate=0.0,
                  compute_rate=1.0, compute_cost_per_tile=1.0, result_size_mb=1.0,
                  ground_stations=[{"id": "gs", "bandwidth_mbps": 8, "compute_rate": 1}],
                  tile_lifecycle_log="")
    cfg = replace(cfg, **changes)
    with patch("sim.env.TopologyModel", FakeTopology), patch("sim.env.VisibilityCache", FakeVisibility):
        return SimulationEnv(cfg)


def add_task(env, src=0, count=1, size=1.0, deadline=None):
    task_id = f"task_{len(env.tasks)}"
    task = Task(task_id, src, env.time, count * size, count, deadline)
    env.tasks[task_id] = task
    env.metrics.total_tasks += 1
    tiles = []
    for i in range(count):
        tile = Tile(f"{task_id}_{i}", task_id, size, 1.0, 0.1,
                    TileState.CREATED, src, TileTimestamps(env.time), deadline)
        env.tiles[tile.tile_id] = tile
        task.tile_ids.append(tile.tile_id)
        tiles.append(tile)
        env.metrics.total_tiles += 1
    env._task_tile_index[task_id] = task.tile_ids
    if env.pipeline_mode in {"distributed", "ground_relay", "ground_tiles", "ground_unique_relay"}:
        for tile in tiles:
            env._enqueue_tile(src, tile)
    else:
        env._ground_pipeline.enqueue_ground_task(task, tiles)
    return tiles


class SimulationTests(unittest.TestCase):
    def test_short_flows_redistribute_spare_capacity(self):
        self.assertEqual(fair_bytes([1, 100, 100], 9), [1, 4, 4])

    def test_isl_capacity_is_shared_and_full_duplex(self):
        env = make_env()
        tiles = add_task(env, count=2, size=2)
        reverse = add_task(env, src=1, size=2)[0]
        actions = [Action(t.tile_id, ActionType.OFFLOAD, target_sat_id=1) for t in tiles]
        actions.append(Action(reverse.tile_id, ActionType.OFFLOAD, target_sat_id=0))
        env.step(actions)
        self.assertEqual([tr.remaining_mb for tr in env.transfers], [1.5, 1.5, 1.0])
        self.assertEqual(env.metrics.network_sent_mb["isl:0->1"], 1.0)
        self.assertEqual(env.metrics.network_sent_mb["isl:1->0"], 1.0)

    def test_ground_uploads_and_results_share_one_channel(self):
        env = make_env()
        flows = [GroundTileTransfer("a", 0, "gs", 2, 0),
                 DownlinkTransfer("b", 0, "gs", 2, 0),
                 GroundTileTransfer("c", 1, "gs", 2, 0)]
        advance_ground_transfers(env, flows)
        self.assertEqual([f.remaining_mb for f in flows], [1.5, 1.5, 2])
        env.time += 1
        advance_ground_transfers(env, flows)
        self.assertEqual([f.remaining_mb for f in flows], [1.5, 1.5, 1])
        self.assertEqual(env.metrics.network_sent_mb["ground:gs"], 2.0)

    def test_ground_two_channels_and_single_sat_radio(self):
        env = make_env(ground_stations=[{"id": "a", "bandwidth_mbps": 8, "receive_channels": 2},
                                       {"id": "b", "bandwidth_mbps": 8}])
        flows = [GroundTileTransfer("a", 0, "a", 4, 0),
                 GroundTileTransfer("b", 1, "a", 4, 0),
                 GroundTileTransfer("c", 0, "b", 4, 0)]
        advance_ground_transfers(env, flows)
        self.assertEqual([f.remaining_mb for f in flows], [3, 3, 4])

    def test_pause_resume_preserves_progress(self):
        env = make_env()
        tile = add_task(env, size=3)[0]
        env.step([Action(tile.tile_id, ActionType.OFFLOAD, target_sat_id=1)])
        env.topology.links["0-1"].up = False
        env.step([])
        self.assertEqual(env.transfers[0].remaining_mb, 2)
        env.topology.links["0-1"].up = True
        env.step([])
        self.assertEqual(env.transfers[0].remaining_mb, 1)

    def test_failed_reservation_does_not_release_other_memory(self):
        env = make_env(mem_capacity_gb=1.0)
        first = add_task(env, size=800)[0]
        second = add_task(env, size=800)[0]
        self.assertEqual(second.state, TileState.FAILED)
        self.assertAlmostEqual(env.satellites[0].mem_used_gb, first.data_size_mb / 1024)
        env._fail_tile(second, FailureReason.MEM_FULL)
        self.assertEqual(env.metrics.failed_tiles, 1)

    def test_link_failure_releases_each_reservation_once(self):
        env = make_env(transfer_fail_on_link_down=True)
        resident = add_task(env, src=1, size=10)[0]
        moved = add_task(env, size=10)[0]
        env.step([Action(moved.tile_id, ActionType.OFFLOAD, target_sat_id=1)])
        env.topology.links["0-1"].up = False
        env.step([])
        self.assertEqual(moved.state, TileState.FAILED)
        self.assertAlmostEqual(env.satellites[1].mem_used_gb, resident.data_size_mb / 1024)
        self.assertEqual(env.satellites[0].mem_used_gb, 0)

    def test_local_compute_then_downlink_uses_distinct_steps(self):
        env = make_env(dt=5.0)
        tile = add_task(env)[0]
        env.step([Action(tile.tile_id, ActionType.LOCAL)])
        self.assertEqual(tile.state, TileState.COMPUTED)
        self.assertEqual(env.metrics.completed_tasks, 0)
        env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.assertEqual(env.metrics.summary()["latency"]["task_mean"], 10.0)
        env._check_task_done(tile.parent_task_id)
        self.assertEqual(env.metrics.completed_tasks, 1)
        self.assertEqual(env.metrics.compute_busy_time[0], 1.0)

    def test_raw_tile_upload_completes_before_ground_compute(self):
        env = make_env()
        tile = add_task(env)[0]
        env.step([Action(tile.tile_id, ActionType.OFFLOAD, target_gs_id="gs")])
        self.assertEqual(tile.state, TileState.GROUND_QUEUED)
        env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.assertEqual(env.metrics.summary()["latency"]["tile_mean"], 2)

    def test_ground_task_deadline_cancels_transfer_and_memory(self):
        env = make_env(pipeline_mode="ground_compute", deadline_steps=1)
        tiles = add_task(env, count=2, size=10, deadline=1)
        env.step([])
        self.assertTrue(all(t.state == TileState.FAILED for t in tiles))
        env.step([])
        self.assertTrue(all(t.state == TileState.FAILED for t in tiles))
        self.assertFalse(env.ground_task_transfers)
        self.assertFalse(env.task_source_mem_gb)
        self.assertEqual(env.satellites[0].mem_used_gb, 0)
        for _ in range(30):
            env.step([])
        self.assertEqual(env.metrics.completed_tasks, 0)

    def test_whole_ground_task_finishes_once(self):
        env = make_env(pipeline_mode="ground_compute")
        add_task(env, count=2, size=1)
        for _ in range(4):
            env.step([])
        self.assertEqual(env.metrics.completed_tasks, 1)
        self.assertEqual(env.metrics.completed_tiles, 2)
        self.assertEqual(env.metrics.summary()["latency"]["task_mean"], 4)
        self.assertEqual(env.satellites[0].mem_used_gb, 0)

    def test_resource_and_terminal_counts_under_contention(self):
        for mode in ("distributed", "ground_compute"):
            with self.subTest(mode=mode):
                env = make_env(pipeline_mode=mode, task_arrival_rate=0.8, image_size_mb=32,
                               mem_capacity_gb=0.05, deadline_steps=10,
                               transfer_fail_on_link_down=True)
                policy = RandomPolicy(3)
                for step in range(60):
                    env.topology.links["0-1"].up = step % 5 < 3
                    env.vis_cache.visible = step % 7 < 4
                    env.step(policy.select_actions(env.export_state()))
                    done = sum(tile.state == TileState.DONE for tile in env.tiles.values())
                    failed = sum(tile.state == TileState.FAILED for tile in env.tiles.values())
                    self.assertEqual(env.metrics.completed_tiles, done)
                    self.assertEqual(env.metrics.failed_tiles, failed)
                    for sid, sat in env.satellites.items():
                        held = sum(t.data_size_mb / 1024 for t in env.tiles.values() if sid in t.memory_holders)
                        held += sum(size for tid, size in env.task_source_mem_gb.items()
                                    if env.tasks[tid].source_sat_id == sid)
                        self.assertAlmostEqual(sat.mem_used_gb, held)
                        self.assertLessEqual(sat.mem_used_gb, sat.mem_capacity_gb + 1e-9)
                for key, sent in env.metrics.network_sent_mb.items():
                    self.assertLessEqual(sent, env.metrics.network_capacity_mb[key] + 1e-9)

    def test_pending_count_counts_tiles_not_locations(self):
        env = make_env()
        tiles = add_task(env, count=5)
        state = env.export_state()
        self.assertEqual(state.tiles[tiles[0].tile_id]["task_pending_count"], 5)

    def test_empty_metrics_are_strict_json(self):
        summary = Metrics().summary()
        self.assertIsNone(summary["latency"]["task_mean"])
        json.dumps(summary, allow_nan=False)

    def test_visibility_and_windows_use_seconds(self):
        cache = VisibilityCache(1, 2, 1800, dt=5)
        stations = {"gs": SimpleNamespace(min_elevation_deg=10, bandwidth_mbps=20)}
        with patch("sim.cache.visibility.sat_to_ground_geometry", return_value=(30, 100)) as geometry, \
             patch("sim.cache.visibility.next_gs_windows_for_sat", return_value={"gs": (10, 100)}) as windows:
            cache.refresh(5, [None], None, {"gs": None}, stations)
        self.assertEqual(geometry.call_args.kwargs["t_seconds"], 20)
        self.assertEqual(windows.call_args.kwargs["t_now_s"], 20)
        self.assertEqual(cache.windows_at(0, 5)["gs"], (5, 100))

    def test_topology_uses_seconds(self):
        cfg = SimpleNamespace(num_sats=1, bandwidth_period=0, max_neighbors=0)
        topo = object.__new__(TopologyModel)
        topo.cfg, topo.sat_recs, topo.t0, topo._executor = cfg, [None], None, None
        with patch("sim.topology.position_km", return_value=(7000, 0, 0)) as position:
            topo.snapshot(7, dt=5)
        self.assertEqual(position.call_args.args[-1], 35)


if __name__ == "__main__":
    unittest.main()
