import unittest
from types import SimpleNamespace
from dataclasses import replace

from sim.entities import Action, ActionType, FailureReason, Link, TileState
from test_simulation import add_task, make_env


class RelayVisibility:
    def __init__(self, windows):
        self.windows = windows
        self.lookahead_s = 100
        self.now = 0

    def refresh(self, t, recs, t0, station_objs, stations):
        self.now = t
        self.visible_gs_by_sat = {sid: [gid for gid in stations if self.is_visible(sid, gid, stations)]
                                  for sid in range(len(recs))}
        self.best_gs_by_sat = {sid: next(iter(gids), None) for sid, gids in self.visible_gs_by_sat.items()}

    def is_visible(self, sid, gid, stations):
        window = self.windows.get(sid)
        return window is not None and window[0] <= self.now < window[1]

    def windows_at(self, sid, t):
        window = self.windows.get(sid)
        return {"gs": (max(0, window[0] - t), window[1] - max(t, window[0]))
                if window and t < window[1] else None}


def relay_env(**changes):
    options = dict(num_sats=3, execution_commitment=True, result_buffering=True, result_relay=True)
    options.update(changes)
    env = make_env(**options)
    env.topology.links = {"0-1": Link(0, 1, True, 8, 0), "1-2": Link(1, 2, True, 8, 0)}
    env.vis_cache = RelayVisibility({2: (0, 100)})
    return env


class ResultRelayTests(unittest.TestCase):
    def check_memory(self, env):
        for sid, sat in env.satellites.items():
            held = sum(tile.data_size_mb / 1024 for tile in env.tiles.values() if sid in tile.memory_holders)
            held += sum(env.cfg.result_size_mb / 1024 for tile in env.tiles.values()
                        if sid in tile.result_memory_holders)
            self.assertAlmostEqual(sat.mem_used_gb, held)
            self.assertLessEqual(held, sat.mem_capacity_gb + 1e-9)

    def test_two_hop_results_deliver_without_source_ground_window(self):
        for enabled in (False, True):
            env = relay_env(result_relay=enabled)
            self.addCleanup(env.close)
            tile = add_task(env, size=8)[0]
            env.step([Action(tile.tile_id, ActionType.LOCAL)])
            self.assertEqual(tile.state, TileState.COMPUTED)
            self.assertAlmostEqual(env.satellites[0].mem_used_gb, 1 / 1024)
            for _ in range(8):
                env.step([])
                self.check_memory(env)
            self.assertEqual(env.metrics.completed_tasks, int(enabled))
            if enabled:
                self.assertEqual(tile.result_path, [0, 1, 2])
                self.assertEqual(env.metrics.task_latencies, [4])
                self.assertEqual(env.metrics.compute_busy_time, {0: 1})
                self.assertEqual(env.summary()["task_outcomes"][0]["result_relay_hops"], 2)
                self.assertEqual(sum(s.mem_used_gb for s in env.satellites.values()), 0)

    def test_raw_and_result_flows_share_isl_capacity(self):
        env = relay_env(result_size_mb=2)
        self.addCleanup(env.close)
        env.vis_cache = RelayVisibility({1: (0, 100)})
        result_tile = add_task(env, size=2)[0]
        raw_tile = add_task(env, size=2)[0]
        env.step([Action(result_tile.tile_id, ActionType.LOCAL)])
        env.step([Action(raw_tile.tile_id, ActionType.OFFLOAD, target_sat_id=1)])
        self.assertEqual(sorted(tr.remaining_mb for tr in env.transfers), [1.5, 1.5])
        self.assertEqual(env.metrics.network_sent_mb["isl:0->1"], 1)
        self.check_memory(env)

    def test_store_and_forward_waits_for_future_ground_contact(self):
        env = relay_env()
        self.addCleanup(env.close)
        env.vis_cache = RelayVisibility({2: (10, 20)})
        tile = add_task(env)[0]
        env.step([Action(tile.tile_id, ActionType.LOCAL)])
        for _ in range(9):
            env.step([])
        self.assertEqual(tile.result_path, [0, 1, 2])
        self.assertEqual(tile.state, TileState.COMPUTED)
        self.assertEqual(tile.result_memory_holders, {2})
        self.assertEqual(env.metrics.completed_tasks, 0)
        env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.assertEqual(env.metrics.task_latencies, [11])
        self.check_memory(env)

    def test_disconnection_preserves_progress_or_cleans_both_reservations(self):
        for fail in (False, True):
            env = relay_env(result_size_mb=2, transfer_fail_on_link_down=fail)
            self.addCleanup(env.close)
            env.vis_cache = RelayVisibility({1: (0, 100)})
            tile = add_task(env)[0]
            env.step([Action(tile.tile_id, ActionType.LOCAL)])
            env.step([])
            self.assertEqual(env.transfers[0].remaining_mb, 1)
            self.assertEqual(tile.result_memory_holders, {0, 1})
            env.topology.links["0-1"].up = False
            env.step([])
            if fail:
                self.assertEqual(tile.state, TileState.FAILED)
                self.assertFalse(env.transfers)
                self.assertFalse(tile.result_memory_holders)
            else:
                self.assertEqual(env.transfers[0].remaining_mb, 1)
                env.topology.links["0-1"].up = True
                for _ in range(4):
                    env.step([])
                self.assertEqual(tile.state, TileState.DONE)
            self.check_memory(env)

    def test_full_destination_waits_and_retries_after_space_released(self):
        env = relay_env(mem_capacity_gb=1 / 1024)
        self.addCleanup(env.close)
        resident = add_task(env, src=1)[0]
        tile = add_task(env)[0]
        env.step([Action(tile.tile_id, ActionType.LOCAL)])
        env.step([])
        self.assertFalse(env.transfers)
        self.assertEqual(tile.state, TileState.COMPUTED)
        env._fail_tile(resident, FailureReason.DEADLINE_MISS)
        for _ in range(4):
            env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.check_memory(env)

    def test_result_expansion_cannot_exceed_memory_and_deadline_cleans_inflight(self):
        env = relay_env(mem_capacity_gb=1 / 1024, result_size_mb=2)
        self.addCleanup(env.close)
        tile = add_task(env)[0]
        env.step([Action(tile.tile_id, ActionType.LOCAL)])
        self.assertEqual(tile.failure_reason, FailureReason.MEM_FULL)
        self.check_memory(env)
        env = relay_env(result_size_mb=10, deadline_steps=3)
        self.addCleanup(env.close)
        tile = add_task(env, deadline=3)[0]
        env.step([Action(tile.tile_id, ActionType.LOCAL)])
        env.step([])
        env.step([])
        self.assertEqual(tile.state, TileState.FAILED)
        self.assertFalse(env.transfers)
        self.assertFalse(tile.result_memory_holders)
        self.check_memory(env)

    def test_short_window_slow_links_and_hop_limit_do_not_create_delivery(self):
        for scenario in ("short_window", "slow", "hop_limit"):
            env = relay_env(relay_max_hops=1 if scenario == "hop_limit" else 8)
            self.addCleanup(env.close)
            env.vis_cache = RelayVisibility({2: (0, 3 if scenario == "short_window" else 100)})
            if scenario == "slow":
                for link in env.topology.links.values():
                    link.bandwidth_mbps = .08
            tile = add_task(env)[0]
            env.step([Action(tile.tile_id, ActionType.LOCAL)])
            for _ in range(10):
                env.step([])
            self.assertEqual(env.metrics.completed_tasks, 0)
            self.check_memory(env)

    def test_ground_relay_only_computes_on_ground(self):
        env = relay_env(pipeline_mode="ground_relay")
        self.addCleanup(env.close)
        tile = add_task(env, size=2)[0]
        for _ in range(8):
            env.step([])
        self.assertEqual(tile.raw_relay_path, [0, 1, 2])
        self.assertFalse(env.metrics.compute_busy_time)
        self.assertEqual(tile.state, TileState.DONE)
        self.assertEqual(env.metrics.task_latencies, [7])
        self.check_memory(env)

    def test_ground_tiles_has_no_isl_or_onboard_compute(self):
        env = relay_env(pipeline_mode="ground_tiles")
        self.addCleanup(env.close)
        tile = add_task(env, size=2)[0]
        for _ in range(8):
            env.step([])
        self.assertEqual(tile.state, TileState.QUEUED)
        self.assertFalse(env.transfers)
        env.vis_cache = RelayVisibility({0: (0, 100)})
        for _ in range(4):
            env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.assertEqual(tile.raw_relay_path, [0])
        self.assertFalse(env.metrics.compute_busy_time)
        self.check_memory(env)

    def test_contact_guard_rejects_short_and_downstream_expiring_contacts(self):
        from sim.routing import delivery_route
        env = relay_env()
        self.addCleanup(env.close)
        env.cfg = replace(env.cfg, relay_contact_guard=True,
                          topology={**env.cfg.topology, "predict_contacts": True})
        env.topology.cfg = SimpleNamespace(bandwidth_mbps_min=8)
        for duration, expected in ((1, None), (2.5, None), (4, ("sat", 1))):
            env.contact_cache = SimpleNamespace(scan_s=1, windows_at=lambda t: {
                "0-1": [(0, 10)], "1-2": [(0, duration)]})
            self.assertEqual(delivery_route(env, 0, 1, env.topology.links, [0]), expected)

    def test_invalid_relay_configuration_is_rejected(self):
        for options in ({"result_buffering": False}, {"relay_max_hops": 0}, {"relay_max_hops": 1.5}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                relay_env(**options)

    def test_arrivals_contention_and_terminal_cleanup_in_both_modes(self):
        for mode in ("distributed", "ground_relay"):
            env = relay_env(pipeline_mode=mode, task_arrival_rate=.7,
                            task_arrival_end_step=10, image_size_mb=4,
                            mem_capacity_gb=.02, deadline_steps=20)
            self.addCleanup(env.close)
            for step in range(40):
                actions = [Action(tile.tile_id, ActionType.LOCAL) for tile in env.tiles.values()
                           if tile.state == TileState.QUEUED]
                env.topology.links["0-1"].up = step % 7 < 5
                env.step(actions)
                self.check_memory(env)
                self.assertEqual(env.metrics.completed_tiles,
                                 sum(tile.state == TileState.DONE for tile in env.tiles.values()))
            self.assertGreater(env.metrics.total_tasks, 0)
            self.assertEqual(sum(s.mem_used_gb for s in env.satellites.values()), 0)
            for link, sent in env.metrics.network_sent_mb.items():
                self.assertLessEqual(sent, env.metrics.network_capacity_mb[link] + 1e-9)


if __name__ == "__main__":
    unittest.main()
