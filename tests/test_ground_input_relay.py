from dataclasses import asdict
import unittest

from sim.config import config_from_dict
from sim.entities import Action, ActionType, FailureReason, Link, TileState
from sim.routing import delivery_route
from test_simulation import add_task, make_env


class GroundWindows:
    def __init__(self, windows):
        self.windows = windows
        self.lookahead_s = 100
        self.now = 0

    def refresh(self, t, recs, t0, station_objs, stations):
        self.now = t
        self.visible_gs_by_sat = {sid: [gid for gid in stations if self.is_visible(sid, gid, stations)]
                                  for sid in range(len(recs))}
        self.best_gs_by_sat = {sid: next(iter(gids), None)
                               for sid, gids in self.visible_gs_by_sat.items()}

    def is_visible(self, sid, gid, stations):
        window = self.windows.get((sid, gid))
        return window is not None and window[0] <= self.now < window[1]

    def windows_at(self, sid, t):
        return {gid: (max(0, start - t), end - max(t, start)) if t < end else None
                for (node, gid), (start, end) in self.windows.items() if node == sid}


class GroundInputRelayTests(unittest.TestCase):
    def env(self, **changes):
        options = dict(num_sats=3, ground_input_relay=True, execution_commitment=True,
                       compute_service_mode="work_conserving",
                       topology={"tle_lines": [("fake-topology", "fake-topology")]})
        options.update(changes)
        env = make_env(**options)
        self.addCleanup(env.close)
        env.topology.links = {"0-1": Link(0, 1, True, 8, 0), "1-2": Link(1, 2, True, 8, 0)}
        env.vis_cache = GroundWindows({(2, "gs"): (0, 100)})
        return env

    def action(self, tile, target="gs"):
        return Action(tile.tile_id, ActionType.OFFLOAD, target_gs_id=target, relay_to_ground=True)

    def check_memory(self, env):
        for sid, sat in env.satellites.items():
            expected = sum(env.memory_size_gb(tile.data_size_mb) for tile in env.tiles.values()
                           if sid in tile.memory_holders)
            expected += sum(env.memory_size_gb(env.tile_result_size_mb(tile)) for tile in env.tiles.values()
                            if sid in tile.result_memory_holders)
            self.assertAlmostEqual(sat.mem_used_gb, expected)
            self.assertLessEqual(sat.mem_used_gb, sat.mem_capacity_gb + 1e-12)

    def assert_committed(self, env, amount, target="gs"):
        state = env.export_state()
        self.assertTrue(all(sat["committed_compute_s"] == 0 for sat in state.satellites.values()))
        for gid, ground in state.ground_stations.items():
            self.assertAlmostEqual(ground["committed_compute_s"], amount if gid == target else 0)

    def test_config_default_validation_and_round_trip(self):
        self.assertFalse(config_from_dict({}).ground_input_relay)
        cfg = config_from_dict({"ground_input_relay": True})
        self.assertTrue(config_from_dict(asdict(cfg)).ground_input_relay)
        for value in (1, 0, "false", None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "ground_input_relay"):
                config_from_dict({"ground_input_relay": value})

    def test_two_hops_then_ground_compute_without_any_satellite_compute(self):
        for mode in ("step", "work_conserving"):
            with self.subTest(mode=mode):
                env = self.env(compute_service_mode=mode)
                tile = add_task(env)[0]
                env.step([self.action(tile)])
                self.assertEqual(tile.state, TileState.GROUND_RELAY_PENDING)
                self.assertEqual(tile.location, 1)
                self.assertEqual(tile.memory_holders, {1})
                self.assertFalse(env.satellites[1].queue)
                env.step([])
                self.assertEqual(tile.state, TileState.GROUND_RELAY_PENDING)
                self.assertEqual(tile.location, 2)
                self.assertEqual(tile.memory_holders, {2})
                env.step([])
                self.assertEqual(tile.state, TileState.GROUND_QUEUED)
                self.assertFalse(tile.memory_holders)
                self.assertEqual(tile.timestamps.start_tx, 0)
                self.assertEqual(tile.timestamps.end_tx, 3)
                self.assertIsNone(tile.timestamps.start_compute)
                env.step([])
                self.assertEqual(tile.state, TileState.DONE)
                self.assertEqual(tile.timestamps.start_compute, 3)
                self.assertEqual(tile.timestamps.end_compute, 4)
                self.assertEqual(tile.raw_relay_path, [0, 1, 2])
                self.assertEqual(tile.ground_relay_target, "gs")
                self.assertFalse(tile.result_path)
                self.assertFalse(env.metrics.compute_busy_time)
                self.assertEqual(env.metrics.network_sent_mb["isl:0->1"], 1)
                self.assertEqual(env.metrics.network_sent_mb["isl:1->2"], 1)
                self.assertEqual(env.metrics.network_sent_mb["ground:gs"], 1)
                self.check_memory(env)

    def test_ground_commitment_is_unique_through_wait_input_upload_queue_and_running(self):
        env = self.env()
        env.topology.links["1-2"].up = False
        tile = add_task(env, size=2)[0]
        tile.compute_cost = 2.5
        env._dist_pipeline.apply_actions([self.action(tile)], env.topology.links)
        self.assertEqual(tile.state, TileState.GROUND_RELAY_PENDING)
        self.assert_committed(env, 2.5)
        env.topology.links["1-2"].up = True
        for expected in (TileState.TRANSFERRING, TileState.GROUND_RELAY_PENDING,
                         TileState.TRANSFERRING, TileState.GROUND_RELAY_PENDING,
                         TileState.TRANSFERRING, TileState.GROUND_QUEUED):
            env.step([])
            self.assertEqual(tile.state, expected)
            self.assert_committed(env, 2.5)
            self.check_memory(env)
        env.step([])
        self.assertEqual(tile.state, TileState.GROUND_RUNNING)
        self.assert_committed(env, 1.5)
        env.step([])
        self.assert_committed(env, 0.5)
        env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.assert_committed(env, 0)
        self.assertAlmostEqual(env.ground_compute_busy_time["gs"], 2.5)
        self.assertFalse(env.metrics.compute_busy_time)

    def test_next_hop_waits_for_arrival_boundary(self):
        env = self.env()
        tile = add_task(env)[0]
        env._dist_pipeline.apply_actions([self.action(tile)], env.topology.links)
        env._dist_pipeline.advance_transfers(env.topology.links)
        self.assertEqual(tile.timestamps.end_tx, 1)
        env._dist_pipeline.advance_ground_input_relays(env.topology.links)
        self.assertFalse(env.transfers)
        self.assertEqual(tile.state, TileState.GROUND_RELAY_PENDING)
        env.time = 1
        env._dist_pipeline.advance_ground_input_relays(env.topology.links)
        self.assertEqual([(tr.src, tr.dst) for tr in env.transfers], [(1, 2)])

    def test_target_ground_station_is_fixed_even_when_another_is_visible(self):
        env = self.env(ground_stations=[{"id": "near", "bandwidth_mbps": 8, "compute_rate": 1},
                                       {"id": "far", "bandwidth_mbps": 8, "compute_rate": 1}])
        env.vis_cache = GroundWindows({(0, "near"): (0, 100), (2, "far"): (0, 100)})
        env.export_state()
        self.assertEqual(delivery_route(env, 0, 1, env.topology.links, [0]), ("ground", "near"))
        self.assertEqual(delivery_route(env, 0, 1, env.topology.links, [0], target_gs_id="far"), ("sat", 1))
        self.assertIsNone(delivery_route(env, 0, 1, env.topology.links, [0], target_gs_id="absent"))
        tile = add_task(env)[0]
        env.step([self.action(tile, "far")])
        self.assert_committed(env, 1, "far")
        for _ in range(3):
            env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.assertEqual(env.ground_compute_busy_time, {"far": 1})
        self.assertEqual(env.metrics.network_sent_mb["ground:far"], 1)
        self.assertEqual(env.metrics.network_sent_mb.get("ground:near", 0), 0)
        self.assert_committed(env, 0, "far")

    def test_no_route_wait_keeps_age_and_cannot_be_rescheduled_to_local(self):
        env = self.env()
        env.topology.links["1-2"].up = False
        tile = add_task(env)[0]
        env.step([self.action(tile)])
        env.step([Action(tile.tile_id, ActionType.LOCAL)])
        self.assertEqual(tile.state, TileState.GROUND_RELAY_PENDING)
        self.assertEqual(tile.location, 0)
        self.assertEqual(tile.memory_holders, {0})
        self.assertEqual(tile.timestamps.created, 0)
        self.assertIsNone(tile.timestamps.start_tx)
        self.assertNotIn(tile.tile_id, env.export_state().tiles)
        env.topology.links["1-2"].up = True
        for _ in range(4):
            env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.assertEqual(tile.timestamps.start_tx, 2)
        self.assertFalse(env.metrics.compute_busy_time)

    def test_future_ground_window_waits_with_actual_input_at_downlink_satellite(self):
        env = self.env()
        env.vis_cache = GroundWindows({(2, "gs"): (10, 20)})
        tile = add_task(env)[0]
        env.step([self.action(tile)])
        for _ in range(9):
            env.step([])
        self.assertEqual(tile.state, TileState.GROUND_RELAY_PENDING)
        self.assertEqual(tile.location, 2)
        self.assertEqual(tile.raw_relay_path, [0, 1, 2])
        self.assertEqual(tile.memory_holders, {2})
        self.assertFalse(env.ground_compute_busy_time)
        self.assert_committed(env, 1)
        env.step([])
        self.assertEqual(tile.state, TileState.GROUND_QUEUED)
        env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.assertEqual(tile.timestamps.start_tx, 0)

    def test_full_next_hop_waits_without_failure_or_cache_overbooking(self):
        env = self.env(mem_capacity_gb=1 / 1024)
        tile = add_task(env)[0]
        env.step([self.action(tile)])
        resident = add_task(env, src=2)[0]
        env.step([])
        self.assertEqual(tile.state, TileState.GROUND_RELAY_PENDING)
        self.assertEqual(tile.location, 1)
        self.assertEqual(tile.memory_holders, {1})
        self.check_memory(env)
        env._fail_tile(resident, FailureReason.DEADLINE_MISS)
        for _ in range(3):
            env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        self.check_memory(env)

    def test_link_drop_pause_or_fail_and_failure_cleanup(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                env = self.env(transfer_fail_on_link_down=fail)
                tile = add_task(env, size=2)[0]
                env.step([self.action(tile)])
                env.topology.links["0-1"].up = False
                env.step([])
                if fail:
                    self.assertEqual(tile.state, TileState.FAILED)
                    self.assertFalse(env.transfers)
                    self.assertFalse(tile.memory_holders)
                    self.assert_committed(env, 0)
                else:
                    self.assertEqual(tile.state, TileState.TRANSFERRING)
                    self.assertEqual(env.transfers[0].remaining_mb, 1)
                    self.assertEqual(tile.memory_holders, {0, 1})
                    self.assert_committed(env, 1)
                    env.topology.links["0-1"].up = True
                    for _ in range(6):
                        env.step([])
                    self.assertEqual(tile.state, TileState.DONE)
                self.check_memory(env)

    def test_deadline_cleans_waiting_and_ground_upload_commitments(self):
        for disconnected in (False, True):
            with self.subTest(disconnected=disconnected):
                env = self.env(deadline_steps=3)
                tile = add_task(env, deadline=3)[0]
                if disconnected:
                    env.topology.links["1-2"].up = False
                env.step([self.action(tile)])
                for _ in range(2):
                    env.step([])
                self.assertEqual(tile.state, TileState.FAILED)
                self.assertEqual(tile.failure_reason, FailureReason.DEADLINE_MISS)
                self.assertFalse(tile.memory_holders)
                self.assertFalse(env.transfers)
                self.assertFalse(env.ground_tile_transfers)
                self.assertFalse(any(env.ground_tile_queues.values()))
                self.assert_committed(env, 0)
                self.check_memory(env)

    def test_hop_limit_and_visited_nodes_do_not_allow_input_loops(self):
        env = self.env(relay_max_hops=1)
        tile = add_task(env)[0]
        env.step([self.action(tile)])
        self.assertEqual(tile.state, TileState.GROUND_RELAY_PENDING)
        self.assertEqual(tile.location, 0)
        self.assertFalse(env.transfers)
        env = self.env()
        tile = add_task(env)[0]
        env.step([self.action(tile)])
        env.topology.links["1-2"].up = False
        env.vis_cache = GroundWindows({(0, "gs"): (0, 100)})
        env.step([])
        self.assertEqual(tile.location, 1)
        self.assertEqual(tile.raw_relay_path, [0, 1])
        self.assertEqual(tile.state, TileState.GROUND_RELAY_PENDING)
        self.assertFalse(env.transfers)

    def test_disabled_or_malformed_actions_do_not_commit_or_fail(self):
        for case in ("disabled", "unknown", "wrong_type", "input_path", "sat_target", "local", "flag"):
            with self.subTest(case=case):
                env = self.env(ground_input_relay=case != "disabled")
                tile = add_task(env)[0]
                action = self.action(tile)
                if case == "unknown":
                    action.target_gs_id = "absent"
                elif case == "wrong_type":
                    action.target_gs_id = ["gs"]
                elif case == "input_path":
                    action.input_path = (0, 1, 2)
                elif case == "sat_target":
                    action.target_sat_id = 2
                elif case == "local":
                    action.action_type = ActionType.LOCAL
                elif case == "flag":
                    action.relay_to_ground = "yes"
                env.step([action])
                self.assertEqual(tile.state, TileState.QUEUED)
                self.assertIsNone(tile.ground_relay_target)
                self.assertEqual(tile.memory_holders, {0})
                self.assertFalse(env.transfers)

    def test_committed_or_running_tile_cannot_be_reassigned(self):
        for state in (TileState.READY, TileState.RUNNING):
            with self.subTest(state=state):
                env = self.env(execution_commitment=False)
                tile = add_task(env)[0]
                tile.state = state
                env._dist_pipeline.apply_actions([self.action(tile)], env.topology.links)
                self.assertEqual(tile.state, state)
                self.assertIsNone(tile.ground_relay_target)
                self.assertFalse(env.transfers)

    def test_ground_relay_input_and_result_share_the_same_link_capacity(self):
        env = self.env(result_size_mb=2, result_buffering=True, result_relay=True)
        result_tile = add_task(env)[0]
        env.step([Action(result_tile.tile_id, ActionType.LOCAL)])
        raw, legacy = add_task(env, count=2, size=2)
        env.step([self.action(raw), Action(legacy.tile_id, ActionType.OFFLOAD, target_sat_id=1)])
        self.assertEqual(len(env.transfers), 3)
        for transfer in env.transfers:
            self.assertAlmostEqual(transfer.remaining_mb, 2 - 1 / 3)
        self.assertEqual(env.metrics.network_sent_mb["isl:0->1"], 1)
        self.check_memory(env)

    def test_legacy_direct_ground_upload_keeps_its_existing_semantics(self):
        env = self.env(ground_input_relay=False)
        env.vis_cache = GroundWindows({(0, "gs"): (0, 100)})
        tile = add_task(env)[0]
        env.step([Action(tile.tile_id, ActionType.OFFLOAD, target_gs_id="gs")])
        self.assertEqual(tile.state, TileState.GROUND_QUEUED)
        self.assertIsNone(tile.ground_relay_target)
        env.step([])
        self.assertEqual(tile.state, TileState.DONE)


if __name__ == "__main__":
    unittest.main()
