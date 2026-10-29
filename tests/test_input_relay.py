from dataclasses import asdict
import unittest

from sim.config import config_from_dict
from sim.entities import Action, ActionType, FailureReason, Link, TileState
from test_simulation import add_task, make_env


class InputRelayTests(unittest.TestCase):
    def env(self, **changes):
        options = dict(num_sats=3, input_relay_max_hops=2, execution_commitment=True)
        options.update(changes)
        env = make_env(**options)
        self.addCleanup(env.close)
        env.topology.links = {f"{sid}-{sid + 1}": Link(sid, sid + 1, True, 8, 0)
                              for sid in range(env.cfg.num_sats - 1)}
        env.vis_cache.visible = False
        return env

    def action(self, tile, path=(0, 1, 2)):
        return Action(tile.tile_id, ActionType.OFFLOAD, target_sat_id=path[-1], input_path=path)

    def check_memory(self, env):
        for sid, sat in env.satellites.items():
            expected = sum(env.memory_size_gb(tile.data_size_mb) for tile in env.tiles.values()
                           if sid in tile.memory_holders)
            self.assertAlmostEqual(sat.mem_used_gb, expected)
            self.assertLessEqual(sat.mem_used_gb, sat.mem_capacity_gb + 1e-12)

    def test_configuration_default_validation_and_round_trip(self):
        self.assertEqual(config_from_dict({}).input_relay_max_hops, 1)
        cfg = config_from_dict({"input_relay_max_hops": 3})
        self.assertEqual(config_from_dict(asdict(cfg)).input_relay_max_hops, 3)
        for value in (0, -1, 1.5, True, "2", None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "input_relay_max_hops"):
                config_from_dict({"input_relay_max_hops": value})

    def test_two_hops_charge_bytes_and_memory_without_intermediate_compute(self):
        for mode in ("step", "work_conserving"):
            with self.subTest(mode=mode):
                env = self.env(compute_service_mode=mode)
                tile = add_task(env)[0]
                env.step([self.action(tile)])
                self.assertEqual(tile.state, TileState.INPUT_RELAY)
                self.assertEqual(tile.location, 1)
                self.assertEqual(tile.memory_holders, {1, 2})
                self.assertEqual(tile.raw_relay_path, [0, 1])
                self.assertFalse(env.satellites[1].queue)
                self.assertFalse(env.metrics.compute_busy_time)
                self.check_memory(env)
                env.step([])
                self.assertEqual(tile.state, TileState.READY)
                self.assertEqual(tile.location, 2)
                self.assertEqual(tile.memory_holders, {2})
                self.assertEqual(tile.timestamps.start_tx, 0)
                self.assertEqual(tile.timestamps.end_tx, 2)
                self.assertIsNone(tile.timestamps.start_compute)
                self.check_memory(env)
                env.step([])
                self.assertEqual(tile.state, TileState.COMPUTED)
                self.assertEqual(tile.timestamps.start_compute, 2)
                self.assertEqual(env.metrics.compute_busy_time, {2: 1})
                self.assertEqual(tile.input_route, (0, 1, 2))
                self.assertEqual(tile.input_target_sat_id, 2)
                self.assertEqual(tile.raw_relay_path, [0, 1, 2])
                self.assertEqual(env.metrics.network_sent_mb["isl:0->1"], 1)
                self.assertEqual(env.metrics.network_sent_mb["isl:1->2"], 1)
                self.check_memory(env)
                env.vis_cache.visible = True
                env.step([])
                self.assertEqual(tile.state, TileState.DONE)
                self.assertFalse(tile.memory_holders)

    def test_next_hop_cannot_start_before_previous_arrival_boundary(self):
        env = self.env()
        tile = add_task(env)[0]
        env._dist_pipeline.apply_actions([self.action(tile)], env.topology.links)
        self.assertEqual(tile.memory_holders, {0, 1, 2})
        env._dist_pipeline.advance_transfers(env.topology.links)
        self.assertEqual(tile.timestamps.end_tx, 1)
        env._dist_pipeline.advance_input_relays(env.topology.links)
        self.assertFalse(env.transfers)
        self.assertEqual(tile.state, TileState.INPUT_RELAY)
        env.time = 1
        env._dist_pipeline.advance_input_relays(env.topology.links)
        self.assertEqual([(tr.src, tr.dst) for tr in env.transfers], [(1, 2)])

    def test_final_and_first_hop_reservations_are_atomic(self):
        for blocked in (1, 2):
            with self.subTest(blocked=blocked):
                env = self.env(mem_capacity_gb=1 / 1024)
                resident = add_task(env, src=blocked)[0]
                tile = add_task(env)[0]
                env.step([self.action(tile)])
                self.assertEqual(tile.state, TileState.QUEUED)
                self.assertEqual(tile.memory_holders, {0})
                self.assertEqual(resident.memory_holders, {blocked})
                self.assertFalse(tile.input_route)
                self.assertIsNone(tile.input_target_sat_id)
                self.assertFalse(env.transfers)
                self.check_memory(env)

    def test_final_reservation_is_not_counted_twice_for_explicit_one_hop(self):
        env = self.env(mem_capacity_gb=1 / 1024)
        tile = add_task(env)[0]
        env._dist_pipeline.apply_actions([self.action(tile, (0, 1))], env.topology.links)
        self.assertEqual(tile.memory_holders, {0, 1})
        self.check_memory(env)
        env.step([])
        self.assertEqual(tile.state, TileState.READY)
        self.assertEqual(tile.location, 1)
        self.check_memory(env)

    def test_intermediate_waits_for_next_hop_cache_without_computing(self):
        env = self.env(num_sats=4, input_relay_max_hops=3, mem_capacity_gb=1 / 1024)
        resident = add_task(env, src=2)[0]
        tile = add_task(env)[0]
        env.step([self.action(tile, (0, 1, 2, 3))])
        env.step([])
        self.assertEqual(tile.state, TileState.INPUT_RELAY)
        self.assertEqual(tile.memory_holders, {1, 3})
        self.assertFalse(env.transfers)
        self.assertFalse(env.metrics.compute_busy_time)
        env._fail_tile(resident, FailureReason.DEADLINE_MISS)
        env.step([])
        self.assertEqual(tile.location, 2)
        self.assertEqual(tile.state, TileState.INPUT_RELAY)
        self.assertEqual(tile.memory_holders, {2, 3})
        env.step([])
        self.assertEqual(tile.location, 3)
        self.assertEqual(tile.state, TileState.READY)
        self.check_memory(env)

    def test_disconnected_next_hop_waits_even_with_fail_on_link_down(self):
        env = self.env(transfer_fail_on_link_down=True)
        tile = add_task(env)[0]
        env.step([self.action(tile)])
        env.topology.links["1-2"].up = False
        env.step([Action(tile.tile_id, ActionType.LOCAL)])
        self.assertEqual(tile.state, TileState.INPUT_RELAY)
        self.assertEqual(tile.timestamps.created, 0)
        self.assertEqual(tile.memory_holders, {1, 2})
        self.assertFalse(env.transfers)
        env.topology.links["1-2"].up = True
        env.step([])
        self.assertEqual(tile.timestamps.end_tx, 3)
        self.assertEqual(tile.state, TileState.READY)
        self.check_memory(env)

    def test_inflight_failure_or_pause_uses_existing_transfer_semantics(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                env = self.env(transfer_fail_on_link_down=fail)
                tile = add_task(env, size=2)[0]
                env.step([self.action(tile)])
                self.assertEqual(env.transfers[0].remaining_mb, 1)
                env.topology.links["0-1"].up = False
                env.step([])
                if fail:
                    self.assertEqual(tile.state, TileState.FAILED)
                    self.assertFalse(tile.memory_holders)
                    self.assertFalse(env.transfers)
                else:
                    self.assertEqual(tile.state, TileState.TRANSFERRING)
                    self.assertEqual(env.transfers[0].remaining_mb, 1)
                    self.assertEqual(tile.memory_holders, {0, 1, 2})
                    env.topology.links["0-1"].up = True
                    for _ in range(3):
                        env.step([])
                    self.assertEqual(tile.state, TileState.READY)
                    self.assertEqual(tile.location, 2)
                self.check_memory(env)

    def test_deadline_cleans_intermediate_and_final_reservation(self):
        env = self.env(deadline_steps=2)
        tile = add_task(env, deadline=2)[0]
        env.step([self.action(tile)])
        env.topology.links["1-2"].up = False
        env.step([])
        self.assertEqual(tile.state, TileState.FAILED)
        self.assertEqual(tile.failure_reason, FailureReason.DEADLINE_MISS)
        self.assertFalse(tile.memory_holders)
        self.assertFalse(env.transfers)
        self.check_memory(env)

    def test_invalid_paths_are_ignored_without_failing_or_reserving(self):
        paths = ((1, 2), (0, 1, 0, 1, 2), (0, 2), (0, 1, 3), (0, True, 2), [0, 1, 2], None)
        for path in paths:
            with self.subTest(path=path):
                env = self.env()
                tile = add_task(env)[0]
                env.step([Action(tile.tile_id, ActionType.OFFLOAD, target_sat_id=2, input_path=path)])
                self.assertEqual(tile.state, TileState.QUEUED)
                self.assertFalse(env.transfers)
                self.assertEqual(tile.memory_holders, {0})
                self.assertFalse(tile.input_route)
                self.check_memory(env)
        for mode in ("hops", "link_down", "mismatch", "ground", "local"):
            with self.subTest(mode=mode):
                env = self.env(input_relay_max_hops=1 if mode == "hops" else 2)
                tile = add_task(env)[0]
                action = self.action(tile)
                if mode == "link_down":
                    env.topology.links["1-2"].up = False
                elif mode == "mismatch":
                    action.target_sat_id = 1
                elif mode == "ground":
                    action.target_gs_id = "gs"
                elif mode == "local":
                    action.action_type = ActionType.LOCAL
                env.step([action])
                self.assertEqual(tile.state, TileState.QUEUED)
                self.assertEqual(tile.memory_holders, {0})
                self.assertFalse(env.transfers)

    def test_explicit_route_cannot_migrate_committed_or_running_tile(self):
        for state in (TileState.READY, TileState.RUNNING):
            with self.subTest(state=state):
                env = self.env(execution_commitment=False)
                tile = add_task(env)[0]
                tile.state = state
                env._dist_pipeline.apply_actions([self.action(tile)], env.topology.links)
                self.assertEqual(tile.state, state)
                self.assertEqual(tile.memory_holders, {0})
                self.assertFalse(env.transfers)

    def test_relay_and_legacy_input_share_link_capacity(self):
        env = self.env()
        relay, legacy = add_task(env, count=2, size=2)
        env.step([self.action(relay), Action(legacy.tile_id, ActionType.OFFLOAD, target_sat_id=1)])
        self.assertEqual([tr.remaining_mb for tr in env.transfers], [1.5, 1.5])
        self.assertEqual(env.metrics.network_sent_mb["isl:0->1"], 1)
        self.assertFalse(env.metrics.compute_busy_time)
        self.check_memory(env)

    def test_commitment_is_charged_only_to_final_node_in_all_input_phases(self):
        env = self.env()
        tile = add_task(env, size=2)[0]

        def assert_committed(expected):
            state = env.export_state()
            self.assertEqual({sid: sat["committed_compute_s"] for sid, sat in state.satellites.items()},
                             {0: 0, 1: 0, 2: expected})

        env._dist_pipeline.apply_actions([self.action(tile)], env.topology.links)
        assert_committed(1)
        env.step([])
        assert_committed(1)
        env.step([])
        self.assertEqual(tile.state, TileState.INPUT_RELAY)
        assert_committed(1)
        env.step([])
        self.assertEqual(tile.state, TileState.TRANSFERRING)
        assert_committed(1)
        env.step([])
        self.assertEqual(tile.state, TileState.READY)
        assert_committed(1)
        env.step([])
        self.assertEqual(tile.state, TileState.COMPUTED)
        assert_committed(0)
        env.vis_cache.visible = True
        env.step([])
        self.assertEqual(tile.state, TileState.DONE)
        assert_committed(0)


if __name__ == "__main__":
    unittest.main()
