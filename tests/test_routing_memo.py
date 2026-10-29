import unittest
from unittest.mock import patch

from sim.entities import TileState
from test_simulation import add_task, make_env


ROUTE = "sim.pipeline.distributed.delivery_route"


class RoutingMemoTests(unittest.TestCase):
    def raw_env(self, **changes):
        options = dict(pipeline_mode="ground_relay", result_buffering=True, result_relay=True)
        options.update(changes)
        env = make_env(**options)
        self.addCleanup(env.close)
        return env

    def result_env(self, count=3):
        env = self.raw_env(pipeline_mode="distributed")
        tiles = add_task(env, count=count)
        for tile in tiles:
            env._release_tile_memory(tile, tile.location)
            env._reserve_result_memory(tile, tile.location)
            tile.state = TileState.COMPUTED
            tile.result_path = [tile.location]
            tile.timestamps.end_compute = 0
            env._computed_waiting_downlink.add(tile.tile_id)
        return env, tiles

    def test_raw_none_is_reused_only_inside_one_call(self):
        env = self.raw_env()
        tiles = add_task(env, count=6)
        with patch(ROUTE, return_value=None) as route:
            env._dist_pipeline.route_raw_tiles({})
            self.assertEqual(route.call_count, 1)
            self.assertTrue(all(tile.raw_relay_path == [0] for tile in tiles))
            env._dist_pipeline.route_raw_tiles({})
            self.assertEqual(route.call_count, 2)
            env.time += 1
            env._dist_pipeline.route_raw_tiles({})
            self.assertEqual(route.call_count, 3)
        self.assertTrue(all(tile.state == TileState.QUEUED for tile in tiles))

    def test_raw_none_key_includes_source_size_and_path(self):
        env = self.raw_env()
        add_task(env, count=2)
        add_task(env, src=1)
        add_task(env, size=2)
        different_path = add_task(env)[0]
        different_path.raw_relay_path = [1, 0]
        with patch(ROUTE, return_value=None) as route:
            env._dist_pipeline.route_raw_tiles({})
        self.assertEqual(route.call_count, 4)

    def test_ground_only_skips_hidden_source_but_relay_does_not(self):
        for mode, expected_calls in (("ground_tiles", 0), ("ground_relay", 1)):
            with self.subTest(mode=mode):
                env = self.raw_env(pipeline_mode=mode)
                env.vis_cache.visible = False
                tiles = add_task(env, count=3)
                with patch(ROUTE, return_value=None) as route:
                    env._dist_pipeline.route_raw_tiles({})
                    self.assertEqual(route.call_count, expected_calls)
                    env.vis_cache.visible = True
                    env._dist_pipeline.route_raw_tiles({})
                    self.assertEqual(route.call_count, expected_calls + 1)
                self.assertTrue(all(tile.raw_relay_path == [0] for tile in tiles))

    def test_raw_success_invalidates_prior_none_and_is_not_cached(self):
        env = self.raw_env()
        tiles = add_task(env, count=4)
        tiles[1].data_size_mb = 2
        with patch(ROUTE, side_effect=[None, ("ground", "gs"), ("ground", "gs"), ("ground", "gs")]) as route:
            env._dist_pipeline.route_raw_tiles({})
        self.assertEqual(route.call_count, 4)
        self.assertEqual(len(env.ground_tile_transfers), 3)
        self.assertEqual(tiles[0].state, TileState.QUEUED)

    def test_raw_failed_transfer_attempt_invalidates_prior_none(self):
        env = self.raw_env()
        tiles = add_task(env, count=3)
        tiles[1].data_size_mb = 2
        with patch(ROUTE, side_effect=[None, ("sat", 1), None]) as route:
            env._dist_pipeline.route_raw_tiles({})
        self.assertEqual(route.call_count, 3)
        self.assertEqual(tiles[1].state, TileState.FAILED)
        self.assertEqual(tiles[2].state, TileState.QUEUED)

    def test_result_none_is_reused_only_inside_one_call(self):
        env, tiles = self.result_env(count=6)
        with patch(ROUTE, return_value=None) as route:
            env._dist_pipeline.start_downlinks({})
            self.assertEqual(route.call_count, 1)
            env._dist_pipeline.start_downlinks({})
            self.assertEqual(route.call_count, 2)
            env.time += 1
            env._dist_pipeline.start_downlinks({})
            self.assertEqual(route.call_count, 3)
        self.assertTrue(all(tile.state == TileState.COMPUTED for tile in tiles))

    def test_result_none_key_includes_source_size_and_path(self):
        env, tiles = self.result_env(count=5)
        tiles[2].location = 1
        tiles[2].result_path = [1]
        tiles[3].result_size_mb = 2
        tiles[4].result_path = [1, 0]
        with patch(ROUTE, return_value=None) as route:
            env._dist_pipeline.start_downlinks({})
        self.assertEqual(route.call_count, 4)

    def test_result_success_invalidates_prior_none_and_is_not_cached(self):
        env, tiles = self.result_env(count=4)
        tiles[1].result_size_mb = 2
        with patch(ROUTE, side_effect=[None, ("ground", "gs"), ("ground", "gs"), ("ground", "gs")]) as route:
            env._dist_pipeline.start_downlinks({})
        self.assertEqual(route.call_count, 4)
        self.assertEqual(len(env.downlink_transfers), 3)
        self.assertEqual(tiles[0].state, TileState.COMPUTED)

    def test_result_failed_reservation_invalidates_prior_none(self):
        env, tiles = self.result_env()
        tiles[1].result_size_mb = 2
        with patch(ROUTE, side_effect=[None, ("sat", 1), None]) as route:
            with patch.object(env, "_reserve_result_memory", return_value=False) as reserve:
                env._dist_pipeline.start_downlinks({})
        self.assertEqual(route.call_count, 3)
        reserve.assert_called_once_with(tiles[1], 1)
        self.assertFalse(env.transfers)
        self.assertTrue(all(tile.state == TileState.COMPUTED for tile in tiles))


if __name__ == "__main__":
    unittest.main()
