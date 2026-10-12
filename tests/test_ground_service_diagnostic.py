import unittest

from analysis.ground_service_diagnostic import task_serial_ground_transfers
from sim.entities import GroundTileTransfer
from test_simulation import make_env, add_task


class GroundServiceTests(unittest.TestCase):
    def test_oldest_task_gets_service_without_increasing_capacity(self):
        env = make_env()
        self.addCleanup(env.close)
        early = add_task(env, count=2)
        env.time = 1
        later = add_task(env)[0]
        flows = [GroundTileTransfer(later.tile_id, 0, "gs", 1, 1)]
        flows += [GroundTileTransfer(tile.tile_id, 0, "gs", 1, 0) for tile in early]
        task_serial_ground_transfers(env, flows)
        self.assertEqual([tr.remaining_mb for tr in flows], [1, .5, .5])
        self.assertEqual(env.metrics.network_sent_mb["ground:gs"], 1)
        self.assertEqual(env.metrics.network_capacity_mb["ground:gs"], 1)
