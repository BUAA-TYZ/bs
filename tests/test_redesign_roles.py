import unittest
from unittest.mock import patch

from sim.entities import Action, ActionType, FailureReason, Link, TileState
from sim.scheduler.literature import contact_plan
from sim.topology import bounded_edges
from test_simulation import add_task, make_env


class RedesignRoleTests(unittest.TestCase):
    def env(self, **kwargs):
        env = make_env(sar_source_isolation=True, task_source_sat_ids=[0, 1], **kwargs)
        self.addCleanup(env.close)
        return env

    def test_source_cannot_accept_foreign_input_but_can_compute_own(self):
        env = self.env()
        tile = add_task(env)[0]
        env._dist_pipeline.apply_actions([Action(tile.tile_id, ActionType.OFFLOAD, target_sat_id=1)], env.topology.links)
        self.assertEqual(env.transfers, [])
        self.assertEqual(tile.state, TileState.QUEUED)
        env._dist_pipeline.apply_actions([Action(tile.tile_id, ActionType.LOCAL)], env.topology.links)
        env._dist_pipeline.advance_compute()
        self.assertEqual(tile.state, TileState.COMPUTED)

    def test_executor_rejects_foreign_compute_even_without_policy_mask(self):
        env = self.env()
        tile = add_task(env)[0]
        env.satellites[0].queue.remove(tile.tile_id)
        env.satellites[1].queue.append(tile.tile_id)
        tile.location, tile.state = 1, TileState.READY
        env._dist_pipeline.advance_compute()
        self.assertEqual(tile.failure_reason, FailureReason.ROLE_FORBIDDEN)
        self.assertIsNone(tile.timestamps.start_compute)

    def test_result_relay_cannot_use_source_even_if_route_is_faulty(self):
        env = self.env(result_buffering=True, result_relay=True)
        tile = add_task(env)[0]
        tile.state, tile.result_path = TileState.COMPUTED, [0]
        env._computed_waiting_downlink.add(tile.tile_id)
        with patch("sim.pipeline.distributed.delivery_route", return_value=("sat", 1)):
            env._dist_pipeline.start_downlinks(env.topology.links)
        self.assertEqual(env.transfers, [])

    def test_planner_never_receives_at_source(self):
        env = self.env()
        state = env.export_state()
        state.links["0-1"]["contact_windows"] = [(0, 100)]
        self.assertFalse(any(isinstance(c.target, int) for c in contact_plan(state).contacts))

    def test_terminal_matching_preserves_backbone_and_excludes_source_pairs(self):
        pairs = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        geometry = [(True, i + 1) for i in range(len(pairs))]
        edges = bounded_edges(pairs, geometry, 0, sources=[0, 1], limits=[1, 1, 2, 2], preferred=[(2, 3)])
        self.assertNotIn((0, 1), edges)
        self.assertIn((2, 3), edges)
        self.assertEqual(sum(0 in edge for edge in edges), 1)
        self.assertEqual(sum(1 in edge for edge in edges), 1)


if __name__ == "__main__":
    unittest.main()
