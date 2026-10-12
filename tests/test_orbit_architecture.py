from dataclasses import replace
import json
import unittest

import numpy as np
from sgp4.api import Satrec
from sgp4.io import verify_checksum

from analyze_orbit_architecture import layout_config, spans
from sim.config import load_config
from sim.constellation import clustered_walker_tle
from sim.entities import Action, ActionType, TileState
from test_simulation import make_env, add_task


class OrbitArchitectureTests(unittest.TestCase):
    def test_roles_preserve_default_rng_and_restrict_sources(self):
        default = make_env(task_arrival_rate=2)
        explicit = make_env(task_arrival_rate=2, task_source_sat_ids=[0, 1])
        imaging = make_env(task_arrival_rate=2, task_source_sat_ids=[1])
        for env in (default, explicit, imaging):
            self.addCleanup(env.close)
            for t in range(10):
                env._task_arrivals(t)
        trace = lambda env: [(task.source_sat_id, task.release_time) for task in env.tasks.values()]
        self.assertEqual(trace(default), trace(explicit))
        self.assertGreater(len(imaging.tasks), 0)
        self.assertEqual({task.source_sat_id for task in imaging.tasks.values()}, {1})
        self.assertEqual([t.release_time for t in default.tasks.values()],
                         [t.release_time for t in imaging.tasks.values()])

    def test_heterogeneous_rates_affect_real_compute_and_observation(self):
        env = make_env(satellite_compute_rates=[0.1, 1.0])
        self.addCleanup(env.close)
        slow = add_task(env, src=0)[0]
        fast = add_task(env, src=1)[0]
        env.step([Action(slow.tile_id, ActionType.LOCAL), Action(fast.tile_id, ActionType.LOCAL)])
        self.assertEqual(fast.state, TileState.COMPUTED)
        self.assertEqual(slow.state, TileState.RUNNING)
        self.assertEqual(env.export_state().satellites[0]["compute_rate"], 0.1)

    def test_invalid_roles_fail_early(self):
        cfg = load_config("examples/config_committed.yaml")
        for ids in ([], [0, 0], [-1], [24], [True]):
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                replace(cfg, task_source_sat_ids=ids)
        for rates in ([1], [float("nan")] * 24, [0] * 24, [True] * 24):
            with self.subTest(rates=rates), self.assertRaises(ValueError):
                replace(cfg, satellite_compute_rates=rates)

    def test_cluster_keeps_source_orbits_budget_and_close_neighbors(self):
        base = load_config("examples/config_committed.yaml")
        uniform = layout_config(base)
        cluster = layout_config(base, clustered=True, heterogeneous=True)
        self.assertEqual(uniform.task_source_sat_ids, cluster.task_source_sat_ids)
        self.assertAlmostEqual(sum(uniform.satellite_compute_rates), sum(cluster.satellite_compute_rates))
        positions = []
        for sid, lines in enumerate(cluster.topology["tle_lines"]):
            verify_checksum(*lines)
            sat = Satrec.twoline2rv(*lines)
            self.assertEqual(sat.satnum, sid + 1)
            error, position, _ = sat.sgp4(sat.jdsatepoch, sat.jdsatepochF)
            self.assertEqual(error, 0)
            positions.append(np.asarray(position))
            if sid in cluster.task_source_sat_ids:
                self.assertEqual(lines, uniform.topology["tle_lines"][sid])
        for sid in cluster.task_source_sat_ids:
            for nb in (sid + 1, sid + 2):
                self.assertTrue(200 < np.linalg.norm(positions[sid] - positions[nb]) < 300)

    def test_rejects_duplicate_and_overlapping_cluster_phases(self):
        for offsets in ([0, 0], [1, 2], [0, 180], [0, float("nan")]):
            with self.subTest(offsets=offsets), self.assertRaises(ValueError):
                clustered_walker_tle("2026-03-20T17:30:00Z", 4, 2, offsets, 705, 98.2)

    def test_interval_boundaries_are_censored(self):
        rows = spans([True, True, False, True, False, True], 30)
        self.assertEqual([r["duration_s"] for r in rows], [60, 30, 30])
        self.assertTrue(rows[0]["left_censored"])
        self.assertFalse(rows[1]["left_censored"] or rows[1]["right_censored"])
        self.assertTrue(rows[2]["right_censored"])
        self.assertEqual(spans([False, False], 30), [])
        self.assertEqual(json.loads(json.dumps(rows)), rows)


if __name__ == "__main__":
    unittest.main()
