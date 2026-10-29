from dataclasses import asdict
import random
import unittest
from unittest.mock import patch

import numpy as np

import main
from sim.config import config_from_dict
from sim.entities import TileState
from sim.marl.decision import build_batch, candidate_tiles, decision_state
from test_simulation import add_task, make_env


class ScheduledArrivalTests(unittest.TestCase):
    def env(self, **changes):
        env = make_env(**changes)
        self.addCleanup(env.close)
        return env

    def test_schedule_round_trip_preserves_geometry_metadata(self):
        events = [{"release_step": 2, "source_sat_id": 1,
                   "geometry": {"target_lat_deg": 37.0, "solar_elevation_deg": 45.0}}]
        cfg = config_from_dict({"task_arrival_rate": 0, "scheduled_task_arrivals": events,
                                "decision_tile_limit_per_sat": 64})
        self.assertEqual(config_from_dict(asdict(cfg)), cfg)
        self.assertEqual(cfg.scheduled_task_arrivals, events)
        self.assertEqual(config_from_dict({}).decision_tile_limit_per_sat, 4)
        self.assertIsNone(config_from_dict({}).scheduled_task_arrivals)

    def test_invalid_schedules_reject_ambiguous_or_unreachable_events(self):
        base = {"num_sats": 2, "sim_steps": 10, "task_arrival_rate": 0,
                "task_source_sat_ids": [1], "task_arrival_end_step": 8}
        invalid = [None, {}, {"release_step": 0}, {"source_sat_id": 1},
                   {"release_step": True, "source_sat_id": 1},
                   {"release_step": 1.5, "source_sat_id": 1},
                   {"release_step": -1, "source_sat_id": 1},
                   {"release_step": 8, "source_sat_id": 1},
                   {"release_step": 10, "source_sat_id": 1},
                   {"release_step": 1, "source_sat_id": True},
                   {"release_step": 1, "source_sat_id": 0},
                   {"release_step": 1, "source_sat_id": 2}]
        for event in invalid:
            with self.subTest(event=event), self.assertRaises(ValueError):
                config_from_dict({**base, "scheduled_task_arrivals": [event]})
        for events in ({}, "events"):
            with self.subTest(events=events), self.assertRaises(ValueError):
                config_from_dict({**base, "scheduled_task_arrivals": events})
        for rate in (0.1, float("nan")):
            with self.subTest(rate=rate), self.assertRaises(ValueError):
                config_from_dict({**base, "task_arrival_rate": rate, "scheduled_task_arrivals": []})
        self.assertEqual(config_from_dict({**base, "scheduled_task_arrivals": []}).scheduled_task_arrivals, [])
        with self.assertRaises(ValueError):
            config_from_dict({"sim_steps": 10, "task_arrival_rate": 0,
                              "scheduled_task_arrivals": [{"release_step": 10, "source_sat_id": 0}]})

    def test_schedule_uses_physical_steps_and_preserves_same_step_order(self):
        events = [{"release_step": 3, "source_sat_id": 1},
                  {"release_step": 1, "source_sat_id": 1},
                  {"release_step": 1, "source_sat_id": 0}]
        env = self.env(dt=0.5, sim_steps=8, task_arrival_end_step=4,
                       deadline_steps=2, scheduled_task_arrivals=events)
        source_rng = env.py_rng.getstate()
        for step in range(8):
            env._task_arrivals(step)
        self.assertEqual([(task.task_id, task.source_sat_id, task.release_time, task.deadline)
                          for task in env.tasks.values()],
                         [("task_0", 1, 1, 3), ("task_1", 0, 1, 3), ("task_2", 1, 3, 5)])
        self.assertEqual(env.py_rng.getstate(), source_rng)
        summary = env.summary()
        self.assertEqual([row["release_seconds"] for row in summary["task_outcomes"]], [0.5, 0.5, 1.5])
        self.assertEqual(summary["experiment"]["config"]["scheduled_task_arrivals"], events)

    def test_all_pipelines_receive_identical_scheduled_task_trace(self):
        traces = []
        for mode in ("distributed", "ground_compute", "ground_tiles", "ground_relay"):
            env = self.env(pipeline_mode=mode, scheduled_task_arrivals=[
                {"release_step": 0, "source_sat_id": 0},
                {"release_step": 2, "source_sat_id": 1}])
            for step in range(3):
                env._task_arrivals(step)
            traces.append(env.summary()["experiment"]["task_trace_sha256"])
            self.assertEqual(env.metrics.total_tasks, 2)
            self.assertEqual(env.metrics.total_tiles, 4)
        self.assertEqual(len(set(traces)), 1)

    def test_legacy_poisson_keeps_rng_draw_order(self):
        env = self.env(seed=29, task_arrival_rate=0.8, task_source_sat_ids=[1, 0])
        rng, source_rng = np.random.default_rng(29), random.Random(29)
        expected = []
        for step in range(20):
            for _ in range(int(rng.poisson(0.8))):
                source = [1, 0][source_rng.randrange(2)]
                sizes = np.maximum(0.01, 1 + rng.normal(0, 0.02, 2))
                sizes *= 2.0 / sizes.sum()
                expected.append((step, source, sizes.tolist()))
            env._task_arrivals(step)
        actual = [(task.release_time, task.source_sat_id,
                   [env.tiles[tid].data_size_mb for tid in task.tile_ids]) for task in env.tasks.values()]
        self.assertEqual(actual, expected)
        self.assertEqual(env.py_rng.getstate(), source_rng.getstate())
        self.assertEqual(env.rng.bit_generator.state, rng.bit_generator.state)


class LargeTaskDecisionTests(unittest.TestCase):
    def env(self, **changes):
        env = make_env(**changes)
        self.addCleanup(env.close)
        return env

    def test_batch_limit_is_validated_and_preserves_per_satellite_fifo(self):
        for limit in (0, -1, True, 4.5, "64"):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                config_from_dict({"decision_tile_limit_per_sat": limit})
        env = self.env(decision_tile_limit_per_sat=64)
        older = add_task(env, count=70)
        env.time = 1
        newer = add_task(env, count=5)
        remote = add_task(env, src=1, count=6)
        state = env.export_state()
        limited = decision_state(state, 64)
        self.assertEqual(list(limited.tiles), [t.tile_id for t in older[:64] + remote])
        self.assertEqual(candidate_tiles(state, 0), [t.tile_id for t in older[:4]])
        self.assertFalse(set(t.tile_id for t in newer) & set(limited.tiles))
        self.assertIs(limited.pending_task_tiles, state.pending_task_tiles)
        self.assertEqual(env.summary()["experiment"]["decision_tile_limit_per_sat"], 64)

    def test_main_passes_configured_limit_to_rules(self):
        env = self.env(sim_steps=3, num_tiles=10, decision_tile_limit_per_sat=64,
                       execution_commitment=True,
                       scheduled_task_arrivals=[{"release_step": 0, "source_sat_id": 0}])
        class Observer:
            def __init__(self):
                self.counts = []

            def on_start(self, _env):
                pass

            def on_step(self, _env, _step, actions):
                self.counts.append(len(actions))

        observer = Observer()
        with patch("main.SimulationEnv", return_value=env):
            main.run_config(env.cfg, "local", observer=observer)
        self.assertEqual(observer.counts, [0, 10, 0])

    def test_marl_rejects_rule_batch_protocol_before_loading_checkpoint(self):
        env = self.env(decision_tile_limit_per_sat=64)
        with self.assertRaisesRegex(ValueError, "decision_tile_limit_per_sat=4"):
            build_batch(env.export_state())
        with patch("main.make_policy") as factory, self.assertRaisesRegex(ValueError, "decision_tile_limit_per_sat=4"):
            main.run_config(env.cfg, "marl", marl_checkpoint="missing-checkpoint.pt")
        factory.assert_not_called()

    def test_sibling_counts_match_reference_across_states_and_locations(self):
        env = self.env()
        tiles = add_task(env, count=1000, size=0.01)
        states = (TileState.QUEUED, TileState.READY, TileState.RUNNING,
                  TileState.COMPUTED, TileState.DONE, TileState.FAILED)
        for index, tile in enumerate(tiles):
            tile.state = states[index % len(states)]
            tile.location = (index // len(states)) % 2
        state = env.export_state()
        for tid, tile in state.tiles.items():
            expected = {}
            for sibling in tiles:
                if sibling.tile_id != tid and sibling.state not in (TileState.DONE, TileState.FAILED):
                    expected[sibling.location] = expected.get(sibling.location, 0) + 1
            self.assertEqual(tile["sibling_locations"], expected)
            self.assertEqual(tile["task_pending_count"], sum(expected.values()) + 1)
        singleton = self.env()
        only = add_task(singleton)[0]
        self.assertEqual(singleton.export_state().tiles[only.tile_id]["sibling_locations"], {})


if __name__ == "__main__":
    unittest.main()
