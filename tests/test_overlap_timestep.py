from dataclasses import replace
import unittest

from analysis.check_overlap_timestep import (ReplayObserver, actual_task_trace, arrival_events,
                                             expected_task_trace, rescale_config, steps)
from sim.config import load_config
from sim.env import SimulationEnv
from test_simulation import make_env


class OverlapTimestepTests(unittest.TestCase):
    def config(self):
        cfg = load_config("examples/workload_overlap.json")
        return replace(cfg, seed=71, num_sats=2, dt=1, sim_steps=60,
                       task_arrival_end_step=20, task_arrival_rate=0.5,
                       decision_interval_steps=1, topology_update_steps=10,
                       ground_visibility_update_steps=10, compute_service_mode="work_conserving",
                       topology={"bandwidth_period": 600, "bandwidth_noise": 0, "contact_scan_s": 30},
                       task_source_sat_ids=[1, 0], deadline_steps=30)

    def env(self, cfg):
        env = make_env(image_tiling=cfg.image_tiling, task_source_sat_ids=cfg.task_source_sat_ids,
                       seed=cfg.seed, dt=cfg.dt, sim_steps=cfg.sim_steps,
                       task_arrival_end_step=cfg.task_arrival_end_step,
                       task_arrival_rate=cfg.task_arrival_rate, deadline_steps=cfg.deadline_steps,
                       mem_capacity_gb=1000)
        self.addCleanup(env.close)
        return env

    def test_rescaling_preserves_physical_periods(self):
        base = self.config()
        for dt in (1, 0.5, 0.25):
            cfg = rescale_config(base, dt)
            self.assertEqual(cfg.sim_steps * dt, 60)
            self.assertEqual(cfg.task_arrival_end_step * dt, 20)
            self.assertEqual(cfg.deadline_steps * dt, 30)
            self.assertEqual(cfg.decision_interval_steps * dt, 1)
            self.assertEqual(cfg.topology_update_steps * dt, 10)
            self.assertEqual(cfg.ground_visibility_update_steps * dt, 10)
            self.assertEqual(cfg.topology["bandwidth_period"] * dt, 600)
            self.assertEqual(cfg.task_arrival_rate / dt, 0.5)
            self.assertEqual(cfg.topology["contact_scan_s"], 30)
        with self.assertRaises(ValueError):
            steps(1, 0.3)

    def test_replay_matches_native_one_second_arrivals_and_all_finer_grids(self):
        base = self.config()
        events = arrival_events(base)
        expected = expected_task_trace(base, events)
        self.assertTrue(events)
        native = self.env(base)
        for step in range(base.sim_steps):
            SimulationEnv._task_arrivals(native, step)
        self.assertEqual(actual_task_trace(native), expected)
        for dt in (1, 0.5, 0.25):
            cfg = rescale_config(base, dt)
            env = self.env(cfg)
            observer = ReplayObserver(cfg, events)
            rng, py_rng = env.rng, env.py_rng
            observer.on_start(env)
            for step in range(cfg.sim_steps):
                env._task_arrivals(step)
                self.assertIs(env.rng, rng)
                self.assertIs(env.py_rng, py_rng)
            self.assertEqual(actual_task_trace(env), expected)

    def test_random_interfaces_are_restored_after_failure(self):
        cfg = self.config()
        events = arrival_events(cfg)
        env = self.env(cfg)
        original_rng, original_py_rng = env.rng, env.py_rng

        def fail(_step):
            raise RuntimeError("injected constructor failure")

        env._task_arrivals = fail
        ReplayObserver(cfg, events).on_start(env)
        with self.assertRaisesRegex(RuntimeError, "injected"):
            env._task_arrivals(int(events[0]["release_seconds"]))
        self.assertIs(env.rng, original_rng)
        self.assertIs(env.py_rng, original_py_rng)

    def test_duplicate_arrival_execution_is_rejected(self):
        cfg = self.config()
        events = arrival_events(cfg)
        env = self.env(cfg)
        ReplayObserver(cfg, events).on_start(env)
        first = int(events[0]["release_seconds"])
        env._task_arrivals(first)
        with self.assertRaisesRegex(AssertionError, "twice"):
            env._task_arrivals(first)


if __name__ == "__main__":
    unittest.main()
