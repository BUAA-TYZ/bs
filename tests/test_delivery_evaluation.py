from dataclasses import asdict, replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import evaluate_policies
from main import run_config
from sim.entities import Action, ActionType, TileState
from sim.evaluation import paired_task_metrics, task_age_delivery_curve
from test_simulation import FakeTopology, FakeVisibility, add_task, make_env


class DeliveryTests(unittest.TestCase):
    def test_both_modes_reward_only_after_aggregation(self):
        for mode in ("distributed", "ground_compute"):
            with self.subTest(mode=mode):
                env = make_env(pipeline_mode=mode, ground_relay_delay_s=1, aggregation_delay_s=2)
                self.addCleanup(env.close)
                tile = add_task(env)[0]
                env.step([Action(tile.tile_id, ActionType.LOCAL)])
                env.step([])
                self.assertEqual(tile.state, TileState.DONE)
                self.assertEqual(env.metrics.completed_tasks, 0)
                self.assertEqual(env.tasks[tile.parent_task_id].results_ready_time, 2)
                for _ in range(2):
                    self.assertEqual(env.step([]).reward, 0)
                self.assertEqual(env.summary()["overall"]["aggregating_tasks"], 1)
                self.assertGreater(env.step([]).reward, 0)
                row = env.summary()["task_outcomes"][0]
                self.assertEqual((row["status"], row["latency_seconds"]), ("completed", 5))
                env._check_task_done(tile.parent_task_id)
                env.step([])
                self.assertEqual(env.metrics.completed_tasks, 1)

    def test_aggregation_deadline_and_exact_boundary(self):
        for deadline, completed in ((4, False), (5, True)):
            with self.subTest(deadline=deadline):
                env = make_env(deadline_steps=deadline, aggregation_delay_s=3)
                self.addCleanup(env.close)
                tile = add_task(env, deadline=deadline)[0]
                env.step([Action(tile.tile_id, ActionType.LOCAL)])
                for _ in range(6):
                    env.step([])
                summary = env.summary()
                self.assertEqual(summary["overall"]["completed_tasks"], int(completed))
                self.assertEqual(summary["overall"]["failed_tasks"], int(not completed))
                self.assertEqual(summary["overall"]["pending_tasks"], 0)
                self.assertEqual(summary["overall"]["completed_tiles"], 1)
                self.assertEqual(summary["overall"]["failed_tiles"], 0)
                if not completed:
                    self.assertEqual(summary["task_outcomes"][0]["failure_reason"], "aggregation_deadline_miss")

    def test_delay_rounding_and_horizon_censoring(self):
        env = make_env(dt=5, aggregation_delay_s=6, ground_relay_delay_s=1)
        self.addCleanup(env.close)
        tile = add_task(env)[0]
        env.step([Action(tile.tile_id, ActionType.LOCAL)])
        env.step([])
        env.step([])
        row = env.summary()["task_outcomes"][0]
        self.assertEqual(row["status"], "pending")
        self.assertIsNone(row["latency_seconds"])
        self.assertIsNone(row["completion_seconds"])
        self.assertEqual(row["observed_age_seconds"], 15)
        env.step([])
        self.assertEqual(env.summary()["task_outcomes"][0]["latency_seconds"], 20)

    def test_last_tile_controls_aggregation_start(self):
        env = make_env(aggregation_delay_s=2)
        self.addCleanup(env.close)
        tiles = add_task(env, count=2)
        env.step([Action(tiles[0].tile_id, ActionType.LOCAL)])
        env.step([])
        self.assertIsNone(env.tasks[tiles[0].parent_task_id].results_ready_time)
        env.step([Action(tiles[1].tile_id, ActionType.LOCAL)])
        env.step([])
        self.assertEqual(env.tasks[tiles[0].parent_task_id].results_ready_time, 4)
        env.step([])
        self.assertEqual(env.metrics.completed_tasks, 0)
        env.step([])
        self.assertEqual(env.metrics.task_latencies, [6])

    def test_arrivals_stop_and_config_rejects_invalid_delays(self):
        env = make_env(task_arrival_rate=3, task_arrival_end_step=2)
        self.addCleanup(env.close)
        env.step([])
        env.step([])
        count = len(env.tasks)
        self.assertGreater(count, 0)
        for _ in range(10):
            env.step([])
        self.assertEqual(len(env.tasks), count)
        for value in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                replace(env.cfg, aggregation_delay_s=value)
        with self.assertRaises(ValueError):
            replace(env.cfg, task_arrival_end_step=-1)

    def test_ground_mode_does_not_construct_or_consult_actor(self):
        env = make_env(pipeline_mode="ground_compute", sim_steps=3)
        self.addCleanup(env.close)
        with patch("main.make_policy", side_effect=AssertionError("Actor must not run")), \
                patch("main.SimulationEnv", return_value=env):
            result = run_config(env.cfg, "marl", "nonexistent.pt")
        self.assertEqual(result["experiment"]["policy"], "ground_compute")
        self.assertIsNone(result["experiment"]["checkpoint"])


class EvaluationTests(unittest.TestCase):
    @staticmethod
    def row(tid, latency=None, status="pending"):
        return {"task_id": tid, "source_sat_id": 0, "release_seconds": 0,
                "deadline_seconds": None, "latency_seconds": latency, "status": status}

    def test_pairing_reports_unmatched_completions_and_latency(self):
        ref = [self.row("a", 10, "completed"), self.row("b", 20, "completed"), self.row("c")]
        cand = [self.row("c", 1, "completed"), self.row("a", 5, "completed"), self.row("b", status="failed")]
        result = paired_task_metrics(ref, cand)
        self.assertEqual(result["common_completed_tasks"], 1)
        self.assertEqual(result["reference_only_completed_tasks"], 1)
        self.assertEqual(result["candidate_only_completed_tasks"], 1)
        self.assertEqual(result["candidate_failed_tasks"], 1)
        self.assertEqual(result["candidate_minus_reference_latency_mean_seconds"], -5)
        self.assertEqual(result["ratio_of_mean_latencies_reference_over_candidate"], 2)
        self.assertEqual(result["candidate_latency_mean_seconds"], 5)

    def test_empty_pairs_and_mismatched_tasks(self):
        result = paired_task_metrics([self.row("a")], [self.row("a")])
        self.assertIsNone(result["candidate_minus_reference_latency_mean_seconds"])
        self.assertIsNone(result["ratio_of_mean_latencies_reference_over_candidate"])
        self.assertEqual(result["neither_completed_tasks"], 1)
        with self.assertRaises(ValueError):
            paired_task_metrics([self.row("a")], [self.row("b")])
        with self.assertRaises(ValueError):
            paired_task_metrics([self.row("a")], [self.row("a"), self.row("a")])
        with self.assertRaises(ValueError):
            paired_task_metrics([self.row("a")], [{**self.row("a"), "release_seconds": 1}])

    def test_age_curve_uses_relative_time_and_includes_all_arrivals(self):
        rows = [self.row("early", 10, "completed"),
                {**self.row("late", 10, "completed"), "release_seconds": 90},
                self.row("slow", 11, "completed"), self.row("pending"),
                self.row("failed", status="failed")]
        curve = task_age_delivery_curve(rows, [5, 10, 20], 120)
        self.assertEqual([r["delivered_within_horizon"] for r in curve], [0, 2, 3])
        self.assertEqual([r["total_tasks"] for r in curve], [5, 5, 5])
        self.assertEqual(curve[1]["delivery_rate"], .4)
        self.assertEqual(curve[1]["not_delivered_within_horizon"], 3)

    def test_age_curve_rejects_insufficient_follow_up_and_invalid_outcomes(self):
        with self.assertRaisesRegex(ValueError, "Insufficient follow-up"):
            task_age_delivery_curve([{**self.row("late"), "release_seconds": 91}], [10], 100)
        for rows in ([self.row("a"), self.row("a")], [self.row("a", -1, "completed")],
                     [self.row("a", 101, "completed")], [self.row("a", 1, "pending")],
                     [self.row("a", status="unknown")], [self.row("a", None, "completed")]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                task_age_delivery_curve(rows, [10], 100)
        for horizons in ([], [0], [float("nan")], [float("inf")], [10, 10], [20, 10]):
            with self.subTest(horizons=horizons), self.assertRaises(ValueError):
                task_age_delivery_curve([], horizons, 100)

    def test_age_curve_empty_cohort_has_no_completion_rate(self):
        result = task_age_delivery_curve([], [10], 100)[0]
        self.assertIsNone(result["delivery_rate"])
        self.assertEqual(result["total_tasks"], 0)

    def test_cli_compares_modes_with_identical_trace_and_drain(self):
        env = make_env(sim_steps=12, task_arrival_rate=0.5)
        self.addCleanup(env.close)
        with tempfile.TemporaryDirectory() as tmp:
            config, output = Path(tmp) / "config.json", Path(tmp) / "result.json"
            config.write_text(json.dumps(asdict(env.cfg)))
            args = ["evaluate_policies.py", "--config", str(config), "--output", str(output),
                    "--policies", "ground_compute,local", "--seeds", "5,6", "--drain-steps", "10",
                    "--aggregation-delay-s", "2", "--delivery-horizons-minutes", "0.1"]
            with patch("sys.argv", args), patch("sim.env.TopologyModel", FakeTopology), \
                    patch("sim.env.VisibilityCache", FakeVisibility), patch("builtins.print"):
                evaluate_policies.main()
            report = json.loads(output.read_text())
            self.assertEqual(len(report["paired_task_comparisons"]), 2)
            for seed in (5, 6):
                runs = [r["metrics"] for r in report["runs"] if r["seed"] == seed]
                self.assertEqual(runs[0]["experiment"]["task_trace_sha256"], runs[1]["experiment"]["task_trace_sha256"])
                self.assertEqual({r["experiment"]["config"]["pipeline_mode"] for r in runs},
                                 {"distributed", "ground_compute"})
                for run in runs:
                    self.assertEqual(run["task_age_delivery_curve"][0]["horizon_seconds"], 6)
                    self.assertEqual(run["task_age_delivery_curve"][0]["total_tasks"],
                                     run["overall"]["total_tasks"])
                    self.assertEqual(run["experiment"]["config"]["sim_steps"], 22)
                    self.assertEqual(run["experiment"]["config"]["task_arrival_end_step"], 12)
                    self.assertTrue(all(r["release_seconds"] < 12 for r in run["task_outcomes"]))
