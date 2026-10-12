from types import SimpleNamespace
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from sim.evaluation import task_age_score
from sim.marl.reward import delivered_task_reward
from train_marl import validation_score, parser, train
from test_simulation import make_env, FakeTopology, FakeVisibility


class TaskAgeTrainingTests(unittest.TestCase):
    def test_reward_matches_delivery_curve_including_late_and_pending_tasks(self):
        horizons = [10, 20, 30]
        tasks = {}
        rows = []
        for i, (release, latency) in enumerate(((0, 10), (5, 20), (10, 35), (0, None))):
            tasks[i] = SimpleNamespace(release_time=release,
                                       completion_time=None if latency is None else release + latency)
            rows.append(dict(task_id=i, release_seconds=release,
                             latency_seconds=latency, status="pending" if latency is None else "completed"))
        env = SimpleNamespace(tasks=tasks, cfg=SimpleNamespace(dt=1), time=0)
        reward = 0
        for t in range(1, 51):
            env.time = t
            reward += delivered_task_reward(env, horizons)
        score = task_age_score(rows, horizons, 50)
        self.assertAlmostEqual(reward / len(tasks), score["delivery_score"])
        self.assertAlmostEqual(score["restricted_mean_latency_seconds"], 22.5)

    def test_new_selection_prioritizes_age_score_over_eventual_completion(self):
        fast = dict(delivery_score=.7, restricted_mean_latency_seconds=100,
                    task_completion_rate=.8, task_mean_seconds=90)
        late = dict(delivery_score=.5, restricted_mean_latency_seconds=200,
                    task_completion_rate=1, task_mean_seconds=190)
        self.assertGreater(validation_score(fast, [10, 20]), validation_score(late, [10, 20]))
        self.assertLess(validation_score(fast), validation_score(late))

    def test_inconsistent_training_protocol_fails_before_writing(self):
        args = parser().parse_args(["--config", "examples/config_relay.yaml", "--out-dir", "unused",
                                   "--delivery-horizons-minutes", "30,60,120,240", "--reward-mode", "task"])
        with self.assertRaisesRegex(ValueError, "gamma=1"):
            train(args)
        args.gamma = 1
        args.delivery_horizons_minutes = "300"
        with self.assertRaisesRegex(ValueError, "follow-up"):
            train(args)

    def test_training_persists_age_score_and_reward_matches_actual_deliveries(self):
        env = make_env(sim_steps=20, task_arrival_end_step=5, task_arrival_rate=2)
        self.addCleanup(env.close)
        with tempfile.TemporaryDirectory() as tmp:
            config, out = Path(tmp) / "config.json", Path(tmp) / "train"
            config.write_text(json.dumps(asdict(env.cfg)))
            args = parser().parse_args([
                "--config", str(config), "--out-dir", str(out), "--episodes", "2",
                "--hidden", "8", "--ppo-epochs", "1", "--eval-every", "1",
                "--validation-seeds", "9001", "--reward-mode", "task", "--gamma", "1",
                "--delivery-horizons-minutes", "0.1,0.2"])
            with patch("sim.env.TopologyModel", FakeTopology), \
                    patch("sim.env.VisibilityCache", FakeVisibility), patch("builtins.print"):
                train(args)
            records = [json.loads(line) for line in (out / "train_log.jsonl").read_text().splitlines()]
            for row in records:
                self.assertGreater(row["total_tasks"], 0)
                self.assertAlmostEqual(row["ep_reward"], row["task_age"]["delivery_score"] * row["total_tasks"])
                self.assertIn("delivery_score", row["validation"])
            self.assertEqual(json.loads((out / "run.json").read_text())["delivery_horizons_seconds"], [6, 12])
