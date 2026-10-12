from dataclasses import asdict
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import torch

from sim.marl.actor import SatActorCritic
from sim.marl.imitation import collect_demonstrations, merge_demonstrations, fit_demonstrations
from train_marl import parser, train
from test_simulation import make_env, FakeTopology, FakeVisibility


class ImitationTests(unittest.TestCase):
    def test_dagger_uses_student_states_but_teacher_labels_and_replays_both(self):
        torch.set_num_threads(1)
        env = make_env(sim_steps=12, task_arrival_end_step=3, task_arrival_rate=3)
        self.addCleanup(env.close)
        model = SatActorCritic(num_gs=len(env.cfg.ground_stations), hidden=8)
        with torch.no_grad():
            model.policy_head.weight.zero_()
            model.policy_head.bias.zero_()
            model.policy_head.bias.reshape(4, -1)[:, 0] = 100
        with patch("sim.env.TopologyModel", FakeTopology), patch("sim.env.VisibilityCache", FakeVisibility):
            expert, a = collect_demonstrations(model, env.cfg, "cpu", "greedy")
            student, b = collect_demonstrations(model, env.cfg, "cpu", "greedy", True)
        self.assertGreater(a["samples"], 0)
        self.assertGreater(b["samples"], a["samples"])
        self.assertEqual(set(b["prediction_counts"]), {0})
        self.assertNotIn(0, b["action_counts"])
        self.assertEqual(b["agreement"], 0)
        combined = merge_demonstrations([expert, None, student])
        self.assertEqual(len(combined[0]), len(expert[0]) + len(student[0]))
        self.assertTrue(torch.equal(combined[-1][-len(student[0]):], student[-1]))
        result = fit_demonstrations(model, torch.optim.Adam(model.parameters()), combined, "cpu", 1, 16)
        self.assertTrue(0 <= result["agreement"] <= 1)

    def test_pooled_training_records_distinct_dagger_seeds_and_preserves_checkpoint(self):
        env = make_env(sim_steps=6, task_arrival_end_step=2, task_arrival_rate=2)
        self.addCleanup(env.close)
        with tempfile.TemporaryDirectory() as tmp:
            config, out = Path(tmp) / "config.json", Path(tmp) / "run"
            config.write_text(json.dumps(asdict(env.cfg)))
            args = parser().parse_args(["--config", str(config), "--out-dir", str(out), "--episodes", "1",
                "--seed", "7", "--hidden", "8", "--ppo-epochs", "1", "--validation-seeds", "9001",
                "--warmstart-teacher", "greedy", "--warmstart-mode", "pooled", "--warmstart-episodes", "2",
                "--dagger-episodes", "1", "--warmstart-epochs", "1"])
            with patch("sim.env.TopologyModel", FakeTopology), patch("sim.env.VisibilityCache", FakeVisibility), \
                    patch("builtins.print"):
                train(args)
            warm = json.loads((out / "warmstart.json").read_text())
            self.assertEqual([r["seed"] for r in warm["episodes"]], [8, 9, 10])
            self.assertEqual([r["rollout"] for r in warm["episodes"]], ["teacher", "teacher", "learner"])
            self.assertEqual(len(warm["pooled_fits"]), 2)
            self.assertTrue((out / "teacher_bc.pt").exists())
            self.assertEqual(json.loads((out / "run.json").read_text())["warmstart_environment_steps"], 18)
            fresh = deepcopy(args)
            fresh.out_dir = str(Path(tmp) / "initialized")
            fresh.warmstart_episodes = fresh.dagger_episodes = 0
            fresh.warmstart_mode = "sequential"
            fresh.initialize_checkpoint = str(out / "teacher_bc.pt")
            with patch("sim.env.TopologyModel", FakeTopology), patch("sim.env.VisibilityCache", FakeVisibility), \
                    patch("builtins.print"):
                train(fresh)
            initial = torch.load(Path(fresh.out_dir) / "initial.pt", weights_only=True)
            parent = torch.load(out / "teacher_bc.pt", weights_only=True)
            self.assertTrue(all(torch.equal(tensor, parent["model_state"][key])
                                for key, tensor in initial["model_state"].items()))
            self.assertEqual(initial["initialization"]["optimizer"], "fresh Adam")
            fresh.resume = True
            with self.assertRaisesRegex(ValueError, "cannot be combined"):
                train(fresh)
