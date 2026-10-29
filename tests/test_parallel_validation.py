from contextlib import redirect_stdout
from dataclasses import asdict, replace
import io
import json
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import torch

from analysis.resume_parallel_validation import inspect_resume, parallel_evaluate, resume_training, validation_digest
from sim.config import load_config
from sim.marl.actor import SatActorCritic
from sim.marl.scenarios import ScenarioDistribution
import train_marl


ROOT = Path(__file__).resolve().parents[1]


def small_config():
    base = load_config(str(ROOT / "examples/overlap_proxy/config.json"))
    cfg, _ = ScenarioDistribution(ROOT / "examples/overlap_proxy/distribution.json", base).cases("validation")[0]
    topology = {**cfg.topology, "tle_lines": cfg.topology["tle_lines"][:2], "window_lookahead_s": 20,
                "contact_scan_s": 5, "contact_update_s": 5}
    return replace(cfg, num_sats=2, satellite_compute_rates=[1.0, 1.0], task_source_sat_ids=[0],
                   sim_steps=20, task_arrival_end_step=10, task_arrival_rate=.5, topology=topology,
                   ground_stations=[{**station, "min_elevation_deg": -90} for station in cfg.ground_stations],
                   image_tiling={**cfg.image_tiling, "width_px": 1024, "height_px": 1024})


def assert_nested_equal(test, left, right):
    if isinstance(left, torch.Tensor):
        test.assertTrue(torch.equal(left, right))
    elif isinstance(left, dict):
        test.assertEqual(left.keys(), right.keys())
        for key in left:
            assert_nested_equal(test, left[key], right[key])
    elif isinstance(left, (list, tuple)):
        test.assertEqual(len(left), len(right))
        for a, b in zip(left, right):
            assert_nested_equal(test, a, b)
    else:
        test.assertEqual(left, right)


class ParallelValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def test_spawn_results_equal_serial_and_preserve_parent_rng(self):
        cfg = small_config()
        model_config = {"num_gs": len(cfg.ground_stations), "hidden": 16, "critic_dim": 0,
                        "feature_set": "overlap_contact", "encoder": "task_attention"}
        torch.manual_seed(444)
        model = SatActorCritic(**model_config)
        cases = [(replace(cfg, seed=seed), {"id": f"small_{seed}", "seed": seed}) for seed in (771, 772)]
        before = train_marl.rng_state()
        expected = train_marl.evaluate(model, cfg, [771, 772], "cpu", cases, [5, 10])
        assert_nested_equal(self, before, train_marl.rng_state())
        for workers in (1, 2):
            with self.subTest(workers=workers):
                audits = []
                actual = parallel_evaluate(model, cfg, [771, 772], "cpu", cases, [5, 10],
                                           workers=workers, model_config=model_config, audit=audits.append)
                self.assertEqual(expected, actual)
                assert_nested_equal(self, before, train_marl.rng_state())
                self.assertEqual(audits[0]["case_ids"], ["small_771", "small_772"])
                if workers == 1:
                    self.assertEqual(len(set(audits[0]["worker_pids"])), 1)
                self.assertFalse(model.training)
        self.assertTrue(expected["cases"][0]["action_counts"])

    def test_failure_also_preserves_parent_rng(self):
        cfg = small_config()
        config = {"num_gs": len(cfg.ground_stations), "hidden": 16, "feature_set": "overlap_contact"}
        model = SatActorCritic(**config)
        before = train_marl.rng_state()

        def fail_guard():
            random.random()
            np.random.random(10)
            torch.rand(10)
            raise ValueError("Changed inputs")

        with self.assertRaisesRegex(ValueError, "Changed inputs"):
            parallel_evaluate(model, cfg, [771], "cpu", workers=1, model_config=config, guard=fail_guard)
        assert_nested_equal(self, before, train_marl.rng_state())

    def test_validation_hash_survives_integer_key_json_roundtrip(self):
        validation = {"cases": [{"action_counts": {2: 3, 10: 4, 1: 5}}]}
        self.assertEqual(validation_digest(validation), validation_digest(json.loads(json.dumps(validation))))

    def test_exact_cpu_resume_matches_uninterrupted_training_and_records_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = root / "config.json"
            config.write_text(json.dumps(asdict(small_config())), encoding="utf-8")

            def arguments(directory):
                return train_marl.parser().parse_args([
                    "--config", str(config), "--out-dir", str(directory), "--features", "overlap_contact",
                    "--encoder", "task_attention", "--hidden", "16", "--episodes", "2", "--rollout-steps", "20",
                    "--ppo-epochs", "1", "--minibatch", "40", "--eval-every", "2", "--seed", "177",
                    "--validation-seeds", "771,772", "--reward-mode", "task", "--gamma", "1",
                    "--delivery-horizons-minutes", "0.08333333333333333,0.16666666666666666"])

            with redirect_stdout(io.StringIO()):
                train_marl.train(arguments(root / "full"))
            original_update = train_marl.ppo_update
            calls = 0

            def interrupted_update(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError("test interruption after episode 1")
                return original_update(*args, **kwargs)

            training = root / "continued"
            with patch.object(train_marl, "ppo_update", interrupted_update), redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "test interruption"):
                    train_marl.train(arguments(training))
            _, _, _, saved, resume_args, _, guard = inspect_resume(training)
            self.assertEqual(saved["episode"], 1)
            self.assertTrue(resume_args.resume)
            guard()
            checkpoint_bytes = (training / "last.pt").read_bytes()
            changed = dict(saved)
            changed["episode"] = 2
            torch.save(changed, training / "last.pt")
            with self.assertRaisesRegex(ValueError, "1..episodes-1"):
                inspect_resume(training)
            (training / "last.pt").write_bytes(checkpoint_bytes)
            log_bytes = (training / "train_log.jsonl").read_bytes()
            (training / "train_log.jsonl").write_bytes(b"")
            with self.assertRaisesRegex(ValueError, "log does not end"):
                inspect_resume(training)
            (training / "train_log.jsonl").write_bytes(log_bytes)
            config_bytes = config.read_bytes()
            config.write_bytes(config_bytes + b" ")
            with self.assertRaisesRegex(ValueError, "changed during"):
                guard()
            config.write_bytes(config_bytes)
            with redirect_stdout(io.StringIO()):
                sidecar = resume_training(training, 2, command=["test", "--workers", "2"])
            full = torch.load(root / "full/last.pt", weights_only=True)
            continued = torch.load(training / "last.pt", weights_only=True)
            for key in ("model_state", "optimizer_state", "rng_state", "validation", "best_score"):
                assert_nested_equal(self, full[key], continued[key])
            self.assertEqual(sidecar["status"], "completed")
            self.assertEqual(sidecar["starting_episode"], 1)
            self.assertEqual(sidecar["episode_budget"], 2)
            self.assertEqual(sidecar["command"], ["test", "--workers", "2"])
            self.assertEqual(len(sidecar["validation_calls"]), 1)
            self.assertFalse(sidecar["original_training_args"]["resume"])
            self.assertTrue(sidecar["resumed_training_args"]["resume"])
            source = training / "resume_parallel_validation_source"
            self.assertEqual((source / "last.pt").read_bytes(), checkpoint_bytes)
            self.assertEqual((source / "train_log.jsonl").read_bytes(), log_bytes)
            self.assertEqual((source / "wrapper_source.py").read_bytes(),
                             Path(sidecar["wrapper"]["path"]).read_bytes())
            self.assertIs(train_marl.evaluate, __import__("analysis.resume_parallel_validation", fromlist=["ORIGINAL_EVALUATE"]).ORIGINAL_EVALUATE)


if __name__ == "__main__":
    unittest.main()
