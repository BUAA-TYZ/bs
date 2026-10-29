import copy
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, replace
import json
import math
import multiprocessing
from pathlib import Path
import tempfile
import unittest

import torch
from torch.nn import functional as F

from analysis.train_episode_equal_bc import (collect_one, digest, episode_equal_loss, fit_equal,
                                           initialize_worker, load_cache, load_checkpoint, validate_dataset)
from sim.config import load_config
from sim.marl.actor import SatActorCritic
from sim.marl.scenarios import ScenarioDistribution, config_digest
from sim.reproducibility import runtime_metadata


class TinyActor(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = torch.nn.Linear(3, 8)

    def forward(self, obs):
        return self.linear(obs).reshape(-1, 4, 2), None


def examples():
    obs = torch.tensor([[1., 0., 0.], [0., 1., 0.], [0., 0., 1.]])
    tiles = torch.tensor([[True, False, False, False], [True, True, True, True], [True, True, False, False]])
    masks = torch.ones(3, 4, 2, dtype=torch.bool)
    actions = torch.tensor([[1, 0, 0, 0], [0, 0, 1, 1], [0, 1, 0, 0]])
    return obs, tiles, masks, actions


class EpisodeEqualBCTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def test_row_minibatch_estimator_matches_episode_label_objective_and_gradient(self):
        obs, tiles, masks, actions = examples()
        logits = torch.arange(24, dtype=torch.float32).reshape(3, 4, 2).div(9).requires_grad_()
        episodes, counts = torch.tensor([0, 1, 1]), torch.tensor([1, 6])
        actual = episode_equal_loss(logits, tiles, masks, actions, episodes, counts, 3)
        expected = (F.cross_entropy(logits[:1][tiles[:1]], actions[:1][tiles[:1]]) +
                    F.cross_entropy(logits[1:][tiles[1:]], actions[1:][tiles[1:]])) / 2
        self.assertTrue(torch.allclose(actual, expected))
        singleton_mean = sum(episode_equal_loss(logits[i:i+1], tiles[i:i+1], masks[i:i+1], actions[i:i+1],
                                                episodes[i:i+1], counts, 3) for i in range(3)) / 3
        self.assertTrue(torch.allclose(singleton_mean, expected))
        gradient = torch.autograd.grad(singleton_mean, logits, retain_graph=True)[0]
        expected_gradient = torch.autograd.grad(expected, logits)[0]
        self.assertTrue(torch.allclose(gradient, expected_gradient))
        pooled = F.cross_entropy(logits[tiles], actions[tiles])
        self.assertFalse(torch.allclose(actual, pooled))

    def test_duplicating_one_episode_does_not_change_its_objective_weight(self):
        _, tiles, masks, actions = examples()
        logits = torch.arange(24, dtype=torch.float32).reshape(3, 4, 2).div(7)
        original = episode_equal_loss(logits, tiles, masks, actions, torch.tensor([0, 1, 1]), torch.tensor([1, 6]), 3)
        select = torch.tensor([0, 1, 2, 1, 2])
        duplicated = episode_equal_loss(logits[select], tiles[select], masks[select], actions[select],
                                         torch.tensor([0, 1, 1, 1, 1]), torch.tensor([1, 12]), 5)
        self.assertTrue(torch.allclose(original, duplicated))

    def test_fit_keeps_exact_update_budget_and_rejects_infeasible_labels(self):
        data = examples()
        self.assertEqual(validate_dataset(data, 3, {"0": 3, "1": 4}), (3, 7))
        invalid = copy.deepcopy(data)
        invalid[2][0, 0, 1] = False
        with self.assertRaisesRegex(ValueError, "action masks"):
            validate_dataset(invalid)
        datasets = [tuple(tensor[:1] for tensor in data), tuple(tensor[1:] for tensor in data)]
        fit = fit_equal(TinyActor(), datasets, seed=1, lr=.001, epochs=2, minibatch=2, expected_updates=4)
        self.assertEqual(fit["optimizer_updates"], 4)
        self.assertEqual([row["tile_labels"] for row in fit["per_episode"]], [1, 6])
        self.assertEqual(fit["class_counts"], {"0": 3, "1": 4})
        with self.assertRaisesRegex(ValueError, "budget differs"):
            fit_equal(TinyActor(), datasets, seed=1, lr=.001, epochs=2, minibatch=2, expected_updates=5)

    def test_one_worker_can_handle_multiple_tasks_with_single_thread_initialization(self):
        with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"),
                                 initializer=initialize_worker) as pool:
            self.assertEqual(pool.submit(torch.get_num_interop_threads).result(), 1)
            self.assertEqual(pool.submit(torch.get_num_interop_threads).result(), 1)

    def test_real_collection_cache_fit_and_checkpoint_roundtrip(self):
        root = Path(__file__).resolve().parents[1]
        distribution = ScenarioDistribution(root / "examples/overlap_proxy/distribution.json",
                                            load_config(str(root / "examples/overlap_proxy/config.json")))
        cfg, case = distribution.training_case(1701, 1)
        cfg = replace(cfg, sim_steps=20, task_arrival_end_step=1, task_arrival_rate=10)
        case = {**case, "id": "temporary_smoke", "config_sha256": config_digest(cfg)}
        model_config = {"num_gs": 2, "hidden": 64, "feature_set": "overlap_contact", "encoder": "task_attention"}
        model = SatActorCritic(**model_config)
        runtime = runtime_metadata()
        initial = {**runtime, "observation_version": 3, "model_config": model_config,
                   "config": asdict(cfg), "model_state": model.state_dict(), "episode": 0}
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            initial_path = directory / "initial.pt"
            torch.save(initial, initial_path)
            initial_sha = digest(initial_path.read_bytes())
            expected = {"source_sha256": runtime["source_sha256"], "initial_sha256": initial_sha,
                        "distribution_sha256": distribution.sha256, "config_sha256": config_digest(cfg),
                        "teacher": "cgs_compute_adapted"}
            with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"),
                                     initializer=initialize_worker) as pool:
                entry = pool.submit(collect_one, cfg, case, str(initial_path), initial_sha,
                                    str(directory / "episode_001.pt"), "cgs_compute_adapted").result()
            (directory / "index.json").write_text(json.dumps({**expected, "episodes": [entry]}))
            datasets, records, _ = load_cache(directory, expected, [(cfg, case)])
            self.assertGreater(len(records[0]["metrics"]["task_outcomes"]), 0)
            updates = math.ceil(len(datasets[0][0]) / 512)
            fitted = fit_equal(model, datasets, seed=1701, lr=.001, epochs=1, minibatch=512,
                               expected_updates=updates)
            final_path = directory / "teacher_bc.pt"
            torch.save({**initial, "training_stage": "episode_equal_bc", "fit": fitted,
                        "model_state": model.state_dict()}, final_path)
            loaded, _ = load_checkpoint(final_path)
            model.load_state_dict(loaded["model_state"])
            self.assertEqual(loaded["fit"]["optimizer_updates"], updates)


if __name__ == "__main__":
    unittest.main()
