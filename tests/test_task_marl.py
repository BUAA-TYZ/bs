import copy
from pathlib import Path
import tempfile
import unittest

import numpy as np
import torch

from sim.entities import TileState
from sim.marl.actor import SatActorCritic
from sim.marl.decision import OBSERVATION_VERSION, build_batch
from sim.marl.features import TASK_EXTRA_DIM, feature_dim
from sim.marl.observation import MAX_NEIGHBORS, MAX_TILES_PER_SAT, obs_dim
from sim.marl.reward import shaped_task_reward, task_potential
from sim.scheduler.marl_policy import MARLPolicy
from train_marl import critic_observations, parser, ppo_update
from run_marl_study import crossed_interval, aggregate
from analyze_reachability import optimistic_reachability
from test_simulation import add_task, make_env


class TaskMARLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def test_completed_and_failed_siblings_are_distinct(self):
        env = make_env()
        tiles = add_task(env, count=3)
        tiles[1].state = TileState.DONE
        tiles[2].state = TileState.FAILED
        state = env.export_state()
        progress = state.tiles[tiles[0].tile_id]["task_progress"]
        self.assertEqual((progress["done"], progress["failed"], progress["total"]), (1, 1, 3))
        obs = build_batch(state, "task_window")[0]
        self.assertEqual(obs.shape, (2, feature_dim(1, "task_window")))
        self.assertTrue(np.isfinite(obs).all())
        np.testing.assert_allclose(obs[0, -MAX_TILES_PER_SAT * TASK_EXTRA_DIM:][:2], [1 / 3, 1 / 3])

    def test_feature_ablations_remove_all_paths(self):
        env = make_env()
        add_task(env, count=2)
        state = env.export_state()
        changed = copy.deepcopy(state)
        for tile in changed.tiles.values():
            tile["sibling_locations"] = {1: 99}
            tile["task_pending_count"] = 99
            tile["task_progress"] = dict(total=100, done=30, failed=20, running=10, awaiting_delivery=5)
        for sat in changed.satellites.values():
            sat["next_gs_windows"] = {"gs": (7, 18)}
        a = build_batch(state, "task_window", False, False)[0]
        b = build_batch(changed, "task_window", False, False)[0]
        np.testing.assert_array_equal(a, b)
        self.assertFalse(np.array_equal(build_batch(state, "task_window")[0], build_batch(changed, "task_window")[0]))

    def test_actor_neighbor_permutation_equivariance(self):
        torch.manual_seed(1)
        model = SatActorCritic(1, hidden=16, feature_set="task_window", encoder="task_attention")
        obs = torch.rand(2, feature_dim(1, "task_window"))
        nb_start = 9
        tile_start = nb_start + 6 * MAX_NEIGHBORS
        original = obs.clone()
        order = torch.tensor([1, 0, 2, 3, 4, 5, 6, 7])
        obs[:, nb_start:tile_start] = original[:, nb_start:tile_start].reshape(2, 8, 6)[:, order].reshape(2, -1)
        extra_start = obs_dim(1) + 2
        obs[:, extra_start:extra_start + 16] = original[:, extra_start:extra_start + 16].reshape(2, 8, 2)[:, order].reshape(2, -1)
        tail = obs[:, extra_start + 16:].reshape(2, 4, TASK_EXTRA_DIM)
        original_tail = original[:, extra_start + 16:].reshape(2, 4, TASK_EXTRA_DIM)
        tail[:, :, 7:] = original_tail[:, :, 7:][:, :, order]
        logits, _ = model(original)
        swapped, _ = model(obs)
        action_order = torch.tensor([0, 1, 3, 2, 4, 5, 6, 7, 8, 9, 10])
        torch.testing.assert_close(swapped, logits[:, :, action_order])

    def test_potential_telescopes_at_terminal(self):
        gamma, coefficient = 0.9, 2.0
        potentials = [0.0, 0.25, 0.5, 1.0]
        rewards = [0, 0, 1]
        shaped = [shaped_task_reward(rewards[i], potentials[i], potentials[i + 1],
                                     gamma, coefficient, i == 2) for i in range(3)]
        self.assertAlmostEqual(sum(gamma ** i * value for i, value in enumerate(shaped)), gamma ** 2)
        env = make_env()
        tiles = add_task(env, count=2)
        tiles[0].state = TileState.DONE
        self.assertEqual(task_potential(env), 0.25)
        tiles[1].state = TileState.FAILED
        self.assertEqual(task_potential(env), 0.0)

    def test_crossed_intervals_preserve_paired_difference(self):
        self.assertEqual(crossed_interval(np.full((5, 3), 0.25)), [0.25, 0.25])
        self.assertIsNone(crossed_interval(np.zeros((1, 3))))
        records = []
        for label in ("greedy", "feature_mlp", "task_attention"):
            for seed in ([None] if label == "greedy" else [7, 107]):
                for case in [1, 2]:
                    rate = case / 10 + (0.2 if label == "task_attention" else 0)
                    records.append(dict(label=label, training_seed=seed, test_seed=case,
                                        metrics={"overall": {"task_completion_rate": rate, "completed_tasks": 0},
                                                 "latency": {"task_mean": None}}))
        result = aggregate(records, ["greedy", "feature_mlp", "task_attention"], [7, 107], [1, 2])
        self.assertAlmostEqual(result["task_attention"]["structure_delta_vs_feature_mlp"]["mean"], 0.2)

    def test_temporal_reachability_allows_buffering_but_not_time_travel(self):
        # 0 先把数据给 1，1 后续遇到地面站；最后才接上 1 的 2 已错过窗口。
        result = optimistic_reachability([[(0, 1)], [], [(1, 2)]], [set(), {1}, set()], 3)
        np.testing.assert_array_equal(result[0], [True, True, False])
        self.assertFalse(result[2].any())

    def test_attention_updates_and_checkpoint_roundtrip(self):
        env = make_env()
        add_task(env, count=2)
        state = env.export_state()
        options = dict(feature_set="task_window", task_features=True, window_features=True)
        model_config = dict(num_gs=1, hidden=16, critic_dim=3 * feature_dim(1, "task_window"),
                            encoder="task_attention", **options)
        model = SatActorCritic(**model_config)
        obs, tile_masks, masks, _ = build_batch(state, **options)
        obs_t, tile_t, mask_t = torch.tensor(obs), torch.tensor(tile_masks), torch.tensor(masks)
        central = critic_observations(obs_t, "mappo")
        with torch.no_grad():
            action, log_prob, _, value = model.get_action_and_value(obs_t, tile_t, action_mask=mask_t, critic_obs=central)
        buffer = [dict(obs=obs_t, tile_mask=tile_t, action_mask=mask_t, action=action,
                       log_prob=log_prob, value=value.squeeze(-1), reward=torch.ones(2),
                       done=torch.ones(2, dtype=torch.bool), critic_obs=central)]
        args = parser().parse_args(["--config", "examples/config_smoke.yaml", "--out-dir", "unused", "--ppo-epochs", "1"])
        before = model.task_actor.score[-1].weight.detach().clone()
        loss = ppo_update(model, torch.optim.Adam(model.parameters()), buffer, torch.zeros(2), args, "cpu")
        self.assertTrue(np.isfinite(loss))
        self.assertFalse(torch.equal(before, model.task_actor.score[-1].weight))
        logits_a, _ = model(obs_t, central)
        logits_b, _ = model(obs_t, central + 100)
        torch.testing.assert_close(logits_a, logits_b)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.pt"
            torch.save(dict(observation_version=OBSERVATION_VERSION, model_config=model_config,
                            model_state=model.state_dict(), ground_station_ids=["gs"]), path)
            policy = MARLPolicy(str(path))
            actions = policy.select_actions(state)
            self.assertEqual(len(actions), 2)
            torch.testing.assert_close(policy.model(obs_t)[0], logits_a)


if __name__ == "__main__":
    unittest.main()
