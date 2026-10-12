import tempfile
from pathlib import Path
import unittest

import numpy as np
import torch

from sim.marl.actor import SatActorCritic
from sim.marl.decision import build_batch, decision_state
from sim.marl.observation import MAX_TILES_PER_SAT, obs_dim
from sim.scheduler.marl_policy import MARLPolicy
from sim.scheduler.greedy import GreedyEarliestFinish
from sim.scheduler.window_aware import WindowAwareGreedy
from main import make_policy
from sim.entities import ActionType
from train_marl import compute_gae, critic_observations, parser, ppo_update
from test_simulation import make_env, add_task


class MARLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def test_gae_keeps_agents_separate(self):
        rewards = torch.tensor([[1.0, 10.0], [2.0, 20.0]])
        advantages, _ = compute_gae(rewards, torch.zeros_like(rewards), torch.tensor([3.0, 30.0]),
                                     [[False, False], [False, False]], gamma=1, lam=1)
        torch.testing.assert_close(advantages, torch.tensor([[6.0, 60.0], [5.0, 50.0]]))

    def test_terminal_stops_bootstrapping(self):
        rewards = torch.tensor([[1.0, 10.0]])
        advantages, _ = compute_gae(rewards, torch.zeros_like(rewards), torch.tensor([99.0, 99.0]),
                                     [[True, True]])
        torch.testing.assert_close(advantages, rewards)

    def test_masks_neighbors_ground_and_padding(self):
        env = make_env()
        add_task(env, count=1)
        env.vis_cache.visible = False
        state = env.export_state()
        obs, tile_masks, masks, _ = build_batch(state)
        self.assertFalse(masks[0, 0, -1])
        self.assertFalse(masks[0, 0, 3:10].any())
        self.assertEqual(masks[1].sum(), MAX_TILES_PER_SAT)
        model = SatActorCritic(num_gs=1, hidden=16)
        obs = torch.tensor(obs)
        for _ in range(8):
            action, log_prob, entropy, _ = model.get_action_and_value(
                obs, torch.tensor(tile_masks), action_mask=torch.tensor(masks))
            selected = np.take_along_axis(masks, action.numpy()[..., None], axis=-1)
            self.assertTrue(selected.all())
            self.assertTrue(torch.isfinite(log_prob).all())
            self.assertTrue(torch.isfinite(entropy).all())

    def test_heuristics_and_marl_share_candidate_limit(self):
        env = make_env()
        add_task(env, count=12)
        state = env.export_state()
        limited = decision_state(state)
        self.assertEqual(len(limited.tiles), MAX_TILES_PER_SAT)
        self.assertEqual(set(limited.tiles), set(build_batch(state)[3][0]))

    def test_heuristics_do_not_select_full_neighbors(self):
        env = make_env()
        add_task(env, count=12)
        env.satellites[1].mem_used_gb = env.satellites[1].mem_capacity_gb
        env.vis_cache.visible = False
        state = decision_state(env.export_state())
        for policy in (GreedyEarliestFinish(), WindowAwareGreedy()):
            actions = policy.select_actions(state)
            self.assertTrue(all(a.action_type == ActionType.LOCAL for a in actions))

    def test_local_control_uses_same_candidate_and_vram_constraints(self):
        env = make_env()
        add_task(env, count=8)
        state = decision_state(env.export_state())
        actions = make_policy("local", seed=7).select_actions(state)
        self.assertEqual(len(actions), MAX_TILES_PER_SAT)
        self.assertTrue(all(a.action_type == ActionType.LOCAL for a in actions))
        state.config["vram_capacity_gb"] = 0.01
        self.assertTrue(all(a.action_type == ActionType.WAIT for a in make_policy("local", 7).select_actions(state)))

    def test_mappo_actor_is_independent_of_central_state(self):
        dim = obs_dim(1)
        model = SatActorCritic(num_gs=1, hidden=16, critic_dim=3 * dim)
        obs = torch.randn(2, dim)
        central = critic_observations(obs, "mappo")
        logits_a, _ = model(obs, central)
        logits_b, _ = model(obs, central + 100)
        torch.testing.assert_close(logits_a, logits_b)
        self.assertEqual(model.get_value(obs, central).shape, (2, 1))

    def test_partial_rollout_updates_without_nan(self):
        args = parser().parse_args(["--config", "examples/config.yaml", "--out-dir", "unused",
                                    "--ppo-epochs", "1", "--minibatch", "2"])
        env = make_env()
        add_task(env)
        obs, tile_masks, masks, _ = build_batch(env.export_state())
        model = SatActorCritic(num_gs=1, hidden=16)
        optimizer = torch.optim.Adam(model.parameters())
        obs_t, tile_t, mask_t = torch.tensor(obs), torch.tensor(tile_masks), torch.tensor(masks)
        with torch.no_grad():
            action, log_prob, _, value = model.get_action_and_value(obs_t, tile_t, action_mask=mask_t)
        buffer = [{"obs": obs_t, "tile_mask": tile_t, "action_mask": mask_t,
                   "action": action, "log_prob": log_prob, "value": value.squeeze(-1),
                   "reward": torch.ones(2), "done": torch.ones(2, dtype=torch.bool)}]
        loss = ppo_update(model, optimizer, buffer, torch.zeros(2), args, torch.device("cpu"))
        self.assertTrue(np.isfinite(loss))

    def test_legacy_checkpoints_are_not_silently_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.pt"
            torch.save(SatActorCritic(num_gs=1, hidden=16).state_dict(), path)
            with self.assertRaisesRegex(ValueError, "Legacy"):
                MARLPolicy(str(path))


if __name__ == "__main__":
    unittest.main()
