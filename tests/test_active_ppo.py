from copy import deepcopy
import math
import unittest

import torch

from sim.marl.actor import SatActorCritic
from train_marl import parser, ppo_update


class ActivePPOTests(unittest.TestCase):
    def setUp(self):
        torch.set_num_threads(1)
        torch.manual_seed(43)
        self.model = SatActorCritic(num_gs=1, hidden=8, critic_dim=10)
        self.args = parser().parse_args([
            "--config", "unused", "--out-dir", "unused", "--actor-batching", "active",
            "--ppo-epochs", "1", "--minibatch", "32", "--gamma", "1", "--lam", "1",
            "--ent-coef", "0", "--vf-coef", "0", "--max-grad-norm", "100"])
        self.buffer = []
        for t in range(4):
            obs = torch.rand(3, self.model.in_dim)
            central = torch.rand(3, 10)
            tiles = torch.zeros(3, 4, dtype=torch.bool)
            if t == 0:
                tiles[:, 0] = True
            masks = torch.ones(3, 4, self.model.act_dim, dtype=torch.bool)
            with torch.no_grad():
                action, log_prob, _, value = self.model.get_action_and_value(
                    obs, tiles, action_mask=masks, critic_obs=central)
            self.buffer.append(dict(obs=obs, critic_obs=central, tile_mask=tiles, action_mask=masks,
                                    action=action, log_prob=log_prob, value=value.squeeze(-1),
                                    reward=torch.tensor([1., 3., 8.]) if t == 3 else torch.zeros(3),
                                    done=torch.full((3,), t == 3, dtype=torch.bool)))

    def test_delayed_reward_crosses_idle_steps_before_actor_filtering(self):
        expected = deepcopy(self.model)
        manual_optimizer = torch.optim.SGD(expected.parameters(), lr=0.001)
        first = self.buffer[0]
        advantage = torch.tensor([1., 3., 8.]) - first["value"]
        advantage = (advantage - advantage.mean()) / advantage.std(unbiased=False)
        _, log_prob, _, _ = expected.get_action_and_value(
            first["obs"], first["tile_mask"], first["action"], first["action_mask"])
        loss = -((log_prob - first["log_prob"]).exp() * advantage).mean()
        loss.backward()
        manual_optimizer.step()
        diagnostics = {}
        ppo_update(self.model, torch.optim.SGD(self.model.parameters(), lr=0.001),
                   self.buffer, torch.zeros(3), self.args, "cpu", diagnostics)
        for key, value in self.model.state_dict().items():
            torch.testing.assert_close(value, expected.state_dict()[key], rtol=1e-6, atol=1e-7)
        self.assertEqual(diagnostics["active_rows"], 3)
        self.assertEqual(diagnostics["rows"], 12)
        self.assertEqual(diagnostics["actor_batch_sizes"], [3])
        self.assertEqual(diagnostics["critic_updates"], 1)
        self.assertGreaterEqual(diagnostics["joint_kl_mean"], 0)

    def test_kl_stops_actor_but_critic_keeps_all_rows(self):
        self.args.actor_epochs = 20
        self.args.ppo_epochs = 2
        self.args.minibatch = 2
        self.args.target_kl = 1e-9
        self.args.vf_coef = 0.5
        before = deepcopy(self.model.central_critic.state_dict())
        diagnostics = {}
        loss = ppo_update(self.model, torch.optim.Adam(self.model.parameters(), lr=0.02),
                          self.buffer, torch.zeros(3), self.args, "cpu", diagnostics)
        self.assertTrue(math.isfinite(loss))
        self.assertTrue(diagnostics["kl_early_stop"])
        self.assertEqual(diagnostics["actor_updates"], 1)
        self.assertEqual(diagnostics["critic_updates"], 12)
        self.assertGreater(diagnostics["joint_kl_mean"], self.args.target_kl)
        self.assertTrue(any(not torch.equal(before[k], v) for k, v in self.model.central_critic.state_dict().items()))

    def test_rejects_shared_ippo_critic(self):
        model = SatActorCritic(num_gs=1, hidden=8)
        with self.assertRaisesRegex(ValueError, "separate centralized critic"):
            ppo_update(model, torch.optim.Adam(model.parameters()), self.buffer,
                       torch.zeros(3), self.args, "cpu")


if __name__ == "__main__":
    unittest.main()
