from types import SimpleNamespace
import unittest

import torch

from sim.marl.actor import SatActorCritic, act_dim
from sim.marl.attention_q import AttentionQCritic, joint_q_observations, counterfactual_baseline
from sim.marl.decision import build_batch
from sim.marl.observation import obs_dim
from train_marl import parser, ppo_update
from test_simulation import make_env, add_task


class CoMAPPOTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def test_joint_context_retains_other_actions_and_excludes_self(self):
        obs = torch.tensor([[1.0], [2.0], [3.0]])
        actions = torch.tensor([[0, 0, 0, 0], [1, 0, 0, 0], [0, 1, 0, 0]])
        central = joint_q_observations(obs, actions, 2).reshape(3, 3, 9)
        torch.testing.assert_close(central[:, 0, 0], obs[:, 0])
        torch.testing.assert_close(central[1, :, 0], torch.tensor([2.0, 1.0, 3.0]))
        self.assertEqual(central[0, 1, 2].item(), 1.0)

    def test_attention_critic_other_agent_order_invariant(self):
        torch.manual_seed(0)
        critic = AttentionQCritic(7, 16)
        context = torch.randn(2, 4, 7)
        torch.testing.assert_close(critic(context.flatten(1)), critic(context[:, [0, 3, 1, 2]].flatten(1)))
        self.assertFalse(torch.allclose(critic(context.flatten(1)), critic((context + 1).flatten(1))))
        self.assertTrue(torch.isfinite(critic(context[:, :1].flatten(1))).all())

    def test_counterfactual_matches_enumerated_joint_baseline(self):
        # 两个有效 tile 的动作存在交互项，可检测错误的逐 tile 独立边缘化。
        torch.manual_seed(4)
        probabilities = torch.tensor([[[0.3, 0.7], [0.8, 0.2], [1.0, 0.0], [1.0, 0.0]]])
        mask = probabilities > 0
        obs = torch.zeros(1, 2)
        context = torch.zeros(1, 20)
        context[0, 13] = 1.0  # 其他 agent 的动作固定为 1。

        def critic(x):
            first, second, other = x[:, 3], x[:, 5], x[:, 13]
            return (10 * first + 5 * second + 7 * first * second + 20 * other)[:, None]

        model = SimpleNamespace(in_dim=2, act_dim=2, central_critic=critic)
        class ToyModel:
            in_dim, act_dim, central_critic = model.in_dim, model.act_dim, staticmethod(critic)

            def __call__(self, observations):
                return probabilities.clamp(min=1e-20).log(), None

        expectation = 0.0
        for first in (0, 1):
            for second in (0, 1):
                expectation += probabilities[0, 0, first] * probabilities[0, 1, second] * (
                    10 * first + 5 * second + 7 * first * second + 20)
        sampled = counterfactual_baseline(ToyModel(), obs, mask, context, 20000)
        self.assertAlmostEqual(sampled.item(), expectation.item(), delta=0.12)
        self.assertEqual(context[0, 3].item(), 0.0)

    def test_q_critic_and_actor_train_with_stored_joint_context(self):
        env = make_env()
        add_task(env, count=2)
        add_task(env, src=1, count=1)
        obs, tiles, masks, _ = build_batch(env.export_state())
        obs, tiles, masks = torch.tensor(obs), torch.tensor(tiles), torch.tensor(masks)
        model = SatActorCritic(1, 16, critic_dim=2 * (obs_dim(1) + 4 * act_dim(1)), critic_kind="attention_q")
        with torch.no_grad():
            actions, log_prob, _, _ = model.get_action_and_value(obs, tiles, action_mask=masks)
            central = joint_q_observations(obs, actions, model.act_dim)
            q = model.get_value(obs, central).squeeze(-1)
            baseline = counterfactual_baseline(model, obs, masks, central, 16)
        buffer = [dict(obs=obs, tile_mask=tiles, action_mask=masks, action=actions,
                       log_prob=log_prob, value=q, reward=torch.tensor([1.0, 2.0]),
                       done=torch.ones(2, dtype=torch.bool), critic_obs=central, counterfactual=baseline)]
        args = parser().parse_args(["--config", "examples/config_smoke.yaml", "--out-dir", "unused", "--ppo-epochs", "1"])
        before = model.central_critic.head[-1].weight.detach().clone()
        loss = ppo_update(model, torch.optim.Adam(model.parameters()), buffer, torch.zeros(2), args, "cpu")
        self.assertTrue(torch.isfinite(torch.tensor(loss)))
        self.assertFalse(torch.equal(before, model.central_critic.head[-1].weight))
        torch.testing.assert_close(model(obs, central)[0], model(obs, central + 100)[0])


if __name__ == "__main__":
    unittest.main()
