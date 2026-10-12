from copy import deepcopy
from itertools import product
import unittest

import torch

from sim.marl.actor import SatActorCritic
from train_marl import parser, ppo_update


class IdleActorUpdateTests(unittest.TestCase):
    def test_idle_batch_does_not_move_actor_with_adam_momentum(self):
        torch.set_num_threads(1)
        for encoder, batching in product(("mlp", "task_attention"), ("mixed", "active")):
            with self.subTest(encoder=encoder, batching=batching):
                model = SatActorCritic(num_gs=1, hidden=8, critic_dim=10,
                                      encoder=encoder, feature_set="role_contact")
                optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
                obs = torch.rand(4, model.in_dim)
                central = torch.rand(4, model.critic_dim)
                # 先建立非零 Adam 动量，再检查无决策数据能否保持 actor 不变。
                logits, _ = model(obs, central)
                logits[..., 1].sum().backward()
                optimizer.step()
                actor = model.task_actor if model.task_actor is not None else model.policy_head
                before = deepcopy(actor.state_dict())
                trunk_before = deepcopy(model.trunk.state_dict())
                critic_before = deepcopy(model.central_critic.state_dict())
                tiles = torch.zeros(4, 4, dtype=torch.bool)
                masks = torch.zeros(4, 4, model.act_dim, dtype=torch.bool)
                masks[..., 0] = True
                with torch.no_grad():
                    action, log_prob, _, value = model.get_action_and_value(obs, tiles, action_mask=masks,
                                                                            critic_obs=central)
                buffer = [dict(obs=obs, tile_mask=tiles, action_mask=masks, action=action,
                               log_prob=log_prob, value=value.squeeze(-1), critic_obs=central,
                               reward=torch.ones(4), done=torch.ones(4, dtype=torch.bool))]
                args = parser().parse_args(["--config", "unused", "--out-dir", "unused",
                                           "--ppo-epochs", "2", "--minibatch", "2",
                                           "--actor-batching", batching])
                ppo_update(model, optimizer, buffer, torch.zeros(4), args, "cpu")
                for key, tensor in actor.state_dict().items():
                    self.assertTrue(torch.equal(tensor, before[key]), key)
                if encoder == "mlp":
                    for key, tensor in model.trunk.state_dict().items():
                        self.assertTrue(torch.equal(tensor, trunk_before[key]), key)
                self.assertTrue(any(not torch.equal(tensor, critic_before[key])
                                    for key, tensor in model.central_critic.state_dict().items()))
                buffer[0]["tile_mask"][:] = True
                buffer[0]["action_mask"][..., 1] = True
                ppo_update(model, optimizer, buffer, torch.zeros(4), args, "cpu")
                self.assertTrue(any(not torch.equal(tensor, before[key])
                                    for key, tensor in actor.state_dict().items()))

    def test_ippo_keeps_shared_value_trunk_trainable(self):
        torch.set_num_threads(1)
        model = SatActorCritic(num_gs=1, hidden=8)
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
        obs = torch.rand(4, model.in_dim)
        logits, _ = model(obs)
        logits[..., 1].sum().backward()
        optimizer.step()
        head_before = deepcopy(model.policy_head.state_dict())
        trunk_before = deepcopy(model.trunk.state_dict())
        tiles = torch.zeros(4, 4, dtype=torch.bool)
        masks = torch.zeros(4, 4, model.act_dim, dtype=torch.bool)
        masks[..., 0] = True
        with torch.no_grad():
            action, log_prob, _, value = model.get_action_and_value(obs, tiles, action_mask=masks)
        buffer = [dict(obs=obs, tile_mask=tiles, action_mask=masks, action=action,
                       log_prob=log_prob, value=value.squeeze(-1), reward=torch.ones(4),
                       done=torch.ones(4, dtype=torch.bool))]
        args = parser().parse_args(["--config", "unused", "--out-dir", "unused", "--ppo-epochs", "1"])
        ppo_update(model, optimizer, buffer, torch.zeros(4), args, "cpu")
        self.assertTrue(all(torch.equal(tensor, head_before[key]) for key, tensor in model.policy_head.state_dict().items()))
        self.assertTrue(any(not torch.equal(tensor, trunk_before[key]) for key, tensor in model.trunk.state_dict().items()))
