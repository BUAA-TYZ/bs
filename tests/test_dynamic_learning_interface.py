from copy import deepcopy
import math
import unittest

import torch
from torch.distributions import Categorical

from analysis import main_bc_interface as fixed
from analysis.dynamic_learning_interface import (DynamicActor, DynamicPolicy, build_observation,
    joint_statistics, distribution_metrics, teacher_label, imitation_loss)
from analysis.local_information_baselines import LocalInformationPlane
from analysis.run_sar_frozen import invariants
from test_dynamic_granularity_v2 import fixture


class InterfaceTests(unittest.TestCase):
    def setUp(self):
        torch.set_num_threads(1)
        torch.manual_seed(1701)
        self.env = fixture()
        self.addCleanup(self.env.close)
        self.plane = LocalInformationPlane([0])
        self.plane.history_hops = 2
        self.env.control_plane = self.plane
        self.state = self.plane.view(self.env.export_state(), 0, 2)
        self.obs = build_observation(self.env, self.state, 'task_0')

    def test_fixed_weights_and_disabled_new_mode_are_exact(self):
        old = fixed.HierarchicalActor()
        actor = DynamicActor()
        new_row = actor.image_head.weight[0].detach().clone()
        actor.initialize_fixed(old.state_dict())
        self.assertTrue(torch.equal(new_row, actor.image_head.weight[0]))
        obs = deepcopy(self.obs);obs['image_mask'][0] = False
        a, p = actor.logits(obs)
        b, q = old.logits(obs['variants'][1])
        self.assertTrue(torch.equal(a[1:], b))
        self.assertTrue(torch.equal(p[1], q))

    def test_joint_logp_counts_and_exact_entropy_kl(self):
        actor = DynamicActor()
        im, pos = actor.logits(self.obs)
        for mode in (0, 1):
            count = self.obs['variants'][mode]['block_count']
            places = [0]*count
            value, _ = joint_statistics(im, pos, mode, places)
            self.assertAlmostEqual(float(value.detach()),
                float((im.log_softmax(-1)[mode]+count*pos[mode].log_softmax(-1)[0]).detach()), places=5)
        entropy, kl = distribution_metrics(actor, deepcopy(actor), self.obs)
        expected = Categorical(logits=im).entropy()
        for mode in (0, 1):
            if self.obs['image_mask'][mode]:
                expected = expected+im.softmax(-1)[mode]*self.obs['variants'][mode]['block_count']*Categorical(logits=pos[mode]).entropy()
        self.assertAlmostEqual(float(entropy.detach()), float(expected.detach()), places=5)
        self.assertAlmostEqual(float(kl.detach()), 0., places=6)
        with self.assertRaises(ValueError):joint_statistics(im, pos, 2, [0])

    def test_samples_have_mode_specific_block_count_and_legal_positions(self):
        actor = DynamicActor();generator = torch.Generator().manual_seed(1702)
        for _ in range(40):
            choice = actor.sample(self.obs, generator)
            mode = choice['mode']
            self.assertTrue(self.obs['image_mask'][mode])
            if mode < 2:
                view = self.obs['variants'][mode]
                self.assertEqual(len(choice['positions']), view['block_count'])
                self.assertTrue(all(view['position_mask'][i] for i in choice['positions']))
            else:self.assertEqual(choice['positions'], [])
            self.assertTrue(math.isfinite(choice['log_probability']))

    def test_unknown_remote_truth_does_not_enter_observation(self):
        altered = self.env.export_state()
        for sid, row in altered.satellites.items():
            if sid != 0:
                row.update(committed_compute_s=999999., mem_remaining_gb=0., queue_len=999,
                           isl_backlog_mb={0:999999.}, ground_backlog_mb={'gs':99999.})
        other = build_observation(self.env, self.plane.view(altered, 0, 2), 'task_0')
        self.assertEqual(self.obs, other)
        self.env.cfg.scheduled_task_arrivals[0]['window_result_bytes'] = [999999]*16
        self.assertEqual(self.obs, build_observation(self.env, self.state, 'task_0'))

    def test_window_labels_and_masked_loss_have_finite_gradients(self):
        actor = DynamicActor()
        for mode in (0, 1):
            obs = self.obs['variants'][mode]
            weights = dict(zip(obs['tile_ids'], obs['window_weights']))
            commitments = {tid: obs['source'] for tid in obs['tile_ids']}
            label = teacher_label(self.obs, ('local_4','local_16')[mode], commitments, weights)
            self.assertEqual(label['mode'], mode)
            self.assertEqual(label['represented_windows'], sum(weights.values()))
            loss = imitation_loss(actor, [dict(observation=self.obs,label=label)])
            actor.zero_grad();loss.backward()
            self.assertTrue(all(torch.isfinite(p.grad).all() for p in actor.parameters() if p.grad is not None))

    def test_four_block_commitment_executes_once_and_completes(self):
        actor = DynamicActor()
        policy = DynamicPolicy(actor, 1701, self.env, self.plane)
        actor.sample = lambda obs, generator: dict(mode=0,
            positions=[0]*obs['variants'][0]['block_count'], log_probability=0.,entropy=0.)
        for _ in range(80):
            state = self.env.export_state();self.plane.enqueue(state)
            self.env.step(policy.select_actions(self.plane.view(state, 0, 2)))
        self.assertEqual(len(policy.decisions),1)
        self.assertEqual(self.env.tasks['task_0'].num_tiles,4)
        self.assertIsNotNone(self.env.tasks['task_0'].completion_time)
        self.assertEqual(sum(t.completed_compute_windows for t in self.env.tiles.values()),16)
        self.assertTrue(invariants(self.env)['passed'])

    def test_masked_ground_blocks_are_not_counted_as_represented(self):
        obs = deepcopy(self.obs)
        variant = obs['variants'][0]
        weights = dict(zip(variant['tile_ids'],variant['window_weights']))
        commitments = {tid: fixed.GROUND for tid in variant['tile_ids']}
        variant['position_mask'][variant['node_ids'].index(fixed.GROUND)] = False
        label = teacher_label(obs,'cgs_4',commitments,weights)
        self.assertEqual(label['ground_block_windows'],0)
        self.assertEqual(label['ground_block_committed_windows'],sum(weights.values()))
        self.assertEqual(label['masked_windows'],sum(weights.values()))


if __name__ == '__main__':unittest.main()
