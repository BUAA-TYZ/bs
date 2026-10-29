from copy import deepcopy
import math
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import torch

from analysis.main_rl_interface import (HierarchicalActor, CentralCritic, CommittedPolicy, build_observation,
    joint_statistics, terminal_reward, teacher_label, sat_options, K)
from analysis.local_information_baselines import LocalInformationPlane
from analysis.sar_three_plan import CandidatePipeline, RawImageAction
from analysis.prepare_main_rl_stage1 import date_arrivals, window_differences
from sim.entities import Action, ActionType, Link
from sim.scheduler.literature import contact_plan
from test_local_information_baselines import observation, deliver
from test_simulation import make_env, add_task


def view():
    state = observation()
    plane = LocalInformationPlane([0, 1]); plane.history_hops = 2
    return plane.view(state, 0, 2)


def observed(state=None):
    state = view() if state is None else state
    return build_observation(state, 'task0', state.tiles)


class MainRLTests(unittest.TestCase):
    def test_window_audit_classes_strict_geometry_and_missing_rows(self):
        row = dict(source_sat_id=0, target_id='port', start_s=1., end_s=2.,
                   window_id=0, scheduled_strips=1)
        self.assertEqual(window_differences([row], [dict(row)]), [])
        changed = dict(row, window_id=5, scheduled_strips=0)
        self.assertEqual({d['class'] for d in window_differences([row], [changed])},
                         {'identifier', 'derived'})
        changed['end_s'] += 1e-12
        self.assertIn('geometry', {d['class'] for d in window_differences([row], [changed])})
        self.assertEqual(window_differences([row], [])[0]['class'], 'geometry')
        changed = dict(row, new_field=0)
        self.assertIn('unclassified', {d['class'] for d in window_differences([row], [changed])})

    def test_joint_logp_is_sum_not_mean_for_16_and_four(self):
        im = torch.tensor([.3, -.2], requires_grad=True)
        pos = torch.tensor([.5, .1, -.4], requires_grad=True)
        for n in (4, 16):
            choices = [i % 3 for i in range(n)]
            logp, _ = joint_statistics(im, pos, 0, choices)
            expected = torch.log_softmax(im, -1)[0]+sum(torch.log_softmax(pos, -1)[i] for i in choices)
            self.assertTrue(torch.allclose(logp, expected))
            self.assertTrue(torch.isfinite(torch.autograd.grad(logp, pos, retain_graph=True)[0]).all())
        logp, _ = joint_statistics(im, pos, 1, [])
        self.assertTrue(torch.allclose(logp, torch.log_softmax(im, -1)[1]))

    def test_unknown_global_state_does_not_change_actor_inputs(self):
        state = observation(); plane = LocalInformationPlane([0, 1]); plane.history_hops = 2
        a = observed(plane.view(state, 0, 2))
        changed = deepcopy(state)
        for sid in (1, 2, 3):
            changed.satellites[sid].update(committed_compute_s=99999, queue_len=999, mem_remaining_gb=0,
                vram_remaining_gb=0, isl_backlog_mb={0: 99999}, ground_backlog_mb={'gs': 99999})
        changed.ground_stations['gs'].update(committed_compute_s=99999, queue_len=999)
        b = observed(plane.view(changed, 0, 2))
        self.assertEqual(a, b)
        net = HierarchicalActor().eval()
        self.assertEqual(net.sample(a, torch.Generator().manual_seed(17)), net.sample(b, torch.Generator().manual_seed(17)))

    def test_candidates_stable_under_known_queue_change(self):
        state = view()
        a = observed(state)
        for sid in state.satellites:
            if sid != 0:
                state.satellites[sid]['committed_compute_s'] = 900
                state.satellites[sid]['isl_backlog_mb'] = {0: 1000}
        b = observed(state)
        self.assertEqual(a['node_ids'], b['node_ids'])

    def test_masks_padding_and_independent_sampling(self):
        obs = observed()
        obs['image_mask'] = [True, False]
        obs['position_mask'] = [True, True]+[False]*(K-1)
        obs['block_count'] = 16
        net = HierarchicalActor().eval()
        with torch.no_grad():
            for p in net.parameters():
                p.zero_()
        generator = torch.Generator().manual_seed(1701)
        draws = [net.sample(obs, generator)['positions'] for _ in range(50)]
        self.assertTrue(all(len(x) == 16 and set(x) <= {0, 1} for x in draws))
        self.assertTrue(any(len(set(x)) == 2 for x in draws))
        obs['block_count'] = 4
        self.assertEqual(len(net.sample(obs, generator)['positions']), 4)
        obs['image_mask'] = [False, True]; obs['position_mask'] = [False]*9
        result = net.sample(obs, generator)
        self.assertEqual(result['positions'], [])
        self.assertTrue(math.isfinite(result['log_probability']))
        obs['image_mask'] = [False, False]
        self.assertIsNone(net.sample(obs, generator))

    def test_teacher_mapping_uses_windows_not_blocks(self):
        obs = {'node_ids': [0, 2]+[-1]*7, 'position_mask': [True, True]+[False]*7}
        label = teacher_label(obs, False, {'a': 0, 'b': 2, 'c': 99}, {'a': 1, 'b': 3, 'c': 2})
        self.assertEqual(label['soft_positions'][:2], [.25, .75])
        self.assertEqual(label['outside_topk_windows'], 2)
        self.assertEqual(label['represented_windows'], 4)
        obs['position_mask'][1] = False
        label = teacher_label(obs, False, {'a': 0, 'b': 2}, {'a': 1, 'b': 3, 'c': 2})
        self.assertEqual((label['masked_windows'], label['uncommitted_windows']), (3, 2))

    def test_terminal_reward_and_followup(self):
        self.assertEqual(terminal_reward({'status': 'completed', 'latency_seconds': 60}), .9)
        self.assertAlmostEqual(terminal_reward({'status': 'completed', 'latency_seconds': 90}), -.15)
        self.assertEqual(terminal_reward({'status': 'completed', 'latency_seconds': 900}), -1)
        self.assertEqual(terminal_reward({'status': 'pending', 'observed_age_seconds': 600}), -1)
        with self.assertRaises(ValueError):
            terminal_reward({'status': 'pending', 'observed_age_seconds': 599.9})

    def test_critic_has_no_actor_parameter_sharing(self):
        actor, critic = HierarchicalActor(), CentralCritic()
        self.assertFalse({id(p) for p in actor.parameters()} & {id(p) for p in critic.parameters()})
        a = actor.logits(observed())
        critic(torch.ones(168, 14), torch.ones(4))
        b = actor.logits(observed())
        self.assertTrue(all(torch.equal(x, y) for x, y in zip(a, b)))

    def test_expired_and_not_received_observations(self):
        state = observation(); plane = LocalInformationPlane([0, 1]); plane.history_hops = 2
        deliver(plane, state, 2, received=1)
        self.assertFalse(plane.view(state, 0, 2).satellites[2]['dynamic_known'])
        deliver(plane, state, 2)
        state.time = 200
        self.assertFalse(plane.view(state, 0, 2).satellites[2]['dynamic_known'])

    def test_committed_destinations_survive_retry(self):
        state = view(); obs = observed(state)
        class FixedActor:
            calls = 0
            def sample(self, o, generator):
                self.calls += 1
                return {'mode': 0, 'positions': [0]*len(o['tile_ids']), 'log_probability': 0., 'entropy': 0.}
        actor = FixedActor(); policy = CommittedPolicy(actor, 1701)
        with patch('analysis.main_rl_interface.build_observation', return_value=obs):
            a = policy.select_actions(state)
            b = policy.select_actions(state)
        self.assertEqual(actor.calls, 1)
        self.assertEqual(a, b)

    def test_reference_executor_local_offload_and_raw_commands(self):
        for ground in (False, True):
            envs = [make_env(num_sats=3, execution_commitment=True, input_relay_max_hops=2, relay_contact_guard=False,
                             sar_source_isolation=True, task_source_sat_ids=[0]) for _ in range(2)]
            for env in envs:
                self.addCleanup(env.close)
                env._dist_pipeline = CandidatePipeline(env)
                env.topology.links['1-2'] = Link(1, 2, True, 8., 0.)
                env.vis_cache.windows_at = lambda sid, t: {'gs': (0, 1000)}
                add_task(env, count=4, size=.1)
                env.image_workload = SimpleNamespace(unique_transport_blocks=lambda n: [(i, .4/n) for i in range(n)])
            tids = envs[0].tasks['task_0'].tile_ids
            commands = ([RawImageAction(t, ActionType.OFFLOAD, raw_hops=2) for t in tids] if ground else
                        [Action(tids[0], ActionType.LOCAL), Action(tids[1], ActionType.OFFLOAD, target_sat_id=1),
                         Action(tids[2], ActionType.OFFLOAD, target_sat_id=2, input_path=(0, 1, 2)),
                         Action(tids[3], ActionType.LOCAL)])
            policy = CommittedPolicy(None, 1701)
            policy.decisions['task_0'] = {'mode': int(ground), 'targets': dict(zip(tids, [0, 1, 2, 0]))}
            legal = LocalInformationPlane([0]); legal.history_hops = 2
            state = envs[1].export_state()
            state.config['relay_max_hops'] = 2
            for link in state.links.values():
                link['contact_windows'] = [(0, 1000)]
            adapted = policy.select_actions(legal.view(state, 0, 2))
            self.assertEqual(commands, adapted)
            for step in range(20):
                envs[0].step(deepcopy(commands) if step == 0 else [])
                envs[1].step(deepcopy(adapted) if step == 0 else [])
            self.assertEqual(envs[0].tasks, envs[1].tasks)
            self.assertEqual(envs[0].tiles, envs[1].tiles)
            self.assertIsNotNone(envs[0].tasks['task_0'].completion_time)
            self.assertEqual(dict(envs[0].metrics.network_sent_mb), dict(envs[1].metrics.network_sent_mb))
            self.assertEqual(envs[0].compute_cohort_records, envs[1].compute_cohort_records)

    def test_no_test_date_generation(self):
        with self.assertRaises(ValueError):
            date_arrivals([], set(), '2026-12-21')


if __name__ == '__main__':
    torch.set_num_threads(1)
    unittest.main()
