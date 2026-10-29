from copy import deepcopy
import math
import tempfile
from pathlib import Path
from dataclasses import asdict
import gzip
import json
import unittest
from unittest.mock import patch
import torch
from torch.distributions import Categorical
from analysis import dynamic_mappo as m
from analysis import run_dynamic_learning as runner
from analysis.dynamic_learning_interface import DynamicActor, build_observation, joint_statistics
from analysis.local_information_baselines import LocalInformationPlane
from test_dynamic_granularity_v2 import fixture


class DynamicPPOTests(unittest.TestCase):
    def setUp(self):
        torch.set_num_threads(1); torch.manual_seed(1701)
        env = fixture(); self.addCleanup(env.close)
        plane = LocalInformationPlane([0]); plane.history_hops = 2; env.control_plane = plane
        self.obs = build_observation(env,plane.view(env.export_state(),0,2),'task_0')

    def rows(self, actor):
        rows = []
        for mode in (0,1,2):
            obs = deepcopy(self.obs); obs['image_mask'] = [True,True,True]
            positions = [0]*obs['variants'][mode]['block_count'] if mode<2 else []
            value,_ = joint_statistics(*actor.logits(obs),mode,positions)
            nodes = obs['variants'][1]['nodes']
            rows.append(dict(observation=obs,mode=mode,positions=positions,log_probability=float(value.detach()),
                advantage=float(mode)-1,return_target=mode/2,
                critic_observation=dict(nodes=nodes,source_node=nodes[0],ground=[0.]*4)))
        return rows

    def test_joint_gradient_and_behavior_rejection(self):
        actor = DynamicActor(); critic = m.Critic(); rows = self.rows(actor)
        before = actor.image_head.weight.detach().clone()
        optimizer = torch.optim.Adam(list(actor.parameters())+list(critic.parameters()),lr=m.HP['lr'])
        history = m.update(actor,critic,optimizer,rows,1701)
        self.assertTrue(all(math.isfinite(r['loss']) for r in history))
        self.assertTrue(all(not torch.equal(before[i],actor.image_head.weight[i]) for i in (0,1,2)))
        with self.assertRaisesRegex(AssertionError,'Behavior'): m.update(actor,critic,optimizer,rows,1701)

    def test_wrong_block_count_is_rejected(self):
        actor = DynamicActor(); rows = self.rows(actor); rows[0]['positions'].append(0)
        critic = m.Critic(); optimizer = torch.optim.Adam(list(actor.parameters())+list(critic.parameters()),lr=m.HP['lr'])
        with self.assertRaises(AssertionError): m.update(actor,critic,optimizer,rows,1701)

    def test_entropy_weights_both_modes_and_ignores_unavailable(self):
        obs = deepcopy(self.obs); obs['image_mask'] = [True,True,True]
        im = torch.tensor([.1,.2,.3],requires_grad=True)
        pos = [torch.tensor([1.,0.]),torch.tensor([0.,0.])]
        expected = Categorical(logits=im).entropy()
        for mode in (0,1): expected = expected+im.softmax(-1)[mode]*obs['variants'][mode]['block_count']*Categorical(logits=pos[mode]).entropy()
        self.assertTrue(torch.allclose(m.expected_entropy((im,pos),obs),expected))
        obs['image_mask'] = [False,False,True]
        logits = (torch.tensor([-torch.inf,-torch.inf,0.]),[torch.full((10,),-torch.inf)]*2)
        self.assertEqual(float(m.expected_entropy(logits,obs)),0.)

    def test_critic_global_values_do_not_change_actor_logits(self):
        actor = DynamicActor(); rows = self.rows(actor); before = actor.logits(rows[0]['observation'])
        rows[0]['critic_observation']['nodes'] = [[999.]*14]*3
        after = actor.logits(rows[0]['observation'])
        self.assertTrue(torch.equal(before[0],after[0]))
        self.assertTrue(all(torch.equal(x,y) for x,y in zip(before[1],after[1])))

    def test_selection_uses_completion_guard_and_two_bad_evaluations(self):
        initial = dict(P=[.6,.95],R600=100,mean_completed_s=150)
        rows = [dict(episodes=0,metrics=initial),
            dict(episodes=88,metrics=dict(P=[.8,.94],R600=80,mean_completed_s=100)),
            dict(episodes=176,metrics=dict(P=[.7,.96],R600=90,mean_completed_s=120))]
        self.assertEqual(min(rows,key=lambda r:runner.selection_key(r,initial))['episodes'],176)
        streak = runner.deterioration_streak(0,dict(R600=101),initial)
        self.assertEqual(runner.deterioration_streak(streak,dict(R600=102),initial),2)

    def test_checkpoint_preserves_three_mode_parent_and_budget(self):
        actor = DynamicActor(); critic = m.Critic()
        optimizer = torch.optim.Adam(list(actor.parameters())+list(critic.parameters()),lr=m.HP['lr'])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'checkpoint.pt'
            with patch.object(runner,'sha',return_value='parenthash'), patch.object(runner,'read',return_value={'teacher_hashes':{'train':'hash'}}):
                runner.save_checkpoint(path,actor,critic,optimizer,1701,88,'parent','mappo')
                data = torch.load(path,weights_only=True,map_location='cpu')
                self.assertEqual(data['episodes'],88)
                self.assertEqual(data['state_dict']['image_head.weight'].shape[0],3)
                self.assertEqual(data['parent_sha256'],'parenthash')
                self.assertFalse(data['validation_used_for_gradient'])
                self.assertTrue(torch.equal(data['torch_rng_state'],torch.get_rng_state()))
                with self.assertRaises(FileExistsError): runner.save_checkpoint(path,actor,critic,optimizer,1701,88,'parent','mappo')

    def test_training_rollout_records_real_terminal_credit(self):
        env = fixture(); env.cfg.sim_steps = 4200; env.cfg.decision_tile_limit_per_sat = 32
        actor = DynamicActor(); critic = m.Critic()
        with torch.no_grad(): actor.image_head.bias.copy_(torch.tensor([100.,-100.,-100.]))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); cp = root/'parent.pt'
            torch.save(dict(state_dict=actor.state_dict(),critic=critic.state_dict()),cp)
            initial = root/'training/pure_imitation_1701.pt'; initial.parent.mkdir()
            torch.save(dict(state_dict=actor.state_dict()),initial)
            protocol = dict(date='2026-10-16',score_start_s=0,score_end_s=3600,sim_duration_s=4200,
                scored_task_ids=['task_0'],config=asdict(env.cfg))
            path = root/'protocol.json'; path.write_text(json.dumps(protocol))
            job = dict(kind='train',seed=1701,episode=1,protocol=str(path),checkpoint=str(cp),
                checkpoint_sha256=runner.sha(cp),out=str(root/'rollout.json.gz'))
            with patch.object(runner,'ROOT',root), patch.object(runner,'verify_run',return_value={}), \
                    patch.object(runner,'verify'), patch.object(runner.teacher,'GranularityEnv',return_value=env):
                result = runner.simulate(job)
            self.assertEqual(result['status'],'PASS',result)
            with gzip.open(job['out'],'rt') as f: record = json.load(f)
            self.assertEqual(record['records'][0]['mode'],0)
            self.assertEqual(len(record['records'][0]['positions']),4)
            self.assertEqual(record['credit_audit']['assigned_reward_events'],1)
            self.assertEqual(record['unacted_scored_ids'],[])
            opt = torch.optim.Adam(list(actor.parameters())+list(critic.parameters()),lr=m.HP['lr'])
            self.assertTrue(m.update(actor,critic,opt,record['records'],1701))

    def test_validation_records_all_deadlines_and_passive_resources(self):
        env = fixture(); env.cfg.sim_steps = 22260; env.cfg.decision_tile_limit_per_sat = 32
        actor = DynamicActor(); critic = m.Critic()
        with torch.no_grad(): actor.image_head.bias.copy_(torch.tensor([-100.,100.,-100.]))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); cp = root/'parent.pt'
            torch.save(dict(state_dict=actor.state_dict(),critic=critic.state_dict()),cp)
            initial = root/'training/pure_imitation_1701.pt'; initial.parent.mkdir()
            torch.save(dict(state_dict=actor.state_dict()),initial)
            path = root/'protocol.json'
            path.write_text(json.dumps(dict(date='2026-10-22',score_start_s=60,score_end_s=21660,
                sim_duration_s=22260,scored_task_ids=['task_0'],config=asdict(env.cfg))))
            job = dict(kind='validation',seed=1701,episode=0,protocol=str(path),checkpoint=str(cp),
                checkpoint_sha256=runner.sha(cp),out=str(root/'validation.json'))
            with patch.object(runner,'ROOT',root), patch.object(runner,'verify_run',return_value={}), \
                    patch.object(runner,'verify'), patch.object(runner.teacher,'GranularityEnv',return_value=env):
                result = runner.simulate(job)
            self.assertEqual(result['status'],'PASS',result)
            record = json.loads(Path(job['out']).read_text())
            self.assertEqual([r['D_seconds'] for r in record['curve']],[60,90,120,180,240,300,600])
            self.assertEqual(record['choices'],{'onboard_16':1})
            self.assertEqual(record['actual_blocks'],{'16':1})
            self.assertEqual(record['policy_metrics'][0]['kl_initial'],0.)
            self.assertEqual(set(record['resources']),{'whole','main','warmup','followup'})
            self.assertNotIn('critic_observation',record['decisions'][0])


if __name__ == '__main__': unittest.main()
