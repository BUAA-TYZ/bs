from copy import deepcopy
import math
import unittest
import torch
from analysis import main_mappo as m
from analysis.main_rl_interface import joint_statistics
from analysis.run_main_mappo import selection_key, deterioration_streak, TailRecorder
from test_main_bc_stage2 import observed
from test_simulation import make_env, add_task
from sim.entities import Action, ActionType


class MAPPOTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):torch.set_num_threads(1)

    def test_temporal_credit_is_not_credited_before_completion(self):
        rows=[dict(task_id=t,decision_order=i,observation=dict(source=0,time_seconds=time),old_value=0.)
              for i,(t,time) in enumerate([('a',0),('b',10),('c',20)])]
        oo=[dict(task_id=t,source_sat_id=0,release_seconds=release,status='completed',latency_seconds=lat,observed_age_seconds=700)
            for t,release,lat in [('a',0,15),('b',10,20),('c',20,60)]]
        result,audit=m.assign_returns(rows,oo,{'a','b','c'})
        self.assertEqual(result[0]['reward_events'],[])
        self.assertEqual(result[1]['reward_events'][0]['task_id'],'a')
        self.assertEqual(audit['assigned_reward_events'],3)
        self.assertAlmostEqual(result[0]['return_target'],3-(15+20+60)/600,places=5)
        self.assertAlmostEqual(result[-1]['return_target'],2-(20+60)/600,places=5)

    def test_late_reward_once_and_no_cross_source_leak(self):
        rows=[dict(task_id=t,decision_order=0,observation=dict(source=s,time_seconds=1),old_value=.5)
              for t,s in [('a',0),('b',1)]]
        oo=[dict(task_id='a',source_sat_id=0,release_seconds=0,status='completed',latency_seconds=700,observed_age_seconds=800),
            dict(task_id='b',source_sat_id=1,release_seconds=0,status='pending',latency_seconds=None,observed_age_seconds=800)]
        r,a=m.assign_returns(rows,oo,{'a','b'})
        self.assertEqual(a['assigned_reward_events'],2)
        for row in r:
            self.assertEqual(row['return_target'],-1)
            self.assertEqual(row['reward_events'][0]['time'],600)

    def test_same_timestamp_reward_precedes_new_decision(self):
        rows=[dict(task_id=t,decision_order=i,observation=dict(source=0,time_seconds=x),old_value=0.)
              for i,(t,x) in enumerate([('a',0),('b',10)])]
        oo=[dict(task_id='a',source_sat_id=0,release_seconds=0,status='completed',latency_seconds=10,observed_age_seconds=700)]
        r,_=m.assign_returns(rows,oo,{'a','b'})
        self.assertEqual(len(r[0]['reward_events']),1);self.assertEqual(r[1]['reward_events'],[])

    def test_joint_kl_entropy_and_raw_only_are_finite(self):
        o=observed();a=m.HierarchicalActor();b=deepcopy(a)
        ent,kl=m.distribution_metrics(a,b,o)
        self.assertTrue(torch.isfinite(ent));self.assertAlmostEqual(float(kl.detach()),0.,places=6)
        o['image_mask']=[False,True];o['position_mask']=[False]*10
        ent,kl=m.distribution_metrics(a,b,o)
        self.assertEqual(float(ent.detach()),0.);self.assertEqual(float(kl.detach()),0.)

    def test_real_ppo_update_and_off_policy_rejection(self):
        torch.manual_seed(1701);actor=m.HierarchicalActor();critic=m.Critic();obs=observed();rows=[]
        for i in range(4):
            sample=actor.sample(obs,torch.Generator().manual_seed(i))
            row=dict(observation=obs,critic_observation=dict(nodes=obs['nodes'],source_node=obs['nodes'][0],ground=[0.]*4),
                     advantage=float(i)-1.5,return_target=float(i)/3,**sample)
            rows.append(row)
        before={k:v.detach().clone() for k,v in actor.state_dict().items()}
        opt=torch.optim.Adam(list(actor.parameters())+list(critic.parameters()),lr=m.HP['lr'])
        history=m.update(actor,critic,opt,rows,1701)
        self.assertTrue(all(math.isfinite(r['loss']) for r in history))
        self.assertTrue(any(not torch.equal(before[k],v) for k,v in actor.state_dict().items()))
        with self.assertRaisesRegex(AssertionError,'Behavior'):m.update(actor,critic,opt,rows,1701)

    def test_selection_and_consecutive_stop(self):
        initial=dict(P=[.6,.94],R600=100.,mean_completed_s=230.)
        rows=[dict(episodes=0,metrics=initial),dict(episodes=176,metrics=dict(P=[.8,.90],R600=90.,mean_completed_s=100.)),
              dict(episodes=352,metrics=dict(P=[.7,.95],R600=95.,mean_completed_s=180.))]
        self.assertEqual(min(rows,key=lambda r:selection_key(r,initial))['episodes'],352)
        x=deterioration_streak(0,dict(R600=101),initial);self.assertEqual(x,1)
        self.assertEqual(deterioration_streak(x,dict(R600=102),initial),2)
        self.assertEqual(deterioration_streak(x,dict(R600=100),initial),0)

    def test_passive_recorder_does_not_change_executor(self):
        envs=[make_env() for _ in range(2)]
        for e in envs:self.addCleanup(e.close);add_task(e,count=1,size=.1)
        tail=TailRecorder(envs[1],['task_0'])
        for step in range(30):
            for e in envs:
                actions=[Action(e.tasks['task_0'].tile_ids[0],ActionType.LOCAL)] if step==0 else []
                e.step(actions)
            tail.tick()
        self.assertEqual(envs[0].tasks,envs[1].tasks);self.assertEqual(envs[0].tiles,envs[1].tiles)
        self.assertEqual(dict(envs[0].metrics.network_sent_mb),dict(envs[1].metrics.network_sent_mb))


if __name__=='__main__':unittest.main()
