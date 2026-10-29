from copy import deepcopy
import math
from types import SimpleNamespace
import unittest
import torch
from analysis import main_bc_interface as bc
from analysis.main_rl_interface import joint_statistics
from analysis.local_information_baselines import LocalInformationPlane
from analysis.prepare_main_bc_stage2 import permitted, arrivals, peak
from analysis.prepare_main_rl_stage1 import date_arrivals
from analysis.sar_three_plan import CandidatePipeline, RawImageAction
from sim.entities import Action, ActionType, Link
from test_local_information_baselines import observation
from test_simulation import make_env, add_task


def observed():
    state=observation()
    state.config['ground_input_relay']=True
    plane=LocalInformationPlane([0,1]);plane.history_hops=2
    view=plane.view(state,0,2)
    return bc.build_observation(view,'task0',view.tiles)


class BCTests(unittest.TestCase):
    def test_ground_token_mask_and_no_ground_truth_leak(self):
        obs=observed()
        self.assertEqual(len(obs['nodes']),10)
        self.assertEqual(obs['node_ids'][1],bc.GROUND)
        self.assertEqual(obs['nodes'][1][1],0)
        actor=bc.HierarchicalActor()
        im,pos=actor.logits(obs)
        self.assertEqual(tuple(pos.shape),(10,))
        state=observation();state.config['ground_input_relay']=True
        plane=LocalInformationPlane([0,1]);plane.history_hops=2
        before=bc.build_observation(plane.view(state,0,2),'task0',state.tiles)
        for gs in state.ground_stations.values():
            gs.update(committed_compute_s=99999,queue_len=999,running=99)
        after=bc.build_observation(plane.view(state,0,2),'task0',state.tiles)
        self.assertEqual(before,after)

    def test_mixed_teacher_window_weights(self):
        obs={'node_ids':[0,bc.GROUND,2]+[-1]*7,'position_mask':[True]*3+[False]*7,'image_mask':[True,True]}
        label=bc.teacher_label(obs,False,{'a':0,'b':bc.GROUND,'c':2},{'a':1,'b':2,'c':3})
        self.assertEqual(label['represented_windows'],6)
        self.assertEqual(label['soft_positions'][:3],[1/6,2/6,3/6])
        self.assertEqual(label['ground_block_windows'],2)
        obs['position_mask'][1]=False
        label=bc.teacher_label(obs,False,{'a':0,'b':bc.GROUND,'c':99},{'a':1,'b':2,'c':3})
        self.assertEqual((label['masked_windows'],label['outside_topk_windows']),(2,3))
        raw=bc.teacher_label(obs,True,{}, {'a':6})
        self.assertEqual((raw['mode'],raw['represented_windows']),(1,6))
        self.assertEqual(sum(raw['soft_positions']),0)

    def test_loss_finite_with_padding_and_raw_mode(self):
        obs=observed();actor=bc.HierarchicalActor()
        obs['image_mask']=[True,True];obs['position_mask']=[True,True]+[False]*8
        label=bc.teacher_label(obs,False,{'a':obs['source'],'b':bc.GROUND},{'a':1,'b':3})
        loss=bc.imitation_loss(actor,[{'observation':obs,'label':label},
            {'observation':obs,'label':bc.teacher_label(obs,True,{}, {'a':4})}])
        self.assertTrue(torch.isfinite(loss))
        loss.backward()
        self.assertTrue(all(torch.isfinite(p.grad).all() for p in actor.parameters() if p.grad is not None))
        with torch.no_grad():
            for p in actor.parameters():p.zero_()
        obs['image_mask']=[True,False];obs['block_count']=16
        samples=actor.sample(obs,torch.Generator().manual_seed(1701))
        self.assertEqual(set(samples['positions']),{0,1})
        self.assertAlmostEqual(samples['log_probability'],-16*math.log(2),places=5)

    def test_mixed_ground_commands_match_public_executor(self):
        envs=[make_env(num_sats=3,execution_commitment=True,input_relay_max_hops=2,relay_max_hops=2,
                      ground_input_relay=True,relay_contact_guard=False,sar_source_isolation=True,task_source_sat_ids=[0]) for _ in range(2)]
        for env in envs:
            self.addCleanup(env.close);env._dist_pipeline=CandidatePipeline(env)
            env.topology.links['1-2']=Link(1,2,True,8.,0.)
            env.vis_cache.windows_at=lambda sid,t:{'gs':(0,1000)}
            add_task(env,count=4,size=.1)
        tids=envs[0].tasks['task_0'].tile_ids
        commands=[Action(tids[0],ActionType.LOCAL),Action(tids[1],ActionType.OFFLOAD,target_gs_id='gs',relay_to_ground=True),
                  Action(tids[2],ActionType.OFFLOAD,target_sat_id=1),Action(tids[3],ActionType.OFFLOAD,target_sat_id=2,input_path=(0,1,2))]
        policy=bc.CommittedPolicy(None,1701)
        policy.decisions['task_0']={'mode':0,'targets':dict(zip(tids,[0,bc.GROUND,1,2])), 'observation':{'ground_id':'gs'}}
        state=envs[1].export_state()
        for link in state.links.values():link['contact_windows']=[(0,1000)]
        plane=LocalInformationPlane([0]);plane.history_hops=2
        adapted=policy.select_actions(plane.view(state,0,2))
        self.assertEqual(commands,adapted)
        self.assertFalse(any(isinstance(a,RawImageAction) for a in adapted))
        for step in range(30):
            envs[0].step(deepcopy(commands) if step==0 else [])
            envs[1].step(deepcopy(adapted) if step==0 else [])
        self.assertEqual(envs[0].tasks,envs[1].tasks)
        self.assertEqual(envs[0].tiles,envs[1].tiles)
        self.assertEqual(dict(envs[0].metrics.network_sent_mb),dict(envs[1].metrics.network_sent_mb))
        self.assertFalse(envs[1]._dist_pipeline.raw_tasks)

    def test_date_allowlist_and_generator_identity(self):
        for d in ('2026-12-21','2026-12-15','2026-10-15'):
            with self.assertRaises(ValueError):permitted(d)
        windows=[dict(start_s=10,end_s=100,source_sat_id=0,target_id='x',window_id=1)]
        self.assertEqual(arrivals(windows,'2026-10-16'),date_arrivals(windows,{'x'},'2026-10-16'))

    def test_second_peak_does_not_overlap(self):
        events=[dict(ready_s=t,raw_bytes=b) for t,b in [(10,100),(100,100),(3700,50),(3800,50)]]
        selected=peak(events,3600,(0,3600))
        self.assertGreaterEqual(selected['start_s'],3600)
        self.assertEqual(selected['raw_bytes'],100)


if __name__=='__main__':
    torch.set_num_threads(1)
    unittest.main()
