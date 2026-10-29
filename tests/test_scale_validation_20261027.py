from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from dataclasses import asdict
from unittest.mock import patch
import hashlib
import unittest
import torch

from analysis import run_scale_validation_20261027 as r
from analysis.scale_feature_audit_20261027 import FeatureAudit
from test_dynamic_granularity_v2 import fixture, FakeTopology, FakeVisibility


class ScaleValidation(unittest.TestCase):
    def setUp(self):torch.set_num_threads(1)

    def execute(self,family,observed):
        torch.manual_seed(1701);env=fixture();self.addCleanup(env.close)
        plane=r.LocalInformationPlane([0]);plane.history_hops=2;env.control_plane=plane
        actor=r.fixed.HierarchicalActor().eval() if family=='fixed' else r.dynamic.DynamicActor().eval()
        policy=(r.fixed.CommittedPolicy(actor,1701000) if family=='fixed' else r.dynamic.DynamicPolicy(actor,1701000,env,plane))
        audit=FeatureAudit({})
        wrapped=r.ObservedPolicy(policy,env,audit,{'task_0'}) if observed else policy
        h=hashlib.sha256()
        for step in range(120):
            state=env.export_state();plane.enqueue(state)
            acts=wrapped.select_actions(plane.view(state,0,2))
            r.action_digest_update(h,step,acts);env.step(acts)
        return h.hexdigest(),env.summary()['task_outcomes'],audit.result(),policy.decisions

    def test_fixed_observer_is_passive(self):
        plain=self.execute('fixed',False);observed=self.execute('fixed',True)
        self.assertEqual(plain[:2],observed[:2]);self.assertEqual(observed[2]['decisions'],1)
        self.assertEqual(observed[2]['candidates']['g16/after_topk']['maximum'],len(observed[3]['task_0']['observation']['node_ids'][2:])-observed[3]['task_0']['observation']['node_ids'][2:].count(-1))

    def test_dynamic_observer_is_passive_after_repack(self):
        plain=self.execute('dynamic',False);observed=self.execute('dynamic',True)
        self.assertEqual(plain[:2],observed[:2]);self.assertEqual(observed[2]['decisions'],1)
        self.assertIn('g4/before_topk',observed[2]['candidates']);self.assertIn('g16/before_topk',observed[2]['candidates'])

    def test_dates_and_all_methods(self):
        self.assertEqual(len(r.METHODS),22);self.assertEqual(len(set(r.METHODS)),22)
        for date in ('2026-10-16','2026-12-21','2026-12-22','2026-12-23'):
            with TemporaryDirectory() as tmp:
                with self.assertRaises(AssertionError):r.build_episode({},date,[],{},Path(tmp),{},[])

    def test_validation_hour_preserves_scale_and_followup(self):
        with TemporaryDirectory() as tmp:
            cfg=r.scale_config(48,288)
            p=r.build_episode(cfg,'2026-10-22',[],dict(start_s=1800,end_s=5400),Path(tmp),{},[])
            self.assertEqual(p['score_end_s']-p['score_start_s'],3600)
            self.assertEqual(p['sim_duration_s']-p['score_end_s'],600)
            self.assertEqual(p['config']['num_sats'],336)
            for method in r.METHODS:
                c=r.configuration(p,method);r.config_from_dict(c)
                self.assertEqual(c['topology'],p['config']['topology'])

    def test_worker_record_resources_and_followup_for_all_policy_families(self):
        base=fixture();c=asdict(base.cfg);base.close()
        c.update(sim_steps=710,decision_tile_limit_per_sat=64)
        c['scheduled_task_arrivals'][0]['release_step']=20
        def environment(cfg):
            with patch('sim.env.TopologyModel',FakeTopology),patch('sim.env.VisibilityCache',FakeVisibility):
                env=r.GranularityEnv_original(cfg)
            env.vis_cache.windows_at=lambda sid,t:{'gs':(0,1000)}
            return env
        with TemporaryDirectory() as tmp:
            root=Path(tmp);p=dict(date='2026-10-22',config=c,scored_task_ids=['task_0'],
                score_start_s=10,score_end_s=110,sim_duration_s=710)
            r.write_json(root/'protocol.json',p);r.write_json(root/'run_manifest.json',dict(hashes={}))
            r.write_json(root/'ranges.json',dict(ranges={'fixed_1701':{},'dynamic_1701':{}}))
            models={}
            for family,actor in [('fixed',r.fixed.HierarchicalActor()),('dynamic',r.dynamic.DynamicActor())]:
                path=root/f'{family}.pt';torch.save(dict(state_dict=actor.state_dict()),path);models[family+'_1701']=str(path)
            manifest=dict(models=models,ranges=str(root/'ranges.json'))
            with patch.object(r,'ROOT',root),patch.object(r,'verify_manifest',return_value=manifest),\
                patch.object(r,'verify'),patch.object(r,'GranularityEnv_original',r.GranularityEnv,create=True),\
                patch.object(r,'GranularityEnv',side_effect=environment):
                for method in ('fixed_1701','dynamic_1701','global_v3_dynamic','minload_2_4','local_heft_16'):
                    short=r.simulate((str(root/'protocol.json'),method))
                    self.assertEqual(short['status'],'PASS',short)
                    row=r.read(short['path']);r.check_record(row,p)
                    self.assertTrue(row['checks']['followup600'])
                    self.assertAlmostEqual(row['resources']['whole']['total_transmitted_bytes'],
                        sum(row['resources'][s]['total_transmitted_bytes'] for s in ('warmup','main','followup')))
                    if method.startswith(('fixed_','dynamic_')):
                        self.assertEqual(row['feature_audit']['decisions'],1)


if __name__=='__main__':unittest.main()
