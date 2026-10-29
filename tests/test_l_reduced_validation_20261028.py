from concurrent.futures import Future
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest

import torch

from analysis import run_l_reduced_validation_20261028 as r
from test_dynamic_granularity_v2 import fixture, FakeTopology, FakeVisibility


class ReducedValidationTests(unittest.TestCase):
    def setUp(self):
        torch.set_num_threads(1)
        saved={k:getattr(r.engine,k) for k in ('ROOT','VALID','METHODS','LEARNED','verify_manifest','check_time','ThreePlanChoice','ControlPlane')}
        self.addCleanup(lambda:r.engine.__dict__.update(saved))

    def test_exact_reuse_and_remaining_jobs(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);protocols=[];reused=[]
            for date in r.DATES:
                p=root/f'{date}.json';r.write_json(p,dict(date=date));protocols.append(str(p))
                for seed in (1701,1702,1703):
                    out=root/f'{date}_{seed}.json';r.write_json(out,dict(method=f'fixed_{seed}',date=date))
                    reused.append(dict(path=str(out)))
            jobs=r.pending_jobs(protocols,reused)
            self.assertEqual(len(jobs),33)
            self.assertFalse(any(n.startswith('fixed_') for _,n in jobs))
            self.assertEqual(len(set(r.METHODS)),14)

    def test_drain_output_schema(self):
        rows=r.reuse_records(dict(results=[dict(status='PASS',output='original.json',sha256='abc',pid=10819)]))
        self.assertEqual(rows,[dict(status='PASS',path='original.json',sha256='abc',reused=True)])

    def test_failure_drains_only_current_batch(self):
        class Pool:
            def __init__(self):self.calls=[]
            def submit(self,fn,args):
                self.calls.append(args);f=Future()
                if len(self.calls)==2:f.set_exception(RuntimeError('worker failure'))
                else:f.set_result(dict(status='PASS'))
                return f
        pool=Pool()
        with TemporaryDirectory() as tmp,patch.object(r,'ROOT',Path(tmp)):
            results=r.execute_batches(pool,list(range(33)),[dict(status='PASS')]*9)
            self.assertEqual(len(pool.calls),9)
            self.assertEqual(len(results),18)
            self.assertEqual(sum(x['status']=='FAILED' for x in results),1)

    def test_formal_dates_are_rejected_without_simulation(self):
        with TemporaryDirectory() as tmp,patch.object(r.engine,'simulate') as simulation:
            p=Path(tmp)/'p.json';r.write_json(p,dict(date='2026-12-21'))
            row=r.simulate((str(p),'fixed_1701'))
            self.assertEqual(row['status'],'FAILED');simulation.assert_not_called()

    def test_ground_adapter_resets_for_next_task(self):
        r.install_adapter('ground_16')
        self.assertIsInstance(r.engine.ThreePlanChoice(2),r.EmptyPolicy)
        self.assertIs(r.engine.ControlPlane,r.LocalInformationPlane)
        r.install_adapter('global_v3_16')
        self.assertIs(r.engine.ThreePlanChoice,r.ThreePlanChoice)
        self.assertIs(r.engine.ControlPlane,r.ControlPlane)

    def test_real_toy_worker_all_retained_policy_families(self):
        base=fixture();config=asdict(base.cfg);base.close()
        config.update(sim_steps=710,decision_tile_limit_per_sat=64)
        config['scheduled_task_arrivals'][0]['release_step']=20
        real_env=r.engine.GranularityEnv
        def environment(cfg):
            with patch('sim.env.TopologyModel',FakeTopology),patch('sim.env.VisibilityCache',FakeVisibility):
                env=real_env(cfg)
            env.vis_cache.windows_at=lambda sid,t:{'gs':(0,1000)}
            return env
        with TemporaryDirectory() as tmp:
            root=Path(tmp);p=dict(date='2026-10-22',config=config,scored_task_ids=['task_0'],
                score_start_s=10,score_end_s=110,sim_duration_s=710)
            r.write_json(root/'p.json',p);r.write_json(root/'run_manifest.json',dict(hashes={}))
            models={}
            for family,actor in [('fixed',r.engine.fixed.HierarchicalActor()),('dynamic',r.engine.dynamic.DynamicActor())]:
                path=root/f'{family}.pt';torch.save(dict(state_dict=actor.state_dict()),path)
                models[f'{family}_1701']=str(path)
            r.write_json(root/'ranges.json',dict(ranges={n:{} for n in models}))
            with patch.object(r,'ROOT',root),patch.object(r,'verify_manifest',return_value=dict(models=models,ranges=str(root/'ranges.json'))),\
                 patch.object(r.engine,'verify'),patch.object(r.engine,'GranularityEnv',side_effect=environment):
                for method in ('ground_16','global_v3_16','global_v3_dynamic','fixed_1701','dynamic_1701',
                               'minload_2_4','minload_2_16','zero_2_16','local_cgs_16','local_heft_16'):
                    short=r.simulate((str(root/'p.json'),method))
                    self.assertEqual(short['status'],'PASS',short)
                    row=r.read(short['path']);r.engine.check_record(row,p)
                    self.assertTrue(row['checks']['followup600'])
                    self.assertAlmostEqual(row['resources']['whole']['total_transmitted_bytes'],
                        sum(row['resources'][s]['total_transmitted_bytes'] for s in ('warmup','main','followup')))


if __name__=='__main__':
    unittest.main()
