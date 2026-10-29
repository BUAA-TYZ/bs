from dataclasses import asdict, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest
import os
import shutil
from copy import deepcopy
import numpy as np
import torch

from analysis import run_m_formal_20261027 as r
from analysis import run_scale_validation_20261027 as old
from analysis import report_m_formal_20261027 as report
from test_dynamic_granularity_v2 import fixture, FakeTopology, FakeVisibility


class FormalTests(unittest.TestCase):
    def setUp(self):
        torch.set_num_threads(1)
        self.saved={k:getattr(old,k) for k in ('ROOT','VALID','METHODS','LEARNED','BASELINES','EXTRAS',
            'GranularityEnv','policy_set','ThreePlanChoice','ControlPlane','check_time','verify_manifest')}
        self.addCleanup(lambda:old.__dict__.update(self.saved))

    def test_scope_and_sealed_gate(self):
        self.assertEqual(len(set(r.METHODS)),33)
        self.assertEqual(len(set(r.L_METHODS)),14)
        with TemporaryDirectory() as tmp, patch.object(r,'OPEN',Path(tmp)/'missing.json'):
            with self.assertRaises(FileNotFoundError):r.require_open('2026-12-21')
            with self.assertRaises(AssertionError):r.require_open('2026-12-17')

    def test_arrival_adapter_is_identical(self):
        windows=[dict(start_s=100,end_s=1000,source_sat_id=0,target_id=0,window_id=0),
                 dict(start_s=200,end_s=1100,source_sat_id=1,target_id=1,window_id=1)]
        expected=r.preparation.arrivals(windows,'2026-10-22')
        with patch.object(r,'require_open'):
            self.assertEqual(expected,r.test_arrivals(windows,'2026-10-22'))

    def test_strict_criteria_ties_and_pareto_directions(self):
        a=dict(P60=.6,P600=.9,mean_completed_s=100,R600=80)
        self.assertEqual(report.criteria(a,a),dict(P60=False,mean_completed_s=False,P600=True,R600=False))
        self.assertEqual(set(report.wins(a,a).values()),{'tie'})
        values={'a':a,'b':dict(a,P600=.91,R600=70),'c':dict(a,P60=.7,P600=.85,R600=100)}
        self.assertEqual(set(report.pareto(values,'P600')),{'b','c'})
        self.assertEqual(set(report.pareto(values,'R600')),{'b','c'})
        self.assertEqual(report.paired_bootstrap([2,3,4],[1,2,3],np.array([[0,0,0],[0,1,2],[2,2,2]]))['ci95'],[1.,1.])

    def test_fifo_does_not_use_hidden_remote_memory(self):
        env=fixture();self.addCleanup(env.close)
        plane=r.LocalInformationPlane([0]);plane.history_hops=2
        state=plane.view(env.export_state(),0,2)
        policy=r.LocalFifo(env);seen=[]
        def capture(view, state):
            seen.append({sid:s.mem_capacity_gb-s.mem_used_gb for sid,s in view.satellites.items()})
            return []
        with patch.object(policy.policy,'select_actions',side_effect=capture):
            policy.select_actions(state)
            env.satellites[1].mem_used_gb=env.satellites[1].mem_capacity_gb
            policy.select_actions(state)
        self.assertEqual(seen[0],seen[1])
        self.assertAlmostEqual(seen[0][1],state.satellites[1]['mem_remaining_gb'])

    def test_real_toy_execution_all_new_families(self):
        base=fixture();config=asdict(base.cfg);base.close()
        config.update(sim_steps=710,decision_tile_limit_per_sat=64)
        config['scheduled_task_arrivals'][0]['release_step']=20
        real_env=r.GranularityEnv
        def environment(cfg):
            with patch('sim.env.TopologyModel',FakeTopology),patch('sim.env.VisibilityCache',FakeVisibility):
                env=real_env(cfg)
            env.vis_cache.windows_at=lambda sid,t:{'gs':(0,1000)}
            env.topology.sat_recs={i:i for i in env.satellites};env.topology.t0=None
            return env
        with TemporaryDirectory() as tmp:
            root=Path(tmp);p=dict(date='2026-12-21',config=config,scored_task_ids=['task_0'],
                score_start_s=10,score_end_s=110,sim_duration_s=710)
            r.write_json(root/'p.json',p);r.write_json(root/'run_manifest.json',dict(hashes={}))
            r.write_json(root/'opening.json',{})
            models={}
            for family,actor in [('fixed',old.fixed.HierarchicalActor()),('dynamic',old.dynamic.DynamicActor())]:
                path=root/f'{family}.pt';torch.save(dict(state_dict=actor.state_dict()),path)
                for seed in r.SEEDS:
                    models[f'{family}_{seed}']=str(path);models[f'{family}_bc_{seed}']=str(path)
            r.write_json(root/'ranges.json',dict(ranges={n:{} for n in models}))
            manifest=dict(models=models,ranges=str(root/'ranges.json'))
            with patch.object(r,'ROOT',root),patch.object(r,'OPEN',root/'opening.json'),\
                patch.object(r,'verify_manifest',return_value=manifest),patch.object(r,'require_open'),\
                patch.object(r,'GranularityEnv',side_effect=environment),patch.object(old,'verify'),\
                patch('analysis.run_calibration_capacity.position_km',side_effect=lambda sid,t0,t:[sid,0,0]):
                for method in r.METHODS:
                    short=r.simulate((str(root/'p.json'),method))
                    self.assertEqual(short['status'],'PASS',short)
                    row=r.read(short['path']);old.check_record(row,p)
                    self.assertFalse(row['tests_sealed']);self.assertTrue(row['l_tests_sealed'])
                    self.assertTrue(row['checks']['followup600'])
                    self.assertAlmostEqual(row['resources']['whole']['total_transmitted_bytes'],
                        sum(row['resources'][s]['total_transmitted_bytes'] for s in ('warmup','main','followup')))
                protocols=[];results=[]
                for date in r.DATES:
                    protocol=dict(p,date=date);path=root/f'{date}.json'
                    r.write_json(path,protocol);protocols.append(str(path))
                    for method in r.METHODS:
                        row=deepcopy(r.read(root/'validation'/'2026-12-21'/f'{method}.json'))
                        row.update(date=date,protocol=str(path))
                        path_out=root/'validation'/date/f'{method}.json';r.write_json(path_out,row)
                        results.append(dict(status='PASS',path=str(path_out),sha256=r.sha(path_out)))
                r.write_json(root/'manifest.json',dict(models=models,hashes={},methods=r.METHODS,
                    bootstrap=dict(seed=20261027,draws=10000),comparison_scopes=dict(
                        fixed_six_local_16=[m for m in r.BASELINES if m.endswith('_16')],
                        dynamic_twelve_local_4_16=r.BASELINES)))
                r.write_json(root/'run_manifest.json',dict(protocols=protocols,hashes={}))
                r.write_json(root/'simulation_status.json',dict(status='COMPLETE',results=results))
                with patch.object(report,'verify'):
                    report.report(root)
                self.assertEqual(r.read(root/'FINAL_AUDIT.json')['status'],'PASS')
                self.assertEqual(len(r.read(root/'PAIRED_COMPARISONS.json')),6)
                for name in ('tradeoff_P600.png','tradeoff_R600.png','curves.png'):
                    self.assertGreater((root/name).stat().st_size,10000)
                    if os.environ.get('M_REPORT_SMOKE_OUTPUT'):
                        dest=Path(os.environ['M_REPORT_SMOKE_OUTPUT']);dest.mkdir(exist_ok=True)
                        shutil.copy2(root/name,dest/name)


if __name__=='__main__':unittest.main()
