from concurrent.futures import Future
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest
import torch
from analysis import run_s_formal_20261029 as r
from analysis import report_s_formal_20261029 as report
from analysis import verify_s_formal_report_20261029 as verifier
from analysis.scale_feature_audit_20261027 import FeatureAudit
from test_dynamic_granularity_v2 import fixture, FakeTopology, FakeVisibility


class SFormalTests(unittest.TestCase):
    def setUp(self):
        torch.set_num_threads(1)
        saved = {k:getattr(r.engine,k) for k in ('ROOT','VALID','METHODS','LEARNED','BASELINES',
            'policy_set','check_time','verify_manifest','GranularityEnv')}
        self.addCleanup(lambda:r.engine.__dict__.update(saved))

    def test_full_sets_and_no_finetune(self):
        self.assertEqual(len(r.METHODS),20)
        self.assertEqual(len(set(r.METHODS)),20)
        self.assertEqual([len(v) for v in r.SCOPES.values()],[6,12])
        self.assertEqual(set(r.METHODS)-set(r.MAPPO)-set(r.BASELINES),{'global_v3_16','global_v3_dynamic'})
        aggregates = {n:dict(P60=.5,P600=.9,mean_completed_s=120.,R600=150.) for n in r.METHODS}
        values = report.complete_scopes(aggregates,r.SCOPES)
        for scope in values.values():
            for check in scope['criteria'].values():
                self.assertEqual(check,dict(P60=False,mean_completed_s=False,P600=True,R600=False))
        del aggregates['history_1_4']
        with self.assertRaises(AssertionError): report.complete_scopes(aggregates,r.SCOPES)

    def test_open_gate_and_second_open_rejected(self):
        with TemporaryDirectory() as tmp, patch.object(r,'ROOT',Path(tmp)), patch.object(r,'OPEN',Path(tmp)/'open.json'):
            r.write_json(r.ROOT/'manifest.json',dict(methods=r.METHODS))
            with self.assertRaises(FileNotFoundError): r.require_open(r.DATES[0])
            r.write_json(r.OPEN,dict(scope='S',manifest_sha256=r.sha(r.ROOT/'manifest.json')))
            r.require_open(r.DATES[0])
            with self.assertRaises(AssertionError): r.require_open('2026-10-22')
            with patch.object(r,'verify_manifest',return_value={'dates':r.DATES}):
                with self.assertRaises(FileExistsError): r.open_once()
            r.write_json(r.OPEN,dict(scope='M',manifest_sha256=r.sha(r.ROOT/'manifest.json')))
            with self.assertRaises(AssertionError): r.require_open(r.DATES[0])

    def test_failed_batch_drains_no_prefetch(self):
        class Pool:
            def __init__(self): self.calls=[]
            def submit(self,fn,job):
                self.calls.append(job); f=Future()
                if len(self.calls)==2: f.set_exception(RuntimeError('test'))
                else: f.set_result({'status':'PASS'})
                return f
        pool=Pool()
        with TemporaryDirectory() as tmp,patch.object(r,'ROOT',Path(tmp)):
            rows=r.batches(pool,list(range(60)))
        self.assertEqual(len(rows),9);self.assertEqual(len(pool.calls),9)
        self.assertEqual(sum(v['status']=='FAILED' for v in rows),1)

    def test_six_hour_builder_preserves_physics_and_followup(self):
        # 用既有M模板测试时长适配，不在预登记前生成S正式轨道或任务。
        cfg=r.read(r.MAIN/'config_template.json')
        with TemporaryDirectory() as tmp,patch.object(r,'require_open'),patch.object(r,'verify_manifest',return_value={'score_duration_s':21600}):
            p=r.build_episode(cfg,r.DATES[0],[],dict(start_s=1800,end_s=23400),Path(tmp),{},[])
            self.assertEqual(p['score_end_s']-p['score_start_s'],21600)
            self.assertEqual(p['score_start_s'],600)
            self.assertEqual(p['sim_duration_s']-p['score_end_s'],600)
            for k,v in cfg.items():
                if k not in ('topology','sim_steps','task_arrival_end_step','scheduled_task_arrivals'):
                    self.assertEqual(p['config'][k],v)
            with self.assertRaises(AssertionError):
                r.build_episode(cfg,r.DATES[0],[],dict(start_s=0,end_s=21600),Path(tmp),{},[])

    def test_all20_real_toy_and60_synthetic_report_with_independent_verifier(self):
        base=fixture();cfg=asdict(base.cfg);base.close()
        cfg.update(sim_steps=710,decision_tile_limit_per_sat=64)
        cfg['scheduled_task_arrivals'][0]['release_step']=20
        real_env=r.engine.GranularityEnv
        def environment(config):
            with patch('sim.env.TopologyModel',FakeTopology),patch('sim.env.VisibilityCache',FakeVisibility):
                env=real_env(config)
            env.vis_cache.windows_at=lambda sid,t:{'gs':(0,1000)}
            return env
        with TemporaryDirectory() as tmp:
            root=Path(tmp)/'s';root.mkdir();opening=root.parent/'S_TEST_OPENED.json'
            p=dict(date=r.DATES[0],config=cfg,scored_task_ids=['task_0'],score_start_s=10,score_end_s=110,sim_duration_s=710)
            r.write_json(root/'p.json',p);r.write_json(root/'run_manifest.json',dict(hashes={}))
            r.write_json(root/'manifest.json',{});r.write_json(opening,dict(scope='S',manifest_sha256=r.sha(root/'manifest.json')))
            models={}
            for name in r.MAPPO:
                actor=r.engine.fixed.HierarchicalActor() if name.startswith('fixed_') else r.engine.dynamic.DynamicActor()
                path=root/f'{name}.pt';torch.save(dict(state_dict=actor.state_dict()),path);models[name]=str(path)
            r.write_json(root/'ranges.json',dict(ranges={n:{} for n in models}))
            manifest=dict(models=models,ranges=str(root/'ranges.json'))
            with patch.object(r,'ROOT',root),patch.object(r,'OPEN',opening),patch.object(r,'verify_manifest',return_value=manifest), \
                    patch.object(r.engine,'verify'),patch.object(r.engine,'GranularityEnv',side_effect=environment):
                for method in r.METHODS:
                    short=r.simulate((str(root/'p.json'),method))
                    self.assertEqual(short['status'],'PASS',short)
                    row=r.read(short['path']);r.engine.check_record(row,p)
                    self.assertTrue(row['checks']['followup600'])
                    self.assertFalse(row['tests_sealed'])
            protocols=[];results=[]
            for date in r.DATES:
                protocol=deepcopy(p);protocol.update(date=date,score_start_s=600,score_end_s=22200,sim_duration_s=22800,input_hashes={})
                protocol['config'].update(num_sats=84,task_source_sat_ids=list(range(12)))
                path=root/f'{date}.json';r.write_json(path,protocol);protocols.append(str(path))
                for method in r.METHODS:
                    row=deepcopy(r.read(root/'validation'/r.DATES[0]/f'{method}.json'))
                    row.update(date=date,protocol=str(path),config_sha256=r.digest(report.configuration(protocol,method)))
                    for o in row['outcomes']:
                        if o['task_id']=='task_0':o['release_seconds']=620
                    for seg,duration in [('warmup',600),('main',21600),('followup',600),('whole',22800)]:
                        resource=row['resources'][seg];resource['duration_s']=duration
                        resource['helper_busy_by_node']={str(int(k)+12):v for k,v in resource['helper_busy_by_node'].items()
                            if int(k) not in cfg['task_source_sat_ids']}
                        resource['helper_utilization']=resource['helper_busy_s']/(72*duration)
                        rate=sum(g['bandwidth_mbps']/8*g.get('receive_channels',1) for g in cfg['ground_stations'])*1e6
                        resource['ground_channel_utilization']=resource['ground_bytes']/(rate*duration)
                    if method in models:
                        row.update(feature_audit=FeatureAudit({}).result(),choices={},unacted_scored_ids=['task_0'])
                    out=root/'synthetic'/date/f'{method}.json';r.write_json(out,row)
                    results.append(dict(status='PASS',path=str(out),sha256=r.sha(out)))
            source_hashes={}
            for dest in ('m_formal_test_20261027/FINAL_AUDIT.json','l_formal_test_20261028/independent_audit_20261028/FINAL_AUDIT.json'):
                path=root.parent/dest;r.write_json(path,dict(status='PASS',day_equal={n:dict(P60=.2,P600=.8,R600=200.,unfinished=3) for n in r.MAPPO}))
                source_hashes[str(path)]=r.sha(path)
            r.write_json(root/'manifest.json',dict(**manifest,methods=r.METHODS,dates=r.DATES,hashes=source_hashes,
                score_duration_s=21600,comparison_scopes=r.SCOPES,bootstrap=dict(seed=20261027,draws=10000)))
            r.write_json(root/'run_manifest.json',dict(protocols=protocols,hashes={}))
            r.write_json(root/'final_status.json',dict(status='COMPLETE',results=results))
            with patch.object(report,'ROOT',root),patch.object(report,'OUT',root/'independent_audit_20261029'),patch.object(report,'verify'),patch.object(verifier,'ROOT',root):
                report.main();verifier.main()
            check=r.read(root/'independent_audit_20261029/FOLLOWUP_VERIFICATION.json')
            self.assertEqual(check['status'],'PASS');self.assertEqual(check['metric_intervals'],336)
            self.assertTrue(check['complete_scopes']);self.assertTrue(check['plot_nonblank'])


if __name__=='__main__': unittest.main()
