from concurrent.futures import Future
from copy import deepcopy
from dataclasses import asdict, replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
import torch

from analysis import scaled_ground_adapter_20261029 as a
from analysis import run_ground_scaling_20261029 as r
from analysis import report_ground_scaling_20261029 as report
from analysis import verify_ground_scaling_20261029 as verifier
from analysis.dynamic_granularity_v2 import GranularityEnv
from analysis.local_information_baselines import LocalInformationPlane
from analysis.sar_three_plan import RawImageAction
from analysis.run_sar_frozen import invariants
from sim.entities import Action, ActionType, TileState
from test_dynamic_granularity_v2 import fixture, FakeTopology, FakeVisibility


def config(stations=2):
    base=fixture();cfg=base.cfg;base.close()
    return replace(cfg,ground_input_relay=True,ground_stations=[
        dict(id='gs',bandwidth_mbps=8,compute_rate=1),
        *[dict(id=f'gs_{i}',bandwidth_mbps=8,compute_rate=1) for i in range(1,stations)]])


def environment(cfg, kind=a.SharedEnv):
    with patch('sim.env.TopologyModel',FakeTopology),patch('sim.env.VisibilityCache',FakeVisibility):
        env=kind(cfg)
    env.vis_cache.windows_at=lambda sid,t:{gid:(0,1000) for gid in env.ground_stations}
    return env


class GroundScalingTests(unittest.TestCase):
    def setUp(self):torch.set_num_threads(1)

    def test_derive_changes_only_stations_and_retains_tasks(self):
        p=dict(config=asdict(config(1)),events=[{'id':'same'}],scored_task_ids=['task_0'])
        for scale,count in [('S',1),('M',2),('L',3)]:
            q=r.derive_protocol(p,scale,True)
            self.assertEqual(len(q['config']['ground_stations']),count)
            self.assertEqual(q['config']['ground_stations'][0],p['config']['ground_stations'][0])
            self.assertEqual({k:v for k,v in q['config'].items() if k!='ground_stations'},
                             {k:v for k,v in p['config'].items() if k!='ground_stations'})
        self.assertEqual(len(r.METHODS),20)
        self.assertEqual([len(x) for x in r.SCOPES.values()],[6,12])

    def test_pool_busy_alias_survives_deepcopy(self):
        b=a.PoolBusy({0:2.,('ground','a'):3.,('ground','b'):3.})
        b[('ground','b')]=7
        self.assertEqual(b[('ground','a')],7)
        c=deepcopy(b);c[('ground','a')]=9
        self.assertEqual(c[('ground','b')],9);self.assertEqual(b[('ground','b')],7)

    def test_station_selection_public_ties_and_future_windows(self):
        env=environment(config());self.addCleanup(env.close);env.step([])
        state=env.export_state()
        for row in state.satellites.values():row['next_gs_windows']={'gs':(20.,100.),'gs_1':(0.,100.)}
        for raw in (False,True):self.assertEqual(a.choose_station(state,raw=raw),'gs_1')
        hidden=deepcopy(state)
        for row in hidden.satellites.values():
            row.update(committed_compute_s=99999.,ground_backlog_mb={'gs_1':99999.},mem_remaining_gb=0.)
        for row in hidden.ground_stations.values():row['committed_compute_s']=99999.
        for raw in (False,True):self.assertEqual(a.choose_station(state,raw=raw),a.choose_station(hidden,raw=raw))
        for row in state.satellites.values():row['next_gs_windows']={'gs':(0.,100.),'gs_1':(0.,100.)}
        self.assertEqual(a.choose_station(state,raw=True),'gs')

    def test_raw_pinned_station_and_shared_commitment(self):
        env=environment(config());self.addCleanup(env.close);env.step([])
        commands=[RawImageAction(t,ActionType.OFFLOAD,target_gs_id='gs_1',raw_hops=2) for t in env.tasks['task_0'].tile_ids]
        env.step(commands)
        state=env.export_state()
        self.assertEqual(state.ground_stations['gs']['committed_compute_s'],state.ground_stations['gs_1']['committed_compute_s'])
        self.assertGreater(state.ground_stations['gs']['committed_compute_s'],0.)
        self.assertFalse(state.tiles)
        for _ in range(50):env.step([])
        self.assertIsNotNone(env.tasks['task_0'].completion_time)
        self.assertEqual(env.metrics.network_sent_mb.get('ground:gs',0.),0.)
        self.assertGreater(env.metrics.network_sent_mb['ground:gs_1'],0.)
        self.assertAlmostEqual(sum(env.ground_compute_busy_time.values()),16.)
        self.assertTrue(invariants(env)['passed'])

    def test_two_stations_do_not_double_compute_and_no_duplicate_commitment(self):
        env=environment(config());self.addCleanup(env.close);env.step([])
        tids=env.tasks['task_0'].tile_ids
        commands=[Action(t,ActionType.OFFLOAD,target_gs_id='gs' if i%2 else 'gs_1',relay_to_ground=True) for i,t in enumerate(tids)]
        for i in range(50):
            env.step(commands if i==0 else [])
            self.assertLessEqual(len(env.ground_tile_running),1)
            self.assertLessEqual(sum(env.ground_compute_busy_time.values()),env.time)
            state=env.export_state()
            self.assertEqual(state.ground_stations['gs']['committed_compute_s'],state.ground_stations['gs_1']['committed_compute_s'])
        self.assertTrue(all(t.state==TileState.DONE for t in env.tiles.values()))
        self.assertAlmostEqual(sum(env.ground_compute_busy_time.values()),16.)
        self.assertEqual(env.export_state().ground_stations['gs']['committed_compute_s'],0.)

    def test_single_station_observation_and_raw_execution_identity(self):
        cfg=config(1);old=environment(cfg,GranularityEnv);new=environment(cfg)
        self.addCleanup(old.close);self.addCleanup(new.close)
        for env in (old,new):env.step([])
        plane=LocalInformationPlane({0});plane.history_hops=2
        state=plane.view(old.export_state(),0,2)
        original=a.original_observation(state,'task_0',state.tiles)
        with a.installed(r.engine):
            self.assertEqual(a.fixed.build_observation(state,'task_0',state.tiles),original)
        commands=[RawImageAction(t,ActionType.OFFLOAD,raw_hops=2) for t in old.tasks['task_0'].tile_ids]
        for i in range(50):
            old.step(deepcopy(commands) if i==0 else []);new.step(deepcopy(commands) if i==0 else [])
            self.assertEqual(old.export_state(),new.export_state())
        self.assertEqual(old.tasks,new.tasks);self.assertEqual(old.tiles,new.tiles)
        self.assertEqual(old.metrics.network_sent_mb,new.metrics.network_sent_mb)

    def test_failed_batch_drains_and_does_not_prefetch(self):
        class Pool:
            def __init__(self):self.calls=[]
            def submit(self,fn,job):
                self.calls.append(job);f=Future();f.set_result(dict(status='FAILED' if len(self.calls)==2 else 'PASS'));return f
        pool=Pool();results=[]
        with TemporaryDirectory() as tmp,patch.object(r,'ROOT',Path(tmp)):
            self.assertFalse(r.batches(pool,[('L_scaled','p','m')]*20,results))
        self.assertEqual(len(pool.calls),9);self.assertEqual(len(results),9)

    def test_all20_multi_station_real_toy_and_single_station_identity(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);models={}
            for name in r.MAPPO:
                torch.manual_seed(int(name.rsplit('_',1)[1]))
                actor=r.engine.fixed.HierarchicalActor() if name.startswith('fixed_') else r.engine.dynamic.DynamicActor()
                path=root/f'{name}.pt';torch.save(dict(state_dict=actor.state_dict()),path);models[name]=str(path)
            r.write_json(root/'ranges.json',dict(ranges={n:{} for n in models}))
            manifest=dict(models=models,ranges=str(root/'ranges.json'))
            for stations in (1,2,3):
                cfg=asdict(config(stations));cfg.update(sim_steps=710,decision_tile_limit_per_sat=64)
                cfg['scheduled_task_arrivals'][0]['release_step']=20
                p=dict(date=r.DATES[0],config=cfg,scored_task_ids=['task_0'],score_start_s=10,score_end_s=110,sim_duration_s=710)
                r.write_json(root/f'p{stations}.json',p)
                for adapted in ((False,True) if stations==1 else (True,)):
                    dest=root/f'{stations}_{adapted}';dest.mkdir();r.write_json(dest/'run_manifest.json',dict(hashes={}))
                    with patch.object(r.engine,'ROOT',dest),patch.object(r.engine,'VALID',r.DATES),patch.object(r.engine,'METHODS',r.METHODS), \
                         patch.object(r.engine,'verify_manifest',return_value=manifest),patch.object(r.engine,'check_time'),patch.object(r.engine,'verify'):
                        from contextlib import nullcontext
                        with a.installed(r.engine) if adapted else nullcontext():
                            # 保留真实适配器，只替换几何输入为确定性玩具。
                            factory=r.engine.GranularityEnv
                            def make(cfg):
                                with patch('sim.env.TopologyModel',FakeTopology),patch('sim.env.VisibilityCache',FakeVisibility):env=factory(cfg)
                                env.vis_cache.windows_at=lambda sid,t:{gid:(0,1000) for gid in env.ground_stations}
                                return env
                            with patch.object(r.engine,'GranularityEnv',side_effect=make):
                                for method in r.METHODS:
                                    short=r.engine.simulate((str(root/f'p{stations}.json'),method))
                                    self.assertEqual(short['status'],'PASS',short)
                                    row=r.read(short['path']);r.check_record(row,p)
                                    if stations==1 and adapted:
                                        before=r.read(root/'1_False/validation'/r.DATES[0]/f'{method}.json')
                                        for key in ('outcomes','curve','latency','R600','granularity','action_trace_sha256','feature_audit','choices'):
                                            self.assertEqual(before.get(key),row.get(key),(method,key))
                                    if stations>1:
                                        report.audit_resources(row,p)

    def test_synthetic_full_report_and_independent_audit(self):
        from analysis.report_m_formal_20261027 import metrics
        from analysis.report_main_posthoc import score
        from analysis.scale_feature_audit_20261027 import FeatureAudit
        with TemporaryDirectory() as tmp:
            root=Path(tmp);models={}
            for name in r.MAPPO:
                path=root/f'{name}.json';r.write_json(path,{'synthetic_checkpoint':name});models[name]=str(path)
            r.write_json(root/'ranges.json',dict(ranges={n:{} for n in models}))
            manifest=dict(models=models,ranges=str(root/'ranges.json'),hashes={},comparison_scopes=r.SCOPES,
                reused={},originals={},dates=r.DATES,methods=r.METHODS)
            results=[]
            missing=set(r.BASELINES)-{'minload_2_4','minload_2_16','zero_2_16'}
            for scale in ('S','M','L'):
                manifest['reused'][scale]={};manifest['originals'][scale]={}
                for day,date in enumerate(r.DATES):
                    p=dict(date=date,config=asdict(config(1)),events=[],scored_task_ids=['task_0','task_1'],
                        score_start_s=600,score_end_s=700,sim_duration_s=1300,origin_absolute_s=0,input_hashes={})
                    path=root/'old'/scale/date/'protocol.json';r.write_json(path,p)
                    manifest['originals'][scale][date]=str(path);manifest['reused'][scale][date]={}
                    for setting in ('single','scaled'):
                        if scale=='S' and setting=='scaled':continue
                        q=r.derive_protocol(p,scale,setting=='scaled')
                        qpath=root/scale/setting/date/'protocol.json';r.write_json(qpath,q)
                        for index,method in enumerate(r.METHODS):
                            is_new=setting=='scaled' or (scale=='L' and method in missing)
                            stage=f'{scale}_scaled' if setting=='scaled' else 'L_single_completion'
                            path=root/scale/setting/date/f'{method}.json'
                            rows=[dict(task_id=f'task_{i}',status='completed',latency_seconds=40.+index+day*10+i*200+(5 if setting=='scaled' else 0),
                                observed_age_seconds=650.,release_seconds=620.) for i in range(2)]
                            measured=metrics(rows)
                            res={}
                            for seg,duration in [('warmup',600),('main',100),('followup',600),('whole',1300)]:
                                res[seg]=dict(duration_s=duration,ground_bytes=duration*1000.,raw_ground_bytes=duration*1000.,
                                    result_ground_bytes=0.,total_transmitted_bytes=duration*1000.,control_bytes=0.,
                                    raw_isl_bytes=0.,result_isl_bytes=0.,helper_busy_s=0.,helper_utilization=0.,
                                    helper_busy_by_node={'1':0.},ground_channel_utilization=.001/len(q['config']['ground_stations']))
                                if is_new:
                                    res[seg]['stations']={g['id']:dict(bytes=duration*1000./len(q['config']['ground_stations']),
                                        utilization=.001/len(q['config']['ground_stations'])) for g in q['config']['ground_stations']}
                                    res[seg].update(ground_pool_busy_s=0.,ground_pool_utilization=0.)
                            row=dict(status='PASS',checks={'passed':True},method=method,date=date,protocol=str(qpath),zero_shot=True,
                                config_sha256=r.digest(r.configuration(q,method)),outcomes=rows,curve=[score(rows,d) for d in report.DEADLINES],
                                R600=measured['R600'],latency={k:measured[k] for k in ('mean_completed_s','P50_completed_s','P90_completed_s','unfinished')},
                                resources=res,granularity={t:dict(actual=4 if method.endswith('_4') else 16,
                                    requested=4 if method.endswith('_4') else 16,packet_mb=1.,unique_mb=.5) for t in p['scored_task_ids']})
                            if method in models:row.update(checkpoint=models[method],checkpoint_sha256=r.sha(models[method]),feature_audit=FeatureAudit({}).result())
                            if is_new:row.update(stage=stage,scale=scale,setting=setting)
                            r.write_json(path,row)
                            if is_new:results.append(dict(status='PASS',path=str(path),sha256=r.sha(path),stage=stage))
                            else:manifest['reused'][scale][date][method]=str(path)
            r.write_json(root/'final_status.json',dict(status='COMPLETE',results=results))
            self.assertEqual(len(results),147)
            with patch.object(r,'verify_manifest',return_value=manifest),patch.object(report,'ROOT',root), \
                 patch.object(verifier,'ROOT',root),patch.object(report,'verify'):
                report.main();verifier.main()
            checked=r.read(root/'report/INDEPENDENT_AUDIT.json')
            self.assertEqual(checked['status'],'PASS');self.assertEqual(checked['intervals'],2820)
            partial=root/'partial'
            r.write_json(partial/'final_status.json',dict(status='PARTIAL',results=[x for x in results if x['stage'].startswith('L')]))
            with patch.object(r,'verify_manifest',return_value=manifest),patch.object(report,'ROOT',partial), \
                 patch.object(verifier,'ROOT',partial),patch.object(report,'verify'):
                report.main();verifier.main()
            checked=r.read(partial/'report/FINAL_AUDIT.json')
            self.assertEqual(checked['included_scales'],['S','L']);self.assertEqual(set(checked['missing']),{'M'})


if __name__=='__main__':unittest.main()
