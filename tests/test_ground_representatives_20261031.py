from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict, replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
import torch

from analysis import run_ground_representatives_20261031 as r
from analysis import run_ground_scaling_20261029 as old
from analysis import run_m_formal_20261027 as formal
from analysis.report_ground_scaling_20261029 import audit_resources
from analysis.run_sar_frozen import invariants
from test_ground_scaling_20261029 import config, environment, FakeTopology, FakeVisibility


class RepresentativeTests(unittest.TestCase):
    def setUp(self): torch.set_num_threads(1)

    def test_scope(self):
        self.assertEqual(sum(len(m)*3 for _,m in r.STAGES.values()),45)
        self.assertEqual(len(r.METHODS),6)
        self.assertTrue(all('_representatives' in x for x in r.STAGES))

    def test_ground_routes_second_station_and_waits_for_whole_image(self):
        cfg=replace(config(3),pipeline_mode='ground_unique_relay')
        cfg.ground_stations=[{**g,'bandwidth_mbps':.000008} for g in cfg.ground_stations]
        env=environment(cfg,r.GroundOnlyEnv);self.addCleanup(env.close)
        env.vis_cache.windows_at=lambda sid,t:{'gs':(500,1000),'gs_1':(0,1000),'gs_2':(500,1000)}
        saw_partial=False
        for _ in range(80):
            env.step([])
            task=env.tasks.get('task_0')
            if not task: continue
            arrivals=[env.tiles[t].timestamps.end_tx is not None for t in task.tile_ids]
            if any(arrivals) and not all(arrivals):
                saw_partial=True
                self.assertEqual(sum(env.ground_compute_busy_time.values()),0)
            self.assertLessEqual(len(env.ground_tile_running),1)
        self.assertTrue(saw_partial)
        self.assertEqual(env._dist_pipeline.raw_stations['task_0'],'gs_1')
        self.assertIsNotNone(env.tasks['task_0'].completion_time)
        self.assertGreater(env.metrics.network_sent_mb['ground:gs_1'],0)
        self.assertEqual(env.metrics.network_sent_mb.get('ground:gs',0),0)
        self.assertTrue(invariants(env)['passed'])

    def test_six_methods_full_toy_followup_and_single_identity(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp)
            for stations in (1,2,3):
                cfg=asdict(config(stations));cfg.update(sim_steps=710,decision_tile_limit_per_sat=64)
                cfg['scheduled_task_arrivals'][0]['release_step']=20
                protocol=dict(date=old.DATES[0],config=cfg,scored_task_ids=['task_0'],score_start_s=10,
                              score_end_s=110,sim_duration_s=710)
                path=root/f'p{stations}.json';old.write_json(path,protocol)
                for method in r.METHODS:
                    for variant in (('original','new') if stations==1 else ('new',)):
                        dest=root/f'{stations}_{variant}_{method}';dest.mkdir()
                        old.write_json(dest/'run_manifest.json',dict(hashes={}))
                        with ExitStack() as stack:
                            stack.enter_context(r.installed(method,stations>1))
                            if variant=='original':formal.install_worker_adapter(method)
                            engine=old.engine;factory=engine.GranularityEnv
                            def make(cfg):
                                with patch('sim.env.TopologyModel',FakeTopology),patch('sim.env.VisibilityCache',FakeVisibility):env=factory(cfg)
                                env.vis_cache.windows_at=lambda sid,t:{gid:(0,1000) for gid in env.ground_stations}
                                return env
                            for key,value in dict(ROOT=dest,VALID=old.DATES,METHODS=r.METHODS,
                                verify_manifest=lambda:dict(models={}),check_time=lambda:None,
                                verify=lambda p:None,GranularityEnv=make).items():
                                stack.enter_context(patch.object(engine,key,value))
                            short=engine.simulate((str(path),method))
                        self.assertEqual(short['status'],'PASS',short)
                        row=old.read(short['path']);old.check_record(row,protocol);audit_resources(row,protocol)
                        if stations==1 and variant=='new':
                            before=old.read(root/f'1_original_{method}'/'validation'/old.DATES[0]/f'{method}.json')
                            for key in ('outcomes','curve','latency','R600','granularity','action_trace_sha256','choices'):
                                self.assertEqual(before.get(key),row.get(key),(method,key))

    def test_report_and_independent_audit_full_scope(self):
        from analysis import report_ground_representatives_20261031 as report
        from analysis import verify_ground_representatives_20261031 as audit
        from analysis.report_m_formal_20261027 import metrics, DEADLINES
        from analysis.report_main_posthoc import score
        with TemporaryDirectory() as tmp:
            root=Path(tmp);paths={};provenance={};jobs=[];ownership={}
            reg=root/'REGISTRATION.json';old.write_json(reg,{'synthetic':True})
            for scale in 'SML':
                paths[scale]={}
                for setting in ('single','scaled'):
                    if scale=='S' and setting=='scaled':
                        paths[scale][setting]=deepcopy(paths[scale]['single']);continue
                    paths[scale][setting]={}
                    for day,date in enumerate(old.DATES):
                        cfg=asdict(config(1 if setting=='single' else 2 if scale=='M' else 3))
                        p=dict(date=date,config=cfg,scored_task_ids=['task_0','task_1'],score_start_s=600,
                               score_end_s=700,sim_duration_s=1300)
                        pp=root/scale/setting/date/'protocol.json';old.write_json(pp,p)
                        paths[scale][setting][date]={}
                        for i,method in enumerate((*old.METHODS,*(r.METHODS if scale!='S' else ()))):
                            rows=[dict(task_id=f'task_{j}',status='completed',latency_seconds=40+i+day*10+j*200+(5 if setting=='scaled' else 0),
                                observed_age_seconds=650,release_seconds=620) for j in range(2)]
                            measured=metrics(rows);res={}
                            for seg,duration in [('warmup',600),('main',100),('followup',600),('whole',1300)]:
                                res[seg]=dict(duration_s=duration,ground_bytes=0,raw_ground_bytes=0,result_ground_bytes=0,
                                    total_transmitted_bytes=0,control_bytes=0,raw_isl_bytes=0,result_isl_bytes=0,helper_busy_s=0,
                                    helper_utilization=0,helper_busy_by_node={},ground_channel_utilization=0,
                                    stations={g['id']:dict(bytes=0,utilization=0) for g in cfg['ground_stations']},ground_pool_busy_s=0,ground_pool_utilization=0)
                            path=root/scale/setting/date/f'{method}.json'
                            row=dict(status='PASS',checks={'passed':True},date=date,method=method,protocol=str(pp),
                                config_sha256=old.digest(old.configuration(p,method)),outcomes=rows,curve=[score(rows,d) for d in DEADLINES],
                                R600=measured['R600'],latency={k:measured[k] for k in ('mean_completed_s','unfinished')},resources=res)
                            isnew=method in r.METHODS and (scale in 'ML' and setting=='scaled' or scale=='L' and method in r.STAGES['L_single_representatives'][1])
                            if isnew:
                                stage=f'{scale}_{setting}_representatives';job=[stage,str(pp),method];jid=r.queue.job_id(job)
                                row.update(stage=stage,registration_sha256=r.sha(reg),supplement_adapter_sha256=r.sha(r.__file__))
                                jobs.append(dict(id=jid,job=job,output=str(path)))
                            old.write_json(path,row);provenance[str(path)]=r.sha(path)
                            if isnew:ownership[str(path)]=dict(owner='local:test',job_id=jid,sha256=r.sha(path))
                            paths[scale][setting][date][method]=str(path)
            old.write_json(root/'EXPERIMENT.json',dict(jobs=jobs))
            old.write_json(root/'DISTRIBUTED_AUDIT.json',dict(records=ownership))
            with patch.object(r,'ROOT',root),patch.object(r,'REG',reg),patch.object(r,'SPEC',root/'EXPERIMENT.json'), \
                 patch.object(r,'verify'),patch.object(report,'paths_and_provenance',return_value=(paths,provenance)):
                report.main();audit.main()
            self.assertEqual(old.read(root/'report/INDEPENDENT_AUDIT.json')['intervals'],2310)


if __name__=='__main__':unittest.main()
