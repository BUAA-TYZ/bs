"""L训练日资源探针；先冻结输入，再生成首高峰小时，单池运行。"""
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import argparse
import hashlib
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

import torch
from analysis.register_scale_generalization_20261025 import tle
from analysis.prepare_main_bc_stage2 import source_windows, arrivals, peak_window, fixture, measure_merge, digest
from analysis.run_local_comparison_20261027 import policy_set, local_actions
from analysis.run_dynamic_heuristics_v2 import MAIN, read, sha, GranularityEnv, ThreePlanChoice, DynamicThreePlan, LocalInformationPlane, config_from_dict, decision_state, invariants
from analysis.run_dynamic_learning import write_json
from analysis.run_main_rl_stage1 import action_digest_update
from analysis.run_load_pressure_short import verify
from analysis.run_main_local_panel import verify_main
from analysis.run_sar_frozen import peak_rss_gb
from analysis import main_bc_interface as fixed
from analysis import dynamic_learning_interface as dynamic
from sim.control import ControlPlane
from sim.orbit import load_satellites, parse_start_time_utc, position_km

ROOT=MAIN/'scale_probe_20261027'
PROTOCOL=MAIN/'SCALE_PROBE_PROTOCOL_20261027.md'
DATE='2026-10-16'
METHODS=tuple(f'{family}_{s}' for family in ('fixed','dynamic') for s in (1701,1702,1703))+('global_v3_16','global_v3_dynamic','local_heft_16')
END=datetime(2026,10,29,16,tzinfo=timezone.utc)


def scale_config(sar,compute):
    assert (sar,compute) in ((12,72),(24,144),(48,288),(36,216))
    c=read(MAIN/'config_template.json'); c['num_sats']=sar+compute; c['task_source_sat_ids']=list(range(sar))
    c['topology']['tle_lines']=[list(row) for row in tle(sar,compute)]
    c['topology']['neighbor_limits']=[1]*sar+[4]*compute
    per=compute//12
    c['topology']['preferred_edges']=sorted({tuple(sorted((sar+plane*per+i,sar+plane*per+(i+1)%per))) for plane in range(12) for i in range(per)})
    c['topology']['preferred_edges']=[list(edge) for edge in c['topology']['preferred_edges']]
    config_from_dict(c)
    return c


def check_time():
    if datetime.now(timezone.utc)>=END: raise TimeoutError('Resource probe deadline')


def verify_registration():
    r=read(ROOT/'registration.json')
    for p,h in r['hashes'].items(): assert sha(p)==h,p
    return r


def episode_config(template,date,events,selected,folder,hashes,daily_paths):
    assert date==DATE
    start,end=selected['start_s'],selected['end_s'];origin=max(0,int((start-600)//60*60));duration=int(end+600-origin)
    cfg=deepcopy(template);cfg['topology']['start_time_utc']=(datetime.fromisoformat(date).replace(tzinfo=timezone.utc)+timedelta(seconds=origin)).strftime('%Y-%m-%dT%H:%M:%SZ')
    cfg.update(sim_steps=round(duration/cfg['dt']),task_arrival_end_step=round((end-origin)/cfg['dt']),scheduled_task_arrivals=[])
    details=[]
    for index,e in enumerate(events):
        if not origin<=e['ready_s']<end: continue
        task=f'task_{len(details)}';truth=fixture(e['mode'],e['truth_seed'],16);merge=measure_merge(truth)
        write_json(folder/'truth'/f'{task}.json',truth)
        cfg['scheduled_task_arrivals'].append(dict(release_step=round((e['ready_s']-origin)/cfg['dt']),source_sat_id=e['source_sat_id'],
            image_tiling=truth['tiling'],window_result_bytes=[w['output_bytes'] for w in truth['windows']],aggregation_delay_s=merge['median_seconds']))
        details.append(dict(task_id=task,daily_event_index=index,**e,scored=e['ready_s']>=start,truth_sha256=digest(truth),merge_reference=merge))
    allowed={'topology','sim_steps','task_arrival_end_step','scheduled_task_arrivals'}
    assert all(cfg[k]==v for k,v in template.items() if k not in allowed)
    assert all(cfg['topology'][k]==v for k,v in template['topology'].items() if k!='start_time_utc')
    config_from_dict(cfg); sats=load_satellites(cfg['topology']['tle_lines'])
    midnight=parse_start_time_utc(date+'T00:00:00Z');shifted=parse_start_time_utc(cfg['topology']['start_time_utc'])
    sar=len(cfg['task_source_sat_ids'])
    for sid in (0,sar-1,sar,len(sats)-1):
        for t in (0,start-origin,duration):assert position_km(sats[sid],midnight,origin+t)==position_km(sats[sid],shifted,t)
    original=read(read(MAIN/'bc_stage2_20261022/manifest.json')['train'][0])
    paths=daily_paths+list((folder/'truth').glob('*.json'))
    p=dict(date=date,seed=1701,score_start_s=int(start-origin),score_end_s=int(end-origin),origin_absolute_s=origin,
        sim_duration_s=duration,followup_s=600,D_seconds=60,D_max=600,config=cfg,config_sha256=digest(cfg),events=details,
        peak=selected,scored_task_ids=[e['task_id'] for e in details if e['scored']],frozen_protocol=original['frozen_protocol'],
        input_hashes={**hashes,**{str(p):sha(p) for p in paths}},no_retry=True,resource_probe_only=True)
    write_json(folder/'protocol.json',p);verify(p);return p


def prepare():
    assert not ROOT.exists();check_time();verify_main()
    registration=read(MAIN/'scale_generalization_20261025/registration.json')
    for p,h in registration['input_hashes'].items():assert sha(p)==h,p
    assert read(MAIN/'local_comparison_20261027/independent_audit_20261027/AUDIT.json')['status']=='PASS'
    assert scale_config(24,144)==read(MAIN/'config_template.json')
    cfg=scale_config(48,288);assert digest(cfg['topology']['tle_lines'])==registration['scenario_shapes']['L']['tle_formula_sha256']
    hashes=dict(read(MAIN/'dynamic_learning_20261026/manifest.json')['hashes'])
    for p in (Path(__file__),Path('tests/test_scale_probe_20261027.py'),PROTOCOL,Path('analysis/register_scale_generalization_20261025.py'),
        Path('analysis/run_local_comparison_20261027.py'),Path('analysis/run_dynamic_learning.py'),MAIN/'scale_generalization_20261025/registration.json',
        MAIN/'local_comparison_20261027/independent_audit_20261027/AUDIT.json',MAIN/'dynamic_learning_20261026/BEST_CHECKPOINTS.json'):
        hashes[str(p)]=sha(p)
    selected=read(MAIN/'dynamic_learning_20261026/BEST_CHECKPOINTS.json'); models={}
    for seed in (1701,1702,1703):
        models[f'fixed_{seed}']=str(MAIN/'fixed_mappo_frozen_20261025'/f'mappo_{seed}.pt')
        models[f'dynamic_{seed}']=selected[str(seed)]['checkpoint']
    for p in models.values():hashes[p]=sha(p)
    for p,h in hashes.items():assert sha(p)==h,p
    ROOT.mkdir();write_json(ROOT/'config_template.json',cfg);hashes[str(ROOT/'config_template.json')]=sha(ROOT/'config_template.json')
    write_json(ROOT/'registration.json',dict(date=DATE,scenario='L_48_288',methods=METHODS,models=models,hashes=hashes,
        workers=9,torch_threads=1,score_duration_s=3600,deadline=END.isoformat(),tests_sealed=True,
        purpose='Runtime/RSS only; never model selection or outcome-based scale fallback',no_retry=True))
    print('REGISTERED',sha(ROOT/'registration.json'))


def simulate(method):
    started=time.perf_counter();env=None;out=ROOT/'runs'/f'{method}.json';progress=out.with_suffix('.progress.json')
    try:
        assert not out.exists() and not progress.exists();check_time();reg=verify_registration();torch.set_num_threads(1)
        run=read(ROOT/'run_manifest.json')
        for p,h in run['hashes'].items():assert sha(p)==h,p
        p=read(run['protocol']);verify(p);cfg=config_from_dict(p['config']);env=GranularityEnv(cfg);sources=set(cfg.task_source_sat_ids)
        policies={};global_policy=None
        if method.startswith(('fixed_','dynamic_')):
            seed=int(method.rsplit('_',1)[1]);cp=torch.load(reg['models'][method],weights_only=True,map_location='cpu')
            plane=LocalInformationPlane(sources);plane.history_hops=2
            if method.startswith('fixed_'):
                actor=fixed.HierarchicalActor().eval();actor.load_state_dict(cp['state_dict'])
                policies={s:fixed.CommittedPolicy(actor,seed*1000+s) for s in sources}
            else:
                actor=dynamic.DynamicActor().eval();actor.load_state_dict(cp['state_dict'])
                policies={s:dynamic.DynamicPolicy(actor,seed*1000+s,env,plane) for s in sources}
        elif method=='local_heft_16':plane,policies=policy_set(method,sources,cfg.relay_max_hops)
        else:
            plane=ControlPlane(sources)
            global_policy=DynamicThreePlan(env) if method=='global_v3_dynamic' else ThreePlanChoice(cfg.relay_max_hops)
        env.control_plane=plane;action_hash=hashlib.sha256();setup_seconds=time.perf_counter()-started
        for step in range(cfg.sim_steps):
            actions=[]
            if step%cfg.decision_interval_steps==0:
                state=env.export_state();plane.enqueue(state);state=decision_state(state,cfg.decision_tile_limit_per_sat)
                actions=local_actions(state,plane,policies) if policies else global_policy.select_actions(state)
                action_digest_update(action_hash,step,actions)
            env.step(actions)
            if env.time%round(60/cfg.dt)==0:
                check_time();write_json(progress,dict(status='RUNNING',method=method,pid=os.getpid(),sim_seconds=env.time*cfg.dt,
                    total_sim_seconds=p['sim_duration_s'],wall_seconds=time.perf_counter()-started,setup_seconds=setup_seconds,peak_rss_gb=peak_rss_gb()))
        checks=invariants(env);rows=env.summary()['task_outcomes'];wanted=set(p['scored_task_ids'])
        checks['followup600']=all(o['observed_age_seconds']>=600 for o in rows if o['task_id'] in wanted) and len([o for o in rows if o['task_id'] in wanted])==len(wanted)
        checks['passed']=bool(checks['passed'] and checks['followup600'])
        verify_registration();verify(p)
        result=dict(status='PASS' if checks['passed'] else 'AUDIT_MISMATCH',method=method,protocol=run['protocol'],protocol_sha256=sha(run['protocol']),
            wall_seconds=time.perf_counter()-started,setup_seconds=setup_seconds,peak_rss_gb=peak_rss_gb(),sim_duration_s=p['sim_duration_s'],
            scored_images=len(wanted),all_images=len(rows),checks=checks,action_trace_sha256=action_hash.hexdigest(),
            tests_sealed=True,resource_probe_only=True)
        write_json(out,result);short=dict(status=result['status'],path=str(out),sha256=sha(out),wall_seconds=result['wall_seconds'],peak_rss_gb=result['peak_rss_gb'])
        write_json(progress,short);return short
    except Exception:
        result=dict(status='FAILED',method=method,traceback=traceback.format_exc());write_json(progress,result);return result
    finally:
        if env:env.close()


def panel():
    started=time.perf_counter()
    try:
        reg=verify_registration();check_time();assert not (ROOT/'progress.json').exists()
        cfg=read(ROOT/'config_template.json');targets=read(MAIN/'targets_maritime80.json')
        with ProcessPoolExecutor(max_workers=9,mp_context=multiprocessing.get_context('spawn')) as pool:
            jobs=[pool.submit(source_windows,(sid,cfg['topology']['tle_lines'][:48],targets,DATE)) for sid in range(48)]
            windows=[]
            for i,f in enumerate(as_completed(jobs),1):
                windows.extend(f.result());write_json(ROOT/'progress.json',dict(stage='acquisition',done=i,total=48,wall_seconds=time.perf_counter()-started));check_time()
            windows.sort(key=lambda w:(w['start_s'],w['source_sat_id'],w['target_id']))
            for i,w in enumerate(windows):w['window_id']=i
            events=arrivals(windows,DATE);folder=ROOT/'data'/DATE;write_json(folder/'windows.json',windows);write_json(folder/'arrivals.json',events)
            selected=peak_window([e['ready_s'] for e in events],[e['raw_bytes'] for e in events],3600)
            protocol_path=folder/'hour_1/protocol.json'
            p=episode_config(cfg,DATE,events,selected,protocol_path.parent,reg['hashes'],[folder/'windows.json',folder/'arrivals.json'])
            verify_registration()
            write_json(ROOT/'run_manifest.json',dict(protocol=str(protocol_path),hashes={str(protocol_path):sha(protocol_path),str(ROOT/'registration.json'):sha(ROOT/'registration.json')},
                methods=METHODS,workers=9,tests_sealed=True,generation_wall_seconds=time.perf_counter()-started,
                daily_images=len(events),scored_images=len(p['scored_task_ids']),score_duration_s=3600))
            results=[];futures=[pool.submit(simulate,method) for method in METHODS]
            for f in as_completed(futures):
                results.append(f.result());write_json(ROOT/'progress.json',dict(stage='simulation',done=len(results),total=9,results=results));check_time()
        verify_registration()
        write_json(ROOT/'final_status.json',dict(status='COMPLETE' if all(r['status']=='PASS' for r in results) else 'BLOCKED',results=results,
            total_wall_seconds=time.perf_counter()-started,tests_sealed=True,resource_probe_only=True))
    except Exception:
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',traceback=traceback.format_exc(),tests_sealed=True))


def launch():
    verify_registration();check_time();assert not (ROOT/'launch.json').exists()
    command=[sys.executable,'-B','-m','analysis.run_scale_probe_20261027','panel']
    with (ROOT/'panel.log').open('xb') as log:
        child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    awake=subprocess.Popen(['/usr/bin/caffeinate','-i','-w',str(child.pid)],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    identity=subprocess.check_output(['ps','-p',str(child.pid),'-o','pid=,lstart=,command='],text=True).strip()
    write_json(ROOT/'launch.json',dict(pid=child.pid,caffeinate_pid=awake.pid,command=command,identity=identity,
        started_at=datetime.now(timezone.utc).isoformat(),registration_sha256=sha(ROOT/'registration.json')))
    print(read(ROOT/'launch.json'))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['prepare','launch','panel']);globals()[p.parse_args().mode]()
