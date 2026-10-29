"""L一小时零样本验证；单池生成任务后执行，正式测试入口不可用。"""
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import math
import multiprocessing
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback

import torch
from analysis.run_scale_probe_20261027 import (
    MAIN, read, sha, scale_config, source_windows, arrivals, peak_window, fixture,
    measure_merge, digest, verify, verify_main, GranularityEnv, config_from_dict,
    decision_state, invariants, LocalInformationPlane, ControlPlane,
    ThreePlanChoice, DynamicThreePlan, fixed, dynamic, action_digest_update, peak_rss_gb,
)
from analysis.run_dynamic_learning import write_json
from analysis.run_dynamic_heuristics_v2 import configuration, TrafficAudit, resources, difference
from analysis.run_local_comparison_20261027 import BASELINES, EXTRAS, policy_set, local_actions, check_record
from analysis.report_main_posthoc import score, latency_stats, DEADLINES
from analysis.main_rl_interface import reachable_helpers, sat_options, contact_plan, K
from analysis.dynamic_granularity_v2 import preview
from analysis.scale_feature_audit_20261027 import FeatureAudit
from analysis.freeze_m_training_ranges_20261027 import ROOT as RANGES
from sim.orbit import load_satellites, parse_start_time_utc, position_km

ROOT = MAIN/'scale_validation_20261027'
PROTOCOL = MAIN/'SCALE_VALIDATION_PROTOCOL_20261027.md'
VALID = ('2026-10-22','2026-10-23','2026-10-24')
LEARNED = tuple(f'{family}_{seed}' for family in ('fixed','dynamic') for seed in (1701,1702,1703))
METHODS = LEARNED+BASELINES+EXTRAS+('global_v3_16','global_v3_dynamic')
END = datetime(2026,10,30,16,tzinfo=timezone.utc)


def check_time():
    if datetime.now(timezone.utc)>=END: raise TimeoutError('Scale validation cutoff; no extension')


def verify_manifest():
    m=read(ROOT/'manifest.json')
    for path,h in m['hashes'].items(): assert sha(path)==h,path
    return m


def build_episode(template,date,events,selected,folder,hashes,daily_paths):
    assert date in VALID, 'Only registered validation dates permitted'
    start,end=selected['start_s'],selected['end_s']; assert end-start==3600
    origin=max(0,int((start-600)//60*60));duration=int(end+600-origin)
    cfg=deepcopy(template)
    cfg['topology']['start_time_utc']=(datetime.fromisoformat(date).replace(tzinfo=timezone.utc)+timedelta(seconds=origin)).strftime('%Y-%m-%dT%H:%M:%SZ')
    cfg.update(sim_steps=round(duration/cfg['dt']),task_arrival_end_step=round((end-origin)/cfg['dt']),scheduled_task_arrivals=[])
    details=[]
    for index,e in enumerate(events):
        if not origin<=e['ready_s']<end:continue
        task=f'task_{len(details)}';truth=fixture(e['mode'],e['truth_seed'],16);merge=measure_merge(truth)
        write_json(folder/'truth'/f'{task}.json',truth)
        cfg['scheduled_task_arrivals'].append(dict(release_step=round((e['ready_s']-origin)/cfg['dt']),source_sat_id=e['source_sat_id'],
            image_tiling=truth['tiling'],window_result_bytes=[w['output_bytes'] for w in truth['windows']],aggregation_delay_s=merge['median_seconds']))
        details.append(dict(task_id=task,daily_event_index=index,**e,scored=e['ready_s']>=start,truth_sha256=digest(truth),merge_reference=merge))
    assert all(cfg[k]==v for k,v in template.items() if k not in ('topology','sim_steps','task_arrival_end_step','scheduled_task_arrivals'))
    assert all(cfg['topology'][k]==v for k,v in template['topology'].items() if k!='start_time_utc')
    config_from_dict(cfg);sats=load_satellites(cfg['topology']['tle_lines'])
    midnight=parse_start_time_utc(date+'T00:00:00Z');shifted=parse_start_time_utc(cfg['topology']['start_time_utc'])
    sar=len(cfg['task_source_sat_ids'])
    for sid in (0,sar-1,sar,len(sats)-1):
        for t in (0,start-origin,duration):assert position_km(sats[sid],midnight,origin+t)==position_km(sats[sid],shifted,t)
    original=read(read(MAIN/'bc_stage2_20261022/manifest.json')['train'][0])
    paths=daily_paths+list((folder/'truth').glob('*.json'))
    p=dict(date=date,seed=1701,score_start_s=int(start-origin),score_end_s=int(end-origin),origin_absolute_s=origin,
        sim_duration_s=duration,followup_s=600,D_seconds=60,D_max=600,config=cfg,config_sha256=digest(cfg),events=details,
        peak=selected,scored_task_ids=[e['task_id'] for e in details if e['scored']],frozen_protocol=original['frozen_protocol'],
        input_hashes={**hashes,**{str(p):sha(p) for p in paths}},no_retry=True,zero_shot=True,tests_sealed=True)
    write_json(folder/'protocol.json',p);verify(p);return p


def candidate_counts(env,state,obs):
    result={}
    variants=list(zip((4,16),obs['variants'])) if 'variants' in obs else [(16,obs)]
    for requested,v in variants:
        task=obs['task_id']
        view=preview(env,state,task,requested) if 'variants' in obs else replace(state,tiles={tid:state.tiles[tid] for tid in v['tile_ids']})
        scope=reachable_helpers(view,v['source'],2,set(view.config['task_source_sat_ids']))
        static=replace(view,satellites={sid:{**row,'committed_compute_s':0.,'isl_backlog_mb':{},'ground_backlog_mb':{},
            'committed_result_mb':0.,'unrouted_result_mb':0.,'mem_remaining_gb':view.config['mem_capacity_gb']}
            for sid,row in view.satellites.items()})
        largest=max(view.tiles.values(),key=lambda t:t['data_size_mb'])
        options=sat_options(static,largest,contact_plan(static))
        helpers=sorted((sid for sid in options if sid in scope),key=lambda sid:(options[sid][0],sid))
        actual=[sid for sid,valid in zip(v['node_ids'][2:],v['node_valid'][2:],strict=True) if valid]
        assert actual==helpers[:K], 'Passive candidate reconstruction disagrees with actor'
        result[requested]=dict(before_topk=len(helpers),after_topk=len(actual),
            legal_compute=sum(v['position_mask'][2:]),legal_positions=sum(v['position_mask']))
    return result


class ObservedPolicy:
    def __init__(self, policy, env, audit, wanted):
        self.policy=policy;self.env=env;self.audit=audit;self.wanted=wanted

    def select_actions(self,state):
        before=set(self.policy.decisions)
        # 在决策前保存只读局部视图；动态策略会提交粒度，需用此前视图复算候选。
        actions=self.policy.select_actions(state)
        for task in self.policy.decisions.keys()-before:
            if task not in self.wanted:continue
            obs=self.policy.decisions[task]['observation']
            self.audit.observe(obs,candidate_counts(self.env,state,obs))
        return actions


def prepare():
    assert not ROOT.exists();check_time();verify_main()
    audit=MAIN/'scale_probe_20261027/independent_audit_20261027'
    assert read(audit/'AUDIT.json')['status']=='PASS'
    decision=read(audit/'RESOURCE_DECISION.json');assert decision['score_duration_s']==3600 and decision['scenario']=='L_48_288'
    rr=read(RANGES/'ranges.json');assert rr['status']=='PASS'
    ranges_reg=read(RANGES/'registration.json');assert rr['registration_sha256']==sha(RANGES/'registration.json')
    for path,h in read(RANGES/'HASHES.json').items():assert sha(path)==h,path
    models=ranges_reg['models'];hashes=dict(read(MAIN/'scale_probe_20261027/registration.json')['hashes'])
    for path in [Path(__file__),PROTOCOL,Path('analysis/scale_feature_audit_20261027.py'),
        Path('analysis/freeze_m_training_ranges_20261027.py'),Path('tests/test_scale_feature_audit_20261027.py'),
        Path('tests/test_scale_validation_20261027.py'),RANGES/'ranges.json',RANGES/'registration.json',RANGES/'HASHES.json',
        audit/'AUDIT.json',audit/'RESOURCE_DECISION.json',MAIN/'COMPARISON_SCOPE_AMENDMENT_20261026.md']:
        hashes[str(path)]=sha(path)
    for path in models.values():hashes[path]=sha(path)
    for path,h in hashes.items():assert sha(path)==h,path
    write_json(ROOT/'config_template.json',scale_config(48,288));hashes[str(ROOT/'config_template.json')]=sha(ROOT/'config_template.json')
    write_json(ROOT/'manifest.json',dict(hashes=hashes,models=models,dates=VALID,methods=METHODS,workers=9,torch_threads=1,
        ranges=str(RANGES/'ranges.json'),deadline=END.isoformat(),tests_sealed=True,no_retry=True,zero_shot=True,
        comparison_scopes=dict(fixed_six_local_16=[x for x in BASELINES if x.endswith('_16')],dynamic_twelve_local_4_16=BASELINES),
        candidate_groups='Requested 4/16; feature range groups use actual block_count encoded in observation'))
    print('REGISTERED',sha(ROOT/'manifest.json'))


def simulate(args):
    path,method=args;started=time.perf_counter();env=observer=None
    p=read(path);out=ROOT/'validation'/p['date']/f'{method}.json';progress=out.with_suffix('.progress.json')
    try:
        assert not out.exists() and not progress.exists();check_time();m=verify_manifest();verify(p)
        assert p['date'] in VALID and method in METHODS
        run=read(ROOT/'run_manifest.json')
        for name,h in run['hashes'].items():assert sha(name)==h,name
        torch.set_num_threads(1);config=configuration(p,method);cfg=config_from_dict(config)
        env=GranularityEnv(cfg);sources=set(cfg.task_source_sat_ids);helpers=set(env.satellites)-sources
        wanted=set(p['scored_task_ids']);policies={};original_policies={};global_policy=None;feature=None
        if method in LEARNED:
            seed=int(method.rsplit('_',1)[1]);cp=torch.load(m['models'][method],weights_only=True,map_location='cpu')
            plane=LocalInformationPlane(sources);plane.history_hops=2
            feature=FeatureAudit(read(m['ranges'])['ranges'][method])
            if method.startswith('fixed_'):
                actor=fixed.HierarchicalActor().eval();actor.load_state_dict(cp['state_dict'])
                original_policies={s:fixed.CommittedPolicy(actor,seed*1000+s) for s in sources}
            else:
                actor=dynamic.DynamicActor().eval();actor.load_state_dict(cp['state_dict'])
                original_policies={s:dynamic.DynamicPolicy(actor,seed*1000+s,env,plane) for s in sources}
            policies={s:ObservedPolicy(policy,env,feature,wanted) for s,policy in original_policies.items()}
        elif method in BASELINES+EXTRAS:plane,policies=policy_set(method,sources,cfg.relay_max_hops)
        else:
            plane=ControlPlane(sources)
            global_policy=DynamicThreePlan(env) if method=='global_v3_dynamic' else ThreePlanChoice(cfg.relay_max_hops)
        env.control_plane=plane;observer=TrafficAudit(env);boundaries={0:observer.snapshot()};action_hash=hashlib.sha256()
        for step in range(cfg.sim_steps):
            actions=[]
            if step%cfg.decision_interval_steps==0:
                state=env.export_state();plane.enqueue(state);state=decision_state(state,cfg.decision_tile_limit_per_sat)
                actions=local_actions(state,plane,policies) if policies else global_policy.select_actions(state)
                action_digest_update(action_hash,step,actions)
            env.step(actions);now=round(env.time*cfg.dt,8)
            if now in (p['score_start_s'],p['score_end_s']):boundaries[now]=observer.snapshot()
            if env.time%round(60/cfg.dt)==0:
                check_time();write_json(progress,dict(status='RUNNING',method=method,date=p['date'],pid=os.getpid(),
                    sim_seconds=now,total_sim_seconds=p['sim_duration_s'],wall_seconds=time.perf_counter()-started,peak_rss_gb=peak_rss_gb()))
        checks=invariants(env);outcomes=env.summary()['task_outcomes'];scored=[o for o in outcomes if o['task_id'] in wanted]
        checks['followup600']=len(scored)==len(wanted) and all(o['observed_age_seconds']>=600 for o in scored)
        checks['foreign_compute']=[tid for c in env.compute_cohort_records.values() for tid in c['tile_ids']
            if c['sat_id'] in sources and env.tasks[env.tiles[tid].parent_task_id].source_sat_id!=c['sat_id']]
        checks['illegal_input']=[t.tile_id for t in env.tiles.values() if t.input_route and
            (len(t.input_route)-1>2 or any(s in sources for s in t.input_route[1:]))]
        checks['window_partition']=all(len([w for tid in t.tile_ids for w in env.tiles[tid].windows_xyxy])==
            len({w for tid in t.tile_ids for w in env.tiles[tid].windows_xyxy}) for t in env.tasks.values())
        checks['sar_ingress']=all(abs(v-plane.sent_mb_by_edge[k])<1e-6 for k,v in env.metrics.network_sent_mb.items()
            if k.startswith('isl:') and int(k.split('->')[-1]) in sources)
        checks['passed']=bool(checks['passed'] and checks['followup600'] and checks['window_partition'] and checks['sar_ingress']
            and not checks['foreign_compute'] and not checks['illegal_input'])
        start,stop=p['score_start_s'],p['score_end_s'];end=observer.snapshot()
        resource_rows={name:resources(difference(a,b),helpers,duration,cfg) for name,a,b,duration in [
            ('whole',boundaries[0],end,p['sim_duration_s']),('main',boundaries[start],boundaries[stop],stop-start),
            ('warmup',boundaries[0],boundaries[start],start),('followup',boundaries[stop],end,p['sim_duration_s']-stop)]}
        decisions={task:r for policy in original_policies.values() for task,r in policy.decisions.items() if task in wanted}
        granularity={}
        for task in wanted:
            t=env.tasks[task];d=decisions.get(task)
            if method.startswith('dynamic_'):requested=(4,16,16)[d['mode']] if d else None
            elif method=='global_v3_dynamic':requested=env.granularity_choices.get(task,{}).get('requested')
            else:requested=4 if method.endswith('_4') else 16
            granularity[task]=dict(requested=requested,actual=t.num_tiles,unique_mb=t.image_size_mb,
                packet_mb=sum(x.data_size_mb for x in env._task_workloads[task].tiles()))
        result=dict(status='PASS' if checks['passed'] else 'AUDIT_MISMATCH',method=method,date=p['date'],protocol=path,
            config_sha256=digest(config),checks=checks,outcomes=outcomes,curve=[score(scored,d) for d in DEADLINES],
            latency=latency_stats(scored),R600=math.fsum(min(o['latency_seconds'],600) if o['status']=='completed' else 600 for o in scored)/len(scored),
            resources=resource_rows,granularity=granularity,action_trace_sha256=action_hash.hexdigest(),
            wall_seconds=time.perf_counter()-started,peak_rss_gb=peak_rss_gb(),tests_sealed=True,zero_shot=True)
        if feature:
            result.update(feature_audit=feature.result(),unacted_scored_ids=sorted(wanted-set(decisions)),
                choices={task:r['mode'] for task,r in decisions.items()},checkpoint=m['models'][method],checkpoint_sha256=sha(m['models'][method]))
        elif method=='global_v3_dynamic':
            result['decisions']=[r for r in global_policy.decisions if r['task_id'] in wanted]
        verify_manifest();verify(p);write_json(out,result)
        short=dict(status=result['status'],path=str(out),sha256=sha(out),wall_seconds=result['wall_seconds'])
        write_json(progress,short);return short
    except Exception:
        failed=dict(status='FAILED',method=method,date=p['date'],traceback=traceback.format_exc())
        write_json(progress,failed);return failed
    finally:
        if observer:observer.close()
        if env:env.close()


def panel():
    started=time.perf_counter()
    try:
        check_time();m=verify_manifest();assert not (ROOT/'progress.json').exists()
        cfg=read(ROOT/'config_template.json');targets=read(MAIN/'targets_maritime80.json');protocols=[]
        with ProcessPoolExecutor(max_workers=9,mp_context=multiprocessing.get_context('spawn')) as pool:
            for date in VALID:
                jobs=[pool.submit(source_windows,(sid,cfg['topology']['tle_lines'][:48],targets,date)) for sid in range(48)]
                windows=[]
                for i,f in enumerate(as_completed(jobs),1):
                    windows.extend(f.result());check_time()
                    write_json(ROOT/'progress.json',dict(stage='acquisition',date=date,done=i,total=48))
                windows.sort(key=lambda w:(w['start_s'],w['source_sat_id'],w['target_id']))
                for i,w in enumerate(windows):w['window_id']=i
                events=arrivals(windows,date);folder=ROOT/'data'/date
                write_json(folder/'windows.json',windows);write_json(folder/'arrivals.json',events)
                selected=peak_window([e['ready_s'] for e in events],[e['raw_bytes'] for e in events],3600)
                ppath=folder/'hour_1/protocol.json'
                build_episode(cfg,date,events,selected,ppath.parent,m['hashes'],[folder/'windows.json',folder/'arrivals.json'])
                protocols.append(str(ppath))
            verify_manifest();write_json(ROOT/'run_manifest.json',dict(protocols=protocols,methods=METHODS,
                hashes={p:sha(p) for p in protocols+[str(ROOT/'manifest.json')]},tests_sealed=True,
                generation_wall_seconds=time.perf_counter()-started))
            results=[];jobs=[pool.submit(simulate,(p,method)) for method in METHODS for p in protocols]
            for f in as_completed(jobs):
                results.append(f.result());write_json(ROOT/'progress.json',dict(stage='validation',done=len(results),total=66,results=results))
        verify_manifest();passed=len(results)==66 and all(r['status']=='PASS' for r in results)
        if passed:
            for r in results:
                assert sha(r['path'])==r['sha256'];row=read(r['path']);check_record(row,read(row['protocol']))
            write_json(ROOT/'FINAL_AUDIT.json',dict(status='PASS',hashes={r['path']:r['sha256'] for r in results},tests_sealed=True))
        write_json(ROOT/'final_status.json',dict(status='COMPLETE' if passed else 'BLOCKED',results=results,
            tests_sealed=True,zero_shot=True,total_wall_seconds=time.perf_counter()-started))
    except Exception:
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',traceback=traceback.format_exc(),tests_sealed=True))


def supervise():
    verify_manifest();command=[sys.executable,'-B','-m','analysis.run_scale_validation_20261027','panel']
    with (ROOT/'panel.log').open('xb') as log:
        child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    awake=subprocess.Popen(['/usr/bin/caffeinate','-i','-w',str(child.pid)],start_new_session=True)
    identity=subprocess.check_output(['ps','-p',str(child.pid),'-o','pid=,lstart=,command='],text=True).strip()
    write_json(ROOT/'launch.json',dict(pid=child.pid,supervisor_pid=os.getpid(),caffeinate_pid=awake.pid,command=command,
        identity=identity,started_at=datetime.now(timezone.utc).isoformat(),manifest_sha256=sha(ROOT/'manifest.json')))
    timed_out=False
    while child.poll() is None:
        if datetime.now(timezone.utc)>=END:
            timed_out=True;os.killpg(child.pid,signal.SIGTERM)
            try:child.wait(timeout=10)
            except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait()
            break
        time.sleep(2)
    if timed_out or child.returncode or not (ROOT/'final_status.json').exists():
        write_json(ROOT/'supervisor_failure.json',dict(status='BLOCKED',timeout=timed_out,returncode=child.returncode,
            at=datetime.now(timezone.utc).isoformat(),tests_sealed=True))


def launch():
    check_time();verify_manifest();assert not (ROOT/'launch_request.json').exists()
    assert not (ROOT/'progress.json').exists()
    with (ROOT/'supervisor.log').open('xb') as log:
        child=subprocess.Popen([sys.executable,'-B','-m','analysis.run_scale_validation_20261027','supervise'],
            stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    write_json(ROOT/'launch_request.json',dict(supervisor_pid=child.pid,at=datetime.now(timezone.utc).isoformat()))
    print('SUPERVISOR',child.pid)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=['prepare','launch','panel','supervise'])
    globals()[parser.parse_args().mode]()
