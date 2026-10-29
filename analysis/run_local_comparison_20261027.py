"""补齐12项局部规则包络及局部CGS/HEFT；单池、仅验证日。"""
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import math
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

import torch
from analysis.run_dynamic_heuristics_v2 import (
    MAIN, BC, VALID, read, sha, configuration, TrafficAudit, difference, resources,
    GranularityEnv, LocalInformationPlane, LocalMinLoad, ThreePlanChoice,
    action_digest_update, verify, invariants, score, latency_stats, DEADLINES,
    digest, config_from_dict, decision_state,
)
from analysis.run_dynamic_learning import write_json
from analysis.run_main_local_panel import verify_main
from sim.scheduler.literature import CGSComputeAdapted, HEFTAdapted

ROOT = MAIN/'local_comparison_20261027'
HEURISTIC = MAIN/'dynamic_heuristics_v2_20261025'
LEARNING = MAIN/'dynamic_learning_20261026'
PROTOCOL = MAIN/'LOCAL_COMPARISON_PROTOCOL_20261027.md'
RULES = ('zero_1','zero_2','history_1','history_2','minload_1','minload_2')
BASELINES = tuple(f'{r}_{g}' for r in RULES for g in (4,16))
EXTRAS = ('local_cgs_16','local_heft_16')
NEW = tuple(f'{r}_4' for r in RULES if r != 'minload_2') + EXTRAS
END = datetime(2026,10,30,16,tzinfo=timezone.utc)


def check_time():
    if datetime.now(timezone.utc) >= END: raise TimeoutError('Validation deadline before formal test')


def policy_set(method, sources, relay_hops):
    rule = method.rsplit('_',1)[0]
    plane = LocalInformationPlane(sources)
    plane.history_hops = 2 if method in EXTRAS else int(rule[-1])
    plane.estimator = 'history' if rule.startswith('history') else 'zero'
    if method == 'local_cgs_16': factory = lambda: CGSComputeAdapted()
    elif method == 'local_heft_16': factory = HEFTAdapted
    elif rule.startswith('minload'): factory = LocalMinLoad
    else: factory = lambda: ThreePlanChoice(relay_hops)
    return plane, {s:factory() for s in sources}


def local_actions(state, plane, policies):
    actions = []
    for source in sorted({t['source_sat_id'] for t in state.tiles.values()}):
        actions.extend(policies[source].select_actions(plane.view(state,source,plane.history_hops)))
    return actions


def verify_manifest():
    m = read(ROOT/'manifest.json')
    for path,h in m['hashes'].items(): assert sha(path) == h, path
    return m


def check_record(r,p):
    assert r['status'] == 'PASS' and r['checks']['passed']
    ids=set(p['scored_task_ids']); rows=[o for o in r['outcomes'] if o['task_id'] in ids]
    assert len(rows)==len(ids) and {o['task_id'] for o in rows}==ids
    assert p['sim_duration_s']-p['score_end_s']>=600
    assert all(o['observed_age_seconds']>=600 and p['score_start_s']<=o['release_seconds']<p['score_end_s'] for o in rows)
    for d,c in zip(DEADLINES,r['curve'],strict=True):
        assert c['unknown_count']==0 and c['D_seconds']==d
        assert math.isclose(c['P_lower'],sum(o['status']=='completed' and o['latency_seconds']<=d for o in rows)/len(rows),abs_tol=1e-12)
    return rows


def prepare():
    assert not ROOT.exists(), 'No overwrite'
    check_time(); verify_main()
    assert read(LEARNING/'independent_audit_20261027/AUDIT.json')['status']=='PASS'
    bc=read(BC/'manifest.json'); paths=bc['validation']; reused={}
    hashes={**bc['entry_hashes'],**bc['episode_hashes'],**read(HEURISTIC/'manifest.json')['hashes']}
    assert sha(HEURISTIC/'manifest.json')=='256333abd75099a264a217745621e042a4d5fc2739a66fc4224f9cc05e78278a'
    for p in paths:
        protocol=read(p); verify(protocol); date=protocol['date']; assert date in VALID
        reused[date]={}
        for rule in RULES:
            path=BC/'validation'/date/f'{rule}.json'; r=read(path)
            assert r['method']==rule and r['date']==date and r['config_sha256']==digest(protocol['config'])
            assert configuration(protocol,f'{rule}_16')==protocol['config']
            check_record(r,protocol); reused[date][f'{rule}_16']=str(path); hashes[str(path)]=sha(path)
        path=HEURISTIC/'validation'/date/'minload_4.json'; r=read(path)
        assert r['method']=='minload_4' and r['date']==date
        assert r['config_sha256']==digest(configuration(protocol,'minload_2_4'))
        check_record(r,protocol); reused[date]['minload_2_4']=str(path); hashes[str(path)]=sha(path)
    for p in (Path(__file__),Path('tests/test_local_comparison_20261027.py'),PROTOCOL,
        Path('analysis/run_dynamic_learning.py'),
        MAIN/'COMPARISON_SCOPE_AMENDMENT_20261026.md',HEURISTIC/'manifest.json',
        HEURISTIC/'independent_audit_20261026/AUDIT.json',LEARNING/'independent_audit_20261027/AUDIT.json',
        LEARNING/'BEST_CHECKPOINTS.json',LEARNING/'final_budget.json'):
        hashes[str(p)]=sha(p)
    for p,h in hashes.items(): assert sha(p)==h,p
    ROOT.mkdir()
    write_json(ROOT/'manifest.json',dict(protocols=paths,baselines=BASELINES,extras=EXTRAS,new_methods=NEW,
        reused=reused,hashes=hashes,workers=9,torch_threads=1,deadline=END.isoformat(),tests_sealed=True,no_retry=True,
        config_hashes={read(p)['date']:{method:digest(configuration(read(p),method)) for method in NEW} for p in paths},
        comparison_scopes=dict(fixed_six_local_16=[f'{r}_16' for r in RULES],dynamic_twelve_local_4_16=BASELINES),
        reuse_audit='21 records: frozen source hashes, exact config/score IDs/600 followup/P recomputation; same local factories and fixed-granularity semantics. Old 16 resources unavailable, not zero.',
        orin_four_block_memory_verified=False))
    print('PREPARED',sha(ROOT/'manifest.json'))


def job(args):
    path,method=args; started=time.perf_counter(); env=observer=None
    p=read(path); out=ROOT/'validation'/p['date']/f'{method}.json'; progress=out.with_suffix('.progress.json')
    try:
        assert not out.exists() and not progress.exists(), 'No retries'
        check_time(); m=verify_manifest(); verify(p); torch.set_num_threads(1)
        assert method in NEW and p['date'] in VALID
        config=configuration(p,method); assert digest(config)==m['config_hashes'][p['date']][method]
        cfg=config_from_dict(config); env=GranularityEnv(cfg); sources=set(cfg.task_source_sat_ids)
        helpers=set(env.satellites)-sources; plane,policies=policy_set(method,sources,cfg.relay_max_hops)
        assert plane.ttl_s==15; env.control_plane=plane; observer=TrafficAudit(env)
        boundaries={0:observer.snapshot()}; action_hash=hashlib.sha256()
        for step in range(cfg.sim_steps):
            actions=[]
            if step%cfg.decision_interval_steps==0:
                state=env.export_state(); plane.enqueue(state)
                actions=local_actions(decision_state(state,cfg.decision_tile_limit_per_sat),plane,policies)
                action_digest_update(action_hash,step,actions)
            env.step(actions); now=round(env.time*cfg.dt,8)
            if now in (p['score_start_s'],p['score_end_s']): boundaries[now]=observer.snapshot()
            if env.time%round(60/cfg.dt)==0:
                check_time(); write_json(progress,dict(status='RUNNING',pid=os.getpid(),method=method,date=p['date'],
                    sim_seconds=now,total_sim_seconds=p['sim_duration_s'],wall_seconds=time.perf_counter()-started))
        checks=invariants(env); outcomes=env.summary()['task_outcomes']; wanted=set(p['scored_task_ids'])
        scored=[o for o in outcomes if o['task_id'] in wanted]
        checks['followup600']=len(scored)==len(wanted) and all(o['observed_age_seconds']>=600 for o in scored)
        checks['foreign_compute']=[tid for c in env.compute_cohort_records.values() for tid in c['tile_ids']
            if c['sat_id'] in sources and env.tasks[env.tiles[tid].parent_task_id].source_sat_id != c['sat_id']]
        checks['illegal_input']=[t.tile_id for t in env.tiles.values() if t.input_route and
            (len(t.input_route)-1>2 or any(s in sources for s in t.input_route[1:]))]
        checks['sar_ingress']=all(abs(v-plane.sent_mb_by_edge[k])<1e-6 for k,v in env.metrics.network_sent_mb.items()
            if k.startswith('isl:') and int(k.split('->')[-1]) in sources)
        checks['window_partition']=all(len([w for tid in t.tile_ids for w in env.tiles[tid].windows_xyxy])==
            len({w for tid in t.tile_ids for w in env.tiles[tid].windows_xyxy}) for t in env.tasks.values())
        checks['passed']=bool(checks['passed'] and checks['followup600'] and checks['sar_ingress'] and checks['window_partition']
            and not checks['foreign_compute'] and not checks['illegal_input'])
        end=observer.snapshot(); start,stop=p['score_start_s'],p['score_end_s']
        resource_rows={name:resources(difference(a,b),helpers,duration,cfg) for name,a,b,duration in [
            ('whole',boundaries[0],end,p['sim_duration_s']),('main',boundaries[start],boundaries[stop],stop-start),
            ('warmup',boundaries[0],boundaries[start],start),('followup',boundaries[stop],end,p['sim_duration_s']-stop)]}
        result=dict(status='PASS' if checks['passed'] else 'AUDIT_MISMATCH',method=method,date=p['date'],protocol=path,
            config_sha256=digest(config),checks=checks,outcomes=outcomes,curve=[score(scored,d) for d in DEADLINES],
            latency=latency_stats(scored),R600=math.fsum(min(o['latency_seconds'],600) if o['status']=='completed' else 600 for o in scored)/len(scored),
            action_trace_sha256=action_hash.hexdigest(),resources=resource_rows,wall_seconds=time.perf_counter()-started,
            information=dict(hops=plane.history_hops,ttl_s=plane.ttl_s,estimator=plane.estimator,public_contact_plan=True),
            granularity={task:dict(requested=int(method.rsplit('_',1)[1]),actual=t.num_tiles,
                packet_mb=sum(x.data_size_mb for x in env._task_workloads[task].tiles()),unique_mb=t.image_size_mb)
                for task,t in env.tasks.items() if task in wanted})
        verify_manifest(); write_json(out,result)
        result=dict(status=result['status'],path=str(out),sha256=sha(out),wall_seconds=result['wall_seconds'])
        write_json(progress,result); return result
    except Exception:
        result=dict(status='FAILED',method=method,date=p['date'],traceback=traceback.format_exc())
        write_json(progress,result); return result
    finally:
        if observer: observer.close()
        if env: env.close()


def collect(results):
    m=verify_manifest()
    if len(results)!=21 or any(r['status']!='PASS' for r in results):
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',results=results,tests_sealed=True)); return
    daily={}; summary={}; curves={}; records={}
    for method in BASELINES+EXTRAS:
        daily[method]=[]; sets=[]; records[method]=[]
        for p in m['protocols']:
            protocol=read(p); date=protocol['date']
            path=m['reused'][date].get(method,str(ROOT/'validation'/date/f'{method}.json'))
            r=read(path); rows=check_record(r,protocol); sets.append(rows); records[method].append(dict(path=path,sha256=sha(path)))
            daily[method].append(dict(date=date,P=[c['P_lower'] for c in r['curve']],R600=r['R600'],
                mean_completed_s=r['latency']['mean_completed_s'],P50_completed_s=r['latency']['P50_completed_s'],
                P90_completed_s=r['latency']['P90_completed_s'],unfinished=r['latency']['unfinished'],resources=r.get('resources')))
        ds=daily[method]; a={k:math.fsum(x[k] for x in ds)/3 for k in ('R600','mean_completed_s','P50_completed_s','P90_completed_s')}
        a.update(P=[math.fsum(x['P'][i] for x in ds)/3 for i in range(7)],unfinished=sum(x['unfinished'] for x in ds))
        summary[method]=a
        xs=sorted({60.,600.,*[o['latency_seconds'] for rows in sets for o in rows if o['status']=='completed' and 60<=o['latency_seconds']<=600]})
        curves[method]=dict(D=xs,P=[sum(sum(o['status']=='completed' and o['latency_seconds']<=d for o in rows)/len(rows) for rows in sets)/3 for d in xs])
    envelopes={}
    for scope,methods in m['comparison_scopes'].items():
        values={k:{method:(summary[method]['P'][0 if k=='P60' else -1] if k in ('P60','P600') else summary[method][k]) for method in methods}
            for k in ('P60','mean_completed_s','P600','R600')}
        envelopes[scope]={k:dict(value=(max if k in ('P60','P600') else min)(v.values()),
            methods=[method for method,value in v.items() if value==(max if k in ('P60','P600') else min)(v.values())]) for k,v in values.items()}
    write_json(ROOT/'summary.json',dict(daily=daily,day_equal=summary,envelopes=envelopes,records=records,tests_sealed=True))
    write_json(ROOT/'full_curves.json',curves)
    write_json(ROOT/'FINAL_AUDIT.json',dict(status='PASS',hashes={str(p):sha(p) for p in ROOT.rglob('*.json') if p.name not in ('FINAL_AUDIT.json','final_status.json')}))
    write_json(ROOT/'final_status.json',dict(status='COMPLETE',results=results,reused_records=21,total_records=42,tests_sealed=True))


def panel():
    try:
        check_time(); m=verify_manifest(); results=[]
        with ProcessPoolExecutor(max_workers=9,mp_context=multiprocessing.get_context('spawn')) as pool:
            futures=[pool.submit(job,(p,method)) for method in NEW for p in m['protocols']]
            for f in as_completed(futures):
                results.append(f.result()); write_json(ROOT/'progress.json',dict(done=len(results),total=21,results=results))
        collect(results)
    except Exception:
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',traceback=traceback.format_exc(),tests_sealed=True))


def launch():
    check_time(); verify_manifest(); assert not (ROOT/'launch.json').exists(), 'Already launched'
    assert not (ROOT/'progress.json').exists() and not (ROOT/'final_status.json').exists()
    command=[sys.executable,'-B','-m','analysis.run_local_comparison_20261027','panel']
    with (ROOT/'panel.log').open('xb') as log:
        child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    awake=subprocess.Popen(['/usr/bin/caffeinate','-i','-w',str(child.pid)],stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    identity=subprocess.check_output(['ps','-p',str(child.pid),'-o','pid=,lstart=,command='],text=True).strip()
    write_json(ROOT/'launch.json',dict(pid=child.pid,caffeinate_pid=awake.pid,command=command,
        identity=identity,started_at=datetime.now(timezone.utc).isoformat(),manifest_sha256=sha(ROOT/'manifest.json')))
    print(read(ROOT/'launch.json'))


if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('mode',choices=['prepare','launch','panel'])
    globals()[parser.parse_args().mode]()
