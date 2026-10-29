"""动态v3教师：先同片段被动采集一致性门控，再完成12训练窗口。"""
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import time
import traceback

import torch
from analysis.dynamic_granularity_v2 import GranularityEnv, DynamicThreePlan
from analysis.dynamic_learning_interface import build_observation, teacher_label
from analysis.main_rl_interface import task_groups, complete_uncommitted
from analysis.local_information_baselines import LocalInformationPlane
from analysis.run_main_rl_stage1 import commitments, action_digest_update
from analysis.run_main_local_panel import verify_main
from analysis.run_load_pressure_short import verify
from analysis.run_sar_frozen import invariants
from analysis.redesign_scenario import write_json
from sim.config import config_from_dict
from sim.control import ControlPlane
from sim.marl.decision import decision_state

MAIN=Path('results/redesign_20261021/main_scenario')
ROOT=MAIN/'dynamic_teachers_20261026'
AUDIT=MAIN/'dynamic_heuristics_v2_20261025/independent_audit_20261026'
DEADLINE=datetime(2026,10,30,16,tzinfo=timezone.utc)
TRAIN=tuple(f'2026-10-{d}' for d in range(16,22))


def read(p):return json.loads(Path(p).read_text())
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def episode(path):return Path(path).parent.parent.name+'_'+Path(path).parent.name


def verify_manifest():
    if datetime.now(timezone.utc)>=DEADLINE:raise TimeoutError('Dynamic freeze deadline reached')
    m=read(ROOT/'manifest.json');verify_main()
    for path,h in m['hashes'].items():assert sha(path)==h,path
    return m


def capture_observations(env, state, plane, captured):
    pending={}
    for source in sorted({t['source_sat_id'] for t in state.tiles.values()}):
        view=plane.view(state,source,2)
        for task,tiles in task_groups(view):
            if task not in captured and complete_uncommitted(view,task,tiles):
                pending[task]=build_observation(env,view,task)
    return pending


def job(args):
    path,role=args;env=None;started=time.perf_counter();folder=ROOT/'episodes'/episode(path)
    progress=folder/f'{role}.progress.json';out=folder/f'{role}.json'
    try:
        if out.exists() or progress.exists():raise FileExistsError('No overwrite/retry')
        verify_manifest();p=read(path);verify(p)
        assert p['date'] in TRAIN and p['score_end_s']-p['score_start_s']==3600
        assert role in ('teacher','reference')
        torch.set_num_threads(1)
        cfg=config_from_dict(p['config']);env=GranularityEnv(cfg);sources=set(cfg.task_source_sat_ids)
        plane=LocalInformationPlane(sources) if role=='teacher' else ControlPlane(sources)
        if role=='teacher':plane.history_hops=2
        env.control_plane=plane;policy=DynamicThreePlan(env)
        captures={};mapping={};action_hash=hashlib.sha256()
        for step in range(cfg.sim_steps):
            actions=[];pending={}
            if step%cfg.decision_interval_steps==0:
                state=env.export_state();plane.enqueue(state)
                state=decision_state(state,cfg.decision_tile_limit_per_sat)
                if role=='teacher':pending=capture_observations(env,state,plane,captures)
                actions=policy.select_actions(state)
                for task,obs in pending.items():
                    if task in policy.decided:captures[task]=obs
                action_digest_update(action_hash,step,actions)
            env.step(actions)
            if role=='teacher' and (step%cfg.decision_interval_steps==0 or step==cfg.sim_steps-1):commitments(env,mapping)
            if env.time%round(60/cfg.dt)==0:
                if datetime.now(timezone.utc)>=DEADLINE:raise TimeoutError('Dynamic freeze deadline reached')
                write_json(progress,dict(status='RUNNING',pid=os.getpid(),role=role,date=p['date'],
                    sim_seconds=env.time*cfg.dt,total_sim_seconds=p['sim_duration_s'],wall_seconds=time.perf_counter()-started))
        outcomes=env.summary()['task_outcomes'];ids=set(p['scored_task_ids'])
        scored=[o for o in outcomes if o['task_id'] in ids]
        checks=invariants(env)
        checks['followup600']=len(scored)==len(ids) and all(o['observed_age_seconds']>=600 for o in scored)
        checks['illegal_inputs']=[t.tile_id for t in env.tiles.values() if t.input_route and
            (len(t.input_route)-1>2 or any(s in sources for s in t.input_route[1:]))]
        checks['foreign_compute']=[tid for c in env.compute_cohort_records.values() if c['sat_id'] in sources
            for tid in c['tile_ids'] if env.tasks[env.tiles[tid].parent_task_id].source_sat_id!=c['sat_id']]
        checks['raw_unique'] = all(math.isclose(sum(env.tiles[t].data_size_mb for t in env.tasks[task].tile_ids),
            env.tasks[task].image_size_mb,abs_tol=1e-6) for task in env._dist_pipeline.raw_tasks)
        checks['passed'] &= checks['followup600'] and not checks['illegal_inputs'] and not checks['foreign_compute'] and checks['raw_unique']
        choices={d['task_id']:d for d in policy.decisions};labels={}
        for task,obs in captures.items():
            weights={tid:env.tiles[tid].compute_cost for tid in env.tasks[task].tile_ids}
            labels[task]=dict(observation=obs,label=teacher_label(obs,choices[task]['selected'],mapping,weights),
                actual_commitments={tid:mapping.get(tid) for tid in weights},window_weights=weights,scored=task in ids)
        dynamics=dict(network_mb=dict(env.metrics.network_sent_mb),compute_busy_s=dict(env.metrics.compute_busy_time),
            total_tiles=env.metrics.total_tiles,choices=policy.decisions)
        verify_manifest();verify(p)
        result=dict(status='PASS' if checks['passed'] else 'AUDIT_MISMATCH',role=role,protocol=path,date=p['date'],
            checks=checks,labels=labels,outcomes=outcomes,dynamics=dynamics,action_trace_sha256=action_hash.hexdigest(),
            wall_seconds=time.perf_counter()-started)
        write_json(out,result);status=dict(status=result['status'],path=str(out),sha256=sha(out),wall_seconds=result['wall_seconds'])
        write_json(progress,status);return status
    except Exception:
        status=dict(status='FAILED',path=str(out),traceback=traceback.format_exc())
        write_json(progress,status);return status
    finally:
        if env:env.close()


def prepare():
    assert not ROOT.exists()
    assert read(AUDIT/'AUDIT.json')['status']=='PASS'
    bc=read(MAIN/'bc_stage2_20261022/manifest.json')
    for path in bc['train']:verify(read(path));assert read(path)['date'] in TRAIN
    hashes={**bc['entry_hashes'],**bc['episode_hashes']}
    files=[Path(__file__),Path('analysis/dynamic_granularity_v2.py'),Path('analysis/dynamic_learning_interface.py'),
        Path('tests/test_dynamic_granularity_v2.py'),Path('tests/test_dynamic_learning_interface.py'),
        Path('tests/test_dynamic_teacher_capture.py'),MAIN/'DYNAMIC_TEACHER_PROTOCOL_20261026.md',
        MAIN/'fixed_mappo_frozen_20261025/manifest.json',AUDIT/'AUDIT.json',AUDIT/'HASHES.json']
    for path in files:hashes[str(path)]=sha(path)
    for path,h in read(AUDIT/'HASHES.json').items():assert sha(path)==h
    for path,h in hashes.items():assert sha(path)==h
    write_json(ROOT/'manifest.json',dict(train=bc['train'],workers=9,hashes=hashes,tests_sealed=True,
        source='Registered dynamic v3 only',hard_deadline_utc=DEADLINE.isoformat(),no_retry=True))
    print('PREPARED',sha(ROOT/'manifest.json'))


def pool(jobs,workers):
    results=[]
    with ProcessPoolExecutor(max_workers=workers,mp_context=multiprocessing.get_context('spawn')) as executor:
        futures=[executor.submit(job,a) for a in jobs]
        for f in as_completed(futures):
            results.append(f.result());write_json(ROOT/'batch_progress.json',dict(done=len(results),total=len(jobs),results=results))
    return results


def coverage(m):
    keys=('represented_windows','outside_topk_windows','masked_windows','uncommitted_windows','unsupported_windows')
    totals={k:0. for k in (*keys,'uncaptured_windows','ground_block_windows','total_windows')}
    image_count=captured=illegal=0;rows=[]
    for path in m['train']:
        p=read(path);r=read(ROOT/'episodes'/episode(path)/'teacher.json')
        expected={e['task_id']:e['compute_windows'] for e in p['events'] if e['scored']}
        assert set(expected)==set(p['scored_task_ids'])
        for task,weight in expected.items():
            image_count+=1;totals['total_windows']+=weight
            if task not in r['labels']:totals['uncaptured_windows']+=weight;continue
            captured+=1;row=r['labels'][task];label=row['label'];obs=row['observation']
            assert math.isclose(label['total_windows'],weight) and math.isclose(sum(label[k] for k in keys),weight)
            for k in (*keys,'ground_block_windows'):totals[k]+=label[k]
            illegal+=not obs['image_mask'][label['mode']]
        rows.append(dict(episode=episode(path),sha256=sha(ROOT/'episodes'/episode(path)/'teacher.json')))
    assert math.isclose(sum(totals[k] for k in (*keys,'uncaptured_windows')),totals['total_windows'])
    return dict(images=image_count,captured=captured,illegal_image_mask_images=illegal,totals=totals,teachers=rows,
        represented_fraction=totals['represented_windows']/totals['total_windows'],
        note='ground_block_windows is a represented subset, not an extra disjoint category')


def panel():
    try:
        m=verify_manifest();first=m['train'][0]
        gate=pool([(first,'reference'),(first,'teacher')],2)
        assert all(r['status']=='PASS' for r in gate),gate
        a,b=[read(ROOT/'episodes'/episode(first)/f'{role}.json') for role in ('reference','teacher')]
        checks={k:a[k]==b[k] for k in ('outcomes','dynamics','action_trace_sha256')}
        write_json(ROOT/'REPLAY_GATE.json',dict(status='PASS' if all(checks.values()) else 'AUDIT_MISMATCH',checks=checks,results=gate))
        assert all(checks.values()),checks
        results=pool([(p,'teacher') for p in m['train'][1:]],9)
        assert len(results)==11 and all(r['status']=='PASS' for r in results),results
        verify_manifest();cov=coverage(m);write_json(ROOT/'TEACHER_COVERAGE.json',cov)
        write_json(ROOT/'FINAL_AUDIT.json',dict(status='PASS',hashes={str(p):sha(p) for p in ROOT.rglob('*.json')
            if p.name not in ('FINAL_AUDIT.json','final_status.json','launch.json')}))
        write_json(ROOT/'final_status.json',dict(status='COMPLETE',teachers=12,tests_sealed=True,training_started=False))
    except Exception:
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',traceback=traceback.format_exc(),tests_sealed=True))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['prepare','panel']);a=p.parse_args();globals()[a.mode]()
