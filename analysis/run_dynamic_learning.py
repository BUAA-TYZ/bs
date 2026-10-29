"""动态粒度BC与176片段MAPPO，单池、冻结输入、截止监督。"""
import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import random
import signal
import subprocess
import sys
import time
import traceback

import numpy as np
import torch
from analysis import dynamic_learning_interface as interface
from analysis import dynamic_mappo as learning
from analysis import run_dynamic_teachers as teacher
from analysis.run_dynamic_heuristics_v2 import TrafficAudit, difference, resources
from analysis.run_main_mappo import metrics, selection_key, deterioration_streak
from analysis.main_rl_interface import critic_observation
from analysis.local_information_baselines import LocalInformationPlane
from analysis.run_main_rl_stage1 import action_digest_update, commitments
from analysis.run_load_pressure_short import verify
from analysis.run_main_local_panel import verify_main
from analysis.run_sar_frozen import invariants
from analysis.redesign_scenario import write_json as _write_json
from analysis.report_main_posthoc import score, latency_stats, DEADLINES
from sim.config import config_from_dict
from sim.marl.decision import decision_state

MAIN = teacher.MAIN
ROOT = MAIN/'dynamic_learning_20261026'
BC = MAIN/'bc_stage2_20261022'
FIXED = MAIN/'fixed_mappo_frozen_20261025'
PROTOCOL = MAIN/'DYNAMIC_LEARNING_PROTOCOL_20261026.md'
SEEDS = (1701,1702,1703)
VALID = ('2026-10-22','2026-10-23','2026-10-24')
END = teacher.DEADLINE
BC_HP = dict(lr=.001, epochs=100, batch_size=32, gradient_clip=1.)
read, sha = teacher.read, teacher.sha


def write_json(path, value):
    # 截止监督可随时读取进度，避免看到截断中的JSON。
    path = Path(path)
    temp = path.with_name(path.name+f'.{os.getpid()}.tmp')
    _write_json(temp,value); temp.replace(path)


def check_time():
    if datetime.now(timezone.utc) >= END: raise TimeoutError('Dynamic freeze deadline')


def verify_run(check_deadline=True):
    if check_deadline: check_time()
    m = read(ROOT/'manifest.json'); verify_main()
    for path,h in m['hashes'].items(): assert sha(path) == h, path
    return m


def save_checkpoint(path, actor, critic, optimizer, seed, episodes, parent, training):
    if path.exists(): raise FileExistsError(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    torch.save(dict(schema=interface.SCHEMA, state_dict=actor.state_dict(), critic=critic.state_dict(),
        optimizer=optimizer.state_dict(), seed=seed, episodes=episodes, training=training,
        parent_sha256=sha(parent), fixed_parent_sha256=sha(FIXED/f'mappo_{seed}.pt'),
        teacher_hashes=read(ROOT/'manifest.json')['teacher_hashes'], train_dates=teacher.TRAIN,
        validation_used_for_gradient=False, hyperparameters=BC_HP if training=='pure_imitation' else learning.HP,
        torch_rng_state=torch.get_rng_state(),
        sampling_seed_rule='(seed + 100000 * episode) * 1000 + source; validation seed * 1000 + source',
        update_seed_rule='seed * 10000 + batch_last_episode'),path)


def train_bc(seed):
    started = time.perf_counter()
    try:
        m = verify_run(); torch.set_num_threads(1); torch.manual_seed(seed)
        parent = FIXED/f'mappo_{seed}.pt'
        data = torch.load(parent,weights_only=True,map_location='cpu')
        actor = interface.DynamicActor(); actor.initialize_fixed(data['state_dict'])
        critic = learning.Critic(); critic.load_state_dict(data['critic'])
        optimizer = torch.optim.Adam(actor.parameters(),lr=BC_HP['lr'])
        records = []; illegal = 0
        for path in m['train']:
            p = read(path); assert p['date'] in teacher.TRAIN
            record = read(teacher.ROOT/'episodes'/teacher.episode(path)/'teacher.json')
            for task in p['scored_task_ids']:
                if task not in record['labels']: continue
                row = record['labels'][task]
                if row['observation']['image_mask'][row['label']['mode']]: records.append(row)
                else: illegal += 1
        assert len(records) == m['teacher_images']-m['illegal_image_mask_images']
        rng = random.Random(seed); curve = []
        for epoch in range(BC_HP['epochs']):
            check_time(); rng.shuffle(records); losses = []
            for start in range(0,len(records),BC_HP['batch_size']):
                batch = records[start:start+BC_HP['batch_size']]
                optimizer.zero_grad(set_to_none=True); loss = interface.imitation_loss(actor,batch)
                if not torch.isfinite(loss): raise FloatingPointError('Nonfinite imitation loss')
                loss.backward()
                torch.nn.utils.clip_grad_norm_(actor.parameters(),BC_HP['gradient_clip'],error_if_nonfinite=True)
                optimizer.step(); losses.append((float(loss.detach()),len(batch)))
            curve.append(dict(epoch=epoch+1,loss=sum(v*n for v,n in losses)/len(records)))
            write_json(ROOT/'training'/f'{seed}_progress.json',dict(status='RUNNING',epoch=epoch+1,
                total_epochs=100,wall_seconds=time.perf_counter()-started))
        cp = ROOT/'training'/f'pure_imitation_{seed}.pt'
        save_checkpoint(cp,actor,critic,optimizer,seed,0,parent,'pure_imitation')
        # BC优化器只含actor；PPO重新建立原超参数下的联合优化器。
        optimizer = torch.optim.Adam(list(actor.parameters())+list(critic.parameters()),lr=learning.HP['lr'])
        initial = ROOT/'checkpoints'/str(seed)/'episode_0000.pt'
        save_checkpoint(initial,actor,critic,optimizer,seed,0,cp,'mappo')
        verify_run()
        result = dict(status='PASS',seed=seed,samples=len(records),illegal_image_mask_images=illegal,
            checkpoint=str(cp),checkpoint_sha256=sha(cp),initial=str(initial),curve=curve,
            wall_seconds=time.perf_counter()-started)
        write_json(ROOT/'training'/f'{seed}.json',result)
        write_json(ROOT/'training'/f'{seed}_progress.json',dict(status='PASS',epoch=100,checkpoint=str(cp)))
        return result
    except Exception:
        result = dict(status='FAILED',seed=seed,traceback=traceback.format_exc())
        write_json(ROOT/'training'/f'{seed}_FAILED.json',result); return result


def simulate(job):
    started = time.perf_counter(); env = observer = None; out = Path(job['out'])
    try:
        if out.exists() or out.with_suffix('.progress.json').exists(): raise FileExistsError(out)
        verify_run(); p = read(job['protocol']); verify(p)
        training = job['kind']=='train'
        assert p['date'] in (teacher.TRAIN if training else VALID)
        assert p['score_end_s']-p['score_start_s'] == (3600 if training else 21600)
        torch.set_num_threads(1)
        assert sha(job['checkpoint']) == job['checkpoint_sha256']
        data = torch.load(job['checkpoint'],weights_only=True,map_location='cpu')
        actor = interface.DynamicActor().eval(); actor.load_state_dict(data['state_dict'])
        critic = learning.Critic().eval(); critic.load_state_dict(data['critic'])
        initial = interface.DynamicActor().eval()
        initial.load_state_dict(torch.load(ROOT/'training'/f"pure_imitation_{job['seed']}.pt",
            weights_only=True,map_location='cpu')['state_dict'])
        cfg = config_from_dict(p['config']); env = teacher.GranularityEnv(cfg)
        sources = set(cfg.task_source_sat_ids); helpers = set(env.satellites)-sources
        plane = LocalInformationPlane(sources); plane.history_hops = 2; env.control_plane = plane
        rngseed = job['seed'] + (100000*job['episode'] if training else 0)
        policies = {s:interface.DynamicPolicy(actor,rngseed*1000+s,env,plane) for s in sources}
        wanted = set(p['scored_task_ids']); rows = []; metric_rows = []; order = 0
        action_hash = hashlib.sha256()
        if not training: observer = TrafficAudit(env)
        boundaries = {0:observer.snapshot()} if observer else {}
        for step in range(cfg.sim_steps):
            actions = []
            if step%cfg.decision_interval_steps == 0:
                full = env.export_state(); plane.enqueue(full)
                state = decision_state(full,cfg.decision_tile_limit_per_sat)
                for source in sorted({t['source_sat_id'] for t in state.tiles.values()}):
                    policy = policies[source]; known = set(policy.decisions)
                    actions.extend(policy.select_actions(plane.view(state,source,2)))
                    for task in policy.decisions:
                        if task in known or task not in wanted: continue
                        decision = policy.decisions[task]
                        row = dict(task_id=task,decision_order=order,**decision); order += 1
                        with torch.no_grad():
                            if training:
                                c = critic_observation(full)
                                c['source_node'] = c['nodes'][sorted(full.satellites).index(source)]
                                row['critic_observation'] = c; row['old_value'] = float(critic(row))
                            else:
                                entropy,kl = interface.distribution_metrics(actor,initial,decision['observation'])
                                metric_rows.append(dict(task_id=task,entropy=float(entropy),kl_initial=float(kl)))
                        rows.append(row)
                action_digest_update(action_hash,step,actions)
            env.step(actions)
            now = round(env.time*cfg.dt,8)
            if observer and now in (p['score_start_s'],p['score_end_s']): boundaries[now] = observer.snapshot()
            if env.time%round(60/cfg.dt) == 0:
                check_time()
                write_json(out.with_suffix('.progress.json'),dict(status='RUNNING',pid=os.getpid(),job=job,
                    sim_seconds=now,total_sim_seconds=p['sim_duration_s'],wall_seconds=time.perf_counter()-started))
        outcomes = env.summary()['task_outcomes']; scored = [o for o in outcomes if o['task_id'] in wanted]
        checks = invariants(env); mapping = {}; commitments(env,mapping)
        checks['followup600'] = len(scored)==len(wanted) and all(o['observed_age_seconds']>=600 for o in scored)
        checks['foreign_compute'] = [tid for c in env.compute_cohort_records.values() for tid in c['tile_ids']
            if c['sat_id'] in sources and env.tasks[env.tiles[tid].parent_task_id].source_sat_id != c['sat_id']]
        checks['illegal_input'] = [t.tile_id for t in env.tiles.values() if t.input_route and
            (len(t.input_route)-1>2 or any(s in sources for s in t.input_route[1:]))]
        checks['commitment_errors'] = [tid for r in rows if r['mode']<2 for tid,target in r['targets'].items()
            if tid in mapping and mapping[tid]!=target]
        checks['raw_unique'] = all(math.isclose(sum(env.tiles[tid].data_size_mb for tid in env.tasks[t].tile_ids),
            env.tasks[t].image_size_mb,abs_tol=1e-6) for t in env._dist_pipeline.raw_tasks)
        checks['sar_ingress'] = all(abs(v-plane.sent_mb_by_edge[k])<1e-6 for k,v in env.metrics.network_sent_mb.items()
            if k.startswith('isl:') and int(k.split('->')[-1]) in sources)
        checks['window_partition'] = all(len([b for tid in t.tile_ids for b in env.tiles[tid].windows_xyxy]) ==
            len({b for tid in t.tile_ids for b in env.tiles[tid].windows_xyxy}) for t in env.tasks.values())
        checks['passed'] = bool(checks['passed'] and checks['followup600'] and checks['raw_unique'] and checks['sar_ingress']
            and checks['window_partition'] and not checks['foreign_compute'] and not checks['illegal_input'] and not checks['commitment_errors'])
        result = dict(status='PASS' if checks['passed'] else 'AUDIT_MISMATCH',job=job,checks=checks,outcomes=outcomes,
            curve=[score(scored,d) for d in DEADLINES],latency=latency_stats(scored),
            R600=math.fsum(min(o['latency_seconds'],600) if o['status']=='completed' else 600 for o in scored)/len(scored),
            action_trace_sha256=action_hash.hexdigest(),policy_metrics=metric_rows,wall_seconds=time.perf_counter()-started,
            unacted_scored_ids=sorted(wanted-{r['task_id'] for r in rows}))
        if training:
            credited,credit = learning.assign_returns(rows,outcomes,wanted)
            assert credit['before_first_decision_events']==0
            result.update(records=credited,credit_audit=credit)
        else:
            start,end = p['score_start_s'],p['score_end_s']; final = observer.snapshot()
            result['resources'] = {name:resources(difference(a,b),helpers,duration,cfg) for name,a,b,duration in [
                ('whole',boundaries[0],final,p['sim_duration_s']),('main',boundaries[start],boundaries[end],end-start),
                ('warmup',boundaries[0],boundaries[start],start),('followup',boundaries[end],final,p['sim_duration_s']-end)]}
            result['choices'] = dict(Counter(('onboard_4','onboard_16','raw_ground')[r['mode']] for r in rows))
            result['decisions'] = rows
            result['actual_blocks'] = dict(Counter(str(env.tasks[r['task_id']].num_tiles) for r in rows if r['mode']<2))
        verify_run(); verify(p); out.parent.mkdir(parents=True,exist_ok=True)
        if training:
            with gzip.open(out,'wt') as f: json.dump(result,f,allow_nan=False)
        else: write_json(out,result)
        status = dict(status=result['status'],out=str(out),sha256=sha(out),wall_seconds=result['wall_seconds'])
    except Exception:
        status = dict(status='FAILED',job=job,traceback=traceback.format_exc())
        write_json(out.with_suffix('.FAILED.json'),status)
    finally:
        if observer: observer.close()
        if env: env.close()
    write_json(out.with_suffix('.progress.json'),status); return status


def run_jobs(function, jobs, stage):
    results = []
    with ProcessPoolExecutor(max_workers=min(9,len(jobs)),mp_context=multiprocessing.get_context('spawn')) as pool:
        futures = [pool.submit(function,j) for j in jobs]
        for future in as_completed(futures):
            results.append(future.result())
            write_json(ROOT/'batch_progress.json',dict(stage=stage,done=len(results),total=len(jobs),results=results))
    if any(r['status']!='PASS' for r in results): raise RuntimeError('Batch failed; preserved evidence; no retry')
    return results


def evaluate(m, states, point):
    jobs = []
    for seed,st in states.items():
        if st['stopped']: continue
        for path in m['validation']:
            cp = st['checkpoint']; date = read(path)['date']
            jobs.append(dict(kind='validation',seed=int(seed),episode=point,protocol=path,
                checkpoint=cp,checkpoint_sha256=sha(cp),out=str(ROOT/'validation'/str(point)/seed/f'{date}.json')))
    if not jobs: return
    run_jobs(simulate,jobs,f'validation_{point}')
    for seed,st in states.items():
        if st['stopped']: continue
        selected = [j for j in jobs if j['seed']==int(seed)]; assert len(selected)==3
        recs = [read(j['out']) for j in selected]; x = metrics(recs)
        if point==0: st['initial'] = x
        else:
            st['bad_streak'] = deterioration_streak(st['bad_streak'],x,st['initial'])
            if st['bad_streak']>=2: st.update(stopped=True,stop_reason='R600 worse than BC initialization twice consecutively')
        st['evaluations'].append(dict(episodes=point,checkpoint=st['checkpoint'],checkpoint_sha256=sha(st['checkpoint']),
            metrics=x,validation_hashes={j['out']:sha(j['out']) for j in selected}))
    write_json(ROOT/'progress.json',dict(stage='validated',point=point,states=states))
    report(states,False)
    write_json(ROOT/f'VALIDATION_{point}_READY.json',dict(status='READY',point=point))


def report(states, final, reason=None):
    selected = {}; full_curves = {}; daily = {}
    local = read(BC/'summary.json')['day_equal']
    names = [k for k in local if k.startswith(('zero_','history_','minload_'))]
    assert len(names)==6
    envelope = dict(P60=max(local[k]['P'][0] for k in names),P600=max(local[k]['P'][-1] for k in names),
        mean_completed_s=min(local[k]['mean_completed_s'] for k in names),R600=min(local[k]['R600'] for k in names))
    lines = ['# Dynamic granularity learning validation', '', 'Three validation days weighted equally. Tests remain SEALED.',
        'Original six-local-rule success envelope; extended dynamic heuristic references are reported separately.', '',
        '|Seed / episodes|P60/90/120/180/240/300/600 (%)|Mean / P50 / P90 (s)|R600 (s)|Unfinished|',
        '|---|---|---|---:|---:|']
    for seed,st in states.items():
        for row in st['evaluations']:
            x = row['metrics']; key = f'{seed}_{row["episodes"]}'
            lines.append(f'|{key}|'+ '/'.join(f'{100*v:.3f}' for v in x['P'])+'|'+
                '/'.join(f'{x[k]:.3f}' for k in ('mean_completed_s','P50_completed_s','P90_completed_s'))+
                f'|{x["R600"]:.3f}|{x["unfinished_total"]}|')
            sets = []; daily[key] = {}
            for file,h in row['validation_hashes'].items():
                assert sha(file)==h; r = read(file); p = read(r['job']['protocol'])
                wanted = set(p['scored_task_ids']); outcomes = [o for o in r['outcomes'] if o['task_id'] in wanted]
                assert len(outcomes)==len(wanted); sets.append(outcomes)
                daily[key][p['date']] = {k:r[k] for k in ('curve','latency','R600','resources','choices','actual_blocks')}
            xs = sorted({60.,600.,*[o['latency_seconds'] for oo in sets for o in oo if o['status']=='completed' and 60<=o['latency_seconds']<=600]})
            full_curves[key] = dict(D=xs,P=[sum(sum(o['status']=='completed' and o['latency_seconds']<=d for o in oo)/len(oo) for oo in sets)/3 for d in xs])
        if st['evaluations']:
            best = min(st['evaluations'],key=lambda row:selection_key(row,st['initial']))
            x = best['metrics']; success = dict(P60=x['P'][0]>envelope['P60'],
                mean_completed=x['mean_completed_s']<envelope['mean_completed_s'],
                P600=x['P'][-1]>=envelope['P600'],R600=x['R600']<envelope['R600'])
            selected[seed] = dict(**best,success=success,all_success=all(success.values()))
    references = {k:v for k,v in local.items() if not k.startswith('bc_')}
    references.update({'heuristic_'+k:v for k,v in read(MAIN/'dynamic_heuristics_v2_20261025/summary.json')['day_equal'].items()})
    references.update({'fixed_mappo_'+k:v['metrics'] for k,v in read(FIXED/'manifest.json')['selected'].items()})
    lines += ['', '|Reference|P60/90/120/180/240/300/600 (%)|Mean (s)|R600 (s)|Unfinished|',
        '|---|---|---:|---:|---:|']
    for name,x in references.items():
        lines.append(f'|{name}|'+ '/'.join(f'{100*v:.3f}' for v in x['P'])+
            f'|{x["mean_completed_s"]:.3f}|{x["R600"]:.3f}|{x["unfinished_total"]}|')
    dispersion = {}
    if len(selected)==3:
        for key in ('R600','mean_completed_s','unfinished_total'):
            values = [r['metrics'][key] for r in selected.values()]
            dispersion[key] = dict(sample_std=float(np.std(values,ddof=1)),min=min(values),max=max(values))
        values = np.array([r['metrics']['P'] for r in selected.values()])
        dispersion['P'] = dict(sample_std=values.std(axis=0,ddof=1).tolist(),min=values.min(axis=0).tolist(),max=values.max(axis=0).tolist())
    write_json(ROOT/'summary.json',dict(states=states,selected=selected,original_local_envelope=envelope,final=final,
        stop_reason=reason,tests_sealed=True,daily=daily,references=references,selected_seed_dispersion=dispersion))
    write_json(ROOT/'full_curves.json',full_curves)
    if final: write_json(ROOT/'BEST_CHECKPOINTS.json',selected)
    lines += ['',f'Final: {final}; reason: {reason}', 'Dynamic granularity and four-block Orin memory remain subject to the registered limitations.']
    (ROOT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    if full_curves:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        fig,ax = plt.subplots(figsize=(10,6),layout='constrained')
        for key,c in full_curves.items(): ax.step(c['D'],np.array(c['P'])*100,where='post',label=key)
        ax.set(xlabel='Deadline (s)',ylabel='P(D) (%)',xlim=(60,600)); ax.set_xticks(DEADLINES)
        ax.legend(fontsize=8,ncol=3); ax.grid(alpha=.2); fig.savefig(ROOT/'validation_curves.png',dpi=160); plt.close(fig)


def finish(states, reason):
    verify_run(False); report(states,True,reason)
    budgets = {seed:dict(updated_episodes=st['episodes'],fraction_of_176=st['episodes']/176,
        fraction_of_original_703=st['episodes']/703,collected_pass=sum(1 for p in (ROOT/'rollouts'/seed).glob('*.progress.json')
            if read(p).get('status')=='PASS')) for seed,st in states.items()}
    write_json(ROOT/'final_budget.json',budgets)
    write_json(ROOT/'FINAL_AUDIT.json',dict(status='PASS',hashes={str(f):sha(f) for f in ROOT.rglob('*')
        if f.is_file() and f.suffix in ('.json','.pt','.gz','.md','.png') and f.name not in ('FINAL_AUDIT.json','final_status.json')},tests_sealed=True))
    write_json(ROOT/'final_status.json',dict(status='COMPLETE' if reason=='budget_or_early_stop' else 'DEADLINE_FROZEN',
        reason=reason,states=states,tests_sealed=True,formal_test_dynamic_eligible=
        set(states)==set(map(str,SEEDS)) and all(st['evaluations'] for st in states.values())))


def panel():
    states = {}
    try:
        m = verify_run(); torch.set_num_threads(1)
        write_json(ROOT/'progress.json',dict(stage='pure_imitation',states=states))
        run_jobs(train_bc,list(SEEDS),'pure_imitation')
        for seed in SEEDS:
            states[str(seed)] = dict(episodes=0,checkpoint=str(ROOT/'checkpoints'/str(seed)/'episode_0000.pt'),
                bad_streak=0,stopped=False,evaluations=[])
        evaluate(m,states,0)
        for target in (88,176):
            while any(not st['stopped'] and st['episodes']<target for st in states.values()):
                check_time(); jobs = []; batches = {}
                for seed,st in states.items():
                    if st['stopped'] or st['episodes']>=target: continue
                    batch = []
                    for episode in range(st['episodes']+1,min(st['episodes']+3,target)+1):
                        cp = st['checkpoint']
                        batch.append(dict(kind='train',seed=int(seed),episode=episode,protocol=m['schedule'][seed][episode-1],
                            checkpoint=cp,checkpoint_sha256=sha(cp),out=str(ROOT/'rollouts'/seed/f'{episode:04d}.json.gz')))
                    batches[seed] = batch; jobs.extend(batch)
                write_json(ROOT/'progress.json',dict(stage='sampling',target=target,states=states))
                run_jobs(simulate,jobs,'sampling')
                for seed,batch in batches.items():
                    check_time(); st = states[seed]; data = torch.load(st['checkpoint'],weights_only=True,map_location='cpu')
                    actor = interface.DynamicActor(); actor.load_state_dict(data['state_dict'])
                    critic = learning.Critic(); critic.load_state_dict(data['critic'])
                    optimizer = torch.optim.Adam(list(actor.parameters())+list(critic.parameters()),lr=learning.HP['lr'])
                    optimizer.load_state_dict(data['optimizer']); rows = []
                    for job in batch:
                        with gzip.open(job['out'],'rt') as f: result = json.load(f)
                        assert result['status']=='PASS' and result['job']['checkpoint_sha256']==sha(st['checkpoint'])
                        rows.extend(result['records'])
                    last = batch[-1]['episode']
                    history = learning.update(actor,critic,optimizer,rows,int(seed)*10000+last)
                    cp = ROOT/'checkpoints'/seed/f'episode_{last:04d}.pt'
                    save_checkpoint(cp,actor,critic,optimizer,int(seed),last,st['checkpoint'],'mappo')
                    write_json(ROOT/'updates'/seed/f'{last:04d}.json',dict(status='PASS',history=history,
                        samples=len(rows),rollout_hashes={j['out']:sha(j['out']) for j in batch},checkpoint_sha256=sha(cp)))
                    st.update(episodes=last,checkpoint=str(cp))
                    write_json(ROOT/'progress.json',dict(stage='updating',target=target,states=states))
            evaluate(m,states,target)
        finish(states,'budget_or_early_stop')
    except Exception:
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',traceback=traceback.format_exc(),states=states,no_retry=True,tests_sealed=True))


def prepare():
    assert not ROOT.exists(); tm = teacher.verify_manifest()
    audit = teacher.ROOT/'independent_audit_20261026/AUDIT.json'; cov = read(audit); assert cov['status']=='PASS'
    bc = read(BC/'manifest.json')
    for path in tm['train']+bc['validation']: verify(read(path))
    hashes = dict(tm['hashes']); hashes.update(cov['teacher_hashes'])
    hashes.update(read(FIXED/'manifest.json')['frozen_files'])
    for path in [Path(__file__),Path('analysis/dynamic_mappo.py'),Path('analysis/main_mappo.py'),
        Path('analysis/run_main_mappo.py'),Path('analysis/run_dynamic_heuristics_v2.py'),Path('train_marl.py'),
        Path('analysis/audit_dynamic_teachers_20261026.py'),Path('tests/test_dynamic_mappo.py'),
        teacher.ROOT/'manifest.json',teacher.ROOT/'FINAL_AUDIT.json',audit,BC/'summary.json',
        MAIN/'dynamic_heuristics_v2_20261025/summary.json',PROTOCOL]: hashes[str(path)] = sha(path)
    for path,h in hashes.items(): assert sha(path)==h,path
    schedule = {}
    for seed in SEEDS:
        rng = random.Random(seed); schedule[str(seed)] = [rng.choice(tm['train']) for _ in range(176)]
    write_json(ROOT/'manifest.json',dict(train=tm['train'],validation=bc['validation'],seeds=SEEDS,
        hashes=hashes,teacher_hashes=cov['teacher_hashes'],teacher_images=cov['images'],
        illegal_image_mask_images=cov['illegal_image_mask_images'],schedule=schedule,
        bc_hyperparameters=BC_HP,ppo_hyperparameters=learning.HP,workers=9,torch_threads=1,limits=[0,88,176],
        deadline_utc=END.isoformat(),tests_sealed=True,no_retry=True))
    print('PREPARED',sha(ROOT/'manifest.json'))


def supervise():
    verify_run()
    with (ROOT/'panel.log').open('x') as log:
        child = subprocess.Popen([sys.executable,'-B','-m','analysis.run_dynamic_learning','panel'],
            stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    guard = subprocess.Popen(['caffeinate','-i','-w',str(os.getpid())],start_new_session=True,
        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    write_json(ROOT/'launch.json',dict(pid=child.pid,supervisor_pid=os.getpid(),caffeinate_pid=guard.pid,
        created_utc=datetime.now(timezone.utc).isoformat(),command=f'{sys.executable} -B -m analysis.run_dynamic_learning panel',
        manifest_sha256=sha(ROOT/'manifest.json'),workers=9))
    while child.poll() is None:
        if datetime.now(timezone.utc)>=END:
            os.killpg(child.pid,signal.SIGTERM)
            try: child.wait(timeout=5)
            except subprocess.TimeoutExpired: os.killpg(child.pid,signal.SIGKILL); child.wait()
            states = read(ROOT/'progress.json').get('states',{})
            finish(states,'dynamic_deadline'); return
        time.sleep(2)
    if not (ROOT/'final_status.json').exists():
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',reason='Coordinator exited without terminal status',
            returncode=child.returncode,no_retry=True,tests_sealed=True))


def launch():
    verify_run()
    assert not (ROOT/'launch.json').exists() and not (ROOT/'supervisor.log').exists()
    with (ROOT/'supervisor.log').open('x') as log:
        proc = subprocess.Popen([sys.executable,'-B','-m','analysis.run_dynamic_learning','supervise'],
            stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    print('SUPERVISOR',proc.pid)


if __name__=='__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('mode',choices=['prepare','launch','supervise','panel'])
    globals()[parser.parse_args().mode]()
