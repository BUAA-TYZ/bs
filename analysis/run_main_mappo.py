"""冻结场景MAPPO编排：诊断回放、同步采样、季度验证和退化停止。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import random
import subprocess
import sys
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import numpy as np
import torch
from analysis import main_mappo as learning
from analysis import main_bc_interface as interface
from analysis import run_main_rl_stage1 as stage1
from analysis.main_rl_interface import critic_observation, joint_statistics
from analysis.local_information_baselines import LocalInformationPlane, LocalMinLoad
from analysis.prepare_main_bc_stage2 import ROOT as BC, MAIN, TRAIN, VALID, SEEDS
from analysis.run_main_local_panel import verify_main
from analysis.run_load_pressure_short import verify
from analysis.run_sar_frozen import invariants
from analysis.sar_three_plan import CandidateEnv, ThreePlanChoice
from analysis.redesign_scenario import write_json
from analysis.report_main_posthoc import score, latency_stats, DEADLINES
from sim.config import config_from_dict
from sim.control import ControlPlane
from sim.marl.decision import decision_state

ROOT = MAIN/'mappo_20261022'
PROTOCOL = MAIN/'MAPPO_PROTOCOL_20261022.md'
LIMITS = [176,352,528,703]
END = datetime(2026,11,2,16,tzinfo=timezone.utc)


def read(path):
    return json.loads(Path(path).read_text())


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify_run():
    if datetime.now(timezone.utc)>=END:
        raise TimeoutError('11-02 deadline reached; no extension')
    m=read(ROOT/'manifest.json')
    verify_main()
    for path, expected in m['hashes'].items():
        if sha(path)!=expected:raise AssertionError(f'Frozen input changed: {path}')
    return m


def save_checkpoint(path, actor, critic, optimizer, seed, episodes, parent):
    if path.exists():raise FileExistsError(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    torch.save(dict(state_dict=actor.state_dict(),critic=critic.state_dict(),optimizer=optimizer.state_dict(),
                    seed=seed,episodes=episodes,parent_sha256=sha(parent),initial_sha256=sha(BC/'training'/f'pure_imitation_{seed}.pt'),
                    train_dates=TRAIN,validation_used_for_gradient=False,hyperparameters=learning.HP),path)


def prepare():
    if ROOT.exists():raise FileExistsError(ROOT)
    verify_main()
    bc=read(BC/'manifest.json')
    for key in ('entry_hashes','episode_hashes'):
        for path,h in bc[key].items():assert sha(path)==h
    for p in bc['train']+bc['validation']:verify(read(p))
    assert read(BC/'final_status.json')['status']=='COMPLETE'
    files=[Path(__file__),Path('analysis/main_mappo.py'),Path('tests/test_main_mappo.py'),PROTOCOL,
           BC/'manifest.json',BC/'summary.json',BC/'FINAL_AUDIT.json',Path('train_marl.py')]
    hashes={str(p):sha(p) for p in files}
    hashes.update(bc['entry_hashes']);hashes.update(bc['episode_hashes'])
    hashes.update(read(BC/'FINAL_AUDIT.json')['hashes'])
    schedule={}
    for seed in SEEDS:
        rng=random.Random(seed)
        schedule[str(seed)]=[rng.choice(bc['train']) for _ in range(703)]
    ROOT.mkdir(parents=True)
    manifest=dict(train=bc['train'],validation=bc['validation'],seeds=SEEDS,limits=LIMITS,hashes=hashes,
                  schedule=schedule,hyperparameters=learning.HP,workers=9,deadline=END.isoformat(),test_access=False)
    write_json(ROOT/'manifest.json',manifest)
    for seed in SEEDS:
        torch.manual_seed(seed);actor=interface.HierarchicalActor();critic=learning.Critic()
        parent=BC/'training'/f'pure_imitation_{seed}.pt'
        actor.load_state_dict(torch.load(parent,weights_only=True,map_location='cpu')['state_dict'])
        opt=torch.optim.Adam(list(actor.parameters())+list(critic.parameters()),lr=learning.HP['lr'])
        save_checkpoint(ROOT/'checkpoints'/str(seed)/'episode_0000.pt',actor,critic,opt,seed,0,parent)
    write_json(ROOT/'prepared.json',dict(status='PASS',manifest_sha256=sha(ROOT/'manifest.json')))


class TailRecorder:
    """被动旁路采样；1秒证据计数不是精确连续时长，不回写调度状态。"""
    def __init__(self,env,watch):
        self.env,self.watch=env,set(watch);self.events=[];self.samples={}
        self.original=env._log_tile_event
        def log(tile,event,sat_from,sat_to,extra=None):
            if tile.parent_task_id in self.watch:
                self.events.append(dict(task=tile.parent_task_id,tile=tile.tile_id,time=env.time*env.cfg.dt,
                                        event=event,source=sat_from,target=sat_to,extra=extra))
            return self.original(tile,event,sat_from,sat_to,extra)
        env._log_tile_event=log

    def tick(self):
        env=self.env
        transfers=env.ground_tile_transfers+env.downlink_transfers
        visible=[tr for tr in transfers if env.vis_cache.is_visible(tr.src_sat,tr.gs_id,env.ground_stations)]
        for task in self.watch:
            if task not in env.tasks or env.tasks[task].completion_time is not None:continue
            tiles=[env.tiles[t] for t in env.tasks[task].tile_ids]
            row=self.samples.setdefault(task,dict(observed_s=0,ground_contention_s=0,compute_queue_s=0,
                                                  committed_before_input_s=0))
            row['observed_s']+=1
            own=[tr for tr in visible if env.tiles[tr.tile_id].parent_task_id==task]
            if own and any(env.tiles[tr.tile_id].parent_task_id!=task for tr in visible):row['ground_contention_s']+=1
            if any(t.state.name=='READY' and t.location!=env.tasks[task].source_sat_id
                   and t.timestamps.end_tx is not None and t.timestamps.end_tx<env.time
                   for t in tiles):row['compute_queue_s']+=1
            if any(t.input_target_sat_id is not None and t.timestamps.start_compute is None and t.state.name=='QUEUED'
                   for t in tiles):row['committed_before_input_s']+=1

    def result(self,mapping):
        env=self.env
        return dict(events=self.events,samples=self.samples,interval_s=1,
            tiles={tid:dict(task=t.parent_task_id,processor=mapping.get(tid),state=t.state.name,
                     input_route=list(t.input_route),result_path=list(t.result_path),raw_path=list(t.raw_relay_path),
                     timestamps={k:None if v is None else v*env.cfg.dt for k,v in asdict(t.timestamps).items()})
                   for tid,t in env.tiles.items() if t.parent_task_id in self.watch})


def rollout(job):
    started=time.perf_counter();env=None;out=Path(job['out'])
    try:
        if out.exists() or out.with_suffix('.FAILED.json').exists():raise FileExistsError(out)
        verify_run();p=read(job['protocol']);verify(p)
        kind=job['kind'];assert p['date'] in (TRAIN if kind=='train' else VALID)
        torch.set_num_threads(1)
        cfg=config_from_dict(p['config']);env=CandidateEnv(cfg);sources=set(cfg.task_source_sat_ids)
        local={};actor=critic=initial=None
        if job['method']=='v3':plane=ControlPlane(sources);policy=ThreePlanChoice(cfg.relay_max_hops)
        else:
            class Plane(LocalInformationPlane):
                estimator='zero'
                history_hops=2
            plane=Plane(sources)
            if job['method']=='minload_2':local={s:LocalMinLoad(cfg.relay_max_hops) for s in sources}
            else:
                data=torch.load(job['checkpoint'],weights_only=True,map_location='cpu')
                assert sha(job['checkpoint'])==job['checkpoint_sha256']
                actor=interface.HierarchicalActor().eval();actor.load_state_dict(data['state_dict'])
                if kind=='train':
                    critic=learning.Critic().eval();critic.load_state_dict(data['critic'])
                initial=interface.HierarchicalActor().eval()
                initial.load_state_dict(torch.load(BC/'training'/f"pure_imitation_{job['seed']}.pt",weights_only=True,map_location='cpu')['state_dict'])
                rngseed=job['seed'] if kind!='train' else job['seed']+100000*job['episode']
                local={s:interface.CommittedPolicy(actor,rngseed*1000+s) for s in sources}
        env.control_plane=plane;records=[];order=0;action_hash=hashlib.sha256();metric_rows=[]
        tail=TailRecorder(env,job['watch']) if kind=='diagnostic' else None
        wanted=set(p['scored_task_ids'])
        for step in range(cfg.sim_steps):
            actions=[]
            if step%cfg.decision_interval_steps==0:
                full=env.export_state();plane.enqueue(full);state=decision_state(full,cfg.decision_tile_limit_per_sat)
                if local:
                    for source in sorted({t['source_sat_id'] for t in state.tiles.values()}):
                        pol=local[source];known=set(pol.decisions) if actor is not None else set()
                        actions.extend(pol.select_actions(plane.view(state,source,2)))
                        if actor is not None:
                            for task in pol.decisions:
                                if task in known:continue
                                d=pol.decisions[task]
                                if task not in wanted:continue
                                row=dict(task_id=task,decision_order=order,**d);order+=1
                                with torch.no_grad():
                                    entropy,kl=learning.distribution_metrics(actor,initial,d['observation'])
                                    metric_rows.append(dict(task_id=task,entropy=float(entropy),kl_initial=float(kl)))
                                    if critic is not None:
                                        c=critic_observation(full)
                                        c['source_node']=c['nodes'][sorted(full.satellites).index(source)]
                                        row['critic_observation']=c;row['old_value']=float(critic(row))
                                records.append(row)
                else:actions=policy.select_actions(state)
                stage1.action_digest_update(action_hash,step,actions)
            env.step(actions)
            if tail and env.time%round(1/cfg.dt)==0:tail.tick()
            if env.time%round(60/cfg.dt)==0:
                if datetime.now(timezone.utc)>=END:raise TimeoutError('11-02 deadline')
                write_json(out.with_suffix('.progress.json'),dict(status='RUNNING',pid=os.getpid(),job=job,
                    sim_seconds=env.time*cfg.dt,total_sim_seconds=p['sim_duration_s'],wall_seconds=time.perf_counter()-started))
        outcomes=env.summary()['task_outcomes'];scored=[o for o in outcomes if o['task_id'] in wanted]
        checks=invariants(env);mapping={};stage1.commitments(env,mapping)
        checks['followup']=len(scored)==len(wanted) and all(o['observed_age_seconds']>=600 for o in scored)
        checks['foreign_compute']=[tid for c in env.compute_cohort_records.values() for tid in c['tile_ids']
            if c['sat_id'] in sources and env.tasks[env.tiles[tid].parent_task_id].source_sat_id!=c['sat_id']]
        checks['illegal_input']=[t.tile_id for t in env.tiles.values() if t.input_route and
            (len(t.input_route)-1>2 or any(s in sources for s in t.input_route[1:]))]
        checks['sar_ingress']=[v-plane.sent_mb_by_edge[k] for k,v in env.metrics.network_sent_mb.items()
                              if k.startswith('isl:') and int(k.split('->')[-1]) in sources]
        checks['commitment_errors']=[tid for r in records if r['mode']==0 for tid,target in r['targets'].items()
                                     if tid in mapping and mapping[tid]!=target]
        checks['raw_unique_errors']=[task for task in env._dist_pipeline.raw_tasks if not math.isclose(
            sum(env.tiles[t].data_size_mb for t in env.tasks[task].tile_ids),env.tasks[task].image_size_mb,abs_tol=1e-6)]
        checks['passed']=bool(checks['passed'] and checks['followup'] and not checks['foreign_compute'] and
            not checks['illegal_input'] and not checks['commitment_errors'] and not checks['raw_unique_errors']
            and all(abs(x)<1e-6 for x in checks['sar_ingress']))
        result=dict(status='PASS' if checks['passed'] else 'AUDIT_MISMATCH',job=job,checks=checks,outcomes=outcomes,
                    curve=[score(scored,d) for d in DEADLINES],latency=latency_stats(scored),
                    R600=math.fsum(min(o['latency_seconds'],600) if o['status']=='completed' else 600 for o in scored)/len(scored),
                    action_trace_sha256=action_hash.hexdigest(),policy_metrics=metric_rows,config_sha256=p['config_sha256'],
                    wall_seconds=time.perf_counter()-started)
        if kind=='train':
            rows,credit=learning.assign_returns(records,outcomes,wanted)
            result.update(records=rows,credit_audit=credit,decision_count=len(rows),unacted_scored_ids=sorted(wanted-{r['task_id'] for r in rows}))
        elif kind=='diagnostic':
            original=read(BC/'validation'/p['date']/f"{job['method']}.json")
            result['replay_equal']={k:result[k]==original[k] for k in ('outcomes','action_trace_sha256')}
            if not all(result['replay_equal'].values()):result['status']='AUDIT_MISMATCH'
            result.update(tail=tail.result(mapping),decisions=records,commitments=mapping)
        verify_run();verify(p)
        out.parent.mkdir(parents=True,exist_ok=True)
        if kind=='train':
            with gzip.open(out,'wt') as f:json.dump(result,f,allow_nan=False)
        else:write_json(out,result)
        status=dict(status=result['status'],out=str(out),sha256=sha(out),wall_seconds=result['wall_seconds'])
    except Exception:
        status=dict(status='FAILED',job=job,traceback=traceback.format_exc())
        write_json(out.with_suffix('.FAILED.json'),status)
    finally:
        if env is not None:env.close()
    write_json(out.with_suffix('.progress.json'),status)
    return status


def run_jobs(jobs):
    results=[]
    with ProcessPoolExecutor(max_workers=9,mp_context=multiprocessing.get_context('spawn')) as pool:
        futures=[pool.submit(rollout,j) for j in jobs]
        for f in as_completed(futures):
            results.append(f.result())
            write_json(ROOT/'batch_progress.json',dict(done=len(results),total=len(jobs),results=results))
    if any(r['status']!='PASS' for r in results):raise RuntimeError('Batch failed; no retry; see batch_progress.json')
    return results


def metrics(records):
    return dict(P=np.mean([[c['P_lower'] for c in r['curve']] for r in records],axis=0).tolist(),
                mean_completed_s=float(np.mean([r['latency']['mean_completed_s'] for r in records])),
                P50_completed_s=float(np.mean([r['latency']['P50_completed_s'] for r in records])),
                P90_completed_s=float(np.mean([r['latency']['P90_completed_s'] for r in records])),
                R600=float(np.mean([r['R600'] for r in records])),unfinished_total=sum(r['latency']['unfinished'] for r in records),
                kl_initial=float(np.mean([np.mean([x['kl_initial'] for x in r['policy_metrics']]) for r in records])),
                entropy=float(np.mean([np.mean([x['entropy'] for x in r['policy_metrics']]) for r in records])))


def selection_key(row,initial):
    x=row['metrics']
    return (x['P'][-1]<initial['P'][-1]-1e-12,x['R600'],-x['P'][0],x['mean_completed_s'],row['episodes'])


def deterioration_streak(previous,current,initial):
    return previous+1 if current['R600']>initial['R600']+1e-9 else 0


def tail_report(m):
    rows=[];counts={}
    for path in m['validation']:
        p=read(path);date=p['date'];base=read(BC/'validation'/date/'bc_1701.json')
        watched=[o for o in base['outcomes'] if o['task_id'] in p['scored_task_ids'] and
                 (o['status']!='completed' or o['latency_seconds']>300)]
        replay={method:read(ROOT/'diagnostics'/date/f'{method}.json') for method in ('bc_1701','v3','minload_2')}
        for o in watched:
            task=o['task_id'];r=replay['bc_1701'];decision=next((x for x in r['decisions'] if x['task_id']==task),None)
            ts=[x for x in r['tail']['tiles'].values() if x['task']==task]
            samples=r['tail']['samples'].get(task,{})
            processors=set(str(x['processor']) for x in ts);ends=[x['timestamps']['end_downlink'] for x in ts]
            by300=sum(e is not None and e<=o['release_seconds']+300 for e in ends)
            tags=[]
            if decision and decision['mode']==1 and samples.get('ground_contention_s',0)>0:tags.append('raw_ground_contention_observed')
            if decision and any(isinstance(t,int) and t!=o['source_sat_id'] for t in decision['targets'].values()) and samples.get('compute_queue_s',0)>0:tags.append('helper_queue_observed')
            if decision and len(processors)>1 and 0<by300<len(ts):tags.append('distributed_tail_block_observed')
            if not tags:tags=['other_or_insufficient_causal_evidence']
            counts[tags[0]]=counts.get(tags[0],0)+1
            compare={}
            for method,rec in replay.items():
                out=next(x for x in rec['outcomes'] if x['task_id']==task)
                tiles=[x for x in rec['tail']['tiles'].values() if x['task']==task]
                compare[method]=dict(outcome=out,processor_windows_or_tiles=[dict(processor=t['processor'],timestamps=t['timestamps']) for t in tiles])
            rows.append(dict(date=date,task_id=task,primary=tags[0],evidence_tags=tags,sampled_wait_evidence=samples,
                             delivered_blocks_by300=by300,total_blocks=len(ts),comparison=compare))
    write_json(ROOT/'TAIL_DIAGNOSIS.json',dict(rows=rows,primary_counts=counts,
        limitations='Passive replay association, not counterfactual causality. 1s samples. No path at a sampled instant does not prove future unreachability. Unproven reasons remain other.'))
    lines=['# 纯模仿1701尾部诊断','','动作哈希及逐图outcomes均与冻结验证精确相同；以下是观察证据，非因果隔离。',
           '同图v3/minload_2处理位置及逐块时间戳见TAIL_DIAGNOSIS.json和diagnostics目录。',
           '类别可能重叠；主类别按地面争用、辅助星排队、分散尾块、其他顺序登记，不将相关性当唯一原因。','',
           '| 日期 | 图像 | 主类别 | v3 / minload2 / BC时延(s) |','|---|---|---|---|']
    for r in rows:
        vals=[r['comparison'][k]['outcome']['latency_seconds'] for k in ('v3','minload_2','bc_1701')]
        lines.append(f"| {r['date']} | {r['task_id']} | {r['primary']} | {vals} |")
    (ROOT/'TAIL_DIAGNOSIS.md').write_text('\n'.join(lines)+'\n')


def write_report(states,quarter,final=False):
    bc=read(BC/'summary.json');locals_=[v for k,v in bc['day_equal'].items() if k.startswith(('zero','history','minload'))]
    results={};lines=[f'# MAPPO {"最终" if final else "阶段"}报告','',
        '三验证日等权，训练仅10-16至21。纯模仿及六局部原记录复用；测试继续封存。',
        '| 种子/片段预算 | P60/90/120/180/240/300/600 (%) | 均值/P50/P90(s) | R600 | 未完成 | KL/熵 |',
        '|---|---|---|---:|---:|---|']
    for seed,st in states.items():
        selected=min(st['evaluations'],key=lambda r:selection_key(r,st['initial']))
        current=selected if final else st['evaluations'][-1];x=current['metrics']
        success=dict(P60=x['P'][0]>max(v['P'][0] for v in locals_),mean_latency=x['mean_completed_s']<min(v['mean_completed_s'] for v in locals_),
                     P600=x['P'][-1]>=max(v['P'][-1] for v in locals_)-1e-12,R600=x['R600']<min(v['R600'] for v in locals_))
        results[seed]=dict(selected=selected,current=current,success=success,stop_reason=st.get('stop_reason'),evaluations=st['evaluations'])
        diagnostic=f"{x['kl_initial']:.4f}/{x['entropy']:.4f}" if 'entropy' in x else '0 / not remeasured'
        lines.append(f"| {seed}/{current['episodes']} | "+'/'.join(f'{v*100:.2f}' for v in x['P'])+
            f" | {x['mean_completed_s']:.2f}/{x['P50_completed_s']:.2f}/{x['P90_completed_s']:.2f} | {x['R600']:.2f} | {x['unfinished_total']} | {diagnostic} |")
    lines+=['','逐项成功判定及全部checkpoint、逐日原始数据见JSON；尾部诊断见TAIL_DIAGNOSIS.md。',
            '验证选模不代表测试成绩；KL为当前访问观测上的精确KL(current||initial)。初始化模型可被选回；全局v3不是数学上界。']
    for seed,r in results.items():lines.append(f"种子{seed}: {r['success']}；停止原因 {r['stop_reason']}。")
    name='REPORT' if final else f'MILESTONE_{quarter}'
    write_json(ROOT/f'{name}.json',dict(results=results,quarter=quarter,final=final,tests_sealed=True))
    write_json(ROOT/'BEST_CHECKPOINTS.json',{seed:r['selected'] for seed,r in results.items()})
    (ROOT/f'{name}.md').write_text('\n'.join(lines)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,ax=plt.subplots(figsize=(10,6))
    for method in ('v3','minload_2','history_2'):
        ax.plot(DEADLINES,np.array(bc['day_equal'][method]['P'])*100,'--',label=method)
    full_curves={}
    for seed,r in results.items():
        ep=r['current']['episodes']
        q=LIMITS.index(ep)+1 if ep else None
        records=[read(ROOT/'validation'/f'q{q}'/seed/f'{date}.json') if q else
                 read(BC/'validation'/date/f'bc_{seed}.json') for date in VALID]
        protocols=[read(p) for p in read(ROOT/'manifest.json')['validation']]
        sets=[[o for o in rec['outcomes'] if o['task_id'] in p['scored_task_ids']] for rec,p in zip(records,protocols)]
        xs=sorted({60.,600.,*[o['latency_seconds'] for oo in sets for o in oo if o['status']=='completed' and 60<=o['latency_seconds']<=600]})
        ys=[float(np.mean([sum(o['status']=='completed' and o['latency_seconds']<=d for o in oo)/len(oo) for oo in sets])) for d in xs]
        full_curves[seed]=dict(deadlines=xs,P=ys)
        ax.step(xs,np.array(ys)*100,where='post',label=f'MAPPO {seed}')
        ax.plot(DEADLINES,np.array(bc['day_equal'][f'bc_{seed}']['P'])*100,':',label=f'BC {seed}')
    ax.set(xlabel='Deadline (s)',ylabel='P(D), %',ylim=(0,100));ax.legend();ax.grid(alpha=.2)
    fig.tight_layout();fig.savefig(ROOT/f'{name}_curves.png',dpi=150);plt.close(fig)
    write_json(ROOT/f'{name}_full_curves.json',full_curves)


def panel():
    try:
        m=verify_run();torch.set_num_threads(1)
        states={};bc=read(BC/'summary.json')
        for seed in SEEDS:
            cp=ROOT/'checkpoints'/str(seed)/'episode_0000.pt';initial=bc['day_equal'][f'bc_{seed}']
            states[str(seed)]=dict(episodes=0,checkpoint=str(cp),initial=initial,bad_streak=0,stopped=False,
                evaluations=[dict(episodes=0,checkpoint=str(cp),checkpoint_sha256=sha(cp),metrics=initial)])
        diagnostic=[]
        for path in m['validation']:
            p=read(path);base=read(BC/'validation'/p['date']/'bc_1701.json')
            watch=[o['task_id'] for o in base['outcomes'] if o['task_id'] in p['scored_task_ids'] and (o['status']!='completed' or o['latency_seconds']>300)]
            for method in ('bc_1701','v3','minload_2'):
                cp=BC/'training/pure_imitation_1701.pt'
                diagnostic.append(dict(kind='diagnostic',method=method,seed=1701,protocol=path,watch=watch,
                    checkpoint=str(cp),checkpoint_sha256=sha(cp),out=str(ROOT/'diagnostics'/p['date']/f'{method}.json')))
        first=True
        for quarter,target in enumerate(LIMITS,1):
            while any(not s['stopped'] and s['episodes']<target for s in states.values()):
                jobs=[];per_seed={}
                for seed,st in states.items():
                    if st['stopped'] or st['episodes']>=target:continue
                    per_seed[seed]=[]
                    for episode in range(st['episodes']+1,min(target,st['episodes']+3)+1):
                        cp=st['checkpoint'];job=dict(kind='train',method='mappo',seed=int(seed),episode=episode,
                            protocol=m['schedule'][seed][episode-1],checkpoint=cp,checkpoint_sha256=sha(cp),
                            out=str(ROOT/'rollouts'/seed/f'{episode:04d}.json.gz'))
                        jobs.append(job);per_seed[seed].append(job)
                if first:
                    # 先三项采样加六项回放，其余任务仍在同一9槽进程池排队。
                    jobs=jobs[::3]+diagnostic+ [j for i,j in enumerate(jobs) if i%3!=0]
                run_jobs(jobs)
                if first:
                    tail_report(m);write_json(ROOT/'REPLAY_GATE.json',dict(status='PASS',runs=9));first=False
                for seed,batch in per_seed.items():
                    st=states[seed];data=torch.load(st['checkpoint'],weights_only=True,map_location='cpu')
                    actor=interface.HierarchicalActor();actor.load_state_dict(data['state_dict'])
                    critic=learning.Critic();critic.load_state_dict(data['critic'])
                    optimizer=torch.optim.Adam(list(actor.parameters())+list(critic.parameters()),lr=learning.HP['lr']);optimizer.load_state_dict(data['optimizer'])
                    rows=[]
                    for j in batch:
                        with gzip.open(j['out'],'rt') as f:record=json.load(f)
                        assert record['job']['checkpoint_sha256']==sha(st['checkpoint']);rows.extend(record['records'])
                    last=batch[-1]['episode'];history=learning.update(actor,critic,optimizer,rows,int(seed)*10000+last)
                    cp=ROOT/'checkpoints'/seed/f'episode_{last:04d}.pt'
                    save_checkpoint(cp,actor,critic,optimizer,int(seed),last,st['checkpoint'])
                    write_json(ROOT/'updates'/seed/f'{last:04d}.json',dict(status='PASS',samples=len(rows),history=history,
                        rollout_hashes={j['out']:sha(j['out']) for j in batch},checkpoint_sha256=sha(cp)))
                    st.update(episodes=last,checkpoint=str(cp))
                write_json(ROOT/'progress.json',dict(stage='training',quarter=quarter,target=target,states=states))
            jobs=[]
            for seed,st in states.items():
                if st['stopped']:continue
                for path in m['validation']:
                    p=read(path);cp=st['checkpoint']
                    jobs.append(dict(kind='validation',method='mappo',seed=int(seed),protocol=path,checkpoint=cp,checkpoint_sha256=sha(cp),
                        out=str(ROOT/'validation'/f'q{quarter}'/seed/f"{p['date']}.json")))
            if jobs:run_jobs(jobs)
            for seed,st in states.items():
                if st['stopped']:continue
                recs=[read(j['out']) for j in jobs if j['seed']==int(seed)]
                assert len(recs)==3
                x=metrics(recs);st['evaluations'].append(dict(episodes=st['episodes'],checkpoint=st['checkpoint'],checkpoint_sha256=sha(st['checkpoint']),metrics=x))
                st['bad_streak']=deterioration_streak(st['bad_streak'],x,st['initial'])
                if st['bad_streak']>=2:st.update(stopped=True,stop_reason='R600 worse than initialization twice consecutively')
            write_json(ROOT/'progress.json',dict(stage='milestone',quarter=quarter,states=states))
            write_report(states,quarter)
            if quarter==1:write_json(ROOT/'FIRST_QUARTER_READY.json',dict(status='READY',report='MILESTONE_1.md',continue_without_confirmation=True))
            if all(s['stopped'] for s in states.values()):break
        verify_run();write_report(states,quarter,final=True)
        write_json(ROOT/'FINAL_AUDIT.json',dict(status='PASS',hashes={str(f):sha(f) for f in ROOT.rglob('*') if f.is_file() and f.suffix in ('.json','.md','.pt','.png','.gz')},tests_sealed=True))
        write_json(ROOT/'final_status.json',dict(status='COMPLETE',stop='await user instruction',states=states))
    except Exception:
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',traceback=traceback.format_exc(),no_retry=True))


def launch():
    verify_run()
    if (ROOT/'launch.json').exists():raise FileExistsError('No relaunch')
    with (ROOT/'panel.log').open('x') as f:
        proc=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'panel'],stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
    guard=subprocess.Popen(['caffeinate','-i','-w',str(proc.pid)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    record=dict(pid=proc.pid,caffeinate_pid=guard.pid,created_utc=datetime.now(timezone.utc).isoformat(),
                command=f'{sys.executable} {Path(__file__).resolve()} panel',manifest_sha256=sha(ROOT/'manifest.json'),workers=9)
    write_json(ROOT/'launch.json',record);print(json.dumps(record))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=['prepare','launch','panel'])
    globals()[parser.parse_args().mode]()
