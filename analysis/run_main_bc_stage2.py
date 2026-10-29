"""第二阶段：教师轨迹、一致性门控、三种子纯模仿、封存外验证。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
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
from analysis import main_bc_interface as interface
from analysis import run_main_rl_stage1 as stage1
from analysis.prepare_main_bc_stage2 import ROOT, STAGE1, MAIN, TRAIN, VALID, METHODS, SEEDS, read
from analysis.run_main_local_panel import verify_main
from analysis.run_load_pressure_short import verify
from analysis.redesign_scenario import write_json, digest
from analysis.estimate_load_pressure import sha
from analysis.local_information_baselines import LocalInformationPlane, LocalMinLoad
from analysis.sar_three_plan import CandidateEnv, ThreePlanChoice
from analysis.run_calibration_capacity import FifoDiagnostic
from analysis.run_sar_frozen import invariants, peak_rss_gb
from analysis.report_main_posthoc import score, latency_stats, DEADLINES
from sim.config import config_from_dict
from sim.control import ControlPlane
from sim.marl.decision import decision_state


def verify_manifest():
    if datetime.now(timezone.utc) >= datetime(2026,10,26,16,tzinfo=timezone.utc):
        raise TimeoutError('10-26 milestone time box expired; no extension')
    m = read(ROOT/'manifest.json')
    verify_main()
    for name in ('entry_hashes', 'episode_hashes'):
        for path, expected in m[name].items():
            assert sha(Path(path)) == expected, path
    return m


def episode_id(path):
    p = Path(path)
    return p.parent.parent.name+'_'+p.parent.name


def teacher_job(path):
    p = read(Path(path))
    assert p['date'] in TRAIN
    out = ROOT/'teachers'/episode_id(path)
    if (out/'teacher.json').exists() or (out/'teacher_FAILED.json').exists():
        raise FileExistsError('No teacher retry')
    verify_manifest()
    stage1.ROOT = out
    stage1.build_observation = interface.build_observation
    stage1.teacher_label = interface.teacher_label
    stage1.HierarchicalActor = interface.HierarchicalActor
    stage1.CommittedPolicy = interface.CommittedPolicy
    result = stage1.run_job('teacher', p)
    return dict(job=episode_id(path), **result)


def coverage(records, protocols):
    totals = dict(total_windows=0., represented_windows=0., outside_topk_windows=0., masked_windows=0.,
                  uncommitted_windows=0., unsupported_windows=0., ground_block_windows=0., uncaptured_windows=0.)
    full = captured = total_images = helper_total = helper_inside = 0
    for record, p in zip(records, protocols):
        wanted = set(p['scored_task_ids'])
        total_images += len(wanted)
        weights = {e['task_id']: e['compute_windows'] for e in p['events'] if e['scored']}
        for task in wanted:
            totals['total_windows'] += weights[task]
            if task not in record['labels']:
                totals['uncaptured_windows'] += weights[task]
                continue
            captured += 1
            row = record['labels'][task]
            obs, label = row['observation'], row['label']
            assert label['total_windows'] == weights[task]
            full += label['represented_windows'] == label['total_windows']
            for key in totals:
                if key not in ('total_windows','uncaptured_windows'):
                    totals[key] += label[key]
            block_weights = dict(zip(obs['tile_ids'], obs['window_weights']))
            for tid, target in row['actual_commitments'].items():
                if isinstance(target, int) and target != obs['source']:
                    helper_total += block_weights[tid]
                    helper_inside += block_weights[tid]*(target in obs['node_ids'][2:])
    assert math.isclose(sum(totals[k] for k in ('represented_windows','outside_topk_windows','masked_windows',
                         'uncommitted_windows','unsupported_windows','uncaptured_windows')), totals['total_windows'])
    return dict(images=total_images, captured_images=captured, fully_representable_images=full, **totals,
                window_coverage=totals['represented_windows']/totals['total_windows'],
                helper_windows=helper_total, helper_topk_windows=helper_inside,
                helper_topk_coverage=helper_inside/helper_total if helper_total else None)


def train_job(seed):
    started = time.perf_counter()
    try:
        m = verify_manifest()
        torch.set_num_threads(1)
        torch.manual_seed(seed)
        rng = random.Random(seed)
        records = []
        inputs = {}
        for path in m['train']:
            file = ROOT/'teachers'/episode_id(path)/'teacher.json'
            record, p = read(file), read(Path(path))
            assert record['status'] == 'PASS'
            inputs[str(file)] = sha(file)
            for task in p['scored_task_ids']:
                if task not in record['labels']:
                    continue
                row = record['labels'][task]
                if row['observation']['image_mask'][row['label']['mode']]:
                    records.append(row)
        assert records
        actor = interface.HierarchicalActor()
        hp = m['hyperparameters']
        optimizer = torch.optim.Adam(actor.parameters(), lr=hp['lr'], weight_decay=hp['weight_decay'])
        curve = []
        for epoch in range(hp['epochs']):
            rng.shuffle(records)
            losses = []
            actor.train()
            for i in range(0, len(records), hp['batch_size']):
                batch = records[i:i+hp['batch_size']]
                optimizer.zero_grad(set_to_none=True)
                loss = interface.imitation_loss(actor, batch)
                assert torch.isfinite(loss)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(actor.parameters(), hp['gradient_clip'], error_if_nonfinite=True)
                optimizer.step()
                losses.append((float(loss.detach()), len(batch)))
            curve.append(dict(epoch=epoch+1, train_joint_cross_entropy=sum(x*n for x,n in losses)/len(records)))
            write_json(ROOT/'training'/f'{seed}_progress.json', dict(epoch=epoch+1, total_epochs=hp['epochs'],
                       loss=curve[-1]['train_joint_cross_entropy'], wall_seconds=time.perf_counter()-started))
        actor.eval()
        mode_correct = 0
        mode_total = [0,0]
        mode_hits = [0,0]
        with torch.no_grad():
            for row in records:
                logits, _ = actor.logits(row['observation'])
                true = row['label']['mode']
                hit = int(logits.argmax()) == true
                mode_correct += hit
                mode_total[true] += 1
                mode_hits[true] += hit
        checkpoint = ROOT/'training'/f'pure_imitation_{seed}.pt'
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        if checkpoint.exists():
            raise FileExistsError('Do not overwrite pure imitation checkpoint')
        torch.save(dict(schema=interface.SCHEMA, seed=seed, state_dict=actor.state_dict(), training='pure_imitation_only',
                        hyperparameters=hp, teacher_hashes=inputs, train_dates=TRAIN, validation_used_for_training=False), checkpoint)
        verify_manifest()
        result = dict(status='PASS', seed=seed, samples=len(records), curve=curve, teacher_hashes=inputs,
                      train_mode_accuracy=mode_correct/len(records),
                      train_mode_recall=[mode_hits[i]/mode_total[i] if mode_total[i] else None for i in (0,1)],
                      checkpoint=str(checkpoint), checkpoint_sha256=sha(checkpoint), wall_seconds=time.perf_counter()-started)
        write_json(ROOT/'training'/f'{seed}.json', result)
        return dict(job=str(seed), status='PASS', wall_seconds=result['wall_seconds'])
    except Exception:
        result = dict(job=str(seed), status='FAILED', traceback=traceback.format_exc())
        write_json(ROOT/'training'/f'{seed}_FAILED.json', result)
        return result


def evaluation_job(args):
    path, method = args
    started, env = time.perf_counter(), None
    p = read(Path(path))
    out = ROOT/'validation'/p['date']
    try:
        assert p['date'] in VALID and method in METHODS
        if (out/f'{method}.json').exists() or (out/f'{method}_FAILED.json').exists():
            raise FileExistsError('No validation retry')
        verify_manifest(); verify(p)
        torch.set_num_threads(1)
        cfg = config_from_dict(p['config'])
        env = CandidateEnv(cfg)
        sources = set(cfg.task_source_sat_ids)
        local, actor, checkpoint_hash = {}, None, None
        if method.startswith(('zero_', 'history_', 'minload_', 'bc_')):
            class Plane(LocalInformationPlane):
                estimator = 'history' if method.startswith('history_') else 'zero'
                history_hops = 2 if method.startswith('bc_') else int(method[-1])
            plane = Plane(sources)
            if method.startswith('bc_'):
                seed = int(method.split('_')[-1])
                train = read(ROOT/'training'/f'{seed}.json')
                checkpoint = Path(train['checkpoint'])
                assert sha(checkpoint) == train['checkpoint_sha256']
                checkpoint_hash = sha(checkpoint)
                data = torch.load(checkpoint, map_location='cpu', weights_only=True)
                actor = interface.HierarchicalActor().eval()
                actor.load_state_dict(data['state_dict'])
                local = {s:interface.CommittedPolicy(actor, seed*1000+s) for s in sources}
            else:
                factory = LocalMinLoad if method.startswith('minload_') else ThreePlanChoice
                local = {s:factory(cfg.relay_max_hops) for s in sources}
        else:
            plane = ControlPlane(sources)
        env.control_plane = plane
        policy = FifoDiagnostic() if method == 'fifo' else ThreePlanChoice(cfg.relay_max_hops)
        action_hash = hashlib.sha256()
        for step in range(cfg.sim_steps):
            actions = []
            if step % cfg.decision_interval_steps == 0:
                state = env.export_state()
                plane.enqueue(state)
                state = decision_state(state, cfg.decision_tile_limit_per_sat)
                if local:
                    for source in sorted({t['source_sat_id'] for t in state.tiles.values()}):
                        actions.extend(local[source].select_actions(plane.view(state, source, plane.history_hops)))
                elif method == 'fifo':
                    actions = policy.select_actions(env, state)
                else:
                    actions = policy.select_actions(state)
                stage1.action_digest_update(action_hash, step, actions)
            env.step(actions)
            if env.time % round(60/cfg.dt) == 0:
                if datetime.now(timezone.utc) >= datetime(2026,10,26,16,tzinfo=timezone.utc):
                    raise TimeoutError('10-26 milestone time box expired')
                write_json(out/'progress'/f'{method}.json', dict(status='RUNNING', pid=os.getpid(), sim_seconds=env.time*cfg.dt,
                           total_sim_seconds=p['sim_duration_s'], wall_seconds=time.perf_counter()-started))
        checks = invariants(env)
        checks['foreign_compute'] = [(c['sat_id'], tid) for c in env.compute_cohort_records.values() for tid in c['tile_ids']
            if c['sat_id'] in sources and env.tasks[env.tiles[tid].parent_task_id].source_sat_id != c['sat_id']]
        incoming = [v-plane.sent_mb_by_edge[k] for k,v in env.metrics.network_sent_mb.items()
                    if k.startswith('isl:') and int(k.split('->')[-1]) in sources]
        checks['sar_incoming_data_mb'] = math.fsum(incoming)
        checks['illegal_input_paths'] = [t.tile_id for t in env.tiles.values()
            if t.input_route and (len(t.input_route)-1 > 2 or any(s in sources for s in t.input_route[1:]))]
        wanted = set(p['scored_task_ids'])
        outcomes = env.summary()['task_outcomes']
        scored = [o for o in outcomes if o['task_id'] in wanted]
        checks['followup600'] = len(scored) == len(wanted) and all(o['observed_age_seconds'] >= 600 for o in scored)
        checks['raw_unique_errors'] = [task for task in env._dist_pipeline.raw_tasks if not math.isclose(
            sum(env.tiles[t].data_size_mb for t in env.tasks[task].tile_ids), env.tasks[task].image_size_mb, abs_tol=1e-6)]
        if actor is not None:
            mapping = {}
            stage1.commitments(env, mapping)
            checks['commitment_errors'] = [tid for policy in local.values() for d in policy.decisions.values() if d['mode']==0
                for tid,target in d['targets'].items() if tid in mapping and mapping[tid] != target]
        checks['passed'] &= (not checks['foreign_compute'] and not checks['illegal_input_paths'] and checks['followup600']
                             and all(abs(v)<1e-6 for v in incoming) and not checks['raw_unique_errors'] and not checks.get('commitment_errors'))
        verify_manifest(); verify(p)
        stats = latency_stats(scored)
        r600 = math.fsum(min(o['latency_seconds'],600) if o['status']=='completed' else 600 for o in scored)/len(scored)
        record = dict(status='PASS' if checks['passed'] else 'AUDIT_MISMATCH', method=method, date=p['date'],
                      checks=checks, outcomes=outcomes, curve=[score(scored,d) for d in DEADLINES], latency=stats,
                      R600=r600, config_sha256=p['config_sha256'], checkpoint_sha256=checkpoint_hash,
                      action_trace_sha256=action_hash.hexdigest(), control=dict(plane.stats),
                      wall_seconds=time.perf_counter()-started, peak_rss_gb=peak_rss_gb())
        write_json(out/f'{method}.json', record)
        result = dict(job=p['date']+'_'+method, status=record['status'], wall_seconds=record['wall_seconds'])
    except Exception:
        result = dict(job=p['date']+'_'+method, status='FAILED', traceback=traceback.format_exc())
        write_json(out/f'{method}_FAILED.json', dict(**result, config=p['config']))
    finally:
        if env is not None:
            env.close()
    write_json(out/'progress'/f'{method}.json', result)
    return result


def pool_stage(name, function, jobs, workers):
    results = []
    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context('spawn')) as pool:
        pending = {pool.submit(function, job):str(job) for job in jobs}
        for future in as_completed(pending):
            try:
                result = future.result()
            except Exception:
                result = dict(job=pending[future], status='FAILED', traceback=traceback.format_exc())
            results.append(result)
            write_json(ROOT/f'{name}_progress.json', dict(stage=name, done=len(results), total=len(jobs), results=results))
    if any(r['status'] != 'PASS' for r in results):
        raise RuntimeError(f'{name} has failed/audit-mismatched jobs; no retry or next stage')
    return results


def report(m):
    verify_manifest()
    daily, aggregate, outcome_sets = {}, {}, {}
    for path in m['validation']:
        p = read(Path(path)); verify(p)
        for method in METHODS:
            r = read(ROOT/'validation'/p['date']/f'{method}.json')
            assert r['status']=='PASS' and r['checks']['passed'] and r['config_sha256']==p['config_sha256']
            outcomes = [o for o in r['outcomes'] if o['task_id'] in p['scored_task_ids']]
            outcome_sets.setdefault(method, []).append(outcomes)
            assert {o['task_id'] for o in outcomes} == set(p['scored_task_ids'])
            assert all(o['observed_age_seconds']>=600 for o in outcomes)
            assert [score(outcomes,d) for d in DEADLINES] == r['curve']
            assert latency_stats(outcomes) == r['latency']
            r600 = math.fsum(min(o['latency_seconds'],600) if o['status']=='completed' else 600 for o in outcomes)/len(outcomes)
            assert abs(r600-r['R600'])<1e-9
            daily.setdefault(method, []).append(dict(date=p['date'], P=[x['P_lower'] for x in r['curve']],
                **r['latency'], R600=r600, completion_fraction=r['latency']['completed']/len(outcomes)))
    for method, rows in daily.items():
        aggregate[method] = dict(P=np.mean([r['P'] for r in rows],axis=0).tolist(),
            **{k:float(np.mean([r[k] for r in rows])) if all(r[k] is not None for r in rows) else None
               for k in ('mean_completed_s','P50_completed_s','P90_completed_s','R600','completion_fraction')},
            unfinished_total=sum(r['unfinished'] for r in rows))
    bc = [aggregate[f'bc_{s}'] for s in SEEDS]
    dispersion = {k:dict(mean=np.mean([r[k] for r in bc],axis=0).tolist(), std=np.std([r[k] for r in bc],axis=0,ddof=1).tolist(),
                         min=np.min([r[k] for r in bc],axis=0).tolist(), max=np.max([r[k] for r in bc],axis=0).tolist())
                  for k in ('P','mean_completed_s','R600') if all(r[k] is not None for r in bc)}
    locals_ = [aggregate[k] for k in METHODS if k.startswith(('zero','history','minload'))]
    success = {str(s):dict(P60=aggregate[f'bc_{s}']['P'][0]>max(r['P'][0] for r in locals_),
                          mean_latency=aggregate[f'bc_{s}']['mean_completed_s'] is not None and
                          all(r['mean_completed_s'] is not None for r in locals_) and
                          aggregate[f'bc_{s}']['mean_completed_s']<min(r['mean_completed_s'] for r in locals_),
                          completion=aggregate[f'bc_{s}']['completion_fraction']>=max(r['completion_fraction'] for r in locals_),
                          R600=aggregate[f'bc_{s}']['R600']<min(r['R600'] for r in locals_)) for s in SEEDS}
    summary = dict(status='PASS', deadlines=DEADLINES, daily=daily, day_equal=aggregate, seed_dispersion=dispersion,
                   success_criteria=success, coverage=read(ROOT/'TEACHER_COVERAGE.json'), MAPPO_started=False, tests_sealed=True)
    write_json(ROOT/'summary.json',summary)
    lines = ['# 第二阶段：纯模仿验证', '', '三验证日等权；每条曲线均用全部评分图为分母，600秒完整随访。FIFO仅诊断，全局v3是特权参考。', '',
             '| 方法 | P60/90/120/180/240/300/600 (%) | 已交付均值/P50/P90 (s) | 未完成总数 | R600 (s) |',
             '|---|---|---|---:|---:|']
    for method,r in aggregate.items():
        latency='/'.join('NA' if r[k] is None else f'{r[k]:.2f}' for k in ('mean_completed_s','P50_completed_s','P90_completed_s'))
        lines.append(f'| {method} | '+ '/'.join(f'{x*100:.2f}' for x in r['P'])+f' | {latency} | {r["unfinished_total"]} | {r["R600"]:.2f} |')
    lines += ['', '均值及分位数先逐日计算再日等权；未完成总数单列，条件均值不替代完成率/R600。逐日表与三种子均值、样本标准差、范围见summary.json。',
              '训练只用10-16至21的12个高峰小时，固定100epoch，最后epoch独立保存pure_imitation checkpoint；未按验证表现调参或挑epoch。',
              f'成功标准逐项：{json.dumps(success,ensure_ascii=False)}。四项全部满足才称该种子通过；否则如实视为未达到。',
              '分块地面与原图地面两条执行路径区分；动态粒度为后续工作。通告未单独计传播时延，量级见BC_STAGE2_PROTOCOL_20261022.md。',
              '本阶段结束，等待用户确认。未启动MAPPO、未启封测试、未改论文或提交。']
    (ROOT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    plot_results(m, outcome_sets)
    files = [ROOT/'summary.json',ROOT/'REPORT.md',ROOT/'validation_curves.png',ROOT/'validation_full_curves.json',ROOT/'training_curves.png',ROOT/'TEACHER_COVERAGE.json',ROOT/'ACTION_GATE.json']
    files += list((ROOT/'training').glob('*.pt'))+list((ROOT/'validation').glob('*/*.json'))
    write_json(ROOT/'FINAL_AUDIT.json',dict(status='PASS',hashes={str(f):sha(f) for f in files}, no_test_access=True))


def plot_results(m,outcome_sets):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,ax=plt.subplots(figsize=(10,5),layout='constrained')
    full_curves = {}
    for method in METHODS:
        xs = sorted({60.,600.,*[o['latency_seconds'] for rows in outcome_sets[method] for o in rows
                                if o['status']=='completed' and 60 <= o['latency_seconds'] <= 600]})
        ys = [float(np.mean([score(rows,d)['P_lower'] for rows in outcome_sets[method]])) for d in xs]
        full_curves[method] = dict(D=xs,P_day_equal=ys)
        ax.step(xs,100*np.asarray(ys),where='post',label=method)
    write_json(ROOT/'validation_full_curves.json',full_curves)
    ax.set(xlabel='Deadline (s)',ylabel='Day-equal P(D), %',xticks=DEADLINES,ylim=(0,102))
    ax.grid(alpha=.2);ax.legend(ncol=3,fontsize=8)
    fig.savefig(ROOT/'validation_curves.png',dpi=160);plt.close(fig)
    fig,ax=plt.subplots(figsize=(8,4),layout='constrained')
    for seed in SEEDS:
        rows=read(ROOT/'training'/f'{seed}.json')['curve']
        ax.plot([r['epoch'] for r in rows],[r['train_joint_cross_entropy'] for r in rows],label=str(seed))
    ax.set(xlabel='Epoch',ylabel='Training joint cross entropy');ax.legend();ax.grid(alpha=.2)
    fig.savefig(ROOT/'training_curves.png',dpi=160);plt.close(fig)


def panel():
    try:
        m=verify_manifest()
        first=m['train'][0]
        result=teacher_job(first)
        assert result['status']=='PASS',result
        reference=read(STAGE1/'reference.json')
        teacher=read(ROOT/'teachers'/episode_id(first)/'teacher.json')
        equal={k:reference[k]==teacher[k] for k in ('action_trace_sha256','dynamics_sha256','outcomes')}
        gate=dict(status='PASS' if all(equal.values()) else 'BLOCKED',trace_equal=equal,
                  coverage=coverage([teacher],[read(Path(first))]))
        write_json(ROOT/'ACTION_GATE.json',gate)
        assert gate['status']=='PASS','Reference/teacher mismatch'
        pool_stage('teacher',teacher_job,m['train'][1:],m['workers'])
        cov=coverage([read(ROOT/'teachers'/episode_id(p)/'teacher.json') for p in m['train']],
                     [read(Path(p)) for p in m['train']])
        write_json(ROOT/'TEACHER_COVERAGE.json',cov)
        assert cov['captured_images']==cov['images'],'Uncaptured teacher decisions; stop for audit'
        pool_stage('training',train_job,SEEDS,3)
        pool_stage('validation',evaluation_job,[(p,method) for p in m['validation'] for method in METHODS],m['workers'])
        report(m)
        write_json(ROOT/'final_status.json',dict(status='COMPLETE',stop='Stage2 milestone; await confirmation; no MAPPO'))
    except Exception:
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',traceback=traceback.format_exc(),no_retry=True))


def launch():
    verify_manifest()
    if (ROOT/'launch.json').exists():
        raise FileExistsError('No automatic relaunch')
    with (ROOT/'panel.log').open('xb') as log:
        env=dict(os.environ,OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1')
        proc=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'panel'],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    guard=subprocess.Popen(['caffeinate','-i','-w',str(proc.pid)],start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    write_json(ROOT/'launch.json',dict(pid=proc.pid,caffeinate_pid=guard.pid,created_utc=datetime.now(timezone.utc).isoformat(),
               command=f'{sys.executable} {Path(__file__).resolve()} panel',manifest_sha256=sha(ROOT/'manifest.json')))
    print(json.dumps(read(ROOT/'launch.json')))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=['panel','launch']);args=parser.parse_args()
    globals()[args.mode]()
