"""三项第一阶段审计；结束后只报告，绝不自动进入梯度训练。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import torch

from analysis.main_rl_interface import (HierarchicalActor, CentralCritic, CommittedPolicy, build_observation,
    critic_observation, task_groups, complete_uncommitted, teacher_label, terminal_reward)
from analysis.local_information_baselines import LocalInformationPlane
from analysis.sar_three_plan import CandidateEnv, ThreePlanChoice
from analysis.prepare_main_rl_stage1 import ROOT, read
from analysis.redesign_scenario import digest, write_json
from analysis.run_main_local_panel import verify_main
from analysis.run_load_pressure_short import verify
from analysis.run_sar_frozen import invariants, peak_rss_gb
from sim.control import ControlPlane
from sim.config import config_from_dict
from sim.marl.decision import decision_state
from sim.entities import TileState


class Plane(LocalInformationPlane):
    history_hops = 2


def action_digest_update(hasher, step, actions):
    rows = []
    for a in actions:
        d = asdict(a)
        d['action_type'] = a.action_type.name
        rows.append(d)
    hasher.update(json.dumps([step, rows], sort_keys=True, separators=(',', ':')).encode())


def commitments(env, mapping):
    for c in env.compute_cohort_records.values():
        for tid in c['tile_ids']:
            if tid in mapping:
                assert mapping[tid] == c['sat_id'], ('compute commitment changed', tid)
            mapping[tid] = c['sat_id']
    transfers = {tr.tile_id: tr.dst for tr in env.transfers if not tr.is_result}
    ground_transfers = {tr.tile_id for tr in env.ground_tile_transfers}
    for tid, tile in env.tiles.items():
        if tid in mapping:
            continue
        if tile.parent_task_id in env._dist_pipeline.raw_tasks:
            mapping[tid] = 'raw_ground'
        elif tile.input_target_sat_id is not None:
            mapping[tid] = tile.input_target_sat_id
        elif tile.ground_relay_target is not None or tid in ground_transfers:
            mapping[tid] = 'ground_block'
        elif tid in transfers:
            mapping[tid] = transfers[tid]
        elif tile.state in {TileState.READY, TileState.RUNNING}:
            mapping[tid] = tile.location


def run_job(method, p):
    started = time.perf_counter()
    env = None
    try:
        verify(p); verify_main()
        torch.set_num_threads(1)
        torch.manual_seed(p['seed'])
        cfg = config_from_dict(p['config'])
        env = CandidateEnv(cfg)
        sources = set(cfg.task_source_sat_ids)
        env.control_plane = plane = ControlPlane(sources) if method == 'reference' else Plane(sources)
        teacher = ThreePlanChoice(cfg.relay_max_hops)
        actor, critic = HierarchicalActor().eval(), CentralCritic().eval()
        before_weights = {k: v.detach().clone() for k, v in actor.state_dict().items()}
        policies = {sid: CommittedPolicy(actor, p['seed']*1000+sid) for sid in sources}
        captures, values, mapping = {}, {}, {}
        action_hash = hashlib.sha256()
        inference_s = teacher_s = observation_s = 0.
        initialization_s = time.perf_counter()-started
        for step in range(cfg.sim_steps):
            actions = []
            pending_captures = {}
            if step % cfg.decision_interval_steps == 0:
                full = env.export_state()
                plane.enqueue(full)
                state = decision_state(full, cfg.decision_tile_limit_per_sat)
                if method == 'reference':
                    t = time.perf_counter()
                    actions = teacher.select_actions(state)
                    teacher_s += time.perf_counter()-t
                else:
                    active = sorted({t['source_sat_id'] for t in state.tiles.values()})
                    for sid in active:
                        t = time.perf_counter()
                        view = plane.view(state, sid, 2)
                        if method == 'teacher':
                            for task, tiles in task_groups(view):
                                if task not in captures and complete_uncommitted(view, task, tiles):
                                    pending_captures[task] = build_observation(view, task, tiles)
                        observation_s += time.perf_counter()-t
                        if method == 'student':
                            known = set(policies[sid].decisions)
                            t = time.perf_counter()
                            actions.extend(policies[sid].select_actions(view))
                            inference_s += time.perf_counter()-t
                            for task in set(policies[sid].decisions)-known:
                                # 全局critic单独计时/记录，绝不进入actor的参数或输入。
                                c = critic_observation(full)
                                t = time.perf_counter()
                                with torch.no_grad():
                                    val = critic(torch.tensor(c['nodes'], dtype=torch.float32), torch.tensor(c['ground'], dtype=torch.float32))
                                inference_s += time.perf_counter()-t
                                values[task] = {'value': float(val), 'critic_observation': c}
                    if method == 'teacher':
                        t = time.perf_counter()
                        actions = teacher.select_actions(state)
                        teacher_s += time.perf_counter()-t
                action_digest_update(action_hash, step, actions)
            env.step(actions)
            if method != 'reference' and (step % cfg.decision_interval_steps == 0 or step == cfg.sim_steps-1):
                commitments(env, mapping)
                for task, obs in pending_captures.items():
                    if any(tid in mapping for tid in env.tasks[task].tile_ids):
                        captures[task] = obs
            if env.time % round(60/cfg.dt) == 0:
                write_json(ROOT/'progress'/f'{method}.json', {'status': 'RUNNING', 'pid': os.getpid(),
                    'sim_seconds': env.time*cfg.dt, 'total_sim_seconds': p['sim_duration_s'],
                    'wall_seconds': time.perf_counter()-started, 'updated_utc': datetime.now(timezone.utc).isoformat()})
        checks = invariants(env)
        checks['foreign_compute'] = [(c['sat_id'], tid) for c in env.compute_cohort_records.values() for tid in c['tile_ids']
            if c['sat_id'] in sources and env.tasks[env.tiles[tid].parent_task_id].source_sat_id != c['sat_id']]
        checks['illegal_input_paths'] = [tid for tid, tile in env.tiles.items()
            if tile.input_route and (len(tile.input_route)-1 > 2 or any(sid in sources for sid in tile.input_route[1:]))]
        checks['sar_data_ingress'] = [v-plane.sent_mb_by_edge[k] for k, v in env.metrics.network_sent_mb.items()
            if k.startswith('isl:') and int(k.split('->')[-1]) in sources]
        checks['weights_unchanged'] = all(torch.equal(before_weights[k], v) for k, v in actor.state_dict().items())
        outcomes = env.summary()['task_outcomes']
        wanted = set(p['scored_task_ids'])
        scored = [o for o in outcomes if o['task_id'] in wanted]
        checks['followup'] = len(scored) == len(wanted) and all(o['observed_age_seconds'] >= 600 for o in scored)
        checks['passed'] = bool(checks['passed'] and not checks['foreign_compute'] and not checks['illegal_input_paths']
            and all(abs(x)<1e-6 for x in checks['sar_data_ingress']) and checks['weights_unchanged'] and checks['followup'])
        labels = {}
        for task, obs in captures.items():
            weights = {tid: env.tiles[tid].compute_cost for tid in env.tasks[task].tile_ids}
            labels[task] = {'observation': obs, 'label': teacher_label(obs, task in env._dist_pipeline.raw_tasks, mapping, weights),
                            'actual_commitments': {tid: mapping.get(tid) for tid in weights}, 'scored': task in wanted}
        choices = {task: d for pol in policies.values() for task, d in pol.decisions.items()} if method == 'student' else {}
        if method == 'student':
            checks['student_commitment_errors'] = [tid for task, d in choices.items() if d['mode'] == 0
                for tid, target in d['targets'].items() if tid in mapping and mapping[tid] != target]
            checks['passed'] &= not checks['student_commitment_errors']
            torch.save({'schema': 'untrained-throughput-probe-only', 'seed': p['seed'], 'state_dict': actor.state_dict()}, ROOT/'probe_actor.pt')
        dynamics = {'outcomes': outcomes, 'network_sent_mb': dict(env.metrics.network_sent_mb),
                    'compute_cohorts': env.compute_cohort_records,
                    'tiles': {tid: {'route': list(tile.input_route), 'target': tile.input_target_sat_id,
                                   'windows': tile.completed_compute_windows, 'state': tile.state.name}
                              for tid, tile in sorted(env.tiles.items())}}
        rewards = {o['task_id']: terminal_reward(o) for o in scored}
        verify(p); verify_main()
        record = {'status': 'PASS' if checks['passed'] else 'AUDIT_MISMATCH', 'method': method,
                  'wall_seconds': time.perf_counter()-started, 'initialization_seconds': initialization_s,
                  'inference_and_dispatch_seconds': inference_s, 'observation_seconds': observation_s,
                  'teacher_seconds': teacher_s, 'peak_rss_gb': peak_rss_gb(),
                  'config_sha256': p['config_sha256'], 'action_trace_sha256': action_hash.hexdigest(),
                  'dynamics_sha256': digest(dynamics), 'outcomes': outcomes, 'checks': checks,
                  'labels': labels, 'decisions': choices, 'critic_values': values, 'rewards': rewards,
                  'compute_commitments': mapping, 'gradient_updates': 0}
        write_json(ROOT/f'{method}.json', record)
        status = {k: record[k] for k in ('method', 'status', 'wall_seconds')}
    except Exception:
        status = {'method': method, 'status': 'FAILED', 'traceback': traceback.format_exc()}
        write_json(ROOT/f'{method}_FAILED.json', {**status, 'config': p['config']})
    finally:
        if env is not None:
            env.close()
    write_json(ROOT/'progress'/f'{method}.json', status)
    return status


def collect(p):
    verify(p); verify_main()
    statuses = [read(ROOT/'progress'/f'{m}.json') for m in p['methods']]
    if any(s['status'] != 'PASS' for s in statuses):
        write_json(ROOT/'final_status.json', {'status': 'BLOCKED', 'statuses': statuses, 'no_retry': True})
        return
    rows = {m: read(ROOT/f'{m}.json') for m in p['methods']}
    ref, teacher, student = (rows[m] for m in p['methods'])
    identical = all(ref[k] == teacher[k] for k in ('action_trace_sha256', 'dynamics_sha256', 'outcomes'))
    if not identical:
        write_json(ROOT/'final_status.json', {'status': 'BLOCKED', 'reason': 'TEACHER_INSTRUMENTATION_MISMATCH',
            'reference_hashes': {k: ref[k] for k in ('action_trace_sha256', 'dynamics_sha256')},
            'teacher_hashes': {k: teacher[k] for k in ('action_trace_sha256', 'dynamics_sha256')}})
        return
    labels = [r['label'] for r in teacher['labels'].values() if r['scored']]
    fields = ('total_windows', 'represented_windows', 'outside_topk_windows', 'masked_windows',
              'uncommitted_windows', 'ground_block_windows')
    sums = {k: math.fsum(r[k] for r in labels) for k in fields}
    total = sums['total_windows']
    coverage = {**sums, 'window_coverage': sums['represented_windows']/total if total else None,
                'captured_images': len(labels), 'scored_images': len(p['scored_task_ids']),
                'fully_representable_images': sum(r['represented_windows'] == r['total_windows'] for r in labels),
                'mode_counts': {str(i): sum(r['mode'] == i for r in labels) for i in (0, 1)}}
    decision_count = len(set(student['decisions']) & set(p['scored_task_ids']))
    throughput = {'single_episode_wall_seconds': student['wall_seconds'], 'scored_images': len(p['scored_task_ids']),
                  'scored_decisions': decision_count, 'nine_worker_efficiency_assumption': [.6, 1.],
                  'episodes_per_hour': [9*eff*3600/student['wall_seconds'] for eff in (.6, 1.)],
                  'sample_hours_10_27_to_11_02': 84,
                  'seed_decision_budget': {str(seed): [int(84*9*eff*3600/student['wall_seconds']*decision_count/3)
                      for eff in (.6, 1.)] for seed in (1701, 1702, 1703)},
                  'limitation': 'Untrained actor, three concurrent jobs; nine-worker efficiency and trained-policy congestion are unmeasured. No gradient update timing included.'}
    summary = {'status': 'PASS', 'executor_trace_identical': identical, 'coverage': coverage, 'throughput': throughput,
               'runs': statuses, 'test_dates_accessed': False, 'training_started': False,
               'needs_confirmation': True, 'dynamic_granularity': 'future work; existing fallback block counts retained'}
    write_json(ROOT/'summary.json', summary)
    lines = ['# 第一阶段：两层动作与教师映射审计', '',
        '仅10-16训练日最忙一小时；原始字节选窗，真实预热任务、600秒随访。reference/teacher/student三项，无梯度更新。', '',
        f"原始v3与记录增强v3动作序列、逐图结果、网络字节、计算分组及承诺轨迹：{'完全一致' if identical else '不一致'}。",
        f"主评分图{len(p['scored_task_ids'])}；教师捕获{len(labels)}；完整可表示{coverage['fully_representable_images']}；按窗口可表示率{coverage['window_coverage']:.4%}。",
        f"窗口统计：{json.dumps(sums, ensure_ascii=False)}。候选外、掩码外、未承诺和分块地面处理分别记录，禁止混入Top-K外掩盖。",
        f"含网络/critic推理的单片段墙钟{student['wall_seconds']/60:.2f}分钟；实际整图决策{decision_count}。",
        f"9worker每小时片段预算{throughput['episodes_per_hour']}；84小时、三种子均分决策预算{throughput['seed_decision_budget']}。这是实测单次与并行效率假设的组合，非9worker训练吞吐实测。",
        '观测/动作schema及接口测试见源文件和unit_test.log。固定实际分块数：16块图求16项logp，现有小图4块仅求4项；动态粒度为后续工作。',
        '软标签是窗口比例；独立同分布采样只能覆盖多星分配支持集，不保证逐块复现教师，也不保证每图各节点窗口量等于标签期望。',
        'probe_actor.pt为未训练吞吐探针，不是纯模仿checkpoint。纯模仿和MAPPO尚未启动；需用户确认后进入下一阶段。']
    (ROOT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    audit_paths = [ROOT/f'{m}.json' for m in p['methods']] + [ROOT/'protocol.json', ROOT/'summary.json', ROOT/'REPORT.md']
    write_json(ROOT/'FINAL_AUDIT.json', {'status': 'PASS', 'source_count': len(p['frozen_protocol']['source_hashes']),
        'hashes': {str(f): hashlib.sha256(f.read_bytes()).hexdigest() for f in audit_paths},
        'teacher_trace_identical': True, 'config_unchanged': True, 'gradient_updates': 0})
    write_json(ROOT/'final_status.json', {'status': 'COMPLETE', 'stop': 'Await stage1 confirmation; no BC/MAPPO launch'})


def panel():
    p = read(ROOT/'protocol.json')
    started = time.perf_counter()
    try:
        verify(p); verify_main()
        statuses = []
        with ProcessPoolExecutor(max_workers=3, mp_context=multiprocessing.get_context('spawn')) as pool:
            jobs = {pool.submit(run_job, m, p): m for m in p['methods']}
            for future in as_completed(jobs):
                m = jobs[future]
                try:
                    status = future.result()
                except Exception:
                    status = {'method': m, 'status': 'FAILED', 'traceback': traceback.format_exc()}
                    write_json(ROOT/f'{m}_FAILED.json', {**status, 'config': p['config']})
                    write_json(ROOT/'progress'/f'{m}.json', status)
                statuses.append(status)
                write_json(ROOT/'progress.json', {'done': len(statuses), 'total': 3, 'statuses': statuses,
                    'wall_seconds': time.perf_counter()-started, 'pid': os.getpid()})
        collect(p)
    except Exception:
        write_json(ROOT/'final_status.json', {'status': 'BLOCKED', 'traceback': traceback.format_exc()})


def launch():
    p = read(ROOT/'protocol.json')
    verify(p); verify_main()
    if (ROOT/'launch.json').exists():
        raise FileExistsError('No automatic relaunch')
    with (ROOT/'panel.log').open('xb') as log:
        env = dict(os.environ, OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1', VECLIB_MAXIMUM_THREADS='1')
        proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'panel'], env=env,
                                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    guard = subprocess.Popen(['caffeinate', '-i', '-w', str(proc.pid)], stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
    data = {'pid': proc.pid, 'caffeinate_pid': guard.pid, 'workers': 3,
            'command': f'{sys.executable} {Path(__file__).resolve()} panel',
            'created_utc': datetime.now(timezone.utc).isoformat(),
            'protocol_sha256': hashlib.sha256((ROOT/'protocol.json').read_bytes()).hexdigest()}
    write_json(ROOT/'launch.json', data)
    print(json.dumps(data))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['panel', 'launch'])
    args = parser.parse_args()
    globals()[args.mode]()
