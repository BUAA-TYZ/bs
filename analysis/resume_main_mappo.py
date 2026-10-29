"""主场景续训与故障恢复；复用冻结的采样器和 PPO 更新。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import torch
from analysis import run_main_mappo as base

ROOT = base.ROOT
RESUME = ROOT / 'resume_20261025'
DEADLINE = datetime(2026, 10, 29, 16, tzinfo=timezone.utc)
ANCHOR = 296
MAX_RECOVERIES = 3
ORIGINAL_VERIFY = base.verify_run


class AuditFailure(Exception):
    pass


def now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, ensure_ascii=False, indent=2, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    sync_directory(path.parent)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_torch(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('wb') as f:
        torch.save(value, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    sync_directory(path.parent)


def signature(data):
    h = hashlib.sha256()
    for group in ('state_dict', 'critic'):
        for key, tensor in sorted(data[group].items()):
            x = tensor.detach().cpu().contiguous()
            h.update(f'{group}:{key}:{x.dtype}:{tuple(x.shape)}'.encode())
            h.update(x.numpy().tobytes())
    return h.hexdigest()


def verify_resume():
    try:
        m = ORIGINAL_VERIFY()
        config = base.read(RESUME / 'manifest.json')
        for path, expected in config['entry_hashes'].items():
            assert base.sha(path) == expected, path
        assert base.sha(ROOT / 'manifest.json') == config['original_manifest_sha256']
        return m
    except Exception as exc:
        raise AuditFailure(str(exc)) from exc


def load_record(path):
    if str(path).endswith('.gz'):
        with gzip.open(path, 'rt') as f:
            return json.load(f)
    return base.read(path)


def reusable(job):
    out = Path(job['out'])
    status_path = out.with_suffix('.progress.json')
    if not status_path.exists():
        return False
    try:
        status = base.read(status_path)
        if status.get('status') == 'AUDIT_MISMATCH':
            raise AuditFailure(f'Prior audit mismatch: {out}')
        if status.get('status') != 'PASS':
            return False
        if not out.exists():
            raise AuditFailure(f'Missing PASS output: {out}')
        record = load_record(out)
        if status['status'] != 'PASS' or record['status'] != 'PASS':
            raise AuditFailure(f'Non-PASS existing output: {out}')
        if status['sha256'] != base.sha(out):
            raise AuditFailure(f'Output hash mismatch: {out}')
        keys = ('kind', 'method', 'seed', 'episode', 'protocol', 'policy_signature')
        if any(record['job'].get(k) != job.get(k) for k in keys):
            raise AuditFailure(f'On-policy provenance mismatch: {out}')
        return True
    except (ValueError, KeyError, OSError, EOFError) as exc:
        raise AuditFailure(f'Invalid completed output: {out}: {exc}') from exc


def archive_incomplete(job):
    out = Path(job['out'])
    paths = [out, out.with_suffix('.progress.json'), out.with_suffix('.FAILED.json')]
    existing = [p for p in paths if p.exists()]
    if not existing:
        return
    failure = out.with_suffix('.FAILED.json')
    if failure.exists():
        detail = base.read(failure)
        if any(x in detail.get('traceback', '') for x in ('AssertionError', 'AuditFailure')):
            raise AuditFailure(f'Prior audit failure: {failure}')
    destination = RESUME / 'interrupted' / str(time.time_ns())
    for path in existing:
        target = destination / path.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(path, target)


def worker(job):
    base.verify_run = verify_resume
    return base.rollout(job)


def run_jobs(jobs, on_complete=None):
    results = []
    pending = []
    for job in jobs:
        if reusable(job):
            results.append(base.read(Path(job['out']).with_suffix('.progress.json')))
            if on_complete:
                on_complete(job)
        else:
            archive_incomplete(job)
            pending.append(job)
    with ProcessPoolExecutor(max_workers=9, mp_context=multiprocessing.get_context('spawn')) as pool:
        futures = {pool.submit(worker, job): job for job in pending}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            atomic_json(ROOT / 'batch_progress.json', dict(done=len(results), total=len(jobs), results=results))
            if result['status'] == 'PASS' and on_complete:
                on_complete(futures[future])
    failures = [r for r in results if r['status'] != 'PASS']
    if failures:
        if any(r['status'] == 'AUDIT_MISMATCH' or any(s in r.get('traceback', '') for s in
               ('AssertionError', 'AuditFailure')) for r in failures):
            raise AuditFailure(str(failures))
        raise RuntimeError(str(failures))


def checkpoint_due(completed, previous):
    return (completed - ANCHOR) // 25 > (previous - ANCHOR) // 25


def checkpoint_payload(actor, critic, optimizer, seed, episodes, parent_hash):
    return dict(state_dict=actor.state_dict(), critic=critic.state_dict(), optimizer=optimizer.state_dict(),
                seed=int(seed), episodes=episodes, parent_sha256=parent_hash,
                initial_sha256=base.sha(base.BC / 'training' / f'pure_imitation_{seed}.pt'),
                train_dates=base.TRAIN, validation_used_for_gradient=False, hyperparameters=base.learning.HP,
                rng_note='Sampling: (seed + 100000 * episode) * 1000 + source; PPO shuffle: seed * 10000 + batch_last_episode.')


def prune():
    recovery = base.read(RESUME / 'recovery.json')
    ledger_path = RESUME / 'checkpoint_ledger.json'
    ledger = base.read(ledger_path) if ledger_path.exists() else {}
    remove = []
    for seed, state in recovery['states'].items():
        protected = {state['checkpoint']} | {e['checkpoint'] for e in state['evaluations']}
        paths = sorted((ROOT / 'checkpoints' / seed).glob('*.pt'), key=lambda p: p.stat().st_mtime_ns)
        keep = {str(p) for p in paths[-3:]} | protected
        for path in paths:
            if str(path) not in keep:
                ledger[str(path)] = dict(sha256=base.sha(path), removed_utc=now(), reason='latest three plus validation checkpoints')
                remove.append(path)
    atomic_json(ledger_path, ledger)
    for path in remove:
        path.unlink(missing_ok=True)


def persist_seed(seed, states, models, completed, pending=None):
    state = states[seed]
    actor, critic, optimizer = models[seed]
    previous = base.read(RESUME / 'recovery.json')
    parent = previous['states'][seed]
    path = ROOT / 'checkpoints' / seed / f"resume_ep{state['episodes']:04d}_samples{completed:04d}_{time.time_ns()}.pt"
    data = checkpoint_payload(actor, critic, optimizer, seed, state['episodes'], parent['checkpoint_sha256'])
    data.update(collected_through_save=completed, pending_batch=pending or [])
    atomic_torch(path, data)
    saved = deepcopy(state)
    saved.update(checkpoint=str(path), checkpoint_sha256=base.sha(path), collected_through_save=completed,
                 pending_batch=pending or [])
    previous['states'][seed] = saved
    previous['updated_utc'] = now()
    atomic_json(RESUME / 'recovery.json', previous)
    state.update(checkpoint=str(path), checkpoint_sha256=saved['checkpoint_sha256'])
    prune()
    return path


def write_policy(seed, state, models):
    actor, critic, _ = models[seed]
    data = dict(state_dict=actor.state_dict(), critic=critic.state_dict())
    sig = signature(data)
    path = RESUME / 'transport' / seed / f"episode_{state['episodes']:04d}.pt"
    # 仅用于向 spawn worker 传模型；不含优化器，不作为恢复检查点。
    atomic_torch(path, data)
    return str(path), base.sha(path), sig


def restore():
    recovery = base.read(RESUME / 'recovery.json')
    states = deepcopy(recovery['states'])
    models = {}
    for seed, state in states.items():
        if base.sha(state['checkpoint']) != state['checkpoint_sha256']:
            raise AuditFailure(f'Checkpoint hash: {seed}')
        data = torch.load(state['checkpoint'], map_location='cpu', weights_only=True)
        if data['episodes'] != state['episodes'] or data['hyperparameters'] != base.learning.HP:
            raise AuditFailure(f'Checkpoint metadata: {seed}')
        torch.manual_seed(int(seed) * 10000 + state['episodes'])
        actor = base.interface.HierarchicalActor()
        critic = base.learning.Critic()
        actor.load_state_dict(data['state_dict'])
        critic.load_state_dict(data['critic'])
        optimizer = torch.optim.Adam(list(actor.parameters()) + list(critic.parameters()), lr=base.learning.HP['lr'])
        optimizer.load_state_dict(data['optimizer'])
        if any(group['lr'] != base.learning.HP['lr'] for group in optimizer.param_groups):
            raise AuditFailure('Unexpected optimizer learning rate')
        models[seed] = actor, critic, optimizer
    return states, models


def finalize(reason):
    verify_resume()
    states = base.read(RESUME / 'recovery.json')['states']
    base.write_report(states, max(len(s['evaluations']) - 1 for s in states.values()), final=True)
    budget = {seed: dict(updated_episodes=s['episodes'], updated_fraction=s['episodes'] / 703,
                         collected_pass=sum(base.read(p).get('status') == 'PASS' for p in
                                            (ROOT / 'rollouts' / seed).glob('*.progress.json')))
              for seed, s in states.items()}
    with (ROOT / 'REPORT.md').open('a') as f:
        f.write('\n续训说明：原 checkpoint 未保存全局 RNG；沿用片段编号派生采样与 PPO 洗牌种子。\n')
        f.write('保持 CPU、9 workers、每种子每3片段一次更新；每25个已完成采样保存模型/优化器及待更新batch，验证点另存。\n')
        f.write(f'停止原因：{reason}。实际更新预算与已完成采样数：{budget}。\n')
    atomic_json(RESUME / 'final_budget.json', dict(reason=reason, budget=budget, cutoff=DEADLINE.isoformat()))
    atomic_json(ROOT / 'FINAL_AUDIT.json', dict(status='PASS', tests_sealed=True,
        hashes={str(p): base.sha(p) for p in ROOT.rglob('*') if p.is_file() and
                p.suffix in ('.pt', '.json', '.gz', '.md', '.png') and
                p.name not in ('FINAL_AUDIT.json', 'supervisor.json', 'final_status.json')}))
    atomic_json(ROOT / 'final_status.json', dict(status='COMPLETE', stop=reason, states=states, budget=budget))


def panel():
    torch.set_num_threads(1)
    manifest = verify_resume()
    states, models = restore()
    completed = {seed: s['episodes'] for seed, s in states.items()}
    last_saved = {seed: s.get('collected_through_save', s['episodes']) for seed, s in states.items()}
    for quarter, target in enumerate(base.LIMITS, 1):
        while any(not s['stopped'] and s['episodes'] < target for s in states.values()):
            jobs, per_seed = [], {}
            for seed, state in states.items():
                if state['stopped'] or state['episodes'] >= target:
                    continue
                cp, digest, sig = write_policy(seed, state, models)
                per_seed[seed] = []
                for episode in range(state['episodes'] + 1, min(target, state['episodes'] + 3) + 1):
                    job = dict(kind='train', method='mappo', seed=int(seed), episode=episode,
                        protocol=manifest['schedule'][seed][episode - 1], checkpoint=cp, checkpoint_sha256=digest,
                        policy_signature=sig, out=str(ROOT / 'rollouts' / seed / f'{episode:04d}.json.gz'))
                    jobs.append(job)
                    per_seed[seed].append(job)

            def collected(job):
                seed = str(job['seed'])
                completed[seed] += 1
                if checkpoint_due(completed[seed], last_saved[seed]):
                    persist_seed(seed, states, models, completed[seed], per_seed[seed])
                    last_saved[seed] = completed[seed]

            run_jobs(jobs, collected)
            for seed, batch in per_seed.items():
                state = states[seed]
                rows = []
                for job in batch:
                    rows.extend(load_record(job['out'])['records'])
                last = batch[-1]['episode']
                history = base.learning.update(*models[seed], rows, int(seed) * 10000 + last)
                state['episodes'] = last
                atomic_json(RESUME / 'updates' / seed / f'{last:04d}.json', dict(status='PASS', samples=len(rows),
                    history=history, rollout_hashes={j['out']: base.sha(j['out']) for j in batch}))
                if last == target:
                    path = persist_seed(seed, states, models, completed[seed])
                    state.update(checkpoint=str(path), checkpoint_sha256=base.sha(path))
                for path in (RESUME / 'transport' / seed).glob('*.pt'):
                    path.unlink()
            atomic_json(ROOT / 'progress.json', dict(stage='training', quarter=quarter, target=target, states=states,
                        durable_recovery=str(RESUME / 'recovery.json')))
        jobs, evaluated = [], []
        for seed, state in states.items():
            if state['stopped'] or any(e['episodes'] == target for e in state['evaluations']):
                continue
            evaluated.append(seed)
            cp = state['checkpoint']
            sig = signature(torch.load(cp, map_location='cpu', weights_only=True))
            for path in manifest['validation']:
                p = base.read(path)
                jobs.append(dict(kind='validation', method='mappo', seed=int(seed), protocol=path,
                    checkpoint=cp, checkpoint_sha256=base.sha(cp), policy_signature=sig,
                    out=str(ROOT / 'validation' / f'q{quarter}' / seed / f"{p['date']}.json")))
        if jobs:
            run_jobs(jobs)
        for seed in evaluated:
            state = states[seed]
            x = base.metrics([base.read(j['out']) for j in jobs if j['seed'] == int(seed)])
            state['evaluations'].append(dict(episodes=state['episodes'], checkpoint=state['checkpoint'],
                checkpoint_sha256=base.sha(state['checkpoint']), metrics=x))
            state['bad_streak'] = base.deterioration_streak(state['bad_streak'], x, state['initial'])
            if state['bad_streak'] >= 2:
                state.update(stopped=True, stop_reason='R600 worse than initialization twice consecutively')
            persist_seed(seed, states, models, completed[seed])
        if evaluated:
            atomic_json(ROOT / 'progress.json', dict(stage='milestone', quarter=quarter, states=states))
            base.write_report(states, quarter)
            if quarter == 2:
                atomic_json(ROOT / 'HALF_READY.json', dict(status='READY', report='MILESTONE_2.md', continue_without_confirmation=True))
        if all(s['stopped'] for s in states.values()):
            break
    verify_resume()
    finalize('budget completed or pre-registered deterioration stop')


def system_excerpt():
    result = {}
    for args in (['sysctl', '-n', 'kern.boottime'], ['ps', '-axo', 'pid,ppid,lstart,command']):
        try:
            text = subprocess.check_output(args, text=True, timeout=5)
            if args[0] == 'ps':
                text = '\n'.join(line for line in text.splitlines() if 'resume_main_mappo.py' in line)
            result[args[0]] = text[:3000]
        except Exception as exc:
            result[args[0]] = str(exc)
    reports = sorted((p for p in Path('/Library/Logs/DiagnosticReports').glob('*panic*') if p.is_file()), key=lambda p: p.stat().st_mtime)
    if reports:
        path = reports[-1]
        result['panic_report'] = str(path)
        result['panic_excerpt'] = path.read_text(errors='replace')[:3500]
    log = RESUME / 'panel.log'
    if log.exists():
        with log.open('rb') as f:
            f.seek(max(0, log.stat().st_size - 5000))
            result['panel_log_tail'] = f.read().decode(errors='replace')
    return result


def consume_recovery(state, reason, evidence):
    event = dict(time_utc=now(), reason=reason, system_log=evidence)
    if state['recoveries'] >= MAX_RECOVERIES:
        event['action'] = 'STOP_LIMIT'
        state['events'].append(event)
        return False
    state['recoveries'] += 1
    event.update(action='AUTO_RESUME', number=state['recoveries'])
    state['events'].append(event)
    return True


def stop_group(pid):
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    time.sleep(1)
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def supervisor():
    RESUME.mkdir(parents=True, exist_ok=True)
    with (RESUME / 'supervisor.lock').open('w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if (ROOT / 'final_status.json').exists():
            return
        path = RESUME / 'supervisor.json'
        state = base.read(path) if path.exists() else dict(recoveries=0, events=[], active=False)
        if state.get('active'):
            old_pid = state.get('child_pid')
            if old_pid:
                check = subprocess.run(['ps', '-p', str(old_pid), '-o', 'lstart=,command='], capture_output=True, text=True)
                if check.stdout.strip() == state.get('child_identity'):
                    stop_group(old_pid)
            allowed = consume_recovery(state, 'Previous supervisor interrupted or system rebooted', system_excerpt())
            atomic_json(path, state)
            if not allowed:
                atomic_json(ROOT / 'final_status.json', dict(status='BLOCKED', reason='automatic recovery limit exceeded'))
                return
        while True:
            if datetime.now(timezone.utc) >= DEADLINE:
                finalize('2026-10-30 00:00 Asia/Shanghai cutoff; best validated checkpoint frozen')
                break
            try:
                verify_resume()
            except Exception:
                atomic_json(ROOT / 'final_status.json', dict(status='BLOCKED', traceback=traceback.format_exc()))
                break
            state.update(active=True, child_pid=None, supervisor_pid=os.getpid(), started_utc=now())
            atomic_json(path, state)
            with (RESUME / 'panel.log').open('a') as log:
                child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'panel'],
                    stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            guard = subprocess.Popen(['caffeinate', '-i', '-w', str(child.pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            identity = subprocess.check_output(['ps', '-p', str(child.pid), '-o', 'lstart=,command='], text=True).strip()
            state.update(child_pid=child.pid, child_identity=identity, caffeinate_pid=guard.pid)
            atomic_json(path, state)
            while child.poll() is None and datetime.now(timezone.utc) < DEADLINE:
                time.sleep(5)
            if child.poll() is None:
                stop_group(child.pid)
                child.wait()
                finalize('2026-10-30 00:00 Asia/Shanghai cutoff; best validated checkpoint frozen')
                break
            guard.wait(timeout=10)
            if child.returncode == 0 and (ROOT / 'final_status.json').exists():
                break
            stop_group(child.pid)
            if child.returncode == 2:
                atomic_json(ROOT / 'final_status.json', dict(status='BLOCKED', reason='audit failure; no automatic retry', evidence=system_excerpt()))
                break
            allowed = consume_recovery(state, f'Panel exited with code {child.returncode}', system_excerpt())
            state['active'] = False
            atomic_json(path, state)
            if not allowed:
                atomic_json(ROOT / 'final_status.json', dict(status='BLOCKED', reason='automatic recovery limit exceeded', evidence=state['events']))
                break
        state.update(active=False, finished_utc=now())
        atomic_json(path, state)


def prepare():
    verify = ORIGINAL_VERIFY()
    if RESUME.exists():
        raise FileExistsError(RESUME)
    progress = base.read(ROOT / 'progress.json')
    for seed, state in progress['states'].items():
        assert state['episodes'] == ANCHOR and not state['stopped']
        data = torch.load(state['checkpoint'], map_location='cpu', weights_only=True)
        assert data['episodes'] == ANCHOR and data['seed'] == int(seed)
        state.update(checkpoint_sha256=base.sha(state['checkpoint']), collected_through_save=ANCHOR)
    RESUME.mkdir()
    atomic_json(RESUME / 'initial_progress.json', progress)
    files = [Path(__file__), Path('tests/test_main_mappo_resume.py'), base.MAIN / 'MAPPO_RESUME_20261025.md']
    atomic_json(RESUME / 'manifest.json', dict(entry_hashes={str(p): base.sha(p) for p in files},
        original_manifest_sha256=base.sha(ROOT / 'manifest.json'), anchor=ANCHOR, workers=verify['workers'],
        backend='CPU; torch intraop threads=1; multiprocessing spawn', save_every_collected_episodes=25,
        keep_recent=3, keep_validation=True, max_automatic_recoveries=MAX_RECOVERIES, deadline=DEADLINE.isoformat(),
        limits=base.LIMITS, old_rng_saved=False, rng_policy='existing per-episode sampling and per-batch shuffle seeds'))
    atomic_json(RESUME / 'recovery.json', dict(states=progress['states'], updated_utc=now()))
    atomic_json(RESUME / 'initial_interruption.json', dict(time_utc=now(), evidence=system_excerpt(), charged_to_retry_budget=False))
    print(json.dumps(dict(status='PREPARED', root=str(RESUME))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'supervisor', 'panel'])
    mode = parser.parse_args().mode
    try:
        globals()[mode]()
    except AuditFailure:
        traceback.print_exc()
        sys.exit(2)
