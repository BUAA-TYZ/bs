"""已授权跨机调度：批次交接、排他分片、原冻结仿真入口和原报告器。"""
import argparse
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
import json
import multiprocessing
import os
import platform
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback

from analysis import run_ground_scaling_20261029 as r

DIST = r.ROOT/'distributed_20261030'
REG = DIST/'REGISTRATION.json'
REMOTE_ROOT = '/home/tianyuzhe/bs_ground_scaling_20261030'
HOSTS = ('devbox1', 'devbox2', 'devbox3')


def now():
    return datetime.now(timezone.utc).isoformat()


def exclusive(path, data):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x') as stream:
        json.dump(data,stream,indent=2)


def registration():
    m = r.read(REG)
    assert r.sha(r.ROOT/'manifest.json') == m['manifest_sha256']
    for p, h in m['hashes'].items():
        assert r.sha(p) == h, p
    r.verify_manifest()
    assignments = [tuple(job) for host in HOSTS for job in m['assignments'][host]]
    run = r.read(r.ROOT/'M_scaled/run_manifest.json')
    expected = {('M_scaled',p,n) for p in run['protocols'] for n in run['methods']}
    assert len(assignments) == len(set(assignments)) == 60 and set(assignments)==expected
    return m


def finished():
    results = []
    for stage in r.STAGES:
        for p in sorted((r.ROOT/stage/'validation').glob('*/*.progress.json')):
            short = r.read(p)
            if short['status'] == 'PASS' and Path(short['path']).exists() and r.sha(short['path'])==short['sha256']:
                row = r.read(short['path'])
                assert row['stage']==stage and row['status']=='PASS'
                results.append(dict(short,stage=stage))
    assert len({x['path'] for x in results}) == len(results)
    return results


def make_assignments(run):
    assignments = {host:[] for host in HOSTS}
    for i, name in enumerate(run['methods']):
        for j, protocol in enumerate(run['protocols']):
            assignments[HOSTS[(i+j)%len(HOSTS)]].append(('M_scaled',protocol,name))
    return assignments


def start_remotes():
    for target in HOSTS:
        subprocess.run(['scp','-q',str(DIST/'M_OWNERSHIP_ACQUIRED.json'),f'{target}:{REMOTE_ROOT}/{DIST}/'],check=True,timeout=45)
        command = f'cd {REMOTE_ROOT} && OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 .venv/bin/python -B -m {__spec__.name} launch {target}'
        completed = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10',target,command],
            capture_output=True,text=True,timeout=60)
        r.write_json(DIST/f'launch_{target}_response.json',dict(at=now(),returncode=completed.returncode,
            stdout=completed.stdout,stderr=completed.stderr))
        assert completed.returncode==0, f'Uncertain/failed launch {target}; do not retry automatically'


def stop_after_batch(reason):
    errors = []
    r.write_json(DIST/'local/STOP_AFTER_BATCH.json',dict(at=now(),reason=reason))
    for target in HOSTS:
        control = DIST/target/'STOP_AFTER_BATCH.json'
        if not control.exists():
            r.write_json(control,dict(at=now(),reason=reason))
        try:
            subprocess.run(['scp','-q',str(control),f'{target}:{REMOTE_ROOT}/{control}'],timeout=30,check=True)
        except (subprocess.TimeoutExpired,subprocess.CalledProcessError):
            errors.append(dict(host=target,error='Could not deliver stop-after-batch control'))
    return errors


def drain_old():
    m = registration(); launch = r.read(r.ROOT/'launch.json')
    assert launch['pid']==m['old_launch']['pid']
    assert not list((r.ROOT/'M_scaled/validation').glob('*/*.progress.json'))
    identity = subprocess.check_output(['ps','-p',str(launch['pid']),'-o','pid=,lstart=,command='],text=True).strip()
    assert identity == launch['identity'], (identity,launch['identity'])
    supervisor = subprocess.check_output(['ps','-p',str(launch['supervisor_pid']),'-o','pid=,lstart=,command='],text=True).strip()
    assert supervisor==m['old_supervisor_identity']
    exclusive(DIST/'TAKEOVER_STARTED.json',dict(at=now(),old_launch=launch,registration_sha256=r.sha(REG)))
    # 仅暂停派发与旧监督；原工作进程继续跑完已派发批次。
    os.kill(launch['supervisor_pid'],signal.SIGSTOP)
    os.kill(launch['pid'],signal.SIGSTOP)
    time.sleep(1)
    for pid in (launch['pid'], launch['supervisor_pid']):
        assert 'T' in subprocess.check_output(['ps','-p',str(pid),'-o','stat='],text=True)
    assert not list((r.ROOT/'M_scaled/validation').glob('*/*.progress.json'))
    exclusive(DIST/'M_OWNERSHIP_ACQUIRED.json',dict(at=now(),registration_sha256=r.sha(REG),
        old_coordinator_stopped=True,old_supervisor_stopped=True,no_M_jobs_started=True))
    start_remotes()
    workers = [int(p) for p in subprocess.check_output(['pgrep','-P',str(launch['pid'])],text=True).split()]
    def worker_times():
        value = subprocess.check_output(['ps','-p',','.join(map(str,workers)),'-o','pid=,time='],text=True)
        return value.strip()
    while True:
        r.check_time()
        progress = [r.read(p) for stage in ('L_single_completion','L_scaled')
                    for p in (r.ROOT/stage/'validation').glob('*/*.progress.json')]
        active = [x for x in progress if x['status']=='RUNNING']
        r.write_json(DIST/'local/status.json',dict(status='DRAINING_OLD_BATCH',at=now(),active=len(active),passed=len(finished())))
        if not active:
            assert all(x['status']=='PASS' for x in progress), 'Old batch failure; no retry'
            # 首条进度可能尚未落盘；工作进程CPU时间静止后才确认池已空闲。
            before = worker_times(); time.sleep(20)
            if worker_times()==before:
                break
        time.sleep(15)
        sync_remote()
    rows = [x for x in finished() if x['stage']!='M_scaled']; assert len(rows)==len(progress)
    exclusive(DIST/'OLD_BATCH_AUDIT.json',dict(status='PASS',at=now(),results=rows))
    os.killpg(launch['pid'],signal.SIGTERM)
    os.kill(launch['pid'],signal.SIGCONT)
    os.kill(launch['supervisor_pid'],signal.SIGTERM)
    os.kill(launch['supervisor_pid'],signal.SIGCONT)
    time.sleep(2)
    for pid in (launch['pid'],launch['supervisor_pid']):
        assert subprocess.run(['kill','-0',str(pid)],capture_output=True).returncode != 0, f'Old process still present: {pid}'
    r.verify_manifest()
    exclusive(DIST/'TAKEOVER_COMPLETE.json',dict(at=now(),old_jobs_passed=len(rows),
        reason='User-authorized placement change; drained tasks retained; old coordinator intentionally retired, not simulation failure'))
    return rows


def execute(job):
    # 不修改原仿真函数、输入、模型或输出统计。
    return r.simulate(tuple(job))


def run_batches(host, jobs, workers, sync=None):
    result = []; destination = DIST/host
    with ProcessPoolExecutor(max_workers=workers,mp_context=multiprocessing.get_context('spawn')) as pool:
        for offset in range(0,len(jobs),workers):
            r.check_time()
            if (destination/'STOP_AFTER_BATCH.json').exists():
                return result,False
            batch = jobs[offset:offset+workers]
            exclusive(destination/f'batch_{offset:03d}.json',dict(at=now(),jobs=batch))
            futures = {pool.submit(execute,job):job for job in batch}; current = []
            while futures:
                done,_ = wait(futures,timeout=30,return_when=FIRST_COMPLETED)
                for f in done:
                    job = futures.pop(f)
                    try:
                        row = f.result()
                    except Exception:
                        row = dict(status='FAILED',job=job,traceback=traceback.format_exc())
                    current.append(row); result.append(row)
                r.write_json(destination/'status.json',dict(status='RUNNING',at=now(),done=len(result),
                    total=len(jobs),inflight=len(futures),results=result))
                if sync:
                    sync()
            if any(x['status']!='PASS' for x in current):
                return result,False
    return result,True


def launch(host):
    registration()
    if host!='local':
        assert platform.node()==r.read(DIST/'RUNTIME_ENVIRONMENTS.json')[host]['host']
    dest = DIST/host; dest.mkdir(parents=True,exist_ok=True)
    exclusive(dest/'launch_request.json',dict(at=now(),registration_sha256=r.sha(REG)))
    with (dest/'supervisor.log').open('xb') as log:
        child = subprocess.Popen([sys.executable,'-B','-m',__spec__.name,'supervise',host],
            stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    print(json.dumps(dict(host=host,supervisor_pid=child.pid)),flush=True)


def supervise(host):
    dest = DIST/host
    with (dest/'panel.log').open('xb') as log:
        child = subprocess.Popen([sys.executable,'-B','-m',__spec__.name,'panel',host],
            stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    info = subprocess.check_output(['ps','-p',str(child.pid),'-o','pid=,lstart=,args='],text=True).strip()
    r.write_json(dest/'launch.json',dict(at=now(),pid=child.pid,supervisor_pid=os.getpid(),identity=info,
        registration_sha256=r.sha(REG)))
    if host=='local':
        subprocess.Popen(['/usr/bin/caffeinate','-i','-w',str(child.pid)],start_new_session=True)
    while child.poll() is None:
        if datetime.now(timezone.utc)>=r.END:
            if host=='local' and (DIST/'TAKEOVER_STARTED.json').exists() and not (DIST/'TAKEOVER_COMPLETE.json').exists():
                old = r.read(r.ROOT/'launch.json')
                try: os.killpg(old['pid'],signal.SIGKILL)
                except ProcessLookupError: pass
                try: os.kill(old['supervisor_pid'],signal.SIGKILL)
                except ProcessLookupError: pass
            os.killpg(child.pid,signal.SIGTERM)
            try: child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid,signal.SIGKILL); child.wait()
            r.write_json(dest/'supervisor_failure.json',dict(status='DEADLINE',at=now()))
            return
        time.sleep(2)
    if child.returncode:
        r.write_json(dest/'supervisor_failure.json',dict(status='BLOCKED',at=now(),returncode=child.returncode))
        if host=='local' and (DIST/'TAKEOVER_STARTED.json').exists() and not (DIST/'TAKEOVER_COMPLETE.json').exists():
            # 交接异常也保留在途L批次；旧协调器保持停止，监督仍负责硬截止。
            old=r.read(r.ROOT/'launch.json')
            stable=None
            while True:
                active=[r.read(p) for stage in ('L_single_completion','L_scaled')
                    for p in (r.ROOT/stage/'validation').glob('*/*.progress.json') if r.read(p)['status']=='RUNNING']
                times=subprocess.run(['ps','-axo','pid=,ppid=,time='],capture_output=True,text=True).stdout
                times='\n'.join(line for line in times.splitlines() if line.split()[1]==str(old['pid']))
                if datetime.now(timezone.utc)>=r.END or (not active and stable==times):
                    break
                stable=times if not active else None
                time.sleep(30)
            for pid in (old['pid'],old['supervisor_pid']):
                try: os.killpg(pid,signal.SIGKILL)
                except ProcessLookupError: pass
            r.write_json(dest/'FAILED_HANDOFF_DRAINED.json',dict(at=now(),deadline=datetime.now(timezone.utc)>=r.END))


def sync_remote():
    failures = []
    for host in HOSTS:
        remote = f'{host}:{REMOTE_ROOT}/'
        dest = DIST/host; dest.mkdir(exist_ok=True)
        commands = [
            ['rsync','-a','--timeout=25','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',
             remote+str(DIST/host)+'/',str(dest)+'/'],
            ['rsync','-a','--timeout=25','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',
             remote+str(r.ROOT/'M_scaled/validation')+'/',str(r.ROOT/'M_scaled/validation')+'/']]
        for command in commands:
            try:
                completed = subprocess.run(command,capture_output=True,text=True,timeout=45)
                if completed.returncode:
                    failures.append(dict(host=host,error=completed.stderr[-2000:]))
            except subprocess.TimeoutExpired:
                failures.append(dict(host=host,error='Transport timeout; task status unconfirmed; no restart'))
        if (dest/'final_status.json').exists() and r.read(dest/'final_status.json')['status']!='COMPLETE':
            failures.append(dict(host=host,worker_failure=True))
    results = finished()
    if any(x.get('worker_failure') for x in failures):
        failures.extend(stop_after_batch('Peer failed; drain current batch only'))
    r.write_json(r.ROOT/'progress.json',dict(stage='DISTRIBUTED_L_M',done=len(results),total=147,results=results))
    r.write_json(DIST/'SYNC_STATUS.json',dict(at=now(),transport_errors=failures,passed=len(results)))
    return failures


def panel(host):
    started = time.perf_counter(); dest = DIST/host; m = registration()
    try:
        if host!='local':
            assert r.read(DIST/'M_OWNERSHIP_ACQUIRED.json')['registration_sha256']==r.sha(REG)
            rows,ok = run_batches(host,m['assignments'][host],m['remote_workers'])
            registration()
            r.write_json(dest/'final_status.json',dict(status='COMPLETE' if ok else 'BLOCKED',at=now(),results=rows,
                wall_seconds=time.perf_counter()-started,registration_sha256=r.sha(REG)))
            return
        rows = drain_old()
        old_paths = {row['path'] for row in rows}
        run = r.read(r.ROOT/'L_scaled/run_manifest.json')
        jobs = [('L_scaled',p,n) for n in run['methods'] for p in run['protocols']
                if str(r.ROOT/'L_scaled/validation'/r.read(p)['date']/f'{n}.json') not in old_paths]
        local_rows,ok = run_batches('local',jobs,9,sync_remote)
        assert ok, 'Local failure; current batch drained; no retry'
        while True:
            r.check_time(); errors = sync_remote()
            states = [r.read(DIST/h/'final_status.json') if (DIST/h/'final_status.json').exists() else None for h in HOSTS]
            assert not any(s and s['status']=='BLOCKED' for s in states), 'Remote failure; see per-host final_status'
            if not errors and all(s and s['status']=='COMPLETE' for s in states):
                break
            r.write_json(dest/'status.json',dict(status='WAITING_REMOTE',at=now(),done=len(finished()),total=147))
            time.sleep(60)
        results = finished(); assert len(results)==147
        assigned = {tuple(job):h for h in HOSTS for job in m['assignments'][h]}
        execution = {}
        for h in HOSTS:
            state = r.read(DIST/h/'final_status.json')
            assert len(state['results'])==20
            for short in state['results']:
                row = r.read(short['path'])
                assert assigned[('M_scaled',row['protocol'],row['method'])]==h
                assert r.sha(short['path'])==short['sha256']
                execution[short['path']] = dict(host=h,sha256=short['sha256'])
        registration()
        r.write_json(DIST/'DISTRIBUTED_AUDIT.json',dict(status='PASS',at=now(),remote_records=execution,
            registration_sha256=r.sha(REG),preflight=m['preflight_hashes'],notes=m['notes']))
        r.write_json(r.ROOT/'final_status.json',dict(status='COMPLETE',results=results,skipped=[],
            wall_seconds=(datetime.now(timezone.utc)-datetime.fromisoformat(m['old_launch']['started_at'])).total_seconds(),
            distributed_registration_sha256=r.sha(REG)))
        r.emit_report()
        r.write_json(dest/'final_status.json',dict(status='COMPLETE',at=now(),total=147))
    except Exception:
        r.write_json(dest/'final_status.json',dict(status='BLOCKED',at=now(),traceback=traceback.format_exc()))
        if host=='local':
            r.write_json(dest/'STOP_DELIVERY.json',dict(at=now(),errors=stop_after_batch('Coordinator blocked; drain current batch only')))
        raise


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=('launch','supervise','panel'))
    parser.add_argument('host',choices=('local',*HOSTS));args=parser.parse_args()
    globals()[args.mode](args.host)
