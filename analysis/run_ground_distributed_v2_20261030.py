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

DIST = r.ROOT/'distributed_20261030_v2'
PREVIOUS = r.ROOT/'distributed_20261030'
REG = DIST/'REGISTRATION.json'
REMOTE_ROOT = '/Users/bytedance/Projects/bs'
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
    m = registration()
    # 用户明确批准此次启动前路径错误修正；旧协调器只负责原L批次收尾。
    for host in HOSTS:
        old=r.read(PREVIOUS/host/'final_status.json')
        assert old['status']=='BLOCKED' and len(old['results'])==6
        assert all(x['status']=='FAILED' and 'Frozen source or diagnostic implementation changed' in x['traceback'] for x in old['results'])
    exclusive(DIST/'TAKEOVER_STARTED.json',dict(at=now(),previous=str(PREVIOUS),registration_sha256=r.sha(REG)))
    exclusive(DIST/'M_OWNERSHIP_ACQUIRED.json',dict(at=now(),registration_sha256=r.sha(REG),
        previous_remote_batches_stopped=True,previous_runs_failed_before_environment_creation=True,
        user_authorized_path_correction=True))
    start_remotes()
    while True:
        r.check_time()
        previous_final=PREVIOUS/'local/final_status.json'
        if previous_final.exists():
            assert (PREVIOUS/'TAKEOVER_COMPLETE.json').exists(), 'Previous L handoff failed; inspect without retry'
            assert r.read(previous_final)['status']=='BLOCKED'
            old=r.read(PREVIOUS/'local/launch.json')
            if all(subprocess.run(['kill','-0',str(old[k])],capture_output=True).returncode!=0 for k in ('pid','supervisor_pid')):
                break
        errors=sync_remote(copy_results=False)
        assert not any(x.get('worker_failure') for x in errors), 'Corrected deployment failed; no automatic retry'
        r.write_json(DIST/'local/status.json',dict(status='WAITING_PREVIOUS_L_BATCH',at=now(),passed=len(finished())))
        time.sleep(30)
    failures=list((r.ROOT/'M_scaled/validation').glob('*/*.progress.json'))
    assert len(failures)==18
    archive={}
    for p in failures:
        failed=r.read(p)
        assert failed['status']=='FAILED' and 'Frozen source or diagnostic implementation changed' in failed['traceback']
        assert not p.with_name(p.name.replace('.progress.json','.json')).exists()
        dest=DIST/'retired_startup_failures'/p.parent.name/p.name
        dest.parent.mkdir(parents=True,exist_ok=True); assert not dest.exists()
        h=r.sha(p); p.rename(dest); assert r.sha(dest)==h
        archive[str(p)]=dict(archive=str(dest),sha256=h)
    exclusive(DIST/'RETIRED_FAILURES.json',dict(at=now(),records=archive))
    progress=[r.read(p) for stage in ('L_single_completion','L_scaled') for p in (r.ROOT/stage/'validation').glob('*/*.progress.json')]
    assert all(x['status']=='PASS' for x in progress)
    rows=[x for x in finished() if x['stage']!='M_scaled']; assert len(rows)==len(progress)
    exclusive(DIST/'OLD_BATCH_AUDIT.json',dict(status='PASS',at=now(),results=rows))
    r.verify_manifest()
    exclusive(DIST/'TAKEOVER_COMPLETE.json',dict(at=now(),old_jobs_passed=len(rows),
        reason='User-authorized correction; prior failure evidence archived; previous L batch retained'))
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
            os.killpg(child.pid,signal.SIGTERM)
            try: child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid,signal.SIGKILL); child.wait()
            r.write_json(dest/'supervisor_failure.json',dict(status='DEADLINE',at=now()))
            return
        time.sleep(2)
    if child.returncode:
        r.write_json(dest/'supervisor_failure.json',dict(status='BLOCKED',at=now(),returncode=child.returncode))


def sync_remote(copy_results=True):
    failures = []
    for host in HOSTS:
        remote = f'{host}:{REMOTE_ROOT}/'
        dest = DIST/host; dest.mkdir(exist_ok=True)
        commands = [
            ['rsync','-a','--timeout=25','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',
             remote+str(DIST/host)+'/',str(dest)+'/']]
        if copy_results:
            commands.append(['rsync','-a','--timeout=25','-e','ssh -o BatchMode=yes -o ConnectTimeout=10',
                remote+str(r.ROOT/'M_scaled/validation')+'/',str(r.ROOT/'M_scaled/validation')+'/'])
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
