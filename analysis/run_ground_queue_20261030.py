"""扩站实验的队列交接适配器，原仿真和报告入口保持冻结。"""
import argparse
import base64
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time
import traceback
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener, ProxyHandler
import uuid

from analysis import run_ground_distributed_v2_20261030 as old

r = old.r
ROOT = r.ROOT / 'http_queue_20261030'
REG = ROOT / 'REGISTRATION.json'
SPEC = ROOT / 'EXPERIMENT.json'
HOSTS = ('local', *old.HOSTS)
URL = 'http://10.251.250.34:18765'
TOKEN = Path.home() / '.local/state/bs-evaluation-queue/token'
MODULE = 'analysis.run_ground_queue_20261030'
EXPERIMENT = 'ground_scaling_20261029'


def now():
    return datetime.now(timezone.utc).isoformat()


def job_id(job):
    return hashlib.sha256(json.dumps(list(job), separators=(',', ':')).encode()).hexdigest()


def api(route, body, retry=True):
    request = Request(URL + route, json.dumps(body).encode(),
                      {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + TOKEN.read_text().strip()})
    opener = build_opener(ProxyHandler({}))
    while True:
        r.check_time()
        try:
            with opener.open(request, timeout=40) as response:
                return json.load(response)
        except HTTPError as exc:
            if retry and exc.code in (429, 500, 502, 503, 504):
                time.sleep(10)
                continue
            raise RuntimeError(f'Queue HTTP {exc.code}: {exc.read().decode()}') from exc
        except (URLError, TimeoutError, ConnectionError, OSError):
            if not retry:
                raise
            # 只重传同一HTTP请求，不重新执行仿真；领取请求携带幂等键。
            time.sleep(10)


def verify():
    registration = r.read(REG)
    for path, sha in registration['hashes'].items():
        assert r.sha(path) == sha, path
    old.registration()
    spec = r.read(SPEC)
    assert len(spec['jobs']) == len({j['id'] for j in spec['jobs']}) == 147
    return spec


def register():
    assert not REG.exists()
    previous = old.registration()
    assert (old.DIST / 'TAKEOVER_COMPLETE.json').exists(), 'Wait for prior path-correction handoff'
    assignments = {tuple(j): h for h in old.HOSTS for j in previous['assignments'][h]}
    jobs = []
    for stage in r.STAGES:
        run = r.read(r.ROOT / stage / 'run_manifest.json')
        for method in run['methods']:
            for protocol in run['protocols']:
                job = [stage, protocol, method]
                out = r.ROOT / stage / 'validation' / r.read(protocol)['date'] / f'{method}.json'
                jobs.append(dict(id=job_id(job), job=job, output=str(out),
                                 legacy_host=assignments[tuple(job)] if stage == 'M_scaled' else 'local',
                                 hosts=list(HOSTS) if stage == 'M_scaled' else ['local']))
    old.exclusive(SPEC, dict(id=EXPERIMENT, jobs=jobs, hosts=dict(local=9, devbox1=6, devbox2=6, devbox3=6),
                            deadline=r.END.timestamp(), priority='L on Mac, then M; remote M available immediately',
                            dispatch='per-free-slot; any failure stops all new claims; no automatic reassignment'))
    paths = [Path(__file__).resolve(), Path('analysis/evaluation_queue.py').resolve(),
             Path('analysis/EVALUATION_QUEUE.md'),
             Path('tests/test_evaluation_queue.py'), ROOT / 'DISPATCH_TESTS.json',
             ROOT / 'QUEUE_HANDOFF_AMENDMENT.md', SPEC, old.REG]
    for stage in r.STAGES:
        paths.append(r.ROOT / stage / 'run_manifest.json')
        paths += [Path(p) for p in r.read(paths[-1])['protocols']]
    assert r.read(ROOT / 'DISPATCH_TESTS.json')['returncode'] == 0
    old.exclusive(REG, dict(at=now(), hashes={str(p): r.sha(p) for p in paths},
                           original_manifest_sha256=r.sha(r.ROOT / 'manifest.json'),
                           previous_registration_sha256=r.sha(old.REG),
                           deadline=r.END.isoformat(), host_workers=r.read(SPEC)['hosts'],
                           user_authorization='2026-10-30 HTTP queue on devbox3; existing four machines; no new machine',
                           unchanged='simulation/model/inputs/protocol/statistics; no retry, no tuning, no paper edits'))
    verify()
    print(json.dumps({'status': 'REGISTERED', 'sha256': r.sha(REG)}))


def process_active(launch, key):
    pid = launch[key]
    row = subprocess.run(['ps', '-p', str(pid), '-o', 'args='], capture_output=True, text=True)
    return row.returncode == 0 and 'analysis.run_ground_distributed_v2_20261030' in row.stdout


def release_legacy(host, spec):
    dest = ROOT / host
    release_file = dest / 'LEGACY_RELEASE.json'
    if release_file.exists():
        body = r.read(release_file)
        return api('/release', body)
    launch = r.read(old.DIST / host / 'launch.json')
    while process_active(launch, 'pid') or process_active(launch, 'supervisor_pid'):
        r.check_time()
        r.write_json(dest / 'status.json', dict(status='DRAINING_LEGACY_BATCH', at=now(), legacy=launch))
        time.sleep(20)
    final = old.DIST / host / 'final_status.json'
    state = r.read(final)
    assert state['status'] in ('BLOCKED', 'COMPLETE')
    assert (old.DIST / host / 'STOP_AFTER_BATCH.json').exists()
    assert all(row['status'] == 'PASS' for row in state.get('results', [])), 'Legacy simulation failed; do not release'
    completed = {}
    own = {tuple(j['job']): j for j in spec['jobs'] if j['legacy_host'] == host}
    for item in own.values():
        out = Path(item['output'])
        progress = out.with_suffix('.progress.json')
        if progress.exists():
            short = r.read(progress)
            assert short['status'] == 'PASS' and out.exists(), f'Legacy task not PASS: {progress}'
            assert r.sha(out) == short['sha256']
            row = r.read(out)
            assert [row['stage'], row['protocol'], row['method']] == item['job']
            assert short['path'] == str(out)
            completed[item['id']] = short
        else:
            assert not out.exists(), f'Orphan result: {out}'
    evidence = {str(final): r.sha(final)}
    for path in sorted((old.DIST / host).glob('batch_*.json')):
        for job in r.read(path)['jobs']:
            assert job_id(job) in completed, 'A started legacy job is missing; never reissue'
        evidence[str(path)] = r.sha(path)
    if host == 'local':
        assert (old.DIST / 'TAKEOVER_COMPLETE.json').exists()
        # v2本地的非成功终态只能来自本次显式停止派发。
        assert state['status'] == 'COMPLETE' or 'Local failure; current batch drained; no retry' in state.get('traceback', '')
    body = dict(exp=EXPERIMENT, host=host, completed=completed, evidence=evidence)
    old.exclusive(release_file, body)
    return api('/release', body)


def execute(item):
    # 不改冻结函数，不允许覆盖旧进度，也不从部分仿真恢复。
    assert not Path(item['output']).exists()
    assert not Path(item['output']).with_suffix('.progress.json').exists()
    return r.simulate(item['job'])


def worker(host):
    spec = verify()
    if host != 'local':
        assert platform.node() == r.read(old.DIST / 'RUNTIME_ENVIRONMENTS.json')[host]['host']
    dest = ROOT / host
    owner = host + ':' + uuid.uuid4().hex
    try:
        release_legacy(host, spec)
        capacity = spec['hosts'][host]
        r.write_json(dest / 'owner.json', dict(owner=owner, capacity=capacity, at=now()))
        with ProcessPoolExecutor(max_workers=capacity, mp_context=multiprocessing.get_context('spawn')) as pool:
            futures = {}
            stopped = False
            last_status = 0
            while True:
                r.check_time()
                api('/heartbeat', dict(exp=EXPERIMENT, owner=owner))
                if futures:
                    done, _ = wait(futures, timeout=5, return_when=FIRST_COMPLETED)
                    for future in done:
                        item = futures.pop(future)
                        try:
                            short = future.result()
                        except Exception:
                            short = dict(status='FAILED', job=item['job'], traceback=traceback.format_exc())
                        blob = None
                        if short['status'] == 'PASS':
                            data = Path(short['path']).read_bytes()
                            assert hashlib.sha256(data).hexdigest() == short['sha256']
                            blob = base64.b64encode(data).decode()
                        else:
                            stopped = True
                        api('/finish', dict(exp=EXPERIMENT, jid=item['id'], owner=owner, short=short, blob=blob))
                if not stopped and len(futures) < capacity:
                    request = uuid.uuid4().hex
                    claim = api('/claim', dict(exp=EXPERIMENT, host=host, owner=owner,
                                               capacity=capacity-len(futures), request=request))
                    old.exclusive(dest / 'claims' / f'{request}.json', dict(at=now(), **claim))
                    stopped = bool(claim.get('blocked'))
                    for item in claim['jobs']:
                        futures[pool.submit(execute, item)] = item
                if time.monotonic() - last_status > 30 or not futures:
                    state = api('/status', dict(exp=EXPERIMENT))
                    stopped = stopped or bool(state['blocked'])
                    r.write_json(dest / 'status.json', dict(at=now(), status='DRAINING' if stopped else 'RUNNING',
                        owner=owner, capacity=capacity, inflight=[x['id'] for x in futures.values()], counts=state['counts']))
                    last_status = time.monotonic()
                    if not futures and (stopped or state['complete']):
                        r.write_json(dest / 'final_status.json', dict(at=now(), status='BLOCKED' if stopped else 'COMPLETE'))
                        return
                if not futures:
                    time.sleep(15)
    except Exception:
        failure = dict(status='BLOCKED', at=now(), traceback=traceback.format_exc())
        r.write_json(dest / 'final_status.json', failure)
        try:
            api('/block', dict(exp=EXPERIMENT, reason=json.dumps(failure)), retry=False)
        except Exception:
            pass
        raise


def launch(role):
    verify()
    dest = ROOT / role
    old.exclusive(dest / 'launch_request.json', dict(at=now(), registration_sha256=r.sha(REG)))
    with (dest / 'supervisor.log').open('xb') as log:
        process = subprocess.Popen([sys.executable, '-B', '-m', MODULE, 'supervise', role],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    print(json.dumps(dict(role=role, supervisor_pid=process.pid)))


def supervise(role):
    dest = ROOT / role
    if role == 'server':
        command = [sys.executable, '-B', '-m', 'analysis.evaluation_queue', '--db', str(dest / 'queue.sqlite'),
                   '--token-file', str(TOKEN), '--bind', '10.251.250.34', '--port', '18765']
    else:
        command = [sys.executable, '-B', '-m', MODULE, 'coordinate' if role == 'coordinator' else 'worker', role]
    with (dest / 'process.log').open('xb') as log:
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
    identity = subprocess.check_output(['ps', '-p', str(process.pid), '-o', 'pid=,lstart=,args='], text=True).strip()
    r.write_json(dest / 'launch.json', dict(at=now(), pid=process.pid, supervisor_pid=os.getpid(), identity=identity))
    if role in ('local', 'coordinator'):
        subprocess.Popen(['/usr/bin/caffeinate', '-i', '-w', str(process.pid)], start_new_session=True)
    while process.poll() is None:
        if datetime.now(timezone.utc) >= r.END:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            r.write_json(dest / 'supervisor_failure.json', dict(status='DEADLINE', at=now()))
            return
        time.sleep(2)
    if process.returncode:
        r.write_json(dest / 'supervisor_failure.json', dict(status='BLOCKED', at=now(), returncode=process.returncode))


def collect(state):
    results = []
    ownership = {}
    for item in state['jobs']:
        assert item['state'] == 'PASS'
        short = item['result']
        out = Path(item['spec']['output'])
        assert short['path'] == str(out)
        out.parent.mkdir(parents=True, exist_ok=True)
        if not out.exists():
            if item['owner'].startswith('legacy:'):
                host = item['owner'].split(':', 1)[1]
                assert host != 'local'
                source = old.REMOTE_ROOT + '/' + str(out)
            else:
                host = 'devbox3'
                name = hashlib.sha256((EXPERIMENT + '/' + item['id']).encode()).hexdigest()
                source = old.REMOTE_ROOT + '/' + str(ROOT / 'server/artifacts' / name)
            temp = out.with_suffix('.download')
            while True:
                r.check_time()
                try:
                    subprocess.run(['scp', '-q', '-o', 'ConnectTimeout=10', f'{host}:{source}', str(temp)], check=True, timeout=120)
                    break
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                    time.sleep(30)
            assert r.sha(temp) == short['sha256']
            temp.replace(out)
        assert r.sha(out) == short['sha256'], str(out)
        row = r.read(out)
        assert [row['stage'], row['protocol'], row['method']] == item['spec']['job']
        r.write_json(out.with_suffix('.progress.json'), short)
        results.append(dict(short, stage=row['stage']))
        ownership[str(out)] = dict(owner=item['owner'], sha256=short['sha256'], job_id=item['id'])
    assert len(results) == len(ownership) == 147
    verify()
    r.write_json(ROOT / 'DISTRIBUTED_AUDIT.json', dict(status='PASS', at=now(), records=ownership,
        registration_sha256=r.sha(REG), notes='Legacy batches drained; atomic claims; no reissue; Mac ARM/Linux x86 full trajectory bitwise equality not proven'))
    r.write_json(r.ROOT / 'final_status.json', dict(status='COMPLETE', results=results, skipped=[],
        wall_seconds=(datetime.now(timezone.utc)-datetime.fromisoformat(r.read(old.REG)['old_launch']['started_at'])).total_seconds(),
        queue_registration_sha256=r.sha(REG)))
    r.emit_report()


def coordinate(role):
    verify()
    dest = ROOT / role
    try:
        while True:
            r.check_time()
            state = api('/status', dict(exp=EXPERIMENT))
            r.write_json(ROOT / 'QUEUE_STATUS.json', dict(at=now(), **state))
            if state['blocked']:
                r.write_json(dest / 'final_status.json', dict(status='BLOCKED', at=now(), reason=state['blocked']))
                return
            if state['complete']:
                collect(state)
                r.write_json(dest / 'final_status.json', dict(status='COMPLETE', at=now(), total=147))
                return
            time.sleep(30)
    except Exception:
        r.write_json(dest / 'final_status.json', dict(status='BLOCKED', at=now(), traceback=traceback.format_exc()))
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('register', 'launch', 'supervise', 'worker', 'coordinate'))
    parser.add_argument('role', nargs='?', choices=(*HOSTS, 'server', 'coordinator'))
    args = parser.parse_args()
    if args.mode == 'register':
        register()
    else:
        globals()[args.mode](args.role)
