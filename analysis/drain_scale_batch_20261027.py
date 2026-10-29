"""按用户指令收尾已运行的九项；不修改冻结的实验入口。"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path('results/redesign_20261021/main_scenario/scale_validation_20261027')


def read(path):
    return json.loads(path.read_text())


def write(path, value):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2)+'\n')
    temp.replace(path)


def identity(pid):
    return subprocess.check_output(['ps', '-p', str(pid), '-o', 'pid=,lstart=,command='], text=True).strip()


def guard():
    request = read(ROOT/'DRAIN_REQUEST.json')
    assert identity(request['coordinator']) == request['coordinator_identity']
    for job in request['jobs']:
        assert identity(job['pid']) == job['identity']
    # 暂停父进程继续入队；旧 executor 已预取的任务仍需逐 worker 收尾防护。
    os.kill(request['coordinator'], signal.SIGSTOP)
    write(ROOT/'DRAIN_GUARD.json', dict(pid=os.getpid(), status='DRAINING', started_at=datetime.now(timezone.utc).isoformat()))
    pending = list(request['jobs'])
    done = []
    while pending:
        for job in pending[:]:
            output = Path(job['output'])
            progress = Path(job['progress'])
            if not output.exists() and read(progress).get('status') == 'RUNNING':
                continue
            assert identity(job['pid']) == job['identity']
            os.kill(job['pid'], signal.SIGSTOP)
            row = read(output) if output.exists() else read(progress)
            done.append(dict(**job, status=row['status'], sha256=hashlib.sha256(output.read_bytes()).hexdigest() if output.exists() else None))
            pending.remove(job)
            write(ROOT/'DRAIN_GUARD.json', dict(pid=os.getpid(), status='DRAINING', done=done, remaining=len(pending)))
        if pending:
            time.sleep(.01)
    expected = {job['progress'] for job in request['jobs']}
    unexpected = [str(p) for p in ROOT.glob('validation/*/*.progress.json') if str(p) not in expected]
    # 监督和旧队列统一退场；已落盘结果与旧 progress 原样保留。
    assert identity(request['supervisor']) == request['supervisor_identity']
    os.kill(request['supervisor'], signal.SIGTERM)
    os.killpg(request['coordinator'], signal.SIGKILL)
    time.sleep(1)
    write(ROOT/'PAUSED_BY_USER.json', dict(status='PAUSED_BY_USER' if not unexpected and all(j['status']=='PASS' for j in done) else 'REVIEW_REQUIRED',
        at=datetime.now(timezone.utc).isoformat(), reason='M formal test prioritized by user; current nine only',
        results=done, unexpected_progress=unexpected, original_manifest_sha256=request['manifest_sha256'],
        queued_tasks_cancelled=True, old_coordinator_must_not_resume=True))


def launch():
    assert not (ROOT/'DRAIN_REQUEST.json').exists()
    launch = read(ROOT/'launch.json')
    assert identity(launch['pid']) == launch['identity']
    jobs = []
    for path in sorted(ROOT.glob('validation/*/*.progress.json')):
        row = read(path)
        assert row['status'] == 'RUNNING' and row['method'].startswith('fixed_')
        jobs.append(dict(pid=row['pid'], identity=identity(row['pid']), progress=str(path), output=str(path).replace('.progress.json', '.json')))
    assert len(jobs) == 9 and len({j['pid'] for j in jobs}) == 9
    write(ROOT/'DRAIN_REQUEST.json', dict(coordinator=launch['pid'], coordinator_identity=launch['identity'],
        supervisor=launch['supervisor_pid'], supervisor_identity=identity(launch['supervisor_pid']), jobs=jobs,
        manifest_sha256=launch['manifest_sha256'], requested_at=datetime.now(timezone.utc).isoformat()))
    with (ROOT/'drain.log').open('xb') as log:
        proc = subprocess.Popen([sys.executable, '-B', '-m', 'analysis.drain_scale_batch_20261027', 'guard'],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    print('DRAIN_GUARD', proc.pid)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['launch', 'guard'])
    globals()[parser.parse_args().mode]()
