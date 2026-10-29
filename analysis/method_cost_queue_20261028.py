"""等待当前L池正常退出后，单进程顺序录制，避免争抢实验CPU。"""
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

from analysis.method_cost_20261028 import ROOT, MAIN, read, write, verify_registration


def run():
    verify_registration()
    lroot = MAIN/'l_reduced_validation_20261028'
    launch = read(lroot/'launch.json')
    deadline = datetime(2026, 11, 5, 16, tzinfo=timezone.utc)
    write(ROOT/'queue_launch.json', dict(pid=os.getpid(), started_at=datetime.now(timezone.utc).isoformat(),
        command='analysis.method_cost_queue_20261028', waits_for=launch,
        execution='Two sequential single-process Mac recordings, no pool'))
    try:
        while True:
            if datetime.now(timezone.utc) >= deadline:
                raise TimeoutError('Overall hard deadline')
            result = subprocess.run(['ps', '-p', str(launch['pid']), '-o', 'command='], capture_output=True, text=True)
            alive = result.returncode == 0 and 'analysis.run_l_reduced_validation_20261028 panel' in result.stdout
            if not alive:
                assert (lroot/'final_status.json').exists(), 'L exited without final status'
                assert read(lroot/'final_status.json')['status'] == 'COMPLETE', 'L did not complete normally'
                workers = subprocess.check_output(['ps', '-axo', 'ppid=,command='], text=True)
                assert not any(line.split(maxsplit=1)[0] == str(launch['pid'])
                    and 'multiprocessing.spawn' in line for line in workers.splitlines() if line.strip()), 'L workers still draining'
                break
            write(ROOT/'queue_status.json', dict(status='WAITING_FOR_L', observed_at=datetime.now(timezone.utc).isoformat()))
            time.sleep(20)
        for family in ('fixed', 'dynamic'):
            write(ROOT/'queue_status.json', dict(status='MAC_CAPTURE', family=family))
            with (ROOT/f'mac_{family}.log').open('xb') as log:
                child = subprocess.Popen([sys.executable, '-B', '-m', 'analysis.method_cost_20261028',
                    'capture', '--family', family], stdout=log, stderr=subprocess.STDOUT)
                write(ROOT/'capture_launch.json', dict(pid=child.pid, family=family,
                    started_at=datetime.now(timezone.utc).isoformat(), concurrency=1))
                while child.poll() is None:
                    if datetime.now(timezone.utc) >= deadline:
                        child.terminate()
                        try:
                            child.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            child.kill(); child.wait()
                        raise TimeoutError('Capture terminated at overall deadline')
                    time.sleep(5)
                assert child.returncode == 0, f'{family} capture exited {child.returncode}; no retry'
        write(ROOT/'queue_status.json', dict(status='MAC_CAPTURE_COMPLETE_ORIN_REPLAY_PENDING',
            finished_at=datetime.now(timezone.utc).isoformat()))
    except Exception:
        write(ROOT/'queue_status.json', dict(status='BLOCKED', traceback=traceback.format_exc()))
        raise


if __name__ == '__main__':
    run()
