"""接管已运行的首组，之后持久串行执行剩余组；不重跑首组。"""
from datetime import datetime, timezone
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback
from analysis.method_cost_orin_suite_20261028 import ROOT, metadata, now, save, sha

FOLDER = ROOT/'orin_unloaded_v2'
STATE = ROOT/'ORIN_QUEUE_STATUS.json'
PARENT, WORKER, MONITOR = 767876, 767879, 767878


def command(pid):
    try:
        return Path(f'/proc/{pid}/cmdline').read_bytes().replace(b'\0', b' ').decode()
    except FileNotFoundError:
        return ''


def alive(pid):
    try:
        return Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()[0] != 'Z'
    except FileNotFoundError:
        return False


def main():
    assert 'method_cost_orin_suite_20261028 --name orin_unloaded_v2' in command(PARENT)
    assert 'method_cost_replay_v2_20261028 replay --family fixed --device cpu --threads 1' in command(WORKER)
    assert 'tegrastats --interval 100' in command(MONITOR)
    # 仅暂停空闲编排器；正在计时的worker与监控保持原进程连续执行。
    os.kill(PARENT, signal.SIGSTOP)
    save(ROOT/'ORIN_HANDOFF.json', dict(at=now(), old_parent=PARENT, adopted_worker=WORKER,
        adopted_monitor=MONITOR, guard_pid=os.getpid(), action='Pause waiting parent; preserve current measurement; no retry'))
    save(STATE, dict(status='RUNNING', current='fixed_cpu_t1', adopted=True, pid=os.getpid(), at=now()))
    while alive(WORKER):
        time.sleep(2)
    first = FOLDER/'fixed_cpu_t1.json'
    # 首组结束后才关闭旧编排器与其监控，避免与下一组发生竞争。
    if 'method_cost_orin_suite_20261028' in command(PARENT):
        os.kill(PARENT, signal.SIGKILL)
    if 'tegrastats --interval 100' in command(MONITOR):
        os.kill(MONITOR, signal.SIGTERM)
    from analysis.method_cost_20261028 import read
    assert first.exists() and read(first)['status'] == 'PASS', 'Adopted first group failed; preserve logs and stop'
    original = read(FOLDER/'status.json')['current']
    completed = [dict(**original, finished_at=now(), sha256=sha(first), adopted=True)]
    save(FOLDER/'status.json', dict(status='RUNNING_DURABLE_CONTINUATION', completed=completed))
    for device in ('cpu', 'cuda:0'):
        for threads in (1, 2):
            for family in ('fixed', 'dynamic'):
                if (device, threads, family) == ('cpu', 1, 'fixed'):
                    continue
                assert datetime.now(timezone.utc) < datetime(2026, 11, 5, 16, tzinfo=timezone.utc)
                name = f'{family}_{device.replace(":", "_")}_t{threads}'
                output = FOLDER/f'{name}.json'; assert not output.exists()
                entry = dict(group=name, started_at=now())
                save(STATE, dict(status='RUNNING', current=entry, completed=completed, pid=os.getpid()))
                with (FOLDER/f'{name}.tegrastats.log').open('x') as stats, (FOLDER/f'{name}.log').open('x') as log:
                    monitor = subprocess.Popen(['tegrastats', '--interval', '100'], stdout=stats, stderr=subprocess.STDOUT)
                    start = time.monotonic()
                    try:
                        result = subprocess.run([sys.executable, '-B', '-m', 'analysis.method_cost_replay_v2_20261028',
                            'replay', '--family', family, '--device', device, '--threads', str(threads), '--output', str(output)],
                            stdout=log, stderr=subprocess.STDOUT, timeout=max(1, (datetime(2026, 11, 5, 16, tzinfo=timezone.utc)-datetime.now(timezone.utc)).total_seconds()))
                    finally:
                        monitor.terminate(); monitor.wait(timeout=10)
                entry.update(finished_at=now(), wall_seconds=time.monotonic()-start, returncode=result.returncode)
                assert result.returncode == 0 and read(output)['status'] == 'PASS', name
                entry['sha256'] = sha(output); completed.append(entry)
                save(FOLDER/'status.json', dict(status='RUNNING', completed=completed))
    save(FOLDER/'after.json', metadata())
    save(FOLDER/'status.json', dict(status='COMPLETE', completed=completed, finished_at=now()))
    save(STATE, dict(status='UNLOADED_COMPLETE', completed=completed, pid=os.getpid(), at=now()))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        save(STATE, dict(status='BLOCKED', traceback=traceback.format_exc(), at=now(), pid=os.getpid()))
        raise
