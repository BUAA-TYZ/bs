"""Orin串行测量编排，保留每组日志、功耗时钟及原始结果。"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path('results/method_cost_20261028')


def now():
    return datetime.now(timezone.utc).isoformat()


def save(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def metadata():
    clocks = {}
    for pattern in ('/sys/devices/system/cpu/cpu*/cpufreq/scaling_*',
                    '/sys/class/devfreq/*/cur_freq', '/sys/class/devfreq/*/min_freq',
                    '/sys/class/devfreq/*/max_freq', '/sys/class/devfreq/*/governor'):
        import glob
        for filename in glob.glob(pattern):
            try:
                clocks[filename] = Path(filename).read_text().strip()
            except (OSError, UnicodeError):
                pass
    power = subprocess.run(['/usr/sbin/nvpmodel', '-q'], capture_output=True, text=True)
    return dict(at=now(), power=power.stdout, power_error=power.stderr,
                power_returncode=power.returncode, clocks=clocks)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', default='orin_unloaded')
    parser.add_argument('--cpu-only', action='store_true')
    parser.add_argument('--load-pid', type=int)
    parser.add_argument('--replay-module', default='analysis.method_cost_20261028')
    args = parser.parse_args()
    folder = ROOT / args.name
    folder.mkdir(exist_ok=False)
    reg = json.loads((ROOT/'registration.json').read_text())
    verified = {}
    for name, expected in reg['hashes'].items():
        if name.startswith(('analysis/', 'sim/')) or name in reg['models'].values():
            assert sha(name) == expected, name
            verified[name] = expected
    for family in ('fixed', 'dynamic'):
        result = ROOT/'capture'/family/'result.json'
        capture = json.loads(result.read_text())
        assert capture['status'] == 'PASS'
        assert capture['registration_sha256'] == sha(ROOT/'registration.json')
        verified[str(result)] = sha(result)
        config = ROOT/'capture'/family/'config.json'
        verified[str(config)] = sha(config)
        for name, expected in capture['decision_hashes'].items():
            assert sha(name) == expected, name
            verified[name] = expected
    save(folder/'input_hashes.json', verified)
    save(folder/'before.json', metadata())
    groups = []
    for device in (['cpu'] if args.cpu_only else ['cpu', 'cuda:0']):
        for threads in (1, 2):
            for family in ('fixed', 'dynamic'):
                name = f'{family}_{device.replace(":", "_")}_t{threads}'
                output = folder/f'{name}.json'
                if args.load_pid:
                    os.kill(args.load_pid, 0)
                entry = dict(group=name, started_at=now(), load_pid=args.load_pid)
                save(folder/'status.json', dict(status='RUNNING', current=entry, completed=groups))
                with (folder/f'{name}.tegrastats.log').open('w') as stats, (folder/f'{name}.log').open('w') as log:
                    monitor = subprocess.Popen(['tegrastats', '--interval', '100'], stdout=stats, stderr=subprocess.STDOUT)
                    start = time.monotonic()
                    try:
                        process = subprocess.run([sys.executable, '-B', '-m', args.replay_module,
                            'replay', '--family', family, '--device', device, '--threads', str(threads),
                            '--output', str(output)], stdout=log, stderr=subprocess.STDOUT)
                    finally:
                        monitor.terminate()
                        monitor.wait(timeout=10)
                entry.update(finished_at=now(), wall_seconds=time.monotonic()-start,
                             returncode=process.returncode)
                if args.load_pid:
                    os.kill(args.load_pid, 0)
                groups.append(entry)
                if process.returncode:
                    save(folder/'status.json', dict(status='BLOCKED', completed=groups))
                    raise SystemExit(process.returncode)
                assert json.loads(output.read_text())['status'] == 'PASS'
                entry['sha256'] = sha(output)
                print(name, 'PASS', flush=True)
    save(folder/'after.json', metadata())
    save(folder/'status.json', dict(status='COMPLETE', completed=groups, finished_at=now()))


if __name__ == '__main__':
    main()
