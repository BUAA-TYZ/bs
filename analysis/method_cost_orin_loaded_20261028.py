"""用原双流识别代理覆盖CPU重放；完成后在cohort边界停止负载。"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback

from analysis.method_cost_orin_suite_20261028 import ROOT, metadata, now, save, sha

FOLDER = ROOT / 'orin_sar_loaded'
STATE = ROOT / 'ORIN_LOADED_STATUS.json'
BENCHMARK = Path('/home/tianyz/bs_concurrency_20261016_2MTC8n/benchmark_jetson_concurrency.py')
FIXTURE = BENCHMARK.with_name('workload_fixture.json')
WEIGHTS = Path('/home/tianyz/bs_jetson_20261015_7TtC5t/yolo11n-obb.pt')


def read(path):
    return json.loads(path.read_text())


def load_worker():
    sys.path.insert(0, str(BENCHMARK.parent))
    spec = importlib.util.spec_from_file_location('original_sar_benchmark', BENCHMARK)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    args = argparse.Namespace(output=FOLDER/'sar_load', weights=WEIGHTS,
        workload_json=FIXTURE, mode='streams2', warmup=12, windows=64,
        duration=14400.0, profile=False)

    def finish_after_cohort(signum, frame):
        # 原循环每个cohort检查duration；负值使下一次检查退出并正常保存报告。
        args.duration = -1.0
        save(FOLDER/'load_stop_received.json', dict(at=now(), epoch_s=time.time()))

    signal.signal(signal.SIGUSR1, finish_after_cohort)
    raise SystemExit(module.worker(args))


def last_load_epoch():
    report = read(FOLDER/'sar_load/report.json')
    if not report.get('rows'):
        return 0.0
    return report['measurement_epoch_start_s'] + report['rows'][-1]['elapsed_wall_s']


def main():
    assert read(ROOT/'ORIN_QUEUE_STATUS.json')['status'] == 'UNLOADED_COMPLETE'
    FOLDER.mkdir(exist_ok=False)
    source_hashes = {str(p): sha(p) for p in (
        Path(__file__), BENCHMARK, BENCHMARK.with_name('calibrate_jetson_sar.py'),
        FIXTURE, WEIGHTS, Path('analysis/method_cost_replay_v2_20261028.py'),
        ROOT/'registration.json', ROOT/'REPLAY_CHECK_AMENDMENT.md')}
    verified = read(ROOT/'orin_unloaded_v2/input_hashes.json')
    for p, expected in verified.items():
        assert sha(p) == expected, p
    save(FOLDER/'registration.json', dict(at=now(), hashes=source_hashes,
        input_hashes=verified, groups=['fixed_cpu_t1','dynamic_cpu_t1','fixed_cpu_t2','dynamic_cpu_t2'],
        load='Original streams2 FP32 YOLO11n-OBB controlled SAR proxy, not SAR accuracy',
        stop='SIGUSR1 changes duration only; original benchmark exits at next cohort boundary',
        load_max_seconds=14400, cpu_group_max_seconds=7200,
        validity='All replay process lifetimes covered by active load rows; inspect tegrastats GPU activity'))
    save(FOLDER/'before.json', metadata())
    load = monitor = replay = None
    completed = []
    try:
        with (FOLDER/'sar_load.log').open('x') as log, (FOLDER/'tegrastats.log').open('x') as stats:
            monitor = subprocess.Popen(['tegrastats','--interval','100'], stdout=stats, stderr=subprocess.STDOUT)
            load = subprocess.Popen([sys.executable,'-B','-m', 'analysis.method_cost_orin_loaded_20261028','--load-worker'],
                                    stdout=log, stderr=subprocess.STDOUT)
            save(STATE, dict(status='WARMING_LOAD', pid=os.getpid(), load_pid=load.pid, at=now()))
            deadline = time.monotonic() + 600
            while True:
                assert load.poll() is None, 'SAR load exited before CPU replay'
                assert time.monotonic() < deadline, 'SAR load readiness timeout'
                try:
                    if last_load_epoch() > 0:
                        break
                except (FileNotFoundError, json.JSONDecodeError):
                    pass
                time.sleep(2)
            for threads in (1,2):
                for family in ('fixed','dynamic'):
                    group = f'{family}_cpu_t{threads}'
                    output = FOLDER/f'{group}.json'
                    entry = dict(group=group, started_at=now(), start_epoch_s=time.time(), load_pid=load.pid)
                    with (FOLDER/f'{group}.log').open('x') as replay_log:
                        replay = subprocess.Popen([sys.executable,'-B','-m','analysis.method_cost_replay_v2_20261028',
                            'replay','--family',family,'--device','cpu','--threads',str(threads),'--output',str(output)],
                            stdout=replay_log, stderr=subprocess.STDOUT)
                        save(STATE, dict(status='RUNNING', pid=os.getpid(), replay_pid=replay.pid,
                                         current=entry, completed=completed))
                        started = time.monotonic()
                        while replay.poll() is None:
                            assert load.poll() is None, 'SAR load ended during CPU measurement'
                            assert time.monotonic()-started < 7200, 'CPU group exceeded resource budget'
                            time.sleep(2)
                    entry.update(finished_at=now(), end_epoch_s=time.time(), returncode=replay.returncode)
                    assert replay.returncode == 0 and read(output)['status'] == 'PASS', group
                    entry['sha256'] = sha(output)
                    completed.append(entry)
                    save(STATE, dict(status='RUNNING', pid=os.getpid(), completed=completed))
            deadline = time.monotonic()+120
            while last_load_epoch() < completed[-1]['end_epoch_s']:
                assert load.poll() is None and time.monotonic()<deadline, 'Load rows do not cover final CPU sample'
                time.sleep(2)
            load.send_signal(signal.SIGUSR1)
            assert load.wait(timeout=120) == 0, 'Load failed during orderly stop'
            report = read(FOLDER/'sar_load/report.json')
            assert report['status'] == 'passed'
            for entry in completed:
                assert report['measurement_epoch_start_s'] <= entry['start_epoch_s']
                assert report['measurement_epoch_end_s'] >= entry['end_epoch_s']
            save(FOLDER/'after.json', metadata())
            save(STATE, dict(status='LOADED_COMPLETE', at=now(), pid=os.getpid(), completed=completed,
                             load_report_sha256=sha(FOLDER/'sar_load/report.json'),
                             coverage='Entire replay processes covered; GPU utilization audit pending'))
    except BaseException:
        save(STATE, dict(status='BLOCKED', at=now(), pid=os.getpid(), completed=completed,
                         traceback=traceback.format_exc()))
        raise
    finally:
        if replay is not None and replay.poll() is None:
            replay.terminate(); replay.wait(timeout=30)
        if load is not None and load.poll() is None:
            load.send_signal(signal.SIGUSR1)
            try:
                load.wait(timeout=120)
            except subprocess.TimeoutExpired:
                load.terminate(); load.wait(timeout=30)
        if monitor is not None:
            monitor.terminate(); monitor.wait(timeout=10)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--load-worker', action='store_true')
    if parser.parse_args().load_worker:
        load_worker()
    else:
        main()
