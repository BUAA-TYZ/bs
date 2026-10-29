"""L精简正式测试：独立唯一开启、单池按批、冻结零样本模型。"""
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import multiprocessing
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback
from analysis import prepare_main_bc_stage2 as preparation
from analysis import run_scale_validation_20261027 as engine
from analysis import run_l_reduced_validation_20261028 as reduced
from analysis.run_m_formal_20261027 import L_METHODS, DATES, END, check_time
from analysis.run_dynamic_heuristics_v2 import read, sha
from analysis.run_dynamic_learning import write_json

MAIN = engine.MAIN
ROOT = MAIN/'l_formal_test_20261028'
OPEN = MAIN/'L_TEST_OPENED.json'
PROTOCOL = MAIN/'L_FORMAL_TEST_PROTOCOL_20261028.md'
AUDIT = reduced.ROOT/'independent_audit_20261028'
METHODS = L_METHODS


def verify_manifest():
    m = read(ROOT/'manifest.json')
    for path, expected in m['hashes'].items():
        assert sha(path) == expected, path
    return m


def require_open(date):
    assert date in DATES
    opening = read(OPEN)
    assert opening['scope'] == 'L' and opening['manifest_sha256'] == sha(ROOT/'manifest.json')


def source_windows(args):
    require_open(args[-1]); preparation.permitted = require_open
    return preparation.source_windows(args)


def install_adapter(method):
    reduced.install_adapter(method)
    engine.ROOT = ROOT; engine.VALID = DATES; engine.METHODS = METHODS
    engine.check_time = check_time; engine.verify_manifest = verify_manifest


def simulate(args):
    try:
        path, method = args
        require_open(read(path)['date']); assert method in METHODS
        install_adapter(method)
        short = engine.simulate(args)
        if short['status'] == 'PASS':
            output = Path(short['path']); row = read(output)
            row.update(tests_sealed=False, stage='L_FORMAL_TEST', opening_sha256=sha(OPEN),
                       information_group='global' if method.startswith('global_') else 'local')
            write_json(output, row); short['sha256'] = sha(output)
            write_json(output.with_suffix('.progress.json'), short)
        return short
    except Exception:
        return dict(status='FAILED', job=args, traceback=traceback.format_exc())


def prepare():
    assert not ROOT.exists() and not OPEN.exists()
    check_time(); engine.verify_main()
    old = reduced.verify_manifest(); audit = read(AUDIT/'FINAL_AUDIT.json')
    assert audit['status'] == 'PASS' and audit['records'] == 42
    assert not any(audit['finetune_diagnostic']['needs'].values())
    assert read(Path('results/method_cost_20261028/queue_status.json'))['status'] == 'MAC_CAPTURE_COMPLETE_ORIN_REPLAY_PENDING'
    hashes = dict(old['hashes'])
    for path, expected in read(AUDIT/'REPORT_HASHES.json').items():
        assert sha(path) == expected, path
        hashes[path] = expected
    for path in (Path(__file__), PROTOCOL, Path('analysis/audit_l_reduced_20261028.py'),
                 Path('tests/test_l_formal_20261028.py'), MAIN/'COMPARISON_SCOPE_AMENDMENT_20261026.md',
                 MAIN/'M_TEST_PRIORITY_AMENDMENT_20261027.md', reduced.ROOT/'manifest.json',
                 reduced.ROOT/'L_VALIDATION_NOTIFIED.json', MAIN/'scale_validation_20261027/config_template.json'):
        hashes[str(path)] = sha(path)
    for path, expected in hashes.items():
        assert sha(path) == expected, path
    write_json(ROOT/'config_template.json', read(MAIN/'scale_validation_20261027/config_template.json'))
    hashes[str(ROOT/'config_template.json')] = sha(ROOT/'config_template.json')
    write_json(ROOT/'manifest.json', dict(hashes=hashes, models=old['models'], ranges=old['ranges'],
        methods=METHODS, dates=DATES, workers=9, torch_threads=1, score_duration_s=3600,
        zero_shot=True, no_retry=True, deadline=END.isoformat(),
        comparison_scopes_status='INCOMPLETE_NOT_DETERMINABLE',
        comparison_scopes=['fixed_six_local_16', 'dynamic_twelve_local_4_16'],
        bootstrap=dict(unit='paired day', draws=10000, seed=20261027, confidence=.95),
        no_finetune_reason='All six validation R600 values <= retained local-rule minimum; no trigger',
        input_range_sha256=sha(old['ranges'])))
    print('PREPARED', sha(ROOT/'manifest.json'))


def open_once():
    check_time(); m = verify_manifest()
    assert read(reduced.ROOT/'final_status.json')['status'] == 'COMPLETE'
    assert read(reduced.ROOT/'L_VALIDATION_NOTIFIED.json')['status'] == 'NOTIFIED'
    assert not any(read(AUDIT/'FINAL_AUDIT.json')['finetune_diagnostic']['needs'].values())
    assert read(Path('results/method_cost_20261028/queue_status.json'))['status'] == 'MAC_CAPTURE_COMPLETE_ORIN_REPLAY_PENDING'
    with OPEN.open('x') as stream:
        json.dump(dict(scope='L', dates=m['dates'], at=datetime.now(timezone.utc).isoformat(),
            manifest_sha256=sha(ROOT/'manifest.json'), protocol_sha256=sha(PROTOCOL),
            user_authorization='2026-10-27: reduced L validation followed by separately opened L test'), stream, indent=2)


def batches(pool, tasks):
    results = []
    for offset in range(0, len(tasks), 9):
        check_time(); current = []
        submitted = {pool.submit(simulate, task):task for task in tasks[offset:offset+9]}
        for future in as_completed(submitted):
            try:
                row = future.result()
            except Exception:
                row = dict(status='FAILED', job=submitted[future], traceback=traceback.format_exc())
            current.append(row); results.append(row)
            write_json(ROOT/'progress.json', dict(stage='test', done=len(results), total=len(tasks), results=results))
        if any(r['status'] != 'PASS' for r in current):
            return results
    return results


def panel():
    started = time.perf_counter(); results = []
    try:
        m = verify_manifest(); open_once()
        cfg = read(ROOT/'config_template.json'); targets = read(MAIN/'targets_maritime80.json'); protocols = []
        with ProcessPoolExecutor(max_workers=9, mp_context=multiprocessing.get_context('spawn')) as pool:
            for date in DATES:
                require_open(date)
                futures = [pool.submit(source_windows, (sid, cfg['topology']['tle_lines'][:48], targets, date)) for sid in range(48)]
                windows = []
                for i, future in enumerate(as_completed(futures), 1):
                    windows.extend(future.result()); check_time()
                    write_json(ROOT/'progress.json', dict(stage='acquisition', date=date, done=i, total=48))
                windows.sort(key=lambda w:(w['start_s'], w['source_sat_id'], w['target_id']))
                for i, w in enumerate(windows):
                    w['window_id'] = i
                preparation.permitted = require_open
                events = preparation.arrivals(windows, date); folder = ROOT/'data'/date
                write_json(folder/'windows.json', windows); write_json(folder/'arrivals.json', events)
                selected = preparation.peak_window([e['ready_s'] for e in events], [e['raw_bytes'] for e in events], 3600)
                engine.VALID = DATES
                path = folder/'hour_1/protocol.json'
                p = engine.build_episode(cfg, date, events, selected, path.parent,
                    {**m['hashes'], str(OPEN):sha(OPEN)}, [folder/'windows.json', folder/'arrivals.json'])
                p.update(tests_sealed=False, stage='L_FORMAL_TEST', opening_sha256=sha(OPEN))
                write_json(path, p); engine.verify(p); protocols.append(str(path))
            write_json(ROOT/'run_manifest.json', dict(protocols=protocols, methods=METHODS,
                hashes={p:sha(p) for p in protocols+[str(ROOT/'manifest.json'), str(OPEN)]},
                tests_sealed=False, generation_wall_seconds=time.perf_counter()-started))
            results = batches(pool, [(p, n) for n in METHODS for p in protocols])
        passed = len(results) == 42 and all(r['status'] == 'PASS' for r in results)
        verify_manifest()
        write_json(ROOT/'final_status.json', dict(status='COMPLETE' if passed else 'BLOCKED', results=results,
            tests_sealed=False, independent_report_pending=True, wall_seconds=time.perf_counter()-started))
    except Exception:
        write_json(ROOT/'final_status.json', dict(status='BLOCKED', results=results, traceback=traceback.format_exc(),
            tests_sealed=not OPEN.exists()))
        raise


def supervise():
    verify_manifest()
    command = [sys.executable, '-B', '-m', 'analysis.run_l_formal_20261028', 'panel']
    with (ROOT/'panel.log').open('xb') as log:
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    awake = subprocess.Popen(['/usr/bin/caffeinate', '-i', '-w', str(child.pid)], start_new_session=True)
    identity = subprocess.check_output(['ps', '-p', str(child.pid), '-o', 'pid=,lstart=,command='], text=True).strip()
    write_json(ROOT/'launch.json', dict(pid=child.pid, supervisor_pid=os.getpid(), caffeinate_pid=awake.pid,
        command=command, identity=identity, started_at=datetime.now(timezone.utc).isoformat(), manifest_sha256=sha(ROOT/'manifest.json')))
    while child.poll() is None:
        if datetime.now(timezone.utc) >= END:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL); child.wait()
            write_json(ROOT/'supervisor_failure.json', dict(status='DEADLINE', returncode=child.returncode))
            return
        time.sleep(2)
    if child.returncode or not (ROOT/'final_status.json').exists():
        write_json(ROOT/'supervisor_failure.json', dict(status='BLOCKED', returncode=child.returncode))


def launch():
    check_time(); verify_manifest(); assert not (ROOT/'launch_request.json').exists() and not OPEN.exists()
    with (ROOT/'supervisor.log').open('xb') as log:
        child = subprocess.Popen([sys.executable, '-B', '-m', 'analysis.run_l_formal_20261028', 'supervise'],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    write_json(ROOT/'launch_request.json', dict(supervisor_pid=child.pid, at=datetime.now(timezone.utc).isoformat()))
    print('SUPERVISOR', child.pid)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('mode', choices=['prepare', 'launch', 'panel', 'supervise'])
    globals()[parser.parse_args().mode]()
