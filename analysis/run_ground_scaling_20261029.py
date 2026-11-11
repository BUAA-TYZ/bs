"""已授权地面站扩展补充评估：先冻结登记，后派生配置，单池逐批执行。"""
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import ExitStack
from copy import deepcopy
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
from unittest.mock import patch

from analysis import run_scale_validation_20261027 as engine
from analysis import scaled_ground_adapter_20261029 as adapter
from analysis.run_dynamic_heuristics_v2 import read, sha, digest, configuration
from analysis.run_dynamic_learning import write_json
from analysis.run_local_comparison_20261027 import BASELINES, policy_set, check_record
from analysis.run_m_formal_20261027 import MAPPO, DATES

MAIN = engine.MAIN
ROOT = MAIN/'ground_scaling_20261029'
PROTOCOL = MAIN/'GROUND_SCALING_FORMAL_TEST_PROTOCOL_20261029.md'
END = datetime(2026, 11, 4, 16, tzinfo=timezone.utc)
METHODS = MAPPO+BASELINES+('global_v3_16', 'global_v3_dynamic')
SCOPES = dict(fixed_six_local_16=[n for n in BASELINES if n.endswith('_16')],
              dynamic_twelve_local_4_16=list(BASELINES))
ORIGINALS = dict(S=MAIN/'s_formal_test_20261029', M=MAIN/'m_formal_test_20261027',
                 L=MAIN/'l_formal_test_20261028')
STAGES = ('L_single_completion', 'L_scaled', 'M_scaled')
STAGE_BUDGET_HOURS = dict(L_single_completion=58., L_scaled=46., M_scaled=36.)
# 采用同一篇已发表研究表2的成套坐标；坐标文献差异在登记中披露。
NEW_STATIONS = [dict(id='gs_kashgar', lat_deg=39.5051, lon_deg=75.99287, alt_m=1307),
                dict(id='gs_sanya', lat_deg=18.3121, lon_deg=109.3099, alt_m=22)]


def check_time():
    if datetime.now(timezone.utc) >= END:
        raise TimeoutError('Before November 5 Beijing cutoff; no extension')


def verify_manifest():
    m = read(ROOT/'manifest.json')
    for path, expected in m['hashes'].items():
        assert sha(path) == expected, path
    return m


def estimate():
    # 只使用历史墙钟和方法清单，不读取指标决定预算或优先级。
    evidence = {}; estimates = {}
    for scale, root in ORIGINALS.items():
        path = root/('simulation_status.json' if scale == 'M' else 'final_status.json')
        state = read(path); evidence[str(path)] = sha(path)
        wall = state['wall_seconds']
        estimates[scale] = dict(old_tasks=len(state['results']), old_hours=wall/3600)
    estimates['L']['scaled_budget_hours'] = 40.
    estimates['L']['missing_control_budget_hours'] = 12.
    estimates['M']['scaled_budget_hours'] = 30.
    estimates['S']['scaled_budget_hours'] = 0.
    return dict(evidence=evidence, scales=estimates, total_budget_hours=88.,
        audit_budget_hours=6., workers=9, priority=['L', 'M', 'S'],
        reason='Historical wall time plus conservative multi-station overhead; S identical single station reused; no metric-based selection',
        deadline=END.isoformat())


def prepare():
    check_time(); assert not ROOT.exists(), 'No overwrite or second registration'
    budget = estimate()
    assert (END-datetime.now(timezone.utc)).total_seconds()/3600 > budget['total_budget_hours'], 'Budget needs explicit scope reduction before registration'
    old = read(ORIGINALS['L']/'manifest.json')
    hashes = dict(old['hashes'])
    sources = {}; reused = {}; originals = {}
    for scale, root in ORIGINALS.items():
        manifest = read(root/'manifest.json'); run = read(root/'run_manifest.json')
        hashes.update(manifest['hashes']); hashes.update(run['hashes'])
        originals[scale] = {read(p)['date']:p for p in run['protocols']}
        reused[scale] = {date:{} for date in DATES}
        for date in DATES:
            p = read(originals[scale][date]); engine.verify(p)
            for method in METHODS:
                path = root/'validation'/date/f'{method}.json'
                if method not in run['methods']:
                    continue
                row = read(path); check_record(row, p)
                assert row['config_sha256'] == digest(configuration(p, method))
                reused[scale][date][method] = str(path); hashes[str(path)] = sha(path)
        sources[scale] = str(root)
    missing = [n for n in METHODS if n not in reused['L'][DATES[0]]]
    assert len(missing) == 9 and all(n in BASELINES for n in missing)
    files = [Path(__file__), Path(adapter.__file__), PROTOCOL,
        Path('analysis/report_ground_scaling_20261029.py'), Path('analysis/verify_ground_scaling_20261029.py'),
        Path('analysis/check_ground_scaling_geometry_20261029.py'),
        Path('results/ground_scaling_preflight_20261029/GEOMETRY.json'),
        Path('tests/test_ground_scaling_20261029.py'), Path('results/ground_scaling_preflight_20261029/TESTS.json')]
    for p in files:
        hashes[str(p)] = sha(p)
    for p, expected in hashes.items():
        assert sha(p) == expected, p
    assert read(files[-1])['status'] == 'PASS'
    assert read('results/ground_scaling_preflight_20261029/GEOMETRY.json')['status'] == 'PASS'
    ROOT.mkdir()
    registration = dict(at=datetime.now(timezone.utc).isoformat(), protocol_sha256=sha(PROTOCOL),
        hashes=hashes, models=old['models'], ranges=old['ranges'], methods=METHODS, dates=DATES,
        originals=originals, reused=reused, missing_L=missing, new_stations=NEW_STATIONS,
        stages=STAGES, workers=9, torch_threads=1, deadline=END.isoformat(), estimate=budget,
        comparison_scopes=SCOPES, bootstrap=dict(draws=10000, seed=20261027, confidence=.95),
        ground_compute='single shared FIFO, original total rate unchanged',
        no_retry=True, zero_shot=True, no_training=True, no_model_selection=True,
        note='Same dates and matched original tasks; scene variants, not independent test dates')
    with (ROOT/'PREGEN_REGISTRATION.json').open('x') as f:
        json.dump(registration, f, indent=2)
    registration['hashes'] = {**hashes, str(ROOT/'PREGEN_REGISTRATION.json'):sha(ROOT/'PREGEN_REGISTRATION.json')}
    write_json(ROOT/'manifest.json', registration)
    print('REGISTERED', sha(ROOT/'manifest.json'))


def derive_protocol(p, scale, scaled):
    q = deepcopy(p)
    if scaled:
        first = q['config']['ground_stations'][0]
        assert len(q['config']['ground_stations']) == 1
        q['config']['ground_stations'].extend({**first, **g} for g in NEW_STATIONS[:{'S':0,'M':1,'L':2}[scale]])
    assert all(q['config'][k] == v for k,v in p['config'].items() if k != 'ground_stations')
    assert q['events'] == p['events'] and q['scored_task_ids'] == p['scored_task_ids']
    q['config_sha256'] = digest(q['config'])
    q.update(stage='GROUND_SCALING_FORMAL_TEST', tests_sealed=False)
    return q


def generate():
    m = verify_manifest(); check_time()
    with (ROOT/'TEST_OPENED.json').open('x') as f:
        json.dump(dict(at=datetime.now(timezone.utc).isoformat(), manifest_sha256=sha(ROOT/'manifest.json'),
            protocol_sha256=sha(PROTOCOL), same_dates_variant=True), f, indent=2)
    all_stages = {}
    for stage in STAGES:
        scale = stage[0]; scaled = stage.endswith('_scaled'); folder = ROOT/stage
        paths = []
        for date in DATES:
            original = m['originals'][scale][date]
            q = derive_protocol(read(original), scale, scaled)
            q.update(original_protocol=original, original_protocol_sha256=sha(original))
            q['input_hashes'].update({**m['hashes'], str(ROOT/'manifest.json'):sha(ROOT/'manifest.json'),
                str(ROOT/'TEST_OPENED.json'):sha(ROOT/'TEST_OPENED.json')})
            p = folder/'data'/date/'protocol.json'; write_json(p, q); engine.verify(q); paths.append(str(p))
        methods = METHODS if scaled else m['missing_L']
        run = dict(protocols=paths, methods=methods, hashes={p:sha(p) for p in paths},
            scale=scale, setting='scaled' if scaled else 'single')
        write_json(folder/'run_manifest.json', run); all_stages[stage] = run
    write_json(ROOT/'run_manifest.json', all_stages)
    return all_stages


def simulate(job):
    stage, path, method = job
    try:
        m = verify_manifest(); opening = read(ROOT/'TEST_OPENED.json')
        assert opening['manifest_sha256'] == sha(ROOT/'manifest.json')
        assert stage in STAGES and method in read(ROOT/stage/'run_manifest.json')['methods']
        with ExitStack() as stack:
            changes = dict(ROOT=ROOT/stage, VALID=DATES, METHODS=METHODS, LEARNED=MAPPO,
                BASELINES=BASELINES, policy_set=policy_set, check_time=check_time, verify_manifest=lambda:m)
            for key, value in changes.items():
                stack.enter_context(patch.object(engine, key, value))
            if stage.endswith('_scaled'):
                stack.enter_context(adapter.installed(engine))
            else:
                stack.enter_context(patch.object(engine, 'TrafficAudit', adapter.StationTraffic))
                stack.enter_context(patch.object(engine, 'resources', adapter.resources))
            short = engine.simulate((path, method))
        if short['status'] == 'PASS':
            row = read(short['path'])
            row.update(tests_sealed=False, scale=stage[0], setting='scaled' if stage.endswith('_scaled') else 'single',
                stage=stage, opening_sha256=sha(ROOT/'TEST_OPENED.json'),
                adapter_sha256=sha(adapter.__file__), original_protocol=read(path)['original_protocol'])
            write_json(Path(short['path']), row); short['sha256'] = sha(short['path'])
            write_json(Path(short['path']).with_suffix('.progress.json'), short)
        return dict(short, stage=stage)
    except Exception:
        return dict(status='FAILED', job=job, traceback=traceback.format_exc(), stage=stage)


def batches(pool, jobs, results):
    for offset in range(0, len(jobs), 9):
        check_time(); current = []
        submitted = {pool.submit(simulate, job):job for job in jobs[offset:offset+9]}
        for future in as_completed(submitted):
            try:
                row = future.result()
            except Exception:
                row = dict(status='FAILED', job=submitted[future], traceback=traceback.format_exc())
            current.append(row); results.append(row)
            write_json(ROOT/'progress.json', dict(stage=jobs[offset][0], done=len(results), total=147, results=results))
        if any(r['status'] != 'PASS' for r in current):
            return False
    return True


def panel():
    started = time.perf_counter(); results = []
    try:
        runs = generate(); ok = True; skipped = []
        with ProcessPoolExecutor(max_workers=9, mp_context=multiprocessing.get_context('spawn')) as pool:
            for stage in STAGES:
                remaining = (END-datetime.now(timezone.utc)).total_seconds()/3600
                if remaining < STAGE_BUDGET_HOURS[stage]:
                    skipped = list(STAGES[STAGES.index(stage):])
                    write_json(ROOT/'WALLCLOCK_SCOPE_REDUCTION.json', dict(at=datetime.now(timezone.utc).isoformat(),
                        remaining_hours=remaining, required_hours=STAGE_BUDGET_HOURS[stage], skipped=skipped,
                        reason='Registered wall-clock budget only; no performance-based selection'))
                    break
                run = runs[stage]
                jobs = [(stage, p, n) for n in run['methods'] for p in run['protocols']]
                if not batches(pool, jobs, results):
                    ok = False; break
        verify_manifest()
        write_json(ROOT/'final_status.json', dict(status=('PARTIAL' if skipped else 'COMPLETE') if ok else 'BLOCKED',
            results=results, skipped=skipped, wall_seconds=time.perf_counter()-started))
        if ok:
            emit_report()
    except Exception:
        target = 'report_failure.json' if (ROOT/'final_status.json').exists() else 'final_status.json'
        write_json(ROOT/target, dict(status='BLOCKED', traceback=traceback.format_exc(), results=results))
        raise


def emit_report():
    from analysis.report_ground_scaling_20261029 import main as report
    from analysis.verify_ground_scaling_20261029 import main as audit
    report(); audit()
    write_json(ROOT/'REPORT_COMPLETE.json', dict(status='PASS', completion=read(ROOT/'final_status.json')['status'],
        at=datetime.now(timezone.utc).isoformat(), report_hashes_sha256=sha(ROOT/'report/REPORT_HASHES.json'),
        independent_audit_sha256=sha(ROOT/'report/INDEPENDENT_AUDIT.json')))


def supervise():
    verify_manifest()
    command = [sys.executable, '-B', '-m', 'analysis.run_ground_scaling_20261029', 'panel']
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
    if child.returncode:
        write_json(ROOT/'supervisor_failure.json', dict(status='BLOCKED', returncode=child.returncode))


def launch():
    check_time(); verify_manifest()
    assert not (ROOT/'launch_request.json').exists() and not (ROOT/'TEST_OPENED.json').exists()
    processes = subprocess.check_output(['ps', '-axo', 'command='], text=True).splitlines()
    assert not any((' -m analysis.run_' in c and c.rstrip().endswith((' panel',' supervise'))) or
                   'multiprocessing.spawn import spawn_main' in c for c in processes), 'Another experiment is active'
    with (ROOT/'launch_request.json').open('x') as stream:
        json.dump(dict(at=datetime.now(timezone.utc).isoformat()), stream)
    with (ROOT/'supervisor.log').open('xb') as log:
        child = subprocess.Popen([sys.executable, '-B', '-m', 'analysis.run_ground_scaling_20261029', 'supervise'],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    print('SUPERVISOR', child.pid)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare','launch','panel','supervise'])
    globals()[parser.parse_args().mode]()
