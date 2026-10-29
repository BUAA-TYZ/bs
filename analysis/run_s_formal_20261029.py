"""S正式测试：先登记后生成、完整规则包络、冻结零样本模型。"""
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
from analysis.run_local_comparison_20261027 import BASELINES, policy_set
from analysis.run_m_formal_20261027 import MAPPO
from analysis.estimate_s_formal_20261029 import OUTPUT as ESTIMATE, END
from analysis.run_m_formal_20261027 import DATES
from analysis.run_dynamic_heuristics_v2 import read, sha
from analysis.run_dynamic_learning import write_json

MAIN = engine.MAIN
ROOT = MAIN/'s_formal_test_20261029'
OPEN = MAIN/'S_TEST_OPENED.json'
PROTOCOL = MAIN/'S_FORMAL_TEST_PROTOCOL_20261029.md'
METHODS = MAPPO+BASELINES+('global_v3_16', 'global_v3_dynamic')
SCOPES = dict(fixed_six_local_16=[n for n in BASELINES if n.endswith('_16')],
              dynamic_twelve_local_4_16=list(BASELINES))
SAR = 12


def check_time():
    if datetime.now(timezone.utc) >= END:
        raise TimeoutError('S cutoff: 2026-11-05 00:00 Beijing; no extension')


def verify_manifest():
    m = read(ROOT/'manifest.json')
    for path, expected in m['hashes'].items():
        assert sha(path) == expected, path
    return m


def require_open(date):
    assert date in DATES
    opening = read(OPEN)
    assert opening['scope'] == 'S' and opening['manifest_sha256'] == sha(ROOT/'manifest.json')


def source_windows(args):
    require_open(args[-1]); preparation.permitted = require_open
    return preparation.source_windows(args)


def install_adapter(method):
    assert method in METHODS
    engine.ROOT = ROOT; engine.VALID = DATES; engine.METHODS = METHODS
    engine.LEARNED = MAPPO; engine.BASELINES = BASELINES; engine.policy_set = policy_set
    engine.check_time = check_time; engine.verify_manifest = verify_manifest


def simulate(args):
    try:
        path, method = args
        require_open(read(path)['date']); assert method in METHODS
        install_adapter(method)
        short = engine.simulate(args)
        if short['status'] == 'PASS':
            output = Path(short['path']); row = read(output)
            row.update(tests_sealed=False, stage='S_FORMAL_TEST', opening_sha256=sha(OPEN),
                       information_group='global' if method.startswith('global_') else 'local')
            write_json(output, row); short['sha256'] = sha(output)
            write_json(output.with_suffix('.progress.json'), short)
        return short
    except Exception:
        return dict(status='FAILED', job=args, traceback=traceback.format_exc())


from copy import deepcopy
from datetime import timedelta
from analysis.run_scale_validation_20261027 import (fixture, measure_merge, digest, config_from_dict,
    load_satellites, parse_start_time_utc, position_km, verify)


def build_episode(template,date,events,selected,folder,hashes,daily_paths):
    require_open(date)
    start,end=selected['start_s'],selected['end_s']; assert end-start == verify_manifest()['score_duration_s']
    assert start >= 600, 'Earliest peak lacks full same-day warmup; do not silently change scoring window'
    origin=max(0,int((start-600)//60*60));duration=int(end+600-origin)
    cfg=deepcopy(template)
    cfg['topology']['start_time_utc']=(datetime.fromisoformat(date).replace(tzinfo=timezone.utc)+timedelta(seconds=origin)).strftime('%Y-%m-%dT%H:%M:%SZ')
    cfg.update(sim_steps=round(duration/cfg['dt']),task_arrival_end_step=round((end-origin)/cfg['dt']),scheduled_task_arrivals=[])
    details=[]
    for index,e in enumerate(events):
        if not origin<=e['ready_s']<end:continue
        task=f'task_{len(details)}';truth=fixture(e['mode'],e['truth_seed'],16);merge=measure_merge(truth)
        write_json(folder/'truth'/f'{task}.json',truth)
        cfg['scheduled_task_arrivals'].append(dict(release_step=round((e['ready_s']-origin)/cfg['dt']),source_sat_id=e['source_sat_id'],
            image_tiling=truth['tiling'],window_result_bytes=[w['output_bytes'] for w in truth['windows']],aggregation_delay_s=merge['median_seconds']))
        details.append(dict(task_id=task,daily_event_index=index,**e,scored=e['ready_s']>=start,truth_sha256=digest(truth),merge_reference=merge))
    assert all(cfg[k]==v for k,v in template.items() if k not in ('topology','sim_steps','task_arrival_end_step','scheduled_task_arrivals'))
    assert all(cfg['topology'][k]==v for k,v in template['topology'].items() if k!='start_time_utc')
    config_from_dict(cfg);sats=load_satellites(cfg['topology']['tle_lines'])
    midnight=parse_start_time_utc(date+'T00:00:00Z');shifted=parse_start_time_utc(cfg['topology']['start_time_utc'])
    sar=len(cfg['task_source_sat_ids'])
    for sid in (0,sar-1,sar,len(sats)-1):
        for t in (0,start-origin,duration):assert position_km(sats[sid],midnight,origin+t)==position_km(sats[sid],shifted,t)
    original=read(read(MAIN/'bc_stage2_20261022/manifest.json')['train'][0])
    paths=daily_paths+list((folder/'truth').glob('*.json'))
    p=dict(date=date,seed=1701,score_start_s=int(start-origin),score_end_s=int(end-origin),origin_absolute_s=origin,
        sim_duration_s=duration,followup_s=600,D_seconds=60,D_max=600,config=cfg,config_sha256=digest(cfg),events=details,
        peak=selected,scored_task_ids=[e['task_id'] for e in details if e['scored']],frozen_protocol=original['frozen_protocol'],
        input_hashes={**hashes,**{str(p):sha(p) for p in paths}},no_retry=True,zero_shot=True,tests_sealed=False)
    write_json(folder/'protocol.json',p);verify(p);return p

def prepare():
    assert not ROOT.exists() and not OPEN.exists(), 'No overwrite or second S opening'
    check_time(); engine.verify_main()
    old_path = MAIN/'l_formal_test_20261028/manifest.json'
    old = read(old_path); estimate = read(ESTIMATE)
    assert estimate['methods'] == list(METHODS) and estimate['score_duration_s'] in (3600, 21600)
    hashes = dict(old['hashes'])
    files = [Path(__file__), PROTOCOL, ESTIMATE, old_path,
        Path('analysis/estimate_s_formal_20261029.py'), Path('analysis/report_s_formal_20261029.py'),
        Path('analysis/verify_s_formal_report_20261029.py'), Path('tests/test_s_formal_20261029.py'),
        MAIN/'SCALE_GENERALIZATION_AMENDMENT_20261025.md', MAIN/'SCALE_VALIDATION_PROTOCOL_20261027.md',
        MAIN/'L_FORMAL_TEST_PROTOCOL_20261028.md', MAIN/'m_formal_test_20261027/FINAL_AUDIT.json',
        MAIN/'l_formal_test_20261028/independent_audit_20261028/FINAL_AUDIT.json',
        MAIN/'scale_generalization_20261025/registration.json']
    for path in files:
        hashes[str(path)] = sha(path)
    for path, expected in hashes.items():
        assert sha(path) == expected, path
    assert set(old['models']) == set(MAPPO)
    for item in estimate['evidence']:
        assert sha(item['path']) == item['sha256']
    # 排他登记并冻结协议哈希后，才生成S轨道模板及后续日期任务。
    ROOT.mkdir()
    registration = dict(scope='S', registered_at=datetime.now(timezone.utc).isoformat(),
        protocol_sha256=sha(PROTOCOL), hashes=hashes, methods=METHODS, models=old['models'],
        ranges=old['ranges'], dates=DATES, sar=SAR, compute=72,
        score_duration_s=estimate['score_duration_s'], comparison_scopes=SCOPES,
        deadline=END.isoformat(), zero_shot=True, no_finetune=True, no_retry=True)
    with (ROOT/'PREGEN_REGISTRATION.json').open('x') as stream:
        json.dump(registration, stream, indent=2)
    hashes[str(ROOT/'PREGEN_REGISTRATION.json')] = sha(ROOT/'PREGEN_REGISTRATION.json')
    cfg = engine.scale_config(SAR, 72)
    shape = read(MAIN/'scale_generalization_20261025/registration.json')['scenario_shapes']['S']
    assert engine.digest(cfg['topology']['tle_lines']) == shape['tle_formula_sha256']
    write_json(ROOT/'config_template.json', cfg)
    hashes[str(ROOT/'config_template.json')] = sha(ROOT/'config_template.json')
    write_json(ROOT/'manifest.json', dict(hashes=hashes, models=old['models'], ranges=old['ranges'],
        methods=METHODS, dates=DATES, workers=9, torch_threads=1, sar=SAR, compute=72,
        score_duration_s=estimate['score_duration_s'], zero_shot=True, no_retry=True,
        no_finetune=True, deadline=END.isoformat(), comparison_scopes=SCOPES,
        bootstrap=dict(unit='paired day', draws=10000, seed=20261027, confidence=.95),
        scenario_note='Same dates, different scenario, frozen models',
        input_range_sha256=sha(old['ranges'])))
    print('PREPARED', sha(ROOT/'manifest.json'))


def open_once():
    check_time(); m = verify_manifest()
    with OPEN.open('x') as stream:
        json.dump(dict(scope='S', dates=m['dates'], at=datetime.now(timezone.utc).isoformat(),
            manifest_sha256=sha(ROOT/'manifest.json'), protocol_sha256=sha(PROTOCOL),
            user_authorization='2026-10-29: restore S zero-shot formal test with full baseline scopes'),
            stream, indent=2)


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
                futures = [pool.submit(source_windows, (sid, cfg['topology']['tle_lines'][:SAR], targets, date)) for sid in range(SAR)]
                windows = []
                for i, future in enumerate(as_completed(futures), 1):
                    windows.extend(future.result()); check_time()
                    write_json(ROOT/'progress.json', dict(stage='acquisition', date=date, done=i, total=SAR))
                windows.sort(key=lambda w:(w['start_s'], w['source_sat_id'], w['target_id']))
                for i, w in enumerate(windows):
                    w['window_id'] = i
                preparation.permitted = require_open
                events = preparation.arrivals(windows, date); folder = ROOT/'data'/date
                write_json(folder/'windows.json', windows); write_json(folder/'arrivals.json', events)
                selected = preparation.peak_window([e['ready_s'] for e in events], [e['raw_bytes'] for e in events], m['score_duration_s'])
                engine.VALID = DATES
                path = folder/'scoring_window/protocol.json'
                p = build_episode(cfg, date, events, selected, path.parent,
                    {**m['hashes'], str(OPEN):sha(OPEN)}, [folder/'windows.json', folder/'arrivals.json'])
                p.update(tests_sealed=False, stage='S_FORMAL_TEST', opening_sha256=sha(OPEN))
                write_json(path, p); engine.verify(p); protocols.append(str(path))
            write_json(ROOT/'run_manifest.json', dict(protocols=protocols, methods=METHODS,
                hashes={p:sha(p) for p in protocols+[str(ROOT/'manifest.json'), str(OPEN)]},
                tests_sealed=False, generation_wall_seconds=time.perf_counter()-started))
            results = batches(pool, [(p, n) for n in METHODS for p in protocols])
        passed = len(results) == len(METHODS)*len(DATES) and all(r['status'] == 'PASS' for r in results)
        verify_manifest()
        write_json(ROOT/'final_status.json', dict(status='COMPLETE' if passed else 'BLOCKED', results=results,
            tests_sealed=False, independent_report_pending=True, wall_seconds=time.perf_counter()-started))
        if passed:
            from analysis.report_s_formal_20261029 import main as report
            from analysis.verify_s_formal_report_20261029 import main as verify_report
            report(); verify_report()
            write_json(ROOT/'REPORT_COMPLETE.json', dict(status='PASS', at=datetime.now(timezone.utc).isoformat(),
                report_hashes_sha256=sha(ROOT/'independent_audit_20261029/REPORT_HASHES.json'),
                verification_sha256=sha(ROOT/'independent_audit_20261029/FOLLOWUP_VERIFICATION.json')))
    except Exception:
        status_path = ROOT/'report_failure.json' if (ROOT/'final_status.json').exists() and read(ROOT/'final_status.json')['status'] == 'COMPLETE' else ROOT/'final_status.json'
        write_json(status_path, dict(status='BLOCKED', results=results, traceback=traceback.format_exc(),
            tests_sealed=not OPEN.exists()))
        raise


def supervise():
    verify_manifest()
    command = [sys.executable, '-B', '-m', 'analysis.run_s_formal_20261029', 'panel']
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
    processes = subprocess.check_output(['ps', '-axo', 'command='], text=True).splitlines()
    assert not any((' -m analysis.run_' in c and c.rstrip().endswith((' panel', ' supervise'))) or
                   'multiprocessing.spawn import spawn_main' in c for c in processes), 'Another experiment pool is active'
    with (ROOT/'launch_request.json').open('x') as stream:
        json.dump(dict(at=datetime.now(timezone.utc).isoformat(), state='LAUNCHING'), stream)
    with (ROOT/'supervisor.log').open('xb') as log:
        child = subprocess.Popen([sys.executable, '-B', '-m', 'analysis.run_s_formal_20261029', 'supervise'],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    write_json(ROOT/'launch_request.json', dict(supervisor_pid=child.pid, at=datetime.now(timezone.utc).isoformat()))
    print('SUPERVISOR', child.pid)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('mode', choices=['prepare', 'launch', 'panel', 'supervise'])
    globals()[parser.parse_args().mode]()
