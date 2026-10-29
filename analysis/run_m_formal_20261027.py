"""M正式测试独立入口；排他开启、单池、冻结模型、无重试。"""
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
from types import SimpleNamespace

from analysis import prepare_main_bc_stage2 as preparation
from analysis import run_scale_validation_20261027 as validation
from analysis.run_dynamic_learning import write_json
from analysis.run_dynamic_heuristics_v2 import read, sha, GranularityEnv
from analysis.run_main_local_panel import MAIN, verify_main
from analysis.run_local_comparison_20261027 import BASELINES, EXTRAS, policy_set as local_policy_set
from analysis.run_calibration_capacity import FifoDiagnostic
from analysis.sar_three_plan import ThreePlanChoice
from analysis.local_information_baselines import LocalInformationPlane
from sim.control import ControlPlane
from sim.scheduler.local import LocalOnlyPolicy
from sim.scheduler.literature import CGSComputeAdapted, HEFTAdapted

ROOT = MAIN/'m_formal_test_20261027'
LROOT = MAIN/'scale_validation_20261027'
PROTOCOL = MAIN/'M_TEST_PRIORITY_AMENDMENT_20261027.md'
OPEN = MAIN/'M_TEST_OPENED.json'
DATES = ('2026-12-21', '2026-12-22', '2026-12-23')
SEEDS = (1701, 1702, 1703)
MAPPO = tuple(f'{family}_{seed}' for family in ('fixed', 'dynamic') for seed in SEEDS)
BC = tuple(f'{family}_bc_{seed}' for family in ('fixed', 'dynamic') for seed in SEEDS)
GLOBAL = ('global_v3_16', 'global_v3_dynamic', 'global_cgs_16', 'global_heft_16')
NEUTRAL = ('fifo_16', 'local_16', 'ground_16')
METHODS = MAPPO+BC+BASELINES+EXTRAS+NEUTRAL+GLOBAL
L_METHODS = MAPPO+('global_v3_16', 'global_v3_dynamic', 'minload_2_4', 'minload_2_16',
                   'zero_2_16', 'local_cgs_16', 'local_heft_16', 'ground_16')
END = datetime(2026, 11, 5, 16, tzinfo=timezone.utc)


def check_time():
    if datetime.now(timezone.utc) >= END:
        raise TimeoutError('11-06 Beijing hard deadline; no extension')


def verify_manifest():
    m = read(ROOT/'manifest.json')
    for path, expected in m['hashes'].items():
        assert sha(path) == expected, path
    return m


def require_open(date):
    assert date in DATES, 'Only the three registered M test dates'
    opening = read(OPEN)
    assert opening['scope'] == 'M' and opening['manifest_sha256'] == sha(ROOT/'manifest.json')


def source_windows(args):
    require_open(args[-1])
    # 子进程内用正式日期门控替代旧准备器的训练/验证门控；生成算法完全复用。
    preparation.permitted = require_open
    return preparation.source_windows(args)


def test_arrivals(windows, date):
    require_open(date)
    original = preparation.permitted
    try:
        preparation.permitted = require_open
        return preparation.arrivals(windows, date)
    finally:
        preparation.permitted = original


class LocalFifo:
    def __init__(self, env):
        self.env = env
        self.policy = FifoDiagnostic()

    def select_actions(self, state):
        env = self.env
        class View:
            def __getattr__(self, name):
                return getattr(env, name)
        view = View()
        # FIFO只替换远端动态内存观测，排序、几何和执行器约束保持原实现。
        view.satellites = {sid: SimpleNamespace(mem_capacity_gb=s.mem_capacity_gb,
            mem_used_gb=s.mem_capacity_gb-state.satellites[sid]['mem_remaining_gb'])
            for sid, s in env.satellites.items()}
        return self.policy.select_actions(view, state)


class EmptyPolicy:
    def select_actions(self, state):
        return []


def install_worker_adapter(method, root=None):
    """仅本worker内适配已测试的仿真记录器；磁盘上的冻结入口不变。"""
    holder = {}
    def environment(cfg):
        holder['env'] = GranularityEnv(cfg)
        return holder['env']
    def policies(name, sources, hops):
        if name not in ('fifo_16', 'local_16'):
            return local_policy_set(name, sources, hops)
        plane = LocalInformationPlane(sources)
        plane.history_hops = 2
        factory = (lambda: LocalFifo(holder['env'])) if name == 'fifo_16' else LocalOnlyPolicy
        return plane, {s: factory() for s in sources}
    def global_factory(hops):
        if method == 'global_cgs_16':
            return CGSComputeAdapted()
        if method == 'global_heft_16':
            return HEFTAdapted()
        if method == 'ground_16':
            return EmptyPolicy()
        return ThreePlanChoice(hops)
    validation.ROOT = root or ROOT
    validation.VALID = DATES
    validation.METHODS = METHODS
    validation.LEARNED = MAPPO+BC
    validation.BASELINES = BASELINES+('fifo_16', 'local_16')
    validation.EXTRAS = EXTRAS
    validation.GranularityEnv = environment
    validation.policy_set = policies
    validation.ThreePlanChoice = global_factory
    validation.ControlPlane = LocalInformationPlane if method == 'ground_16' else ControlPlane
    validation.check_time = check_time
    validation.verify_manifest = verify_manifest


def simulate(args):
    path, method = args
    require_open(read(path)['date'])
    install_worker_adapter(method)
    short = validation.simulate(args)
    if 'path' in short:
        path = Path(short['path'])
        row = read(path)
        row.update(tests_sealed=False, l_tests_sealed=True, stage='M_FORMAL_TEST',
                   opening_sha256=sha(OPEN), information_group='global' if method in GLOBAL else 'local')
        if method == 'fifo_16':
            row['fifo_information'] = 'Original FIFO ranking; remote memory from source-local 2-hop TTL view'
        write_json(path, row)
        short['sha256'] = sha(path)
        write_json(path.with_suffix('.progress.json'), short)
    return short


def prepare():
    assert not ROOT.exists() and not OPEN.exists(), 'No re-preparation or second opening'
    check_time(); verify_main()
    assert len(METHODS) == len(set(METHODS)) == 33 and len(L_METHODS) == 14
    old = read(LROOT/'manifest.json')
    hashes = dict(old['hashes'])
    models = dict(old['models'])
    ranges = read(old['ranges'])
    for family in ('fixed', 'dynamic'):
        for seed in SEEDS:
            stage = 'bc_stage2_20261022' if family == 'fixed' else 'dynamic_learning_20261026'
            path = MAIN/stage/'training'/f'pure_imitation_{seed}.pt'
            models[f'{family}_bc_{seed}'] = str(path)
            hashes[str(path)] = sha(path)
            # M不报告此BC旁路的分布外率；仅保留记录器结构，范围不进入actor。
            ranges['ranges'][f'{family}_bc_{seed}'] = ranges['ranges'][f'{family}_{seed}']
    for p in (PROTOCOL, Path(__file__), Path('analysis/report_m_formal_20261027.py'),
              Path('analysis/drain_scale_batch_20261027.py'), Path('tests/test_m_formal_20261027.py'),
              Path('analysis/prepare_main_bc_stage2.py'), Path('analysis/run_calibration_capacity.py'),
              Path('sim/scheduler/local.py'), MAIN/'date_split.json'):
        hashes[str(p)] = sha(p)
    for p, h in hashes.items():
        assert sha(p) == h, p
    write_json(ROOT/'passive_ranges.json', ranges)
    hashes[str(ROOT/'passive_ranges.json')] = sha(ROOT/'passive_ranges.json')
    write_json(ROOT/'manifest.json', dict(hashes=hashes, models=models, methods=METHODS, dates=DATES,
        ranges=str(ROOT/'passive_ranges.json'), workers=9, torch_threads=1, score_duration_s=21600,
        comparison_scopes=dict(fixed_six_local_16=[m for m in BASELINES if m.endswith('_16')],
                               dynamic_twelve_local_4_16=BASELINES),
        l_methods=L_METHODS, l_test_status='SEALED', deadline=END.isoformat(), no_retry=True,
        bootstrap=dict(unit='paired day', draws=10000, seed=20261027, confidence=.95),
        prioritization='M audit and report before reduced L validation and separate L test opening'))
    print('PREPARED', sha(ROOT/'manifest.json'))


def open_once():
    m = verify_manifest()
    paused = read(LROOT/'PAUSED_BY_USER.json')
    assert paused['status'] == 'PAUSED_BY_USER' and len(paused['results']) == 9
    for item in paused['results']:
        assert item['status'] == 'PASS' and sha(item['output']) == item['sha256']
        row = read(item['output']); validation.check_record(row, read(row['protocol']))
    for pid in (read(LROOT/'launch.json')['pid'], *[r['pid'] for r in paused['results']]):
        assert subprocess.run(['ps', '-p', str(pid), '-o', 'pid='], capture_output=True).returncode != 0
    with OPEN.open('x') as stream:
        json.dump(dict(scope='M', dates=m['dates'], at=datetime.now(timezone.utc).isoformat(),
            manifest_sha256=sha(ROOT/'manifest.json'), protocol_sha256=sha(PROTOCOL), l_tests_sealed=True,
            user_authorization='2026-10-27: prioritize M official test, open once after current L batch'), stream, indent=2)


def panel():
    started = time.perf_counter()
    try:
        check_time(); m = verify_manifest(); open_once()
        cfg = read(MAIN/'config_template.json'); targets = read(MAIN/'targets_maritime80.json')
        protocols = []; results = []
        with ProcessPoolExecutor(max_workers=9, mp_context=multiprocessing.get_context('spawn')) as pool:
            for date in DATES:
                futures = [pool.submit(source_windows, (sid, cfg['topology']['tle_lines'][:24], targets, date)) for sid in range(24)]
                windows = []
                for i, future in enumerate(as_completed(futures), 1):
                    windows.extend(future.result()); check_time()
                    write_json(ROOT/'progress.json', dict(stage='acquisition', date=date, done=i, total=24))
                windows.sort(key=lambda w:(w['start_s'], w['source_sat_id'], w['target_id']))
                for i, w in enumerate(windows):
                    w['window_id'] = i
                events = test_arrivals(windows, date); folder = ROOT/'data'/date
                write_json(folder/'windows.json', windows); write_json(folder/'arrivals.json', events)
                selected = preparation.peak_window([e['ready_s'] for e in events], [e['raw_bytes'] for e in events], 21600)
                p = preparation.build_episode(date, events, selected, folder/'six_hours',
                    {**m['hashes'], str(OPEN):sha(OPEN)}, [folder/'windows.json', folder/'arrivals.json'])
                p.update(stage='M_FORMAL_TEST', l_tests_sealed=True)
                path = folder/'six_hours/protocol.json'; write_json(path, p); protocols.append(str(path))
            write_json(ROOT/'run_manifest.json', dict(protocols=protocols, methods=METHODS,
                hashes={p:sha(p) for p in protocols+[str(ROOT/'manifest.json'), str(OPEN)]},
                generation_wall_seconds=time.perf_counter()-started, l_tests_sealed=True))
            # 真实批次边界：任何一批失败时只收完当前批，绝不预取后续批。
            tasks = [(p, method) for method in METHODS for p in protocols]
            for offset in range(0, len(tasks), 9):
                check_time()
                if (ROOT/'PAUSE_REQUESTED.json').exists():
                    raise RuntimeError('Explicit pause request at batch boundary')
                batch = [pool.submit(simulate, job) for job in tasks[offset:offset+9]]
                current = []
                for future in as_completed(batch):
                    row = future.result(); current.append(row); results.append(row)
                    write_json(ROOT/'progress.json', dict(stage='test', done=len(results), total=len(tasks), results=results))
                if any(r['status'] != 'PASS' for r in current):
                    raise RuntimeError('Failed task; current batch drained, no retry')
        write_json(ROOT/'simulation_status.json', dict(status='COMPLETE', results=results, wall_seconds=time.perf_counter()-started))
        from analysis.report_m_formal_20261027 import report
        report(ROOT)
        write_json(ROOT/'final_status.json', dict(status='COMPLETE', results=results, report=str(ROOT/'REPORT.md'), l_tests_sealed=True))
    except Exception:
        write_json(ROOT/'final_status.json', dict(status='BLOCKED', traceback=traceback.format_exc(), l_tests_sealed=True))
        raise


def supervise():
    verify_manifest()
    write_json(ROOT/'supervisor.json', dict(pid=os.getpid(), status='WAITING_FOR_L_DRAIN'))
    while not (LROOT/'PAUSED_BY_USER.json').exists():
        check_time()
        guard = read(LROOT/'DRAIN_GUARD.json')
        assert subprocess.run(['ps', '-p', str(guard['pid']), '-o', 'pid='], capture_output=True).returncode == 0, 'Drain guard exited'
        time.sleep(2)
    assert read(LROOT/'PAUSED_BY_USER.json')['status'] == 'PAUSED_BY_USER'
    check_time(); verify_manifest()
    with (ROOT/'panel.log').open('xb') as log:
        child = subprocess.Popen([sys.executable, '-B', '-m', 'analysis.run_m_formal_20261027', 'panel'],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    awake = subprocess.Popen(['/usr/bin/caffeinate', '-i', '-w', str(child.pid)], start_new_session=True)
    identity = subprocess.check_output(['ps', '-p', str(child.pid), '-o', 'pid=,lstart=,command='], text=True).strip()
    write_json(ROOT/'launch.json', dict(pid=child.pid, supervisor_pid=os.getpid(), caffeinate_pid=awake.pid,
        identity=identity, command='analysis.run_m_formal_20261027 panel', started_at=datetime.now(timezone.utc).isoformat(),
        manifest_sha256=sha(ROOT/'manifest.json')))
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
    verify_manifest(); assert not (ROOT/'launch_request.json').exists()
    with (ROOT/'supervisor.log').open('xb') as log:
        child = subprocess.Popen([sys.executable, '-B', '-m', 'analysis.run_m_formal_20261027', 'supervise'],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    write_json(ROOT/'launch_request.json', dict(supervisor_pid=child.pid, at=datetime.now(timezone.utc).isoformat()))
    print('SUPERVISOR', child.pid)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'launch', 'panel', 'supervise'])
    mode = parser.parse_args().mode
    try:
        globals()[mode]()
    except Exception:
        if mode == 'supervise':
            write_json(ROOT/'supervisor_failure.json', dict(status='BLOCKED', traceback=traceback.format_exc()))
        raise
