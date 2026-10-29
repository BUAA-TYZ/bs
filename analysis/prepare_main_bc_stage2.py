"""只生成已登记训练/验证日期；第一阶段及封存测试保持原样。"""

from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import multiprocessing
from pathlib import Path
import random
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.prepare_main_rl_stage1 import ROOT as STAGE1, read
from analysis.run_main_local_panel import MAIN, verify_main
from analysis.run_load_pressure_short import verify
from analysis.estimate_load_pressure import peak_window, sha
from analysis.redesign_scenario import MODES, tiling, fixture, measure_merge, digest, write_json
from sim.acquisition import generate_acquisition_schedule
from sim.config import config_from_dict
from sim.orbit import load_satellites, parse_start_time_utc, position_km

ROOT = MAIN/'bc_stage2_20261022'
TRAIN = tuple(f'2026-10-{d}' for d in range(16, 22))
VALID = tuple(f'2026-10-{d}' for d in range(22, 25))
SEEDS = (1701, 1702, 1703)
METHODS = ('v3', 'zero_1', 'zero_2', 'history_1', 'history_2', 'minload_1', 'minload_2', 'fifo',
           'bc_1701', 'bc_1702', 'bc_1703')


def permitted(date):
    if date not in TRAIN+VALID:
        raise ValueError('Date outside registered training/validation allowlist')


def source_windows(args):
    sid, tle, targets, date = args
    permitted(date)
    return generate_acquisition_schedule(tle, date+'T00:00:00Z', [sid], duration_s=86400, dt_s=.1,
        targets=[{k: r[k] for k in ('target_id', 'lat_deg', 'lon_deg', 'radius_km')} for r in targets],
        scan_step_s=10, max_off_nadir_deg=40, min_sun_elevation_deg=-90, sun_guard_deg=0,
        min_spacing_s=60, pass_cooldown_s=0, incidence_range_deg=(20, 40))['windows']


def arrivals(windows, date):
    permitted(date)
    rng, events, last = random.Random(1701+int(date.replace('-', ''))), [], {}
    for w in sorted(windows, key=lambda w: (w['start_s'], w['source_sat_id'], w['target_id'])):
        mode = rng.choices(list(MODES), weights=(.2, .6, .2))[0]
        sid = w['source_sat_id']
        start = max(w['start_s'], last.get(sid, -60)+60)
        end, ready = start+MODES[mode][3], start+MODES[mode][3]+60
        if end > w['end_s']-10 or ready >= 86400:
            continue
        last[sid] = start
        seed, work = rng.randrange(2**32), tiling(mode, 16)
        events.append(dict(source_sat_id=sid, capture_start_s=start, capture_end_s=end, ready_s=ready,
            target_id=w['target_id'], mode=mode, truth_seed=seed, window_id=w['window_id'],
            raw_bytes=work.image_size_mb*1e6, compute_windows=sum(t.compute_cost for t in work.tiles())))
    return sorted(events, key=lambda e: (e['ready_s'], e['source_sat_id']))


def peak(events, duration, excluded=None):
    # 只在时间区间完整不重叠的候选中比较原始字节量，最早破同分。
    starts = {0., 86400.-duration, *[min(e['ready_s'],86400.-duration) for e in events]}
    if excluded:
        starts.update((max(0.,excluded[0]-duration), min(86400.-duration,excluded[1])))
    choices = []
    for start in sorted(starts):
        end = start+duration
        if end > 86400 or (excluded and not (end <= excluded[0] or start >= excluded[1])):
            continue
        ids = [i for i, e in enumerate(events) if start <= e['ready_s'] < end]
        choices.append(dict(start_s=start, end_s=end, raw_bytes=sum(events[i]['raw_bytes'] for i in ids),
                            count=len(ids), event_indices=ids))
    return min(choices, key=lambda r: (-r['raw_bytes'], r['start_s']))


def build_episode(date, events, selected, folder, entry_hashes, daily_paths):
    start, end = selected['start_s'], selected['end_s']
    origin = max(0, int((start-600)//60*60))
    duration = int(end+600-origin)
    original = read(MAIN/'config_template.json')
    cfg = deepcopy(original)
    cfg['topology']['start_time_utc'] = (datetime.fromisoformat(date).replace(tzinfo=timezone.utc)+timedelta(seconds=origin)).strftime('%Y-%m-%dT%H:%M:%SZ')
    cfg.update(sim_steps=round(duration/cfg['dt']), task_arrival_end_step=round((end-origin)/cfg['dt']), scheduled_task_arrivals=[])
    details = []
    for index, e in enumerate(events):
        if not origin <= e['ready_s'] < end:
            continue
        task = f'task_{len(details)}'
        truth = fixture(e['mode'], e['truth_seed'], 16)
        merge = measure_merge(truth)
        write_json(folder/'truth'/f'{task}.json', truth)
        cfg['scheduled_task_arrivals'].append(dict(release_step=round((e['ready_s']-origin)/cfg['dt']),
            source_sat_id=e['source_sat_id'], image_tiling=truth['tiling'],
            window_result_bytes=[w['output_bytes'] for w in truth['windows']], aggregation_delay_s=merge['median_seconds']))
        details.append(dict(task_id=task, daily_event_index=index, **e, scored=e['ready_s'] >= start,
                            truth_sha256=digest(truth), merge_reference=merge))
    assert all(cfg[k] == v for k, v in original.items() if k not in {'topology','sim_steps','task_arrival_end_step','scheduled_task_arrivals'})
    assert all(cfg['topology'][k] == v for k, v in original['topology'].items() if k != 'start_time_utc')
    config_from_dict(cfg)
    sats = load_satellites(cfg['topology']['tle_lines'])
    midnight, shifted = parse_start_time_utc(date+'T00:00:00Z'), parse_start_time_utc(cfg['topology']['start_time_utc'])
    for sid in (0, 23, 24, 167):
        for t in (0, start-origin, duration):
            assert position_km(sats[sid], midnight, origin+t) == position_km(sats[sid], shifted, t)
    paths = daily_paths+list((folder/'truth').glob('*.json'))
    p = dict(date=date, seed=1701, score_start_s=int(start-origin), score_end_s=int(end-origin),
             origin_absolute_s=origin, sim_duration_s=duration, followup_s=600, D_seconds=60, D_max=600,
             config=cfg, config_sha256=digest(cfg), events=details, peak=selected,
             scored_task_ids=[e['task_id'] for e in details if e['scored']],
             frozen_protocol=read(STAGE1/'protocol.json')['frozen_protocol'],
             input_hashes={**entry_hashes, **{str(f): sha(f) for f in paths}}, no_retry=True)
    write_json(folder/'protocol.json', p)
    verify(p)
    return p


def prepare():
    verify_main()
    verify(read(STAGE1/'protocol.json'))
    assert read(STAGE1/'INDEPENDENT_AUDIT.json')['status'] == 'PASS'
    if ROOT.exists():
        raise FileExistsError('Preserve existing stage; no preparation retry')
    ROOT.mkdir()
    files = [Path(__file__), Path('analysis/main_bc_interface.py'), Path('analysis/run_main_bc_stage2.py'),
             Path('tests/test_main_bc_stage2.py'), MAIN/'BC_STAGE2_PROTOCOL_20261022.md',
             MAIN/'manifest.json', MAIN/'date_split.json', MAIN/'targets_maritime80.json', MAIN/'config_template.json',
             STAGE1/'protocol.json', STAGE1/'reference.json', STAGE1/'INDEPENDENT_AUDIT.json']
    files += [Path('analysis/local_information_baselines.py'),Path('analysis/report_main_posthoc.py'),
              Path('analysis/run_main_local_panel.py')]
    entry_hashes = {**read(STAGE1/'protocol.json')['input_hashes'], **{str(f): sha(f) for f in files}}
    train, validation = [], []
    targets, config = read(MAIN/'targets_maritime80.json'), read(MAIN/'config_template.json')
    with ProcessPoolExecutor(max_workers=8, mp_context=multiprocessing.get_context('spawn')) as pool:
        for date in TRAIN+VALID:
            folder = ROOT/'data'/date
            if date == TRAIN[0]:
                windows, events = read(STAGE1/'windows.json'), read(STAGE1/'daily_arrivals.json')
                assert arrivals(windows, date) == events
            else:
                windows = [w for rows in pool.map(source_windows, [(s, config['topology']['tle_lines'][:24], targets, date) for s in range(24)]) for w in rows]
                windows.sort(key=lambda w: (w['start_s'], w['source_sat_id'], w['target_id']))
                for i, w in enumerate(windows):
                    w['window_id'] = i
                events = arrivals(windows, date)
            write_json(folder/'windows.json', windows)
            write_json(folder/'arrivals.json', events)
            daily = [folder/'windows.json', folder/'arrivals.json']
            if date in TRAIN:
                # 第一窗口沿用已审计的同一最忙字节选窗算法。
                first = peak_window([e['ready_s'] for e in events], [e['raw_bytes'] for e in events], 3600)
                selected = [first, peak(events, 3600, (first['start_s'], first['end_s']))]
                for n, window in enumerate(selected):
                    episode = folder/f'hour_{n+1}'
                    if date == TRAIN[0] and n == 0:
                        p = deepcopy(read(STAGE1/'protocol.json'))
                        p['input_hashes'].update(entry_hashes)
                        write_json(episode/'protocol.json', p)
                        verify(p)
                    else:
                        p = build_episode(date, events, window, episode, entry_hashes, daily)
                    train.append(str(episode/'protocol.json'))
            else:
                window = peak_window([e['ready_s'] for e in events], [e['raw_bytes'] for e in events], 21600)
                build_episode(date, events, window, folder/'six_hours', entry_hashes, daily)
                validation.append(str(folder/'six_hours/protocol.json'))
            write_json(ROOT/'prepare_progress.json', dict(date=date, daily_images=len(events), train_episodes=len(train), validation_episodes=len(validation)))
    manifest = dict(stage='BC_ONLY', train=train, validation=validation, seeds=SEEDS, methods=METHODS, workers=9,
                    hyperparameters=dict(hidden=64, heads=4, epochs=100, batch_size=32, lr=.001, weight_decay=0.,
                                         optimizer='Adam', gradient_clip=1., checkpoint='fixed final epoch; no validation selection'),
                    entry_hashes=entry_hashes, episode_hashes={p:sha(Path(p)) for p in train+validation},
                    stop_after='Full BC validation report; await human confirmation; MAPPO forbidden',
                    sealed_tests=True, no_retry=True)
    write_json(ROOT/'manifest.json', manifest)
    print(json.dumps({'status':'PREPARED','train_episodes':len(train),'validation_episodes':len(validation),'validation_runs':len(validation)*len(METHODS)}))


if __name__ == '__main__':
    prepare()
