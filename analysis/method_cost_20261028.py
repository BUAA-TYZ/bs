"""冻结策略的独立录制与硬件回放；不写正式实验目录。"""
import argparse
from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import pickle
import resource
import sys
import time
from types import SimpleNamespace

import numpy as np
import torch

from analysis import main_bc_interface as fixed
from analysis import dynamic_learning_interface as dynamic

ROOT = Path('results/method_cost_20261028')
MAIN = Path('results/redesign_20261021/main_scenario')
FIELDS = {'nodes': torch.float32, 'image': torch.float32,
          'node_valid': torch.bool, 'adjacency': torch.bool,
          'image_mask': torch.bool, 'position_mask': torch.bool}


def read(path):
    return json.loads(Path(path).read_text())


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False))
    temporary.replace(path)


def stats(values):
    if not values:
        return {'n': 0}
    return dict(n=len(values), mean=float(np.mean(values)),
                **{f'P{q}': float(np.percentile(values, q)) for q in (50, 95, 99)})


def tensors(obs, device='cpu'):
    out = dict(obs)
    for key, dtype in FIELDS.items():
        if key in obs:
            out[key] = torch.as_tensor(obs[key], dtype=dtype, device=device)
    if 'variants' in obs:
        out['variants'] = [tensors(v, device) for v in obs['variants']]
    return out


def probabilities(logits):
    image, positions = logits
    positions = positions if isinstance(positions, list) else [positions]
    # 全掩码条件分布不可采样，显式保留为零向量，另核对合法性掩码。
    return [x.softmax(-1).detach().cpu().numpy() if torch.isfinite(x).any()
            else np.zeros(x.numel(), dtype=np.float32) for x in [image, *positions]]


def snapshot(env, state, task):
    ids = env.tasks[task].tile_ids
    index = int(task.split('_')[-1])
    events = [None] * (index + 1)
    events[index] = env.cfg.scheduled_task_arrivals[index]
    return portable(deepcopy(dict(state=state, task=task, workload=env._task_workloads[task],
                         task_record=env.tasks[task], tiles={tid: env.tiles[tid] for tid in ids},
                         events=events, image_workload=bool(env.image_workload))))


def portable(value):
    # Mac与原Orin环境的NumPy主版本不同，快照避免依赖NumPy内部pickle模块名。
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if is_dataclass(value):
        return replace(value, **{f.name: portable(getattr(value, f.name)) for f in fields(value) if f.init})
    if isinstance(value, dict):
        return {portable(k): portable(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return type(value)(portable(v) for v in value)
    return value


class SnapshotEnv:
    def __init__(self, saved):
        task = saved['task']
        self._task_workloads = {task: saved['workload']}
        self.tasks = {task: saved['task_record']}
        self.tiles = saved['tiles']
        self.cfg = SimpleNamespace(scheduled_task_arrivals=saved['events'])
        self.image_workload = saved['image_workload']

    def memory_size_gb(self, size_mb):
        return size_mb / (1000 if self.image_workload else 1024)


def rebuild(saved, family):
    state, task = saved['state'], saved['task']
    if family == 'dynamic':
        return dynamic.build_observation(SnapshotEnv(saved), state, task)
    return fixed.build_observation(state, task,
        {tid: row for tid, row in state.tiles.items() if row['parent_task_id'] == task})


class Recorder:
    def __init__(self, env, actor, family, folder):
        self.env, self.actor, self.family, self.folder = env, actor, family, Path(folder)
        self.records, self.calls, self.admissions = [], [], []
        self.overhead_s = 0.
        self.module = dynamic if family == 'dynamic' else fixed
        self.original_build = self.module.build_observation
        self.original_logits, self.original_sample = actor.logits, actor.sample
        self.original_apply = env._dist_pipeline.apply_actions

    def __enter__(self):
        def build(*args):
            state, task = args[1:3] if self.family == 'dynamic' else args[:2]
            start = time.perf_counter()
            saved = snapshot(self.env, state, task)
            self.overhead_s += time.perf_counter() - start
            start = time.perf_counter()
            obs = self.original_build(*args)
            elapsed = time.perf_counter() - start
            start = time.perf_counter()
            self.records.append(dict(snapshot=saved, observation=portable(deepcopy(obs)),
                tensors=tensors(obs), observation_s=elapsed, forward_s=0., sampling_s=0.))
            self.overhead_s += time.perf_counter() - start
            return obs

        def logits(obs):
            start = time.perf_counter()
            result = self.original_logits(obs)
            self.records[-1]['forward_s'] += time.perf_counter() - start
            start = time.perf_counter()
            self.records[-1]['probabilities'] = [p.tolist() for p in probabilities(result)]
            self.overhead_s += time.perf_counter() - start
            return result

        def sample(obs, generator):
            overhead = self.overhead_s
            start = time.perf_counter()
            result = self.original_sample(obs, generator)
            elapsed = time.perf_counter() - start - (self.overhead_s - overhead)
            self.records[-1]['sampling_s'] = elapsed - self.records[-1]['forward_s']
            self.records[-1]['choice'] = result
            return result

        def apply(actions, links):
            start = time.perf_counter()
            result = self.original_apply(actions, links)
            elapsed = time.perf_counter() - start
            if actions:
                self.admissions.append(dict(actions=len(actions), seconds=elapsed))
            return result

        self.module.build_observation, self.actor.logits, self.actor.sample = build, logits, sample
        self.env._dist_pipeline.apply_actions = apply
        return self

    def select(self, policy, state):
        before, overhead = len(self.records), self.overhead_s
        start = time.perf_counter()
        actions = policy.select_actions(state)
        elapsed = time.perf_counter() - start - (self.overhead_s - overhead)
        records = self.records[before:]
        if records or actions:
            obs = sum(r['observation_s'] for r in records)
            forward = sum(r['forward_s'] for r in records)
            sample = sum(r['sampling_s'] for r in records)
            self.calls.append(dict(decisions=len(records), actions=len(actions), total_s=elapsed,
                observation_s=obs, forward_s=forward, sampling_s=sample,
                commitment_s=elapsed-obs-forward-sample))
        return actions

    def flush(self, offset):
        self.folder.mkdir(parents=True, exist_ok=True)
        for index in range(offset, len(self.records)):
            record = self.records[index]
            path = self.folder / f'{index:06d}.pkl.gz'
            with gzip.open(path, 'wb') as stream:
                pickle.dump(record, stream, protocol=4)
            self.records[index] = {k: record[k] for k in
                ('observation_s', 'forward_s', 'sampling_s')}

    def __exit__(self, *exc):
        self.module.build_observation = self.original_build
        self.actor.logits, self.actor.sample = self.original_logits, self.original_sample
        self.env._dist_pipeline.apply_actions = self.original_apply


def prepare():
    from analysis.run_m_formal_20261027 import verify_manifest, require_open
    from sim.workload import ImageTiling
    m = verify_manifest()
    require_open('2026-12-21')
    protocol = MAIN/'m_formal_test_20261027/data/2026-12-21/six_hours/protocol.json'
    p = read(protocol)
    assert not (ROOT/'registration.json').exists()
    models = {family: m['models'][f'{family}_1701'] for family in ('fixed', 'dynamic')}
    hashes = dict(m['hashes'])
    for path in (protocol, Path(__file__), Path('analysis/method_cost_queue_20261028.py'),
                 Path('analysis/method_cost_memory_20261028.py'),
                 Path('tests/test_method_cost_20261028.py'), *map(Path, models.values())):
        hashes[str(path)] = sha(path)
    tilings = [e['image_tiling'] for e in p['config']['scheduled_task_arrivals']]
    largest = max(tilings, key=lambda t: ImageTiling.from_dict(t).image_size_mb)
    workload = ImageTiling.from_dict({**largest, 'requested_num_tiles': 4})
    geometry = dict(selection='Largest raw-byte scene geometry in registered M test-day input, independent of outcomes',
        tiling={**largest, 'requested_num_tiles': 4},
        tiles=[dict(windows=t.windows_xyxy, data_size_mb=t.data_size_mb) for t in workload.tiles()])
    assert len(geometry['tiles']) == 4
    for tile in geometry['tiles']:
        windows = tile['windows']
        rectangle_bytes = (max(w[2] for w in windows)-min(w[0] for w in windows)) * (max(w[3] for w in windows)-min(w[1] for w in windows)) * 2
        assert abs(rectangle_bytes-tile['data_size_mb']*1e6) < 1e-5
    write(ROOT/'four_block_geometry.json', geometry)
    hashes[str(ROOT/'four_block_geometry.json')] = sha(ROOT/'four_block_geometry.json')
    write(ROOT/'registration.json', dict(status='REGISTERED', models=models, hashes=hashes,
        source_protocol=str(protocol), source_protocol_sha256=sha(protocol), date=p['date'],
        score_start_s=p['score_start_s'], score_end_s=p['score_start_s']+3600,
        seed=1701, selection='First registered test date, first hour of its score window; lowest seed, no performance selection',
        warmup_calls=50, repeats=20, batch=1, mac_threads=1, orin_threads=[1, 2],
        probability_atol=1e-5, probability_rtol=1e-4, workers=1,
        prior_power='MAXN mode 0; dynamic clocks, no jetson_clocks lock',
        purpose='Compute-cost diagnostic only, no performance retest or checkpoint selection',
        mac_gate='Wait for current L validation coordinator to exit normally; no competing simulation pool',
        paper='/Users/bytedance/Downloads/中期_content',
        timing='Observation/forward/sampling per decision; commitment per active source policy call; executor admission per nonempty action batch. Full computational batch additionally includes state export, local control enqueue, decision filtering and local view construction. Disk I/O, snapshot copies, simulation transport/wait excluded. Orin observation reconstruction starts at saved legal local view.',
        created_at=datetime.now(timezone.utc).isoformat()))


def verify_registration():
    r = read(ROOT/'registration.json')
    for path, expected in r['hashes'].items():
        assert sha(path) == expected, path
    return r


def capture(family):
    from analysis.run_scale_probe_20261027 import config_from_dict, decision_state
    from analysis.run_dynamic_heuristics_v2 import configuration, GranularityEnv
    from analysis.local_information_baselines import LocalInformationPlane
    from analysis.run_sar_frozen import invariants
    r = verify_registration()
    folder = ROOT/'capture'/family
    assert not folder.exists(), 'No automatic retry or overwrite'
    folder.mkdir(parents=True)
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    p = read(r['source_protocol'])
    config = configuration(p, f'{family}_1701')
    end = r['score_end_s']
    config['sim_steps'] = round((end+600)/config['dt'])
    config['task_arrival_end_step'] = round(end/config['dt'])
    config['scheduled_task_arrivals'] = [e for e in config['scheduled_task_arrivals']
                                       if e['release_step']*config['dt'] < end]
    write(folder/'config.json', config)
    cfg = config_from_dict(config)
    actor = fixed.HierarchicalActor() if family == 'fixed' else dynamic.DynamicActor()
    actor.load_state_dict(torch.load(r['models'][family], map_location='cpu', weights_only=True)['state_dict'])
    actor.eval()
    # 预热仅网络，不消耗策略生成器，不改变仿真动作轨迹。
    with torch.no_grad():
        nodes = dict(schema=fixed.SCHEMA, nodes=[[0.]*14]*10, adjacency=[[True]*10]*10,
            node_valid=[True]*10, image=[0.]*8, image_mask=[True]*2,
            position_mask=[True]*10, block_count=16)
        obs = nodes if family == 'fixed' else dict(schema=dynamic.SCHEMA, variants=[nodes, nodes], image_mask=[True]*3)
        for _ in range(r['warmup_calls']):
            actor.logits(obs)
    env = GranularityEnv(cfg)
    plane = LocalInformationPlane(set(cfg.task_source_sat_ids))
    plane.history_hops = 2
    env.control_plane = plane
    policies = {s: fixed.CommittedPolicy(actor, 1701000+s) if family == 'fixed'
                else dynamic.DynamicPolicy(actor, 1701000+s, env, plane) for s in cfg.task_source_sat_ids}
    started = time.perf_counter()
    control_calls = []
    try:
        with Recorder(env, actor, family, folder/'decisions') as recorder:
            for step in range(cfg.sim_steps):
                actions = []
                offset = len(recorder.records)
                admission_offset = len(recorder.admissions)
                control = None
                if step % cfg.decision_interval_steps == 0:
                    call_offset = len(recorder.calls)
                    start = time.perf_counter()
                    state = env.export_state()
                    export_s = time.perf_counter()-start
                    start = time.perf_counter()
                    plane.enqueue(state)
                    enqueue_s = time.perf_counter()-start
                    start = time.perf_counter()
                    state = decision_state(state, cfg.decision_tile_limit_per_sat)
                    filter_s = time.perf_counter()-start
                    view_s = 0.
                    for source in sorted({t['source_sat_id'] for t in state.tiles.values()}):
                        start = time.perf_counter()
                        view = plane.view(state, source, 2)
                        view_s += time.perf_counter()-start
                        actions.extend(recorder.select(policies[source], view))
                    calls = recorder.calls[call_offset:]
                    control = dict(sim_seconds=env.time*cfg.dt, decisions=len(recorder.records)-offset,
                        actions=len(actions), state_export_s=export_s, control_enqueue_s=enqueue_s,
                        decision_filter_s=filter_s, local_view_s=view_s,
                        policy_s=sum(c['total_s'] for c in calls))
                env.step(actions)
                if control is not None:
                    control['executor_admission_s'] = sum(a['seconds'] for a in recorder.admissions[admission_offset:])
                    control['total_computation_s'] = sum(control[k] for k in
                        ('state_export_s', 'control_enqueue_s', 'decision_filter_s', 'local_view_s', 'policy_s', 'executor_admission_s'))
                    control_calls.append(control)
                recorder.flush(offset)
                if step % round(60/cfg.dt) == 0:
                    write(folder/'progress.json', dict(status='RUNNING', family=family,
                        sim_seconds=env.time*cfg.dt, total_seconds=cfg.sim_steps*cfg.dt,
                        decisions=len(recorder.records), wall_seconds=time.perf_counter()-started))
            checks = invariants(env)
            assert checks['passed'], checks
            files = sorted((folder/'decisions').glob('*.pkl.gz'))
            write(folder/'result.json', dict(status='PASS', family=family, checks=checks,
                records=len(files), decision_hashes={str(p): sha(p) for p in files},
                decision_timings=recorder.records, policy_calls=recorder.calls, control_batches=control_calls,
                executor_admissions=recorder.admissions, wall_seconds=time.perf_counter()-started,
                checkpoint=r['models'][family], checkpoint_sha256=sha(r['models'][family]),
                torch=torch.__version__, threads=torch.get_num_threads(), batch=1,
                capture_overhead_s=recorder.overhead_s,
                peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                registration_sha256=sha(ROOT/'registration.json')))
            write(folder/'progress.json', dict(status='COMPLETE', decisions=len(files)))
    finally:
        env.close()


def replay(family, device, threads, output):
    r = read(ROOT/'registration.json')
    torch.set_num_threads(threads)
    torch.set_num_interop_threads(1)
    actor = fixed.HierarchicalActor() if family == 'fixed' else dynamic.DynamicActor()
    assert sha(r['models'][family]) == r['hashes'][r['models'][family]]
    actor.load_state_dict(torch.load(r['models'][family], map_location='cpu', weights_only=True)['state_dict'])
    actor.to(device).eval()
    captured = read(ROOT/'capture'/family/'result.json')
    assert captured['status'] == 'PASS'
    data = []
    for path, expected in captured['decision_hashes'].items():
        assert sha(path) == expected, path
        with gzip.open(path, 'rb') as stream:
            row = pickle.load(stream)
        if r['score_start_s'] <= row['observation']['time_seconds'] < r['score_end_s']:
            data.append(row)
    assert data
    sync = torch.cuda.synchronize if device.startswith('cuda') else lambda: None
    observation_times, forward_times, forward_resident_times = [], [], []
    max_error = 0.
    with torch.no_grad():
        for i in range(r['warmup_calls']):
            row = data[i % len(data)]
            rebuild(row['snapshot'], family)
            actor.logits(tensors(row['observation'], device))
            sync()
        if device.startswith('cuda'):
            torch.cuda.reset_peak_memory_stats()
        for row in data:
            rebuilt = rebuild(row['snapshot'], family)
            assert rebuilt == row['observation'], 'Snapshot reconstruction differs'
            # 显式校验落盘张量与原观测完全相同，含全部mask。
            def equal(a, b):
                for key in FIELDS:
                    if key in a:
                        assert torch.equal(a[key], b[key]), key
                for av, bv in zip(a.get('variants', []), b.get('variants', [])):
                    equal(av, bv)
            equal(tensors(row['observation']), row['tensors'])
            resident = tensors(row['observation'], device)
            expected_probs = row.get('probabilities')
            actual_probs = probabilities(actor.logits(resident))
            if expected_probs is not None:
                for a, b in zip(actual_probs, expected_probs, strict=True):
                    max_error = max(max_error, float(np.max(np.abs(a-b))))
                    assert np.allclose(a, b, atol=r['probability_atol'], rtol=r['probability_rtol']), max_error
            for _ in range(r['repeats']):
                start = time.perf_counter()
                rebuild(row['snapshot'], family)
                observation_times.append(time.perf_counter()-start)
                sync(); start = time.perf_counter()
                actor.logits(row['tensors'])
                sync(); forward_times.append(time.perf_counter()-start)
                sync(); start = time.perf_counter()
                actor.logits(resident)
                sync(); forward_resident_times.append(time.perf_counter()-start)
    write(output, dict(status='PASS', family=family, device=device, threads=threads,
        batch=1, decisions=len(data), warmup=r['warmup_calls'], repeats=r['repeats'],
        observation_seconds=stats(observation_times),
        actor_host_inputs_seconds=stats(forward_times), actor_resident_inputs_seconds=stats(forward_resident_times),
        maximum_probability_absolute_error=max_error, snapshot_reconstruction='exact',
        torch=torch.__version__, numpy=np.__version__, python=sys.version,
        peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1024 if sys.platform=='linux' else 1),
        cuda_peak_allocated_bytes=torch.cuda.max_memory_allocated() if device.startswith('cuda') else None,
        cuda_peak_reserved_bytes=torch.cuda.max_memory_reserved() if device.startswith('cuda') else None,
        raw_observation_seconds=observation_times, raw_actor_host_seconds=forward_times,
        raw_actor_resident_seconds=forward_resident_times,
        registration_sha256=sha(ROOT/'registration.json')))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['prepare', 'capture', 'replay'])
    parser.add_argument('--family', choices=['fixed', 'dynamic'])
    parser.add_argument('--device', default='cpu')
    parser.add_argument('--threads', type=int, choices=[1, 2], default=1)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare()
    elif args.command == 'capture':
        assert args.family
        capture(args.family)
    else:
        assert args.family and args.output and not args.output.exists()
        replay(args.family, args.device, args.threads, args.output)


if __name__ == '__main__':
    main()
