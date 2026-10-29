"""回放校验按actor实际float32张量逐位比较，保留float64末位诊断。"""
import argparse
import gzip
import pickle
import resource
import sys
import time
import numpy as np
import torch
from analysis.method_cost_20261028 import (
    ROOT, FIELDS, read, write, sha, stats, tensors, rebuild, probabilities, fixed, dynamic,
)


def exact_actor_input(a, b):
    assert a.keys() == b.keys()
    error = 0.
    for key in a:
        if key in FIELDS:
            left, right = torch.as_tensor(a[key], dtype=FIELDS[key]), torch.as_tensor(b[key], dtype=FIELDS[key])
            assert torch.equal(left, right), key
            if FIELDS[key] == torch.float32:
                error = max(error, float(np.max(np.abs(np.asarray(a[key])-np.asarray(b[key])))))
        elif key == 'variants':
            assert len(a[key]) == len(b[key])
            for av, bv in zip(a[key], b[key]):
                error = max(error, exact_actor_input(av, bv))
        else:
            assert a[key] == b[key], key
    return error


def replay(family, device, threads, output):
    r = read(ROOT/'registration.json')
    torch.set_num_threads(threads); torch.set_num_interop_threads(1)
    actor = fixed.HierarchicalActor() if family == 'fixed' else dynamic.DynamicActor()
    assert sha(r['models'][family]) == r['hashes'][r['models'][family]]
    actor.load_state_dict(torch.load(r['models'][family], map_location='cpu', weights_only=True)['state_dict'])
    actor.to(device).eval()
    captured = read(ROOT/'capture'/family/'result.json'); assert captured['status'] == 'PASS'
    data = []
    for path, expected in captured['decision_hashes'].items():
        assert sha(path) == expected, path
        with gzip.open(path, 'rb') as stream:
            row = pickle.load(stream)
        if r['score_start_s'] <= row['observation']['time_seconds'] < r['score_end_s']:
            data.append(row)
    assert data
    sync = torch.cuda.synchronize if device.startswith('cuda') else lambda: None
    observation_times, forward_times, resident_times = [], [], []
    max_probability_error = max_reconstruction_error = 0.
    with torch.no_grad():
        for i in range(r['warmup_calls']):
            row = data[i % len(data)]
            rebuild(row['snapshot'], family)
            actor.logits(tensors(row['observation'], device)); sync()
        if device.startswith('cuda'):
            torch.cuda.reset_peak_memory_stats()
        for row in data:
            reconstructed = rebuild(row['snapshot'], family)
            max_reconstruction_error = max(max_reconstruction_error, exact_actor_input(reconstructed, row['observation']))
            exact_actor_input(tensors(row['observation']), row['tensors'])
            resident = tensors(row['observation'], device)
            expected = row.get('probabilities')
            actual = probabilities(actor.logits(resident))
            if expected is not None:
                for a, b in zip(actual, expected, strict=True):
                    max_probability_error = max(max_probability_error, float(np.max(np.abs(a-b))))
                    assert np.allclose(a, b, atol=r['probability_atol'], rtol=r['probability_rtol']), max_probability_error
            for _ in range(r['repeats']):
                start = time.perf_counter(); rebuild(row['snapshot'], family)
                observation_times.append(time.perf_counter()-start)
                sync(); start = time.perf_counter(); actor.logits(row['tensors']); sync()
                forward_times.append(time.perf_counter()-start)
                sync(); start = time.perf_counter(); actor.logits(resident); sync()
                resident_times.append(time.perf_counter()-start)
    write(output, dict(status='PASS', family=family, device=device, threads=threads, batch=1,
        decisions=len(data), warmup=r['warmup_calls'], repeats=r['repeats'],
        observation_seconds=stats(observation_times), actor_host_inputs_seconds=stats(forward_times),
        actor_resident_inputs_seconds=stats(resident_times), maximum_probability_absolute_error=max_probability_error,
        snapshot_reconstruction='Actor float32 tensors, masks and metadata exact',
        maximum_reconstruction_float64_error=max_reconstruction_error,
        torch=torch.__version__, numpy=np.__version__, python=sys.version,
        peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1024 if sys.platform=='linux' else 1),
        cuda_peak_allocated_bytes=torch.cuda.max_memory_allocated() if device.startswith('cuda') else None,
        cuda_peak_reserved_bytes=torch.cuda.max_memory_reserved() if device.startswith('cuda') else None,
        raw_observation_seconds=observation_times, raw_actor_host_seconds=forward_times,
        raw_actor_resident_seconds=resident_times, registration_sha256=sha(ROOT/'registration.json'),
        replay_source_sha256=sha(__file__), amendment_sha256=sha(ROOT/'REPLAY_CHECK_AMENDMENT.md')))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['replay'])
    parser.add_argument('--family', choices=['fixed', 'dynamic'], required=True)
    parser.add_argument('--device', default='cpu')
    parser.add_argument('--threads', type=int, choices=[1, 2], required=True)
    parser.add_argument('--output', type=__import__('pathlib').Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    replay(args.family, args.device, args.threads, args.output)
