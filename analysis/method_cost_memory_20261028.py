"""Orin四调度块输入缓存与真实双流窗口推理的独立峰值测量。"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import json
from pathlib import Path
import resource
import subprocess
import threading
import time

import numpy as np
import torch

from analysis.method_cost_20261028 import write, sha, stats
from analysis.calibrate_jetson_sar import WEIGHTS_SHA256, load_workloads, polygon_nms, encode_previews


def memory():
    out = {}
    for filename in ('/proc/meminfo', '/proc/self/status'):
        for line in Path(filename).read_text().splitlines():
            key = line.split(':')[0]
            if key in ('MemTotal', 'MemAvailable', 'SwapFree', 'SwapTotal', 'VmRSS', 'VmHWM'):
                out[key] = int(line.split()[1])*1024
    for line in Path('/proc/vmstat').read_text().splitlines():
        key, value = line.split()
        if key in ('pswpin', 'pswpout'):
            out[key] = int(value)
    out['time'] = time.time()
    return out


def run(args):
    assert not args.output.exists()
    args.output.mkdir(parents=True)
    assert sha(args.weights) == WEIGHTS_SHA256
    power = subprocess.check_output(['nvpmodel', '-q'], text=True)
    assert 'MAXN' in power and power.strip().endswith('0'), power
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    torch.cuda.set_device(0)
    telemetry_file = (args.output/'tegrastats.log').open('w')
    telemetry = subprocess.Popen(['tegrastats', '--interval', '100'], stdout=telemetry_file, stderr=subprocess.STDOUT)
    samples, stop = [], threading.Event()
    def monitor():
        while not stop.is_set():
            samples.append(memory())
            stop.wait(.05)
    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    report = dict(status='RUNNING', power=power, mode=args.mode, torch=torch.__version__,
        cpu_threads=1, precision='FP32', batch=1, clock_policy='Unchanged dynamic clocks, no jetson_clocks lock',
        geometry_sha256=sha(args.geometry), weights_sha256=sha(args.weights), fixture_sha256=sha(args.fixture),
        before=memory(), warmup_windows=12, measured_passes=3,
        scope='Four materialized uint16 scheduling-block input rectangles resident; window-at-a-time preprocessing; batch1 or two independent batch1 streams. No full original raster retained; no unrelated queue cache filled.',
        hardware_proxy='Same optical YOLO11n-OBB and controlled SAR output proxy as service calibration; not SAR accuracy evidence')
    write(args.output/'report.json', report)
    try:
        geometry = json.loads(args.geometry.read_text())
        blocks, jobs = [], []
        rng = np.random.default_rng(20261028)
        for tile in geometry['tiles']:
            windows = tile['windows']
            x0, y0 = min(w[0] for w in windows), min(w[1] for w in windows)
            x1, y1 = max(w[2] for w in windows), max(w[3] for w in windows)
            blocks.append(rng.integers(0, 65536, (y1-y0, x1-x0), dtype=np.uint16))
            jobs.extend((len(blocks)-1, x-x0, y-y0) for x, y, _, _ in windows)
        assert len(blocks) == 4
        report['input_buffers_bytes'] = sum(b.nbytes for b in blocks)
        report['windows'] = len(jobs)
        report['input_resident'] = memory()
        def preprocess(job):
            block, x, y = job
            raster = blocks[block][y:y+4096, x:x+4096]
            assert raster.shape == (4096, 4096)
            gray = np.right_shift(raster, 8).astype(np.uint8)
            host = np.ascontiguousarray(np.broadcast_to(gray, (3, 4096, 4096)))
            return gray, host
        preprocessing = []
        for job in jobs:
            start = time.perf_counter()
            gray, host = preprocess(job)
            preprocessing.append(time.perf_counter()-start)
            del gray, host
        report['preprocessing_seconds'] = stats(preprocessing)
        report['after_preprocessing'] = memory()
        from ultralytics import YOLO, settings
        settings.update({'sync': False})
        model = YOLO(str(args.weights)).model.float().cuda().eval()
        model.fuse(verbose=False)
        models = [model, copy.deepcopy(model)] if args.mode == 'streams2' else [model]
        streams = [torch.cuda.Stream() for _ in models]
        workloads = load_workloads(args.fixture, 4096, 100)
        def execute(index, job, item):
            stream = streams[index]
            with torch.inference_mode(), torch.cuda.stream(stream):
                gray, host = preprocess(job)
                tensor = torch.from_numpy(host[None]).cuda().float().div_(255)
                result = models[index](tensor)
                prediction = result[0] if isinstance(result, tuple) else result
                cpu = prediction.detach().cpu()
                stream.synchronize()
                assert torch.isfinite(cpu).all()
                kept = polygon_nms(item['candidates'])
                assert len(kept) == item['targets']
                payload, _ = encode_previews(gray, kept, 64, window_id=item['window_id'],
                    origin=item['origin'], roi_by_object=item['rois'])
                return hashlib.sha256(payload).hexdigest()
        width = len(models)
        times, hashes = [], []
        with ThreadPoolExecutor(max_workers=width) as pool:
            for i in range(12):
                execute(i % width, jobs[i % len(jobs)], workloads[i % len(workloads)])
            torch.cuda.synchronize()
            torch.cuda.reset_peak_memory_stats()
            report['measurement_start'] = memory()
            for repetition in range(3):
                for offset in range(0, len(jobs), width):
                    assert memory()['MemAvailable'] > 1024**3
                    start = time.perf_counter()
                    futures = [pool.submit(execute, i, jobs[j], workloads[j % len(workloads)])
                               for i, j in enumerate(range(offset, min(offset+width, len(jobs))))]
                    hashes.extend(f.result() for f in futures)
                    times.append(time.perf_counter()-start)
        assert hashes[:len(jobs)] == hashes[len(jobs):2*len(jobs)] == hashes[2*len(jobs):]
        report.update(status='PASS', service_cohort_seconds=stats(times), output_hashes=hashes,
            measurement_end=memory(), cuda_peak_allocated_bytes=torch.cuda.max_memory_allocated(),
            cuda_peak_reserved_bytes=torch.cuda.max_memory_reserved(),
            cudnn_tf32=torch.backends.cudnn.allow_tf32, matmul_tf32=torch.backends.cuda.matmul.allow_tf32)
    except Exception:
        import traceback
        report.update(status='FAILED', traceback=traceback.format_exc())
        raise
    finally:
        stop.set(); thread.join(timeout=2)
        telemetry.terminate(); telemetry.wait(timeout=10); telemetry_file.close()
        samples.append(memory())
        report.update(samples=samples, peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
            min_system_available_bytes=min(s['MemAvailable'] for s in samples),
            swap_in_pages=samples[-1]['pswpin']-samples[0]['pswpin'],
            swap_out_pages=samples[-1]['pswpout']-samples[0]['pswpout'],
            memory_note='Unified RAM: RSS, CUDA reservation and system usage overlap; do not add them.')
        write(args.output/'report.json', report)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--geometry', required=True, type=Path)
    p.add_argument('--weights', required=True, type=Path)
    p.add_argument('--fixture', required=True, type=Path)
    p.add_argument('--mode', choices=['batch1', 'streams2'], required=True)
    p.add_argument('--output', required=True, type=Path)
    run(p.parse_args())
