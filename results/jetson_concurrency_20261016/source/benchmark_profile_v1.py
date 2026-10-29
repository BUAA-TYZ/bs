"""Measure fixed SAR workload cohorts on Jetson without changing runtime settings."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import copy
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import threading
import time
import traceback

from calibrate_jetson_sar import (
    WEIGHTS_SHA256, encode_previews, load_workloads, percentile, polygon_nms,
    save_report, sha256_file, utc_now,
)

MODES = {"batch1": 1, "batch2": 2, "batch4": 4, "streams2": 2}


def memory_snapshot():
    values = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        if key in ("MemTotal", "MemAvailable", "MemFree", "SwapTotal", "SwapFree"):
            values[key] = int(value.split()[0]) * 1024
    for line in Path("/proc/vmstat").read_text().splitlines():
        key, value = line.split()
        if key in ("pswpin", "pswpout"):
            values[key] = int(value)
    values["monotonic_s"] = time.monotonic()
    return values


def worker(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    report = {"schema_version": 1, "status": "running", "started_at_utc": utc_now(),
              "mode": args.mode, "cohort_size": MODES[args.mode], "rows": [],
              "diagnostic": args.profile,
              "protocol": {"window_px": 4096, "precision": "float32", "cpu_threads": 1,
                           "warmup_windows": args.warmup, "measured_windows": args.windows,
                           "duration_s": args.duration, "model_copies": 2 if args.mode == "streams2" else 1,
                           "stream_workers": 2 if args.mode == "streams2" else 1,
                           "cohort_barrier": True, "wait_to_fill_cohort": False,
                           "boundary": "uint16 host raster through SRP1 encoding; cohort wall time includes dispatch",
                           "excludes": ["model loading", "warmup", "hashing", "log IO", "SAR focusing"],
                           "output_semantics": "fixed optical network plus controlled SAR postprocessing proxy"},
              "script_sha256": sha256_file(Path(__file__)),
              "baseline_script_sha256": sha256_file(Path(__file__).with_name("calibrate_jetson_sar.py")),
              "memory_before_import": memory_snapshot()}
    save_report(output / "report.json", report)
    samples, stop = [], threading.Event()

    def monitor():
        while not stop.is_set():
            samples.append(memory_snapshot())
            stop.wait(0.2)

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    pool = None
    try:
        if sha256_file(args.weights) != WEIGHTS_SHA256:
            raise ValueError("Unexpected weights checksum")
        os.environ["YOLO_CONFIG_DIR"] = str(output / "ultralytics_config")
        os.environ["YOLO_AUTOINSTALL"] = "false"
        os.environ["YOLO_OFFLINE"] = "true"
        os.environ["WANDB_MODE"] = "disabled"
        import numpy as np
        import torch
        from ultralytics import YOLO, settings

        settings.update({"sync": False})
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA required")
        torch.set_num_threads(1)
        torch.set_num_interop_threads(1)
        torch.cuda.set_device(0)
        report["versions"] = {name: importlib.metadata.version(name) for name in
                              ("torch", "torchvision", "ultralytics", "numpy", "shapely", "Pillow")}
        report["backend_flags"] = {"cudnn_allow_tf32": torch.backends.cudnn.allow_tf32,
                                   "matmul_allow_tf32": torch.backends.cuda.matmul.allow_tf32,
                                   "cudnn_benchmark": torch.backends.cudnn.benchmark,
                                   "cudnn_deterministic": torch.backends.cudnn.deterministic}
        report["weights_sha256"] = sha256_file(args.weights)
        report["fixture_sha256"] = sha256_file(args.workload_json)
        report["device"] = {"name": torch.cuda.get_device_name(),
                            "total_memory_bytes": torch.cuda.get_device_properties(0).total_memory}
        workloads = load_workloads(args.workload_json, 4096, 100)
        raster = np.random.default_rng(20261015).integers(0, 65536, (4096, 4096), dtype=np.uint16)
        report["raster_sha256"] = hashlib.sha256(raster.tobytes()).hexdigest()
        model = YOLO(str(args.weights)).model.float().to("cuda:0").eval()
        model.fuse(verbose=False)
        # OBB头前向会写入angle缓存；双流各持一个模型，避免共享可变状态。
        models = [model, copy.deepcopy(model)] if args.mode == "streams2" else [model]
        torch.cuda.synchronize()
        streams = [torch.cuda.Stream() for _ in models]
        report["memory_resident_model"] = memory_snapshot()

        @contextmanager
        def phase(name, phases, gpu=False):
            if not args.profile:
                yield
                return
            torch.cuda.nvtx.range_push(name)
            events = [torch.cuda.Event(enable_timing=True) for _ in range(2)] if gpu else None
            start = time.perf_counter()
            if events:
                events[0].record()
            try:
                yield
            finally:
                if events:
                    events[1].record()
                phases[name] = {"host_s": time.perf_counter() - start, "events": events}
                torch.cuda.nvtx.range_pop()

        def batch(items, index):
            stream = streams[index]
            phases = {}
            with torch.inference_mode(), torch.cuda.stream(stream):
                start = time.perf_counter()
                with phase("preprocess", phases):
                    grays = [np.right_shift(raster, 8).astype(np.uint8) for _ in items]
                    hosts = [np.ascontiguousarray(np.broadcast_to(gray, (3, 4096, 4096))) for gray in grays]
                    host = hosts[0][None] if len(items) == 1 else np.stack(hosts)
                with phase("upload_normalize", phases, gpu=True):
                    tensor = torch.from_numpy(host).to("cuda:0").float().div_(255)
                with phase("forward", phases, gpu=True):
                    raw = models[index](tensor)
                    prediction = raw[0] if isinstance(raw, tuple) else raw
                    if prediction.ndim != 3 or tuple(prediction.shape[:2]) != (len(items), 20):
                        raise ValueError("Unexpected decoded output shape")
                with phase("download", phases, gpu=True):
                    cpu = prediction.detach().cpu()
                stream.synchronize()
                outputs = []
                with phase("postprocess_encode", phases):
                    for gray, item in zip(grays, items):
                        kept = polygon_nms(item["candidates"])
                        if len(kept) != item["targets"]:
                            raise ValueError("Controlled output target mismatch")
                        payload, _ = encode_previews(gray, kept, 64, window_id=item["window_id"],
                                                    origin=item["origin"], roi_by_object=item["rois"])
                        outputs.append((item["window_id"], payload))
                end = time.perf_counter()
            # GPU事件仅用于诊断；额外同步、事件读取不进入原服务时间标定。
            if args.profile:
                stream.synchronize()
                for value in phases.values():
                    events = value.pop("events")
                    if events:
                        value["gpu_elapsed_s"] = events[0].elapsed_time(events[1]) / 1000
            return outputs, end - start, cpu, phases

        size = MODES[args.mode]
        initial = 1 if args.mode == "streams2" else size
        for index in range(len(models)):
            batch(workloads[:initial], index)
        if args.mode == "streams2":
            pool = ThreadPoolExecutor(max_workers=2)

        def cohort(items):
            if memory_snapshot()["MemAvailable"] < 1024 ** 3:
                raise RuntimeError("Less than 1 GiB available shared memory")
            start = time.perf_counter()
            if pool:
                futures = [pool.submit(batch, [item], i) for i, item in enumerate(items)]
                measured = [future.result() for future in futures]
            else:
                measured = [batch(items, 0)]
            elapsed = time.perf_counter() - start
            if any(not bool(torch.isfinite(cpu).all()) for _, _, cpu, _ in measured):
                raise ValueError("Nonfinite network predictions")
            outputs = [item for result, _, _, _ in measured for item in result]
            return elapsed, outputs, [duration for _, duration, _, _ in measured], [p for _, _, _, p in measured]

        print(f"{args.mode}: warmup", flush=True)
        for offset in range(0, args.warmup, size):
            cohort([workloads[(offset + j) % len(workloads)] for j in range(size)])
        torch.cuda.reset_peak_memory_stats()
        report["memory_before_measurement"] = memory_snapshot()
        if args.profile:
            torch.cuda.cudart().cudaProfilerStart()
            torch.cuda.nvtx.range_push("measured_service")
        report["measurement_epoch_start_s"] = time.time()
        started = last_update = time.perf_counter()
        count, hashes = 0, {}
        while (time.perf_counter() - started < args.duration if args.duration else count < args.windows):
            items = [workloads[(count + j) % len(workloads)] for j in range(size)]
            elapsed, outputs, durations, phases = cohort(items)
            row = {"index": len(report["rows"]), "windows": len(items), "cohort_s": elapsed,
                   "worker_service_s": durations, "elapsed_wall_s": time.perf_counter() - started,
                   "window_ids": [item["window_id"] for item in items]}
            if args.profile:
                row["worker_phases"] = phases
            for window_id, payload in outputs:
                value = {"sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}
                if str(window_id) in hashes and hashes[str(window_id)] != value:
                    raise ValueError("Output changed across repeated workload cycles")
                hashes[str(window_id)] = value
            report["rows"].append(row)
            count += len(items)
            if time.perf_counter() - last_update >= 30:
                print(f"{args.mode}: {count} windows, elapsed {time.perf_counter() - started:.1f}s", flush=True)
                save_report(output / "report.json", report)
                last_update = time.perf_counter()
        report["measurement_epoch_end_s"] = time.time()
        if args.profile:
            torch.cuda.synchronize()
            torch.cuda.nvtx.range_pop()
            torch.cuda.cudart().cudaProfilerStop()
        values = [row["cohort_s"] for row in report["rows"]]
        report["summary"] = {"windows": count, "cohorts": len(values),
                             "cohort_mean_s": statistics.mean(values), "cohort_p95_s": percentile(values, 95),
                             "windows_per_s": count / sum(values), "full_64_window_service_s": 64 * sum(values) / count,
                             "measured_wall_s": time.perf_counter() - started}
        report["output_hashes"] = hashes
        report["cuda_memory"] = {"peak_allocated_bytes": torch.cuda.max_memory_allocated(),
                                 "peak_reserved_bytes": torch.cuda.max_memory_reserved(),
                                 "resident_allocated_bytes": torch.cuda.memory_allocated(),
                                 "resident_reserved_bytes": torch.cuda.memory_reserved()}
        report["status"] = "passed"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = {"type": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()}
    finally:
        if pool:
            pool.shutdown(wait=True)
        stop.set()
        thread.join(timeout=2)
        samples.append(memory_snapshot())
        report["memory_samples"] = samples
        report["memory"] = {"min_available_bytes": min(item["MemAvailable"] for item in samples),
                            "max_unavailable_bytes": max(item["MemTotal"] - item["MemAvailable"] for item in samples),
                            "swap_in_pages": samples[-1]["pswpin"] - samples[0]["pswpin"],
                            "swap_out_pages": samples[-1]["pswpout"] - samples[0]["pswpout"],
                            "process_max_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                            "scope": "System-wide MemAvailable observation, not exact per-process physical allocation"}
        report["finished_at_utc"] = utc_now()
        save_report(output / "report.json", report)
        print(json.dumps({"mode": args.mode, "status": report["status"], "summary": report.get("summary"),
                          "memory": report["memory"], "error": report.get("error")}), flush=True)
    return 0 if report["status"] == "passed" else 1


def series(args):
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"schema_version": 1, "started_at_utc": utc_now(), "status": "running", "runs": [],
              "hypothesis": "Batching or two streams improves 64-window throughput by at least 5% without swap or OOM",
              "selection": "Reject failures/swap; require >=5% paired speedup in all three rounds; otherwise retain batch1",
              "time_budget_s": 1800, "warmup_windows": args.warmup, "measured_windows": args.windows}
    start = time.monotonic()
    failed = set()
    modes = list(MODES)
    for repeat in range(3):
        order = modes[repeat:] + modes[:repeat]
        for mode in order:
            if mode in failed:
                continue
            remaining = 1800 - (time.monotonic() - start)
            if remaining < 90:
                report["status"] = "budget_exhausted"
                save_report(args.output / "series.json", report)
                return 1
            name = f"round{repeat + 1}_{mode}"
            command = [sys.executable, str(Path(__file__).resolve()), "--mode", mode,
                       "--weights", str(args.weights), "--workload-json", str(args.workload_json),
                       "--output", str(args.output / name), "--warmup", str(args.warmup),
                       "--windows", str(args.windows)]
            print(f"Starting {name}: {utc_now()}", flush=True)
            with (args.output / (name + ".log")).open("w") as log:
                try:
                    result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=min(240, remaining))
                    code = result.returncode
                except subprocess.TimeoutExpired:
                    code = -1
            item = {"name": name, "mode": mode, "round": repeat + 1, "returncode": code}
            report["runs"].append(item)
            if code:
                failed.add(mode)
            save_report(args.output / "series.json", report)
            print(json.dumps(item), flush=True)
    report["status"] = "completed_with_failed_modes" if failed else "passed"
    report["finished_at_utc"] = utc_now()
    save_report(args.output / "series.json", report)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weights", required=True, type=Path)
    parser.add_argument("--workload-json", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--mode", choices=MODES)
    parser.add_argument("--warmup", type=int, default=12)
    parser.add_argument("--windows", type=int, default=64)
    parser.add_argument("--duration", type=float, default=0)
    parser.add_argument("--profile", action="store_true", help="Diagnostic NVTX/CUDA events; timings are not calibration")
    args = parser.parse_args()
    if args.windows <= 0 or args.windows % 4 or args.warmup < 0 or args.warmup % 4 or args.duration < 0:
        parser.error("Window counts must be multiples of four; measured count must be positive")
    return worker(args) if args.mode else series(args)


if __name__ == "__main__":
    sys.exit(main())
