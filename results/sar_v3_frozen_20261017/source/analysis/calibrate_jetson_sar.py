#!/usr/bin/env python3
"""CUDA service calibration using a fixed network and explicit synthetic SAR workload."""

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import resource
import statistics
import struct
import sys
import time
import traceback


WEIGHTS_SHA256 = "b62898ebf38940ca4df323863e45ee9d84a1a46d5d11ebdde529fb33aa9f3a32"
HEADER = struct.Struct("<4s6I")
RECORD = struct.Struct("<9fI")
ROI_HEADER = struct.Struct("<5I")
PREVIEW_SIDE = 512
PHASES = ("preprocess_ms", "host_to_device_ms", "forward_ms", "device_to_host_ms",
          "controlled_nms_ms", "preview_encode_ms")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def save_report(path, report):
    path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def percentile(values, percent):
    ordered = sorted(values)
    position = (len(ordered) - 1) * percent / 100
    lower, upper = math.floor(position), math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize(rows):
    if not rows:
        return {"count": 0}
    values = [row["service_ms"] for row in rows]
    total = sum(values) / 1000
    return {
        "count": len(rows), "total_service_s": total,
        "mean_ms": statistics.mean(values), "p50_ms": percentile(values, 50),
        "p95_ms": percentile(values, 95), "min_ms": min(values), "max_ms": max(values),
        "service_rate_windows_per_s": len(rows) / total,
        "phase_mean_ms": {phase: statistics.mean(row[phase] for row in rows) for phase in PHASES},
        "output_bytes_min": min(row["output_bytes"] for row in rows),
        "output_bytes_max": max(row["output_bytes"] for row in rows),
    }


@dataclass(frozen=True)
class Detection:
    detection_id: int
    class_id: int
    score: float
    xy: tuple
    object_id: int = -1


def make_candidates(window, count, targets, length_px, width_px, angle_deg):
    if targets == 0:
        if count != 0:
            raise ValueError("Zero targets requires zero candidates")
        return []
    if count < targets:
        raise ValueError("Candidates must be at least the number of targets")
    columns = math.ceil(math.sqrt(targets))
    rows = math.ceil(targets / columns)
    angle = math.radians(angle_deg)
    cosine, sine = math.cos(angle), math.sin(angle)
    candidates = []
    for index in range(count):
        target, duplicate = index % targets, index // targets
        cx = (target % columns + 0.5) * window / columns
        cy = (target // columns + 0.5) * window / rows
        # 每组首框最高分且无抖动，确保字节账单不随NMS执行顺序变化。
        shift = 0 if duplicate == 0 else (duplicate % 7 - 3) * 0.1
        coordinates = []
        for x, y in ((-length_px / 2, -width_px / 2), (length_px / 2, -width_px / 2),
                     (length_px / 2, width_px / 2), (-length_px / 2, width_px / 2)):
            coordinates.extend((cx + cosine * x - sine * y + shift,
                                cy + sine * x + cosine * y + shift))
        if min(coordinates) < 0 or max(coordinates) > window:
            raise ValueError("Controlled target does not fit the window")
        candidates.append(Detection(index, 0, 0.99 - 0.5 * duplicate / count, tuple(coordinates)))
    return candidates


def load_workloads(path, window, candidate_count):
    fixture = json.loads(path.read_text(encoding="utf-8"))
    workloads = []
    seen_windows = set()
    for source in fixture["windows"]:
        window_id = int(source["window_id"])
        if window_id in seen_windows:
            raise ValueError("Duplicate fixture window ID")
        seen_windows.add(window_id)
        records = source["records"]
        if len(records) > candidate_count:
            raise ValueError("Fixture has more targets than controlled candidates")
        rois, bases = {}, []
        for record in records:
            object_id = int(record["object_id"])
            if object_id in rois:
                raise ValueError("Duplicate object ID inside fixture window")
            vertices = tuple(float(value) for point in record["vertices"] for value in point)
            if len(vertices) != 8 or not all(math.isfinite(value) and 0 <= value <= window for value in vertices):
                raise ValueError("Fixture OBB must fit the inference window")
            roi = tuple(int(value) for value in record["roi_xyxy"])
            if len(roi) != 4 or not (0 <= roi[0] < roi[2] <= window and 0 <= roi[1] < roi[3] <= window):
                raise ValueError("Fixture ROI must fit the inference window")
            if not (roi[0] <= min(vertices[::2]) <= max(vertices[::2]) <= roi[2] and
                    roi[1] <= min(vertices[1::2]) <= max(vertices[1::2]) <= roi[3]):
                raise ValueError("Fixture ROI does not contain its complete OBB")
            score = float(record["score"])
            class_id = int(record["class_id"])
            if not 0 < score <= 1 or class_id < 0:
                raise ValueError("Invalid fixture score or class")
            rois[object_id] = roi
            bases.append(Detection(len(bases), class_id, score, vertices, object_id))
        candidates = []
        for index in range(candidate_count if bases else 0):
            base = bases[index % len(bases)]
            duplicate = index // len(bases)
            # 重复框完全重合；仅降低置信度，保证保留fixture给出的原始包围框。
            candidates.append(Detection(index, base.class_id, base.score / (1 + duplicate * 0.01),
                                        base.xy, base.object_id))
        workloads.append({"window_id": window_id, "origin": (int(source["x0"]), int(source["y0"])),
                          "targets": len(bases), "candidates": candidates, "rois": rois})
    if not workloads:
        raise ValueError("Fixture must contain at least one window")
    return workloads


def polygon_nms(candidates, threshold=0.7):
    from shapely.geometry import Polygon

    ordered = sorted(candidates, key=lambda item: (-item.score, item.detection_id))
    polygons = [Polygon(list(zip(item.xy[::2], item.xy[1::2]))) for item in ordered]
    if any(not polygon.is_valid or polygon.area <= 0 for polygon in polygons):
        raise ValueError("Controlled candidate has invalid geometry")
    kept = []
    for index, candidate in enumerate(ordered):
        accept = True
        for other in kept:
            if candidate.class_id != ordered[other].class_id:
                continue
            intersection = polygons[index].intersection(polygons[other]).area
            union = polygons[index].area + polygons[other].area - intersection
            if intersection / union > threshold:
                accept = False
                break
        if accept:
            kept.append(index)
    return [ordered[index] for index in kept]


def preview_geometry(width, height):
    if width <= 0 or height <= 0:
        raise ValueError("Empty source ROI")
    scale = min(PREVIEW_SIDE / width, PREVIEW_SIDE / height)
    resized = (max(1, int(round(width * scale))), max(1, int(round(height * scale))))
    offset = ((PREVIEW_SIDE - resized[0]) // 2, (PREVIEW_SIDE - resized[1]) // 2)
    return resized, offset


def encode_previews(gray_u8, detections, context_px, *, window_id=0, origin=(0, 0), roi_by_object=None):
    from PIL import Image

    height, width = gray_u8.shape
    payload = bytearray(HEADER.pack(b"SRP1", window_id, *origin, width, height, len(detections)))
    rois = []
    for detection in detections:
        xs, ys = detection.xy[::2], detection.xy[1::2]
        box = (roi_by_object[detection.object_id] if roi_by_object is not None else
               (max(0, math.floor(min(xs)) - context_px),
                max(0, math.floor(min(ys)) - context_px),
                min(width, math.ceil(max(xs)) + context_px),
                min(height, math.ceil(max(ys)) + context_px)))
        x0, y0, x1, y1 = box
        resized, offset = preview_geometry(x1 - x0, y1 - y0)
        crop = Image.fromarray(gray_u8[y0:y1, x0:x1])
        preview = Image.new("L", (PREVIEW_SIDE, PREVIEW_SIDE))
        preview.paste(crop.resize(resized, resample=Image.Resampling.BILINEAR), offset)
        raw = preview.tobytes()
        payload.extend(RECORD.pack(*detection.xy, detection.score, detection.class_id))
        payload.extend(ROI_HEADER.pack(*box, len(raw)))
        payload.extend(raw)
        rois.append({"object_id": detection.object_id, "source_box_xyxy": list(box), "resized_wh": list(resized),
                     "padding_left_top": list(offset), "preview_bytes": len(raw)})
    return bytes(payload), rois


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def nonnegative_int(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return number


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weights", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path, help="New directory; never overwritten")
    parser.add_argument("--workload-json", type=Path, help="Fixed full-image geometry; cycle through its windows")
    parser.add_argument("--window", type=positive_int, default=4096)
    parser.add_argument("--warmup", type=nonnegative_int, default=10)
    parser.add_argument("--repeats", type=positive_int, default=30)
    parser.add_argument("--duration-sec", type=positive_float, help="Continuous timed phase instead of repeats")
    parser.add_argument("--targets", type=nonnegative_int, default=2)
    parser.add_argument("--candidates", type=nonnegative_int, default=100)
    parser.add_argument("--pixel-spacing-m", type=positive_float, default=0.25)
    parser.add_argument("--ship-length-m", type=positive_float, default=200)
    parser.add_argument("--ship-width-m", type=positive_float, default=32)
    parser.add_argument("--context-m", type=positive_float, default=16)
    parser.add_argument("--angle-deg", type=float, default=30)
    parser.add_argument("--seed", type=nonnegative_int, default=20261015)
    args = parser.parse_args()
    if args.window % 32 or not math.isfinite(args.angle_deg):
        parser.error("Window must be divisible by 32 and angle must be finite")
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        output.mkdir()
    except FileExistsError:
        parser.error("--output already exists")
    report_path = output / "report.json"
    report = {
        "schema_version": 1, "started_at_utc": utc_now(), "status": "running",
        "purpose": "Composition-based real-device proxy calibration; not SAR accuracy or flight hardware measurement",
        "python": platform.python_version(), "command": sys.argv,
        "protocol": {
            "window_px": args.window, "pixel_spacing_m": args.pixel_spacing_m,
            "input": "One seeded uniform-random uint16 amplitude raster; synthetic, not an observed SAR product",
            "input_seed": args.seed, "work_unit": "1 native-scale window = 1 U",
            "precision": "fp32", "batch": 1, "threads": 1, "warmup": args.warmup,
            "repeats": None if args.duration_sec else args.repeats, "duration_sec": args.duration_sec,
            "normalization": "uint16 >> 8 to uint8; replicated 3 channels; float32 / 255 on CUDA",
            "resize_for_inference": False, "model": "official YOLO11n-OBB, 15-class optical proxy",
            "controlled_targets": None if args.workload_json else args.targets, "controlled_candidates": args.candidates,
            "ship_length_m": args.ship_length_m, "ship_width_m": args.ship_width_m,
            "angle_deg": args.angle_deg, "context_m": args.context_m, "window_nms_iou": 0.7,
            "result_format": "SRP1: 28-byte <4s6I header; each target has 40-byte <9fI OBB, "
                             "20-byte <5I local source ROI xyxy and byte length, then 512x512 uint8 raw preview; "
                             "header stores window ID and global origin; OBB coordinates are local",
            "preview": "Full axis-aligned OBB envelope plus context, clipped to source, aspect-preserving "
                       "bilinear resize and centered zero padding to 512; source coordinates enable reconstruction",
            "timing_boundary": "host uint16 raster -> preprocessing -> CUDA transfer/normalization -> "
                               "fused FP32 forward including head decode -> full decoded prediction to CPU -> "
                               "controlled polygon NMS -> ROI preview and SRP1 encoding",
            "excluded": ["raster generation", "model load", "warmup", "result hashing", "log I/O",
                         "original file decode", "inter-window/global aggregation", "network transmission"],
            "limitations": ["Synthetic pixels and controlled candidates are engineering assumptions",
                            "Optical predictions are discarded and do not estimate SAR accuracy or target density",
                            "Preview downsampling preserves the complete crop but not native image detail",
                            "One resident model and serial window execution; no flight resource contention model",
                            "No package installs, downloads, power-mode changes or clock changes"],
        },
    }
    save_report(report_path, report)
    os.environ["YOLO_CONFIG_DIR"] = str(output / "ultralytics_config")
    (output / "ultralytics_config").mkdir()
    os.environ["YOLO_AUTOINSTALL"] = "false"
    os.environ["YOLO_OFFLINE"] = "true"
    os.environ["WANDB_MODE"] = "disabled"
    rows = []
    try:
        weights = args.weights.resolve(strict=True)
        digest = sha256_file(weights)
        if digest != WEIGHTS_SHA256:
            raise ValueError("Unexpected model checksum")
        report["weights"] = {"path": str(weights), "sha256": digest, "bytes": weights.stat().st_size}
        report["script_sha256"] = sha256_file(Path(__file__).resolve())
        import numpy as np
        import torch
        from ultralytics import YOLO, settings

        settings.update({"sync": False})
        report["versions"] = {name: importlib.metadata.version(name) for name in
                              ("numpy", "Pillow", "torch", "torchvision", "ultralytics", "shapely")}
        report["versions"].update({"torch_cuda_build": torch.version.cuda,
                                    "cudnn": torch.backends.cudnn.version()})
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA unavailable; CPU fallback is prohibited")
        torch.set_num_threads(1)
        torch.set_num_interop_threads(1)
        torch.cuda.set_device(0)
        report["device"] = {"name": torch.cuda.get_device_name(0),
                            "total_memory_bytes": torch.cuda.get_device_properties(0).total_memory,
                            "compute_capability": list(torch.cuda.get_device_capability(0)),
                            "torch_cpu_threads": torch.get_num_threads()}
        report["backend_flags"] = {"cudnn_benchmark": torch.backends.cudnn.benchmark,
                                   "cudnn_deterministic": torch.backends.cudnn.deterministic,
                                   "cudnn_allow_tf32": torch.backends.cudnn.allow_tf32,
                                   "matmul_allow_tf32": torch.backends.cuda.matmul.allow_tf32}
        raster = np.random.default_rng(args.seed).integers(0, 65536, (args.window, args.window), dtype=np.uint16)
        report["raster"] = {"bytes": raster.nbytes, "sha256": hashlib.sha256(raster.tobytes()).hexdigest(),
                            "dtype": str(raster.dtype), "shape": list(raster.shape)}
        context_px = math.ceil(args.context_m / args.pixel_spacing_m)
        report["protocol"]["context_px"] = context_px
        if args.workload_json:
            workload_path = args.workload_json.resolve(strict=True)
            workloads = load_workloads(workload_path, args.window, args.candidates)
            report["workload_fixture"] = {"path": str(workload_path), "sha256": sha256_file(workload_path),
                                           "windows": len(workloads),
                                           "targets_per_cycle": sum(item["targets"] for item in workloads),
                                           "targets_per_window": [item["targets"] for item in workloads]}
            report["protocol"]["controlled_geometry"] = "OBBs, ROI boxes and context are authoritative from workload fixture"
        else:
            candidates = make_candidates(args.window, args.candidates, args.targets,
                                         args.ship_length_m / args.pixel_spacing_m,
                                         args.ship_width_m / args.pixel_spacing_m, args.angle_deg)
            workloads = [{"window_id": 0, "origin": (0, 0), "targets": args.targets,
                          "candidates": candidates, "rois": None}]
        load_start = time.perf_counter()
        wrapper = YOLO(str(weights))
        if wrapper.task != "obb":
            raise ValueError("Expected OBB network")
        model = wrapper.model.float().to("cuda:0").eval()
        model.fuse(verbose=False)
        torch.cuda.synchronize()
        report["model_load_ms_excluded"] = (time.perf_counter() - load_start) * 1000
        parameter = next(model.parameters())
        report["model"] = {"parameters": sum(item.numel() for item in model.parameters()),
                           "dtype": str(parameter.dtype), "device": str(parameter.device),
                           "head_classes": int(model.model[-1].nc), "fused": True}
        if parameter.dtype != torch.float32 or parameter.device.type != "cuda":
            raise RuntimeError("Actual model dtype/device differs from protocol")
        save_report(report_path, report)

        def one_window(workload):
            torch.cuda.synchronize()
            stamps = [time.perf_counter()]
            gray = np.right_shift(raster, 8).astype(np.uint8)
            host = np.ascontiguousarray(np.broadcast_to(gray, (3, args.window, args.window)))
            stamps.append(time.perf_counter())
            tensor = torch.from_numpy(host).unsqueeze(0).to("cuda:0").float().div_(255)
            torch.cuda.synchronize()
            stamps.append(time.perf_counter())
            raw = model(tensor)
            prediction = raw[0] if isinstance(raw, tuple) else raw
            if not isinstance(prediction, torch.Tensor) or prediction.ndim != 3 or prediction.shape[1] != 20:
                raise ValueError("Expected decoded OBB output [1,20,anchors]")
            torch.cuda.synchronize()
            stamps.append(time.perf_counter())
            cpu_prediction = prediction.detach().cpu()
            torch.cuda.synchronize()
            stamps.append(time.perf_counter())
            kept = polygon_nms(workload["candidates"])
            if len(kept) != workload["targets"]:
                raise ValueError("Controlled polygon NMS did not preserve the requested target count")
            stamps.append(time.perf_counter())
            payload, rois = encode_previews(gray, kept, context_px, window_id=workload["window_id"],
                                           origin=workload["origin"], roi_by_object=workload["rois"])
            stamps.append(time.perf_counter())
            row = {phase: (stamps[index + 1] - stamps[index]) * 1000 for index, phase in enumerate(PHASES)}
            row.update({"service_ms": (stamps[-1] - stamps[0]) * 1000,
                        "output_bytes": len(payload), "targets": len(kept),
                        "window_id": workload["window_id"], "window_origin_xy": list(workload["origin"]),
                        "decoded_output_shape": list(cpu_prediction.shape),
                        "decoded_output_bytes": cpu_prediction.numel() * cpu_prediction.element_size(),
                        "output_sha256": hashlib.sha256(payload).hexdigest(), "rois": rois})
            return row

        with torch.inference_mode():
            print("Warmup: {} windows at {}x{}, FP32".format(args.warmup, args.window, args.window), flush=True)
            for index in range(args.warmup):
                one_window(workloads[index % len(workloads)])
                report["warmup_completed"] = index + 1
                save_report(report_path, report)
            torch.cuda.reset_peak_memory_stats()
            start = time.perf_counter()
            last_update = start
            with (output / "windows.jsonl").open("w", encoding="utf-8") as log:
                while (time.perf_counter() - start < args.duration_sec if args.duration_sec else len(rows) < args.repeats):
                    row = one_window(workloads[len(rows) % len(workloads)])
                    row.update({"iteration": len(rows), "finished_at_utc": utc_now(),
                                "elapsed_wall_s": time.perf_counter() - start})
                    rows.append(row)
                    log.write(json.dumps(row, allow_nan=False) + "\n")
                    log.flush()
                    if time.perf_counter() - last_update >= 30:
                        report["summary"] = summarize(rows)
                        save_report(report_path, report)
                        print("Measured {} windows; mean {:.3f} ms; elapsed {:.1f} s".format(
                            len(rows), report["summary"]["mean_ms"], row["elapsed_wall_s"]), flush=True)
                        last_update = time.perf_counter()
            report["measured_wall_s"] = time.perf_counter() - start
        report["workload_cycles"] = {"completed": len(rows) // len(workloads),
                                     "partial_cycle_windows": len(rows) % len(workloads)}
        report["memory"] = {"peak_cuda_allocated_bytes": torch.cuda.max_memory_allocated(),
                            "peak_cuda_reserved_bytes": torch.cuda.max_memory_reserved(),
                            "process_max_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                            "rss_convention": "Linux ru_maxrss KiB; process lifetime maximum"}
        report["status"] = "passed"
    except (Exception, KeyboardInterrupt) as exc:
        report["status"] = "failed"
        report["error"] = {"type": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()}
    finally:
        report["summary"] = summarize(rows)
        minutes = {}
        for row in rows:
            minutes.setdefault(int(row["elapsed_wall_s"] // 60), []).append(row)
        report["minute_bins"] = [{"minute": minute, **summarize(items)} for minute, items in sorted(minutes.items())]
        report["finished_at_utc"] = utc_now()
        save_report(report_path, report)
        print("{}: {}; summary: {}".format(report["status"], report_path, report["summary"]), flush=True)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
