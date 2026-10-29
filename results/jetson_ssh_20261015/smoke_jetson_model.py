#!/usr/bin/env python3
"""Jetson CUDA compatibility smoke check for one fixed optical workload window."""

import argparse
import gc
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import statistics
import struct
import sys
import time
import traceback
from datetime import datetime, timezone


EXPECTED_SHA256 = {
    "weights": "b62898ebf38940ca4df323863e45ee9d84a1a46d5d11ebdde529fb33aa9f3a32",
    "image": "8c5ada657cf8110a9f8aaac954c1dd96cde0187315b581276c32b0d1863e756f",
}
HEADER = struct.Struct("<4s6I")
RECORD = struct.Struct("<9fI")
WINDOW = 1024


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def save_report(path, report):
    path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def describe_error(exc):
    return {"type": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()}


def percentile(values, percent):
    ordered = sorted(values)
    position = (len(ordered) - 1) * percent / 100
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize(rows):
    if not rows:
        return {"count": 0}
    values = [row["service_ms"] for row in rows]
    return {
        "count": len(values), "mean_ms": statistics.mean(values),
        "p50_ms": percentile(values, 50), "p95_ms": percentile(values, 95),
        "min_ms": min(values), "max_ms": max(values),
        "detections_min": min(row["detections"] for row in rows),
        "detections_max": max(row["detections"] for row in rows),
    }


def serialize(polygons, scores, classes):
    if len(polygons) != len(scores) or len(polygons) != len(classes):
        raise ValueError("OBB result array lengths disagree")
    payload = bytearray(HEADER.pack(b"OBB1", 0, 0, 0, WINDOW, WINDOW, len(scores)))
    for polygon, score, cls in zip(polygons, scores, classes):
        coordinates = tuple(float(value) for value in polygon.reshape(-1))
        score, cls = float(score), float(cls)
        if len(coordinates) != 8 or not all(math.isfinite(value) for value in coordinates):
            raise ValueError("Invalid OBB coordinates")
        if not math.isfinite(score) or not 0 <= score <= 1:
            raise ValueError("Invalid OBB confidence")
        if not math.isfinite(cls) or cls != int(cls) or not 0 <= cls < 2 ** 32:
            raise ValueError("Invalid OBB class")
        payload.extend(RECORD.pack(*coordinates, score, int(cls)))
    return bytes(payload)


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weights", type=Path, required=True, help="Local official yolo11n-obb.pt")
    parser.add_argument("--image", type=Path, required=True, help="Local official boats.jpg")
    parser.add_argument("--output", type=Path, required=True, help="New output directory; must not exist")
    parser.add_argument("--warmup", type=positive_int, default=5)
    parser.add_argument("--repeats", type=positive_int, default=20)
    args = parser.parse_args()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        output.mkdir()
    except FileExistsError:
        parser.error("--output already exists; choose a new directory")
    report_path = output / "smoke_report.json"
    report = {
        "schema_version": 1, "started_at_utc": utc_now(), "status": "running",
        "purpose": "CUDA model compatibility and short timing smoke; not a calibration result",
        "python": platform.python_version(), "profiles": [],
        "protocol": {
            "model": "official YOLO11n-OBB", "image": "official optical boats.jpg",
            "window_xyxy": [0, 0, WINDOW, WINDOW], "batch": 1, "imgsz": WINDOW,
            "conf": 0.25, "iou": 0.7, "max_det": 300, "rect": False,
            "precision_order": ["fp32", "fp16"], "warmup": args.warmup,
            "repeats": args.repeats, "independent_model_per_precision": True,
            "wire_format": "OBB1: little endian <4s6I header plus <9fI records; 28 + 40*K bytes",
            "timing_boundary": "decoded RGB image -> crop/BGR -> predict including preprocessing, "
                               "inference and window OBB NMS -> all OBB fields on CPU -> OBB1 encoding",
            "excluded": ["file decode", "model load", "warmup", "report I/O", "global polygon NMS"],
            "limitations": ["Optical sample is not SAR calibration or recognition validation",
                            "Short smoke does not establish steady thermal performance or energy",
                            "No Shapely polygon validity check; framing matches existing OBB1 format",
                            "No simulation parameters are updated", "No download or package installation"],
        },
    }
    save_report(report_path, report)
    os.environ["YOLO_CONFIG_DIR"] = str(output / "ultralytics_config")
    (output / "ultralytics_config").mkdir()
    os.environ["YOLO_AUTOINSTALL"] = "false"
    os.environ["YOLO_OFFLINE"] = "true"
    os.environ["WANDB_MODE"] = "disabled"
    try:
        print("Checking local assets and CUDA runtime", flush=True)
        report["assets"] = {}
        for name, path in (("weights", args.weights), ("image", args.image)):
            path = path.resolve(strict=True)
            digest = sha256(path)
            report["assets"][name] = {"path": str(path), "bytes": path.stat().st_size, "sha256": digest}
            if digest != EXPECTED_SHA256[name]:
                raise ValueError("Unexpected {} SHA256; this smoke uses fixed official assets".format(name))
        import numpy as np
        from PIL import Image
        import torch
        from ultralytics import YOLO, settings

        settings.update({"sync": False})
        report["versions"] = {}
        for package in ("numpy", "Pillow", "torch", "torchvision", "ultralytics"):
            try:
                report["versions"][package] = importlib.metadata.version(package)
            except importlib.metadata.PackageNotFoundError:
                report["versions"][package] = None
        report["versions"]["torch_cuda_build"] = torch.version.cuda
        report["versions"]["cudnn"] = torch.backends.cudnn.version()
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA is not available in this Python environment")
        torch.cuda.set_device(0)
        properties = torch.cuda.get_device_properties(0)
        report["device"] = {
            "index": 0, "name": torch.cuda.get_device_name(0),
            "compute_capability": list(torch.cuda.get_device_capability(0)),
            "total_memory_bytes": properties.total_memory,
            "torch_cpu_threads": torch.get_num_threads(),
        }
        decode_start = time.perf_counter()
        with Image.open(args.image) as source:
            rgb = source.convert("RGB")
        report["decode_ms_excluded"] = (time.perf_counter() - decode_start) * 1000
        report["source_image"] = {"width": rgb.width, "height": rgb.height, "mode": rgb.mode}
        if rgb.width < WINDOW or rgb.height < WINDOW:
            raise ValueError("Source image must be at least 1024 pixels on both axes")
        save_report(report_path, report)

        for precision, half in (("fp32", False), ("fp16", True)):
            profile = {"precision": precision, "status": "running", "rows": [], "warmup_completed": 0}
            report["profiles"].append(profile)
            save_report(report_path, report)
            model = None
            print("Starting {}: {} warmup + {} measured windows".format(
                precision, args.warmup, args.repeats), flush=True)
            try:
                load_start = time.perf_counter()
                model = YOLO(str(args.weights.resolve()))
                if model.task != "obb":
                    raise ValueError("Expected an oriented bounding box model")
                model.to("cuda:0")
                model.model.half() if half else model.model.float()
                model.model.eval()
                torch.cuda.synchronize()
                profile["load_ms_excluded"] = (time.perf_counter() - load_start) * 1000
                profile["parameters_before_predict"] = sum(parameter.numel() for parameter in model.model.parameters())

                def one_window():
                    torch.cuda.synchronize()
                    start = time.perf_counter()
                    bgr = np.asarray(rgb.crop((0, 0, WINDOW, WINDOW)))[:, :, ::-1].copy()
                    cropped = time.perf_counter()
                    result = model.predict(
                        bgr, imgsz=WINDOW, device=0, half=half, batch=1,
                        conf=0.25, iou=0.7, max_det=300, rect=False,
                        save=False, verbose=False, augment=False,
                    )[0]
                    if result.obb is None:
                        raise ValueError("Model returned no OBB result object")
                    polygons = result.obb.xyxyxyxy.detach().cpu().numpy()
                    scores = result.obb.conf.detach().cpu().numpy()
                    classes = result.obb.cls.detach().cpu().numpy()
                    torch.cuda.synchronize()
                    predicted = time.perf_counter()
                    payload = serialize(polygons, scores, classes)
                    torch.cuda.synchronize()
                    finished = time.perf_counter()
                    return {
                        "crop_bgr_ms": (cropped - start) * 1000,
                        "predict_to_cpu_ms": (predicted - cropped) * 1000,
                        "encode_ms": (finished - predicted) * 1000,
                        "service_ms": (finished - start) * 1000,
                        "detections": len(scores), "input_rgb_bytes": int(bgr.nbytes),
                        "output_bytes": len(payload), "output_sha256": hashlib.sha256(payload).hexdigest(),
                    }

                with torch.inference_mode():
                    for _ in range(args.warmup):
                        one_window()
                        profile["warmup_completed"] += 1
                        save_report(report_path, report)
                    parameter = next(model.model.parameters())
                    profile["actual_parameter_dtype"] = str(parameter.dtype)
                    profile["actual_parameter_device"] = str(parameter.device)
                    profile["torch_cpu_threads_after_warmup"] = torch.get_num_threads()
                    expected_dtype = torch.float16 if half else torch.float32
                    if parameter.device.type != "cuda" or parameter.dtype != expected_dtype:
                        raise RuntimeError("Actual model device or dtype does not match requested profile")
                    torch.cuda.reset_peak_memory_stats()
                    for index in range(args.repeats):
                        row = one_window()
                        row["iteration"] = index
                        profile["rows"].append(row)
                        save_report(report_path, report)
                profile["peak_cuda_allocated_bytes"] = torch.cuda.max_memory_allocated()
                profile["peak_cuda_reserved_bytes"] = torch.cuda.max_memory_reserved()
                profile["status"] = "passed"
            except Exception as exc:
                profile["status"] = "failed"
                profile["error"] = describe_error(exc)
            finally:
                profile["summary"] = summarize(profile["rows"])
                save_report(report_path, report)
                model = None
                gc.collect()
                try:
                    torch.cuda.empty_cache()
                except Exception:
                    pass
            print("{}: {}; completed {} measured windows".format(
                precision, profile["status"], len(profile["rows"])), flush=True)
        report["status"] = "passed" if all(profile["status"] == "passed" for profile in report["profiles"]) else "failed"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = describe_error(exc)
    finally:
        report["finished_at_utc"] = utc_now()
        save_report(report_path, report)
        print("Result: {}; report: {}".format(report["status"], report_path), flush=True)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
