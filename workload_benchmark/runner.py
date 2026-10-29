"""固定滑窗、官方旋转检测器和全图合并的可复算本机测量。"""

from dataclasses import asdict
from datetime import datetime, timezone
import csv
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

import numpy as np
from PIL import Image, ImageDraw
import psutil
import torch

from sim.workload import ImageTiling, window_starts
from workload_benchmark.assets import ASSETS, prepare_assets, prepare_dota_mirror, sha256
from workload_benchmark.postprocess import (
    Detection, deserialize_detections, label_diagnostic, polygon_nms,
    restore_detection, serialize_detections,
)


CLASS_NAMES = ["plane", "ship", "storage-tank", "baseball-diamond", "tennis-court",
               "basketball-court", "ground-track-field", "harbor", "bridge", "large-vehicle",
               "small-vehicle", "helicopter", "roundabout", "soccer-ball-field", "swimming-pool"]
PROTOCOL = {
    "window_px": 1024, "stride_px": 824, "nominal_overlap_px": 200,
    "edge_rule": "shift_last_window_to_edge_no_padding", "batch": 1, "half": False,
    "confidence": 0.25, "window_nms_iou": 0.7, "max_detections_per_window": 300,
    "global_polygon_nms_iou": 0.1, "quality_match_iou": 0.5,
    "wire_format": "OBB1: 28-byte LE header, 40-byte float32 polygon/score + uint32 class records",
    "scope": "local CPU/MPS proxy; no satellite hardware claim; no threshold tuning",
    "timing_boundary": "in-memory decoded RGB input -> crop/BGR -> predict incl preprocessing/OBB NMS"
                       " -> host polygon coordinates -> coordinate restoration -> serialization",
}


def sync(device):
    if device == "mps":
        torch.mps.synchronize()


def dump_json(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def statistics(values):
    array = np.asarray(values, dtype=float)
    return {"count": len(values), "mean": float(array.mean()), "p50": float(np.median(array)),
            "p95": float(np.percentile(array, 95)), "min": float(array.min()), "max": float(array.max())}


def read_labels(path, size, normalized):
    labels, ignored = [], []
    for line in path.read_text().splitlines():
        parts = line.split()
        if not parts or parts[0].startswith(("imagesource:", "gsd:")):
            continue
        if normalized:
            cls = int(parts[0])
            xy = tuple(float(v) * size[i % 2] for i, v in enumerate(parts[1:]))
            difficult = False
        else:
            xy = tuple(map(float, parts[:8]))
            cls = CLASS_NAMES.index(parts[8])
            difficult = len(parts) > 9 and int(parts[9]) != 0
        target = ignored if difficult else labels
        target.append(Detection(0, len(target), cls, 1.0, xy))
    return labels, ignored


def grouping_check(records, width, height):
    by_box = {tuple(record["box"]): record for record in records}
    hashes = {}
    for group in (1, 2, 4):
        geometry = ImageTiling(width, height, 3, 8, 1024, 824, group, 1, 0)
        messages = [by_box[box]["payload"] for tile in geometry.tiles()
                    for box in tile.windows_xyxy]
        # 模拟包逆序到达；仅全部窗口到齐后执行全图 NMS。
        detections = [d for payload in reversed(messages)
                      for d in deserialize_detections(payload)[1]]
        merged = polygon_nms(detections, PROTOCOL["global_polygon_nms_iou"])
        payload = serialize_detections(merged, 0xFFFFFFFF, (0, 0, width, height))
        hashes[str(group)] = hashlib.sha256(payload).hexdigest()
    if len(set(hashes.values())) != 1:
        raise AssertionError("Packet grouping changed merged detections")
    return hashes


def overlay(image, detections, path):
    preview = image.copy()
    scale = min(1.0, 1600 / max(image.size))
    preview.thumbnail((1600, 1600))
    draw = ImageDraw.Draw(preview)
    colors = ["#d62728", "#00a68c", "#e9b72e", "#00b8e6"]
    for detection in detections:
        points = [(detection.xy[i] * scale, detection.xy[i + 1] * scale) for i in range(0, 8, 2)]
        draw.line(points + points[:1], fill=colors[detection.class_id % len(colors)], width=2)
    preview.save(path)


def run(args):
    assets, output = Path(args.assets).resolve(), Path(args.output).resolve()
    if output.exists() and (output / "summary.json").exists():
        raise ValueError("Output already contains a benchmark; choose a new directory")
    output.mkdir(parents=True, exist_ok=True)
    os.environ["YOLO_CONFIG_DIR"] = str(assets / "ultralytics_config")
    os.environ["YOLO_AUTOINSTALL"] = "false"
    os.environ["YOLO_OFFLINE"] = "true"
    downloads = prepare_assets(assets)
    mirror = prepare_dota_mirror(assets) if args.include_mirror else None
    from ultralytics import YOLO, settings
    settings.update({"sync": False})

    torch.set_num_threads(args.threads)
    torch.set_num_interop_threads(1)
    np.random.seed(0)
    torch.manual_seed(0)
    samples = [{"id": "boats", "image": assets / "boats.jpg", "labels": None,
                "provenance": "Ultralytics official aerial OBB example; no labels supplied"}]
    samples += [{"id": path.stem, "image": path,
                 "labels": assets / "dota8/labels/val" / (path.stem + ".txt"),
                 "normalized": True, "provenance": "DOTA8 val smoke-test tile; not independent test"}
                for path in sorted((assets / "dota8/images/val").glob("*.jpg"))]
    if mirror:
        samples += [{"id": Path(item["image"]).stem, "image": assets / "dota_mirror" / item["image"],
                     "labels": assets / "dota_mirror" / item["label"], "normalized": False,
                     "provenance": mirror["provenance"]} for item in mirror["images"]]
    inventory = []
    for sample in samples:
        start = time.perf_counter()
        with Image.open(sample["image"]) as source:
            sample["rgb"] = source.convert("RGB")
        sample["decode_ms"] = (time.perf_counter() - start) * 1000
        width, height = sample["rgb"].size
        xs, ys = window_starts(width, 1024, 824), window_starts(height, 1024, 824)
        sample["boxes"] = [(x, y, x + 1024, y + 1024) for y in ys for x in xs]
        labels, ignored = read_labels(sample["labels"], (width, height), sample.get("normalized")) \
            if sample["labels"] else ([], [])
        sample["ground_truth"], sample["ignored_labels"] = labels, ignored
        inventory.append({"id": sample["id"], "width": width, "height": height,
                          "file_bytes": sample["image"].stat().st_size, "raw_rgb_bytes": width * height * 3,
                          "image_sha256": sha256(sample["image"]), "windows": sample["boxes"],
                          "window_input_bytes": len(sample["boxes"]) * 1024 * 1024 * 3,
                          "decode_ms": sample["decode_ms"], "source": sample["provenance"],
                          "labels_sha256": sha256(sample["labels"]) if sample["labels"] else None,
                          "label_count": len(labels), "difficult_label_count": len(ignored)})
    metadata = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), "protocol": PROTOCOL,
                "command": sys.argv,
                "assets": downloads, "mirror": mirror, "samples": inventory,
                "host": {"platform": platform.platform(), "python": platform.python_version(),
                         "cpu": subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip(),
                         "ram_bytes": psutil.virtual_memory().total, "cpu_threads": args.threads,
                         "mps_available": torch.backends.mps.is_available(), "torch": torch.__version__},
                "dependencies": {d.metadata["Name"]: d.version for d in importlib.metadata.distributions()},
                "warmup_windows_per_device": args.warmup, "repeats": args.repeats,
                "methodology": {"power_mode": "not instrumented", "thermal_steady_state": False,
                                "background": "project training/simulation/tests paused; user applications still running; not dedicated server",
                                "memory": "sampled RSS/MPS allocation, not activation peak",
                                "quality": "fixed confidence/IoU counts; mAP not computed; tiny non-independent set",
                                "io": "decode and artifact disk writes excluded from per-window service"}}
    dump_json(output / "protocol.json", metadata)
    rows, images, device_records = [], [], []
    process = psutil.Process()
    for device in args.devices:
        if device == "mps" and not torch.backends.mps.is_available():
            raise ValueError("MPS requested but unavailable")
        print(f"Starting device={device}, images={len(samples)}, repeats={args.repeats}", flush=True)
        start = time.perf_counter()
        model = YOLO(str(assets / "yolo11n-obb.pt"))
        model.to(device)
        device_record = {"device": device, "load_ms": (time.perf_counter() - start) * 1000,
                         "model_parameters": sum(p.numel() for p in model.model.parameters())}

        def infer(bgr):
            result = model.predict(bgr, imgsz=1024, device=device, half=False, batch=1,
                                   conf=.25, iou=.7, max_det=300, rect=False,
                                   save=False, verbose=False, augment=False)[0]
            polygons = result.obb.xyxyxyxy.detach().cpu().numpy()
            scores = result.obb.conf.detach().cpu().numpy()
            classes = result.obb.cls.detach().cpu().numpy()
            return polygons, scores, classes

        first_bgr = np.asarray(samples[0]["rgb"].crop(samples[0]["boxes"][0]))[:, :, ::-1].copy()
        warm_start = time.perf_counter()
        for _ in range(args.warmup):
            infer(first_bgr)
            sync(device)
            torch.set_num_threads(args.threads)
        device_record["warmup_ms"] = (time.perf_counter() - warm_start) * 1000
        device_record["actual_cpu_threads_after_setup"] = torch.get_num_threads()
        for repeat in range(args.repeats):
            for sample in samples:
                records = []
                width, height = sample["rgb"].size
                case_dir = output / device / sample["id"]
                case_dir.mkdir(parents=True, exist_ok=True)
                for window_id, box in enumerate(sample["boxes"]):
                    sync(device)
                    start = time.perf_counter()
                    bgr = np.asarray(sample["rgb"].crop(box))[:, :, ::-1].copy()
                    cropped = time.perf_counter()
                    polygons, scores, classes = infer(bgr)
                    sync(device)
                    inferred = time.perf_counter()
                    detections = [restore_detection(window_id, i, int(cls), float(score), xy.reshape(-1), box[:2])
                                  for i, (xy, score, cls) in enumerate(zip(polygons, scores, classes))]
                    restored = time.perf_counter()
                    payload = serialize_detections(detections, window_id, box)
                    serialized = time.perf_counter()
                    row = {"device": device, "repeat": repeat, "sample": sample["id"], "window_id": window_id,
                           "x0": box[0], "y0": box[1], "crop_ms": (cropped - start) * 1000,
                           "predict_ms": (inferred - cropped) * 1000,
                           "restore_ms": (restored - inferred) * 1000,
                           "serialize_ms": (serialized - restored) * 1000,
                           "service_ms": (serialized - start) * 1000, "detections": len(detections),
                           "output_bytes": len(payload), "input_bytes": int(bgr.nbytes),
                           "output_sha256": hashlib.sha256(payload).hexdigest(),
                           "rss_bytes": process.memory_info().rss,
                           "mps_allocated_bytes": torch.mps.current_allocated_memory() if device == "mps" else 0}
                    rows.append(row)
                    records.append({"box": box, "payload": payload})
                    if repeat == 0:
                        (case_dir / f"window_{window_id:03d}.bin").write_bytes(payload)
                merge_start = time.perf_counter()
                received = [d for record in records for d in deserialize_detections(record["payload"])[1]]
                decoded = time.perf_counter()
                merged = polygon_nms(received, .1)
                suppressed = time.perf_counter()
                final_payload = serialize_detections(merged, 0xFFFFFFFF, (0, 0, width, height))
                finished = time.perf_counter()
                row = {"device": device, "repeat": repeat, "sample": sample["id"],
                       "windows": len(records), "pre_merge_detections": len(received), "merged_detections": len(merged),
                       "unmerged_output_bytes": sum(len(r["payload"]) for r in records),
                       "merged_output_bytes": len(final_payload), "decode_messages_ms": (decoded - merge_start) * 1000,
                       "polygon_nms_ms": (suppressed - decoded) * 1000, "serialize_final_ms": (finished - suppressed) * 1000,
                       "merge_service_ms": (finished - merge_start) * 1000,
                       "merged_sha256": hashlib.sha256(final_payload).hexdigest()}
                row["window_service_ms_sum"] = sum(r["service_ms"] for r in rows[-len(records):])
                row["end_to_end_service_ms"] = row["window_service_ms_sum"] + row["merge_service_ms"]
                row["grouping_hashes"] = grouping_check(records, width, height)
                if row["merged_sha256"] != row["grouping_hashes"]["1"]:
                    raise AssertionError("Serialization/grouping changed final merged result")
                if sample["labels"]:
                    row["quality"] = label_diagnostic(merged, sample["ground_truth"],
                                                       ignored_labels=sample["ignored_labels"])
                else:
                    row["quality"] = {"mAP": None, "status": "no ground truth"}
                images.append(row)
                if repeat == 0:
                    (case_dir / "merged.bin").write_bytes(final_payload)
                    dump_json(case_dir / "merged.json", [asdict(d) for d in merged])
                    overlay(sample["rgb"], merged, case_dir / "overlay.jpg")
                print(f"{device} repeat={repeat} {sample['id']}: {len(records)} windows, "
                      f"{len(received)} -> {len(merged)} detections", flush=True)
        device_records.append(device_record)
        del model
        if device == "mps":
            torch.mps.empty_cache()
    with (output / "windows.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    dump_json(output / "images.json", images)
    summaries = {}
    for device in args.devices:
        current = [row for row in rows if row["device"] == device]
        summaries[device] = {"window_service_ms": statistics([r["service_ms"] for r in current]),
                             "window_predict_ms": statistics([r["predict_ms"] for r in current]),
                             "window_output_bytes": statistics([r["output_bytes"] for r in current]),
                             "sampled_rss_max_bytes": max(r["rss_bytes"] for r in current),
                             "sampled_mps_allocation_max_bytes": max(r["mps_allocated_bytes"] for r in current),
                             "merge_service_ms": statistics([r["merge_service_ms"] for r in images if r["device"] == device])}
    source_files = [Path(__file__), Path(__file__).with_name("assets.py"),
                    Path(__file__).with_name("postprocess.py"), Path("sim/workload.py")]
    summary = {"protocol": PROTOCOL, "host": metadata["host"], "devices": summaries,
               "device_setup": device_records, "images": len(samples), "window_rows": len(rows),
               "source_sha256": {str(path): sha256(path) for path in source_files},
               "all_grouping_checks_passed": True,
               "notes": ["No mAP: small non-independent sample, official model may have seen DOTA data.",
                         "CPU and MPS are two backends on the same M5 Pro, not satellite/ground devices.",
                         "Three short repetitions do not establish thermal steady state or a general throughput distribution.",
                         "Per-window timings include processing and result serialization; decode/network/disk output excluded."]}
    summary["per_image"] = []
    for sample in inventory:
        for device in args.devices:
            window_rows = [r for r in rows if r["device"] == device and r["sample"] == sample["id"]]
            image_rows = [r for r in images if r["device"] == device and r["sample"] == sample["id"]]
            summary["per_image"].append({
                "sample": sample["id"], "device": device, "width": sample["width"], "height": sample["height"],
                "window_count": len(sample["windows"]), "raw_rgb_bytes": sample["raw_rgb_bytes"],
                "window_input_bytes": sample["window_input_bytes"],
                "window_service_ms": statistics([r["service_ms"] for r in window_rows]),
                "end_to_end_service_ms": statistics([r["end_to_end_service_ms"] for r in image_rows]),
                "merge_service_ms": statistics([r["merge_service_ms"] for r in image_rows]),
                "unmerged_output_bytes": [r["unmerged_output_bytes"] for r in image_rows],
                "merged_output_bytes": [r["merged_output_bytes"] for r in image_rows],
                "merged_sha256": [r["merged_sha256"] for r in image_rows],
                "quality": image_rows[0]["quality"]})
    dump_json(output / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return summary
