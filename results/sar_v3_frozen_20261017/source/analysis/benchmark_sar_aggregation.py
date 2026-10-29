#!/usr/bin/env python3
"""Measure CPU parsing, polygon NMS and encoding for a synthetic SAR fixture."""

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import struct
import sys
import time

import shapely
from shapely.geometry import Polygon


HEADER = struct.Struct("<4s6I")
OBB = struct.Struct("<9fI")
ROI = struct.Struct("<5I")
MAGIC = b"SRP1"
PREVIEW_BYTES = 512 ** 2


@dataclass(frozen=True)
class Detection:
    window_id: int
    detection_id: int
    class_id: int
    score: float
    xy: tuple
    roi_xyxy: tuple
    preview: bytes


def validate_detection(detection):
    if len(detection.xy) != 8 or not all(math.isfinite(value) for value in detection.xy):
        raise ValueError("Expected four finite polygon vertices")
    if not 0 <= detection.score <= 1 or detection.class_id < 0:
        raise ValueError("Invalid confidence or class")
    polygon = Polygon(list(zip(detection.xy[::2], detection.xy[1::2])))
    if not polygon.is_valid or polygon.area <= 0:
        raise ValueError("Invalid or empty predicted quadrilateral")
    return polygon


def polygon_iou(left, right):
    intersection = left.intersection(right).area
    union = left.area + right.area - intersection
    return intersection / union if union > 0 else 0.0


def polygon_nms(detections, iou_threshold=0.1):
    # 与现有后处理保持同样的逐类几何NMS及稳定身份排序。
    ordered = sorted(detections, key=lambda item: (-item.score, item.window_id,
                                                  item.detection_id))
    identities = [(item.window_id, item.detection_id) for item in ordered]
    if len(set(identities)) != len(identities):
        raise ValueError("Duplicate window/detection identity")
    polygons = [validate_detection(item) for item in ordered]
    kept = []
    for index, candidate in enumerate(ordered):
        if all(candidate.class_id != ordered[other].class_id
               or polygon_iou(polygons[index], polygons[other]) <= iou_threshold
               for other in kept):
            kept.append(index)
    return [ordered[index] for index in kept]


def encode(detections, window_id, origin, size):
    payload = bytearray(HEADER.pack(MAGIC, window_id, *origin, *size, len(detections)))
    for item in detections:
        validate_detection(item)
        payload.extend(OBB.pack(*item.xy, item.score, item.class_id))
        payload.extend(ROI.pack(*item.roi_xyxy, len(item.preview)))
        payload.extend(item.preview)
    return bytes(payload)


def decode(payload):
    if len(payload) < HEADER.size:
        raise ValueError("Truncated SRP1 header")
    magic, window_id, x0, y0, width, height, count = HEADER.unpack_from(payload)
    if magic != MAGIC or width == 0 or height == 0:
        raise ValueError("Invalid SRP1 header")
    offset = HEADER.size
    detections = []
    for detection_id in range(count):
        if offset + OBB.size + ROI.size > len(payload):
            raise ValueError("Truncated SRP1 record")
        fields = OBB.unpack_from(payload, offset)
        offset += OBB.size
        rx0, ry0, rx1, ry1, preview_size = ROI.unpack_from(payload, offset)
        offset += ROI.size
        if (not 0 <= rx0 < rx1 <= width or not 0 <= ry0 < ry1 <= height
                or preview_size != PREVIEW_BYTES or offset + preview_size > len(payload)):
            raise ValueError("Invalid ROI or truncated preview")
        # bytes切片会真实复制预览载荷；计时包含解析、坐标恢复及图像字节搬运。
        preview = payload[offset:offset + preview_size]
        offset += preview_size
        xy = tuple(value + (x0 if index % 2 == 0 else y0)
                   for index, value in enumerate(fields[:8]))
        detections.append(Detection(window_id, detection_id, fields[-1], fields[-2],
                                    xy, (rx0 + x0, ry0 + y0, rx1 + x0, ry1 + y0),
                                    preview))
    if offset != len(payload):
        raise ValueError("Unexpected trailing SRP1 bytes")
    return detections


def construct_packets(fixture):
    preview = bytes(PREVIEW_BYTES)
    window_size = fixture["tiling"]["window_px"]
    packets = []
    for window in fixture["windows"]:
        detections = [Detection(
            window["window_id"], index, record["class_id"], record["score"],
            tuple(value for xy in record["vertices"] for value in xy),
            tuple(record["roi_xyxy"]), preview,
        ) for index, record in enumerate(window["records"])]
        payload = encode(detections, window["window_id"], (window["x0"], window["y0"]),
                         (window_size, window_size))
        if len(payload) != window["output_bytes"]:
            raise ValueError("Window payload disagrees with fixture byte count")
        packets.append(payload)
    if sum(map(len, packets)) != fixture["summary"]["total_output_bytes"]:
        raise ValueError("Input packet byte total disagrees with fixture")
    return packets


def aggregate(packets, image_size):
    restored = [item for payload in packets for item in decode(payload)]
    kept = polygon_nms(restored)
    result = encode(kept, 0, (0, 0), image_size)
    return result, len(restored), len(kept)


def validate_global_output(payload, fixture):
    actual = decode(payload)
    expected = fixture["global_objects"]
    if len(actual) != len(expected):
        raise ValueError("Merged count disagrees with canonical global objects")
    unmatched = set(range(len(expected)))
    for item in actual:
        matches = []
        for index in unmatched:
            obj = expected[index]
            canonical_xy = [value for xy in obj["vertices"] for value in xy]
            if (obj["class_id"] == item.class_id
                    and abs(obj["score"] - item.score) <= 1e-6
                    and tuple(obj["roi_xyxy"]) == item.roi_xyxy
                    and all(abs(left - right) <= 0.0025
                            for left, right in zip(canonical_xy, item.xy))):
                matches.append(index)
        if len(matches) != 1:
            raise ValueError("Merged geometry does not uniquely match canonical objects")
        unmatched.remove(matches[0])
    if unmatched:
        raise ValueError("Canonical objects missing from global output")


def percentile(values, fraction):
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    low = math.floor(position)
    high = math.ceil(position)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload-json", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=20)
    args = parser.parse_args()
    if args.warmup < 0 or args.repeats < 1:
        parser.error("warmup must be nonnegative and repeats positive")
    args.output.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": 1,
        "status": "running",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "CPU reference measurement on the recorded machine; not automatically a ground-server measurement",
        "measurement_boundary": "Already-received bytes -> parse and copy previews -> global coordinates -> class-wise polygon NMS -> encode merged bytes",
        "excluded": ["Disk and network IO", "Fixture packet construction", "Model inference",
                     "SHA256 and correctness checks", "Global image recognition accuracy"],
        "preview_content": "All-zero dummy uint8 rasters; measures parsing/copying/encoding only, no codec or visual processing",
        "iou_threshold": 0.1,
        "ordering": "Descending confidence, ascending source window_id and detection_id",
        "coordinate_check_tolerance_px": 0.0025,
        "runtime": {"python": sys.version, "system": platform.system(),
                    "machine": platform.machine(), "logical_cpu_count": os.cpu_count(),
                    "shapely": shapely.__version__},
        "fixture_sha256": file_hash(args.workload_json),
        "script_sha256": file_hash(Path(__file__)),
        "warmup": args.warmup,
        "repeats": args.repeats,
        "samples": [],
    }
    write_json(args.output / "report.json", report)
    try:
        fixture = json.loads(args.workload_json.read_text(encoding="utf-8"))
        packets = construct_packets(fixture)
        image_size = (fixture["image"]["width_px"], fixture["image"]["height_px"])
        forward, before_count, after_count = aggregate(packets, image_size)
        backward, reverse_before, reverse_after = aggregate(list(reversed(packets)), image_size)
        validate_global_output(forward, fixture)
        validate_global_output(backward, fixture)
        if (forward != backward or before_count != reverse_before or after_count != reverse_after
                or before_count != fixture["summary"]["sum_record_count"]):
            raise ValueError("Arrival-order invariance or record count check failed")
        expected_bytes = HEADER.size + after_count * (OBB.size + ROI.size + PREVIEW_BYTES)
        if len(forward) != expected_bytes:
            raise ValueError("Invalid global result byte count")
        expected_hash = hashlib.sha256(forward).hexdigest()
        report["validation"] = {
            "packet_count": len(packets), "input_bytes": sum(map(len, packets)),
            "input_records": before_count, "output_records": after_count,
            "output_bytes": len(forward), "output_sha256": expected_hash,
            "canonical_global_objects_match": True, "arrival_order_invariant": True,
            "validation_aggregations_before_warmup": 2,
        }
        del forward, backward
        for _ in range(args.warmup):
            aggregate(packets, image_size)
        for index in range(args.repeats):
            ordered_packets = packets if index % 2 == 0 else list(reversed(packets))
            started = time.perf_counter_ns()
            output, input_count, output_count = aggregate(ordered_packets, image_size)
            elapsed_ms = (time.perf_counter_ns() - started) / 1e6
            output_hash = hashlib.sha256(output).hexdigest()
            if output_hash != expected_hash or (input_count, output_count) != (before_count, after_count):
                raise ValueError("Measured iteration changed the canonical result")
            report["samples"].append({
                "repeat": index, "packet_order": "forward" if index % 2 == 0 else "reverse",
                "service_ms": elapsed_ms, "input_records": input_count,
                "output_records": output_count, "output_bytes": len(output),
                "output_sha256": output_hash,
            })
            del output
            write_json(args.output / "report.json", report)
        timings = [sample["service_ms"] for sample in report["samples"]]
        report["summary"] = {
            "mean_ms": statistics.mean(timings), "median_ms": statistics.median(timings),
            "p95_ms": percentile(timings, 0.95), "min_ms": min(timings),
            "max_ms": max(timings), "total_measured_ms": sum(timings),
            "completed_repeats": len(timings),
        }
        report["status"] = "passed"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        write_json(args.output / "report.json", report)
    print(json.dumps({"output": str(args.output), "status": report["status"],
                      **report["validation"], **report["summary"]}, indent=2))


if __name__ == "__main__":
    main()
