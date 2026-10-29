#!/usr/bin/env python3
"""Build a deterministic synthetic SAR output-load fixture, not SAR annotations."""

import argparse
from collections import Counter
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.workload import window_starts


IMAGE_PX = 20_000
WINDOW_PX = 4096
STRIDE_PX = 2304
PIXEL_SPACING_M = 0.25
CONTEXT_PX = 64
PREVIEW_PX = 512
WINDOW_HEADER_BYTES = 28
OBB_RECORD_BYTES = 40
ROI_HEADER_BYTES = 20
PREVIEW_BYTES = PREVIEW_PX ** 2


def vertices(cx, cy, length, width, angle):
    cosine, sine = math.cos(angle), math.sin(angle)
    return [
        [cx + dx * cosine - dy * sine, cy + dx * sine + dy * cosine]
        for dx, dy in (
            (-length / 2, -width / 2),
            (length / 2, -width / 2),
            (length / 2, width / 2),
            (-length / 2, width / 2),
        )
    ]


def physically_disjoint(left, right):
    # 凸矩形分离轴检查，不把相邻上下文区域重叠误作船体碰撞。
    for polygon in (left, right):
        for index in range(4):
            a, b = polygon[index], polygon[(index + 1) % 4]
            axis = (-(b[1] - a[1]), b[0] - a[0])
            lp = [x * axis[0] + y * axis[1] for x, y in left]
            rp = [x * axis[0] + y * axis[1] for x, y in right]
            if max(lp) <= min(rp) or max(rp) <= min(lp):
                return True
    return False


def require(condition, message):
    if not condition:
        raise ValueError(message)


def build_fixture():
    objects = []
    for row in range(10):
        for col in range(10):
            object_id = row * 10 + col
            length_m, width_m = ((100, 20), (200, 35), (400, 61.5))[object_id % 3]
            angle_deg = (0, 45, 90, 135)[object_id % 4]
            cx = 896 + (IMAGE_PX - 1792) * (col + 0.5) / 10
            cy = 896 + (IMAGE_PX - 1792) * (row + 0.5) / 10
            corners = vertices(cx, cy, length_m / PIXEL_SPACING_M,
                               width_m / PIXEL_SPACING_M, math.radians(angle_deg))
            xs, ys = zip(*corners)
            roi = [math.floor(min(xs)) - CONTEXT_PX,
                   math.floor(min(ys)) - CONTEXT_PX,
                   math.ceil(max(xs)) + CONTEXT_PX,
                   math.ceil(max(ys)) + CONTEXT_PX]
            objects.append({
                "object_id": object_id,
                "class_id": object_id % 3,
                "score": 0.9,
                "center_xy": [cx, cy],
                "length_m": length_m,
                "width_m": width_m,
                "angle_deg": angle_deg,
                "vertices": corners,
                "roi_xyxy": roi,
            })

    starts = window_starts(IMAGE_PX, WINDOW_PX, STRIDE_PX)
    windows = []
    counts = Counter()
    for y0 in starts:
        for x0 in starts:
            records = []
            for obj in objects:
                rx0, ry0, rx1, ry1 = obj["roi_xyxy"]
                if not (x0 <= rx0 < rx1 <= x0 + WINDOW_PX
                        and y0 <= ry0 < ry1 <= y0 + WINDOW_PX):
                    continue
                records.append({
                    "object_id": obj["object_id"],
                    "class_id": obj["class_id"],
                    "score": obj["score"],
                    "vertices": [[x - x0, y - y0] for x, y in obj["vertices"]],
                    "roi_xyxy": [rx0 - x0, ry0 - y0, rx1 - x0, ry1 - y0],
                })
                counts[obj["object_id"]] += 1
            windows.append({
                "window_id": len(windows),
                "x0": x0,
                "y0": y0,
                "records": records,
                "raw_input_bytes": WINDOW_PX ** 2 * 2,
                "output_bytes": WINDOW_HEADER_BYTES + len(records)
                    * (OBB_RECORD_BYTES + ROI_HEADER_BYTES + PREVIEW_BYTES),
            })

    require(len(windows) == 64, "Expected 64 windows")
    require(set(counts) == set(range(100)), "Every global object must be covered")
    for index, obj in enumerate(objects):
        for other in objects[index + 1:]:
            require(physically_disjoint(obj["vertices"], other["vertices"]),
                    f"Overlapping ships: {obj['object_id']}, {other['object_id']}")
    for window in windows:
        for record in window["records"]:
            rx0, ry0, rx1, ry1 = record["roi_xyxy"]
            require(0 <= rx0 < rx1 <= WINDOW_PX and 0 <= ry0 < ry1 <= WINDOW_PX,
                    "ROI must be completely inside its window")
            for x, y in record["vertices"]:
                require(rx0 + CONTEXT_PX <= x <= rx1 - CONTEXT_PX
                        and ry0 + CONTEXT_PX <= y <= ry1 - CONTEXT_PX,
                        "ROI must contain the OBB and all context")
                require(0 <= x <= WINDOW_PX and 0 <= y <= WINDOW_PX,
                        "OBB must be completely inside its window")

    sum_records = sum(len(window["records"]) for window in windows)
    input_bytes = sum(window["raw_input_bytes"] for window in windows)
    output_bytes = sum(window["output_bytes"] for window in windows)
    require(sum_records == 212, "Unexpected change to canonical record count")
    require(input_bytes == 2_147_483_648, "Unexpected raw-window byte total")
    require(output_bytes == 55_589_040, "Unexpected result byte total")
    return {
        "schema_version": 1,
        "fixture_id": "sar_spotfine_100_objects_4096_1792_v1",
        "purpose": "Synthetic output-load assumption for timing and scheduling calibration",
        "limitations": [
            "Not observed SAR annotations or measured harbor density",
            "Class IDs are arbitrary workload labels, not validated vessel classes",
            "Preview resizing preserves the entire ROI but not native spatial detail",
            "All repeated per-window records are transmitted before ground deduplication",
            "A fixed mean output byte count is a simulator approximation; exact per-window counts remain here",
        ],
        "image": {
            "width_px": IMAGE_PX, "height_px": IMAGE_PX,
            "pixel_spacing_m": PIXEL_SPACING_M,
            "ground_resolution_m": 0.5,
            "scene_width_m": 5000, "scene_height_m": 5000,
            "bands": 1, "bits_per_sample": 16,
            "raw_bytes": IMAGE_PX ** 2 * 2,
            "compression_ratio": 1.0,
        },
        "tiling": {
            "window_px": WINDOW_PX, "stride_px": STRIDE_PX,
            "overlap_px": WINDOW_PX - STRIDE_PX,
            "context_px_per_side": CONTEXT_PX,
            "context_m_per_side": CONTEXT_PX * PIXEL_SPACING_M,
            "window_starts": list(starts), "num_windows": len(windows),
            "windows_per_packet": 1,
            "edge_rule": "shift_last_window_to_edge_no_padding",
            "input_rule": "Independent full uint16 windows; duplicated overlap is transmitted",
            "record_rule": "Include only when the outward-rounded OBB ROI plus context fits completely",
        },
        "output_encoding": {
            "window_header_bytes": WINDOW_HEADER_BYTES,
            "obb_record_bytes": OBB_RECORD_BYTES,
            "obb_record_format": "<9fI: four xy vertices, score, class_id",
            "roi_header_bytes": ROI_HEADER_BYTES,
            "roi_header_format": "<5I: x0, y0, x1, y1, preview_byte_count",
            "preview_width_px": PREVIEW_PX, "preview_height_px": PREVIEW_PX,
            "preview_bands": 1, "preview_bits_per_sample": 8,
            "preview_bytes": PREVIEW_BYTES,
            "preview_rule": "Extract complete local ROI; aspect-preserving resize and zero letterbox to 512x512; no compression",
            "coordinate_convention": "Local xy pixels, integer ROI upper bounds exclusive",
            "object_id_is_fixture_only": True,
        },
        "summary": {
            "unique_objects": len(objects), "sum_record_count": sum_records,
            "mean_k": sum_records / len(windows),
            "min_k": min(len(window["records"]) for window in windows),
            "max_k": max(len(window["records"]) for window in windows),
            "min_object_inclusions": min(counts.values()),
            "max_object_inclusions": max(counts.values()),
            "raw_window_input_bytes": input_bytes,
            "total_output_bytes": output_bytes,
            "mean_output_bytes_per_window": output_bytes / len(windows),
            "mean_output_mb_per_window": output_bytes / len(windows) / 1e6,
            "total_output_mb": output_bytes / 1e6,
            "all_objects_fully_covered": True,
            "all_physical_objects_disjoint": True,
            "all_rois_contain_obb_and_context": True,
        },
        "global_objects": objects,
        "windows": windows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    fixture = build_fixture()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(fixture, handle, indent=2, ensure_ascii=True, allow_nan=False)
        handle.write("\n")
    print(json.dumps({"output": str(args.output), **fixture["summary"]}, indent=2))


if __name__ == "__main__":
    main()
