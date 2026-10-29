"""由 Jetson 标定和固定 SAR 负载生成独立开发配置，不运行调度仿真。"""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
import math
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sim.config import config_from_dict
from sim.constellation import walker_tle
from sim.workload import ImageTiling, window_starts


COVERAGE_PATH = ROOT / "results/sar_coastal_coverage_20261015/coverage.json"
DT = 0.1
ACQUISITION_END_S = 86400
READY_DELAY_S = 60
FOLLOWUP_S = 301
SOURCE_COUNT = 24
HELPER_COUNT = 22
IMAGE_GEOMETRY = {
    "width_px": 20000, "height_px": 20000, "bands": 3, "bits_per_sample": 8,
    "window_px": 4096, "stride_px": 2304, "windows_per_tile_axis": 1,
    "transport_components": [{"name": "SAR_amplitude_uint16", "downsample": 1,
                              "bands": 1, "bits_per_sample": 16}],
    "transport_compression_ratio": 1.0,
}


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def encoded(value):
    return (json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode()


def positive_number(values, key, *, zero_allowed=False):
    value = values.get(key)
    if (type(value) not in (int, float) or not math.isfinite(value)
            or value < 0 or (value == 0 and not zero_allowed)):
        raise ValueError(f"{key} must be a finite {'nonnegative' if zero_allowed else 'positive'} number")
    return float(value)


def source_record(path):
    path = Path(path).resolve()
    try:
        label = str(path.relative_to(ROOT))
    except ValueError:
        label = str(path)
    return {"path": label, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def step_at(seconds):
    step = round(seconds / DT)
    if not math.isclose(step * DT, seconds, rel_tol=0, abs_tol=1e-7):
        raise ValueError("Arrival times must lie on the fixed 0.1-second time grid")
    return step


def ready_summary(seconds, end_s):
    hourly = [sum(hour * 3600 <= value < (hour + 1) * 3600 for value in seconds)
              for hour in range(24)]
    times = [0.0, *sorted(seconds), end_s]
    start, end = max(zip(times, times[1:]), key=lambda pair: pair[1] - pair[0])
    return {
        "total_tasks": len(seconds), "hourly_counts_first_24h": hourly,
        "nonempty_hours_first_24h": sum(count > 0 for count in hourly),
        "every_clock_hour_has_ready_task": all(hourly),
        "arrivals_after_first_24h": sum(value >= ACQUISITION_END_S for value in seconds),
        "first_ready_s": min(seconds), "last_ready_s": max(seconds),
        "longest_gap_including_arrival_interval_boundaries": {
            "start_s": start, "end_s": end, "duration_s": end - start},
        "scope": "Fixed UTC clock hours for all five targets together; not each target or every rolling hour",
    }


def validate_fixture(fixture):
    from shapely.geometry import Polygon

    image_expected = {"width_px": 20000, "height_px": 20000, "bands": 1,
                      "bits_per_sample": 16, "pixel_spacing_m": 0.25,
                      "ground_resolution_m": 0.5, "raw_bytes": 800000000,
                      "compression_ratio": 1.0}
    starts = window_starts(20000, 4096, 2304)
    tiling_expected = {"window_px": 4096, "stride_px": 2304, "overlap_px": 1792,
                       "context_px_per_side": 64, "context_m_per_side": 16,
                       "window_starts": list(starts), "num_windows": 64,
                       "windows_per_packet": 1, "edge_rule": "shift_last_window_to_edge_no_padding"}
    encoding_expected = {"window_header_bytes": 28, "obb_record_bytes": 40,
                         "roi_header_bytes": 20, "preview_width_px": 512,
                         "preview_height_px": 512, "preview_bands": 1,
                         "preview_bits_per_sample": 8, "preview_bytes": 512 ** 2}
    for section, expected in (("image", image_expected), ("tiling", tiling_expected),
                              ("output_encoding", encoding_expected)):
        for key, value in expected.items():
            if fixture[section].get(key) != value:
                raise ValueError(f"Fixture {section}.{key} conflicts with the frozen format")
    windows, objects = fixture["windows"], fixture["global_objects"]
    if len(windows) != 64 or not objects:
        raise ValueError("Fixture requires 64 windows and at least one canonical object")
    by_id, polygons, inclusions = {}, [], {}
    for obj in objects:
        object_id = obj["object_id"]
        if type(object_id) is not int or object_id < 0 or object_id in by_id:
            raise ValueError("Canonical object IDs must be unique nonnegative integers")
        points = obj["vertices"]
        if (len(points) != 4 or any(len(point) != 2 for point in points)
                or not all(type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 20000
                           for point in points for value in point)):
            raise ValueError("Canonical objects must contain four finite in-image vertices")
        polygon = Polygon(points)
        if not polygon.is_valid or polygon.area <= 0:
            raise ValueError("Invalid canonical object polygon")
        xs, ys = zip(*points)
        expected_roi = [math.floor(min(xs)) - 64, math.floor(min(ys)) - 64,
                        math.ceil(max(xs)) + 64, math.ceil(max(ys)) + 64]
        if obj["roi_xyxy"] != expected_roi or min(expected_roi) < 0 or max(expected_roi) > 20000:
            raise ValueError("Canonical ROI must contain the OBB and the fixed 64-pixel context")
        if type(obj["class_id"]) is not int or obj["class_id"] < 0 or not 0 < obj["score"] <= 1:
            raise ValueError("Invalid canonical class or score")
        by_id[object_id], inclusions[object_id] = obj, 0
        polygons.append(polygon)
    disjoint = all(not left.intersects(right) or left.intersection(right).area == 0
                   for index, left in enumerate(polygons) for right in polygons[index + 1:])
    counts, input_bytes, output_bytes = [], 0, 0
    for index, (window, (x0, y0)) in enumerate(zip(windows, ((x, y) for y in starts for x in starts))):
        if (window["window_id"], window["x0"], window["y0"]) != (index, x0, y0):
            raise ValueError("Fixture window IDs or origins differ from simulator window order")
        expected_ids = {object_id for object_id, obj in by_id.items()
                        if x0 <= obj["roi_xyxy"][0] < obj["roi_xyxy"][2] <= x0 + 4096
                        and y0 <= obj["roi_xyxy"][1] < obj["roi_xyxy"][3] <= y0 + 4096}
        records = window["records"]
        ids = [record["object_id"] for record in records]
        if len(set(ids)) != len(ids) or set(ids) != expected_ids:
            raise ValueError("Window records must exactly match fully contained canonical objects")
        for record in records:
            obj = by_id[record["object_id"]]
            expected_points = [[x - x0, y - y0] for x, y in obj["vertices"]]
            expected_roi = [obj["roi_xyxy"][0] - x0, obj["roi_xyxy"][1] - y0,
                            obj["roi_xyxy"][2] - x0, obj["roi_xyxy"][3] - y0]
            if (record["vertices"] != expected_points or record["roi_xyxy"] != expected_roi
                    or record["class_id"] != obj["class_id"] or record["score"] != obj["score"]):
                raise ValueError("Local window records differ from canonical global geometry")
            inclusions[record["object_id"]] += 1
        expected_input, expected_output = 4096 ** 2 * 2, 28 + len(records) * (40 + 20 + 512 ** 2)
        if window["raw_input_bytes"] != expected_input or window["output_bytes"] != expected_output:
            raise ValueError("Window byte count differs from records and SRP1 encoding")
        input_bytes += expected_input
        output_bytes += expected_output
        counts.append(len(records))
    derived = {
        "unique_objects": len(objects), "sum_record_count": sum(counts),
        "mean_k": sum(counts) / 64, "min_k": min(counts), "max_k": max(counts),
        "min_object_inclusions": min(inclusions.values()), "max_object_inclusions": max(inclusions.values()),
        "raw_window_input_bytes": input_bytes, "total_output_bytes": output_bytes,
        "mean_output_bytes_per_window": output_bytes / 64,
        "mean_output_mb_per_window": output_bytes / 64 / 1e6, "total_output_mb": output_bytes / 1e6,
        "all_objects_fully_covered": min(inclusions.values()) > 0,
        "all_physical_objects_disjoint": disjoint, "all_rois_contain_obb_and_context": True,
    }
    for key, value in derived.items():
        if fixture["summary"].get(key) != value:
            raise ValueError(f"Fixture summary.{key} differs from its records and geometry")
    if not derived["all_objects_fully_covered"] or not disjoint:
        raise ValueError("The frozen fixture requires full object coverage and disjoint physical objects")
    return derived


def build(calibration, fixture, coverage, *, ground_rate_multiplier=1,
          ground_rate_sensitivity=()):
    ground_rate_multiplier = positive_number({"ground_rate_multiplier": ground_rate_multiplier},
                                             "ground_rate_multiplier")
    sensitivity = sorted({positive_number({"ground_rate_sensitivity": value},
                                         "ground_rate_sensitivity")
                          for value in ground_rate_sensitivity} - {ground_rate_multiplier})
    t_window = positive_number(calibration, "t_window_s")
    aggregation = positive_number(calibration, "aggregation_delay_s", zero_allowed=True)
    execution_memory = positive_number(calibration, "execution_memory_gb")
    if execution_memory > 8:
        raise ValueError("Execution reservation exceeds the fixed 8 GB budget")
    fixture_summary = validate_fixture(fixture)
    result_bytes = fixture_summary["mean_output_bytes_per_window"]
    if calibration.get("window_px", 4096) != 4096:
        raise ValueError("Calibration must measure the 4096-pixel window without shrinking it")

    selection = coverage["selection"]
    selection_hash = hashlib.sha256(json.dumps(selection, sort_keys=True, allow_nan=False).encode()).hexdigest()
    if selection_hash != coverage["selection_sha256"]:
        raise ValueError("Coverage selection hash mismatch")
    groups = [group for group in coverage["groups"] if group["id"] == "sar24"]
    if len(groups) != 1:
        raise ValueError("Expected exactly one frozen sar24 geometry group")
    group = groups[0]
    source_ids = group["source_sat_ids"]
    if source_ids != list(range(24, 72, 2)) or group["sar_count"] != SOURCE_COUNT:
        raise ValueError("SAR source membership differs from the audited 12-plane, two-per-plane subset")
    if not group["events"] or any(event["sensor"] != "sar" for event in group["events"]):
        raise ValueError("Expected nonempty SAR-only acquisition events")
    id_map = {old: new for new, old in enumerate(source_ids)}
    epoch = selection["epoch"]
    source_tles = [coverage["tle_by_source"][str(old)] for old in source_ids]
    helper_tles = walker_tle(epoch, 2, 11, 550, 53, raan_offset_deg=0, phasing=1,
                            plane_raan_offsets_deg=[0, 90], first_satellite_number=25)
    tles = source_tles + helper_tles
    catalog_ids = [int(lines[0][2:7]) for lines in tles]
    if len(set(catalog_ids)) != SOURCE_COUNT + HELPER_COUNT:
        raise ValueError("Synthetic NORAD catalog numbers must remain unique after node remapping")

    arrivals, ready_seconds = [], []
    for event in group["events"]:
        release_s = positive_number(event, "release_s", zero_allowed=True)
        if release_s >= ACQUISITION_END_S:
            raise ValueError("An acquisition lies outside the audited 24-hour interval")
        ready_s = release_s + READY_DELAY_S
        ready_seconds.append(ready_s)
        arrivals.append({
            "release_step": step_at(ready_s), "source_sat_id": id_map[event["source_sat_id"]],
            "target_id": event["target_id"], "window_id": event["window_id"],
        })
    arrivals.sort(key=lambda event: (event["release_step"], event["source_sat_id"], event["target_id"]))
    cutoff_step = max(step_at(ACQUISITION_END_S), max(event["release_step"] for event in arrivals) + 1)
    sim_steps = max(step_at(ACQUISITION_END_S), max(event["release_step"] for event in arrivals)) + step_at(FOLLOWUP_S)
    summary = ready_summary(ready_seconds, cutoff_step * DT)
    if not summary["every_clock_hour_has_ready_task"]:
        raise ValueError("The +60-second ready shift no longer supplies every clock hour; cannot silently freeze")
    if summary["longest_gap_including_arrival_interval_boundaries"]["duration_s"] >= 3600:
        raise ValueError("Ready-task gaps must remain below the stated one-hour architecture target")

    workload = ImageTiling.from_dict({**deepcopy(IMAGE_GEOMETRY), "compute_cost_per_window": 1.0,
                                     "result_size_mb_per_window": result_bytes / 1e6})
    workload_manifest = workload.manifest()
    workload_manifest["transport"]["compute_rule"] = (
        "Calibrated uint16 amplitude normalization, replicated three-channel 4096 input, "
        "FP32 network forward, controlled polygon NMS and target-preview encoding; "
        "SAR focusing is upstream and original file decoding is excluded")
    if workload_manifest["num_windows"] != 64 or workload_manifest["num_tiles"] != 64:
        raise ValueError("The frozen Spot Fine geometry must produce exactly 64 windows and packets")
    helper_rate = 1 / t_window
    base = {
        "seed": 1701, "num_sats": SOURCE_COUNT + HELPER_COUNT, "sim_steps": sim_steps,
        "dt": DT, "decision_interval_steps": step_at(1), "topology_update_steps": step_at(10),
        "ground_visibility_update_steps": step_at(10), "task_arrival_rate": 0,
        "vram_base_gb": execution_memory, "vram_alpha_per_mb": 0,
        "deadline_steps": 0, "mem_capacity_gb": 8, "vram_capacity_gb": 8,
        "compute_rate": helper_rate, "transfer_fail_on_link_down": False,
        "vram_policy": "wait", "pipeline_mode": "distributed", "tile_lifecycle_log": "",
        "ground_stations": [{
            "id": "gs_beijing", "lat_deg": 39.9042, "lon_deg": 116.4074, "alt_m": 50,
            "min_elevation_deg": 15, "bandwidth_mbps": 1200, "receive_channels": 1,
            "compute_rate": helper_rate * ground_rate_multiplier,
        }],
        "topology": {
            "start_time_utc": epoch, "earth_radius_km": 6378.137,
            "max_range_km": 4500, "max_neighbors": 4, "predict_contacts": True,
            "precompute_visibility": True, "contact_scan_s": 30, "contact_update_s": 60,
            "bandwidth_distance_scale_km": 0, "bandwidth_mbps_min": 10000,
            "bandwidth_mbps_max": 10000, "bandwidth_period": 0, "bandwidth_noise": 0,
            "latency_ms": 0, "visibility_workers": 1, "window_lookahead_s": 7200,
            "tle_lines": tles,
        },
        "ground_relay_delay_s": 0, "aggregation_delay_s": aggregation,
        "task_arrival_end_step": cutoff_step, "execution_commitment": True,
        "task_source_sat_ids": list(range(SOURCE_COUNT)), "result_buffering": True,
        "result_relay": True, "relay_max_hops": 8, "relay_contact_guard": True,
        "image_tiling": asdict(workload), "compute_service_mode": "work_conserving",
        "scheduled_task_arrivals": arrivals, "decision_tile_limit_per_sat": 64,
        "input_relay_max_hops": 2, "ground_input_relay": True,
    }
    base["satellite_compute_rates"] = [helper_rate] * (SOURCE_COUNT + HELPER_COUNT)
    # 历史协议保持原字段集合；包内去重仅由新调度分组协议显式开启。
    base["image_tiling"].pop("deduplicate_within_tile", None)
    configs = {"main_config.json": asdict(config_from_dict(base))}
    for multiplier in sensitivity:
        variant = deepcopy(configs["main_config.json"])
        for station in variant["ground_stations"]:
            station["compute_rate"] = helper_rate * multiplier
        configs[f"ground_x{multiplier!r}_config.json"] = variant
    manifest = {
        "schema_version": 1,
        "status": "frozen development parameters; no scheduling results produced by this builder",
        "compute_unit": "One U is one 4096x4096 inference window including the calibrated service boundary",
        "primary_metric": "P300: all window results and target crops aggregated on the ground within 300 seconds of image readiness",
        "calibration_report": calibration, "workload_fixture": fixture,
        "validated_fixture_summary": fixture_summary,
        "workload": workload_manifest, "ready_arrival_summary": summary,
        "coverage_selection_sha256": selection_hash,
        "acquisition_summary_before_ready_shift": group["summary"],
        "source_node_mapping": [{"legacy_id": old, "node_id": new,
                                 "synthetic_norad_id": catalog_ids[new]}
                                for old, new in id_map.items()],
        "helper_node_mapping": [{"legacy_id": old, "node_id": SOURCE_COUNT + index,
                                 "synthetic_norad_id": catalog_ids[SOURCE_COUNT + index]}
                                for index, old in enumerate(range(2, 24))],
        "architecture": {
            "sar": {"count": 24, "planes": 12, "per_plane": 2, "altitude_km": 570,
                    "inclination_deg": 97.7, "raan_deg": list(range(0, 360, 30)), "phase_deg": [0, 180]},
            "helpers": {"count": 22, "planes": 2, "per_plane": 11, "altitude_km": 550,
                        "inclination_deg": 53, "raan_deg": [0, 90], "walker_phasing": 1},
            "choice_reason": "Smallest audited group with both every clock hour represented and longest ready gap below one hour",
            "targets": selection["targets"],
        },
        "parameter_basis": {
            "image_product": "ICEYE Spot Fine reference: nominal 5x5 km, 0.5 m resolution, 0.25 m pixel spacing, uint16 amplitude; exact square raster is a modeling choice",
            "image_product_source": "https://sar.iceye.com/6.0.8/productspecification/imagingmodes/",
            "helper_compute_rate_u_per_s": helper_rate,
            "source_rate_multiplier": 1,
            "ground_rate_multiplier": ground_rate_multiplier,
            "compute_basis": "All SAR and helper satellites use the same measured Jetson service rate; ground capacity is an explicit end-to-end service-rate multiple, not a GPU clock or hardware benchmark ratio",
            "ground_capacity_model": {
                "model": "single_fifo_work_conserving_equivalent_service_pool",
                "main_multiplier": ground_rate_multiplier,
                "sensitivity_multipliers": sensitivity,
                "ground_rate_u_per_s": helper_rate * ground_rate_multiplier,
                "equivalent_window_service_s": t_window / ground_rate_multiplier,
                "equivalent_image_service_s": 64 * t_window / ground_rate_multiplier,
                "definition": "mu_ground = beta / t_J; W units take W / mu_ground seconds of isolated fluid service before step-end event quantization",
                "selection_basis": "Beta is an experimental resource-provisioning assumption fixed before scheduling comparisons; beta=10 represents one order of magnitude more effective capacity, not an L4 speed claim",
                "memory_assumption": "Ground host RAM and execution memory are provisioned sufficiently and do not cause admission failures; no finite L4-sized memory limit or physical multi-GPU placement is simulated",
                "scope": "Scales the full per-window calibrated service boundary, including CPU and device transfers, as equivalent provisioning; excludes radio transfer, queuing and whole-image aggregation",
                "aggregation": "Independent fixed Jetson CPU proxy delay, unchanged by beta; no shared aggregation queue",
                "limits": "Finite beta only; more independent GPUs do not physically divide a single window latency by their count; this fluid abstraction is not a detailed GPU-concurrency model",
                "comparison_rule": "Every scheduler and ground-only baseline uses the same beta within a comparison; report all preregistered sensitivities, not just favorable results",
                "status": "parameters frozen; no ground hardware benchmark or scheduling comparison run",
            },
            "ready_delay_s": READY_DELAY_S,
            "ready_delay_basis": "Fixed upstream image-formation delay assumption; shifts release times, does not consume modeled inference resources; P300 starts afterwards",
            "memory": {"nominal_shared_memory_gb": 32, "queue_budget_gb": 8,
                       "execution_budget_gb": 8, "execution_reservation_gb": execution_memory,
                       "unmodeled_system_and_reserve_budget_gb": 16,
                       "basis": "Separate logical budgets within shared memory, not independent physical RAM and VRAM; decimal GB"},
            "links": {"isl_mbps": 10000, "ground_mbps": 1200, "payload_efficiency": 1,
                      "basis": "Cross-mission equipment-reference combination, not an actual ICEYE network; ideal payload efficiency and zero acquisition overhead",
                      "isl_source": "https://www.tesat.de/images/tesat/products/250820_DataSheet_SCOT80_A4.pdf",
                      "ground_source": "https://zenodo.org/records/7244968/files/Session%202B-Le_Lann-Image%20Compression%20on%20Pleiades%20Neo.pdf"},
        },
        "execution_limits": [
            "One physical proxy inference slot per satellite; one abstract FIFO service pool per ground station, not a physical multi-GPU executor; work-conserving service with step-end completion events",
            "Distributed input queues materialize overlapping windows: 2.147483648 GB per image; the 8 GB queue fits three complete unprocessed images, not ten 800 MB shared views",
            "Aggregation is a fixed delay without a shared aggregation queue; ground RAM/VRAM admission is not implemented and sufficient provisioning is an explicit ground-pool assumption",
            "Ground capacity scales full window service rather than GPU clock frequency; finite high-beta variants are sensitivity cases, not infinite-compute bounds; step-end timestamps still require fine-step validation",
            "At 10 Gbps the input window needs 0.0268435456 seconds ideally; dt=0.1 rounds hop completion to step boundaries, requiring a finer-step check before algorithm ranking claims",
            "Topology and ground visibility update every 10 seconds; contact predictions scan every 30 seconds; geometry is an optimistic screen",
            "Ground site and its single aggregate receiving channel are research assumptions",
            "No independent energy, pointing, SAR duty-cycle, image focusing, or thermal-flight constraints",
            "Per-window mean result bytes are fixed at arrival; test-image detections are not leaked into scheduling",
            "Full original-image delivery within the same SLA is not implemented by this protocol",
            "Only one 24-hour development day; multi-day held-out evaluation waits for method freezing",
        ],
        "simulation_horizon": {"dt_s": DT, "arrival_end_exclusive_s": cutoff_step * DT,
                               "simulation_end_s": sim_steps * DT, "followup_at_least_s": FOLLOWUP_S,
                               "start_empty_once": True, "reset_between_clock_hours": False,
                               "hard_expiry": False},
    }
    return configs, manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calibration-report", type=Path, required=True,
                        help="JSON: t_window_s, aggregation_delay_s, execution_memory_gb (reserved budget, not measured physical peak)")
    parser.add_argument("--workload-fixture", type=Path, required=True,
                        help="Canonical SAR fixture with image, tiling, output_encoding, windows, global_objects and summary")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--ground-rate-multiplier", type=float, default=1,
                        help="Ground effective service capacity relative to Jetson; finite positive provisioning assumption")
    parser.add_argument("--ground-rate-sensitivity", type=float, nargs="*", default=[],
                        help="Prepare additional ground-only capacity variants without running simulations")
    args = parser.parse_args()
    if args.output_dir.exists():
        raise FileExistsError("Choose a new output directory; frozen records are never overwritten")
    calibration = read_json(args.calibration_report)
    if calibration.get("status") != "passed":
        raise ValueError("Calibration must pass validation before parameters are frozen")
    if calibration.get("fixture_sha256") != source_record(args.workload_fixture)["sha256"]:
        raise ValueError("Calibration and configuration must use the exact same workload fixture")
    configs, manifest = build(calibration, read_json(args.workload_fixture), read_json(COVERAGE_PATH),
                              ground_rate_multiplier=args.ground_rate_multiplier,
                              ground_rate_sensitivity=args.ground_rate_sensitivity)
    manifest["inputs"] = {"calibration": source_record(args.calibration_report),
                          "workload_fixture": source_record(args.workload_fixture),
                          "coverage": source_record(COVERAGE_PATH), "builder": source_record(__file__)}
    output_bytes = {name: encoded(value) for name, value in configs.items()}
    manifest["config_sha256"] = {name: hashlib.sha256(data).hexdigest() for name, data in output_bytes.items()}
    output_bytes["manifest.json"] = encoded(manifest)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    for name, data in output_bytes.items():
        (args.output_dir / name).write_bytes(data)
    print(json.dumps({"output_dir": str(args.output_dir), "helper_rate_u_per_s": configs["main_config.json"]["compute_rate"],
                      "ready_arrival_summary": manifest["ready_arrival_summary"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
