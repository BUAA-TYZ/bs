"""固定实测计算服务，登记链路变量与尚未接入的分片候选，不运行调度实验。"""

import argparse
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sim.config import config_from_dict
from sim.workload import ImageTiling, window_starts


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
                    encoding="utf-8")


def make_config(base, nominal_mbps=2500, isl_efficiency=0.8, ground_efficiency=0.8, neighbors=2):
    config = deepcopy(base)
    config["topology"].update(
        bandwidth_mbps_min=nominal_mbps * isl_efficiency,
        bandwidth_mbps_max=nominal_mbps * isl_efficiency,
        bandwidth_period=0, bandwidth_noise=0, bandwidth_distance_scale_km=0,
        max_neighbors=neighbors,
    )
    for station in config["ground_stations"]:
        station["bandwidth_mbps"] = 1200 * ground_efficiency
    return asdict(config_from_dict(config))


def packet_candidates(base):
    workload = ImageTiling.from_dict(base["image_tiling"])
    overlap = workload.window_px - workload.stride_px
    recut = []
    for side in (4096, 5120, 6400, 8192):
        xs = window_starts(workload.width_px, side, side - overlap)
        ys = window_starts(workload.height_px, side, side - overlap)
        count = len(xs) * len(ys)
        size = workload._transport_size_mb((0, 0, side, side))
        recut.append({"side_px": side, "tiles": count, "tile_mb": size,
                      "all_tiles_mb": count * size,
                      "serialization_s_by_effective_isl_mbps": {
                          str(rate): size * 8 / rate for rate in (80, 480, 2000, 8000)},
                      "compute_calibrated": side == workload.window_px})
    grouped = []
    for group in (1, 2, 4):
        values = deepcopy(base["image_tiling"])
        values["windows_per_tile_axis"] = group
        packets = ImageTiling.from_dict(values).tiles()
        sizes = []
        for packet in packets:
            boxes = packet.windows_xyxy
            bounds = (min(b[0] for b in boxes), min(b[1] for b in boxes),
                      max(b[2] for b in boxes), max(b[3] for b in boxes))
            sizes.append(workload._transport_size_mb(bounds))
        grouped.append({
            "windows_per_tile_axis": group, "dispatch_tiles": len(packets),
            "inference_windows": sum(len(p.windows_xyxy) for p in packets),
            "deduplicated_input_mb": sum(sizes), "max_packet_mb": max(sizes),
            "current_packer_input_mb": sum(p.data_size_mb for p in packets),
            "runnable_with_measured_executor": group == 1,
        })
    return {"overlap_px": overlap, "inference_window_px": workload.window_px,
            "recut_geometry_only": recut, "fixed_inference_group_candidates": grouped,
            "selection_rule": "Select dispatch size on development data with the same 64 inference windows; never reuse 4096 service time for a larger network input",
            "pending": ["Pixel deduplication within each dispatch packet",
                        "Multi-window dispatch packet execution with measured cohorts and completion accounting",
                        "Per-image empty and positive outputs shared by all methods",
                        "Measure or report dispatch overhead; do not invent a per-packet link acquisition cost"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    source = ROOT / "results/sar_compute_profile_20261016/main_config.json"
    source_manifest = source.with_name("manifest.json")
    previous = json.loads(source_manifest.read_text())
    if previous["config_sha256"][source.name] != digest(source):
        raise ValueError("Measured source configuration hash mismatch")
    base = json.loads(source.read_text())
    calibration = ROOT / "results/jetson_concurrency_20261016/summary.json"
    configs = {"main_config.json": make_config(base)}
    for rate in (100, 600, 10000):
        configs[f"isl_nominal_{rate}_config.json"] = make_config(base, nominal_mbps=rate)
    for efficiency in (0.6, 1.0):
        configs[f"isl_eta_{efficiency:g}_config.json"] = make_config(base, isl_efficiency=efficiency)
    configs["ground_eta_1_config.json"] = make_config(base, ground_efficiency=1)
    configs["neighbors_4_config.json"] = make_config(base, neighbors=4)
    main_cfg = configs["main_config.json"]
    # 除明确登记的链路条件，任务、计算服务、内存与图像处理均不得改变。
    for config in configs.values():
        for key, value in base.items():
            if key not in {"topology", "ground_stations"} and config[key] != value:
                raise ValueError(f"Unexpected non-network change: {key}")
        for before, after in zip(base["ground_stations"], config["ground_stations"], strict=True):
            if {k: v for k, v in before.items() if k != "bandwidth_mbps"} != {
                    k: v for k, v in after.items() if k != "bandwidth_mbps"}:
                raise ValueError("Ground computation or geometry changed")
    out = args.out_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    for name, config in configs.items():
        write_json(out / name, config)
    write_json(out / "packet_candidates.json", packet_candidates(base))
    manifest = {
        "schema_version": 1,
        "status": "Prepared network baseline; grouping and empty-output execution still pending; no new performance results",
        "config_sha256": {name: digest(out / name) for name in configs},
        "inputs": {"previous_config": {"path": str(source.relative_to(ROOT)), "sha256": digest(source)},
                   "previous_manifest": {"path": str(source_manifest.relative_to(ROOT)), "sha256": digest(source_manifest)},
                   "compute_calibration": {"path": str(calibration.relative_to(ROOT)), "sha256": digest(calibration)},
                   "generator": {"path": str(Path(__file__).resolve().relative_to(ROOT)), "sha256": digest(Path(__file__))}},
        "link_contract": {
            "nominal_isl_mbps": 2500, "isl_payload_efficiency": 0.8,
            "effective_isl_mbps": 2000, "simultaneous_neighbors": 2,
            "nominal_ground_mbps": 1200, "ground_payload_efficiency": 0.8,
            "effective_ground_mbps": 960, "ground_receive_channels": 1,
            "max_range_km": 4500,
            "efficiency_basis": "Declared engineering sensitivity, not measured throughput",
            "rate_model": "Constant payload capacity during admitted geometric contacts; zero while disconnected; per-directed-edge sharing; no arbitrary sine/noise law",
            "duplex_assumption": "Independent equal-rate directional capacities; not a measured aggregate duplex throughput",
            "terminal_selection": "Deterministic nearest-edge matching with both endpoint degree limits, not an optimized terminal scheduler",
            "power_scope": "SCOT80-class two active channels imply approximately 120-160 W and 23.8 kg for terminals alone; platform power/thermal feasibility remains unverified",
        },
        "fixed_compute": {"service_s": main_cfg["onboard_compute_profile"]["service_s"],
                          "shared_memory_gb": main_cfg["shared_memory_gb"],
                          "scope": "AGX Orin ground proxy, same hardware/service for source and helper satellites, fixed 4096 inference operator",
                          "ground_beta": 10},
        "pending_before_final_realism_claim": [
            "Link acquisition/repointing time and propagation are not charged by current transfer execution",
            "No battery, thermal, SAR peak-load or whole-platform power feasibility model",
            "Source orbit schedule is a geometric workload, not an operator-validated task plan",
            "Empty detections need acknowledgements; current executable output size is still a uniform positive-result proxy",
            "Reduced dispatch tile count must preserve the measured inference workload",
        ],
        "experiment_rule": "Single-factor comparisons on the same development arrivals; main network revision versus historical configuration is not an algorithm gain; keep held-out data sealed",
        "sources": [
            {"url": "https://www.tesat.de/images/tesat/products/250820_DataSheet_SCOT80_A4.pdf",
             "supports": "2.5/10 Gbps nominal channels, 60-80 W, 11.9 kg per optical channel; not measured payload efficiency"},
            {"url": "https://www.tesat.de/news/blog/942-kepler-validates-tesat-optical-communication-terminals-in-orbit-for-kepler-network",
             "supports": "LEO-LEO SCOT80 in-orbit link validation; no sustained application throughput curve"},
            {"url": "https://zenodo.org/records/7244968/files/Session%202B-Le_Lann-Image%20Compression%20on%20Pleiades%20Neo.pdf",
             "supports": "1.2 Gbps aggregate X-band downlink on another imaging platform; cross-platform reference, not this SAR constellation"},
            {"url": "https://sar.iceye.com/6.0.8/productspecification/imagingmodes/",
             "supports": "Spot Fine image geometry; not hardware configuration or measured pipeline service"},
        ],
    }
    write_json(out / "manifest.json", manifest)
    print(json.dumps({"out_dir": str(out), "prepared_configs": len(configs),
                      "effective_isl_mbps": 2000, "effective_ground_mbps": 960,
                      "inference_windows": main_cfg["num_tiles"], "experiments_run": False}))


if __name__ == "__main__":
    main()
