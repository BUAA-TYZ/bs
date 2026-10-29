"""登记固定推理工作量的调度分组，以及各方法共享且计算前不可见的合成识别输出。"""

import argparse
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import random
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import yaml

from analysis.build_sar_workload_fixture import build_fixture
from sim.config import config_from_dict
from sim.workload import ImageTiling


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def derive(base, group, objects=30, empty_probability=0.1):
    fixture = build_fixture()
    cfg = deepcopy(base)
    tiling = cfg["image_tiling"]
    tiling.update(windows_per_tile_axis=group, deduplicate_within_tile=True)
    encoding = fixture["output_encoding"]
    header = encoding["window_header_bytes"]
    record = encoding["obb_record_bytes"] + encoding["roi_header_bytes"] + encoding["preview_bytes"]
    expected_records = fixture["summary"]["sum_record_count"] / len(fixture["windows"])
    tiling["result_size_mb_per_window"] = (
        header + expected_records * objects / 100 * (1 - empty_probability) * record) / 1e6
    for key in ("num_tiles", "compute_cost_per_tile", "result_size_mb"):
        cfg.pop(key, None)
    image_records = []
    for index, event in enumerate(cfg["scheduled_task_arrivals"]):
        # 独立局部RNG，不随策略动作、打包尺寸或任务处理顺序变化。
        identity = f"sar-output-v1:{cfg['seed']}:{index}:{event['source_sat_id']}:{event['release_step']}"
        rng = random.Random(int.from_bytes(hashlib.sha256(identity.encode()).digest(), "big"))
        count = 0 if rng.random() < empty_probability else objects
        selected = set(rng.sample(range(100), count))
        window_counts = [sum(r["object_id"] in selected for r in w["records"])
                         for w in fixture["windows"]]
        event["window_result_bytes"] = [header + n * record for n in window_counts]
        image_records.append({"task_id": f"task_{index}", "unique_objects": count,
                              "selected_object_ids": sorted(selected), "window_counts": window_counts,
                              "empty_windows": window_counts.count(0),
                              "result_bytes": sum(event["window_result_bytes"])})
    return asdict(config_from_dict(cfg)), image_records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--update-current", action="store_true")
    args = parser.parse_args()
    source = ROOT / "results/sar_parameter_contract_20261016/main_config.json"
    old_manifest = json.loads(source.with_name("manifest.json").read_text())
    if sha(source) != old_manifest["config_sha256"][source.name]:
        raise ValueError("Source configuration checksum failed")
    base = json.loads(source.read_text())
    configs, workloads = {}, {}
    for name, group, objects in (("main_config.json", 2, 30), ("tiles_64_config.json", 1, 30),
                                  ("tiles_4_config.json", 4, 30), ("empty_config.json", 2, 0),
                                  ("dense_config.json", 2, 100)):
        config, truth = derive(base, group, objects)
        assert sum(t.compute_cost for t in ImageTiling.from_dict(config["image_tiling"]).tiles()) == 64
        assert config["onboard_compute_profile"] == base["onboard_compute_profile"]
        configs[name], workloads[name] = config, truth
    assert workloads["main_config.json"] == workloads["tiles_64_config.json"] == workloads["tiles_4_config.json"]
    out = args.out_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    for name, config in configs.items():
        write(out / name, config)
    write(out / "output_workloads.json", workloads)
    manifest = {
        "schema_version": 1,
        "config_sha256": {name: sha(out / name) for name in configs},
        "inputs": {"network_contract": {"path": str(source.relative_to(ROOT)), "sha256": sha(source)},
                   "generator": {"path": str(Path(__file__).resolve().relative_to(ROOT)), "sha256": sha(Path(__file__))},
                   "fixture_generator": {"path": "analysis/build_sar_workload_fixture.py",
                                         "sha256": sha(ROOT / "analysis/build_sar_workload_fixture.py")}},
        "hypothesis": "Grouping adjacent windows reduces repeated input bytes and dispatch overhead but may reduce parallelism; report both gains and losses against ground relay and literature rules",
        "fixed_work": "64 identical 4096 inference windows per image, same measured cohort curve, same arrivals and exogenous detections across methods/groups",
        "result_model": {"main_nonempty_image_objects": 30, "empty_image_probability": 0.1,
                         "window_header_bytes": 28, "object_record_and_preview_bytes": 262204,
                         "meaning": "Controlled synthetic output-load assumption, not measured port density or SAR accuracy",
                         "overlap_rule": "Same object may appear in multiple window outputs; all outputs sent before ground merge",
                         "information_rule": "Actual result sizes only exposed after all windows in that dispatch packet finish; scheduling before compute uses population mean",
                         "compute_rule": "Fixed service independent of detection count; conservative proxy retaining measured controlled postprocessing cost, not a new benchmark",
                         "empty_rule": "Every window must compute and return its 28-byte completion header; no image/preview for empty detections"},
        "grouping": {name: ImageTiling.from_dict(c["image_tiling"]).manifest() for name, c in configs.items()},
        "limitations": old_manifest["pending_before_final_realism_claim"][:3] + [
            "Planner hypothetical new work is conservatively serialized; current commitments use cohort service",
            "No per-dispatch wall-time charge; host scheduling wall time reported separately",
            "No object-density inference from synthetic placement; empty/dense configurations are sensitivity controls"],
        "held_out": "Not accessed; development day only",
    }
    write(out / "manifest.json", manifest)
    if args.update_current:
        (ROOT / "examples/config_sar_current.yaml").write_text(
            "# 当前SAR：16调度块、64推理窗；块内像素去重，空窗仅完成确认。\n"
            "# 星间有效2 Gbps、两邻星；计算前不向策略暴露真实目标数。\n"
            "# 参数与假设：results/sar_dispatch_20261016/manifest.json。\n"
            + yaml.safe_dump(configs["main_config.json"], allow_unicode=True, sort_keys=False))
    truth = workloads["main_config.json"]
    print(json.dumps({"configs": len(configs), "tasks": len(truth),
                      "empty_images": sum(r["unique_objects"] == 0 for r in truth),
                      "empty_windows": sum(r["empty_windows"] for r in truth),
                      "main_tiles": configs["main_config.json"]["num_tiles"]}))


if __name__ == "__main__":
    main()
