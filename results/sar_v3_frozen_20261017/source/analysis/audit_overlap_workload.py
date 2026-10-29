"""输出重叠窗口/打包核算及可运行配置，不产生算法性能结论。"""

import argparse
from dataclasses import asdict, replace
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.config import load_config, config_from_dict
from sim.workload import ImageTiling


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", default="examples/workload_overlap.json")
    parser.add_argument("--base-config", default="examples/config_relay.yaml")
    parser.add_argument("--output-dir", default="results/overlap_workload_audit")
    args = parser.parse_args()
    source = json.loads(Path(args.workload).read_text(encoding="utf-8"))
    workload = ImageTiling.from_dict(source["image_tiling"])
    rows = []
    for overlap in (200, 512):
        for group in (1, 2, 4):
            candidate = replace(workload, stride_px=workload.window_px - overlap,
                                windows_per_tile_axis=group)
            manifest = candidate.manifest()
            rows.append({"overlap_px": overlap, "group_axis": group, **manifest})
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / "audit.json").write_text(json.dumps({"input": source, "cases": rows},
                                                ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    cfg = asdict(load_config(args.base_config))
    for key in ("image_size_mb", "num_tiles", "compute_cost_per_tile", "result_size_mb"):
        cfg.pop(key)
    cfg["image_tiling"] = source["image_tiling"]
    cfg = asdict(config_from_dict(cfg))
    (output / "config.json").write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n",
                                        encoding="utf-8")
    print("Synthetic accounting only; no inference, quality measurement, or policy evaluation.")
    for row in rows:
        print(f"overlap={row['overlap_px']} group={row['group_axis']}x{row['group_axis']} "
              f"windows={row['num_windows']} packets={row['num_tiles']} "
              f"raw={row['image_size_mb']:.6f}MB input={row['packet_input_mb']:.6f}MB "
              f"work={row['total_compute_cost']:.6f} result={row['unmerged_result_mb']:.6f}MB")
    print(f"Saved audit and explicit synthetic configuration to {output}")


if __name__ == "__main__":
    main()
