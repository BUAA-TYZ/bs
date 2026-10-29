"""由冻结主配置生成单因素敏感性和首个成像窗口的数值收敛诊断。"""

import argparse
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.config import config_from_dict


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--variant", choices=("first-pass-dt1", "first-pass-dt05", "first-pass-dt025", "first-pass-dt01", "first-pass-dt005", "ground10x", "isl2500"), required=True)
    args = parser.parse_args()
    source = Path(args.prepared_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((source / "run.json").read_text())
    schedule = json.loads((source / "acquisition.json").read_text())
    provenance = deepcopy(manifest["provenance"])
    provenance["sensitivity"] = {"variant": args.variant, "source_directory": str(source.resolve())}
    info = deepcopy(manifest["case_info"][0])
    cfg = json.loads((source / f"{info['id']}_config.json").read_text())
    if args.variant.startswith("first-pass"):
        first_window = cfg["scheduled_task_arrivals"][0]["window_id"]
        events = [e for e in cfg["scheduled_task_arrivals"] if e["window_id"] == first_window]
        old_dt = cfg["dt"]
        new_dt = {"first-pass-dt1": 1, "first-pass-dt05": .5,
                  "first-pass-dt025": .25, "first-pass-dt01": .1, "first-pass-dt005": .05}[args.variant]
        duration_s = max(e["release_step"] * old_dt for e in events) + 14400
        cfg["scheduled_task_arrivals"] = [dict(e, release_step=round(e["release_step"] * old_dt / new_dt)) for e in events]
        for key in ("decision_interval_steps", "topology_update_steps", "ground_visibility_update_steps"):
            cfg[key] = round(cfg[key] * old_dt / new_dt)
        cfg["dt"] = new_dt
        cfg["sim_steps"] = round(duration_s / new_dt)
        cfg["task_arrival_end_step"] = max(e["release_step"] for e in cfg["scheduled_task_arrivals"]) + 1
        info.update(num_acquisitions=len(events), all_qualifying_events_retained=False,
                    selection="first chronological imaging window only, for timestep diagnostics, not headline performance")
        schedule["events"] = [e for e in schedule["events"] if e["window_id"] == first_window]
        provenance["scope"] = "first-pass numerical timestep diagnostic; not a separate performance test set"
    elif args.variant == "ground10x":
        for gs in cfg["ground_stations"]:
            gs["compute_rate"] *= 10
        provenance["compute_proxy"] += "; ground reference service rate multiplied by 10 as explicit sensitivity"
    else:
        cfg["topology"]["bandwidth_mbps_min"] = 2500
        cfg["topology"]["bandwidth_mbps_max"] = 2500
        provenance["nominal_rates_mbps"]["isl"] = 2500
        provenance["serialization_seconds"]["largest_packet_isl"] *= 4
    normalized = config_from_dict(cfg)
    manifest.update(provenance=provenance, case_info=[info], scope=provenance["scope"],
                    preparation={"command_args": vars(args), "valid_for_execution": "load saved config; original CLI fields are historical"})
    write_json(out / "run.json", manifest)
    write_json(out / "acquisition.json", schedule)
    write_json(out / f"{info['id']}_config.json", asdict(normalized))
    print(json.dumps({"variant": args.variant, "dt": cfg["dt"], "tasks": info["num_acquisitions"]}))


if __name__ == "__main__":
    main()
