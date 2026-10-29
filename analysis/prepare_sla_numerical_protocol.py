"""保留全部成像到达，将物理周期不变的细步长配置用于固定 SLA 比较。"""

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.check_twohop_window_timestep import prefix_config


def derive(source, target, dt, followup_seconds=301):
    if followup_seconds < 301:
        raise ValueError("Every image needs the full 300-second SLA plus one decision period")
    original = json.loads((source / "run.json").read_text())
    target.mkdir(parents=True, exist_ok=False)
    revision = {"dt": dt, "followup_seconds": followup_seconds,
        "base_prepared_dir": str(source),
        "base_manifest_sha256": hashlib.sha256((source / "run.json").read_bytes()).hexdigest(),
        "preparer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scope": "same physical arrivals and periods; complete P300/R300 observation, not eventual completion",
        "cases": {}}
    for info in original["case_info"]:
        path = source / f"{info['id']}_config.json"
        base = json.loads(path.read_text())
        if not base["scheduled_task_arrivals"]:
            raise ValueError("Empty acquisition case must be reported separately")
        last = max(event["release_step"] * base["dt"] for event in base["scheduled_task_arrivals"])
        cfg = asdict(prefix_config(base, dt, last + followup_seconds))
        temporal = {"dt", "sim_steps", "decision_interval_steps", "topology_update_steps",
                    "ground_visibility_update_steps", "deadline_steps", "task_arrival_end_step",
                    "scheduled_task_arrivals", "topology"}
        for key, value in base.items():
            if key not in temporal and (key not in cfg or json.loads(json.dumps(cfg[key])) != value):
                raise AssertionError(f"Configuration round-trip dropped or changed {key}")
        if len(cfg["scheduled_task_arrivals"]) != len(base["scheduled_task_arrivals"]):
            raise AssertionError("Numerical protocol dropped an arrival")
        physical_before = [{**event, "release_step": event["release_step"] * base["dt"]}
                           for event in base["scheduled_task_arrivals"]]
        physical_after = [{**event, "release_step": event["release_step"] * dt}
                          for event in cfg["scheduled_task_arrivals"]]
        if physical_before != physical_after:
            raise AssertionError("Numerical protocol changed acquisition time or metadata")
        revision["cases"][info["id"]] = {
            "base_config_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "arrival_count": len(physical_before), "last_release_seconds": last,
            "original_end_seconds": base["sim_steps"] * base["dt"],
            "observation_end_seconds": cfg["sim_steps"] * dt,
            "ground_input_relay": cfg["ground_input_relay"],
        }
        (target / path.name).write_text(json.dumps(cfg, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    original["numerical_protocol_revision"] = revision
    original["provenance"]["numerical_protocol_revision"] = revision
    if (source / "selection.json").exists():
        raw = (source / "selection.json").read_bytes()
        if hashlib.sha256(raw).hexdigest() != original["selection_sha256"]:
            raise ValueError("Original selection manifest changed")
        selection = json.loads(raw)
        selection["numerical_protocol_revision"] = revision
        (target / "selection.json").write_text(json.dumps(selection, indent=2, ensure_ascii=False) + "\n")
        original["selection_sha256"] = hashlib.sha256((target / "selection.json").read_bytes()).hexdigest()
    (target / "run.json").write_text(json.dumps(original, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    return revision


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-dir", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--dt", required=True, type=float)
    parser.add_argument("--followup-seconds", type=float, default=301)
    args = parser.parse_args()
    print(json.dumps(derive(args.prepared_dir, args.out_dir, args.dt, args.followup_seconds),
                     indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
