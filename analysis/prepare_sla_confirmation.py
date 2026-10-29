"""在查看候选确认成绩前固定日期与成像负载，不根据策略结果选择场景。"""

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

from evaluate_highres_scene import build_cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    # 原epoch后第7/21/35天，低负载与原负载配对；只改变日期和每次成像量。
    offsets, loads = [7, 21, 35], [1, 3]
    plan = {"epoch": "2026-10-15T00:00:00Z", "day_offsets": offsets,
            "strips_per_pass": loads, "primary_seconds": 300,
            "selection": "fixed before confirmation outcomes; no replacement of poor geometry",
            "scope": "temporal/load confirmation in the same synthetic 24-satellite architecture; not universal generalization"}
    (out / "selection.json").write_text(json.dumps(plan, indent=2) + "\n")
    infos, acquisitions, provenance, empty = [], {}, None, []
    for load in loads:
        cfg_args = argparse.Namespace(days=max(offsets) + 1, architecture="heterogeneous",
            strips_per_pass=load, dt=1.0, compression_ratio=3.0, decision_batch=64,
            source_compute_rate=1.0, downlink_mbps=1200, efficiency=1.0,
            ground_compute_multiplier=1.0, isl_mbps=10000)
        cases, schedule, source_provenance = build_cases(cfg_args)
        provenance = source_provenance if provenance is None else provenance
        by_id = {info["id"]: (cfg, info) for cfg, info in cases}
        acquisitions[str(load)] = {**schedule, "events": [event for event in schedule["events"]
            if int(event["release_step"] // 86400) in offsets]}
        for offset in offsets:
            original_id = f"day{offset + 1:02d}"
            case_id = f"{original_id}_load{load}"
            if original_id not in by_id:
                empty.append(case_id)
                continue
            cfg, info = by_id[original_id]
            info = dict(info, id=case_id, strips_per_pass=load, split="confirmation")
            infos.append(info)
            (out / f"{case_id}_config.json").write_text(json.dumps(asdict(cfg), indent=2, allow_nan=False) + "\n")
    manifest = {"case_info": infos, "provenance": {**provenance, "confirmation_selection": plan},
                "empty_imaging_cases": empty, "selection_sha256": hashlib.sha256((out / "selection.json").read_bytes()).hexdigest(),
                "preparer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    (out / "run.json").write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n")
    (out / "acquisition.json").write_text(json.dumps(acquisitions, indent=2, allow_nan=False) + "\n")
    print(json.dumps({"cases": infos, "empty_imaging_cases": empty}, indent=2))


if __name__ == "__main__":
    main()
