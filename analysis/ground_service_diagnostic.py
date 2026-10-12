"""单独诊断下行流服务顺序；相同物理容量下按任务串行接入。"""

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.result_relay_study import run
from sim.config import load_config
from sim.marl.scenarios import ScenarioDistribution
from sim.network import advance_ground_transfers


def task_serial_ground_transfers(env, transfers):
    # 只控制哪些流获得本步服务；沿用原接入、容量和字节推进规则。
    grouped = defaultdict(list)
    for transfer in transfers:
        grouped[(transfer.src_sat, transfer.gs_id)].append(transfer)
    selected = []
    for flows in grouped.values():
        task_ids = {env.tiles[tr.tile_id].parent_task_id for tr in flows}
        first = min(task_ids, key=lambda tid: (env.tasks[tid].release_time, tid))
        selected.extend(tr for tr in flows if env.tiles[tr.tile_id].parent_task_id == first)
    advance_ground_transfers(env, selected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="examples/config_relay.yaml")
    parser.add_argument("--distribution", default="examples/scenario_roles.json")
    parser.add_argument("--case", default="shared24_a")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=False)
    cfg = next(cfg for cfg, info in ScenarioDistribution(args.distribution, load_config(args.config)).cases("validation")
               if info["id"] == args.case)
    records = []
    rule = {"ground_service_rule": "oldest task per satellite/station; shared round-robin station capacity",
            "diagnostic_implementation_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    with patch("sim.pipeline.distributed.advance_ground_transfers", task_serial_ground_transfers):
        for label in ("ground_tiles", "ground_relay", "window_relay"):
            result = run(cfg, label, out / label, False, rule)
            records.append({"label": label, "overall": result["overall"],
                            "task_age_delivery_curve": result["task_age_delivery_curve"]})
    report = {"scope": "development service-rule diagnostic; not trained-policy performance",
              "case": args.case, "service_rule": "oldest task per satellite/station; shared round-robin station capacity",
              "implementation_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "runs": records}
    (out / "service_rule.json").write_text(json.dumps(report, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
