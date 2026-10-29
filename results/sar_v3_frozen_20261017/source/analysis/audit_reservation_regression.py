"""重放预约退化案例，保存动作和完整分片生命周期，核对既有结果。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import multiprocessing
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from audit_policy_behavior import audit_run
from sim.config import load_config
from sim.marl.scenarios import ScenarioDistribution


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    old = json.loads(Path("results/relay_reservation_20261010/summary.json").read_text())
    references = {(r["case"]["seed"], r["policy"]): r for r in old["records"]}
    cases = ScenarioDistribution("examples/scenario_roles.json", load_config("examples/config_relay.yaml")).cases("validation")
    records = []
    with ProcessPoolExecutor(max_workers=3, mp_context=multiprocessing.get_context("spawn")) as executor:
        jobs = [executor.submit(audit_run, cfg, policy, "", out / f"{case['id']}_{policy}")
                for cfg, case in cases if case["id"].startswith("uniform12")
                for policy in ("reservation", "reservation_independent")]
        for future in as_completed(jobs):
            row = future.result()
            reference = references[row["seed"], row["policy"]]["metrics"]
            if row["metrics"]["task_outcomes"] != reference["task_outcomes"]:
                raise ValueError("Audit no longer reproduces archived outcomes")
            records.append(row)
            print(json.dumps({"seed": row["seed"], "policy": row["policy"],
                              "actions": row["proposed_actions"], "compute": row["computed_unique_tiles"]}), flush=True)
    (out / "summary.json").write_text(json.dumps({"records": records, "archived_outcomes_reproduced": True}, indent=2))


if __name__ == "__main__":
    main()
