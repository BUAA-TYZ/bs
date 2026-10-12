"""重放两条整图接纳退化轨迹，保存决策报价与对应实际链路快照。"""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.config import load_config
from sim.env import SimulationEnv
from sim.marl.decision import decision_state
from sim.marl.scenarios import ScenarioDistribution
from main import make_policy


class AuditPolicy:
    def __init__(self, task, steps, name):
        self.delegate = make_policy(name, 0)
        self.original_plan = self.delegate.plan
        self.delegate.plan = self.plan
        self.task, self.steps, self.records = task, steps, []

    def select_actions(self, state):
        return self.delegate.select_actions(state)

    def plan(self, ledger, tiles):
        result = self.original_plan(ledger, tiles)
        if ledger.state.time in self.steps and any(t["parent_task_id"] == self.task for t in tiles.values()):
            self.records.append({"step": ledger.state.time, "tiles": tiles,
                                 "satellites": ledger.state.satellites,
                                 "links": ledger.state.links,
                                 "choices": result[1] if result else None,
                                 "compute_before": ledger.compute,
                                 "all_options_before": {tid: ledger.options(tid, tile) for tid, tile in tiles.items()}})
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--policy", default="reservation_joint")
    parser.add_argument("--reference-dir", default="results/relay_joint_reservation_20261010")
    args = parser.parse_args()
    cases = ScenarioDistribution("examples/scenario_roles.json", load_config("examples/config_relay.yaml")).cases("validation")
    output = []
    for (cfg, case), (task, steps) in zip(cases[:2], [("task_20", [605, 606, 609, 610]), ("task_25", [643, 644, 660, 661])]):
        policy, links = AuditPolicy(task, steps, args.policy), []
        env = SimulationEnv(cfg)
        try:
            for step in range(cfg.sim_steps):
                state = decision_state(env.export_state())
                if min(steps) <= step <= max(steps) + 10:
                    links.append({"step": step, "links": state.links})
                env.step(policy.select_actions(state))
            metrics = env.summary()
        finally:
            env.close()
        expected = json.loads((Path(args.reference_dir) /
                               f"{case['id']}_{args.policy}.json").read_text())
        if metrics["task_outcomes"] != expected["metrics"]["task_outcomes"]:
            raise ValueError("Replay differs from evaluation")
        output.append({"case": case, "quotes": policy.records, "actual_links": links,
                       "archived_outcomes_reproduced": True})
    with Path(args.out).open("x") as stream:
        json.dump(output, stream, indent=2, allow_nan=False)


if __name__ == "__main__":
    main()
