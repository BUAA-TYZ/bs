"""已知退化事件的定点干预，只验证接收通道竞争，不作为在线算法成绩。"""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import audit_policy_behavior as behavior
from sim.config import load_config
from sim.entities import Action, ActionType
from sim.evaluation import paired_task_metrics, task_age_score
from sim.marl.scenarios import ScenarioDistribution
from sim.scheduler.literature import CGSComputeAdapted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    out = Path(args.out_dir)
    cfg, case = ScenarioDistribution("examples/scenario_roles.json", load_config("examples/config_relay.yaml")).training_case(2026101400, 8)
    original = behavior.make_policy
    blocked = []

    class Intervention:
        def __init__(self):
            self.policy = CGSComputeAdapted(future_ground=True)

        def select_actions(self, state):
            actions = self.policy.select_actions(state)
            result = []
            for action in actions:
                tile = state.tiles[action.tile_id]
                if (state.time in (220, 221) and tile["parent_task_id"] == "task_29"
                        and action.target_gs_id == "gs_urumqi"):
                    blocked.append({"step": state.time, "tile": action.tile_id})
                    result.append(Action(action.tile_id, ActionType.WAIT))
                else:
                    result.append(action)
            return result

    def factory(name, *args, **kwargs):
        return Intervention() if name == "diagnostic_intervention" else original(name, *args, **kwargs)

    behavior.make_policy = factory
    try:
        row = behavior.audit_run(cfg, "diagnostic_intervention", "", out)
    finally:
        behavior.make_policy = original
    reference = json.loads(Path("results/ground_opportunity_probe_20261014/train_2026101400_8_cgs_ground_wait.json").read_text())
    report = {"scope": "case-specific intervention selected after inspecting outcomes; NOT an online policy or held-out result",
              "case": case, "held_actions": blocked,
              "paired": paired_task_metrics(reference["metrics"]["task_outcomes"], row["metrics"]["task_outcomes"]),
              "task_age": task_age_score(row["metrics"]["task_outcomes"], [1800,3600,7200,14400],cfg.sim_steps*cfg.dt),
              "affected_tasks": [r for r in row["metrics"]["task_outcomes"] if r["task_id"] in ("task_19","task_23","task_31","task_29")]}
    (out / "intervention.json").write_text(json.dumps(report, indent=2, allow_nan=False))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
