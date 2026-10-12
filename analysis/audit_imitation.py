"""开发集同状态教师/学生决策审计；不把开发集样本写入训练数据。"""

import argparse
import json
from pathlib import Path
import sys

import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.config import load_config
from sim.evaluation import task_age_score
from sim.marl.actor import SatActorCritic
from sim.marl.imitation import collect_demonstrations
from sim.marl.scenarios import ScenarioDistribution
from sim.reproducibility import runtime_metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--config", default="examples/config_relay.yaml")
    parser.add_argument("--distribution", default="examples/scenario_roles.json")
    parser.add_argument("--teacher", default="greedy", choices=("greedy", "window_aware"))
    parser.add_argument("--rollouts", nargs="+", choices=("learner", "teacher"), default=["learner", "teacher"])
    args = parser.parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(1)
    saved = torch.load(args.checkpoint, map_location="cpu", weights_only=True)
    model = SatActorCritic(**saved["model_config"])
    model.load_state_dict(saved["model_state"])
    metadata = {**runtime_metadata(), "checkpoint": args.checkpoint,
                "checkpoint_source_sha256": saved["source_sha256"], "teacher": args.teacher,
                "scope": "development diagnosis only; no fitting"}
    (out / "run.json").write_text(json.dumps(metadata, indent=2))
    records = []
    for cfg, info in ScenarioDistribution(args.distribution, load_config(args.config)).cases("validation"):
        for rollout in args.rollouts:
            _, record = collect_demonstrations(model, cfg, "cpu", args.teacher, rollout == "learner")
            record.update(case=info["id"], seed=cfg.seed)
            record["task_age"] = task_age_score(record["metrics"]["task_outcomes"], [1800, 3600, 7200, 14400],
                                                 record["metrics"]["experiment"]["elapsed_sim_seconds"])
            (out / f"{info['id']}_{rollout}.json").write_text(json.dumps(record, indent=2))
            records.append(record)
            print(json.dumps({k: record[k] for k in ("case", "rollout", "agreement", "task_age",
                                                    "action_counts", "prediction_counts")}), flush=True)
    (out / "summary.json").write_text(json.dumps({**metadata, "records": records}, indent=2))


if __name__ == "__main__":
    main()
