"""固定场景、改变动作采样，检验轨迹间可利用的交付收益差异。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
import json
import multiprocessing
from pathlib import Path
import statistics
import sys

import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.config import SimConfig
from sim.env import SimulationEnv
from sim.evaluation import task_age_score
from sim.marl.actor import SatActorCritic
from sim.marl.decision import build_batch, decode_batch
from sim.reproducibility import runtime_metadata


def rollout(cfg, model_config, weights, horizons, policy_seed):
    torch.set_num_threads(1)
    model = SatActorCritic(**model_config)
    model.load_state_dict(weights)
    model.eval()
    torch.manual_seed(policy_seed)
    env = SimulationEnv(cfg)
    try:
        for _ in range(cfg.sim_steps):
            state = env.export_state()
            obs, tiles, masks, tile_ids = build_batch(state, **model.feature_options)
            with torch.no_grad():
                action, _, _, _ = model.get_action_and_value(
                    torch.as_tensor(obs), torch.as_tensor(tiles), action_mask=torch.as_tensor(masks))
            env.step(decode_batch(action.numpy(), state, tile_ids))
        metrics = env.summary()
        age = task_age_score(metrics["task_outcomes"], horizons, env.time * cfg.dt)
    finally:
        env.close()
    return {"scenario_seed": cfg.seed, "policy_seed": policy_seed, "task_age": age, "metrics": metrics}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", required=True)
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--seeds", default="1019,1020,1028,1010")
    p.add_argument("--repeats", type=int, default=4)
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--out-dir", required=True)
    args = p.parse_args()
    seeds = list(map(int, args.seeds.split(",")))
    if args.repeats < 2 or args.workers < 1:
        raise ValueError("Require at least two trajectories and one worker")
    data = torch.load(args.data, weights_only=True, map_location="cpu")
    saved = torch.load(args.checkpoint, weights_only=True, map_location="cpu")
    cases = {c["seed"]: c for c in data["training"] + data["probes"]}
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    runtime = runtime_metadata()
    records = []
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as executor:
        pending = []
        for seed in seeds:
            cfg = SimConfig(**cases[seed]["metrics"]["experiment"]["config"])
            for repeat in range(args.repeats):
                pending.append(executor.submit(rollout, cfg, saved["model_config"], saved["model_state"],
                                               saved["delivery_horizons_seconds"], 300_000 + repeat))
        for future in as_completed(pending):
            record = future.result()
            reference = cases[record["scenario_seed"]]["metrics"]["experiment"]
            for key in ("config", "task_trace_sha256", "tle_sha256"):
                if record["metrics"]["experiment"][key] != reference[key]:
                    raise ValueError(f"Scenario changed: {key}")
            if record["metrics"]["experiment"]["source_sha256"] != runtime["source_sha256"]:
                raise ValueError("Source changed during probe")
            records.append(record)
            print(json.dumps({"completed": len(records), "scenario": record["scenario_seed"],
                              "policy_seed": record["policy_seed"], "score": record["task_age"]["delivery_score"]}), flush=True)
    summaries = []
    for seed in seeds:
        scores = [r["task_age"]["delivery_score"] for r in records if r["scenario_seed"] == seed]
        summaries.append({"seed": seed, "scenario": cases[seed]["scenario"], "scores": scores,
                          "min": min(scores), "max": max(scores), "std": statistics.pstdev(scores),
                          "constant_return_in_original_rollout": float(cases[seed]["data"]["active"]["target"].std(unbiased=False)) < 1e-6})
    report = {"args": vars(args), **runtime, "checkpoint_sha256": hashlib.sha256(Path(args.checkpoint).read_bytes()).hexdigest(),
              "scope": "selected training-distribution diagnostics, not an independent test or an optimization upper bound",
              "summaries": summaries, "records": sorted(records, key=lambda r: (r["scenario_seed"], r["policy_seed"]))}
    (out / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False))
    print(json.dumps(summaries, indent=2))


if __name__ == "__main__":
    main()
