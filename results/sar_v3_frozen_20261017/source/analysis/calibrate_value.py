"""同数据、同预算比较顺序与混合 critic 校准，actor 全程冻结。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import asdict
import hashlib
import io
import json
import multiprocessing
from pathlib import Path
import sys
import time

import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.config import load_config
from sim.marl.actor import SatActorCritic
from sim.marl.checkpoint import validate_checkpoint_protocol
from sim.marl.scenarios import ScenarioDistribution
from sim.marl.value_calibration import (balanced_value_dataset, collect_value_case,
                                       evaluate_value_critic, fit_value_critic)
from sim.reproducibility import runtime_metadata


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--config", default="examples/config_relay.yaml")
    p.add_argument("--distribution", default="examples/scenario_roles.json")
    p.add_argument("--seed", type=int, default=1007)
    p.add_argument("--train-cases", type=int, default=18)
    p.add_argument("--probe-cases", type=int, default=9)
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--epochs", type=int, default=32)
    p.add_argument("--minibatch", type=int, default=256)
    p.add_argument("--rows-per-case", type=int, default=512)
    p.add_argument("--uniform-rows", type=int, default=512)
    p.add_argument("--lr", type=float, default=3e-4)
    args = p.parse_args()
    if min(args.train_cases, args.probe_cases, args.workers, args.epochs, args.minibatch, args.uniform_rows) < 1:
        raise ValueError("Counts must be positive")
    if args.rows_per_case < 2 or args.rows_per_case % 2 or args.rows_per_case % args.minibatch:
        raise ValueError("Rows per case must be even and divisible by minibatch for matched update counts")
    torch.set_num_threads(1)
    checkpoint_bytes = Path(args.checkpoint).read_bytes()
    saved = torch.load(io.BytesIO(checkpoint_bytes), weights_only=True, map_location="cpu")
    validate_checkpoint_protocol(saved)
    cfg = load_config(args.config)
    if saved["config"] != asdict(cfg) or not saved.get("delivery_horizons_seconds"):
        raise ValueError("Calibration requires identical base environment and a task-age checkpoint")
    horizons = saved["delivery_horizons_seconds"]
    end = cfg.task_arrival_end_step if cfg.task_arrival_end_step is not None else cfg.sim_steps
    if (cfg.sim_steps - end) * cfg.dt < max(horizons):
        raise ValueError("Calibration requires full post-arrival follow-up")
    distribution = ScenarioDistribution(args.distribution, cfg)
    count = args.train_cases + args.probe_cases
    seeds = set(range(args.seed + 1, args.seed + count + 1))
    heldout = {case["seed"] for split in ("validation", "test") for case in distribution.spec[split]}
    if seeds & heldout:
        raise ValueError("Calibration seeds overlap validation/test")
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    runtime = runtime_metadata()
    metadata = {"args": vars(args), **runtime,
                "actor_checkpoint_sha256": hashlib.sha256(checkpoint_bytes).hexdigest(),
                "scenario_distribution_sha256": distribution.sha256,
                "fit_seeds": list(range(args.seed + 1, args.seed + args.train_cases + 1)),
                "probe_seeds": list(range(args.seed + args.train_cases + 1, args.seed + count + 1)),
                "protocol": "fixed stochastic BC actor; equal-case 50% decision/50% uniform-time fitting; same rows and update budget",
                "probe_scope": "unseen by critic fitting, sampled only from training distribution; not final test"}
    (out / "run.json").write_text(json.dumps(metadata, indent=2))
    cases = {}
    started = time.monotonic()
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as executor:
        pending = {}
        for episode in range(1, count + 1):
            case_cfg, scenario = distribution.training_case(args.seed, episode)
            future = executor.submit(collect_value_case, case_cfg, saved["model_config"],
                                     saved["model_state"], horizons, args.uniform_rows)
            pending[future] = (episode, scenario)
        for future in as_completed(pending):
            episode, scenario = pending[future]
            case = future.result()
            case["scenario"] = scenario
            if case["metrics"]["experiment"]["source_sha256"] != runtime["source_sha256"]:
                raise ValueError("Simulation source changed during calibration")
            cases[episode] = case
            print(json.dumps({"collected": len(cases), "total": count, "episode": episode,
                              "role": scenario["role_profile"], "satellites": scenario["num_sats"],
                              "active_rows": case["sampling"]["active_rows"]}), flush=True)
    collection_seconds = time.monotonic() - started
    ordered = [cases[i] for i in range(1, count + 1)]
    training, probes = ordered[:args.train_cases], ordered[args.train_cases:]
    torch.save({"metadata": metadata, "training": training, "probes": probes}, out / "data.pt")
    datasets = balanced_value_dataset(training, args.rows_per_case, args.seed)
    base = SatActorCritic(**saved["model_config"])
    base.load_state_dict(saved["model_state"])
    report = {**metadata, "collection_seconds": collection_seconds,
              "training_active_rows": sum(c["sampling"]["active_rows"] for c in training),
              "probe_active_rows": sum(c["sampling"]["active_rows"] for c in probes),
              "stages": {"initial": {"training": evaluate_value_critic(base, training),
                                       "probe": evaluate_value_critic(base, probes)}}}
    for mode in ("sequential", "pooled"):
        model = deepcopy(base)
        fit = fit_value_critic(model, datasets, mode, args.epochs, args.minibatch, args.lr, args.seed)
        report["stages"][mode] = {"fit": fit, "training": evaluate_value_critic(model, training),
                                  "probe": evaluate_value_critic(model, probes)}
        torch.save({**saved, **runtime, "episode": 0, "validation": None,
                    "model_state": model.state_dict(),
                    "value_calibration": {**metadata, "mode": mode, "fit": fit,
                                          "episode_semantics": "PPO episode 0 after critic-only calibration"}}, out / f"{mode}.pt")
        print(json.dumps({"mode": mode, "fit": fit, "probe": report["stages"][mode]["probe"]["active"]}), flush=True)
    if report["stages"]["pooled"]["fit"]["updates"] != report["stages"]["sequential"]["fit"]["updates"]:
        raise ValueError("Optimizer budgets differ")
    (out / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False))
    lines = ["# Critic Calibration", "", metadata["protocol"], "", metadata["probe_scope"], "",
             "| Stage | Fit active MSE | Probe active MSE | Probe normalized MSE | Probe explained variance |",
             "|---|---:|---:|---:|---:|"]
    for label, stage in report["stages"].items():
        train, probe = stage["training"]["active"], stage["probe"]["active"]
        lines.append(f"| {label} | {train['mse']:.6f} | {probe['mse']:.6f} | {probe['normalized_mse']:.4f} | {probe['explained_variance']:.4f} |")
    lines += ["", "The actor is unchanged in every stage. Critic accuracy does not establish a scheduling improvement.", ""]
    (out / "summary.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
