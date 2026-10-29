"""CPU 精确续训包装器：只把原验证循环的独立场景分派给多个进程。"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, replace
import hashlib
import io
import json
import multiprocessing
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import torch

import train_marl
from sim.config import load_config
from sim.marl.actor import SatActorCritic
from sim.marl.checkpoint import validate_checkpoint_protocol
from sim.marl.scenarios import ScenarioDistribution
from sim.reproducibility import runtime_metadata


ORIGINAL_EVALUATE = train_marl.evaluate
RUNTIME_KEYS = ("source_sha256", "python_version", "packages")
SNAPSHOT_NAMES = ("run.json", "last.pt", "train_log.jsonl")
SIDECAR_NAME = "resume_parallel_validation.json"
WORKER_MODEL = None


def digest(data):
    return hashlib.sha256(data).hexdigest()


def json_value(value):
    return json.loads(json.dumps(value, allow_nan=False))


def validation_digest(value):
    # 日志中的整数键会变成字符串；先统一键类型，再排序计算摘要。
    return digest(json.dumps(json_value(value), sort_keys=True, allow_nan=False).encode())


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def initialize_worker(model_config, state_bytes, torch_threads):
    global WORKER_MODEL
    torch.set_num_threads(torch_threads)
    WORKER_MODEL = SatActorCritic(**model_config)
    WORKER_MODEL.load_state_dict(torch.load(io.BytesIO(state_bytes), map_location="cpu", weights_only=True))
    WORKER_MODEL.eval()


def evaluate_worker(item):
    index, case, horizons = item
    cfg, info = case
    result = ORIGINAL_EVALUATE(WORKER_MODEL, cfg, [info["seed"]], torch.device("cpu"),
                               cases=[case], delivery_horizons=horizons)
    return index, result, os.getpid()


def parallel_evaluate(model, cfg, seeds, device, cases=None, delivery_horizons=None, *,
                      workers, model_config, torch_threads=1, guard=None, audit=None):
    if str(device) != "cpu" or workers < 1:
        raise ValueError("Parallel validation requires CPU and positive worker count")
    model.eval()
    cases = list(cases) if cases is not None else [
        (replace(cfg, seed=seed, tile_lifecycle_log=""), {"id": f"seed_{seed}", "seed": seed}) for seed in seeds]
    if not cases:
        raise ValueError("Validation requires at least one case")
    state = train_marl.rng_state()
    try:
        if guard:
            guard()
        stream = io.BytesIO()
        torch.save(model.state_dict(), stream)
        with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn"),
                                 initializer=initialize_worker,
                                 initargs=(model_config, stream.getvalue(), torch_threads)) as pool:
            results = list(pool.map(evaluate_worker,
                                   [(index, case, delivery_horizons) for index, case in enumerate(cases)],
                                   chunksize=1))
        if [index for index, _, _ in results] != list(range(len(cases))):
            raise RuntimeError("Parallel validation case order changed")
        rows = [result["cases"][0] for _, result, _ in results]
        metrics = [row["metrics"] for row in rows]
        completed = sum(record["overall"]["completed_tasks"] for record in metrics)
        latency = (sum((record["latency"]["task_mean"] or 0.0) * record["overall"]["completed_tasks"]
                       for record in metrics) / completed if completed else None)
        age = ({key: float(np.mean([row["task_age"][key] for row in rows]))
                for key in ("delivery_score", "restricted_mean_latency_seconds")} if delivery_horizons else {})
        result = {"task_completion_rate": float(np.mean([record["overall"]["task_completion_rate"] for record in metrics])),
                  "task_mean_seconds": latency, **age, "completed_tasks": completed,
                  "seeds": [row["seed"] for row in rows], "cases": rows}
        if guard:
            guard()
        if audit:
            audit({"case_ids": [row["id"] for row in rows], "worker_pids": [pid for _, _, pid in results],
                   "validation_sha256": validation_digest(result)})
        return result
    finally:
        # 进程启动和验证结果汇总不得改变后续 PPO 的任何随机数流。
        train_marl.restore_rng(state)


def inspect_resume(training_dir):
    root = Path(training_dir).resolve()
    snapshots = {name: (root / name).read_bytes() for name in SNAPSHOT_NAMES}
    metadata = json.loads(snapshots["run.json"])
    saved = torch.load(io.BytesIO(snapshots["last.pt"]), map_location="cpu", weights_only=True)
    validate_checkpoint_protocol(saved)
    runtime = runtime_metadata()
    if any(metadata.get(key) != runtime[key] for key in RUNTIME_KEYS):
        raise ValueError("Current source/runtime differs from original training")
    if any(json_value(saved.get(key)) != value for key, value in metadata.items()):
        raise ValueError("Checkpoint metadata differs from original run.json")
    arguments = dict(saved["training_args"])
    defaults = vars(train_marl.parser().parse_args(["--config", arguments["config"], "--out-dir", arguments["out_dir"]]))
    if arguments.keys() != defaults.keys() or arguments.get("resume") or arguments.get("device") != "cpu":
        raise ValueError("Expected first exact CPU continuation with complete original arguments")
    if Path(arguments["out_dir"]).resolve() != root:
        raise ValueError("Original output directory does not match training directory")
    episode, budget = saved.get("episode"), arguments.get("episodes")
    if type(episode) is not int or type(budget) is not int or not 1 <= episode < budget:
        raise ValueError("Resume requires a completed episode in 1..episodes-1")
    for key in ("optimizer_state", "rng_state", "best_score", "model_state"):
        if key not in saved:
            raise ValueError(f"Incomplete exact-resume checkpoint: {key}")
    logs = [json.loads(line) for line in snapshots["train_log.jsonl"].splitlines() if line.strip()]
    if [row.get("episode") for row in logs] != list(range(1, episode + 1)):
        raise ValueError("Training log does not end at checkpoint episode")
    if json_value(saved.get("validation")) != logs[-1].get("validation"):
        raise ValueError("Checkpoint validation differs from final original log entry")
    paths = {"config": Path(arguments["config"]).resolve()}
    cfg = load_config(str(paths["config"]))
    if json_value(asdict(cfg)) != metadata["config"]:
        raise ValueError("Configuration differs from original training")
    if arguments.get("scenario_distribution"):
        paths["distribution"] = Path(arguments["scenario_distribution"]).resolve()
        distribution = ScenarioDistribution(paths["distribution"], cfg)
        if (metadata["scenario_distribution"]["sha256"] != distribution.sha256
                or metadata["scenario_distribution"]["spec"] != distribution.spec):
            raise ValueError("Scenario distribution differs from original training")
    inputs = {name: (path, path.read_bytes()) for name, path in paths.items()}
    wrapper = Path(__file__).resolve()
    wrapper_bytes = wrapper.read_bytes()

    def guard():
        if (runtime_metadata() != runtime or wrapper.read_bytes() != wrapper_bytes
                or any(path.read_bytes() != content for path, content in inputs.values())):
            raise ValueError("Source, wrapper, runtime or configuration changed during continuation")

    guard()
    if any((root / name).read_bytes() != content for name, content in snapshots.items()):
        raise ValueError("Original training artifacts changed during continuation preflight")
    arguments["resume"] = True
    return root, snapshots, metadata, saved, argparse.Namespace(**arguments), inputs, guard


def resume_training(training_dir, workers, *, command=None):
    if workers < 1:
        raise ValueError("Worker count must be positive")
    root, snapshots, metadata, saved, args, inputs, guard = inspect_resume(training_dir)
    sidecar_path = root / SIDECAR_NAME
    snapshot_dir = root / "resume_parallel_validation_source"
    if sidecar_path.exists() or snapshot_dir.exists():
        raise FileExistsError("Parallel continuation evidence already exists")
    snapshot_dir.mkdir()
    source_files = {}
    for name, content in snapshots.items():
        target = snapshot_dir / name
        target.write_bytes(content)
        source_files[name] = {"path": str(target), "sha256": digest(content), "bytes": len(content)}
    wrapper_path = Path(__file__).resolve()
    wrapper_snapshot = snapshot_dir / "wrapper_source.py"
    wrapper_snapshot.write_bytes(wrapper_path.read_bytes())
    sidecar = {"schema_version": 1, "kind": "exact_cpu_resume_parallel_validation",
               "status": "prepared", **{key: metadata[key] for key in RUNTIME_KEYS},
               "training_dir": str(root), "starting_episode": saved["episode"],
               "next_episode": saved["episode"] + 1, "episode_budget": args.episodes,
               "workers": workers, "multiprocessing_start_method": "spawn", "torch_threads": args.torch_threads,
               "command": list(command if command is not None else [sys.executable, *sys.argv]),
               "wrapper": {"path": str(wrapper_path), "sha256": digest(wrapper_snapshot.read_bytes()),
                           "snapshot_path": str(wrapper_snapshot)},
               "source_files": source_files,
               "input_files": {name: {"path": str(path), "sha256": digest(content)}
                               for name, (path, content) in inputs.items()},
               "original_training_args": metadata["training_args"], "resumed_training_args": vars(args),
               "changes": {"training_args": {"resume": {"before": False, "after": True}},
                           "validation": "Identical original per-case evaluate, spawn workers, original case order"},
               "rng": "Original CPU optimizer/model/RNG restored by train; validation preserves parent Python/NumPy/Torch RNG",
               "validation_calls": []}
    write_json(sidecar_path, sidecar)

    def audit(record):
        sidecar["validation_calls"].append(record)
        write_json(sidecar_path, sidecar)

    def evaluate(model, cfg, seeds, device, cases=None, delivery_horizons=None):
        return parallel_evaluate(model, cfg, seeds, device, cases, delivery_horizons,
                                 workers=workers, model_config=saved["model_config"],
                                 torch_threads=args.torch_threads, guard=guard, audit=audit)

    original = train_marl.evaluate
    try:
        guard()
        if any((root / name).read_bytes() != content for name, content in snapshots.items()):
            raise ValueError("Original process is still changing training artifacts")
        train_marl.evaluate = evaluate
        sidecar["status"] = "running"
        write_json(sidecar_path, sidecar)
        train_marl.train(args)
        guard()
        final = torch.load(root / "last.pt", map_location="cpu", weights_only=True)
        logs = [json.loads(line) for line in (root / "train_log.jsonl").read_text().splitlines() if line.strip()]
        if final["episode"] != args.episodes or logs[-1]["episode"] != args.episodes:
            raise ValueError("Continuation returned without its final checkpoint/log")
        sidecar["status"] = "completed"
        sidecar["final_files"] = {name: {"path": str(root / name), "sha256": digest((root / name).read_bytes())}
                                  for name in SNAPSHOT_NAMES}
        write_json(sidecar_path, sidecar)
    except BaseException as error:
        sidecar["status"] = "failed"
        sidecar["error"] = {"type": type(error).__name__, "message": str(error)}
        write_json(sidecar_path, sidecar)
        raise
    finally:
        train_marl.evaluate = original
    return sidecar


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--training-dir", required=True)
    parser.add_argument("--workers", required=True, type=int)
    args = parser.parse_args()
    result = resume_training(args.training_dir, args.workers)
    print(json.dumps({"status": result["status"], "episode": result["episode_budget"],
                      "sidecar": str(Path(args.training_dir) / SIDECAR_NAME)}), flush=True)


if __name__ == "__main__":
    main()
