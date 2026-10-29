"""缓存真实教师轨迹，并以 episode 等权目标拟合独立 BC 对照。"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
import hashlib
import io
import json
import math
import multiprocessing
import os
from pathlib import Path
import sys
import time

for thread_variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[thread_variable] = "1"

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import torch
from torch.nn import functional as F

from analysis.export_overlap_validation import check_geometry_and_trace, check_runtime, digest, json_value
from sim.config import load_config
from sim.marl.actor import SatActorCritic
from sim.marl.checkpoint import validate_checkpoint_protocol
from sim.marl.imitation import collect_demonstrations
from sim.marl.scenarios import ScenarioDistribution, config_digest
from sim.reproducibility import runtime_metadata


OBJECTIVE = "L = (1/E) sum_e (1/N_e) sum_valid_tile_labels_in_e CE"
ESTIMATOR = "For B uniformly shuffled rows among R: L_hat = R/(E*B) sum_batch_valid_labels CE/N_episode"


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def load_checkpoint(path, expected_hash=None):
    data = Path(path).read_bytes()
    if expected_hash is not None and digest(data) != expected_hash:
        raise ValueError("Initial checkpoint changed")
    result = torch.load(io.BytesIO(data), map_location="cpu", weights_only=True)
    validate_checkpoint_protocol(result)
    return result, digest(data)


def initialize_worker():
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)


def collect_one(cfg, case, initial_path, initial_hash, output, teacher):
    started = time.monotonic()
    initial, _ = load_checkpoint(initial_path, initial_hash)
    check_runtime(initial, runtime_metadata(), "demonstration worker")
    model = SatActorCritic(**initial["model_config"])
    model.load_state_dict(initial["model_state"])
    dataset, record = collect_demonstrations(model, cfg, torch.device("cpu"), teacher)
    if dataset is None:
        raise ValueError(f"Empty demonstration episode: {case['id']}")
    record = json_value(record)
    payload = {"case": case, "config": json_value(asdict(cfg)), "config_sha256": config_digest(cfg),
               "source_sha256": initial["source_sha256"], "initial_sha256": initial_hash,
               "teacher": teacher, "dataset": dataset, "record": record}
    torch.save(payload, output)
    check_runtime(record["metrics"]["experiment"], initial, case["id"])
    check_geometry_and_trace(record["metrics"], cfg)
    return {"case": case, "file": Path(output).name, "sha256": digest(Path(output).read_bytes()),
            "rows": record["samples"], "tile_labels": sum(record["action_counts"].values()),
            "action_counts": record["action_counts"], "collection_elapsed_s": time.monotonic() - started}


def validate_dataset(dataset, expected_rows=None, expected_counts=None):
    if not isinstance(dataset, (tuple, list)) or len(dataset) != 4:
        raise ValueError("Expected observations, tile masks, action masks and labels")
    obs, tiles, masks, actions = dataset
    rows = len(obs)
    if (rows <= 0 or obs.ndim != 2 or obs.dtype != torch.float32 or tiles.ndim != 2
            or tiles.dtype != torch.bool or masks.dtype != torch.bool or actions.dtype != torch.int64
            or actions.shape != tiles.shape or masks.shape[:2] != tiles.shape or masks.ndim != 3
            or tiles.shape[0] != rows or not tiles.any(dim=1).all()
            or not torch.isfinite(obs).all()):
        raise ValueError("Invalid demonstration tensor dimensions/dtypes")
    if actions.min() < 0 or actions.max() >= masks.shape[-1] or not masks.gather(-1, actions[..., None]).squeeze(-1)[tiles].all():
        raise ValueError("Teacher labels violate action masks")
    counts = {str(int(key)): int(value) for key, value in zip(*torch.unique(actions[tiles], return_counts=True))}
    if expected_rows is not None and rows != expected_rows:
        raise ValueError("Demonstration row count differs from record")
    if expected_counts is not None and counts != {str(key): value for key, value in expected_counts.items()}:
        raise ValueError("Demonstration labels differ from teacher record")
    return rows, int(tiles.sum())


def load_cache(cache_dir, expected, cases):
    cache = Path(cache_dir)
    index = json.loads((cache / "index.json").read_text())
    if any(index.get(key) != value for key, value in expected.items()):
        raise ValueError("Cached demonstrations belong to a different source/initialization/protocol")
    if [row["case"] for row in index["episodes"]] != [info for cfg, info in cases]:
        raise ValueError("Cached episode set/order differs")
    datasets, records = [], []
    for entry, (cfg, info) in zip(index["episodes"], cases):
        path = (cache / entry["file"]).resolve()
        if path.parent != cache.resolve():
            raise ValueError("Invalid demonstration cache path")
        content = path.read_bytes()
        if digest(content) != entry["sha256"]:
            raise ValueError("Demonstration tensor SHA256 mismatch")
        saved = torch.load(io.BytesIO(content), weights_only=True, map_location="cpu")
        if (saved["case"] != info or saved["config"] != json_value(asdict(cfg))
                or saved["config_sha256"] != config_digest(cfg)
                or any(saved[key] != expected[key] for key in ("source_sha256", "initial_sha256", "teacher"))):
            raise ValueError("Demonstration payload metadata mismatch")
        record = saved["record"]
        check_geometry_and_trace(record["metrics"], cfg)
        check_runtime(record["metrics"]["experiment"], runtime_metadata(), info["id"])
        validate_dataset(saved["dataset"], record["samples"], record["action_counts"])
        datasets.append(saved["dataset"])
        records.append(record)
    return datasets, records, index


def episode_equal_loss(logits, tiles, masks, actions, episode_ids, label_counts, total_rows):
    """均匀 row 小批次对完整 episode 标签均值目标的无偏估计。"""
    masked = logits.masked_fill(~masks, torch.finfo(logits.dtype).min)
    loss = F.cross_entropy(masked[tiles], actions[tiles], reduction="none")
    ids = episode_ids[:, None].expand_as(tiles)[tiles]
    weights = label_counts.to(dtype=loss.dtype)[ids].reciprocal()
    return (loss * weights).sum() * total_rows / (len(label_counts) * len(logits))


def fit_equal(model, datasets, *, seed, lr, epochs, minibatch, expected_updates, output=None, shuffle_state=None):
    counts = torch.tensor([int(data[1].sum()) for data in datasets], dtype=torch.int64)
    if not len(counts) or (counts <= 0).any():
        raise ValueError("Every episode must contain valid tile labels")
    data = tuple(torch.cat([dataset[i] for dataset in datasets]) for i in range(4))
    obs, tiles, masks, actions = data
    episode_ids = torch.cat([torch.full((len(dataset[0]),), i, dtype=torch.int64) for i, dataset in enumerate(datasets)])
    rows = len(obs)
    updates = math.ceil(rows / minibatch) * epochs
    if updates != expected_updates:
        raise ValueError(f"Optimizer budget differs: {updates} != {expected_updates}")
    # 使用同种子建模后的 RNG 状态，保留原 pooled BC 的打乱序列。
    generator = torch.Generator().manual_seed(seed)
    if shuffle_state is not None:
        generator.set_state(shuffle_state)
    shuffle_sha256 = digest(bytes(generator.get_state().tolist()))
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, eps=1e-5)
    epoch_log = []
    model.train()
    for epoch in range(1, epochs + 1):
        started, estimates = time.monotonic(), []
        for idx in torch.randperm(rows, generator=generator).split(minibatch):
            logits, _ = model(obs[idx])
            loss = episode_equal_loss(logits, tiles[idx], masks[idx], actions[idx], episode_ids[idx], counts, rows)
            optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), .5)
            optimizer.step()
            estimates.append((float(loss.detach()), len(idx)))
        record = {"epoch": epoch, "updates": len(estimates),
                  "row_weighted_mean_of_minibatch_estimates": sum(value * size for value, size in estimates) / rows,
                  "elapsed_s": time.monotonic() - started}
        epoch_log.append(record)
        if output is not None:
            with (Path(output) / "fit_log.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, allow_nan=False) + "\n")
        print(json.dumps(record), flush=True)
    model.eval()
    correct, labels = Counter(), Counter()
    loss_by_episode = [0.] * len(datasets)
    correct_by_episode = [0] * len(datasets)
    with torch.no_grad():
        for idx in torch.arange(rows).split(minibatch):
            logits, _ = model(obs[idx])
            valid = tiles[idx]
            masked = logits.masked_fill(~masks[idx], torch.finfo(logits.dtype).min)[valid]
            target = actions[idx][valid]
            prediction = masked.argmax(-1)
            losses = F.cross_entropy(masked, target, reduction="none")
            ids = episode_ids[idx, None].expand_as(valid)[valid]
            for episode in ids.unique().tolist():
                selected = ids == episode
                loss_by_episode[episode] += float(losses[selected].double().sum())
                correct_by_episode[episode] += int((prediction[selected] == target[selected]).sum())
            for label in target.unique().tolist():
                labels[label] += int((target == label).sum())
                correct[label] += int(((prediction == target) & (target == label)).sum())
    fit = {"rows": rows, "tile_labels": int(counts.sum()), "optimizer_updates": updates,
           "objective": OBJECTIVE, "estimator": ESTIMATOR,
           "initial_shuffle_rng_sha256": shuffle_sha256,
           "final_episode_equal_loss": sum(value / int(n) for value, n in zip(loss_by_episode, counts)) / len(counts),
           "agreement": sum(correct.values()) / sum(labels.values()),
           "class_recall": {str(key): correct[key] / n for key, n in labels.items()},
           "class_counts": {str(key): value for key, value in labels.items()},
           "per_episode": [{"episode": i + 1, "rows": len(datasets[i][0]), "tile_labels": int(n),
                            "label_mean_loss": loss_by_episode[i] / int(n),
                            "agreement": correct_by_episode[i] / int(n)} for i, n in enumerate(counts)],
           "epochs": epoch_log, "scope": "fit on cached training demonstrations only; no validation/test executed"}
    return fit


def compare_original(records, path):
    if not path.exists():
        return {"available": False, "path": str(path), "note": "Original warmstart JSON has not been written yet"}
    original = json.loads(path.read_text())["episodes"]
    if len(original) != len(records) or any(any(first[key] != second[key] for key in ("samples", "action_counts", "metrics"))
                                           for first, second in zip(original, records)):
        raise ValueError("Replayed teacher samples/actions/environment outcomes differ from original warmstart")
    return {"available": True, "path": str(path), "sha256": digest(path.read_bytes()),
            "samples_actions_and_full_environment_summaries_equal": True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--initial-checkpoint", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--distribution", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--cache-dir", help="Reuse an already complete demonstration cache")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--episodes", type=int, default=8)
    parser.add_argument("--seed", type=int, default=1701)
    parser.add_argument("--lr", type=float, default=.001)
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--minibatch", type=int, default=512)
    parser.add_argument("--expected-updates", type=int, default=3210)
    args = parser.parse_args(argv)
    if min(args.workers, args.episodes, args.lr, args.epochs, args.minibatch, args.expected_updates) <= 0:
        raise ValueError("All training counts and learning rate must be positive")
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    initial_path = Path(args.initial_checkpoint).resolve()
    initial, initial_hash = load_checkpoint(initial_path)
    runtime = runtime_metadata()
    check_runtime(initial, runtime, "initial checkpoint")
    base = load_config(args.config)
    distribution = ScenarioDistribution(args.distribution, base)
    if (initial["episode"] != 0 or initial["config"] != asdict(base)
            or initial["scenario_distribution"]["sha256"] != distribution.sha256
            or initial["seed"] != args.seed or initial["training_args"]["warmstart_episodes"] != args.episodes
            or initial["training_args"]["dagger_episodes"] != 0
            or initial["model_config"]["feature_set"] != "overlap_contact"
            or initial_path.name != "initial.pt" or initial.get("validation") is not None
            or initial["training_args"]["warmstart_mode"] != "pooled"
            or initial["training_args"]["device"] != "cpu"):
        raise ValueError("Require the original overlap BC initialization and exactly its training cases")
    for original_name, current in (("warmstart_lr", args.lr), ("warmstart_epochs", args.epochs), ("minibatch", args.minibatch)):
        if initial["training_args"][original_name] != current:
            raise ValueError(f"Control training budget differs: {original_name}")
    teacher = initial["training_args"]["warmstart_teacher"]
    cases = [distribution.training_case(args.seed, i) for i in range(1, args.episodes + 1)]
    out = Path(args.out_dir).resolve()
    out.mkdir(parents=True, exist_ok=False)
    (out / ".gitignore").write_text("demonstrations/\n", encoding="utf-8")
    expected = {"source_sha256": runtime["source_sha256"], "initial_sha256": initial_hash,
                "distribution_sha256": distribution.sha256, "config_sha256": config_digest(base), "teacher": teacher}
    metadata = {**runtime, "training_stage": "episode_equal_bc", "seed": args.seed,
                "observation_version": initial["observation_version"], "model_config": initial["model_config"],
                "parameter_count": initial["parameter_count"],
                "config": initial["config"], "feature_protocol": initial.get("feature_protocol"),
                "ground_station_ids": initial["ground_station_ids"],
                "delivery_horizons_seconds": initial["delivery_horizons_seconds"],
                "scenario_distribution": initial["scenario_distribution"],
                "training_args": vars(args), "parent_training_args": initial["training_args"],
                "initialization": {"path": str(initial_path), "sha256": initial_hash, "episode": 0},
                "external_trainer": {"path": str(Path(__file__).resolve()), "sha256": digest(Path(__file__).read_bytes())},
                "objective": OBJECTIVE, "minibatch_estimator": ESTIMATOR,
                "normalization": "N_e counts valid tile labels; no per-batch sum-of-weights normalization; final short batch uses its actual B",
                "shuffle": "CPU generator receives RNG state after the same seeded model construction; deterministic teacher collection consumes no torch RNG",
                "scope": "independent BC treatment; no PPO, validation or test evaluation in this script",
                "teacher": teacher, "cases": [{"case": info, "config": json_value(asdict(cfg))} for cfg, info in cases]}
    write_json(out / "run.json", metadata)
    cache = Path(args.cache_dir).resolve() if args.cache_dir else out / "demonstrations"
    if not args.cache_dir:
        cache.mkdir()
        collected = {}
        with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn"),
                                 initializer=initialize_worker) as pool:
            futures = {pool.submit(collect_one, cfg, info, str(initial_path), initial_hash,
                                   str(cache / f"episode_{i:03d}.pt"), teacher): i
                       for i, (cfg, info) in enumerate(cases, 1)}
            for future in as_completed(futures):
                index = futures[future]
                try:
                    collected[index] = future.result()
                except Exception as error:
                    print(json.dumps({"failed_episode": index, "error": repr(error)}), flush=True)
                    raise
                print(json.dumps({"collected_episode": index, **collected[index]}), flush=True)
                if runtime_metadata() != runtime:
                    raise ValueError("Core source/runtime changed during collection")
        write_json(cache / "index.json", {**expected, "episodes": [collected[i] for i in range(1, args.episodes + 1)]})
    datasets, records, cache_index = load_cache(cache, expected, cases)
    comparison = compare_original(records, initial_path.parent / "warmstart.json")
    torch.manual_seed(args.seed)
    model = SatActorCritic(**initial["model_config"])
    model.load_state_dict(initial["model_state"])
    fit = fit_equal(model, datasets, seed=args.seed, lr=args.lr, epochs=args.epochs, minibatch=args.minibatch,
                    expected_updates=args.expected_updates, output=out, shuffle_state=torch.get_rng_state())
    if runtime_metadata() != runtime or digest(initial_path.read_bytes()) != initial_hash:
        raise ValueError("Source/runtime/initialization changed during fitting")
    comparison = compare_original(records, initial_path.parent / "warmstart.json")
    cache_provenance = {"path": str(cache), "index_sha256": digest((cache / "index.json").read_bytes()),
                        "episodes": cache_index["episodes"], "original_teacher_comparison": comparison}
    checkpoint = {**metadata, "episode": 0, "validation": None, "model_state": model.state_dict(),
                  "demonstration_cache": cache_provenance, "fit": fit}
    torch.save(checkpoint, out / "teacher_bc.pt")
    report = {**metadata, "demonstration_cache": cache_provenance, "fit": fit,
              "checkpoint": {"path": str(out / "teacher_bc.pt"), "sha256": digest((out / "teacher_bc.pt").read_bytes())}}
    write_json(out / "summary.json", report)
    lines = ["# Episode-Equal BC Fit", "", OBJECTIVE, "", ESTIMATOR, "",
             "This is a fit diagnostic on cached training demonstrations. No validation/test performance is claimed.", "",
             f"Rows: {fit['rows']}; tile labels: {fit['tile_labels']}; optimizer updates: {fit['optimizer_updates']}.",
             f"Final episode-equal loss: {fit['final_episode_equal_loss']:.6f}; overall fit agreement: {fit['agreement']:.6f}.",
             "", "| Action | Labels | Recall |", "|---|---:|---:|"]
    lines += [f"| {key} | {count} | {fit['class_recall'][key]:.6f} |" for key, count in fit["class_counts"].items()]
    (out / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(out), "fit": {key: value for key, value in fit.items() if key not in ("epochs", "per_episode")}}), flush=True)


if __name__ == "__main__":
    main()
