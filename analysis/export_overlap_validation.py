"""核验并导出训练已有的 BC/PPO 验证轨迹，不重新执行仿真。"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import io
import json
from pathlib import Path
import re
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import torch

from analysis.report_overlap_study import HORIZONS, aggregate, close, context, result_table, validate_record
from sim.config import load_config
from sim.evaluation import task_age_score
from sim.marl.checkpoint import validate_checkpoint_protocol
from sim.marl.scenarios import ScenarioDistribution, config_digest
from sim.reproducibility import runtime_metadata
from sim.workload import ImageTiling


FILES = ("run.json", "warmstart.json", "train_log.jsonl", "teacher_bc.pt", "warmstart.pt", "last.pt")
RUNTIME_KEYS = ("source_sha256", "python_version", "packages")
RESUME_SIDECAR = "resume_parallel_validation.json"
RESUME_ARCHIVE = "resume_parallel_validation_source"
RESUME_FILES = ("run.json", "last.pt", "train_log.jsonl")


def json_value(value):
    return json.loads(json.dumps(value, allow_nan=False))


def digest(data):
    return hashlib.sha256(data).hexdigest()


def json_digest(value, *, sort_keys=False):
    return digest(json.dumps(value, sort_keys=sort_keys, allow_nan=False).encode())


def same_weights(first, second):
    if not isinstance(first, dict) or not first or first.keys() != second.keys():
        return False
    return all(isinstance(tensor, torch.Tensor) and isinstance(second[key], torch.Tensor)
               and tensor.dtype == second[key].dtype and tensor.shape == second[key].shape
               and torch.equal(tensor, second[key]) for key, tensor in first.items())


def read_snapshot(root):
    absent = [name for name in FILES if not (root / name).is_file()]
    if absent:
        raise ValueError(f"Completed training artifacts required; missing: {', '.join(absent)}")
    return {name: (root / name).read_bytes() for name in FILES}


def check_runtime(actual, expected, label):
    if any(actual.get(key) != expected[key] for key in RUNTIME_KEYS):
        raise ValueError(f"Runtime/source hash mismatch: {label}")


def check_resume_lineage(root, snapshot, metadata, logs, inputs):
    """只接受已归档并完成的指定包装器续训，不放宽其他实验身份检查。"""
    if not metadata["training_args"].get("resume"):
        if (root / RESUME_SIDECAR).exists():
            raise ValueError("Unexpected continuation sidecar for non-resumed training")
        return metadata, None
    if not (root / RESUME_SIDECAR).is_file():
        raise ValueError("Unknown resumed training: completed continuation evidence is required")
    sidecar_bytes = (root / RESUME_SIDECAR).read_bytes()
    sidecar = json.loads(sidecar_bytes)
    if (sidecar.get("schema_version") != 1 or sidecar.get("kind") != "exact_cpu_resume_parallel_validation"
            or sidecar.get("status") != "completed" or Path(sidecar["training_dir"]).resolve() != root):
        raise ValueError("Only a completed exact CPU parallel-validation continuation is supported")
    check_runtime(sidecar, metadata, "continuation sidecar")
    snapshot[RESUME_SIDECAR] = sidecar_bytes
    if set(sidecar["source_files"]) != set(RESUME_FILES) or set(sidecar["final_files"]) != set(RESUME_FILES):
        raise ValueError("Continuation requires all original and final run/checkpoint/log hashes")
    original = {}
    for name in RESUME_FILES:
        relative = f"{RESUME_ARCHIVE}/{name}"
        path = root / relative
        saved = sidecar["source_files"][name]
        content = path.read_bytes()
        if (Path(saved["path"]).resolve() != path.resolve() or saved["sha256"] != digest(content)
                or saved["bytes"] != len(content)):
            raise ValueError(f"Original continuation artifact differs: {name}")
        final = sidecar["final_files"][name]
        if Path(final["path"]).resolve() != root / name or final["sha256"] != digest(snapshot[name]):
            raise ValueError(f"Final continuation artifact differs: {name}")
        original[name] = content
        snapshot[relative] = content
    if set(sidecar["input_files"]) != {"config", "distribution"}:
        raise ValueError("Continuation input files must identify configuration and distribution")
    for name, entry in sidecar["input_files"].items():
        path, content = inputs[name]
        if Path(entry["path"]).resolve() != path or entry["sha256"] != digest(content):
            raise ValueError(f"Continuation input hash/path differs: {name}")
    wrapper = sidecar["wrapper"]
    wrapper_path = Path(__file__).resolve().with_name("resume_parallel_validation.py")
    archive_name = f"{RESUME_ARCHIVE}/wrapper_source.py"
    wrapper_bytes = (root / archive_name).read_bytes()
    if (Path(wrapper["path"]).resolve() != wrapper_path
            or Path(wrapper["snapshot_path"]).resolve() != root / archive_name
            or wrapper["sha256"] != digest(wrapper_bytes) or wrapper_path.read_bytes() != wrapper_bytes):
        raise ValueError("Continuation wrapper differs from the archived, reviewed implementation")
    snapshot[archive_name] = wrapper_bytes
    inputs["resume_wrapper"] = (wrapper_path, wrapper_bytes)
    original_metadata = json.loads(original["run.json"])
    check_runtime(original_metadata, metadata, "original continuation metadata")
    before, after = original_metadata["training_args"], metadata["training_args"]
    if (before.get("resume") is not False or after.get("resume") is not True
            or before.get("device") != "cpu" or after != {**before, "resume": True}
            or sidecar["original_training_args"] != before or sidecar["resumed_training_args"] != after
            or {key: value for key, value in original_metadata.items() if key != "training_args"}
            != {key: value for key, value in metadata.items() if key != "training_args"}):
        raise ValueError("Continuation may change only training_args.resume from false to true")
    if (type(sidecar.get("workers")) is not int or sidecar["workers"] < 1
            or sidecar.get("multiprocessing_start_method") != "spawn"
            or type(sidecar.get("torch_threads")) is not int or sidecar["torch_threads"] < 1
            or sidecar["torch_threads"] != before.get("torch_threads")):
        raise ValueError("Continuation worker/thread protocol differs")
    previous = torch.load(io.BytesIO(original["last.pt"]), map_location="cpu", weights_only=True)
    validate_checkpoint_protocol(previous)
    if any(json_value(previous.get(key)) != value for key, value in original_metadata.items()):
        raise ValueError("Original continuation checkpoint metadata differs")
    start, budget = previous.get("episode"), after["episodes"]
    if (type(start) is not int or not 1 <= start < budget
            or sidecar["starting_episode"] != start or sidecar["next_episode"] != start + 1
            or sidecar["episode_budget"] != budget
            or any(key not in previous for key in ("model_state", "optimizer_state", "rng_state", "best_score"))):
        raise ValueError("Original continuation checkpoint is incomplete or has a different episode budget")
    prefix = [json.loads(line) for line in original["train_log.jsonl"].splitlines() if line.strip()]
    if ([row.get("episode") for row in prefix] != list(range(1, start + 1))
            or not snapshot["train_log.jsonl"].startswith(original["train_log.jsonl"])
            or logs[:start] != prefix or json_value(previous.get("validation")) != prefix[-1].get("validation")):
        raise ValueError("Original training log/checkpoint prefix was not preserved")
    validations = [row["validation"] for row in logs[start:] if row.get("validation")]
    calls = sidecar["validation_calls"]
    if len(calls) != len(validations) or not calls:
        raise ValueError("Continuation validation call coverage differs from training log")
    for call, validation in zip(calls, validations):
        ids = [row["id"] for row in validation["cases"]]
        pids = call.get("worker_pids", [])
        if (call.get("case_ids") != ids or call.get("validation_sha256") != json_digest(validation, sort_keys=True)
                or len(pids) != len(ids) or any(type(pid) is not int or pid <= 0 for pid in pids)
                or len(set(pids)) > sidecar["workers"]):
            raise ValueError("Continuation validation audit differs from logged results/case order")
    return original_metadata, {"kind": sidecar["kind"], "sidecar": RESUME_SIDECAR,
                               "sidecar_sha256": digest(sidecar_bytes), "starting_episode": start,
                               "episode_budget": budget, "validation_calls_verified": len(calls),
                               "only_training_argument_change": "resume=false to resume=true",
                               "original_log_prefix_preserved": True,
                               "wrapper_sha256": wrapper["sha256"]}


def check_geometry_and_trace(metrics, cfg):
    exp = metrics["experiment"]
    workload = ImageTiling.from_dict(cfg.image_tiling)
    if json_value(exp.get("workload")) != json_value(workload.manifest()) or exp.get("workload_sha256") != json_digest(workload.manifest(), sort_keys=True):
        raise ValueError("Workload manifest/hash mismatch")
    if exp.get("tle_sha256") != json_digest(cfg.topology["tle_lines"]):
        raise ValueError("TLE hash mismatch")
    sizes = [float(tile.data_size_mb) for tile in workload.tiles()]
    trace = []
    for row in metrics["task_outcomes"]:
        release = row["release_seconds"] / cfg.dt
        if not close(release, round(release)):
            raise ValueError("Task release is not on the simulation time grid")
        trace.append((row["task_id"], row["source_sat_id"], round(release), sizes))
    if exp.get("task_trace_sha256") != json_digest(trace):
        raise ValueError("Task trace hash disagrees with outcomes and packet sizes")


def convert_validation(validation, cases, method, checkpoint_hash, manifest):
    rows = validation["cases"]
    by_id = {row["id"]: row for row in rows}
    if len(rows) != len(by_id) or by_id.keys() != cases.keys():
        raise ValueError(f"Incomplete/duplicate validation case set: {method}")
    if validation.get("seeds") != [info["seed"] for cfg, info in cases.values()]:
        raise ValueError(f"Validation seeds/order mismatch: {method}")
    records = []
    for case_id, (cfg, info) in cases.items():
        original = by_id[case_id]
        if {key: original.get(key) for key in info} != info or original.get("config_sha256") != config_digest(cfg):
            raise ValueError(f"Validation case definition/config hash mismatch: {method}/{case_id}")
        metrics = original["metrics"]
        if metrics["experiment"]["config"] != json_value(asdict(cfg)):
            raise ValueError(f"Validation effective config mismatch: {method}/{case_id}")
        check_runtime(metrics["experiment"], manifest, f"{method}/{case_id}")
        check_geometry_and_trace(metrics, cfg)
        record = {"case": info, "method": method, "metrics": metrics,
                  "checkpoint_sha256": checkpoint_hash, "task_age": original["task_age"],
                  "action_counts": original["action_counts"], "config_sha256": original["config_sha256"],
                  "elapsed_s": None, "decision_timing": None,
                  "origin": {"kind": "training_validation_export", "new_simulation": False,
                             "timing": "not measured by the training validation loop"}}
        validate_record(record, manifest, {"case": info, "config": json_value(asdict(cfg))})
        record["task_age"] = task_age_score(metrics["task_outcomes"], HORIZONS, cfg.sim_steps * cfg.dt)
        records.append(record)
    for key in ("delivery_score", "restricted_mean_latency_seconds"):
        recomputed = statistics.mean(record["task_age"][key] for record in records)
        if not close(validation[key], recomputed):
            raise ValueError(f"Validation aggregate mismatch: {method}/{key}")
    if validation["completed_tasks"] != sum(record["metrics"]["overall"]["completed_tasks"] for record in records):
        raise ValueError(f"Validation completed count mismatch: {method}")
    return records


def export_validation(training_dir, config_path, distribution_path, out_dir, *, bc_name="bc", ppo_name="ppo"):
    if bc_name == ppo_name or any(not re.fullmatch(r"[a-z0-9_-]+", name) for name in (bc_name, ppo_name)):
        raise ValueError("Choose distinct lowercase BC/PPO method names")
    if {bc_name, ppo_name} & {"ground_compute", "ground_tiles", "ground_relay", "local", "cgs_compute_adapted"}:
        raise ValueError("Learned method names must not impersonate baseline methods")
    root = Path(training_dir).resolve()
    runtime = runtime_metadata()
    snapshot = read_snapshot(root)
    inputs = {"config": (Path(config_path).resolve(), Path(config_path).read_bytes()),
              "distribution": (Path(distribution_path).resolve(), Path(distribution_path).read_bytes())}
    metadata = json.loads(snapshot["run.json"])
    warm = json.loads(snapshot["warmstart.json"])
    logs = [json.loads(line) for line in snapshot["train_log.jsonl"].splitlines() if line.strip()]
    check_runtime(metadata, runtime, "training metadata vs current checkout")
    args = metadata["training_args"]
    episode = args["episodes"]
    if type(episode) is not int or episode <= 0:
        raise ValueError("Export requires a positive completed training budget")
    if [row["episode"] for row in logs] != list(range(1, episode + 1)) or not logs[-1].get("validation"):
        raise ValueError("Final training episode and validation log are not complete")
    if not warm.get("episodes") or not warm.get("validation") or args.get("warmstart_mode") != "pooled":
        raise ValueError("A completed pooled BC warmstart and its validation are required")
    original_metadata, continuation = check_resume_lineage(root, snapshot, metadata, logs, inputs)
    base = load_config(str(config_path))
    distribution = ScenarioDistribution(distribution_path, base)
    if base.image_tiling is None or distribution.spec["version"] != 3 or base.decision_interval_steps != 1:
        raise ValueError("Export requires overlap schema v3 and decision_interval_steps=1")
    if metadata["config"] != json_value(asdict(base)):
        raise ValueError("Base configuration differs from training")
    if metadata["scenario_distribution"]["sha256"] != distribution.sha256 or metadata["scenario_distribution"]["spec"] != distribution.spec:
        raise ValueError("Scenario distribution/hash differs from training")
    if metadata["delivery_horizons_seconds"] != HORIZONS:
        raise ValueError("Expected complete 30/60/120/240-minute objective")
    checkpoints = {name: torch.load(io.BytesIO(snapshot[name]), map_location="cpu", weights_only=True)
                   for name in ("teacher_bc.pt", "warmstart.pt", "last.pt")}
    for name, checkpoint in checkpoints.items():
        validate_checkpoint_protocol(checkpoint)
        expected_metadata = metadata if name == "last.pt" else original_metadata
        if any(json_value(checkpoint.get(key)) != value for key, value in expected_metadata.items()):
            raise ValueError(f"Checkpoint metadata differs from run.json: {name}")
    bc, initial, last = (checkpoints[name] for name in ("teacher_bc.pt", "warmstart.pt", "last.pt"))
    if bc["episode"] != 0 or initial["episode"] != 0 or not same_weights(bc["model_state"], initial["model_state"]):
        raise ValueError("Teacher BC and validated warmstart must be identical tensors at episode 0")
    if json_value(initial["validation"]) != warm["validation"]:
        raise ValueError("BC checkpoint validation differs from warmstart.json")
    if last["episode"] != episode or json_value(last["validation"]) != logs[-1]["validation"]:
        raise ValueError("PPO last checkpoint does not match final episode/validation log")
    cases = {info["id"]: (cfg, info) for cfg, info in distribution.cases("validation")}
    if metadata["validation_seeds"] != [info["seed"] for cfg, info in cases.values()]:
        raise ValueError("Training validation seeds differ from supplied distribution")
    checkpoint_hashes = {bc_name: digest(snapshot["teacher_bc.pt"]), ppo_name: digest(snapshot["last.pt"])}
    manifest = {**runtime,
                "args": {"split": "validation", "methods": [], "config": str(Path(config_path).resolve()),
                         "distribution": str(Path(distribution_path).resolve()), "training_dir": str(root),
                         "checkpoint": [f"{bc_name}={root / 'teacher_bc.pt'}", f"{ppo_name}={root / 'last.pt'}"]},
                "delivery_horizons_seconds": HORIZONS, "checkpoint_sha256": checkpoint_hashes,
                "distribution_sha256": distribution.sha256, "config_sha256": config_digest(base),
                "runner_sha256": digest(Path(__file__).read_bytes()),
                "cases": [{"case": info, "config": json_value(asdict(cfg))} for cfg, info in cases.values()],
                "scope": "Existing training validation trajectories; paired controlled-proxy simulation, not a new evaluation run",
                "protocol": "docs/thesis/fixed_overlap_protocol.md",
                "origin": {"kind": "training_validation_export", "new_simulation": False,
                           "bc": {"checkpoint": "teacher_bc.pt", "episode": 0,
                                  "validation": "warmstart.json:validation", "identical_to_warmstart_tensors": True},
                           "ppo": {"checkpoint": "last.pt", "episode": episode,
                                   "validation": f"train_log.jsonl:episode={episode}:validation",
                                   "identical_to_last_checkpoint_validation": True},
                           "timing": "No per-case wall-clock or decision-only timing was measured; exported as null"},
                "source_files": {name: {"path": str(root / name), "sha256": digest(data)} for name, data in snapshot.items()},
                "input_files": {name: {"path": str(path), "sha256": digest(data)}
                                for name, (path, data) in inputs.items()}}
    if continuation is not None:
        manifest["origin"]["training_continuation"] = continuation
    records = convert_validation(warm["validation"], cases, bc_name, checkpoint_hashes[bc_name], manifest)
    records += convert_validation(logs[-1]["validation"], cases, ppo_name, checkpoint_hashes[ppo_name], manifest)
    first = {row["case"]["id"]: context(row) for row in records if row["method"] == bc_name}
    if any(context(row) != first[row["case"]["id"]] for row in records if row["method"] == ppo_name):
        raise ValueError("BC/PPO validation task traces or experiment contexts differ")
    if (runtime_metadata() != runtime or any((root / name).read_bytes() != data for name, data in snapshot.items())
            or any(path.read_bytes() != data for path, data in inputs.values())):
        raise ValueError("Runtime or training artifacts changed during export")
    aggregate_rows = [aggregate([row for row in records if row["method"] == method], method)
                      for method in (bc_name, ppo_name)]
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=False)
    def write(name, value):
        (out / name).write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    write("run.json", manifest)
    filenames = []
    for row in records:
        filename = f"{row['case']['id']}_{row['method']}.json"
        write(filename, row)
        filenames.append(filename)
    write("summary.json", {**manifest, "elapsed_s": None, "aggregate": aggregate_rows, "records": filenames})
    continuation_lines = ([f"Verified exact continuation after episode {continuation['starting_episode']}: "
                           "only the resume flag changed; original model/log and validation audit hashes are retained.", ""]
                          if continuation is not None else [])
    lines = ["# Exported Training Validation", "", manifest["scope"], "", *continuation_lines,
             f"BC: episode 0, teacher_bc.pt tensors equal validated warmstart.pt. PPO: last.pt episode {episode}.",
             "Timing values are null because training validation did not measure them. "
             "Source artifact hashes and exact per-case action counts are retained.", "", *result_table(aggregate_rows), ""]
    (out / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    return {"output": str(out.resolve()), "records": len(records), "methods": [bc_name, ppo_name],
            "ppo_episode": episode, "new_simulation": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--training-dir", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--distribution", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--bc-name", default="bc")
    parser.add_argument("--ppo-name", default="ppo")
    args = parser.parse_args(argv)
    print(json.dumps(export_validation(args.training_dir, args.config, args.distribution, args.out_dir,
                                       bc_name=args.bc_name, ppo_name=args.ppo_name)))


if __name__ == "__main__":
    main()
