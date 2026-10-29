import copy
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest

import torch

from analysis.export_overlap_validation import (check_geometry_and_trace, digest, export_validation,
                                               json_digest, json_value)
from analysis.report_overlap_study import build_panel, load_directory
from sim.config import load_config
from sim.evaluation import task_age_score
from sim.env import SimulationEnv
from sim.marl.scenarios import ScenarioDistribution, config_digest
from sim.reproducibility import runtime_metadata
from sim.workload import ImageTiling


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "examples/overlap_proxy/config.json"
DISTRIBUTION = ROOT / "examples/overlap_proxy/distribution.json"
HORIZONS = [1800, 3600, 7200, 14400]


def write_json(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


def fixture(root):
    root.mkdir()
    base = load_config(str(CONFIG))
    distribution = ScenarioDistribution(DISTRIBUTION, base)
    cases = distribution.cases("validation")
    runtime = runtime_metadata()
    metadata = {**runtime, "training_args": {"episodes": 1, "warmstart_mode": "pooled", "resume": False},
                "config": json_value(asdict(base)), "model_config": {"feature_set": "overlap_contact"},
                "observation_version": 3, "scenario_distribution": {"sha256": distribution.sha256, "spec": distribution.spec},
                "validation_seeds": [info["seed"] for cfg, info in cases], "delivery_horizons_seconds": HORIZONS}
    rows = []
    for cfg, info in cases:
        workload = ImageTiling.from_dict(cfg.image_tiling)
        sid = cfg.task_source_sat_ids[0]
        outcomes = [{"task_id": "task_0", "source_sat_id": sid, "release_seconds": 0,
                     "deadline_seconds": None, "status": "completed", "latency_seconds": 60,
                     "raw_relay_hops": 0, "result_relay_hops": 1}]
        trace = [("task_0", sid, 0, [float(tile.data_size_mb) for tile in workload.tiles()])]
        metrics = {"experiment": {**runtime, "config": json_value(asdict(cfg)),
                                  "elapsed_sim_seconds": cfg.sim_steps * cfg.dt,
                                  "tle_sha256": json_digest(cfg.topology["tle_lines"]),
                                  "workload": json_value(workload.manifest()),
                                  "workload_sha256": json_digest(workload.manifest(), sort_keys=True),
                                  "task_trace_sha256": json_digest(trace)},
                   "overall": {"completed_tasks": 1, "total_tasks": 1}, "task_outcomes": outcomes,
                   "resource": {"compute_busy_time": {str(sid): 1}, "ground_compute_busy_time": {}},
                   "network": {"sent_mb": {"isl:0->1": .001}}}
        rows.append({**info, "config_sha256": config_digest(cfg), "metrics": metrics,
                     "task_age": task_age_score(outcomes, HORIZONS, cfg.sim_steps * cfg.dt),
                     "action_counts": {"1": cfg.num_tiles}})
    validation = {"cases": rows, "seeds": metadata["validation_seeds"], "delivery_score": 1,
                  "restricted_mean_latency_seconds": 60, "completed_tasks": len(rows)}
    write_json(root / "run.json", metadata)
    write_json(root / "warmstart.json", {"episodes": [{"samples": 4}], "validation": validation})
    (root / "train_log.jsonl").write_text(json.dumps({"episode": 1, "validation": validation}) + "\n")
    weights = {"actor.weight": torch.tensor([[1., 2.]])}
    torch.save({**metadata, "episode": 0, "validation": None, "model_state": weights}, root / "teacher_bc.pt")
    torch.save({**metadata, "episode": 0, "validation": validation, "model_state": weights}, root / "warmstart.pt")
    torch.save({**metadata, "episode": 1, "validation": validation, "model_state": {"actor.weight": weights["actor.weight"] + .01}}, root / "last.pt")
    return root


def resumed_fixture(root):
    fixture(root)
    metadata = json.loads((root / "run.json").read_text())
    metadata["training_args"].update(episodes=2, device="cpu", torch_threads=1, lr=.001)
    validation = json.loads((root / "warmstart.json").read_text())["validation"]
    for name in ("teacher_bc.pt", "warmstart.pt", "last.pt"):
        checkpoint = torch.load(root / name, weights_only=True)
        checkpoint.update(metadata)
        torch.save(checkpoint, root / name)
    archive = root / "resume_parallel_validation_source"
    archive.mkdir()
    write_json(archive / "run.json", metadata)
    previous = torch.load(root / "last.pt", weights_only=True)
    previous.update(validation=None, optimizer_state={}, rng_state={}, best_score=[0, 0])
    torch.save(previous, archive / "last.pt")
    prefix = json.dumps({"episode": 1, "validation": None}) + "\n"
    (archive / "train_log.jsonl").write_text(prefix)
    current = copy.deepcopy(metadata)
    current["training_args"]["resume"] = True
    write_json(root / "run.json", current)
    last = {**previous, **current, "episode": 2, "validation": validation}
    torch.save(last, root / "last.pt")
    (root / "train_log.jsonl").write_text(prefix + json.dumps({"episode": 2, "validation": validation}) + "\n")
    wrapper = ROOT / "analysis/resume_parallel_validation.py"
    (archive / "wrapper_source.py").write_bytes(wrapper.read_bytes())
    names = ("run.json", "last.pt", "train_log.jsonl")
    sidecar = {**runtime_metadata(), "schema_version": 1, "kind": "exact_cpu_resume_parallel_validation",
               "status": "completed", "training_dir": str(root), "starting_episode": 1,
               "next_episode": 2, "episode_budget": 2, "workers": 2, "torch_threads": 1,
               "multiprocessing_start_method": "spawn", "original_training_args": metadata["training_args"],
               "resumed_training_args": current["training_args"],
               "wrapper": {"path": str(wrapper), "snapshot_path": str(archive / "wrapper_source.py"),
                           "sha256": digest(wrapper.read_bytes())},
               "input_files": {key: {"path": str(path), "sha256": digest(path.read_bytes())}
                               for key, path in (("config", CONFIG), ("distribution", DISTRIBUTION))},
               "source_files": {name: {"path": str(archive / name), "sha256": digest((archive / name).read_bytes()),
                                       "bytes": (archive / name).stat().st_size} for name in names},
               "final_files": {name: {"path": str(root / name), "sha256": digest((root / name).read_bytes())} for name in names},
               "validation_calls": [{"case_ids": [row["id"] for row in validation["cases"]],
                                     "worker_pids": [101] * len(validation["cases"]),
                                     "validation_sha256": json_digest(validation, sort_keys=True)}]}
    write_json(root / "resume_parallel_validation.json", sidecar)
    return root


class OverlapValidationExportTests(unittest.TestCase):
    def test_completed_exact_continuation_retains_original_and_final_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            training = resumed_fixture(root / "training")
            result = export_validation(training, CONFIG, DISTRIBUTION, root / "export")
            self.assertEqual(result["ppo_episode"], 2)
            manifest = load_directory(root / "export")["manifest"]
            lineage = manifest["origin"]["training_continuation"]
            self.assertEqual(lineage["starting_episode"], 1)
            self.assertTrue(lineage["original_log_prefix_preserved"])
            self.assertEqual(lineage["validation_calls_verified"], 1)
            self.assertIn("resume_parallel_validation_source/last.pt", manifest["source_files"])
            self.assertIn("resume_parallel_validation_source/wrapper_source.py", manifest["source_files"])

    def test_continuation_rejects_missing_lineage_changes_and_inconsistent_audit(self):
        for treatment in ("missing_sidecar", "argument_change", "prefix", "audit", "wrapper"):
            with self.subTest(treatment=treatment), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                training = resumed_fixture(root / "training")
                sidecar_path = training / "resume_parallel_validation.json"
                sidecar = json.loads(sidecar_path.read_text())
                if treatment == "missing_sidecar":
                    sidecar_path.unlink()
                elif treatment == "argument_change":
                    metadata = json.loads((training / "run.json").read_text())
                    metadata["training_args"]["lr"] = .002
                    write_json(training / "run.json", metadata)
                    saved = torch.load(training / "last.pt", weights_only=True)
                    saved["training_args"] = metadata["training_args"]
                    torch.save(saved, training / "last.pt")
                    sidecar["resumed_training_args"] = metadata["training_args"]
                elif treatment == "prefix":
                    lines = (training / "train_log.jsonl").read_text().splitlines()
                    lines[0] = json.dumps({"episode": 1, "validation": None, "changed": True})
                    (training / "train_log.jsonl").write_text("\n".join(lines) + "\n")
                elif treatment == "audit":
                    sidecar["validation_calls"][0]["case_ids"].reverse()
                else:
                    wrapper = training / "resume_parallel_validation_source/wrapper_source.py"
                    wrapper.write_bytes(wrapper.read_bytes() + b"\n")
                if treatment != "missing_sidecar":
                    for name, info in sidecar["final_files"].items():
                        info["sha256"] = digest((training / name).read_bytes())
                    write_json(sidecar_path, sidecar)
                with self.assertRaises(ValueError):
                    export_validation(training, CONFIG, DISTRIBUTION, root / "export")
                self.assertFalse((root / "export").exists())

    def test_live_summary_and_json_roundtrip_have_identical_workload_identity(self):
        cfg, _ = ScenarioDistribution(DISTRIBUTION, load_config(str(CONFIG))).training_case(1701, 1)
        env = SimulationEnv(cfg)
        try:
            metrics = env.summary()
            check_geometry_and_trace(metrics, cfg)
            check_geometry_and_trace(json_value(metrics), cfg)
            metrics["experiment"]["workload_sha256"] = "tampered"
            with self.assertRaises(ValueError):
                check_geometry_and_trace(metrics, cfg)
        finally:
            env.close()

    def test_export_retains_evidence_and_pairs_with_rule_panel(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            training = fixture(root / "training")
            out = root / "export"
            result = export_validation(training, CONFIG, DISTRIBUTION, out)
            self.assertEqual(result["records"], 24)
            exported = load_directory(out)
            self.assertEqual(exported["methods"], ["bc", "ppo"])
            self.assertFalse(exported["manifest"]["origin"]["new_simulation"])
            self.assertEqual(len(exported["manifest"]["source_files"]), 6)
            first = next(iter(exported["records"].values()))
            self.assertNotIn("metrics", first["case"])
            self.assertNotIn("action_counts", first["case"])
            self.assertTrue(first["action_counts"])
            self.assertIsNone(first["elapsed_s"])
            self.assertIsNone(first["decision_timing"])
            baseline = root / "baseline_fixture"
            baseline.mkdir()
            manifest = json.loads((out / "run.json").read_text())
            manifest["args"]["methods"] = ["local"]
            manifest["checkpoint_sha256"] = {}
            names = []
            for (case_id, method), record in exported["records"].items():
                if method != "bc":
                    continue
                record = {**record, "method": "local", "checkpoint_sha256": None}
                name = f"{case_id}_local.json"
                write_json(baseline / name, record)
                names.append(name)
            write_json(baseline / "run.json", manifest)
            write_json(baseline / "summary.json", {**manifest, "records": names})
            panel = build_panel("validation", [baseline, out])
            self.assertEqual(panel["record_count"], 36)

    def test_rejects_mismatched_bc_weights_and_incomplete_ppo_identity(self):
        for treatment in ("bc_weights", "ppo_episode", "ppo_validation"):
            with self.subTest(treatment=treatment), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                training = fixture(root / "training")
                filename = "teacher_bc.pt" if treatment == "bc_weights" else "last.pt"
                saved = torch.load(training / filename, weights_only=True)
                if treatment == "bc_weights":
                    saved["model_state"]["actor.weight"] += 1
                elif treatment == "ppo_episode":
                    saved["episode"] = 0
                else:
                    saved["validation"]["cases"][0]["action_counts"] = {"0": 99}
                torch.save(saved, training / filename)
                with self.assertRaises(ValueError):
                    export_validation(training, CONFIG, DISTRIBUTION, root / "export")
                self.assertFalse((root / "export").exists())

    def test_rejects_missing_case_and_tampered_hash_even_when_saved_validation_agrees(self):
        for treatment in ("case_set", "task_trace", "runtime"):
            with self.subTest(treatment=treatment), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                training = fixture(root / "training")
                warm = json.loads((training / "warmstart.json").read_text())
                if treatment == "case_set":
                    warm["validation"]["cases"].pop()
                else:
                    key = "task_trace_sha256" if treatment == "task_trace" else "source_sha256"
                    warm["validation"]["cases"][0]["metrics"]["experiment"][key] = "tampered"
                write_json(training / "warmstart.json", warm)
                saved = torch.load(training / "warmstart.pt", weights_only=True)
                saved["validation"] = warm["validation"]
                torch.save(saved, training / "warmstart.pt")
                with self.assertRaises(ValueError):
                    export_validation(training, CONFIG, DISTRIBUTION, root / "export")
                self.assertFalse((root / "export").exists())


if __name__ == "__main__":
    unittest.main()
