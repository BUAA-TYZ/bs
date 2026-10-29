from copy import deepcopy
from dataclasses import asdict, replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import torch

from sim.config import load_config
from sim.marl.actor import SatActorCritic
from sim.marl.checkpoint import validate_checkpoint_protocol
from sim.marl.decision import build_batch
from sim.marl.features import build_features, feature_dim, observation_version_for, validate_feature_workload
from sim.marl.imitation import collect_demonstrations
from sim.marl.observation import MAX_NEIGHBORS
from sim.marl.scenarios import ScenarioDistribution
from sim.scheduler.marl_policy import MARLPolicy
from sim.workload import ImageTiling
from train_marl import parser, train
from test_simulation import FakeTopology, FakeVisibility, add_task, make_env


def image_profile(**changes):
    values = dict(width_px=1848, height_px=1848, bands=3, bits_per_sample=8,
                  window_px=1024, stride_px=824, windows_per_tile_axis=2,
                  compute_cost_per_window=1.0, result_size_mb_per_window=0.04)
    return {**values, **changes}


def distribution_spec():
    spec = json.loads(Path("examples/scenario_roles.json").read_text())
    spec["version"] = 3
    spec["training"].pop("compute_cost_per_tile")
    large = image_profile()
    large.update(width_px=2672, compute_cost_per_window=2.5, result_size_mb_per_window=0.12)
    spec["workload"] = {"profiles": [{"id": "small", "image_tiling": image_profile()},
                                      {"id": "large", "image_tiling": large}],
                         "training_profiles": ["small", "large"]}
    for split in ("validation", "test"):
        for index, case in enumerate(spec[split]):
            case.pop("compute_cost_per_tile")
            case["workload_profile"] = ["small", "large"][index % 2]
    for profile in spec["roles"]["profiles"]:
        profile.update(mean_compute_rate=1.8, imager_compute_rate=0.6)
    return spec


class OverlapMARLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def distribution(self, spec=None, base=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "distribution.json"
        path.write_text(json.dumps(distribution_spec() if spec is None else spec))
        return ScenarioDistribution(path, base or replace(load_config("examples/config_roles.yaml"),
                                                         image_tiling=image_profile()))

    def test_workloads_are_balanced_and_geometry_derives_all_totals(self):
        dist = self.distribution()
        before = asdict(dist.base)
        cases = [dist.training_case(7, episode) for episode in range(1, 37)]
        for index in range(0, len(cases), 2):
            self.assertEqual({info["workload_profile"] for _, info in cases[index:index + 2]}, {"small", "large"})
        for block in (cases[:18], cases[18:]):
            self.assertEqual(len({(cfg.num_sats, info["role_profile"], info["workload_profile"])
                                  for cfg, info in block}), 18)
        for cfg, info in cases:
            manifest = ImageTiling.from_dict(cfg.image_tiling).manifest()
            expected_windows = 4 if info["workload_profile"] == "small" else 6
            self.assertEqual(manifest["num_windows"], expected_windows)
            self.assertAlmostEqual(cfg.compute_cost_per_tile * cfg.num_tiles,
                                   expected_windows * cfg.image_tiling["compute_cost_per_window"])
            self.assertAlmostEqual(cfg.result_size_mb * cfg.num_tiles, manifest["unmerged_result_mb"])
            self.assertAlmostEqual(cfg.image_size_mb, manifest["image_size_mb"])
            self.assertAlmostEqual(sum(cfg.satellite_compute_rates), 1.8 * cfg.num_sats)
        self.assertEqual(asdict(dist.base), before)
        self.assertEqual(cases[0], dist.training_case(7, 1))

    def test_fixed_constellation_version_three_needs_no_role_or_walker(self):
        spec = distribution_spec()
        spec.pop("walker")
        spec.pop("roles")
        for split in ("validation", "test"):
            for case in spec[split]:
                case.pop("walker")
                case.pop("role_profile")
        dist = self.distribution(spec)
        self.assertEqual({dist.training_case(17, episode)[1]["workload_profile"] for episode in (1, 2)},
                         {"small", "large"})
        self.assertEqual(dist.training_case(17, 1)[0].satellite_compute_rates, dist.base.satellite_compute_rates)

    def test_equal_hardware_rates_are_allowed_only_by_workload_protocol(self):
        spec = distribution_spec()
        for profile in spec["roles"]["profiles"]:
            profile["imager_compute_rate"] = profile["mean_compute_rate"]
        dist = self.distribution(spec)
        cfg, _ = dist.training_case(7, 1)
        np.testing.assert_allclose(cfg.satellite_compute_rates, 1.8)
        with self.assertRaisesRegex(ValueError, "below"):
            ScenarioDistribution.validate_profile(spec["roles"]["profiles"][0])

    def test_invalid_workloads_and_data_leakage_are_rejected(self):
        variants = []
        for key, value in (("stride_px", 800), ("window_px", 512), ("bands", 4),
                           ("windows_per_tile_axis", 1), ("compute_cost_per_window", 0)):
            bad = distribution_spec()
            bad["workload"]["profiles"][0]["image_tiling"][key] = value
            variants.append(bad)
        bad = distribution_spec()
        bad["training"]["compute_cost_per_tile"] = [1, 100]
        variants.append(bad)
        bad = distribution_spec()
        bad["workload"]["training_profiles"] = ["missing"]
        variants.append(bad)
        bad = distribution_spec()
        bad["validation"][0]["workload_profile"] = "missing"
        variants.append(bad)
        bad = distribution_spec()
        bad["validation"][0]["orbit_offset_seconds"] = 1
        variants.append(bad)
        bad = distribution_spec()
        bad["test"][0]["seed"] = bad["validation"][0]["seed"]
        variants.append(bad)
        for spec in variants:
            with self.subTest(spec=spec), self.assertRaises(ValueError):
                self.distribution(spec)
        dist = self.distribution()
        with self.assertRaisesRegex(ValueError, "held-out seed"):
            dist.training_case(dist.spec["validation"][0]["seed"] - 1, 1)
        with self.assertRaisesRegex(ValueError, "version 3"):
            self.distribution(base=load_config("examples/config_roles.yaml"))

    def test_packing_changes_packets_but_not_window_work(self):
        image = ImageTiling.from_dict(image_profile())
        first, second = image.manifest(), replace(image, windows_per_tile_axis=1).manifest()
        self.assertNotEqual(first["num_tiles"], second["num_tiles"])
        for key in ("num_windows", "total_compute_cost", "packet_input_mb", "unmerged_result_mb"):
            self.assertEqual(first[key], second[key])
        with self.assertRaisesRegex(ValueError, "1024"):
            validate_feature_workload(image_profile(window_px=512), "overlap_contact")

    def pixel_state(self):
        env = make_env(image_tiling=image_profile())
        self.addCleanup(env.close)
        add_task(env, count=2, size=32)
        return env.export_state()

    def test_packet_features_distinguish_bytes_cost_and_results_without_clipping(self):
        state = self.pixel_state()
        before = build_features(state, 0, "overlap_contact")
        tile_id = next(iter(state.tiles))
        for field, first, second in (("data_size_mb", 32, 64), ("compute_cost", 40, 80),
                                     ("result_size_mb", 2, 4)):
            with self.subTest(field=field):
                changed = deepcopy(state)
                changed.tiles[tile_id][field] = first
                a = build_features(changed, 0, "overlap_contact")
                changed.tiles[tile_id][field] = second
                b = build_features(changed, 0, "overlap_contact")
                self.assertFalse(np.array_equal(a, b))
        self.assertTrue(np.isfinite(before).all())
        self.assertEqual(len(before), feature_dim(1, "role_contact"))
        self.assertEqual(feature_dim(2, "overlap_contact"), 254)
        with self.assertRaisesRegex(ValueError, "overlap_contact"):
            build_features(state, 0, "role_contact")
        state.config["image_tiling"] = False
        with self.assertRaisesRegex(ValueError, "legacy workloads"):
            build_features(state, 0, "overlap_contact")

    def test_result_feature_survives_task_ablation_and_window_ablation_still_works(self):
        state = self.pixel_state()
        a = build_features(state, 0, "overlap_contact", False, False)
        changed = deepcopy(state)
        for tile in changed.tiles.values():
            tile["task_pending_count"] = 100
            tile["task_progress"] = dict(total=100, done=20, failed=30, running=10, awaiting_delivery=5)
            tile["sibling_locations"] = {1: 90}
        for sat in changed.satellites.values():
            sat["next_gs_windows"] = {"gs": (10, 30)}
        np.testing.assert_array_equal(a, build_features(changed, 0, "overlap_contact", False, False))
        changed.tiles[next(iter(changed.tiles))]["result_size_mb"] = 40
        self.assertFalse(np.array_equal(a, build_features(changed, 0, "overlap_contact", False, False)))

    def test_actor_size_and_checkpoint_semantics(self):
        options = dict(num_gs=1, hidden=16, encoder="task_attention")
        model = SatActorCritic(**options, feature_set="overlap_contact")
        legacy = SatActorCritic(**options, feature_set="role_contact")
        self.assertEqual(sum(p.numel() for p in model.parameters()), sum(p.numel() for p in legacy.parameters()))
        self.assertEqual(observation_version_for("role_contact"), 2)
        self.assertEqual(observation_version_for("overlap_contact"), 3)
        with self.assertRaises(RuntimeError):
            model.load_state_dict(legacy.state_dict())
        with self.assertRaises(RuntimeError):
            legacy.load_state_dict(model.state_dict())
        checkpoint = dict(observation_version=3, model_config={**options, "feature_set": "overlap_contact"},
                          model_state=model.state_dict(), ground_station_ids=["gs"])
        state = self.pixel_state()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "model.pt"
            torch.save(checkpoint, path)
            policy = MARLPolicy(str(path))
            self.assertEqual(len(policy.select_actions(state)), 2)
            bad = {**checkpoint, "observation_version": 2}
            torch.save(bad, path)
            with self.assertRaisesRegex(ValueError, "version"):
                MARLPolicy(str(path))

    def test_cgs_teacher_labels_obey_masks(self):
        env = make_env(image_tiling=image_profile(), sim_steps=5, task_arrival_end_step=2, task_arrival_rate=2)
        self.addCleanup(env.close)
        model = SatActorCritic(num_gs=1, hidden=8, feature_set="overlap_contact", encoder="task_attention")
        with patch("sim.env.TopologyModel", FakeTopology), patch("sim.env.VisibilityCache", FakeVisibility):
            dataset, record = collect_demonstrations(model, env.cfg, "cpu", "cgs_compute_adapted")
        self.assertGreater(record["samples"], 0)
        _, tiles, masks, actions = dataset
        self.assertTrue(masks.gather(-1, actions[..., None]).squeeze(-1)[tiles].all())
        self.assertLessEqual(int(actions.max()), 2 + MAX_NEIGHBORS)

    def test_training_writes_v3_and_initialization_resume_validate_it(self):
        env = make_env(image_tiling=image_profile(), sim_steps=5, task_arrival_end_step=2, task_arrival_rate=2)
        self.addCleanup(env.close)
        with tempfile.TemporaryDirectory() as directory:
            config, out = Path(directory) / "config.json", Path(directory) / "run"
            config.write_text(json.dumps(asdict(env.cfg)))
            args = parser().parse_args(["--config", str(config), "--out-dir", str(out),
                "--episodes", "1", "--seed", "7", "--hidden", "8", "--ppo-epochs", "1",
                "--validation-seeds", "9001", "--features", "overlap_contact", "--encoder", "task_attention",
                "--warmstart-teacher", "cgs_compute_adapted", "--warmstart-episodes", "1", "--warmstart-epochs", "1",
                "--warmstart-mode", "pooled"])
            with patch("sim.env.TopologyModel", FakeTopology), patch("sim.env.VisibilityCache", FakeVisibility), \
                    patch("builtins.print"):
                train(args)
            saved = torch.load(out / "last.pt", weights_only=True)
            self.assertEqual(saved["observation_version"], 3)
            validate_checkpoint_protocol(saved)
            self.assertTrue((out / "teacher_bc.pt").exists())
            bad = {**saved, "observation_version": 2}
            torch.save(bad, out / "last.pt")
            resume = deepcopy(args)
            resume.resume, resume.episodes = True, 2
            with self.assertRaisesRegex(ValueError, "version"):
                train(resume)
            initialized = deepcopy(args)
            initialized.out_dir = str(Path(directory) / "initialized")
            initialized.warmstart_episodes = 0
            initialized.initialize_checkpoint = str(out / "last.pt")
            with self.assertRaisesRegex(ValueError, "version"):
                train(initialized)


if __name__ == "__main__":
    unittest.main()
