from dataclasses import asdict, replace
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import torch

from sim.config import load_config
from sim.env import SimulationEnv
from sim.marl.actor import SatActorCritic
from sim.marl.decision import build_batch
from sim.marl.features import build_features, feature_dim
from sim.marl.scenarios import ScenarioDistribution


class RoleTrainingTests(unittest.TestCase):
    def setUp(self):
        self.base = load_config("examples/config_roles.yaml")
        self.path = "examples/scenario_roles.json"

    def test_joint_coverage_roles_budget_and_load_scale(self):
        dist = ScenarioDistribution(self.path, self.base)
        before = asdict(self.base)
        cases = [dist.training_case(7, e) for e in range(1, 19)]
        for block in (cases[:9], cases[9:]):
            self.assertEqual({(c.num_sats, i["role_profile"]) for c, i in block},
                             {(n, p) for n in (12, 24, 36) for p in ("near", "uniform", "shared")})
        for cfg, info in cases:
            self.assertAlmostEqual(sum(cfg.satellite_compute_rates), .5 * cfg.num_sats)
            sources = cfg.task_source_sat_ids
            self.assertEqual(len(sources), cfg.num_sats * (2 if info["role_profile"] == "shared" else 1) // 3)
            self.assertAlmostEqual(cfg.task_arrival_rate / cfg.dt / len(sources),
                                   info["arrival_rate_per_second"] / 8)
            self.assertEqual(len(cfg.topology["tle_lines"]), cfg.num_sats)
            for sid, rate in enumerate(cfg.satellite_compute_rates):
                self.assertAlmostEqual(rate, .1 if sid in sources else (1.3 if info["role_profile"] == "shared" else .7))
        self.assertEqual(asdict(self.base), before)
        self.assertEqual(cases[0], dist.training_case(7, 1))
        self.assertEqual(dist.max_training_sats, 36)
        self.assertEqual(dist.cases("test")[-1][0].num_sats, 48)

    def test_near_layout_preserves_source_orbits(self):
        dist = ScenarioDistribution(self.path, self.base)
        values = dict(dist.spec["validation"][2])
        near = dist.materialize(values, 123)
        values["role_profile"] = "uniform"
        uniform = dist.materialize(values, 123)
        self.assertEqual(near.task_source_sat_ids, uniform.task_source_sat_ids)
        for sid in near.task_source_sat_ids:
            self.assertEqual(near.topology["tle_lines"][sid], uniform.topology["tle_lines"][sid])

    def test_invalid_profiles_and_fixed_arrays_rejected(self):
        spec = json.loads(Path(self.path).read_text())
        variants = []
        for key, value in (("source_slots", [0, 0]), ("source_slots", [0, 1, 2]),
                           ("members", 4), ("imager_compute_rate", 1),
                           ("phase_offsets_deg", [0, 180, -180])):
            bad = json.loads(json.dumps(spec))
            bad["roles"]["profiles"][0][key] = value
            variants.append(bad)
        bad = json.loads(json.dumps(spec))
        bad["test"][0]["role_profile"] = "missing"
        variants.append(bad)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.json"
            for bad in variants:
                path.write_text(json.dumps(bad))
                with self.assertRaises(ValueError):
                    ScenarioDistribution(path, self.base)
        with self.assertRaisesRegex(ValueError, "fixed role arrays"):
            ScenarioDistribution("examples/scenario_distribution.json",
                                 replace(self.base, task_source_sat_ids=[0]))

    def test_high_load_features_and_multiple_real_decision_sources(self):
        torch.set_num_threads(1)
        dist = ScenarioDistribution(self.path, self.base)
        cfg = replace(dist.cases("validation")[4][0], task_arrival_rate=3,
                      sim_steps=10, task_arrival_end_step=10)
        env = SimulationEnv(cfg)
        try:
            for _ in range(10):
                env.step([])
            state = env.export_state()
            self.assertGreater(len({t.source_sat_id for t in env.tasks.values()}), 1)
            self.assertTrue(all(t.source_sat_id in cfg.task_source_sat_ids for t in env.tasks.values()))
            sid = next(iter(env.tasks.values())).source_sat_id
            for tile in state.tiles.values():
                tile["compute_cost"] = 30
            old = build_features(state, sid, "contact_window")
            first = build_features(state, sid, "role_contact")
            for tile in state.tiles.values():
                tile["compute_cost"] = 90
            np.testing.assert_array_equal(old, build_features(state, sid, "contact_window"))
            self.assertFalse(np.array_equal(first, build_features(state, sid, "role_contact")))
            obs, tiles, masks, _ = build_batch(state, feature_set="role_contact")
            self.assertGreater(int(tiles.any(axis=1).sum()), 1)
            self.assertEqual(obs.shape, (24, feature_dim(2, "role_contact")))
            self.assertTrue(np.isfinite(obs).all())
            model = SatActorCritic(num_gs=2, hidden=16, feature_set="role_contact", encoder="task_attention")
            action, logp, _, _ = model.get_action_and_value(torch.tensor(obs), torch.tensor(tiles),
                                                         action_mask=torch.tensor(masks))
            self.assertTrue(torch.isfinite(logp).all())
            for row in range(len(action)):
                self.assertTrue(masks[row, np.arange(4), action[row].numpy()].all())
        finally:
            env.close()
