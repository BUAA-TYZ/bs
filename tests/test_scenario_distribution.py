from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
from sgp4.api import Satrec
from sgp4.io import verify_checksum
import torch

from sim.config import load_config
from sim.constellation import walker_tle
from sim.marl.actor import SatActorCritic
from sim.marl.scenarios import ScenarioDistribution
from train_marl import critic_observations
import run_marl_study


class ScenarioDistributionTests(unittest.TestCase):
    def setUp(self):
        self.cfg = load_config("examples/config_training.yaml")
        self.path = "examples/scenario_distribution.json"

    def test_reproducible_independent_sampling_covers_sizes(self):
        dist = ScenarioDistribution(self.path, self.cfg)
        before = asdict(self.cfg)
        a, info_a = dist.training_case(7, 1)
        np.random.seed(123)
        np.random.random(100)
        b, info_b = dist.training_case(7, 1)
        self.assertEqual(asdict(a), asdict(b))
        self.assertEqual(info_a, info_b)
        self.assertEqual(asdict(self.cfg), before)
        cases = [dist.training_case(7, e) for e in range(1, 7)]
        self.assertEqual({cfg.num_sats for cfg, _ in cases[:3]}, {12, 24, 48})
        self.assertEqual(len({info['config_sha256'] for _, info in cases}), 6)
        self.assertEqual(dist.max_training_sats, 48)
        self.assertAlmostEqual(a.task_arrival_rate, info_a['arrival_rate_per_second'] * a.dt)

    def test_holdout_intervals_and_seeds_are_enforced(self):
        spec = json.loads(Path(self.path).read_text())
        spec['validation'][0]['orbit_offset_seconds'] = 25000
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'bad.json'
            path.write_text(json.dumps(spec))
            with self.assertRaisesRegex(ValueError, 'overlaps training'):
                ScenarioDistribution(path, self.cfg)
        dist = ScenarioDistribution(self.path, self.cfg)
        with self.assertRaisesRegex(ValueError, 'held-out seed'):
            dist.training_case(51000, 1)

    def test_generated_orbits_propagate_and_do_not_duplicate_positions(self):
        lines = walker_tle('2026-03-20T17:30:00Z', 3, 8, 550, 53, 25, 15)
        self.assertEqual(len(lines), 24)
        positions = []
        for line1, line2 in lines:
            verify_checksum(line1, line2)
            sat = Satrec.twoline2rv(line1, line2)
            error, position, _ = sat.sgp4(sat.jdsatepoch, sat.jdsatepochF)
            self.assertEqual(error, 0)
            self.assertTrue(6800 < np.linalg.norm(position) < 7100)
            later_error, later, _ = sat.sgp4(sat.jdsatepoch, sat.jdsatepochF + 1 / 24)
            self.assertEqual(later_error, 0)
            self.assertGreater(np.linalg.norm(np.array(later) - position), 100)
            positions.append(position)
        distances = np.linalg.norm(np.array(positions)[:, None] - np.array(positions)[None], axis=-1)
        self.assertGreater(distances[np.triu_indices(24, 1)].min(), 1)

    def test_variable_agent_critic_and_actor_shapes(self):
        torch.set_num_threads(1)
        model = SatActorCritic(num_gs=2, hidden=16, critic_dim=49 * 89)
        for count in (12, 24, 48):
            obs = torch.randn(count, 89)
            central = critic_observations(obs, 'mappo', 48)
            self.assertEqual(central.shape, (count, 49 * 89))
            logits, value = model(obs, central)
            self.assertEqual(value.shape, (count, 1))
            torch.testing.assert_close(logits, model(obs)[0])
        self.assertEqual(model(torch.randn(60, 89))[0].shape[0], 60)
        with self.assertRaisesRegex(ValueError, 'critic capacity'):
            critic_observations(torch.randn(60, 89), 'mappo', 48)

    def test_batch_runner_uses_scenario_configs_and_propagates_distribution(self):
        seen, commands = [], []

        def fake_run(cfg, policy, checkpoint=''):
            seen.append((cfg.num_sats, cfg.seed, policy))
            return {'overall': {'task_completion_rate': 0.5, 'completed_tasks': 1},
                    'latency': {'task_mean': 10.0},
                    'experiment': {'task_trace_sha256': str(cfg.seed)}}

        def fake_train(command, **kwargs):
            commands.append(command)
            directory = Path(command[command.index('--out-dir') + 1])
            torch.save({'test_placeholder': True}, directory / 'best.pt')

        with tempfile.TemporaryDirectory() as tmp:
            args = ['run_marl_study.py', '--config', 'examples/config_training.yaml',
                    '--scenario-distribution', self.path, '--out-dir', str(Path(tmp) / 'study'),
                    '--variants', 'mappo', '--train-seeds', '7', '--episodes', '1',
                    '--min-validation-reachable-rate', '0']
            with patch('sys.argv', args), patch('run_marl_study.run_config', fake_run), \
                    patch('run_marl_study.subprocess.run', fake_train), patch('builtins.print'):
                run_marl_study.main()
            self.assertEqual(len(seen), 20)
            self.assertEqual({n for n, _, _ in seen}, {12, 24, 36, 48, 60})
            self.assertEqual({s for _, s, _ in seen}, {61001, 61002, 61003, 61004, 61005})
            self.assertIn('--scenario-distribution', commands[0])
            report = json.loads((Path(tmp) / 'study' / 'manifest.json').read_text())
            self.assertEqual(report['status'], 'complete')
            self.assertIsNotNone(report['scenario_distribution_sha256'])
