from concurrent.futures import Future
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest
import torch
from analysis import run_l_formal_20261028 as r
from test_dynamic_granularity_v2 import fixture, FakeTopology, FakeVisibility


class LFormalTests(unittest.TestCase):
    def setUp(self):
        torch.set_num_threads(1)
        saved = {k:getattr(r.engine, k) for k in ('ROOT', 'VALID', 'METHODS', 'LEARNED', 'verify_manifest', 'check_time', 'ThreePlanChoice', 'ControlPlane')}
        self.addCleanup(lambda:r.engine.__dict__.update(saved))

    def test_open_gate_requires_matching_manifest_and_date(self):
        with TemporaryDirectory() as tmp, patch.object(r, 'ROOT', Path(tmp)), patch.object(r, 'OPEN', Path(tmp)/'open.json'):
            r.write_json(r.ROOT/'manifest.json', dict(methods=r.METHODS))
            with self.assertRaises(FileNotFoundError):
                r.require_open(r.DATES[0])
            r.write_json(r.OPEN, dict(scope='L', manifest_sha256=r.sha(r.ROOT/'manifest.json')))
            r.require_open(r.DATES[0])
            with self.assertRaises(AssertionError):
                r.require_open('2026-10-22')
            r.write_json(r.OPEN, dict(scope='M', manifest_sha256=r.sha(r.ROOT/'manifest.json')))
            with self.assertRaises(AssertionError):
                r.require_open(r.DATES[0])

    def test_second_open_rejected(self):
        with TemporaryDirectory() as tmp, patch.object(r, 'OPEN', Path(tmp)/'open.json'), \
             patch.object(r, 'verify_manifest', return_value={'dates':r.DATES}), patch.object(r, 'read') as read:
            read.side_effect = [dict(status='COMPLETE'), dict(status='NOTIFIED'), dict(finetune_diagnostic={'needs':{'fixed':False}}), dict(status='MAC_CAPTURE_COMPLETE_ORIN_REPLAY_PENDING')]
            r.OPEN.write_text('original')
            with self.assertRaises(FileExistsError):
                r.open_once()
            self.assertEqual(r.OPEN.read_text(), 'original')

    def test_failed_batch_drains_without_prefetch(self):
        class Pool:
            def __init__(self): self.calls = []
            def submit(self, fn, job):
                self.calls.append(job); f = Future()
                if len(self.calls) == 2: f.set_exception(RuntimeError('test'))
                else: f.set_result({'status':'PASS'})
                return f
        pool = Pool()
        with TemporaryDirectory() as tmp, patch.object(r, 'ROOT', Path(tmp)):
            result = r.batches(pool, list(range(42)))
        self.assertEqual(len(pool.calls), 9); self.assertEqual(len(result), 9)
        self.assertEqual(sum(x['status'] == 'FAILED' for x in result), 1)

    def test_real_toy_formal_records_all_policy_families(self):
        base = fixture(); config = asdict(base.cfg); base.close()
        config.update(sim_steps=710, decision_tile_limit_per_sat=64)
        config['scheduled_task_arrivals'][0]['release_step'] = 20
        real_env = r.engine.GranularityEnv
        def environment(cfg):
            with patch('sim.env.TopologyModel', FakeTopology), patch('sim.env.VisibilityCache', FakeVisibility):
                env = real_env(cfg)
            env.vis_cache.windows_at = lambda sid,t:{'gs':(0,1000)}
            return env
        with TemporaryDirectory() as tmp:
            root = Path(tmp); p = dict(date=r.DATES[0], config=config, scored_task_ids=['task_0'], score_start_s=10, score_end_s=110, sim_duration_s=710)
            r.write_json(root/'p.json', p); r.write_json(root/'run_manifest.json', dict(hashes={}))
            r.write_json(root/'manifest.json', {}); r.write_json(root/'open.json', dict(scope='L', manifest_sha256=r.sha(root/'manifest.json')))
            models = {}
            for family, actor in [('fixed', r.engine.fixed.HierarchicalActor()), ('dynamic', r.engine.dynamic.DynamicActor())]:
                path = root/f'{family}.pt'; torch.save(dict(state_dict=actor.state_dict()), path); models[f'{family}_1701'] = str(path)
            r.write_json(root/'ranges.json', dict(ranges={n:{} for n in models}))
            with patch.object(r, 'ROOT', root), patch.object(r, 'OPEN', root/'open.json'), \
                 patch.object(r, 'verify_manifest', return_value=dict(models=models, ranges=str(root/'ranges.json'))), \
                 patch.object(r.engine, 'verify'), patch.object(r.engine, 'GranularityEnv', side_effect=environment):
                for method in ('ground_16', 'global_v3_16', 'global_v3_dynamic', 'fixed_1701', 'dynamic_1701', 'minload_2_4', 'minload_2_16', 'zero_2_16', 'local_cgs_16', 'local_heft_16'):
                    short = r.simulate((str(root/'p.json'), method))
                    self.assertEqual(short['status'], 'PASS', short)
                    row = r.read(short['path']); r.engine.check_record(row, p)
                    self.assertFalse(row['tests_sealed']); self.assertEqual(row['opening_sha256'], r.sha(r.OPEN))
                    self.assertTrue(row['checks']['followup600'])


if __name__ == '__main__':
    unittest.main()
