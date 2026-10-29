import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import torch
from analysis import resume_main_mappo as resume


class ResumeTests(unittest.TestCase):
    def test_checkpoint_threshold_preserves_three_episode_updates(self):
        last_saved = 296
        saves = []
        for completed in range(297, 353):
            if resume.checkpoint_due(completed, last_saved):
                saves.append(completed)
                last_saved = completed
        self.assertEqual(saves, [321, 346])
        # 第321个采样到达时，模型仍处于320；余下采样后仍以323作更新种子。
        self.assertEqual(list(range(320 + 1, min(352, 320 + 3) + 1)), [321, 322, 323])
        self.assertFalse(resume.checkpoint_due(321, 321))

    def test_recovery_limit_is_total_three(self):
        state = dict(recoveries=0, events=[])
        for i in range(3):
            self.assertTrue(resume.consume_recovery(state, 'crash', {'log': 'test'}))
            self.assertEqual(state['recoveries'], i + 1)
        self.assertFalse(resume.consume_recovery(state, 'crash', {}))
        self.assertEqual(state['recoveries'], 3)
        self.assertEqual(state['events'][-1]['action'], 'STOP_LIMIT')

    def test_atomic_checkpoint_roundtrip_and_signature(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'cp.pt'
            actor = torch.nn.Linear(2, 1)
            critic = torch.nn.Linear(2, 1)
            opt = torch.optim.Adam(list(actor.parameters()) + list(critic.parameters()), lr=.0003)
            (actor(torch.ones(2)).sum() + critic(torch.ones(2)).sum()).backward()
            opt.step()
            with patch.object(resume.base, 'sha', return_value='teacher-hash'):
                d = resume.checkpoint_payload(actor, critic, opt, '1701', 320, 'parent-hash')
            d.update(collected_through_save=321, pending_batch=[321, 322, 323])
            resume.atomic_torch(p, d)
            loaded = torch.load(p, map_location='cpu', weights_only=True)
            self.assertEqual(loaded['episodes'], 320)
            self.assertEqual(loaded['pending_batch'], [321, 322, 323])
            self.assertEqual(loaded['optimizer']['param_groups'][0]['lr'], .0003)
            self.assertTrue(loaded['optimizer']['state'])
            self.assertEqual(resume.signature(d), resume.signature(loaded))
            loaded['state_dict']['weight'][0, 0] += 1
            self.assertNotEqual(resume.signature(d), resume.signature(loaded))

    def test_reuse_checks_policy_and_output_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / '0297.json.gz'
            job = dict(kind='train', method='mappo', seed=1701, episode=297,
                       protocol='training-only', policy_signature='correct', out=str(out))
            with gzip.open(out, 'wt') as f:
                json.dump(dict(status='PASS', job=job), f)
            status = out.with_suffix('.progress.json')
            resume.atomic_json(status, dict(status='PASS', sha256=resume.base.sha(out)))
            self.assertTrue(resume.reusable(job))
            with self.assertRaises(resume.AuditFailure):
                resume.reusable(dict(job, policy_signature='wrong'))
            out.write_bytes(b'truncated')
            with self.assertRaises(resume.AuditFailure):
                resume.reusable(job)
            resume.atomic_json(status, dict(status='RUNNING'))
            self.assertFalse(resume.reusable(job))

    def test_retention_protects_validated_and_last_three(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rr = root / 'resume'
            paths = []
            for ep in [0, 176, 296, 320, 344, 352]:
                p = root / 'checkpoints' / '1701' / f'ep{ep}.pt'
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(str(ep))
                paths.append(p)
            recovery = dict(states={'1701': dict(checkpoint=str(paths[-1]), evaluations=[
                dict(checkpoint=str(paths[0])), dict(checkpoint=str(paths[1]))])})
            resume.atomic_json(rr / 'recovery.json', recovery)
            with patch.object(resume, 'ROOT', root), patch.object(resume, 'RESUME', rr):
                resume.prune()
            self.assertFalse(paths[2].exists())
            self.assertTrue(all(p.exists() for p in [paths[0], paths[1], *paths[-3:]]))
            self.assertIn(str(paths[2]), json.loads((rr / 'checkpoint_ledger.json').read_text()))

    def test_deadline_and_validation_points(self):
        self.assertEqual(resume.DEADLINE.isoformat(), '2026-10-29T16:00:00+00:00')
        self.assertEqual(resume.base.LIMITS, [176, 352, 528, 703])


if __name__ == '__main__':
    unittest.main()
