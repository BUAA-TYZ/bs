from pathlib import Path
from tempfile import TemporaryDirectory
import gzip
import pickle
import unittest

import torch

from analysis import method_cost_20261028 as cost
from analysis.local_information_baselines import LocalInformationPlane
from analysis.run_dynamic_learning import action_digest_update
from test_dynamic_granularity_v2 import fixture
import hashlib


class MethodCost(unittest.TestCase):
    def execute(self, family, record):
        torch.set_num_threads(1)
        torch.manual_seed(1701)
        env = fixture()
        self.addCleanup(env.close)
        plane = LocalInformationPlane([0])
        plane.history_hops = 2
        env.control_plane = plane
        actor = cost.fixed.HierarchicalActor().eval() if family == 'fixed' else cost.dynamic.DynamicActor().eval()
        policy = (cost.fixed.CommittedPolicy(actor, 1701000) if family == 'fixed' else
                  cost.dynamic.DynamicPolicy(actor, 1701000, env, plane))
        digest = hashlib.sha256()
        with TemporaryDirectory() as tmp:
            recorder = cost.Recorder(env, actor, family, tmp)
            if record:
                recorder.__enter__()
            try:
                for step in range(100):
                    state = env.export_state()
                    plane.enqueue(state)
                    local = plane.view(state, 0, 2)
                    actions = recorder.select(policy, local) if record else policy.select_actions(local)
                    action_digest_update(digest, step, actions)
                    env.step(actions)
                if record:
                    self.assertTrue(recorder.records)
                    recorder.flush(0)
                    for path in Path(tmp).glob('*.pkl.gz'):
                        with gzip.open(path, 'rb') as stream:
                            saved = pickle.load(stream)
                        self.assertEqual(cost.rebuild(saved['snapshot'], family), saved['observation'])
                        with torch.no_grad():
                            got = cost.probabilities(recorder.original_logits(saved['tensors']))
                        for a, b in zip(got, saved['probabilities'], strict=True):
                            self.assertEqual(a.tolist(), b)
                    for row in recorder.calls:
                        self.assertGreaterEqual(row['commitment_s'], 0)
            finally:
                if record:
                    recorder.__exit__()
        return digest.hexdigest(), env.summary()['task_outcomes']

    def test_fixed_recording_preserves_actions_and_rebuilds(self):
        self.assertEqual(self.execute('fixed', False), self.execute('fixed', True))

    def test_dynamic_recording_preserves_actions_and_rebuilds(self):
        self.assertEqual(self.execute('dynamic', False), self.execute('dynamic', True))


if __name__ == '__main__':
    unittest.main()
