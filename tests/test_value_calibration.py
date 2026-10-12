from copy import deepcopy
import unittest

import torch

from sim.marl.actor import SatActorCritic
from sim.marl.value_calibration import balanced_value_dataset, fit_value_critic, prediction_metrics


class ValueCalibrationTests(unittest.TestCase):
    def test_constant_targets_have_finite_errors_without_fake_variance(self):
        result = prediction_metrics(torch.tensor([2., 3.]), torch.ones(2))
        self.assertEqual(result["mse"], 2.5)
        self.assertIsNone(result["explained_variance"])
        self.assertIsNone(result["normalized_mse"])

    def test_equal_budget_actor_isolation_and_private_rng(self):
        torch.set_num_threads(1)
        torch.manual_seed(31)
        base = SatActorCritic(num_gs=1, hidden=8, critic_dim=10)
        cases = [{"data": {kind: {"obs": torch.rand(n, 10), "target": torch.rand(n)}
                           for kind, n in (("active", 3), ("uniform", 8))}} for _ in range(3)]
        rng = torch.get_rng_state().clone()
        datasets = balanced_value_dataset(cases, 16, 42)
        records = []
        for mode in ("sequential", "pooled"):
            model = deepcopy(base)
            records.append(fit_value_critic(model, datasets, mode, epochs=3, minibatch=8))
            for key, tensor in base.state_dict().items():
                if not key.startswith("central_critic."):
                    self.assertTrue(torch.equal(tensor, model.state_dict()[key]), key)
            self.assertTrue(any(not torch.equal(v, model.central_critic.state_dict()[k])
                                for k, v in base.central_critic.state_dict().items()))
        self.assertEqual(records[0]["updates"], 18)
        self.assertEqual(records[0]["updates"], records[1]["updates"])
        self.assertTrue(torch.equal(rng, torch.get_rng_state()))

    def test_empty_active_case_is_rejected(self):
        cases = [{"data": {"active": {"obs": torch.empty(0, 10), "target": torch.empty(0)},
                           "uniform": {"obs": torch.rand(8, 10), "target": torch.rand(8)}}}]
        with self.assertRaisesRegex(ValueError, "lacks active"):
            balanced_value_dataset(cases, 16, 42)


if __name__ == "__main__":
    unittest.main()
