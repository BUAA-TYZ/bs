import unittest
import tempfile
from unittest.mock import patch
import sys
from pathlib import Path
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.sar_selector_learning import select_samples, deadline_loss, guard_date
from analysis.sar_selector_model import class_weights, recalls, rows_and_scaler, predict
from analysis import sar_selector_model as model_module


class LearningTests(unittest.TestCase):
    def test_actual_eligible_sampling_and_small_cells(self):
        states = [{"task_id": f"task_{i * 7}"} for i in range(39)]
        sample = select_samples(states, 1701)
        self.assertEqual(len(sample), 8)
        self.assertEqual(sample, select_samples(states, 1701))
        self.assertTrue(all(s in states for s in sample))
        self.assertEqual(select_samples(states[:5], 1701), states[:5])
        self.assertEqual(select_samples([], 1701), [])

    def test_deadline_loss_not_merely_latency_regret(self):
        def row(latency):
            return {"latency_seconds": latency, "release_seconds": 10, "censor_seconds": 7210}
        branches = {"local": row(301), "cgs": row(300), "ground": row(None)}
        self.assertTrue(deadline_loss(branches, "local", "cgs"))
        self.assertTrue(deadline_loss(branches, "ground", "cgs"))
        self.assertFalse(deadline_loss(branches, "cgs", "cgs"))
        branches["cgs"] = row(300.01)
        self.assertFalse(deadline_loss(branches, "local", "cgs"))
        self.assertIsNone(deadline_loss(branches, "local", None))

    def test_blind_and_consumed_test_dates_rejected(self):
        guard_date("2026-10-15")
        guard_date("2026-10-24")
        for date in ("2026-12-15", "2026-12-01", "2026-11-21", "2026-10-25"):
            with self.assertRaises(ValueError):
                guard_date(date)

    def test_weights_equalize_classes_and_report_recall(self):
        y = [0, 0, 0, 1, 1, 2]
        w = class_weights(y)
        np.testing.assert_allclose(np.bincount(y) * w, [2, 2, 2])
        self.assertGreater(w[2], w[0])
        result = recalls(y, [0, 0, 1, 1, 0, 0])
        self.assertEqual(result["recall"]["ground"], 0)
        self.assertEqual(result["recall"]["cgs"], .5)
        with self.assertRaises(ValueError):
            class_weights([0, 1])

    def test_validation_does_not_fit_scaler(self):
        rows = [{"split": split, "status": "DETERMINATE", "features": {"x": value}}
                for split, value in [("train", 0), ("train", 2), ("validation", 10000)]]
        _, _, _, _, mean, std = rows_and_scaler({"labels": rows})
        np.testing.assert_array_equal(mean, [1])
        np.testing.assert_array_equal(std, [1])

    def test_two_variants_have_opposite_selection_objectives(self):
        model = {"feature_names": ["x"], "mean": [0], "std": [1],
                 "layers": [{"weight": [[0], [0], [0]], "bias": [10, 20, 5]}], "variant": "classifier"}
        self.assertEqual(predict(model, {"x": 3}), 1)
        model["variant"] = "regressor"
        self.assertEqual(predict(model, {"x": 3}), 2)

    def test_training_export_with_censored_branch_and_three_seeds(self):
        rows = []
        for split in ("train", "validation"):
            for label in range(3):
                branches = {p: {"latency_seconds": 100 + 200 * (i != label), "release_seconds": 10,
                                "censor_seconds": 7210} for i, p in enumerate(model_module.PLANS)}
                branches[model_module.PLANS[(label + 1) % 3]]["latency_seconds"] = None
                rows.append({"split": split, "status": "DETERMINATE", "label": model_module.PLANS[label],
                             "features": {"x": label, "constant": 1}, "branches": branches})
        with tempfile.TemporaryDirectory() as folder, patch.object(model_module, "OUT", Path(folder)), patch.object(model_module, "curves"):
            model_module.write(Path(folder) / "label_audit.json", {"passed": True, "labels": rows})
            model_module.write(Path(folder) / "protocol.json", {"model": {
                "variants": ["classifier", "regressor"], "epochs": 2, "learning_rate": .003, "weight_decay": .001}})
            model_module.train()
            trained = model_module.read(Path(folder) / "training_complete.json")
            self.assertEqual(len(trained["records"]), 6)
            self.assertEqual({r["seed"] for r in trained["records"]}, {1701, 1702, 1703})
            for r in trained["records"]:
                self.assertEqual(len(r["curve"]), 2)
                self.assertTrue(np.isfinite(r["curve"][-1]["weighted_training_loss"]))


if __name__ == "__main__":
    unittest.main()
