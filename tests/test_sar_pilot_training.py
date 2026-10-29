import math
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np
import torch
import yaml

from workload_benchmark.sar_pilot import (
    class_parameter_hash, export_metrics, normalize_names, reset_class_predictors, validate_manifest,
    training_options,
)


class SARPilotTrainingTests(unittest.TestCase):
    def test_diagnostic_preserves_default_training_options(self):
        default = training_options(10)
        diagnostic = training_options(40)
        self.assertEqual({key for key in default if default[key] != diagnostic[key]}, {"epochs"})
        diagnostic["lr0"] = 999
        self.assertEqual(training_options(40)["lr0"], default["lr0"])
        with self.assertRaises(ValueError):
            training_options(20)

    def test_names_accept_json_yaml_string_indices(self):
        parsed = yaml.safe_load(json.dumps({"names": {0: "a", 1: "b"}}))
        self.assertEqual(normalize_names(parsed["names"]), ["a", "b"])
        for invalid in ({"0": "a", 0: "b"}, {0: "a", 2: "b"}, {0: "a", 1: "a"}):
            with self.assertRaises(ValueError):
                normalize_names(invalid)

    def test_flagged_similarity_manifest_is_rejected_before_training(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text(json.dumps({"names": ["a"], "final_test_created": False,
                                        "summary": {"cross_split_similarity": {"flag_count": 1}}}))
            with self.assertRaisesRegex(ValueError, "similarity audit is flagged"):
                validate_manifest(path, ["a"], {})

    def test_missing_classes_are_not_filled_with_overall_map(self):
        metrics = SimpleNamespace(box=SimpleNamespace(ap_class_index=np.array([1]),
                                                     all_ap=np.array([[0.8] * 10])))
        result = export_metrics(metrics, ["a", "b", "c"], {0: 0, 1: 3, 2: 0})
        self.assertIsNone(result["per_class"][0]["ap50"])
        self.assertIsNone(result["per_class"][2]["ap50_95"])
        self.assertAlmostEqual(result["mAP50_95_supported_classes"], 0.8)
        self.assertEqual(result["supported_class_count"], 1)

    def test_no_correct_predictions_is_zero_only_for_supported_classes(self):
        metrics = SimpleNamespace(box=SimpleNamespace(ap_class_index=[], all_ap=[]))
        result = export_metrics(metrics, ["a", "b"], {0: 4, 1: 0})
        self.assertEqual(result["per_class"][0]["ap50"], 0.0)
        self.assertIsNone(result["per_class"][1]["ap50"])

    def test_reset_changes_only_class_outputs_and_initializes_priors(self):
        head = torch.nn.Module()
        head.nc = 3
        head.stride = torch.tensor([8.0, 16.0])
        head.cv3 = torch.nn.ModuleList([
            torch.nn.Sequential(torch.nn.Conv2d(4, 4, 1), torch.nn.Conv2d(4, 3, 1)) for _ in range(2)])
        head.cv2 = torch.nn.ModuleList([torch.nn.Conv2d(4, 8, 1) for _ in range(2)])
        head.cv4 = torch.nn.Conv2d(4, 1, 1)
        model = torch.nn.Module()
        model.model = torch.nn.ModuleList([torch.nn.Conv2d(3, 4, 1), head])
        before = {name: value.clone() for name, value in model.state_dict().items()}
        previous_hash = class_parameter_hash(model)
        reset_class_predictors(model)
        self.assertNotEqual(class_parameter_hash(model), previous_hash)
        changed = {name for name, value in model.state_dict().items() if not torch.equal(before[name], value)}
        self.assertEqual(changed, {f"model.1.cv3.{index}.1.{part}" for index in range(2)
                                   for part in ("weight", "bias")})
        for branch, stride in zip(head.cv3, head.stride):
            self.assertTrue(torch.allclose(branch[-1].bias,
                                           torch.full_like(branch[-1].bias, math.log(5 / 3 / (640 / stride) ** 2))))


if __name__ == "__main__":
    unittest.main()
