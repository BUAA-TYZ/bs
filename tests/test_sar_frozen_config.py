from copy import deepcopy
from dataclasses import asdict
import json
import unittest

from analysis.build_sar_frozen_config import COVERAGE_PATH, ROOT, build, read_json
from sim.config import config_from_dict


class SARFrozenConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        directory = ROOT / "results/sar_spotfine_calibration_20261015"
        cls.calibration = read_json(directory / "calibration_summary.json")
        cls.fixture = read_json(directory / "workload_fixture.json")
        cls.coverage = read_json(COVERAGE_PATH)
        cls.original = json.loads(json.dumps(asdict(config_from_dict(
            read_json(ROOT / "results/sar_spotfine_frozen_20261015/main_config.json")))))
        cls.configs, cls.manifest = build(
            cls.calibration, cls.fixture, cls.coverage,
            ground_rate_multiplier=10, ground_rate_sensitivity=(1, 5, 10, 20, 100))

    def test_capacity_variants_change_only_ground_service_rate(self):
        rates = set()
        for name, config in self.configs.items():
            with self.subTest(name=name):
                self.assertEqual(asdict(config_from_dict(config)), config)
                restored = deepcopy(config)
                for station, original in zip(restored["ground_stations"], self.original["ground_stations"]):
                    rates.add(round(station["compute_rate"] / original["compute_rate"], 8))
                    station["compute_rate"] = original["compute_rate"]
                self.assertEqual(json.loads(json.dumps(restored)), self.original)
        self.assertEqual(rates, {1, 5, 10, 20, 100})
        self.assertEqual(len(self.configs), 5)

    def test_manifest_distinguishes_measurement_from_capacity_assumption(self):
        basis = self.manifest["parameter_basis"]
        model = basis["ground_capacity_model"]
        self.assertEqual(basis["source_rate_multiplier"], 1)
        self.assertEqual(basis["ground_rate_multiplier"], 10)
        self.assertEqual(model["sensitivity_multipliers"], [1, 5, 20, 100])
        self.assertAlmostEqual(model["equivalent_window_service_s"], 0.0574)
        self.assertAlmostEqual(model["equivalent_image_service_s"], 3.6736)
        self.assertEqual(self.configs["main_config.json"]["aggregation_delay_s"],
                         self.calibration["aggregation_delay_s"])

    def test_invalid_main_and_sensitivity_multipliers_are_rejected(self):
        for value in (0, -1, float("nan"), float("inf"), True):
            for field in ("ground_rate_multiplier", "ground_rate_sensitivity"):
                with self.subTest(field=field, value=value):
                    kwargs = {field: (value,) if field.endswith("sensitivity") else value}
                    with self.assertRaisesRegex(ValueError, field):
                        build(self.calibration, self.fixture, self.coverage, **kwargs)


if __name__ == "__main__":
    unittest.main()
