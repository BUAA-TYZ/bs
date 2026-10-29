import json
from pathlib import Path
import tempfile
import unittest

from analysis.prepare_sla_numerical_protocol import derive


class NumericalProtocolTests(unittest.TestCase):
    def prepare(self, root, **extra):
        source = root / "source"
        source.mkdir()
        config = {"dt": 1, "sim_steps": 15000, "task_arrival_rate": 0,
                  "decision_interval_steps": 1, "topology_update_steps": 10,
                  "ground_visibility_update_steps": 10, "tile_lifecycle_log": "",
                  "deadline_steps": 0, "task_arrival_end_step": 11,
                  "topology": {"bandwidth_noise": 0, "bandwidth_period": 0},
                  "scheduled_task_arrivals": [{"release_step": 10, "source_sat_id": 0}],
                  "input_relay_max_hops": 2, "ground_input_relay": True, **extra}
        (source / "a_config.json").write_text(json.dumps(config))
        (source / "run.json").write_text(json.dumps({"case_info": [{"id": "a"}], "provenance": {}}))
        return source

    def test_preserves_execution_capability_and_complete_sla_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.prepare(root)
            derive(source, root / "target", .1)
            cfg = json.loads((root / "target/a_config.json").read_text())
            self.assertTrue(cfg["ground_input_relay"])
            self.assertEqual(cfg["input_relay_max_hops"], 2)
            self.assertEqual(cfg["scheduled_task_arrivals"][0]["release_step"], 100)
            self.assertEqual(cfg["sim_steps"], 3110)
            self.assertEqual(cfg["decision_interval_steps"], 10)

    def test_rejects_silently_dropped_configuration_field(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.prepare(root, unrecognized_execution_mode=True)
            with self.assertRaisesRegex(AssertionError, "unrecognized_execution_mode"):
                derive(source, root / "target", .1)


if __name__ == "__main__":
    unittest.main()
