from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import unittest

from analysis.sar_condition_scan import derive, strong_ground
from sim.config import config_from_dict


class SARConditionScanTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = json.loads(Path("results/sar_loopfree_20261017/prefix_21600/derived_config.json").read_text())

    def test_compute_scaling_updates_executor_planner_and_ground_consistently(self):
        before = deepcopy(self.base)
        cfg = derive(self.base, compute=10)
        self.assertEqual(cfg["image_tiling"], self.base["image_tiling"])
        self.assertEqual(cfg["onboard_compute_profile"]["workspace_gb"], {"1": 8, "2": 14})
        self.assertAlmostEqual(cfg["onboard_compute_profile"]["service_s"]["2"] * 32, 337.92)
        self.assertAlmostEqual(cfg["compute_rate"], self.base["compute_rate"] / 10)
        self.assertAlmostEqual(cfg["ground_stations"][0]["compute_rate"], self.base["ground_stations"][0]["compute_rate"] / 10)
        self.assertEqual(self.base, before)
        self.assertEqual(cfg, asdict(config_from_dict(cfg)))

    def test_chip_slowdown_alternative_keeps_ground_fixed(self):
        cfg = derive(self.base, compute=4, compute_scope="onboard")
        self.assertEqual(cfg["ground_stations"], self.base["ground_stations"])

    def test_load_preserves_opportunities_and_outputs_without_aliasing(self):
        cfg = derive(self.base, load=4)
        events = cfg["scheduled_task_arrivals"]
        self.assertEqual(len(events), 168)
        for i, event in enumerate(self.base["scheduled_task_arrivals"]):
            self.assertEqual(events[i*4:(i+1)*4], [event] * 4)
        events[0]["window_result_bytes"][0] = -1
        self.assertNotEqual(events[1]["window_result_bytes"][0], -1)
        self.assertNotEqual(self.base["scheduled_task_arrivals"][0]["window_result_bytes"][0], -1)

    def test_bandwidth_changes_only_net_rate_and_rejects_factorial_mix(self):
        cfg = derive(self.base, bandwidth=5)
        normalized_base = asdict(config_from_dict(self.base))
        self.assertEqual([k for k in cfg if cfg[k] != normalized_base[k]], ["topology"])
        self.assertEqual({k for k in cfg["topology"] if cfg["topology"][k] != self.base["topology"][k]},
                         {"bandwidth_mbps_min", "bandwidth_mbps_max"})
        with self.assertRaises(ValueError):
            derive(self.base, compute=4, load=2)

    def test_strong_ground_selects_whole_record_by_P300_then_R300(self):
        def row(p, r):
            return {"method": "ground_unique_relay", "P300": p, "R300_seconds": r,
                    "on_time_task_ids": ["task_0"], "metrics": {"task_outcomes": [
                        {"task_id": "task_0", "source_sat_id": 0, "release_seconds": 0,
                         "status": "completed", "latency_seconds": r}]}}
        records = {"ground_8": row(.8, 150), "ground_loopfree": row(.7, 140)}
        self.assertEqual(strong_ground(records), "ground_8")
        records["ground_loopfree"]["P300"] = .8
        self.assertEqual(strong_ground(records), "ground_loopfree")
        records["ground_loopfree"]["R300_seconds"] = 150
        self.assertEqual(strong_ground(records), "ground_8")


if __name__ == "__main__":
    unittest.main()
