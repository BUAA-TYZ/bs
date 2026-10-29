"""高分场景的轨道布局与公平工作量配置约束。"""

from argparse import Namespace
import unittest
from unittest.mock import patch

from sgp4.api import Satrec

from analysis.evaluate_highres_scene import build_cases
from sim.constellation import walker_tle


class HighresSceneTests(unittest.TestCase):
    def test_explicit_planes_and_unique_catalog_numbers(self):
        lines = walker_tle("2026-10-15T00:00:00Z", 2, 11, 550, 53,
                           plane_raan_offsets_deg=[0, 90], first_satellite_number=25)
        sats = [Satrec.twoline2rv(*pair) for pair in lines]
        self.assertEqual([sat.satnum for sat in sats], list(range(25, 47)))
        self.assertAlmostEqual(sats[0].nodeo, 0)
        self.assertAlmostEqual(sats[11].nodeo, 1.5707963267948966)
        with self.assertRaises(ValueError):
            walker_tle("2026-10-15T00:00:00Z", 2, 11, 550, 53, plane_raan_offsets_deg=[0])

    def test_all_events_retained_and_common_physical_cadence(self):
        args = Namespace(days=1, architecture="heterogeneous", strips_per_pass=3, dt=.5,
                         compression_ratio=3, decision_batch=64, downlink_mbps=1200,
                         isl_mbps=10000, efficiency=.8, source_compute_rate=1,
                         ground_compute_multiplier=1)
        events = [{"release_step": 60000, "source_sat_id": 0},
                  {"release_step": 60300, "source_sat_id": 1}]
        with patch("analysis.evaluate_highres_scene.generate_acquisition_schedule",
                   return_value={"events": events}):
            cases, _, provenance = build_cases(args)
        cfg, info = cases[0]
        self.assertEqual(info["num_acquisitions"], 2)
        self.assertEqual(cfg.scheduled_task_arrivals[0]["release_step"], 240)
        self.assertEqual((cfg.sim_steps - cfg.scheduled_task_arrivals[-1]["release_step"]) * cfg.dt, 14400)
        self.assertEqual(cfg.decision_interval_steps * cfg.dt, 1)
        self.assertEqual(cfg.topology_update_steps * cfg.dt, 10)
        self.assertEqual(cfg.ground_stations[0]["bandwidth_mbps"], 960)
        self.assertEqual(cfg.ground_stations[0]["receive_channels"], 1)
        self.assertEqual(cfg.topology["bandwidth_distance_scale_km"], 0)
        self.assertGreater(cfg.image_size_mb, 2100)
        self.assertEqual(provenance["orbit"]["source_ids"], [0, 1])


if __name__ == "__main__":
    unittest.main()
