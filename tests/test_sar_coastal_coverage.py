import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analysis import audit_daynight_coverage as base
from analysis.audit_sar_coastal_coverage import (
    run_audit, sar_tles_by_source, selection, summarize_groups,
)


class SarCoastalCoverageTests(unittest.TestCase):
    def test_only_sar_trajectories_preserve_existing_orbits(self):
        tles = sar_tles_by_source()
        self.assertEqual(set(tles), set(range(24, 72)))
        self.assertTrue(all(tle == base.tles_by_source()[sid] for sid, tle in tles.items()))
        spec = selection()
        self.assertNotIn("optical", spec)
        self.assertEqual(spec["node_identity"]["optical_count"], 0)
        self.assertEqual(spec["sar"]["compute_cost"], None)
        self.assertEqual(spec["sar"]["focused_product_ready_delay_s"], None)

    def test_five_coastal_coordinates_are_fixed_point_references(self):
        targets = selection()["targets"]
        self.assertEqual([(t["target_id"], t["lat_deg"], t["lon_deg"]) for t in targets], [
            ("la_harbor", 33.72, -118.272),
            ("san_francisco_bay", 37.806305, -122.46589),
            ("new_york_harbor", 40.700554, -74.01417),
            ("savannah_approach", 32.034695, -80.90303),
            ("houston_ship_channel", 29.681667, -94.985),
        ])
        self.assertTrue(all(t["radius_km"] == 0 and t["noaa_station_id"] in t["source_url"] for t in targets))

    def test_summary_counts_exclude_optical_and_keep_empty_hours(self):
        event = {"release_s": 15, "sensor": "sar", "source_sat_id": 24,
                 "target_id": "la_harbor", "night": True}
        groups = summarize_groups([event], selection())
        self.assertEqual([g["total_constellation_satellites"] for g in groups], [34, 46, 70])
        for group in groups:
            self.assertEqual(group["summary"]["count_by_sensor"], {"optical": 0, "sar": 1})
            self.assertEqual(group["summary"]["empty_hours_utc"], list(range(1, 24)))
            self.assertFalse(set(group["source_sat_ids"]) & {0, 1})
        with self.assertRaisesRegex(ValueError, "optical"):
            summarize_groups([{**event, "sensor": "optical"}], selection())

    def test_selection_saved_before_sar_only_propagation_without_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "audit"

            def check_then_stop(spec, *, tle_map):
                self.assertEqual(json.loads((output / "selection.json").read_text()), spec)
                self.assertEqual(set(tle_map), set(range(24, 72)))
                raise RuntimeError("intentional stop before propagation")

            with patch("analysis.audit_daynight_coverage.generate_geometry", side_effect=check_then_stop):
                with self.assertRaisesRegex(RuntimeError, "intentional stop"):
                    run_audit(output)
            with self.assertRaises(FileExistsError):
                run_audit(output)


if __name__ == "__main__":
    unittest.main()
