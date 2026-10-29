import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
from skyfield.api import wgs84
from skyfield.framelib import itrs

from analysis.audit_daynight_coverage import (
    EPOCH, eligible_capture_starts, point_geometry, run_audit, select_candidates,
    selection, source_groups, summarize_events, tles_by_source,
)
from sim.orbit import load_satellites, parse_start_time_utc, time_grid


def event(start, *, sid=24, target="a", window=0, sensor="sar", duration=15, night=False):
    return {"capture_start_s": start, "release_s": start + duration,
            "source_sat_id": sid, "target_id": target, "window_id": window,
            "sensor": sensor, "night": night}


class DaynightCoverageTests(unittest.TestCase):
    def test_architecture_groups_are_nested_and_cover_all_twelve_planes(self):
        groups = source_groups()
        self.assertEqual([len(groups[n]) for n in (12, 24, 48)], [12, 24, 48])
        self.assertLess(set(groups[12]), set(groups[24]))
        self.assertLess(set(groups[24]), set(groups[48]))
        for n, ids in groups.items():
            self.assertEqual({(sid - 24) // 4 for sid in ids}, set(range(12)))
        self.assertEqual(len(tles_by_source()), 50)
        self.assertEqual(selection()["sar"]["compute_cost"], None)

    def test_capture_requires_every_sample_including_both_endpoints(self):
        np.testing.assert_array_equal(eligible_capture_starts([True] * 5, 3), [True, True, False, False, False])
        np.testing.assert_array_equal(eligible_capture_starts([True, True, False, True, True], 3), [False] * 5)
        np.testing.assert_array_equal(eligible_capture_starts([True, False], 0), [True, False])
        self.assertEqual(len(eligible_capture_starts([True], 3)), 1)

    def test_spacing_shared_across_targets_and_window_cap_applied(self):
        candidates = [event(0), event(0, target="b", window=1), event(20, target="b", window=1),
                      event(30, target="b", window=1), event(60), event(90), event(120), event(0, sid=25, window=2)]
        selected = select_candidates(candidates)
        self.assertEqual([(e["source_sat_id"], e["capture_start_s"]) for e in selected],
                         [(24, 0), (25, 0), (24, 30), (24, 60), (24, 90)])
        self.assertEqual(selected, select_candidates(list(reversed(candidates))))

    def test_capture_end_not_start_controls_exclusive_horizon(self):
        selected = select_candidates([event(86380), event(86385, sid=25), event(86400, sensor="optical", duration=0)])
        self.assertEqual([e["release_s"] for e in selected], [86395])

    def test_cached_ground_normal_matches_skyfield_elevation(self):
        satellite = load_satellites([tles_by_source()[24]])[0]
        times = time_grid(parse_start_time_utc(EPOCH), [0, 1234, 43200])
        target = {"lat_deg": 39, "lon_deg": -100}
        actual = point_geometry(satellite.at(times).frame_xyz(itrs).km.T, target)
        ground = wgs84.latlon(target["lat_deg"], target["lon_deg"])
        expected = (satellite - ground).at(times).altaz()[0].degrees
        np.testing.assert_allclose(actual["sat_elevation_deg"], expected, atol=1e-7, rtol=0)
        np.testing.assert_allclose(actual["ground_incidence_deg"], 90 - expected, atol=1e-7, rtol=0)

    def test_summary_includes_edge_gaps_empty_hours_night_and_half_open_density(self):
        events = [event(0, duration=0), event(299, duration=0, night=True), event(300, duration=0),
                  event(3600, sensor="optical", duration=0)]
        summary = summarize_events(events, [24, 25], ["a", "b"], duration_s=10800)
        self.assertEqual([r["total"] for r in summary["hourly_counts"]], [3, 1, 0])
        self.assertEqual(summary["empty_hours_utc"], [2])
        self.assertEqual(summary["night_acquisitions"], 1)
        self.assertEqual(summary["count_by_source"]["25"], 0)
        self.assertEqual(summary["count_by_target"]["b"], 0)
        self.assertEqual(summary["densest_300s"]["count"], 2)
        self.assertEqual(summary["longest_gap_including_start_end"], {"duration_s": 7200, "start_s": 3600, "end_s": 10800.0})
        self.assertFalse(summary["every_hour_has_acquisition"])
        empty = summarize_events([], [24], ["a"])
        self.assertEqual(empty["longest_gap_including_start_end"]["duration_s"], 86400)
        self.assertEqual(empty["densest_300s"]["count"], 0)

    def test_selection_is_persisted_before_propagation_and_existing_output_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            out = Path(temporary) / "audit"
            def check_then_stop(spec):
                self.assertEqual(json.loads((out / "selection.json").read_text()), spec)
                raise RuntimeError("intentional stop before orbit propagation")
            with patch("analysis.audit_daynight_coverage.generate_geometry", side_effect=check_then_stop):
                with self.assertRaisesRegex(RuntimeError, "intentional stop"):
                    run_audit(out)
            with self.assertRaises(FileExistsError):
                run_audit(out)


if __name__ == "__main__":
    unittest.main()
