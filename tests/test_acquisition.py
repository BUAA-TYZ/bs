from datetime import datetime, timedelta, timezone
import json
import unittest

import numpy as np
from skyfield.api import wgs84

from sim.acquisition import ImagingRegion, generate_acquisition_schedule, solar_elevation_deg
from sim.constellation import walker_tle
from sim.orbit import load_satellites, parse_start_time_utc, time_grid


class AcquisitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.epoch = "2026-03-20T12:00:00Z"
        cls.tles = walker_tle(cls.epoch, 1, 2, 620, 60, phasing=0)
        t0 = parse_start_time_utc(cls.epoch)
        state = load_satellites(cls.tles)[0].at(time_grid(t0, [0]))
        point = wgs84.subpoint(state)
        cls.region = ImagingRegion("test_farmland", float(point.latitude.degrees[0]),
                                   float(point.longitude.degrees[0]), 100)

    def schedule(self, **changes):
        kwargs = dict(duration_s=300, scan_step_s=5, targets=[self.region],
                      min_sun_elevation_deg=-90, sun_guard_deg=0,
                      max_off_nadir_deg=30, strips_per_pass=3, min_spacing_s=20)
        kwargs.update(changes)
        return generate_acquisition_schedule(self.tles, self.epoch, [0], **kwargs)

    def test_solar_geometry_day_night_season_and_timezone(self):
        times = [datetime(2026, 3, 20, h, tzinfo=timezone.utc) for h in (0, 12)]
        elevation = solar_elevation_deg(times, 0, 0)
        self.assertLess(elevation[0], -85)
        self.assertGreater(elevation[1], 85)
        np.testing.assert_allclose(solar_elevation_deg(times, 0, 180), -elevation, atol=0.02)
        local_times = [t.astimezone(timezone(timedelta(hours=8))) for t in times]
        np.testing.assert_allclose(solar_elevation_deg(local_times, 0, 0), elevation)
        summer = solar_elevation_deg([datetime(2026, 6, 21, 12, tzinfo=timezone.utc)], 40, 0)[0]
        winter = solar_elevation_deg([datetime(2026, 12, 21, 12, tzinfo=timezone.utc)], 40, 0)[0]
        self.assertTrue(72 < summer < 75)
        self.assertTrue(25 < winter < 28)

    def test_nadir_point_is_visible_and_far_side_is_rejected(self):
        schedule = self.schedule()
        first = schedule["events"][0]
        self.assertEqual(first["release_step"], 0)
        self.assertEqual(first["source_sat_id"], 0)
        self.assertLess(first["geometry"]["off_nadir_deg"], 0.3)
        self.assertGreater(first["geometry"]["sat_elevation_deg"], 89.9)
        antipode = ImagingRegion("far_side", -self.region.lat_deg,
                                 (self.region.lon_deg + 360) % 360 - 180, 0)
        hidden = self.schedule(duration_s=1, targets=[antipode])
        self.assertEqual(hidden["events"], [])
        self.assertEqual(hidden["windows"], [])
        self.assertEqual(hidden["event_count_by_source"], {"0": 0})

    def test_reproducible_json_serializable_and_spacing_and_pass_cap(self):
        first, second = self.schedule(), self.schedule()
        self.assertEqual(first, second)
        self.assertEqual(json.loads(json.dumps(first)), first)
        events = first["events"]
        self.assertGreaterEqual(len(events), 2)
        self.assertLessEqual(len(events), 3)
        self.assertTrue(all(b["release_step"] - a["release_step"] >= 20
                            for a, b in zip(events, events[1:])))
        self.assertTrue(first["windows"][0]["left_censored"])
        self.assertEqual(sum(w["scheduled_strips"] for w in first["windows"]), len(events))
        for event in events:
            self.assertLess(event["release_step"], 300)
            self.assertLessEqual(event["geometry"]["off_nadir_deg"], 30)
            self.assertGreater(event["geometry"]["sat_elevation_deg"], 0)

    def test_same_satellite_regions_share_spacing(self):
        other = ImagingRegion("second_farmland", self.region.lat_deg, self.region.lon_deg, 100)
        result = self.schedule(targets=[other, self.region], strips_per_pass=1)
        self.assertEqual(len(result["windows"]), 2)
        self.assertEqual(len(result["events"]), 2)
        self.assertGreaterEqual(result["events"][1]["release_step"] - result["events"][0]["release_step"], 20)
        reversed_result = self.schedule(targets=[self.region, other], strips_per_pass=1)
        self.assertEqual(result, reversed_result)

    def test_solar_guard_filters_borderline_observation(self):
        visible = self.schedule(duration_s=1)
        measured = visible["events"][0]["geometry"]["sun_elevation_deg"]
        accepted = self.schedule(duration_s=1, min_sun_elevation_deg=measured - 0.5)
        guarded = self.schedule(duration_s=1, min_sun_elevation_deg=measured - 0.5, sun_guard_deg=1)
        self.assertEqual(len(accepted["events"]), 1)
        self.assertEqual(guarded["events"], [])

    def test_invalid_configuration_fails_early(self):
        for changes in ({"dt_s": 3, "scan_step_s": 5}, {"duration_s": 0},
                        {"strips_per_pass": 0}, {"targets": []},
                        {"targets": [self.region, self.region]}, {"sun_guard_deg": -1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.schedule(**changes)
        with self.assertRaises(ValueError):
            generate_acquisition_schedule(self.tles, self.epoch, [2], duration_s=60)
        with self.assertRaises(ValueError):
            solar_elevation_deg([datetime(2026, 1, 1)], 0, 0)


if __name__ == "__main__":
    unittest.main()
