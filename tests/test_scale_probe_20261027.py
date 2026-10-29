from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from analysis.run_scale_probe_20261027 import scale_config, episode_config, DATE, MAIN, read, digest, METHODS
from analysis.register_scale_generalization_20261025 import tle
from analysis.prepare_main_bc_stage2 import arrivals


class ScaleProbeTests(unittest.TestCase):
    def test_m_reproduces_entire_template(self):
        self.assertEqual(scale_config(24,144),read(MAIN/'config_template.json'))

    def test_l_changes_only_registered_scale_fields(self):
        old=read(MAIN/'config_template.json');c=scale_config(48,288)
        self.assertEqual(c['num_sats'],336);self.assertEqual(c['task_source_sat_ids'],list(range(48)))
        self.assertEqual(c['topology']['neighbor_limits'],[1]*48+[4]*288)
        self.assertEqual(len(c['topology']['preferred_edges']),288)
        self.assertTrue(all(48<=a<336 and 48<=b<336 and (a-48)//24==(b-48)//24 for a,b in c['topology']['preferred_edges']))
        for k in old.keys()-{'num_sats','task_source_sat_ids','topology'}:self.assertEqual(c[k],old[k],k)
        for k in old['topology'].keys()-{'tle_lines','neighbor_limits','preferred_edges'}:self.assertEqual(c['topology'][k],old['topology'][k],k)
        reg=read(MAIN/'scale_generalization_20261025/registration.json')
        self.assertEqual(digest(c['topology']['tle_lines']),reg['scenario_shapes']['L']['tle_formula_sha256'])

    def test_sealed_dates_rejected_and_nine_fixed_methods(self):
        for date in ('2026-12-21','2026-12-22','2026-12-23'):
            with self.assertRaises(ValueError):arrivals([],date)
            with TemporaryDirectory() as d:
                with self.assertRaises(AssertionError):episode_config({},date,[],{},Path(d),{},[])
        self.assertEqual(len(METHODS),9);self.assertEqual(len(set(METHODS)),9)

    def test_hour_followup_and_geometry_shift_without_task_copy(self):
        with TemporaryDirectory() as d:
            c=scale_config(48,288)
            p=episode_config(c,DATE,[],dict(start_s=1800,end_s=5400,raw_bytes=0,count=0,event_indices=[]),Path(d),{},[])
            self.assertEqual(p['score_end_s']-p['score_start_s'],3600)
            self.assertEqual(p['sim_duration_s']-p['score_end_s'],600)
            self.assertEqual(p['origin_absolute_s'],1200)
            self.assertEqual(p['config']['scheduled_task_arrivals'],[])


if __name__=='__main__':unittest.main()
