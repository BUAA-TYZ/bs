import unittest

import numpy as np

from analysis.calibrate_redesign_geometry import (
    configuration, geometry_edges, positions_and_visibility, prepared_graph,
    propagate, select_edges, solve, topo_config)
from sim.orbit import load_satellites, parse_start_time_utc, build_ground_station, sat_to_ground_geometry
from sim.topology import TopologyModel, _visible_from_positions


class CalibrationGeometryTest(unittest.TestCase):
    def test_source_cannot_bridge_compute_components(self):
        edges = [(0, 24), (0, 25)]
        future = np.full(26, np.inf)
        visible = np.zeros(26, bool)
        visible[25] = True
        result = propagate(future, prepared_graph(edges, 26), visible, 10)
        self.assertEqual(result[0], 10)
        self.assertTrue(np.isinf(result[24]))
        opened = propagate(future, prepared_graph(edges, 26, False), visible, 10)
        self.assertEqual(opened[24], 10)

    def test_waiting_and_fractional_release(self):
        none = prepared_graph([], 26)
        contact = prepared_graph([(0, 24)], 26)
        frames = [contact, none, none, none]
        visibility = np.zeros((2, 26, 5), bool)
        visibility[1, 24, 0] = True
        out = solve(frames, visibility, [{"source_sat_id":0,"release_step":23}])
        self.assertAlmostEqual(out["tasks"][0]["single_latency_s"], 7.7)

    def test_no_time_travel_after_contact_ends(self):
        none = prepared_graph([], 26)
        contact = prepared_graph([(0, 24)], 26)
        vis = np.zeros((2, 26, 5), bool)
        vis[1, 24, 0] = True
        out = solve([contact, none, none, none],vis,[{"source_sat_id":0,"release_step":51}])
        self.assertIsNone(out["tasks"][0]["single_latency_s"])

    def test_instant_delivery_and_network_subset(self):
        graph = prepared_graph([(0,24)],25)
        vis = np.zeros((1,25,5),bool)
        vis[0,24,1] = True
        out = solve([graph,graph],vis,[{"source_sat_id":0,"release_step":43}])
        self.assertIsNone(out["tasks"][0]["single_latency_s"])
        self.assertEqual(out["tasks"][0]["five_latency_s"],0)

    def test_vector_geometry_and_capture_match_original(self):
        cfg=configuration(12)
        seconds=np.array([0.,10.,20.,3600.])
        positions,visible=positions_and_visibility(cfg,seconds)
        topo=TopologyModel(topo_config(cfg))
        previous={}
        for k,now in enumerate(seconds):
            pairs,distance=geometry_edges(positions[k])
            seen={tuple(p) for p in pairs}
            for i in range(36):
                for j in range(i+1,36):
                    expected,_=_visible_from_positions(tuple(positions[k,i]),tuple(positions[k,j]),6378.137,0,4500)
                    self.assertEqual((i,j) in seen,expected)
            selected=select_edges(pairs,distance,36,cfg["topology"]["preferred_edges"])
            previous={edge:previous.get(edge,now) for edge in selected}
            actual=topo.snapshot(round(now/.1),.1)
            self.assertEqual({(v.i,v.j) for v in actual.values() if v.up},
                             {edge for edge in selected if now>=previous[edge]+5})
        station=cfg["ground_stations"][0]
        gs=build_ground_station(station["lat_deg"],station["lon_deg"],station["alt_m"])
        t0=parse_start_time_utc(cfg["topology"]["start_time_utc"])
        for k,now in enumerate(seconds):
            for sat in (0,12,24,35):
                elevation,_=sat_to_ground_geometry(topo.sat_recs[sat],gs,t0,now)
                self.assertEqual(visible[k,sat,0], elevation>=10)
        topo.close()

    def test_scales_keep_source_orbits_and_workload(self):
        base=configuration(12)
        for count in (144,1056):
            cfg=configuration(count)
            self.assertEqual(cfg["topology"]["tle_lines"][:24],base["topology"]["tle_lines"][:24])
            self.assertEqual(cfg["scheduled_task_arrivals"],base["scheduled_task_arrivals"])
            self.assertEqual(len(cfg["topology"]["tle_lines"]),count+24)


if __name__ == "__main__":
    unittest.main()
