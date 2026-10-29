import unittest
from types import SimpleNamespace as NS
from unittest.mock import patch

import numpy as np

from analysis.run_calibration_capacity import FifoDiagnostic, busiest_window, scaled_config, waiting_snapshot
from analysis.calibrate_redesign_geometry import configuration
from sim.entities import ActionType, TileState


def tile(task, state=TileState.READY, location=24, completed=0):
    return NS(parent_task_id=task, state=state, location=location, completed_compute_windows=completed,
              compute_cost=4, timestamps=NS(end_tx=0), data_size_mb=600, memory_holders={0})


class CapacityDiagnosticTests(unittest.TestCase):
    def test_compute_scaling_preserves_workload_and_physics(self):
        original=configuration(144)
        for multiplier in (1,4,10):
            actual=scaled_config(multiplier)
            self.assertEqual(actual["topology"],original["topology"])
            self.assertEqual(actual["scheduled_task_arrivals"],original["scheduled_task_arrivals"])
            self.assertEqual(actual["onboard_compute_profile"]["workspace_gb"],original["onboard_compute_profile"]["workspace_gb"])
            self.assertAlmostEqual(actual["onboard_compute_profile"]["service_s"]["2"],1.056*multiplier)
            self.assertAlmostEqual(actual["ground_stations"][0]["compute_rate"],original["ground_stations"][0]["compute_rate"]/multiplier)
            self.assertEqual(actual["relay_max_hops"],35)

    def test_nearest_first_contact_is_not_reselected_and_no_source_receiver(self):
        links={"0-24":NS(up=True,bandwidth_mbps=1000),"0-25":NS(up=True,bandwidth_mbps=1000)}
        item=tile("task_0",TileState.QUEUED,0)
        env=NS(time=10,cfg=NS(dt=.1),task_sources=[0,1],
               topology=NS(sat_recs=list(range(26)),t0=None),
               satellites={24:NS(mem_capacity_gb=10,mem_used_gb=0),25:NS(mem_capacity_gb=10,mem_used_gb=0)},
               tiles={"a":item},tasks={"task_0":NS(task_id="task_0",source_sat_id=0,release_time=0,tile_ids=["a"])},
               memory_size_gb=lambda x:x/1000,_get_links=lambda _:links)
        state=NS(tiles={"a":{"in_transfer":False}},neighbors={0:[1,24,25]})
        rule=FifoDiagnostic()
        with patch("analysis.run_calibration_capacity.position_km",side_effect=lambda sat,*_: (100 if sat==24 else 200 if sat==25 else 0,0,0)):
            actions=rule.select_actions(env,state)
            self.assertEqual(actions[0].target_sat_id,24)
            self.assertEqual(actions[0].action_type,ActionType.OFFLOAD)
            links["0-24"].up=False
            self.assertEqual(rule.select_actions(env,state),[])
            self.assertEqual(rule.assignments["task_0"]["target"],24)

    def test_memory_admission_does_not_overbook_or_skip_fifo_head(self):
        link=NS(up=True,bandwidth_mbps=1000)
        env=NS(time=0,cfg=NS(dt=.1),task_sources=[0],topology=NS(sat_recs=list(range(25)),t0=None),
               satellites={24:NS(mem_capacity_gb=1,mem_used_gb=0)},
               tiles={"a":tile("task_0",TileState.QUEUED,0),"b":tile("task_0",TileState.QUEUED,0)},
               tasks={"task_0":NS(task_id="task_0",source_sat_id=0,release_time=0,tile_ids=["a","b"])},
               memory_size_gb=lambda x:x/1000,_get_links=lambda _:{"0-24":link})
        state=NS(tiles={tid:{"in_transfer":False} for tid in env.tiles},neighbors={0:[24]})
        with patch("analysis.run_calibration_capacity.position_km",return_value=(0,0,0)):
            actions=FifoDiagnostic().select_actions(env,state)
        self.assertEqual([a.tile_id for a in actions],["a"])

    def test_waiting_excludes_running_transit_and_partial_whole_image(self):
        env=NS(time=10,tiles={"a":tile("task_0"),"b":tile("task_0",TileState.RUNNING),
                             "c":tile("task_1"),"d":tile("task_2",TileState.TRANSFERRING)},
               tasks={"task_0":NS(source_sat_id=0,tile_ids=["a","b"]),
                      "task_1":NS(source_sat_id=1,tile_ids=["c"]),"task_2":NS(source_sat_id=2,tile_ids=["d"])},
               satellites={24:NS(queue=["a","b","c","d"],executing_batch=["b"])})
        result=waiting_snapshot(env,[24])[0]
        self.assertEqual(result["blocks"],2)
        self.assertEqual(result["waiting_source_count"],2)
        self.assertEqual(result["complete_task_ids"],["task_1"])
        self.assertEqual(waiting_snapshot(env,[24],enabled=False),[])
        env.tiles["c"].completed_compute_windows=1
        self.assertEqual(waiting_snapshot(env,[24])[0]["complete_waiting_images"],0)

    def test_future_arrival_cannot_count_as_waiting(self):
        item=tile("task_0")
        item.timestamps.end_tx=11
        env=NS(time=10,tiles={"a":item},satellites={24:NS(queue=["a"],executing_batch=[])},tasks={})
        self.assertEqual(waiting_snapshot(env,[24]),[])

    def test_just_arrived_image_is_not_resource_queue(self):
        item=tile("task_0")
        item.timestamps.end_tx=10
        env=NS(time=10,tiles={"a":item},satellites={24:NS(queue=["a"],executing_batch=[])},tasks={})
        self.assertEqual(waiting_snapshot(env,[24]),[])

    def test_busy_window_uses_sliding_integral_not_max_instant(self):
        result=busiest_window(np.array([0.,1.,1.,1.,0.]),3)
        self.assertEqual((result["start_s"],result["end_s"],result["busy_s"]),(1,4,3.))
        empty=busiest_window(np.zeros(601),600)
        self.assertEqual(empty["start_s"],0)
        self.assertEqual(empty["busy_s"],0)


if __name__=="__main__":
    unittest.main()
