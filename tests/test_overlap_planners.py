from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from sim.entities import ActionType, TileState
from sim.pipeline.distributed import DistributedPipeline
from sim.pipeline.ground import GroundPipeline
from sim.scheduler.greedy import GreedyEarliestFinish
from sim.scheduler.predictive import PredictivePolicy
from sim.scheduler.window_aware import WindowAwareGreedy


def planner_state(pixel_workload=True):
    tile = dict(state="QUEUED", location=0, in_transfer=False, parent_task_id="task",
                data_size_mb=1.0, data_size_gb=1 / 1024, compute_cost=1.0,
                result_size_mb=1.0, vram_req_gb=0.01, task_pending_count=2)
    sibling = {**tile}
    satellites = {
        0: dict(compute_rate=1.0, queue_len=0, mem_remaining_gb=1.0,
                next_gs_windows={"gs": None}),
        1: dict(compute_rate=10.0, queue_len=0, mem_remaining_gb=1.0,
                next_gs_windows={"gs": (0, 20)}),
    }
    return SimpleNamespace(
        config=dict(dt=1.0, image_tiling=pixel_workload, result_size_mb=0.5,
                    vram_capacity_gb=1.0),
        tiles={"first": tile}, pending_task_tiles={"task": {"first": tile, "second": sibling}},
        satellites=satellites, neighbors={0: [1], 1: [0]},
        links={"0-1": dict(up=True, bandwidth_mbps=8.0, contact_windows=[(0, 10)])},
        ground_options={0: [], 1: [dict(gs_id="gs", bandwidth_mbps=8.0)]},
        ground_stations={"gs": dict(bandwidth_mbps=8.0, compute_rate=10.0,
                                    queue_len=0, running=0, receive_channels=1,
                                    visible_contenders=1)},
    )


class OverlapPlannerTests(unittest.TestCase):
    def test_result_volume_changes_new_greedy_decision_only(self):
        for policy in (GreedyEarliestFinish(), WindowAwareGreedy()):
            for pixel_workload in (False, True):
                with self.subTest(policy=type(policy).__name__, pixel_workload=pixel_workload):
                    state = planner_state(pixel_workload)
                    state.neighbors[0] = []
                    state.ground_options[0] = [dict(gs_id="gs", bandwidth_mbps=8.0)]
                    tile = state.tiles["first"]
                    tile.update(data_size_mb=8.0, data_size_gb=8 / 1024,
                                result_size_mb=12.0, task_pending_count=1)
                    action = policy.select_actions(state)[0]
                    self.assertEqual(action.action_type,
                                     ActionType.OFFLOAD if pixel_workload else ActionType.LOCAL)
                    self.assertEqual(action.target_gs_id, "gs" if pixel_workload else None)

    def test_predictive_accounts_for_heterogeneous_siblings(self):
        for field, value in (("data_size_mb", 30.0), ("data_size_gb", 2.0),
                             ("result_size_mb", 30.0), ("compute_cost", 300.0)):
            with self.subTest(field=field):
                state = planner_state()
                state.pending_task_tiles["task"]["second"][field] = value
                legacy = deepcopy(state)
                legacy.config["image_tiling"] = False
                self.assertEqual(PredictivePolicy().select_actions(legacy)[0].target_sat_id, 1)
                self.assertIsNone(PredictivePolicy().select_actions(state)[0].target_sat_id)

    def test_predictive_does_not_retransmit_sibling_at_another_source(self):
        state = planner_state()
        state.pending_task_tiles["task"]["second"].update(location=1, data_size_mb=30.0,
                                                         result_size_mb=30.0, compute_cost=300.0)
        self.assertEqual(PredictivePolicy().select_actions(state)[0].target_sat_id, 1)

    def test_ground_whole_image_uses_manifest_cost_in_pixel_mode(self):
        for pixel_workload, expected_remaining in ((False, 59.0), (True, 3.5)):
            with self.subTest(pixel_workload=pixel_workload):
                tiles = {"first": SimpleNamespace(state=TileState.QUEUED, compute_cost=2.0),
                         "second": SimpleNamespace(state=TileState.QUEUED, compute_cost=7.0)}
                env = SimpleNamespace(
                    cfg=SimpleNamespace(image_tiling={"enabled": True} if pixel_workload else None,
                                        compute_cost_per_tile=60.0, dt=1.0),
                    time=0, tiles=tiles,
                    tasks={"task": SimpleNamespace(tile_ids=list(tiles), num_tiles=2)},
                    ground_stations={"gs": SimpleNamespace(compute_rate=2.0)},
                    ground_compute_queue={"gs": ["task"]}, ground_task_running={},
                    _log_task_event=lambda *args, **kwargs: None,
                )
                GroundPipeline(env).advance_ground_task_compute()
                self.assertEqual(env.ground_task_running["gs"].remaining_compute, expected_remaining)

    def test_ground_source_uses_environment_memory_units(self):
        for divisor in (1000, 1024):
            with self.subTest(divisor=divisor):
                source = SimpleNamespace(mem_used_gb=0.0, mem_capacity_gb=1.0)
                task = SimpleNamespace(task_id="task", source_sat_id=0, image_size_mb=128.0)
                tile = SimpleNamespace(state=TileState.CREATED)
                env = SimpleNamespace(
                    satellites={0: source}, task_source_mem_gb={}, ground_task_queued={},
                    memory_size_gb=lambda mb: mb / divisor,
                    metrics=SimpleNamespace(update_mem_peak=lambda *args: None),
                    _log_tile_event=lambda *args, **kwargs: None,
                )
                GroundPipeline(env).enqueue_ground_task(task, [tile])
                self.assertEqual(source.mem_used_gb, 128.0 / divisor)
                self.assertEqual(env.task_source_mem_gb["task"], 128.0 / divisor)

    def test_downlink_and_result_relay_use_tile_result_bytes(self):
        for relay in (False, True):
            with self.subTest(relay=relay):
                tile = SimpleNamespace(tile_id="first", location=0, state=TileState.COMPUTED,
                                       timestamps=SimpleNamespace(end_compute=0, start_downlink=None),
                                       result_available_time=0, result_path=[0], result_size_mb=3.5)
                gs = SimpleNamespace(gs_id="gs")
                env = SimpleNamespace(
                    cfg=SimpleNamespace(result_buffering=False, result_relay=relay, result_size_mb=99.0),
                    time=1, tiles={"first": tile}, _computed_waiting_downlink={"first"},
                    downlink_transfers=[], transfers=[], ground_stations={"gs": gs},
                    tile_result_size_mb=lambda current: current.result_size_mb,
                    _select_ground_station=lambda sid: gs,
                    _reserve_result_memory=lambda current, sid: True,
                    _log_tile_event=lambda *args, **kwargs: None,
                )
                with patch("sim.pipeline.distributed.delivery_route", return_value=("sat", 1)) as route:
                    DistributedPipeline(env).start_downlinks({})
                transfers = env.transfers if relay else env.downlink_transfers
                self.assertEqual(len(transfers), 1)
                self.assertEqual(transfers[0].remaining_mb, 3.5)
                if relay:
                    self.assertEqual(route.call_args.args[2], 3.5)


if __name__ == "__main__":
    unittest.main()
