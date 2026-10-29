from copy import deepcopy
from dataclasses import asdict
import unittest

from analysis.redesign_scenario import constellation, fixture, tiling
from analysis.sar_three_plan import CandidatePipeline, RawImageAction, ThreePlanChoice
from sim.control import Advertisement, ControlPlane
from sim.entities import Action, ActionType, Link, TileState
from test_compute_cohorts import PROFILE
from test_simulation import add_task, make_env


class RedesignSceneTests(unittest.TestCase):
    def test_all_granularities_preserve_windows_and_unique_pixels(self):
        for mode, expected in (("spot", 9), ("fine", 64), ("slea", 100)):
            reference = None
            for count in (1, 4, 16, 64):
                work = tiling(mode, count)
                blocks = work.tiles()
                boxes = [box for block in blocks for box in block.windows_xyxy]
                self.assertEqual(len(set(boxes)), expected)
                self.assertEqual(len(boxes), expected)
                self.assertEqual(sum(block.compute_cost for block in blocks), expected)
                if reference is None:
                    reference = set(boxes)
                self.assertEqual(reference, set(boxes))
                self.assertAlmostEqual(sum(size for _, size in work.unique_transport_blocks(len(blocks))), work.image_size_mb)
        self.assertEqual(len(tiling("spot", 16).tiles()), 4)
        self.assertAlmostEqual(sum(b.data_size_mb for b in tiling("fine", 4).tiles()), 949.782528)

    def test_object_truth_and_output_bytes_do_not_depend_on_granularity(self):
        a, b = fixture("fine", 1701, 4), fixture("fine", 1701, 16)
        self.assertEqual(a["global_objects"], b["global_objects"])
        self.assertEqual(a["windows"], b["windows"])

    def test_constellations_share_epoch_and_have_separate_roles(self):
        for tier, ns, nc in (("S",12,12),("M",24,12),("L",48,24),("XL",96,48)):
            tles, sources, rings = constellation(tier)
            self.assertEqual((sources, len(tles)), (ns, ns+nc))
            self.assertTrue(all(a >= ns and b >= ns for a,b in rings))
            self.assertEqual(len({line1[18:32] for line1,_ in tles}), 1)
            self.assertEqual(len(set(line1[2:7] for line1,_ in tles)), ns+nc)

    def test_mixed_arrivals_raw_four_block_conversion_and_compute_work(self):
        events = []
        for mode in ("spot", "fine"):
            work = tiling(mode, 4)
            events.append(dict(release_step=0, source_sat_id=0, image_tiling=asdict(work),
                               window_result_bytes=[28]*sum(len(t.windows_xyxy) for t in work.tiles())))
        env = make_env(image_tiling=asdict(tiling("fine",4)), scheduled_task_arrivals=events,
                       mem_capacity_gb=10, vram_capacity_gb=2, vram_base_gb=1, vram_alpha_per_mb=0,
                       compute_rate=1/.6, onboard_compute_profile=deepcopy(PROFILE),
                       compute_service_mode="work_conserving")
        self.addCleanup(env.close)
        env._task_arrivals(0)
        pipeline = CandidatePipeline(env)
        env._dist_pipeline = pipeline
        pipeline.route_unique = lambda links: None
        first, second = list(env.tasks.values())
        self.assertEqual((first.image_size_mb, second.image_size_mb), (200,800))
        self.assertEqual(sum(env.tiles[t].compute_cost for t in first.tile_ids), 9)
        self.assertEqual(sum(env.tiles[t].compute_cost for t in second.tile_ids), 64)
        pipeline.apply_actions([RawImageAction(tid, ActionType.OFFLOAD) for tid in second.tile_ids], {})
        self.assertEqual(sum(env.tiles[t].data_size_mb for t in second.tile_ids), 800)
        pipeline.apply_actions([Action(tid, ActionType.LOCAL) for tid in first.tile_ids], {})
        for step in range(20):
            env.time = step
            pipeline.advance_compute()
        self.assertEqual(sum(env.tiles[t].completed_compute_windows for t in first.tile_ids), 9)
        self.assertTrue(all(env.tiles[t].state == TileState.COMPUTED for t in first.tile_ids))

    def test_local_view_cannot_read_unadvertised_queue(self):
        env = make_env(num_sats=3)
        self.addCleanup(env.close)
        add_task(env)
        state = env.export_state()
        plane = ControlPlane([0])
        before = plane.view(state, 0, 2)
        state.satellites[2]["committed_compute_s"] = 999999
        next(iter(state.ground_stations.values()))["committed_compute_s"] = 99999
        after = plane.view(state, 0, 2)
        self.assertEqual(before, after)
        actions = ThreePlanChoice(3).select_actions(before)
        self.assertTrue(actions)
        self.assertEqual(actions, ThreePlanChoice(3).select_actions(after))

    def test_control_bytes_share_capacity_and_deliver_after_step(self):
        env = make_env(dt=.1)
        self.addCleanup(env.close)
        plane = ControlPlane([0])
        env.control_plane = plane
        payload = {"sender": 1}
        message = Advertisement((1,0), payload, .1, .1, 0)
        plane.messages = [message]
        env.topology.links["0-1"].bandwidth_mbps = 1
        env._dist_pipeline.advance_transfers(env.topology.links)
        self.assertAlmostEqual(message.remaining_mb, .1-.0125)
        self.assertAlmostEqual(plane.stats["hop_bytes_sent"], 12500)
        self.assertEqual(plane.delivered[0], {})

    def test_control_and_data_on_same_direction_receive_fair_shares(self):
        env = make_env(num_sats=3, dt=.1)
        self.addCleanup(env.close)
        env.topology.links["1-2"] = Link(1, 2, True, 1, 0)
        tile = add_task(env, src=2, size=.1)[0]
        env._dist_pipeline.apply_actions([Action(tile.tile_id, ActionType.OFFLOAD, target_sat_id=1)],
                                         env.topology.links)
        plane = ControlPlane([0])
        env.control_plane = plane
        message = Advertisement((2,1,0), {"sender":2}, .1, .1, 0)
        plane.messages = [message]
        env._dist_pipeline.advance_transfers(env.topology.links)
        self.assertAlmostEqual(env.transfers[0].remaining_mb, .1-.00625)
        self.assertAlmostEqual(message.remaining_mb, .1-.00625)
        self.assertAlmostEqual(env.metrics.network_sent_mb["isl:2->1"], .0125)

    def test_capture_time_is_enforced_inside_topology_sample_interval(self):
        env = make_env(dt=.1, topology_update_steps=100)
        self.addCleanup(env.close)
        env.cfg.topology = {**env.cfg.topology, "acquisition_delay_s":5}
        env.topology.links["0-1"].up = False
        env.topology.links["0-1"].acquisition_ready_s = 5
        self.assertFalse(env._get_links(49)["0-1"].up)
        self.assertTrue(env._get_links(50)["0-1"].up)

    def test_merge_queue_serializes_simultaneous_images(self):
        env = make_env(serialized_aggregation=True, aggregation_delay_s=2)
        self.addCleanup(env.close)
        first, second = add_task(env)[0], add_task(env)[0]
        for tile in (first, second):
            tile.state = TileState.DONE
            tile.timestamps.end_downlink = 1
            env._check_task_done(tile.parent_task_id)
        env._advance_task_delivery(3)
        self.assertEqual(env.tasks[first.parent_task_id].completion_time, 3)
        self.assertIsNone(env.tasks[second.parent_task_id].completion_time)
        env._advance_task_delivery(5)
        self.assertEqual(env.tasks[second.parent_task_id].completion_time, 5)

    def test_raw_image_conversion_is_atomic_when_some_blocks_grow(self):
        work = asdict(tiling("slea",16))
        env = make_env(image_tiling=work, mem_capacity_gb=10,
                       scheduled_task_arrivals=[dict(release_step=0, source_sat_id=0)])
        self.addCleanup(env.close)
        env._task_arrivals(0)
        sat = env.satellites[0]
        # 模拟其余图像已经占用大部分缓存，当前图整体变小但首块会变大。
        sat.mem_capacity_gb = sat.mem_used_gb + .001
        pipeline = CandidatePipeline(env)
        pipeline.route_unique = lambda links: None
        commands = [RawImageAction(tid, ActionType.OFFLOAD) for tid in env.tasks["task_0"].tile_ids]
        pipeline.apply_actions(commands,{})
        self.assertAlmostEqual(sum(t.data_size_mb for t in env.tiles.values()),1800)
        self.assertAlmostEqual(sat.mem_used_gb,1.8)


if __name__ == "__main__":
    unittest.main()
