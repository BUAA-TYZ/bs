from collections import Counter
from copy import deepcopy
from dataclasses import replace
import math
import unittest
from unittest.mock import patch

from analysis.dynamic_granularity_v2 import GranularityEnv, DynamicThreePlan, preview
from analysis.sar_three_plan import RawImageAction, CandidateEnv, ThreePlanChoice
from analysis.run_sar_frozen import invariants
from sim.entities import Action, ActionType, TileState
from sim.workload import ImageTiling
from test_simulation import make_env, FakeTopology, FakeVisibility


TILING = dict(width_px=7, height_px=7, bands=1, bits_per_sample=8, window_px=4,
    stride_px=1, windows_per_tile_axis=1, compute_cost_per_window=1,
    result_size_mb_per_window=.000001, deduplicate_within_tile=True, requested_num_tiles=16)


def fixture(kind=GranularityEnv, count=16):
    tiling = dict(TILING, requested_num_tiles=count)
    event = dict(release_step=0, source_sat_id=0, image_tiling=tiling,
                 window_result_bytes=list(range(1,17)), aggregation_delay_s=0.)
    tmp = make_env(pipeline_mode='distributed', image_tiling=tiling, execution_commitment=True,
        sar_source_isolation=True, task_source_sat_ids=[0], scheduled_task_arrivals=[event],
        vram_base_gb=.01, vram_alpha_per_mb=0., input_relay_max_hops=2,
        onboard_compute_profile={'service_s': {'1': 1.}, 'workspace_gb': {'1': .1}},
        compute_service_mode='work_conserving', relay_contact_guard=False)
    cfg = tmp.cfg
    tmp.close()
    with patch('sim.env.TopologyModel', FakeTopology), patch('sim.env.VisibilityCache', FakeVisibility):
        env = kind(cfg)
    env.vis_cache.windows_at = lambda sid,t: {'gs': (0,1000)}
    env.step([])
    return env


class DynamicTests(unittest.TestCase):
    def test_layout_exact_windows_and_overlap_bytes(self):
        a = ImageTiling.from_dict(TILING)
        b = replace(a, requested_num_tiles=4)
        self.assertEqual(Counter(w for t in a.tiles() for w in t.windows_xyxy),
                         Counter(w for t in b.tiles() for w in t.windows_xyxy))
        self.assertEqual(sum(len(t.windows_xyxy) for t in b.tiles()),16)
        self.assertLess(sum(t.data_size_mb for t in b.tiles()),sum(t.data_size_mb for t in a.tiles()))
        self.assertGreater(sum(t.data_size_mb for t in b.tiles()),b.image_size_mb)
        small = replace(a,width_px=6,height_px=6)
        self.assertEqual(len(small.tiles()),4)

    def test_preview_matches_commit_and_memory_and_actual_results(self):
        env=fixture(); self.addCleanup(env.close)
        state=env.export_state(); old=dict(env.tiles)
        view=preview(env,state,'task_0',4)
        self.assertEqual(env.tiles,old)
        env.repack('task_0',4)
        actual=env.export_state()
        self.assertEqual(set(view.tiles),set(actual.tiles))
        for tid in view.tiles:
            for k in ('data_size_mb','data_size_gb','compute_cost','result_size_mb','unique_input_size_mb'):
                self.assertAlmostEqual(view.tiles[tid][k],actual.tiles[tid][k])
        self.assertAlmostEqual(sum(t.actual_result_size_mb for t in env.tiles.values()),sum(range(1,17))/1e6)
        self.assertAlmostEqual(env.satellites[0].mem_used_gb,sum(t.data_size_mb for t in env.tiles.values())/1000)
        self.assertEqual(env.metrics.total_tiles,4)

    def test_four_local_offload_and_unique_ground_complete(self):
        for mode in ('local','offload','raw'):
            env=fixture(); self.addCleanup(env.close);env.repack('task_0',4)
            tids=env.tasks['task_0'].tile_ids
            commands=([RawImageAction(t,ActionType.OFFLOAD,raw_hops=2) for t in tids] if mode=='raw' else
                [Action(t,ActionType.LOCAL) if mode=='local' else Action(t,ActionType.OFFLOAD,target_sat_id=1) for t in tids])
            for step in range(70):env.step(commands if step==0 else [])
            self.assertIsNotNone(env.tasks['task_0'].completion_time,mode)
            self.assertTrue(invariants(env)['passed'],mode)
            self.assertTrue(all(t.state==TileState.DONE for t in env.tiles.values()))
            if mode!='raw':
                self.assertEqual(sum(t.completed_compute_windows for t in env.tiles.values()),16)
            else:
                self.assertTrue(math.isclose(sum(t.data_size_mb for t in env.tiles.values()),49/1e6))

    def test_no_repack_after_commit(self):
        env=fixture();self.addCleanup(env.close)
        env.step([Action(t,ActionType.LOCAL) for t in env.tasks['task_0'].tile_ids])
        with self.assertRaises(AssertionError):env.repack('task_0',4)

    def test_fixed16_executor_identity(self):
        a,b=fixture(CandidateEnv),fixture(GranularityEnv)
        self.addCleanup(a.close);self.addCleanup(b.close)
        pa,pb=ThreePlanChoice(2),ThreePlanChoice(2)
        for _ in range(60):
            aa,bb=pa.select_actions(a.export_state()),pb.select_actions(b.export_state())
            self.assertEqual(aa,bb)
            a.step(aa);b.step(bb)
        self.assertEqual(a.tasks,b.tasks);self.assertEqual(a.tiles,b.tiles)
        self.assertEqual(a.metrics.network_sent_mb,b.metrics.network_sent_mb)

    def test_dynamic_quotes_and_execution_consistent(self):
        env=fixture();self.addCleanup(env.close);p=DynamicThreePlan(env)
        actions=p.select_actions(env.export_state())
        self.assertEqual(set(p.decisions[0]['quotes_seconds']),{'local_4','local_16','cgs_4','cgs_16','ground'})
        self.assertTrue(set(a.tile_id for a in actions)<=set(env.tiles))
        for i in range(70):env.step(actions if i==0 else p.select_actions(env.export_state()))
        self.assertTrue(invariants(env)['passed'])
        self.assertIsNotNone(env.tasks['task_0'].completion_time)

    def test_hidden_result_truth_cannot_change_quotes(self):
        a,b=fixture(),fixture()
        self.addCleanup(a.close);self.addCleanup(b.close)
        b.cfg.scheduled_task_arrivals[0]['window_result_bytes']=[99999]*16
        pa,pb=DynamicThreePlan(a),DynamicThreePlan(b)
        aa,bb=pa.select_actions(a.export_state()),pb.select_actions(b.export_state())
        self.assertEqual(aa,bb)
        self.assertEqual(pa.decisions[0]['quotes_seconds'],pb.decisions[0]['quotes_seconds'])

    def test_passive_traffic_preserves_physics_and_conserves_bytes(self):
        from analysis.run_dynamic_heuristics_v2 import TrafficAudit, resources, difference
        from sim.control import ControlPlane
        a,b=fixture(),fixture()
        self.addCleanup(a.close);self.addCleanup(b.close)
        for e in (a,b):e.control_plane=ControlPlane([0])
        audit=TrafficAudit(b);self.addCleanup(audit.close)
        initial=audit.snapshot()
        commands=[Action(t,ActionType.OFFLOAD,target_sat_id=1) for t in a.tasks['task_0'].tile_ids]
        for i in range(70):
            a.step(deepcopy(commands) if i==0 else [])
            b.step(deepcopy(commands) if i==0 else [])
        self.assertEqual(a.tasks,b.tasks);self.assertEqual(a.tiles,b.tiles)
        self.assertEqual(a.metrics.network_sent_mb,b.metrics.network_sent_mb)
        result=resources(difference(initial,audit.snapshot()),{1},70,b.cfg)
        self.assertGreater(result['raw_isl_bytes'],0)
        self.assertGreater(result['result_ground_bytes'],0)

    def test_registered_configs_parse_and_keep_fixed16_exact(self):
        from analysis.run_dynamic_heuristics_v2 import BC, read, METHODS, configuration
        from sim.config import config_from_dict
        for path in read(BC/'manifest.json')['validation']:
            protocol = read(path)
            self.assertEqual(configuration(protocol, 'v3_16'), protocol['config'])
            for method in METHODS:
                cfg = config_from_dict(configuration(protocol, method))
                expected = 4 if method.endswith('_4') else 16
                self.assertEqual(cfg.num_tiles, expected)
                self.assertTrue(all(e['image_tiling']['requested_num_tiles']==expected
                                    for e in cfg.scheduled_task_arrivals))


if __name__=='__main__':unittest.main()
