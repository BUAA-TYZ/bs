from collections import Counter
from copy import deepcopy
from dataclasses import replace
import math
import unittest

from analysis import nonuniform_granularity_20261111 as nu
from analysis.dynamic_granularity_v2 import DynamicThreePlan, GranularityEnv
from analysis.sar_three_plan import Calendar
from analysis.run_sar_frozen import invariants
from sim.entities import Action, ActionType, TileState
from sim.workload import ImageTiling, TransportComponent
from test_dynamic_granularity_v2 import fixture, TILING


class ArbitraryGroupingTests(unittest.TestCase):
    def setUp(self):
        self.workload = ImageTiling.from_dict(TILING)

    def test_arbitrary_partition_preserves_every_window_and_work(self):
        labels = tuple(i%3 for i in range(16))
        descriptors = nu.grouped_layout(self.workload, labels)
        self.assertEqual(len(descriptors),3)
        self.assertEqual(Counter(b for t in descriptors for b in t.windows_xyxy),Counter(nu.windows(self.workload)))
        self.assertEqual(sum(t.compute_cost for t in descriptors),16)
        self.assertAlmostEqual(sum(t.result_size_mb for t in descriptors),16e-6)

    def test_all_non_square_block_counts(self):
        for n in (2,3,5,6,7,8,10,11,12,13,14,15):
            descriptors=nu.grouped_layout(self.workload,tuple(i%n for i in range(16)))
            self.assertEqual(len(descriptors),n)
            self.assertTrue(all(t.windows_xyxy for t in descriptors))

    def test_invalid_partition_rejected(self):
        for labels in ([0]*15,[0]*17,[1]*16,[False]*16,[0]*15+[2],[0]*15+[-1]):
            with self.assertRaises(ValueError):nu.grouped_layout(self.workload,labels)
        large=replace(self.workload,width_px=8,height_px=8)
        with self.assertRaises(ValueError):nu.grouped_layout(large,tuple(i%17 for i in range(25)))

    def test_nonrectangular_union_not_bounding_box_or_sum(self):
        boxes=((0,0,4,4),(3,0,7,4),(0,3,4,7))
        self.assertEqual(nu.union_area(boxes),40)
        self.assertEqual(nu.union_bytes(self.workload,boxes),40)
        self.assertLess(nu.union_area(boxes),49)
        self.assertLess(nu.union_area(boxes),48)

    def test_component_union_native_grid_and_bit_packing(self):
        w=replace(self.workload,transport_components=(TransportComponent('a',2,3,3),TransportComponent('b',1,1,12)),transport_compression_ratio=2)
        boxes=((0,0,4,4),(3,0,7,4),(0,3,4,7))
        expected=0
        for c in w.transport_components:
            pixels={(x,y) for a,b,d,e in boxes for x in range(a//c.downsample,math.ceil(d/c.downsample))
                    for y in range(b//c.downsample,math.ceil(e/c.downsample))}
            expected+=math.ceil(len(pixels)*c.bands*c.bits_per_sample/8)
        self.assertEqual(nu.union_bytes(w,boxes),expected/2)

    def test_same_block_count_different_mapping_preserved(self):
        counts=[5,4,7]
        variants=[nu.mapping(self.workload,counts,m) for m in nu.MAPPINGS]
        self.assertEqual(len(set(variants)),3)
        for labels in variants:self.assertEqual([labels.count(i) for i in range(3)],counts)
        choice=nu.choose_mapping(self.workload,counts)
        self.assertEqual(choice['bytes'],min(choice['candidate_bytes'].values()))
        self.assertLessEqual(choice['bytes'],choice['row_snake_bytes'])
        self.assertGreater(len(set(choice['candidate_bytes'].values())),1)
        wide=replace(self.workload,width_px=8,height_px=6)
        self.assertEqual(nu.choose_mapping(wide,[5,4,6])['mapping'],'column_snake')
        for labels in variants:
            descriptors=nu.grouped_layout(self.workload,labels)
            self.assertEqual(len(descriptors),3)

    def test_mapping_tie_and_long_axis_rules(self):
        self.assertEqual(nu.choose_mapping(self.workload,[16])['mapping'],'row_snake')
        tall=replace(self.workload,width_px=6,height_px=8)
        labels=nu.mapping(tall,[5,10],'long_axis_stripes')
        self.assertEqual([b[0] for b,l in zip(nu.windows(tall),labels) if l==0],[0]*5)
        wide=replace(tall,width_px=8,height_px=6)
        labels=nu.mapping(wide,[5,10],'long_axis_stripes')
        self.assertEqual([b[1] for b,l in zip(nu.windows(wide),labels) if l==0],[0]*5)

    def test_source_truth_binding_and_preview_same_count_different_mapping(self):
        for kind in nu.MAPPINGS:
            env=fixture(nu.NonuniformEnv);self.addCleanup(env.close)
            labels=nu.mapping(self.workload,[3,5,8],kind)
            state=env.export_state();before=deepcopy(env.tiles)
            view=nu.group_preview(env,state,'task_0',labels)
            self.assertEqual(env.tiles,before)
            env.repack_groups('task_0',labels)
            actual=env.export_state()
            self.assertEqual(set(view.tiles),set(actual.tiles))
            for tid in view.tiles:
                for key in ('data_size_mb','data_size_gb','compute_cost','result_size_mb','unique_input_size_mb'):
                    self.assertEqual(view.tiles[tid][key],actual.tiles[tid][key])
                tile=env.tiles[tid]
                expected=sum(i+1 for i,b in enumerate(nu.windows(self.workload)) if b in tile.windows_xyxy)/1e6
                self.assertAlmostEqual(tile.actual_result_size_mb,expected)
            self.assertAlmostEqual(sum(t.actual_result_size_mb for t in env.tiles.values()),136e-6)
            self.assertAlmostEqual(view.satellites[0]['mem_remaining_gb'],actual.satellites[0]['mem_remaining_gb'])
            env.repack_groups('task_0',labels)
            with self.assertRaises(ValueError):env.repack_groups('task_0',[0]*16)

    def test_arbitrary_groups_execute_locally_and_remotely(self):
        for target in (0,1):
            env=fixture(nu.NonuniformEnv);self.addCleanup(env.close)
            env.repack_groups('task_0',tuple(i%3 for i in range(16)))
            actions=[Action(t,ActionType.LOCAL) if target==0 else Action(t,ActionType.OFFLOAD,target_sat_id=target) for t in env.tasks['task_0'].tile_ids]
            for i in range(80):env.step(actions if i==0 else [])
            self.assertTrue(invariants(env)['passed'])
            self.assertIsNotNone(env.tasks['task_0'].completion_time)
            self.assertTrue(all(t.state==TileState.DONE for t in env.tiles.values()))
            self.assertEqual(sum(t.completed_compute_windows for t in env.tiles.values()),16)

    def test_reject_repack_after_commit_and_memory_without_mutation(self):
        env=fixture(nu.NonuniformEnv);self.addCleanup(env.close)
        env.satellites[0].mem_capacity_gb=0
        before=deepcopy(env.tiles)
        with self.assertRaises(ValueError):env.repack_groups('task_0',[0]*16)
        self.assertEqual(env.tiles,before)
        self.assertEqual(env.granularity_choices,{})
        env.satellites[0].mem_capacity_gb=1
        env.step([Action(t,ActionType.LOCAL) for t in env.tasks['task_0'].tile_ids])
        with self.assertRaises(ValueError):env.repack_groups('task_0',[0]*16)


class AllocationTests(unittest.TestCase):
    def test_integer_bisection_conserves_windows_and_near_minimax(self):
        tables=[[0,1,2,3,4,5],[0,2,4,6,8,10]]
        counts,T,n=nu.allocate_counts(tables,5,10)
        self.assertEqual(sum(counts),5)
        optimum=min(max(tables[0][i],tables[1][5-i]) for i in range(6))
        self.assertLessEqual(max(tables[i][k] for i,k in enumerate(counts)),optimum+nu.TOLERANCE_S)
        self.assertLessEqual(abs(T-optimum),nu.TOLERANCE_S)
        self.assertLessEqual(n,nu.MAX_BISECTIONS)

    def test_no_capacity_and_deterministic_residual(self):
        self.assertIsNone(nu.allocate_counts([[0,math.inf],[0,math.inf]],1,10))
        self.assertEqual(nu.allocate_counts([[0,1,2],[0,1,2]],3,5)[0],[2,1])

    def test_block_calendar_does_not_change_quote_or_reservations(self):
        for offload in (False,True):
            env=fixture();self.addCleanup(env.close);state=env.export_state()
            a,b=Calendar(state),nu.BlockCalendar(state)
            actions=[Action(t,ActionType.OFFLOAD,target_sat_id=1) if offload else Action(t,ActionType.LOCAL) for t in state.tiles]
            self.assertEqual(a.quote(state,actions),b.quote(state,actions))
            self.assertEqual(a.busy,b.busy);self.assertEqual(a.free,b.free)
            self.assertEqual(a.plan.contacts,b.plan.contacts)
            if b.block_finishes:
                self.assertEqual(set(b.block_finishes),set(state.tiles))
                self.assertTrue(all(math.isfinite(v) for v in b.block_finishes.values()))
            else:
                self.assertIsNone(nu.BlockCalendar(state).quote(state,actions))

    def test_disabled_nu_exact_legacy_actions_and_execution(self):
        a,b=fixture(GranularityEnv),fixture(nu.NonuniformEnv)
        self.addCleanup(a.close);self.addCleanup(b.close)
        pa,pb=DynamicThreePlan(a),nu.NonuniformThreePlan(b,False)
        for _ in range(80):
            aa,bb=pa.select_actions(a.export_state()),pb.select_actions(b.export_state())
            self.assertEqual(aa,bb);a.step(aa);b.step(bb)
        self.assertEqual(a.tasks,b.tasks);self.assertEqual(a.tiles,b.tiles)
        self.assertEqual(a.metrics.network_sent_mb,b.metrics.network_sent_mb)
        self.assertEqual(pa.decisions[0]['quotes_seconds'],pb.decisions[0]['quotes_seconds'])

    def test_hidden_truth_cannot_change_nu_proposal(self):
        a,b=fixture(nu.NonuniformEnv),fixture(nu.NonuniformEnv)
        self.addCleanup(a.close);self.addCleanup(b.close)
        b.cfg.scheduled_task_arrivals[0]['window_result_bytes']=[99999]*16
        ca,ma=nu.nu_candidate(a,a.export_state(),'task_0',Calendar(a.export_state()))
        cb,mb=nu.nu_candidate(b,b.export_state(),'task_0',Calendar(b.export_state()))
        self.assertIsNotNone(ca);self.assertEqual(ma,mb)
        self.assertEqual(ca['actions'],cb['actions'])

    def test_nu_policy_full_execution_and_observations(self):
        env=fixture(nu.NonuniformEnv);self.addCleanup(env.close)
        policy=nu.NonuniformThreePlan(env)
        for _ in range(80):env.step(policy.select_actions(env.export_state()))
        self.assertTrue(invariants(env)['passed'])
        self.assertIsNotNone(env.tasks['task_0'].completion_time)
        self.assertEqual(policy.decisions[0]['nu']['status'],'FEASIBLE')

    def test_two_processor_allocation_and_traced_quote(self):
        env=fixture(nu.NonuniformEnv);self.addCleanup(env.close)
        state=env.export_state()
        state=replace(state,links={k:{**v,'contact_windows':[(0,1000)]} for k,v in state.links.items()},
            ground_stations={k:{**v,'committed_compute_s':500} for k,v in state.ground_stations.items()})
        candidate,meta=nu.nu_candidate(env,state,'task_0',Calendar(state))
        self.assertIsNotNone(candidate,meta)
        self.assertGreaterEqual(len(meta['counts']),2)
        self.assertEqual(sum(meta['counts']),16)
        self.assertEqual(set(meta['allocation_prediction_s']),set(meta['calendar_prediction_s']))
        self.assertTrue(all(math.isfinite(v) for v in meta['calendar_prediction_s'].values()))
        view=nu.adjusted_view(state,nu.group_preview(env,state,'task_0',candidate['labels']),Calendar(state))
        plain=Calendar(state);plain.memory[0]=view.satellites[0]['mem_remaining_gb']
        traced=nu.BlockCalendar(state);traced.memory[0]=plain.memory[0]
        self.assertEqual(plain.quote(view,candidate['actions']),traced.quote(view,candidate['actions']))
        self.assertEqual(plain.busy,traced.busy);self.assertEqual(plain.plan.contacts,traced.plan.contacts)

    def test_committed_nu_targets_execute_and_finish_measurement(self):
        from analysis.run_nonuniform_gate1_20261111 import collect_blocks
        env=fixture(nu.NonuniformEnv);self.addCleanup(env.close)
        state=env.export_state()
        state=replace(state,links={k:{**v,'contact_windows':[(0,1000)]} for k,v in state.links.items()},
            ground_stations={k:{**v,'committed_compute_s':500} for k,v in state.ground_stations.items()})
        candidate,meta=nu.nu_candidate(env,state,'task_0',Calendar(state))
        env.repack_groups('task_0',candidate['labels'])
        policy=nu.NonuniformThreePlan(env)
        policy.nu_targets['task_0']=meta['targets']
        d=dict(task_id='task_0',selected='c_nu',time_seconds=env.time*env.cfg.dt,nu=meta)
        env.step(candidate['actions'])
        for _ in range(80):env.step(policy.select_actions(env.export_state()))
        rows=collect_blocks(env,[d])
        self.assertEqual(len(rows),len(meta['counts']))
        self.assertTrue(all(r['actual_finish_after_decision_s']>0 for r in rows))
        self.assertTrue(invariants(env)['passed'])
        self.assertIsNotNone(env.tasks['task_0'].completion_time)
        from analysis.report_nonuniform_gate1_20261111 import audit_group_geometry
        arrivals=deepcopy(env.cfg.scheduled_task_arrivals)
        arrivals[0]['image_tiling']['transport_components']=[dict(name='native',downsample=1,bands=1,bits_per_sample=8)]
        checked=audit_group_geometry(dict(blocks=rows,decisions=[d]),dict(config={'scheduled_task_arrivals':arrivals}))
        self.assertEqual(checked,len(rows))

    def test_report_metrics_use_full_denominator_and_error_sign(self):
        from analysis.report_nonuniform_gate1_20261111 import metrics,diagnostics
        p={'scored_task_ids':['a','b']}
        r=dict(outcomes=[dict(task_id='a',status='completed',latency_seconds=20,observed_age_seconds=600),
            dict(task_id='b',status='pending',latency_seconds=None,observed_age_seconds=600)],
            resources={'main':{'total_transmitted_bytes':5},'whole':{'total_transmitted_bytes':9}},
            decisions=[dict(task_id='a',selected='c_nu',nu=dict(status='FEASIBLE',counts=[1,1],
                mapping='column_snake',selected_bytes=8,row_snake_bytes=10))],
            blocks=[dict(task_id='a',actual_finish_after_decision_s=10,allocation_prediction_s=7,calendar_prediction_s=8),
                dict(task_id='a',actual_finish_after_decision_s=None,allocation_prediction_s=8,calendar_prediction_s=9)])
        v=metrics(r,p);self.assertEqual(v['P60'],.5);self.assertEqual(v['R600'],310);self.assertEqual(v['unfinished'],1)
        d=diagnostics([r],[p]);self.assertEqual(d['censored_blocks'],1);self.assertEqual(d['actual_spread_s']['n'],0)
        self.assertEqual(d['errors']['allocation_prediction_s']['signed_predicted_minus_actual']['median'],-3)
        self.assertEqual(d['errors']['calendar_prediction_s']['absolute']['P90'],2)


if __name__=='__main__':unittest.main()
