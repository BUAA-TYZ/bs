import copy
import unittest

from analysis.run_local_comparison_20261027 import policy_set, local_actions, NEW, RULES, configuration, read, BC
from sim.config import config_from_dict
from test_local_information_baselines import observation, deliver
from test_dynamic_granularity_v2 import fixture


class LocalComparisonTests(unittest.TestCase):
    def test_literature_policies_cannot_see_hidden_dynamic_truth(self):
        for method in ('local_cgs_16','local_heft_16'):
            state=observation(); changed=copy.deepcopy(state)
            for sid in (1,2,3):
                changed.satellites[sid].update(committed_compute_s=99999,mem_remaining_gb=0,
                    vram_remaining_gb=0,queue_len=999,isl_backlog_mb={0:9999},ground_backlog_mb={'gs':9999})
            changed.ground_stations['gs'].update(committed_compute_s=99999,queue_len=999,running=99)
            pa,aa=policy_set(method,{0,1},2); pb,ab=policy_set(method,{0,1},2)
            self.assertEqual(pa.view(state,0,2),pb.view(changed,0,2))
            self.assertEqual(local_actions(state,pa,aa),local_actions(changed,pb,ab))
            self.assertEqual(pa.ttl_s,15)

    def test_literature_view_uses_received_two_hop_sample_then_expires(self):
        for method in ('local_cgs_16','local_heft_16'):
            state=observation(); plane,_=policy_set(method,{0,1},2)
            state.satellites[2]['committed_compute_s']=17
            deliver(plane,state,hops=2); state.satellites[2]['committed_compute_s']=999
            self.assertEqual(plane.view(state,0,2).satellites[2]['committed_compute_s'],17)
            state.time=16/state.config['dt']
            self.assertEqual(plane.view(state,0,2).satellites[2]['committed_compute_s'],0)
            deliver(plane,state,generated=16,received=16,hops=3)
            self.assertFalse(plane.view(state,0,2).satellites[2]['dynamic_known'])

    def test_registered_granularity_and_original_rule_semantics(self):
        for p in read(BC/'manifest.json')['validation']:
            p=read(p)
            for rule in RULES:
                self.assertEqual(configuration(p,rule+'_16'),p['config'])
                cfg=config_from_dict(configuration(p,rule+'_4'))
                self.assertEqual(cfg.image_tiling['requested_num_tiles'],4)
                plane,_=policy_set(rule+'_4',set(cfg.task_source_sat_ids),cfg.relay_max_hops)
                self.assertEqual(plane.history_hops,int(rule[-1]))
                self.assertEqual(plane.estimator,'history' if rule.startswith('history') else 'zero')

    def test_new_policies_run_on_real_executor_and_keep_source_separation(self):
        for method in NEW:
            env=fixture(count=4 if method.endswith('_4') else 16)
            try:
                plane,policies=policy_set(method,{0},2); env.control_plane=plane
                for _ in range(30):
                    state=env.export_state(); plane.enqueue(state)
                    actions=local_actions(state,plane,policies)
                    self.assertTrue(all(env.tiles[a.tile_id].parent_task_id=='task_0' for a in actions))
                    env.step(actions)
                self.assertTrue(all(not t.input_route or len(t.input_route)-1<=2 for t in env.tiles.values()))
            finally: env.close()


if __name__=='__main__': unittest.main()
