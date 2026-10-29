import unittest
from copy import deepcopy
from analysis.scale_feature_audit_20261027 import add_ranges, merge_ranges, FeatureAudit


def observation():
    rows=[[0.]*14 for _ in range(4)]
    rows[0][0]=1.;rows[0][1]=1.;rows[1][1]=0.;rows[2][1]=1.
    return dict(task_id='x',source=0,block_count=16,node_ids=[0,'ground_block',3,-1],
        node_valid=[True,True,True,False],nodes=rows,image=[1.,1.,1.,0.,1.,1.,.5,.125])


class Features(unittest.TestCase):
    def test_padding_and_roles(self):
        obs=observation();obs['nodes'][-1]=[999.]*14;r={};add_ranges(r,obs)
        self.assertEqual(set(r),{'g16/image','g16/source/known','g16/ground/unknown','g16/compute/known'})
        self.assertEqual(r['g16/compute/known']['maximum'][3],0)

    def test_tolerance_categories_and_decision_denominator(self):
        obs=observation();r={};add_ranges(r,obs);a=FeatureAudit(r)
        small=deepcopy(obs);small['nodes'][2][3]=.5e-6;a.observe(small,{16:dict(before_topk=12,after_topk=8)})
        big=deepcopy(obs);big['nodes'][2][3]=2e-6;big['image'][5]=0
        a.observe(big,{16:dict(before_topk=0,after_topk=0)})
        out=a.result();self.assertEqual(out['outside_decisions'],1);self.assertEqual(out['new_category_decisions'],1)
        self.assertEqual(out['outside_cells'],1);self.assertEqual(out['outside_decision_fraction'],.5)
        self.assertEqual(out['candidates']['g16/before_topk']['P50'],6)

    def test_missing_unknown_stratum(self):
        obs=observation();r={};add_ranges(r,obs);obs['nodes'][2][1]=0
        a=FeatureAudit(r);a.observe(obs,{})
        self.assertEqual(a.result()['groups']['g16/compute/unknown']['missing_reference_rows'],1)
        self.assertGreater(a.result()['outside_cells'],0)

    def test_dynamic_merge_preserves_parent(self):
        old=observation();parent={};add_ranges(parent,old)
        changed=deepcopy(old);changed['nodes'][2][3]=3
        four=deepcopy(changed);four['block_count']=4
        child={};add_ranges(child,dict(variants=[four,changed]))
        merged=merge_ranges(parent,child)
        self.assertEqual(merged['g16/compute/known']['minimum'][3],0)
        self.assertEqual(merged['g16/compute/known']['maximum'][3],3)
        self.assertEqual(merged['g4/compute/known']['minimum'][3],3)
        self.assertEqual(parent['g16/compute/known']['maximum'][3],0)


if __name__=='__main__':unittest.main()
