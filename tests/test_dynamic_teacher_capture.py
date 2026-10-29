import unittest
from analysis.run_dynamic_teachers import capture_observations
from analysis.dynamic_granularity_v2 import DynamicThreePlan
from analysis.local_information_baselines import LocalInformationPlane
from sim.control import ControlPlane
from test_dynamic_granularity_v2 import fixture


class CaptureTest(unittest.TestCase):
    def test_capture_is_passive_across_repacking_and_execution(self):
        a,b=fixture(),fixture();self.addCleanup(a.close);self.addCleanup(b.close)
        a.control_plane=ControlPlane([0]);b.control_plane=LocalInformationPlane([0]);b.control_plane.history_hops=2
        pa,pb=DynamicThreePlan(a),DynamicThreePlan(b);captured={}
        for _ in range(80):
            sa,sb=a.export_state(),b.export_state()
            a.control_plane.enqueue(sa);b.control_plane.enqueue(sb)
            observations=capture_observations(b,sb,b.control_plane,captured)
            aa,bb=pa.select_actions(sa),pb.select_actions(sb)
            self.assertEqual(aa,bb)
            captured.update({k:v for k,v in observations.items() if k in pb.decided})
            a.step(aa);b.step(bb)
        self.assertEqual(a.tasks,b.tasks)
        self.assertEqual(a.tiles,b.tiles)
        self.assertEqual(a.metrics.network_sent_mb,b.metrics.network_sent_mb)
        self.assertEqual(a.metrics.compute_busy_time,b.metrics.compute_busy_time)
        self.assertEqual(pa.decisions,pb.decisions)
        self.assertEqual(set(captured),{'task_0'})


if __name__=='__main__':unittest.main()
