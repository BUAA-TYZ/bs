import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
from concurrent.futures import Future

from analysis import run_ground_distributed_20261030 as d
from analysis.verify_ground_devbox_preflight_20261030 import compare


class DistributedTests(unittest.TestCase):
    def test_sixty_jobs_unique_and_balanced(self):
        run=dict(methods=[f'm{i}' for i in range(20)],protocols=['d1','d2','d3'])
        jobs=d.make_assignments(run)
        self.assertEqual([len(jobs[h]) for h in d.HOSTS],[20,20,20])
        flat=[tuple(x) for rows in jobs.values() for x in rows]
        self.assertEqual(len(set(flat)),60)
        for rows in jobs.values():
            self.assertEqual(len({x[2] for x in rows}),20)
            self.assertEqual({x[1] for x in rows},{'d1','d2','d3'})

    def test_cross_platform_masks_and_actions_are_exact(self):
        with self.assertRaises(AssertionError):
            compare({'observation':[True]},{'observation':[False]})
        with self.assertRaises(AssertionError):
            compare({'positions':[1]},{'positions':[2]})
        compare({'image_prob':[0.5]},{'image_prob':[0.5000001]})
        with self.assertRaises(AssertionError):
            compare({'image_prob':[0.5]},{'image_prob':[0.51]})

    def test_failed_batch_drains_without_next_batch(self):
        class Pool:
            calls=[]
            def __init__(self,**kwargs): pass
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def submit(self,fn,job):
                self.calls.append(job); f=Future()
                f.set_result(dict(status='FAILED' if len(self.calls)==2 else 'PASS'))
                return f
        with tempfile.TemporaryDirectory() as tmp,patch.object(d,'DIST',Path(tmp)),patch.object(d,'ProcessPoolExecutor',Pool):
            rows,ok=d.run_batches('devbox1',list(range(20)),6)
        self.assertFalse(ok);self.assertEqual(len(rows),6);self.assertEqual(len(Pool.calls),6)

    def test_launch_registration_is_exclusive(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'launch.json';d.exclusive(p,{'first':True})
            with self.assertRaises(FileExistsError):d.exclusive(p,{'second':True})
            self.assertEqual(d.r.read(p),{'first':True})

    def test_stop_control_prevents_dispatch(self):
        class Pool:
            def __init__(self,**kwargs): pass
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def submit(self,*args): raise AssertionError('must not dispatch')
        with tempfile.TemporaryDirectory() as tmp,patch.object(d,'DIST',Path(tmp)),patch.object(d,'ProcessPoolExecutor',Pool):
            d.exclusive(Path(tmp)/'devbox1/STOP_AFTER_BATCH.json',{'reason':'peer failure'})
            rows,ok=d.run_batches('devbox1',list(range(20)),6)
        self.assertFalse(ok);self.assertEqual(rows,[])

    def test_incomplete_transfer_is_not_counted(self):
        with tempfile.TemporaryDirectory() as tmp,patch.object(d.r,'ROOT',Path(tmp)):
            result=Path(tmp)/'M_scaled/validation/day/method.json'
            d.exclusive(result,{'status':'PASS','stage':'M_scaled'})
            d.exclusive(result.with_suffix('.progress.json'),dict(status='PASS',path=str(result),sha256='wrong'))
            self.assertEqual(d.finished(),[])
            d.r.write_json(result.with_suffix('.progress.json'),dict(status='PASS',path=str(result),sha256=d.r.sha(result)))
            self.assertEqual(len(d.finished()),1)


if __name__=='__main__':unittest.main()
