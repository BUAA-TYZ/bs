"""跨机只读报价一致性及速度预检；不运行仿真、不改变候选。"""
import argparse
import json
import platform
import sys
import time

from analysis import run_cgs_gap_20261111 as r
from analysis import cgs_gap_20261111 as core


def check(path):
    m=r.verify_manifest('replay');pilot=r.read(path);row=pilot['snapshot'];plan=pilot['plan']
    assert plan['status']=='ELIGIBLE'
    snapshot=core.load_snapshot(row['path'],row['sha256'])
    cgs=[tuple(p) if isinstance(p,list) else p for p in plan['cgs']]
    actual=core.quote(snapshot,cgs,plan['ordered'])
    assert abs(actual-plan['cgs_quote'])<1e-8
    targets=[tuple(p) if isinstance(p,list) else p for p in plan['positions']]
    wanted={record['index']:record for record in pilot['records']}
    elapsed=0.;errors=[]
    for index,assignment in enumerate(core.assignments(plan['blocks'],len(targets))):
        if index not in wanted:continue
        started=time.perf_counter();value=core.quote(snapshot,[targets[i] for i in assignment],plan['ordered'])
        elapsed+=time.perf_counter()-started
        expected=wanted[index]['quote']
        if expected is None:assert value is None
        else:
            assert value is not None and abs(value-expected)<1e-8
            errors.append(abs(value-expected))
    return dict(status='PASS',at=r.now(),python=sys.version,platform=platform.platform(),machine=platform.machine(),
        checked_hashes=len(m['hashes']),snapshot_sha256=row['sha256'],pilot_sha256=r.sha(path),
        source_sha256=r.sha(__file__),quotes_checked=len(wanted),seconds_per_quote=elapsed/len(wanted),
        max_absolute_error_s=max(errors,default=0.),sample=row['sample']['id'])


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('pilot');args=p.parse_args()
    print(json.dumps(check(args.pilot)))
