"""逐刷新网格用原执行器核验下界时间图覆盖；只构造几何，不调度任务。"""
from bisect import bisect_right
from collections import defaultdict
from copy import deepcopy
import gzip
import json
import math
from pathlib import Path
import time

from analysis import run_teacher_analysis_20261111 as a,run_teacher_bounds_20261111 as b


def coverage(intervals,start,end):
    starts=[x[0] for x in intervals]
    index=bisect_right(starts,start+1e-8)-1
    return index>=0 and intervals[index][1]+1e-8>=end


def audit_day(path):
    from analysis.dynamic_granularity_v2 import GranularityEnv
    from sim.config import config_from_dict
    started=time.time();p=a.read(path);result=a.read(a.ROOT/'bounds'/f"{p['date']}.json")
    assert a.sha(result['contacts_path'])==result['contacts_sha256']
    records=json.loads(gzip.decompress(Path(result['contacts_path']).read_bytes()))
    intervals=defaultdict(list)
    for source,target,start,end in records:
        target=tuple(target) if isinstance(target,list) else target
        intervals[source,target].append((start,end))
    for values in intervals.values():values.sort()
    config=deepcopy(p['config']);config['scheduled_task_arrivals']=[]
    env=GranularityEnv(config_from_dict(config));cfg=env.cfg
    isl_checked=ground_checked=0
    try:
        for step in range(0,cfg.sim_steps,cfg.topology_update_steps):
            start=step*cfg.dt;end=min((step+cfg.topology_update_steps)*cfg.dt,cfg.sim_steps*cfg.dt)
            links=env._get_links(step)
            for link in links.values():
                # finite ready表示原scalar几何/终端匹配选中，含尚在建链等待的边。
                if not math.isfinite(link.acquisition_ready_s):continue
                for edge in ((link.i,link.j),(link.j,link.i)):
                    assert coverage(intervals[edge],start,end),(p['date'],step,edge)
                    isl_checked+=1
            env.vis_cache.refresh(step,env.topology.sat_recs,env.topology.t0,
                                  env.ground_station_objs,env.ground_stations)
            for sid,stations in env.vis_cache.visible_gs_by_sat.items():
                for gid in stations:
                    assert coverage(intervals[sid,('ground',gid)],start,end),(p['date'],step,sid,gid)
                    ground_checked+=1
            if step%(50*cfg.topology_update_steps)==0:
                a.write(a.ROOT/'geometry_audit_progress.json',dict(date=p['date'],step=step,
                    total_steps=cfg.sim_steps,wall_seconds=time.time()-started),exclusive=False)
        return dict(date=p['date'],status='PASS',grid_samples=math.ceil(cfg.sim_steps/cfg.topology_update_steps),
                    directed_isl_grid_edges_checked=isl_checked,ground_grid_edges_checked=ground_checked,
                    wall_seconds=time.time()-started,graph_sha256=result['contacts_sha256'])
    finally:env.close()


def main():
    started=time.time();m=b.verify_manifest()
    registration=a.read(a.ROOT/'PROOF_AUDIT_REGISTRATION.json')
    assert a.sha(__file__)==registration['source_sha256']
    rows=[audit_day(path) for path in m['protocols']]
    b.verify_manifest()
    a.write(a.ROOT/'GEOMETRY_EXECUTOR_AUDIT.json',dict(status='PASS',at=a.now(),days=rows,
        wall_seconds=time.time()-started,source_sha256=a.sha(__file__),
        scope='All original scalar topology refresh grids and original ground visibility; no simulation steps or actions'))
    print({'status':'PASS','grid_samples':sum(r['grid_samples'] for r in rows),
           'wall_seconds':time.time()-started})


if __name__=='__main__':main()
