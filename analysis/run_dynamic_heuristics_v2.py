"""三验证日粒度面板；入口和全部输入冻结后才运行。"""
import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import sys
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import numpy as np
import torch
from analysis.dynamic_granularity_v2 import GranularityEnv, DynamicThreePlan
from analysis.sar_three_plan import ThreePlanChoice
from analysis.local_information_baselines import LocalInformationPlane, LocalMinLoad
from analysis.run_main_rl_stage1 import action_digest_update
from analysis.run_load_pressure_short import verify
from analysis.run_sar_frozen import invariants
from analysis.report_main_posthoc import score, latency_stats, DEADLINES
from analysis.redesign_scenario import digest, write_json
from sim.config import config_from_dict
from sim.workload import ImageTiling
from sim.control import ControlPlane
from sim.marl.decision import decision_state

MAIN = Path('results/redesign_20261021/main_scenario')
ROOT = MAIN / 'dynamic_heuristics_v2_20261025'
BC = MAIN / 'bc_stage2_20261022'
METHODS = ('v3_16', 'v3_4', 'v3_dynamic', 'minload_16', 'minload_4', 'ground_16', 'ground_4')
VALID = ('2026-10-22','2026-10-23','2026-10-24')
read = lambda p: json.loads(Path(p).read_text())
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()


def configuration(protocol, method):
    c = deepcopy(protocol['config'])
    count = 4 if method.endswith('_4') else 16
    c['image_tiling']['requested_num_tiles'] = count
    workload = ImageTiling.from_dict(c['image_tiling'])
    tiles = workload.tiles()
    c.update(image_size_mb=workload.image_size_mb, num_tiles=len(tiles),
        compute_cost_per_tile=math.fsum(t.compute_cost for t in tiles)/len(tiles),
        result_size_mb=math.fsum(t.result_size_mb for t in tiles)/len(tiles))
    for event in c['scheduled_task_arrivals']:
        event['image_tiling']['requested_num_tiles'] = count
    if method.startswith('ground'):
        c['pipeline_mode'] = 'ground_unique_relay'
    return c


def verify_manifest():
    m = read(ROOT/'manifest.json')
    for path, expected in m['hashes'].items():
        assert sha(path) == expected, path
    return m


class TrafficAudit:
    """只包裹原传输函数读剩余量，原函数保持唯一容量分配者。"""
    def __init__(self, env):
        import sim.pipeline.distributed as module
        self.env = env
        self.by_kind = Counter()
        original_isl = env._dist_pipeline.advance_transfers
        self.original_ground = module.advance_ground_transfers
        self.module = module

        def isl(links):
            before = [(t, t.remaining_mb, 'result_isl' if t.is_result else 'raw_isl') for t in env.transfers]
            original_isl(links)
            for t, amount, kind in before:
                self.by_kind[kind] += max(0., amount-t.remaining_mb)

        def ground(e, transfers):
            if e is not env:
                return self.original_ground(e, transfers)
            result_ids = {id(t) for t in e.downlink_transfers}
            before = [(t,t.remaining_mb,'result_ground' if id(t) in result_ids else 'raw_ground') for t in transfers]
            self.original_ground(e,transfers)
            for t,amount,kind in before:
                self.by_kind[kind] += max(0.,amount-t.remaining_mb)

        self.original_isl = original_isl
        env._dist_pipeline.advance_transfers = isl
        module.advance_ground_transfers = ground

    def close(self):
        self.env._dist_pipeline.advance_transfers = self.original_isl
        self.module.advance_ground_transfers = self.original_ground

    def snapshot(self):
        env=self.env
        return dict(traffic_mb=dict(self.by_kind), network_mb=dict(env.metrics.network_sent_mb),
            control_mb=dict(env.control_plane.sent_mb_by_edge), compute_busy_s=dict(env.metrics.compute_busy_time))


def difference(a,b):
    return {key:{k:b[key].get(k,0)-a[key].get(k,0) for k in set(a[key])|set(b[key])} for key in a}


def resources(snapshot, helpers, duration, cfg):
    total=math.fsum(snapshot['network_mb'].values());control=math.fsum(snapshot['control_mb'].values())
    data=math.fsum(snapshot['traffic_mb'].values())
    assert math.isclose(total,control+data,abs_tol=1e-8,rel_tol=1e-9),(total,control,data)
    busy=sum(snapshot['compute_busy_s'].get(s,0) for s in helpers)
    ground=sum(v for k,v in snapshot['network_mb'].items() if k.startswith('ground:'))
    capacity=sum(g['bandwidth_mbps']/8*g.get('receive_channels',1) for g in cfg.ground_stations)*duration
    assert busy <= len(helpers)*duration+.001 and ground<=capacity+.001
    return dict(duration_s=duration,total_transmitted_bytes=total*1e6,control_bytes=control*1e6,
        **{k+'_bytes':v*1e6 for k,v in snapshot['traffic_mb'].items()},helper_busy_s=busy,
        helper_utilization=busy/(len(helpers)*duration),ground_bytes=ground*1e6,
        ground_channel_utilization=ground/capacity,helper_busy_by_node=snapshot['compute_busy_s'])


def job(args):
    path,method=args;started=time.perf_counter();env=observer=None
    p=read(path);folder=ROOT/'validation'/p['date'];out=folder/f'{method}.json';status=folder/f'{method}.progress.json'
    try:
        assert p['date'] in VALID and method in METHODS
        if out.exists() or status.exists():raise FileExistsError('No retries or overwrite')
        m=verify_manifest();verify(p);torch.set_num_threads(1)
        config=configuration(p,method)
        assert digest(config)==m['config_hashes'][p['date']][method]
        cfg=config_from_dict(config);env=GranularityEnv(cfg);sources=set(cfg.task_source_sat_ids)
        helpers=set(env.satellites)-sources
        if method.startswith('minload'):
            plane=LocalInformationPlane(sources);plane.history_hops=2
            policies={s:LocalMinLoad() for s in sources}
        else:
            plane=ControlPlane(sources);policies={}
        env.control_plane=plane
        policy=DynamicThreePlan(env) if method=='v3_dynamic' else ThreePlanChoice(cfg.relay_max_hops)
        observer=TrafficAudit(env);boundaries={0:observer.snapshot()};action_hash=hashlib.sha256()
        for step in range(cfg.sim_steps):
            actions=[]
            if step%cfg.decision_interval_steps==0:
                state=env.export_state();plane.enqueue(state)
                state=decision_state(state,cfg.decision_tile_limit_per_sat)
                if policies:
                    for source in sorted({t['source_sat_id'] for t in state.tiles.values()}):
                        actions.extend(policies[source].select_actions(plane.view(state,source,2)))
                elif not method.startswith('ground'):
                    actions=policy.select_actions(state)
                action_digest_update(action_hash,step,actions)
            env.step(actions)
            now=round(env.time*cfg.dt,8)
            if now in (p['score_start_s'],p['score_end_s']):boundaries[now]=observer.snapshot()
            if env.time%round(60/cfg.dt)==0:
                write_json(status,dict(status='RUNNING',pid=os.getpid(),method=method,date=p['date'],
                    sim_seconds=now,total_sim_seconds=p['sim_duration_s'],wall_seconds=time.perf_counter()-started))
        checks=invariants(env);wanted=set(p['scored_task_ids']);outcomes=env.summary()['task_outcomes']
        scored=[o for o in outcomes if o['task_id'] in wanted]
        checks['followup']=len(scored)==len(wanted) and all(o['observed_age_seconds']>=600 for o in scored)
        checks['input_commitment']=all(not t.input_route or len(t.input_route)-1<=2 for t in env.tiles.values())
        checks['window_partition']=all(len([b for tid in t.tile_ids for b in env.tiles[tid].windows_xyxy])==
            len({b for tid in t.tile_ids for b in env.tiles[tid].windows_xyxy}) for t in env.tasks.values())
        if method=='v3_16':
            reference=read(BC/'validation'/p['date']/'v3.json')
            checks['frozen_reference_outcomes']=outcomes==reference['outcomes']
            checks['frozen_reference_actions']=action_hash.hexdigest()==reference['action_trace_sha256']
        for k in ['followup','input_commitment','window_partition','frozen_reference_outcomes','frozen_reference_actions']:
            checks['passed'] &= checks.get(k,True)
        final=observer.snapshot();start,end=p['score_start_s'],p['score_end_s']
        resource_rows={name:resources(difference(a,b),helpers,duration,cfg) for name,a,b,duration in [
            ('whole',boundaries[0],final,p['sim_duration_s']),('main',boundaries[start],boundaries[end],end-start),
            ('warmup',boundaries[0],boundaries[start],start),('followup',boundaries[end],final,p['sim_duration_s']-end)]}
        counts=Counter(d['selected'] for d in policy.decisions if d['task_id'] in wanted) if method=='v3_dynamic' else Counter()
        layouts={task:dict(requested=env.granularity_choices.get(task,{}).get('requested',4 if method.endswith('_4') else 16),
            actual=t.num_tiles,packet_mb=sum(x.data_size_mb for x in env._task_workloads[task].tiles()),
            unique_mb=t.image_size_mb) for task,t in env.tasks.items() if task in wanted}
        result=dict(status='PASS' if checks['passed'] else 'AUDIT_MISMATCH',method=method,date=p['date'],
            checks=checks,protocol=path,config_sha256=digest(config),outcomes=outcomes,
            curve=[score(scored,d) for d in DEADLINES],latency=latency_stats(scored),
            R600=math.fsum(min(o['latency_seconds'],600) if o['status']=='completed' else 600 for o in scored)/len(scored),
            action_trace_sha256=action_hash.hexdigest(),resources=resource_rows,granularity=layouts,
            choices=dict(counts),decisions=policy.decisions if method=='v3_dynamic' else [],
            wall_seconds=time.perf_counter()-started)
        verify_manifest();write_json(out,result)
        summary=dict(status=result['status'],path=str(out),sha256=sha(out),wall_seconds=result['wall_seconds'])
        write_json(status,summary);return summary
    except Exception:
        result=dict(status='FAILED',method=method,date=p['date'],traceback=traceback.format_exc())
        write_json(status,result);return result
    finally:
        if observer:observer.close()
        if env:env.close()


def prepare():
    if ROOT.exists():raise FileExistsError(ROOT)
    bc=read(BC/'manifest.json');paths=bc['validation']
    hashes={**bc['entry_hashes'],**bc['episode_hashes']}
    for path in paths:verify(read(path))
    files=[Path(__file__),Path('analysis/dynamic_granularity_v2.py'),Path('tests/test_dynamic_granularity_v2.py'),
        MAIN/'DYNAMIC_REVISION_PROTOCOL_20261025.md',MAIN/'DYNAMIC_HEURISTIC_ADDENDUM_20261025.md',
        MAIN/'DYNAMIC_HEURISTIC_V2_CORRECTION_20261025.md',
        MAIN/'fixed_mappo_frozen_20261025/manifest.json']
    for path in files:hashes[str(path)]=sha(path)
    for date in VALID:hashes[str(BC/'validation'/date/'v3.json')]=sha(BC/'validation'/date/'v3.json')
    for path,h in hashes.items():assert sha(path)==h
    ROOT.mkdir()
    write_json(ROOT/'manifest.json',dict(protocols=paths,methods=METHODS,workers=9,hashes=hashes,
        config_hashes={read(p)['date']:{method:digest(configuration(read(p),method)) for method in METHODS} for p in paths},
        tests_sealed=True,orin_four_block_memory_verified=False,no_retry=True))
    print('PREPARED',sha(ROOT/'manifest.json'))


def collect(results):
    if len(results)!=21 or any(r['status']!='PASS' for r in results):
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',results=results));return
    summary={};curves={};lines=['# 动态粒度启发式验证','','三日等权；测试封存。16为请求上限，小图保持原4块退化，不复制推理窗口。',
        '4块复用窗口级执行与整图合并；Orin4块峰值内存未验证。','',
        '|方法|P60/90/120/180/240/300/600 (%)|R600(s)|主窗口传输GB|计算星利用率|地面通道利用率|',
        '|---|---|---:|---:|---:|---:|']
    m=verify_manifest()
    for method in METHODS:
        records=[read(ROOT/'validation'/d/f'{method}.json') for d in VALID]
        x=dict(P=np.mean([[s['P_lower'] for s in r['curve']] for r in records],axis=0).tolist(),
            R600=float(np.mean([r['R600'] for r in records])),
            mean_completed_s=float(np.mean([r['latency']['mean_completed_s'] for r in records])),
            unfinished_total=sum(r['latency']['unfinished'] for r in records),
            main_total_bytes=float(np.mean([r['resources']['main']['total_transmitted_bytes'] for r in records])),
            helper_utilization=float(np.mean([r['resources']['main']['helper_utilization'] for r in records])),
            ground_utilization=float(np.mean([r['resources']['main']['ground_channel_utilization'] for r in records])))
        summary[method]=x
        sets=[[o for o in r['outcomes'] if o['task_id'] in set(read(p)['scored_task_ids'])] for r,p in zip(records,m['protocols'])]
        xs=sorted({60.,600.,*[o['latency_seconds'] for oo in sets for o in oo if o['status']=='completed' and 60<=o['latency_seconds']<=600]})
        curves[method]=dict(deadlines=xs,P=[float(np.mean([sum(o['status']=='completed' and o['latency_seconds']<=d for o in oo)/len(oo) for oo in sets])) for d in xs])
        lines.append(f"|{method}|"+'/'.join(f'{v*100:.2f}' for v in x['P'])+f"|{x['R600']:.2f}|{x['main_total_bytes']/1e9:.3f}|{x['helper_utilization']:.3%}|{x['ground_utilization']:.3%}|")
    counts=Counter()
    for date in VALID:counts.update(read(ROOT/'validation'/date/'v3_dynamic.json')['choices'])
    lines+=['','动态选择（评分图，原图地面独立列出）：'+str(dict(counts)),
        '各日各图实际粒度/字节、完整资源分段与未完成见validation记录；资源主窗口统计包含其中所有在服务图像。',
        '传输字节为所有跳累计，包含包间重叠、结果与控制报文；并非原始图像唯一字节。',
        '地面利用率为实际传输字节/单站通道额定容量，计算星利用率为实际忙时/节点数/窗口长。']
    write_json(ROOT/'summary.json',dict(day_equal=summary,choices=dict(counts)))
    write_json(ROOT/'full_curves.json',curves)
    (ROOT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    write_json(ROOT/'FINAL_AUDIT.json',dict(status='PASS',hashes={str(p):sha(p) for p in ROOT.rglob('*') if p.is_file() and p.suffix in ('.json','.md') and p.name not in ('FINAL_AUDIT.json','final_status.json')}))
    write_json(ROOT/'final_status.json',dict(status='COMPLETE',tests_sealed=True,results=results))


def panel():
    try:
        m=verify_manifest();results=[]
        with ProcessPoolExecutor(max_workers=9,mp_context=multiprocessing.get_context('spawn')) as pool:
            futures=[pool.submit(job,(path,method)) for method in METHODS for path in m['protocols']]
            for f in as_completed(futures):
                results.append(f.result());write_json(ROOT/'progress.json',dict(done=len(results),total=21,results=results))
        collect(results)
    except Exception:
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',traceback=traceback.format_exc()))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=['prepare','panel']);args=parser.parse_args();globals()[args.mode]()
