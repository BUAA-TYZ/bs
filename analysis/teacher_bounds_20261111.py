"""空日历参考，以及零序列化/独立窗口时间图下界；不执行调度策略。"""
from bisect import bisect_left
from collections import defaultdict
from copy import deepcopy
from dataclasses import replace
import heapq
import itertools
import math
import time

import numpy as np

from analysis.sar_three_plan import Calendar, RawImageAction
from sim.entities import Action, ActionType, Link
from sim.orbit import positions_km_at_times
from sim.scheduler.literature import action_for, ground_node, input_options
from sim.topology import _visible_from_positions, bounded_edges, link_key
from sim.workload import ImageTiling


def image_view(empty, event, task_id, requested):
    workload = replace(ImageTiling.from_dict(event['image_tiling']), requested_num_tiles=requested)
    descriptors = workload.tiles(); n = len(descriptors)
    unique = workload.unique_transport_blocks(n)
    source = event['source_sat_id']
    rows = {}
    for index, tile in enumerate(descriptors):
        rows[f'{task_id}_tile_{index}'] = dict(source_sat_id=source, task_image_size_mb=workload.image_size_mb,
            unique_input_size_mb=unique[index][1], state='QUEUED', location=source,
            data_size_mb=tile.data_size_mb, data_size_gb=tile.data_size_mb/1000,
            compute_cost=tile.compute_cost, vram_req_gb=event['_vram_req_gb'], in_transfer=False,
            parent_task_id=task_id, task_progress=dict(total=n,done=0,failed=0,running=0,awaiting_delivery=0),
            waiting_time=0., result_size_mb=tile.result_size_mb,
            sibling_locations={source:n-1} if n>1 else {}, task_pending_count=n, deadline_remaining_s=None)
    satellites = deepcopy(empty.satellites)
    satellites[source]['queue_len'] = n
    satellites[source]['mem_remaining_gb'] -= sum(t['data_size_gb'] for t in rows.values())
    return replace(empty, satellites=satellites, tiles=rows, pending_task_tiles={task_id:rows})


def no_contention_reference(empty, event, task_id):
    """L/G保留图内承诺；C每块/目标重置日历。有限计划不可证明全局下界。"""
    quotes, block_records = {}, {}
    for count in (4,16):
        state = image_view(empty,event,task_id,count)
        local = [Action(tid,ActionType.LOCAL) for tid in state.tiles]
        quotes[f'local_{count}'] = Calendar(state).quote(state,local)
        records = []
        for tid,tile in state.tiles.items():
            single = replace(state,tiles={tid:tile},pending_task_tiles={task_id:{tid:tile}})
            initial = Calendar(single)
            options = input_options(single,initial.plan,tile,initial.memory)
            candidates = []
            for target,(_,path) in sorted(options.items(),key=lambda row:repr(row[0])):
                action = action_for(tid,tile['location'],target,plan=initial.plan,path=path,
                                    ground_relay=bool(single.config.get('ground_input_relay')))
                quote = Calendar(single).quote(single,[action])
                if quote is not None: candidates.append((quote,repr(target)))
            best = min(candidates) if candidates else (None,None)
            records.append(dict(tile_id=tid,quote_s=best[0],processor=best[1],candidates=len(candidates)))
        quotes[f'cooperative_{count}'] = (max(r['quote_s'] for r in records)
            if all(r['quote_s'] is not None for r in records) else None)
        block_records[str(count)] = records
        if count == 16:
            raw = [RawImageAction(tid,ActionType.OFFLOAD,raw_hops=empty.config['relay_max_hops']) for tid in state.tiles]
            quotes['ground'] = Calendar(state).quote(state,raw,raw=True,raw_hops=empty.config['relay_max_hops'])
    available = [(v,k) for k,v in quotes.items() if v is not None]
    best = min(available) if available else (None,None)
    return dict(task_id=task_id,latency_seconds=best[0],selected=best[1],quotes=quotes,blocks=block_records)


class TemporalGraph:
    """每条边为闭区间可用集，零传输耗时；FIFO标签算法无跳数上限。"""
    def __init__(self, records):
        self.outgoing = defaultdict(list)
        grouped = defaultdict(list)
        for source,target,start,end in records:
            if end < start: raise ValueError('Invalid interval')
            grouped[source,target].append((float(start),float(end)))
        for (source,target), intervals in grouped.items():
            merged = []
            for start,end in sorted(intervals):
                if merged and start <= merged[-1][1]: merged[-1] = (merged[-1][0],max(end,merged[-1][1]))
                else: merged.append((start,end))
            self.outgoing[source].append((target,merged,[b for _,b in merged]))

    def earliest(self, seeds, *, destinations=None, service=None, processors=None):
        serial = itertools.count(); heap = []; distance = {}; parent = {}
        for node,ready in seeds.items():
            key = (node,0); distance[key] = ready
            heapq.heappush(heap,(ready,next(serial),key))
        while heap:
            ready,_,key = heapq.heappop(heap)
            if ready != distance[key]: continue
            node,phase = key
            if destinations is not None and node in destinations and (service is None or phase == 1):
                witness = []; current = key
                while current in parent:
                    previous,kind = parent[current]
                    witness.append(dict(source=previous[0],target=current[0],kind=kind,
                                        start=distance[previous],end=distance[current]))
                    current = previous
                return ready,list(reversed(witness))
            edges = []
            for target,intervals,ends in self.outgoing.get(node,[]):
                index = bisect_left(ends,ready)
                if index < len(intervals): edges.append(((target,phase),max(ready,intervals[index][0]),'contact'))
            if service is not None and phase == 0 and node in processors:
                edges.append(((node,1),ready+service,'compute'))
            for target,end,kind in edges:
                if end < distance.get(target,math.inf):
                    distance[target] = end; parent[target] = (key,kind)
                    heapq.heappush(heap,(end,next(serial),target))
        return None


def service_floor(cfg, event):
    cost = ImageTiling.from_dict(event['image_tiling']).compute_cost_per_window
    assert cost == 1 and cfg.compute_service_mode == 'work_conserving'
    profile = cfg.onboard_compute_profile
    assert profile and all(float(s)>0 for s in profile['service_s'].values())
    # 不是整块greedy cohort成本；一窗至少经历一次完整cohort或地面正服务。
    return min(min(profile['service_s'].values()),
               *(cost/g['compute_rate'] for g in cfg.ground_stations))


def certified_image_bound(graph, cfg, event, task_id):
    release = event['release_step']*cfg.dt
    ground = {ground_node(g['id']) for g in cfg.ground_stations}
    processors = set(range(cfg.num_sats)) | ground
    service = service_floor(cfg,event)
    merged = cfg.ground_relay_delay_s + event.get('aggregation_delay_s',cfg.aggregation_delay_s)
    source = event['source_sat_id']
    mixed = graph.earliest({source:release},destinations=ground,service=service,processors=processors)
    local = graph.earliest({source:release+service},destinations=ground)
    raw = graph.earliest({source:release},destinations=ground)
    workload = ImageTiling.from_dict(event['image_tiling'])
    work = sum(d.compute_cost for d in workload.tiles())
    raw_service = work/max(g['compute_rate'] for g in cfg.ground_stations)
    times = dict(local=None if local is None else local[0]-release+merged,
                 cooperative=None if mixed is None else mixed[0]-release+merged,
                 ground=None if raw is None else raw[0]-release+raw_service+merged)
    finite = [v for v in times.values() if v is not None]
    latency = min(finite) if finite else None
    return dict(task_id=task_id,release_seconds=release,latency_seconds=latency,
        R600_contribution=min(600.,latency) if latency is not None else 600.,
        service_floor_s=service,fixed_merge_s=merged,relaxed_modes=times,
        windows=int(work),witness=[] if mixed is None else mixed[1])


def geometry_and_reference(env, protocol, progress):
    """按执行器10秒刷新网格重建完整几何；去掉建链延迟，保留选择边的超集。"""
    cfg = env.cfg; topo = env.topology; tc = topo.cfg
    assert cfg.topology_update_steps == cfg.ground_visibility_update_steps
    assert tc.bandwidth_noise == 0 and tc.bandwidth_period == 0 and tc.bandwidth_distance_scale_km == 0
    assert tc.bandwidth_mbps_max > 0
    stride = cfg.topology_update_steps; interval = stride*cfg.dt
    steps = np.arange(0,cfg.sim_steps,stride,dtype=np.int64)
    seconds = steps*cfg.dt
    positions = [positions_km_at_times(sat,topo.t0,seconds) for sat in topo.sat_recs[:cfg.num_sats]]
    pairs = [(i,j) for i in range(cfg.num_sats) for j in range(i+1,cfg.num_sats)]
    scored = set(protocol['scored_task_ids']); events = defaultdict(list)
    for i,event in enumerate(protocol['config']['scheduled_task_arrivals']):
        task_id = f'task_{i}'
        if task_id in scored: events[(event['release_step']//stride)*stride].append((task_id,event))
    records, reference, previous, starts = [], [], {}, {}
    scalar_checks = []
    # 确定的首/中/末三个网格用原scalar snapshot核对；与成绩无关。
    audit_indices = {0,len(steps)//2,len(steps)-1}
    for index,step in enumerate(steps):
        now = float(seconds[index]); end = min(now+interval,cfg.sim_steps*cfg.dt)
        points = [tuple(p[index]) for p in positions]
        geometry = [_visible_from_positions(points[i],points[j],tc.earth_radius_km,
                       tc.min_elevation_deg,tc.max_range_km) for i,j in pairs]
        allowed = bounded_edges(pairs,geometry,tc.max_neighbors,sources=tc.source_sat_ids,
                                limits=tc.neighbor_limits,preferred=tc.preferred_edges)
        since = {pair:previous.get(pair,now) for pair in allowed}
        previous = since
        edges = {(i,j) for i,j in allowed} | {(j,i) for i,j in allowed}
        for sid in range(cfg.num_sats):
            row = int(env.vis_cache._precomputed_index[step])
            assert row >= 0
            for gid,gs in env.ground_stations.items():
                altitude = float(env.vis_cache._precomputed[sid,gid][0][row])
                if altitude >= gs.min_elevation_deg and gs.bandwidth_mbps > 0:
                    edges.add((sid,ground_node(gid)))
        for edge in list(starts):
            if edge not in edges: records.append((*edge,starts.pop(edge),now))
        for edge in edges: starts.setdefault(edge,now)
        if index in audit_indices:
            topo._selected_since = dict(since)
            actual = topo.snapshot(int(step),dt=cfg.dt)
            actual_selected = {pair for pair in pairs if math.isfinite(actual[link_key(*pair)].acquisition_ready_s)}
            assert actual_selected == allowed, ('geometry_mismatch',int(step))
            scalar_checks.append(dict(step=int(step),selected_edges=len(allowed),exact_match=True))
        if int(step) in events:
            topo._selected_since = dict(since)
            env._links_cache = {link_key(i,j):Link(i,j,
                up=(i,j) in allowed and now >= since[i,j]+tc.acquisition_delay_s,
                bandwidth_mbps=tc.bandwidth_mbps_max,latency_ms=tc.latency_ms,
                acquisition_ready_s=since.get((i,j),math.inf)+tc.acquisition_delay_s) for i,j in pairs}
            env._links_cache_time = int(step)
            for task_id,event in sorted(events[int(step)],key=lambda row:row[1]['release_step']):
                env.time = event['release_step']
                empty = env.export_state()
                assert not empty.tiles and all(s['committed_compute_s']==0 for s in empty.satellites.values())
                assert all(g['committed_compute_s']==0 for g in empty.ground_stations.values())
                w = ImageTiling.from_dict(event['image_tiling'])
                entry = dict(event,_vram_req_gb=cfg.vram_base_gb+cfg.vram_alpha_per_mb*w.window_size_mb)
                reference.append(no_contention_reference(empty,entry,task_id))
        if index % 50 == 0: progress(dict(phase='geometry_reference',grid_done=index+1,grid_total=len(steps),images=len(reference)))
    for edge,start in starts.items(): records.append((*edge,start,cfg.sim_steps*cfg.dt))
    assert {r['task_id'] for r in reference} == scored
    return records,reference,dict(grid_seconds=interval,grid_samples=len(steps),scalar_snapshot_checks=scalar_checks,
        horizon_s=cfg.sim_steps*cfg.dt,contact_intervals=len(records),reference_images=len(reference))
