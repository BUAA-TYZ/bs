"""关卡1的独立任意分组与全局候选；旧物理执行器和五候选保持不变。"""
from bisect import bisect_right
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass, replace
import math

from analysis.dynamic_granularity_v2 import GranularityEnv, preview
from analysis.sar_three_plan import Calendar, ThreePlanChoice, RawImageAction
from sim.entities import Action, ActionType, Tile, TileState, TileTimestamps
from sim.scheduler.contact_graph import quantize
from sim.scheduler.input_routing import input_path_arrival, reserve_input_memory, input_route_nodes
from sim.scheduler.literature import input_options, ground_node, action_for
from sim.scheduler.task_choice import CGSTaskChoice
from sim.scheduler.task_gate import whole_service
from sim.workload import ImageTile, TransportComponent, window_starts

MAX_TARGETS = 16
TOLERANCE_S = 0.01
MAX_BISECTIONS = 64
MAX_FINISH_S = 3600.0
MAPPINGS = ('row_snake', 'column_snake', 'long_axis_stripes')


def windows(workload):
    return tuple((x, y, x+workload.window_px, y+workload.window_px)
        for y in window_starts(workload.height_px, workload.window_px, workload.stride_px)
        for x in window_starts(workload.width_px, workload.window_px, workload.stride_px))


def union_area(boxes):
    """扫描x边界并合并y区间，保留L形、孔洞及分量向外取整后的真实并集。"""
    xs = sorted({x for box in boxes for x in (box[0], box[2])})
    total = 0
    for left, right in zip(xs, xs[1:]):
        intervals = sorted((y0, y1) for x0, y0, x1, y1 in boxes if x0 < right and x1 > left)
        length, end = 0, -math.inf
        for lo, hi in intervals:
            length += max(0, hi-max(lo, end))
            end = max(end, hi)
        total += (right-left)*length
    return total


def union_bytes(workload, boxes):
    components = workload.transport_components or (
        TransportComponent('inference', 1, workload.bands, workload.bits_per_sample),)
    size = 0
    for component in components:
        scale = component.downsample
        mapped = [(a//scale, b//scale, (c+scale-1)//scale, (d+scale-1)//scale) for a,b,c,d in boxes]
        bits = union_area(mapped)*component.bands*component.bits_per_sample
        size += (bits+7)//8
    return size/workload.transport_compression_ratio


def grouped_layout(workload, window_to_block):
    boxes = windows(workload)
    labels = tuple(window_to_block)
    if len(labels) != len(boxes) or any(type(x) is not int for x in labels):
        raise ValueError('One integer block label is required for every canonical window')
    count = max(labels, default=-1)+1
    if not 1 <= count <= MAX_TARGETS or set(labels) != set(range(count)):
        raise ValueError('Require contiguous labels and 1..16 nonempty blocks')
    descriptors = []
    for label in range(count):
        group = tuple(box for box, owner in zip(boxes, labels) if owner == label)
        descriptors.append(ImageTile(group, union_bytes(workload, group)/1e6,
            len(group)*workload.compute_cost_per_window, len(group)*workload.result_size_mb_per_window))
    return tuple(descriptors)


def mapping(workload, counts, kind):
    boxes = windows(workload)
    if kind not in MAPPINGS or not counts or len(counts)>MAX_TARGETS or any(type(n) is not int or n<=0 for n in counts) or sum(counts)!=len(boxes):
        raise ValueError('Invalid mapping or positive integer allocation')
    xs, ys = sorted({b[0] for b in boxes}), sorted({b[1] for b in boxes})
    by_xy = {(b[0],b[1]):i for i,b in enumerate(boxes)}
    if kind == 'row_snake':
        order = [by_xy[x,y] for row,y in enumerate(ys) for x in (xs if row%2==0 else xs[::-1])]
    elif kind == 'column_snake':
        order = [by_xy[x,y] for col,x in enumerate(xs) for y in (ys if col%2==0 else ys[::-1])]
    elif workload.width_px >= workload.height_px:
        # 长边平行于x：完整横条带自上而下，每条内自左向右；份额边界可切开条带。
        order = [by_xy[x,y] for y in ys for x in xs]
    else:
        order = [by_xy[x,y] for x in xs for y in ys]
    labels, offset = [None]*len(boxes), 0
    for owner, count in enumerate(counts):
        for index in order[offset:offset+count]:
            labels[index] = owner
        offset += count
    return tuple(labels)


def choose_mapping(workload, counts):
    choices = []
    for kind in MAPPINGS:
        labels = mapping(workload, counts, kind)
        descriptors = grouped_layout(workload, labels)
        cost = math.fsum(union_bytes(workload, d.windows_xyxy) for d in descriptors)
        choices.append((cost, kind, labels, descriptors))
    winner = min(range(len(choices)), key=lambda i:(choices[i][0],i))
    cost, kind, labels, descriptors = choices[winner]
    return dict(mapping=kind, labels=labels, descriptors=descriptors, bytes=cost,
        row_snake_bytes=choices[0][0], candidate_bytes={row[1]:row[0] for row in choices})


@dataclass(frozen=True)
class GroupedWorkload:
    original: object
    descriptors: tuple

    @property
    def image_size_mb(self):
        return self.original.image_size_mb

    def tiles(self):
        return self.descriptors

    def unique_transport_blocks(self, count):
        raise ValueError('C_nu groups are not raw-image transport partitions')


def group_preview(env, state, task, labels):
    descriptors = grouped_layout(env._task_workloads[task], labels)
    original = [t for t in state.tiles.values() if t['parent_task_id']==task]
    first = original[0]
    n = len(descriptors)
    rows = {f'{task}_tile_{i}':dict(first, data_size_mb=d.data_size_mb,
        data_size_gb=env.memory_size_gb(d.data_size_mb), compute_cost=d.compute_cost,
        result_size_mb=d.result_size_mb, unique_input_size_mb=None, task_pending_count=n,
        sibling_locations={first['source_sat_id']:n-1} if n>1 else {},
        task_progress={**first['task_progress'],'total':n}) for i,d in enumerate(descriptors)}
    satellites = deepcopy(state.satellites)
    source = first['source_sat_id']
    satellites[source]['mem_remaining_gb'] += math.fsum(t['data_size_gb'] for t in original)-math.fsum(t['data_size_gb'] for t in rows.values())
    satellites[source]['queue_len'] += n-len(original)
    return replace(state,tiles=rows,pending_task_tiles={task:rows},satellites=satellites)


class NonuniformEnv(GranularityEnv):
    def repack_groups(self, task_id, labels):
        labels = tuple(labels)
        previous = self.granularity_choices.get(task_id)
        if previous is not None:
            if previous.get('labels') != labels:
                raise ValueError('Conflicting layout commitment')
            return
        task = self.tasks[task_id]
        old = [self.tiles[tid] for tid in task.tile_ids]
        if not all(t.state==TileState.QUEUED and t.location==task.source_sat_id and
                   t.memory_holders=={task.source_sat_id} and not t.completed_compute_windows for t in old):
            raise ValueError('Cannot repack committed windows')
        workload = self._task_workloads[task_id]
        descriptors = grouped_layout(workload, labels)
        assert Counter(b for t in old for b in t.windows_xyxy)==Counter(b for t in descriptors for b in t.windows_xyxy)
        assert math.isclose(math.fsum(t.compute_cost for t in old), math.fsum(t.compute_cost for t in descriptors))
        new_mb = math.fsum(t.data_size_mb for t in descriptors)
        sat = self.satellites[task.source_sat_id]
        needed = sat.mem_used_gb-math.fsum(self.memory_size_gb(t.data_size_mb) for t in old)+self.memory_size_gb(new_mb)
        if needed > sat.mem_capacity_gb+1e-9 or any(self.memory_size_gb(t.data_size_mb)>self.cfg.mem_capacity_gb+1e-9 for t in descriptors):
            raise ValueError('Source or per-block receive quota exceeded')
        event = self.cfg.scheduled_task_arrivals[int(task_id.split('_')[-1])]
        actual = event.get('window_result_bytes')
        outputs = dict(zip(windows(workload), actual, strict=True)) if actual is not None else None
        old_ids = set(task.tile_ids)
        for tile in old:
            self._release_tile_memory(tile, task.source_sat_id)
            del self.tiles[tile.tile_id]
        sat.queue[:] = [t for t in sat.queue if t not in old_ids]
        self.metrics.total_tiles += len(descriptors)-len(old)
        task.num_tiles, task.tile_ids = len(descriptors), []
        self._task_tile_index[task_id] = task.tile_ids
        self._task_workloads[task_id] = GroupedWorkload(workload, descriptors)
        self._task_unique_blocks[task_id] = {}
        for i,d in enumerate(descriptors):
            tid = f'{task_id}_tile_{i}'
            tile = Tile(tid,task_id,d.data_size_mb,d.compute_cost,old[0].vram_req_gb,
                TileState.CREATED,task.source_sat_id,TileTimestamps(task.release_time),task.deadline,
                result_size_mb=d.result_size_mb,windows_xyxy=d.windows_xyxy,
                actual_result_size_mb=math.fsum(outputs[b] for b in d.windows_xyxy)/1e6 if outputs is not None else None)
            self.tiles[tid] = tile
            task.tile_ids.append(tid)
            self._enqueue_tile(task.source_sat_id,tile)
        self.granularity_choices[task_id] = dict(requested=len(descriptors),actual=len(descriptors),
            labels=labels,windows=len(labels),unique_mb=workload.image_size_mb,packet_mb=new_mb,
            overlap_mb=new_mb-workload.image_size_mb)


class BlockCalendar(Calendar):
    """仅记录原Calendar.quote的逐块末次delivery返回值，保留原报价执行逻辑。"""
    def quote(self, state, actions, **kwargs):
        self._calls = []
        self._transport_events = []
        answer = super().quote(state,actions,**kwargs)
        self.block_finishes = {}
        if answer is not None and not kwargs.get('raw',False):
            # 所有输入先于所有结果；每块输入均恰好调用一次transport（本地为空路径）。
            ordered = sorted(state.tiles,key=lambda tid:(-state.tiles[tid]['waiting_time'],tid))
            arrivals = dict(zip(ordered, self._transport_events[:len(ordered)], strict=True))
            delivery_order = sorted(ordered,key=lambda tid:(arrivals[tid],tid))
            self.block_finishes = dict(zip(delivery_order,self._calls[-len(ordered):],strict=True))
        return answer

    def transport(self, *args, **kwargs):
        end = super().transport(*args, **kwargs)
        self._transport_events.append(end)
        return end

    def delivery(self, *args, **kwargs):
        answer = super().delivery(*args, **kwargs)
        self._calls.append(None if answer is None else answer[0])
        return answer


def allocate_counts(tables, total, upper):
    """单调上包络的整数容量二分；随后按最早边际完成时刻补窗，稳定目标顺序破同分。"""
    capacities = lambda t:[bisect_right(row,t)-1 for row in tables]
    if sum(capacities(upper)) < total:
        return None
    lo, hi, iterations = 0.0, upper, 0
    while hi-lo > TOLERANCE_S and iterations < MAX_BISECTIONS:
        mid = (lo+hi)/2
        if sum(capacities(mid)) >= total:
            hi = mid
        else:
            lo = mid
        iterations += 1
    counts = capacities(lo)
    if sum(counts)>total:
        counts = [0]*len(tables)
    while sum(counts)<total:
        feasible = [i for i,row in enumerate(tables) if counts[i]+1<len(row) and row[counts[i]+1]<=hi+1e-9]
        if not feasible:
            return None
        target = min(feasible,key=lambda i:(tables[i][counts[i]+1],i))
        counts[target] += 1
    return counts, hi, iterations


def target_key(target):
    return (1,str(target[1])) if isinstance(target,tuple) else (0,target)


def adjusted_view(state, subset, calendar):
    return replace(subset, satellites={sid:{**s,'committed_compute_s':calendar.busy[sid],
        'mem_remaining_gb':calendar.memory[sid]+(s['mem_remaining_gb']-state.satellites[sid]['mem_remaining_gb'])}
        for sid,s in subset.satellites.items()}, ground_stations={gid:{**g,'committed_compute_s':calendar.busy[ground_node(gid)]}
        for gid,g in subset.ground_stations.items()})


def nu_candidate(env, state, task, calendar):
    workload = env._task_workloads[task]
    boxes = windows(workload)
    first = next(t for t in state.tiles.values() if t['parent_task_id']==task)
    # 份额阶段统一使用行蛇形前缀，避免不同位置获得不同几何先验；最终再比较三种映射。
    order = sorted(range(len(boxes)),key=lambda i:(boxes[i][1],boxes[i][0] if sorted({b[1] for b in boxes}).index(boxes[i][1])%2==0 else -boxes[i][0]))
    dt, source = state.config['dt'], first['source_sat_id']
    ground = {ground_node(gid) for gid in state.ground_stations}
    one = dict(first,data_size_mb=union_bytes(workload,[boxes[order[0]]])/1e6,
        compute_cost=workload.compute_cost_per_window,result_size_mb=workload.result_size_mb_per_window)
    one['data_size_gb'] = env.memory_size_gb(one['data_size_mb'])
    options = input_options(state, calendar.plan, one, calendar.memory)
    horizon = min(MAX_FINISH_S,max((c.end+c.delay for c in calendar.plan.contacts),default=0))
    reasons = dict(status='NO_FEASIBLE_TARGET',horizon_s=horizon,candidates=len(options))
    if horizon<=0:
        return None,reasons
    def estimate(target,path,tile):
        quota = calendar.memory[target] if target in state.satellites and target!=source else env.cfg.mem_capacity_gb
        if tile['data_size_gb']>quota+1e-9 or calendar.rate.get(target,0)<=0:
            return math.inf
        arrival = input_path_arrival(state,calendar.plan,tile,calendar.memory,path)
        if arrival is None:
            return math.inf
        service = tile['compute_cost']/calendar.rate[target] if target in ground else whole_service(state,[tile],target)
        ready = max(calendar.busy[target],arrival)+service
        route = calendar.plan.delivery(target,ground,quantize(ready,dt),tile['result_size_mb'],dt=dt,
            max_hops=state.config.get('relay_max_hops',8)+1)
        return math.inf if route is None else route[0]
    ranked = sorted(((estimate(t,path,one),t,path) for t,(_,path) in options.items()),key=lambda row:(row[0],target_key(row[1])))
    ranked = [row for row in ranked if math.isfinite(row[0])][:MAX_TARGETS]
    if not ranked:
        return None,reasons
    tables = [[0.0] for _ in ranked]
    for n in range(1,len(boxes)+1):
        size = union_bytes(workload,[boxes[i] for i in order[:n]])/1e6
        tile = dict(first,data_size_mb=size,data_size_gb=env.memory_size_gb(size),
            compute_cost=n*workload.compute_cost_per_window,result_size_mb=n*workload.result_size_mb_per_window)
        for i,(_,target,path) in enumerate(ranked):
            value = estimate(target,path,tile) if math.isfinite(tables[i][-1]) else math.inf
            tables[i].append(max(tables[i][-1],value))
    result = allocate_counts(tables,len(boxes),horizon)
    if result is None:
        return None,{**reasons,'status':'INSUFFICIENT_CAPACITY','retained_targets':len(ranked)}
    counts,T,iterations = result
    used = [i for i,n in enumerate(counts) if n]
    selection = choose_mapping(workload,[counts[i] for i in used])
    subset = adjusted_view(state,group_preview(env,state,task,selection['labels']),calendar)
    if subset.satellites[source]['mem_remaining_gb'] < -1e-9:
        return None,{**reasons,'status':'SOURCE_MEMORY'}
    trial = BlockCalendar.__new__(BlockCalendar)
    trial.__dict__ = deepcopy(calendar.__dict__)
    trial.memory[source] = subset.satellites[source]['mem_remaining_gb']
    memory = dict(trial.memory)
    actions, predictions, targets = [], {}, {}
    for (tid,tile),index in zip(subset.tiles.items(),used,strict=True):
        target = ranked[index][1]
        quota = memory[target] if target in state.satellites and target!=source else env.cfg.mem_capacity_gb
        available = input_options(subset,calendar.plan,tile,memory)
        if tile['data_size_gb']>quota+1e-9 or target not in available:
            return None,{**reasons,'status':'MAPPED_INPUT_INFEASIBLE'}
        path = available[target][1]
        actions.append(action_for(tid,source,target,plan=calendar.plan,path=path,ground_relay=target in ground))
        reserve_input_memory(memory,calendar.plan,path,tile)
        predictions[tid] = tables[index][counts[index]]
        targets[tid] = target
    quote = trial.quote(subset,actions)
    checker = CGSTaskChoice()
    checker._contact_plan = lambda _:deepcopy(calendar.plan)
    grouped = checker.quote(subset,actions)
    if quote is None or grouped is None:
        return None,{**reasons,'status':'SHARED_CALENDAR_INFEASIBLE'}
    meta = dict(status='FEASIBLE',mapping=selection['mapping'],mapping_bytes=selection['candidate_bytes'],
        row_snake_bytes=selection['row_snake_bytes'],selected_bytes=selection['bytes'],
        counts=[counts[i] for i in used],targets=targets,allocation_prediction_s=predictions,
        calendar_prediction_s=trial.block_finishes,common_finish_s=T,bisection_iterations=iterations,
        retained_targets=len(ranked),horizon_s=horizon,grouped_quote_s=grouped[task])
    return dict(actions=actions,quote=max(quote,grouped[task]),trial=trial,labels=selection['labels'],meta=meta),meta


class NonuniformThreePlan(ThreePlanChoice):
    def __init__(self,env,enabled=True):
        super().__init__(env.cfg.relay_max_hops)
        self.env, self.enabled, self.decided, self.nu_targets = env,enabled,set(),{}

    def select_actions(self,state):
        groups = {}
        for tid,tile in sorted(state.tiles.items(),key=lambda x:(-x[1]['waiting_time'],x[0])):
            if tile['parent_task_id'] not in self.raw_tasks:
                groups.setdefault(tile['parent_task_id'],{})[tid]=tile
        calendar, chosen = Calendar(state), []
        for task,rows in groups.items():
            if task in self.nu_targets:
                # 份额提交后只向已选目标重试接纳，不因后续队列变化重新分组或改目标。
                view = replace(state,tiles=rows)
                for tid,tile in sorted(rows.items()):
                    if tile['state']!='QUEUED' or tile['in_transfer'] or tile['location']!=tile['source_sat_id']:
                        continue
                    target = self.nu_targets[task][tid]
                    options = input_options(view,calendar.plan,tile,calendar.memory)
                    if target in options:
                        path = options[target][1]
                        chosen.append(action_for(tid,tile['location'],target,plan=calendar.plan,path=path,
                            ground_relay=isinstance(target,tuple)))
                        calendar.plan.reserve(path,tile['data_size_mb'])
                        reserve_input_memory(calendar.memory,calendar.plan,path,tile)
                continue
            complete = task not in self.decided and len(rows)==self.env.tasks[task].num_tiles and all(
                t['state']=='QUEUED' and not t['in_transfer'] and not t['task_progress']['failed'] and t['location']==t['source_sat_id'] for t in rows.values())
            if not complete:
                chosen.extend(self._select_actions_with_plan(replace(state,tiles=rows),deepcopy(calendar.plan)))
                continue
            options,quotes,trials = {},{},{}
            for count in (4,16):
                subset = adjusted_view(state,preview(self.env,state,task,count),calendar)
                variants = {f'local_{count}':[Action(t,ActionType.LOCAL) for t in subset.tiles],
                    f'cgs_{count}':self._select_actions_with_plan(subset,deepcopy(calendar.plan))}
                if count==16:
                    variants['ground']=[RawImageAction(t,ActionType.OFFLOAD,raw_hops=self.raw_hops) for t in subset.tiles]
                for name,actions in variants.items():
                    trial=deepcopy(calendar)
                    source=next(iter(rows.values()))['source_sat_id']
                    trial.memory[source] += subset.satellites[source]['mem_remaining_gb']-calendar.memory[source]
                    quote=trial.quote(subset,actions,raw=name=='ground',raw_hops=self.raw_hops)
                    if name!='ground' and quote is not None:
                        checker=CGSTaskChoice();checker._contact_plan=lambda _:deepcopy(calendar.plan)
                        grouped=checker.quote(subset,actions)
                        if grouped is not None:quote=max(quote,grouped[task])
                    options[name],quotes[name],trials[name]=actions,quote,trial
            candidate,meta = nu_candidate(self.env,state,task,calendar) if self.enabled else (None,dict(status='DISABLED'))
            order=('local_4','local_16','cgs_4','cgs_16','ground')
            if candidate is not None:
                options['c_nu'],quotes['c_nu'],trials['c_nu']=candidate['actions'],candidate['quote'],candidate['trial']
                order += ('c_nu',)
            winner=min(order,key=lambda n:quotes[n] if quotes[n] is not None else math.inf)
            if quotes[winner] is None:winner='cgs_16'
            if winner=='c_nu':
                self.env.repack_groups(task,candidate['labels'])
                self.nu_targets[task]=meta['targets']
            else:self.env.repack(task,4 if winner.endswith('_4') else 16)
            if quotes[winner] is not None:calendar=trials[winner]
            chosen.extend(options[winner])
            if winner=='ground':self.raw_tasks.add(task)
            self.decided.add(task)
            self.decisions.append(dict(task_id=task,time_seconds=state.time*state.config['dt'],
                quotes_seconds=quotes,selected=winner,nu=meta,**self.env.granularity_choices[task]))
        return chosen
