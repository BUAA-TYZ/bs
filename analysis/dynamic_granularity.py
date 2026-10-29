"""冻结执行器上的粒度候选；同一推理窗口只归属一个调度块。"""
from collections import Counter
from copy import deepcopy
from dataclasses import replace
import math

from analysis.sar_three_plan import CandidateEnv, Calendar, ThreePlanChoice, RawImageAction
from sim.entities import Action, ActionType, Tile, TileState, TileTimestamps
from sim.scheduler.task_choice import CGSTaskChoice
from sim.scheduler.literature import ground_node


def layout(env, task_id, requested):
    workload = replace(env._task_workloads[task_id], requested_num_tiles=requested)
    descriptors = workload.tiles()
    task = env.tasks[task_id]
    old = [env.tiles[t] for t in task.tile_ids]
    before = Counter(box for t in old for box in t.windows_xyxy)
    after = Counter(box for t in descriptors for box in t.windows_xyxy)
    assert before == after and all(n == 1 for n in after.values())
    assert math.isclose(sum(t.compute_cost for t in old), sum(t.compute_cost for t in descriptors))
    # 真值逐窗结果绑定几何坐标；重打包不能把结果量改回平均代理。
    event = env.cfg.scheduled_task_arrivals[int(task_id.split('_')[-1])]
    boxes = sorted(after, key=lambda b: (b[1], b[0]))
    actual = event.get('window_result_bytes')
    outputs = dict(zip(boxes, actual, strict=True)) if actual is not None else None
    return workload, descriptors, outputs


def preview(env, state, task_id, requested):
    workload, descriptors, outputs = layout(env, task_id, requested)
    original = [t for t in state.tiles.values() if t['parent_task_id'] == task_id]
    assert original
    first = original[0]
    n = len(descriptors)
    unique = workload.unique_transport_blocks(n)
    rows = {}
    for i, desc in enumerate(descriptors):
        tid = f'{task_id}_tile_{i}'
        rows[tid] = dict(first, data_size_mb=desc.data_size_mb,
            data_size_gb=env.memory_size_gb(desc.data_size_mb), compute_cost=desc.compute_cost,
            unique_input_size_mb=unique[i][1], result_size_mb=desc.result_size_mb,
            task_pending_count=n, sibling_locations={first['source_sat_id']: n-1} if n>1 else {},
            task_progress={**first['task_progress'], 'total': n})
    satellites = deepcopy(state.satellites)
    source = first['source_sat_id']
    delta = sum(t['data_size_gb'] for t in original)-sum(t['data_size_gb'] for t in rows.values())
    satellites[source]['mem_remaining_gb'] += delta
    satellites[source]['queue_len'] += n-len(original)
    return replace(state, tiles=rows, pending_task_tiles={task_id: rows}, satellites=satellites)


class GranularityEnv(CandidateEnv):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.granularity_choices = {}

    def repack(self, task_id, requested):
        if task_id in self.granularity_choices:
            assert self.granularity_choices[task_id]['requested'] == requested
            return
        task = self.tasks[task_id]
        old = [self.tiles[t] for t in task.tile_ids]
        assert all(t.state == TileState.QUEUED and t.location == task.source_sat_id and
                   t.memory_holders == {task.source_sat_id} and not t.completed_compute_windows for t in old)
        workload, descriptors, outputs = layout(self, task_id, requested)
        old_mb = sum(t.data_size_mb for t in old)
        new_mb = sum(t.data_size_mb for t in descriptors)
        sat = self.satellites[task.source_sat_id]
        assert sat.mem_used_gb-self.memory_size_gb(old_mb)+self.memory_size_gb(new_mb) <= sat.mem_capacity_gb+1e-9
        self.granularity_choices[task_id] = dict(requested=requested, actual=len(descriptors),
            windows=sum(len(t.windows_xyxy) for t in descriptors), unique_mb=workload.image_size_mb,
            packet_mb=new_mb, overlap_mb=new_mb-workload.image_size_mb)
        if tuple(t.windows_xyxy for t in old) == tuple(t.windows_xyxy for t in descriptors):
            return
        old_ids = set(task.tile_ids)
        for tile in old:
            self._release_tile_memory(tile, task.source_sat_id)
            del self.tiles[tile.tile_id]
        sat.queue[:] = [t for t in sat.queue if t not in old_ids]
        self.metrics.total_tiles += len(descriptors)-len(old)
        task.num_tiles = len(descriptors)
        task.tile_ids = []
        self._task_tile_index[task_id] = task.tile_ids
        self._task_workloads[task_id] = workload
        self._task_unique_blocks[task_id] = {}
        unique = workload.unique_transport_blocks(len(descriptors))
        for i, desc in enumerate(descriptors):
            tid = f'{task_id}_tile_{i}'
            tile = Tile(tid, task_id, desc.data_size_mb, desc.compute_cost, old[0].vram_req_gb,
                TileState.CREATED, task.source_sat_id, TileTimestamps(task.release_time), task.deadline,
                result_size_mb=desc.result_size_mb, windows_xyxy=desc.windows_xyxy,
                actual_result_size_mb=(sum(outputs[b] for b in desc.windows_xyxy)/1e6 if outputs is not None else None))
            self.tiles[tid] = tile
            task.tile_ids.append(tid)
            self._task_unique_blocks[task_id][tid] = unique[i][1]
            self._enqueue_tile(task.source_sat_id, tile)


class DynamicThreePlan(ThreePlanChoice):
    def __init__(self, env):
        super().__init__(env.cfg.relay_max_hops)
        self.env = env
        self.decided = set()

    def select_actions(self, state):
        groups = {}
        for tid, tile in sorted(state.tiles.items(), key=lambda x: (-x[1]['waiting_time'], x[0])):
            if tile['parent_task_id'] not in self.raw_tasks:
                groups.setdefault(tile['parent_task_id'], {})[tid] = tile
        calendar = Calendar(state)
        chosen = []
        for task, rows in groups.items():
            complete = (task not in self.decided and len(rows) == self.env.tasks[task].num_tiles and
                all(t['state']=='QUEUED' and not t['in_transfer'] and not t['task_progress']['failed'] and
                    t['location']==t['source_sat_id'] for t in rows.values()))
            if not complete:
                chosen.extend(self._select_actions_with_plan(replace(state, tiles=rows), deepcopy(calendar.plan)))
                continue
            options, quotes, trials, views = {}, {}, {}, {}
            for count in (4, 16):
                subset = preview(self.env, state, task, count)
                subset = replace(subset,
                    satellites={sid: {**s, 'committed_compute_s': calendar.busy[sid],
                        'mem_remaining_gb': calendar.memory[sid]+(s['mem_remaining_gb']-state.satellites[sid]['mem_remaining_gb'])}
                        for sid,s in subset.satellites.items()},
                    ground_stations={gid: {**g, 'committed_compute_s': calendar.busy[ground_node(gid)]}
                                     for gid,g in subset.ground_stations.items()})
                variants = {f'local_{count}': [Action(t, ActionType.LOCAL) for t in subset.tiles],
                            f'cgs_{count}': self._select_actions_with_plan(subset, deepcopy(calendar.plan))}
                if count == 16:
                    variants['ground'] = [RawImageAction(t, ActionType.OFFLOAD, raw_hops=self.raw_hops) for t in subset.tiles]
                for name, actions in variants.items():
                    trial = deepcopy(calendar)
                    source = next(iter(rows.values()))['source_sat_id']
                    trial.memory[source] += subset.satellites[source]['mem_remaining_gb']-calendar.memory[source]
                    quote = trial.quote(subset, actions, raw=name=='ground', raw_hops=self.raw_hops)
                    if name != 'ground' and quote is not None:
                        checker = CGSTaskChoice()
                        checker._contact_plan = lambda _: deepcopy(calendar.plan)
                        grouped = checker.quote(subset, actions)
                        if grouped is not None:
                            quote = max(quote, grouped[task])
                    options[name], quotes[name], trials[name], views[name] = actions, quote, trial, subset
            winner = min(options, key=lambda n: quotes[n] if quotes[n] is not None else math.inf)
            if quotes[winner] is None:
                winner = 'cgs_16'
            count = 4 if winner.endswith('_4') else 16
            self.env.repack(task, count)
            if quotes[winner] is not None:
                calendar = trials[winner]
            chosen.extend(options[winner])
            if winner == 'ground':
                self.raw_tasks.add(task)
            self.decided.add(task)
            self.decisions.append(dict(task_id=task, time_seconds=state.time*state.config['dt'],
                quotes_seconds=quotes, selected=winner, **self.env.granularity_choices[task]))
        return chosen
