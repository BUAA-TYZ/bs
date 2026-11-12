"""冻结日历上的目标分配枚举；不执行调度、不修改原Calendar。"""
from collections import Counter
from copy import deepcopy
from dataclasses import replace
import gzip
import hashlib
import itertools
import json
import math
from pathlib import Path
import pickle
import time

from analysis.sar_three_plan import Calendar
from sim.entities import Action, ActionType
from sim.scheduler.literature import ground_node, input_options


def target(action, tile):
    if action.action_type == ActionType.LOCAL:
        return tile['location']
    if action.action_type != ActionType.OFFLOAD:
        raise ValueError('CGS includes a non-processing action')
    return ground_node(action.target_gs_id) if action.target_gs_id is not None else action.target_sat_id


def actions_for(state, ordered, positions):
    actions = []
    for tid, position in zip(ordered, positions, strict=True):
        source = state.tiles[tid]['location']
        if position == source:
            actions.append(Action(tid, ActionType.LOCAL))
        elif isinstance(position, tuple):
            actions.append(Action(tid, ActionType.OFFLOAD, target_gs_id=position[1],
                                  relay_to_ground=bool(state.config.get('ground_input_relay'))))
        else:
            actions.append(Action(tid, ActionType.OFFLOAD, target_sat_id=position))
    return actions


def quote(snapshot, positions, ordered=None):
    state = snapshot['state']
    ordered = ordered or sorted(state.tiles, key=lambda tid: int(tid.rsplit('_', 1)[1]))
    return deepcopy(snapshot['calendar']).quote(state, actions_for(state, ordered, positions))


def compositions(total, parts):
    if parts == 1:
        yield (total,)
    else:
        for first in range(total + 1):
            for rest in compositions(total - first, parts - 1):
                yield (first, *rest)


def assignments(blocks, positions):
    if blocks == 4:
        yield from itertools.product(range(positions), repeat=blocks)
    elif blocks == 16:
        for counts in compositions(blocks, positions):
            yield tuple(i for i, count in enumerate(counts) for _ in range(count))
    else:
        raise ValueError(f'Unregistered actual block count: {blocks}')


def load_snapshot(path, expected):
    data = Path(path).read_bytes()
    assert hashlib.sha256(data).hexdigest() == expected
    # 只加载本实验自行生成且登记hash核实的快照，不接收外部pickle。
    return pickle.loads(gzip.decompress(data))


def candidates(snapshot):
    state = snapshot['state']
    ordered = sorted(state.tiles, key=lambda tid: int(tid.rsplit('_', 1)[1]))
    original = {action.tile_id: action for action in snapshot['actions']}
    assert set(original) == set(ordered)
    if snapshot['calendar_quote'] is None:
        return dict(status='NO_FINITE_CGS_QUOTE', blocks=len(ordered))
    cgs = tuple(target(original[tid], state.tiles[tid]) for tid in ordered)
    normalized = quote(snapshot, cgs, ordered)
    assert normalized is not None and abs(normalized-snapshot['calendar_quote']) < 1e-8
    original_quote = deepcopy(snapshot['calendar']).quote(state, snapshot['actions'])
    assert original_quote == snapshot['calendar_quote']
    scores = {}
    single = []
    for tid in ordered:
        tile = state.tiles[tid]
        view = replace(state, tiles={tid:tile}, pending_task_tiles={tile['parent_task_id']:{tid:tile}})
        allowed = input_options(view, snapshot['calendar'].plan, tile, snapshot['calendar'].memory)
        values = []
        for position in sorted(allowed, key=repr):
            value = deepcopy(snapshot['calendar']).quote(view, actions_for(view, [tid], [position]))
            if value is not None:
                scores[position] = min(scores.get(position, math.inf), value)
                values.append(dict(position=position, quote=value))
        single.append(dict(tile=tid, feasible=values))
    ranked = sorted(scores, key=lambda p:(scores[p], repr(p)))
    used = set(cgs)
    # CGS实际位置优先保留；再按单块最早报价补入排名前五的位置。
    selected = set(used)
    for position in ranked[:5]:
        if len(selected) < 8:
            selected.add(position)
    selected = sorted(selected, key=lambda p:(scores.get(p, math.inf), repr(p)))
    status = 'OVER_CAP' if len(used) > 8 else 'ELIGIBLE'
    count = len(selected)**4 if len(ordered)==4 else math.comb(16+len(selected)-1,len(selected)-1)
    return dict(status=status, blocks=len(ordered), ordered=ordered, cgs=cgs,
        cgs_quote=normalized, used_positions=len(used), positions=selected,
        top_five=ranked[:5], local_search_positions=sorted(used|set(ranked[:5]),key=repr),
        single_block_quotes=single, planned_quotes=count if status=='ELIGIBLE' else 0,
        cross_satellite_blocks=sum(not isinstance(p,tuple) and p!=state.tiles[t]['source_sat_id'] for t,p in zip(ordered,cgs)))


def enumerate_snapshot(snapshot, plan, raw_path, progress=lambda _:None):
    assert plan['status'] == 'ELIGIBLE'
    started = time.monotonic()
    positions = [tuple(p) if isinstance(p,list) else p for p in plan['positions']]
    cgs = tuple(tuple(p) if isinstance(p,list) else p for p in plan['cgs'])
    best, best_assignment = plan['cgs_quote'], tuple(positions.index(p) for p in cgs)
    canonical_best = None
    canonical_assignment = None
    valid = count = 0
    with gzip.open(raw_path, 'xt', encoding='utf8') as raw:
        for index, assignment in enumerate(assignments(plan['blocks'],len(positions))):
            value = quote(snapshot, [positions[i] for i in assignment], plan['ordered'])
            raw.write(json.dumps([index,value],allow_nan=False,separators=(',',':'))+'\n')
            count += 1
            if value is not None:
                valid += 1
                if canonical_best is None or value < canonical_best:
                    canonical_best, canonical_assignment = value, assignment
                if value < best:
                    best, best_assignment = value, assignment
            if count % 1000 == 0:
                progress(dict(evaluated=count,total=plan['planned_quotes'],wall_seconds=time.monotonic()-started))
    assert count == plan['planned_quotes']
    assert abs(quote(snapshot,[positions[i] for i in best_assignment],plan['ordered'])-best)<1e-8
    return dict(cgs_quote=plan['cgs_quote'],best_quote=best,
        canonical_best_quote=canonical_best,canonical_best_assignment=canonical_assignment,
        best_assignment=best_assignment,improvement_s=plan['cgs_quote']-best,
        enumeration_minus_cgs_s=best-plan['cgs_quote'],evaluated=count,finite=valid,
        incumbent_better_than_canonical=canonical_best is None or plan['cgs_quote']<canonical_best,
        exact_assignment_space=plan['blocks']==4,wall_seconds=time.monotonic()-started)


def neighbors(assignment, count):
    seen = set()
    for block in range(len(assignment)):
        for position in range(count):
            if position != assignment[block]:
                row=list(assignment);row[block]=position;seen.add(tuple(row))
    for left in range(len(assignment)):
        for right in range(left+1,len(assignment)):
            if assignment[left] != assignment[right]:
                row=list(assignment);row[left],row[right]=row[right],row[left];seen.add(tuple(row))
    return sorted(seen)


def local_search(snapshot, plan, raw_path, progress=lambda _:None):
    assert plan['status']=='OVER_CAP'
    started=time.monotonic()
    positions=[tuple(p) if isinstance(p,list) else p for p in plan['local_search_positions']]
    current=tuple(positions.index(tuple(p) if isinstance(p,list) else p) for p in plan['cgs'])
    best=plan['cgs_quote'];evaluated=0;accepted=0;reason='ITERATION_CAP'
    with gzip.open(raw_path,'xt',encoding='utf8') as raw:
        for iteration in range(20):
            next_best,next_assignment=best,current
            for candidate in neighbors(current,len(positions)):
                value=quote(snapshot,[positions[i] for i in candidate],plan['ordered']);evaluated+=1
                raw.write(json.dumps([iteration,candidate,value],allow_nan=False,separators=(',',':'))+'\n')
                if value is not None and value<next_best-1e-9:
                    next_best,next_assignment=value,candidate
            if next_assignment==current:
                reason='NO_IMPROVEMENT';break
            current,best=next_assignment,next_best;accepted+=1
            progress(dict(iteration=iteration+1,evaluated=evaluated,wall_seconds=time.monotonic()-started))
    return dict(cgs_quote=plan['cgs_quote'],best_quote=best,best_assignment=current,
        improvement_s=plan['cgs_quote']-best,iterations_accepted=accepted,evaluated=evaluated,
        stop_reason=reason,iteration_limit=20,wall_seconds=time.monotonic()-started,
        meaning='Found improvement is a lower bound on the true quote optimality gap within these feasible allocations')
