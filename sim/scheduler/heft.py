"""HEFT 核心：向上排序与插空最早完成；对应 Topcuoglu et al. (2002)。"""

from dataclasses import dataclass
from graphlib import TopologicalSorter
import math
import statistics


@dataclass(frozen=True)
class Placement:
    processor: object
    start: float
    end: float


def first_gap(intervals, ready, duration):
    start = ready
    for left, right in sorted(intervals):
        if start + duration <= left + 1e-9:
            break
        if start < right:
            start = right
    return start


def schedule(costs, successors, communication, average_communication, *,
             initial=None, occupied=None, feasible=None, on_assign=None, arrival_time=None):
    """返回分配与 rank_u；不可达任务及其后继不分配，不修改调用方的日历。"""
    predecessors = {node: [] for node in costs}
    for node, children in successors.items():
        for child in children:
            predecessors[child].append(node)
    order = list(TopologicalSorter(predecessors).static_order())
    ranks = {}
    for node in reversed(order):
        finite = [c for c in costs[node].values() if math.isfinite(c)]
        mean = statistics.mean(finite) if finite else math.inf
        ranks[node] = mean + max((average_communication(node, child) + ranks[child]
                                 for child in successors.get(node, [])), default=0)
    calendars = {p: list(events) for p, events in (occupied or {}).items()}
    placed = dict(initial or {})
    # 同 rank 时采用拓扑稳定顺序，使零成本虚拟节点也保持前驱先行。
    index = {node: i for i, node in enumerate(order)}
    for node in sorted(order, key=lambda n: (-ranks[n], index[n])):
        if node in placed or any(parent not in placed for parent in predecessors[node]):
            continue
        choices = []
        for position, (processor, duration) in enumerate(costs[node].items()):
            if not math.isfinite(duration) or (feasible and not feasible(node, processor)):
                continue
            ready = max(((arrival_time(parent, node, placed[parent].processor, processor, placed[parent].end)
                          if arrival_time else placed[parent].end + communication(
                              parent, node, placed[parent].processor, processor))
                for parent in predecessors[node]), default=0)
            if not math.isfinite(ready):
                continue
            start = first_gap(calendars.get(processor, []), ready, duration)
            choices.append((start + duration, position, processor, start))
        if not choices:
            continue
        end, _, processor, start = min(choices)
        placed[node] = Placement(processor, start, end)
        if end > start:
            calendars.setdefault(processor, []).append((start, end))
        if on_assign:
            on_assign(node, processor)
    return placed, ranks
