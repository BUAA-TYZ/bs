"""按公开接触图重放共享首跳路由的结果投递估计。

只使用当前广播、接触窗口和调用方的接触容量账本；不访问环境未来状态。
当前缓存被视为不变，已知字节按窗口前部服务估计，不预测未来新竞争。
接触总量预约不等于精确带宽日历，因此有竞争时只是报价，不是执行保证。
"""

from bisect import bisect_right
import math

import networkx as nx

from sim.routing import _bounded_paths
from sim.scheduler.contact_graph import quantize


def execution_delivery(state, plan, source, ready, size, *, dt=None,
                       max_hops=None, guard_s=None, visited=None):
    """返回 (完整到达秒数, 原 plan 的接触索引路径)，无已知路径时返回 None。

    时间相对于传入 state；plan 必须未预先扣除 guard。max_hops 包含最后
    一次星地下传，与 ContactPlan.delivery 的口径相同。返回路径可由调用方
    plan.reserve 预约，本查询不修改 state 或 plan。
    """
    dt = state.config["dt"] if dt is None else dt
    if max_hops is None:
        max_hops = (state.config.get("relay_max_hops", 8)
                    if state.config.get("result_relay") else 0) + 1
    guard_s = (state.config.get("contact_scan_s", 0)
               if state.config.get("relay_contact_guard", True) else 0) if guard_s is None else guard_s
    if not all(math.isfinite(x) for x in (ready, size, dt, guard_s)) or min(ready, size, guard_s) < 0 or dt <= 0:
        raise ValueError("Expected finite nonnegative ready, size, guard and positive dt")
    if type(max_hops) is not int or max_hops < 0:
        raise ValueError("max_hops must be a nonnegative integer")
    ground = {("ground", gid) for gid in state.ground_stations}
    if source in ground:
        return quantize(ready, dt), ()
    if source not in state.satellites or max_hops == 0:
        return None
    history = [source] if visited is None else list(visited)
    if not history or history[-1] != source:
        raise ValueError("visited must end at source")
    if len(history) > max_hops:
        return None

    rate_floor = state.config.get("relay_bandwidth_floor_mbps",
                                  state.config.get("isl_bandwidth_floor_mbps", 0)) / 8
    memory_scale = state.config.get("memory_mb_per_gb", 1024)
    contacts = plan.contacts
    events = set()
    for c in contacts:
        events.update((quantize(max(0, c.start), dt), quantize(max(0, c.end), dt)))
        if c.target in state.satellites:
            events.add(quantize(max(0, c.end - guard_s), dt))
        if c.rate > 0:
            used = max(0, (c.end - c.start) * c.rate - c.volume)
            events.add(quantize(max(0, c.start + used / c.rate), dt))
    events = sorted(events)
    now, node, path_indices = quantize(ready, dt), source, []

    def service(contact, at, isl):
        if contact.rate <= 0 or contact.volume + 1e-9 < size:
            return None
        bandwidth = min(contact.rate, rate_floor) if isl and guard_s > 0 and rate_floor > 0 else contact.rate
        # 公开积压及本轮容量预约按窗口前部排队近似；未知未来到达不计入。
        occupied = max(0, (contact.end - contact.start) * contact.rate - contact.volume)
        backlog = max(0, occupied - max(0, at - contact.start) * bandwidth)
        return max(dt, quantize((backlog + size) / bandwidth, dt))

    while True:
        remaining = max_hops - len(history)
        blocked = set(history) - {node}
        graph = nx.DiGraph()
        graph.add_nodes_from(state.satellites)
        for index, c in enumerate(contacts):
            if c.source not in state.satellites or c.target not in state.satellites:
                continue
            if not c.start <= now < c.end or c.target in blocked:
                continue
            if state.satellites[c.target]["mem_remaining_gb"] + 1e-12 < size / memory_scale:
                continue
            duration = service(c, now, True)
            end = c.end - now - guard_s
            if duration is None or duration > end + 1e-9:
                continue
            old = graph.get_edge_data(c.source, c.target)
            if old is None or (duration, -end) < (old["weight"], -old["contact_end"]):
                graph.add_edge(c.source, c.target, weight=duration, contact_end=end, index=index)

        best = None
        for exit_sat, distance, path in _bounded_paths(graph, node, remaining, 0):
            for index in plan.outgoing.get(exit_sat, []):
                c = contacts[index]
                if c.target not in ground or c.end <= now:
                    continue
                duration = service(c, now, False)
                if duration is None:
                    continue
                finish = max(now + distance, quantize(c.start, dt)) + duration
                if finish > c.end + 1e-9:
                    continue
                key = (finish, len(path), exit_sat, c.target[1])
                if best is None or key < best[0]:
                    best = key, path, index, duration
        if best is not None:
            _, path, ground_index, duration = best
            if len(path) > 1:
                edge = graph[path[0]][path[1]]
                path_indices.append(edge["index"])
                now += edge["weight"]
                node = path[1]
                history.append(node)
                continue
            if contacts[ground_index].start <= now:
                return now + duration, tuple(path_indices + [ground_index])
        # 路由没有可执行动作时等公开事件；不能直接沿 CGS 的未来中间边走。
        next_event = bisect_right(events, now + 1e-9)
        if next_event == len(events):
            return None
        now = events[next_event]
