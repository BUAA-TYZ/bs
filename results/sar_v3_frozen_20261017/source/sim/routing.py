"""共享规则路由：当前链路图和星地窗口估计，不读取未来拥塞或任务。"""

import math
from heapq import heappop, heappush
from itertools import count

import networkx as nx

from sim.topology import link_key


def _bounded_paths(graph, source, max_hops, ready_delay):
    """按节点与跳数保留最早到达，扩展时检查累计接触时限。"""
    if max_hops < 0:
        return
    labels = {(source, 0): (0.0, [source])}
    sequence = count()
    queue = [(0.0, next(sequence), source, 0)]
    while queue:
        distance, _, node, hops = heappop(queue)
        current, path = labels[node, hops]
        if distance != current:
            continue
        yield node, distance, path
        if hops == max_hops:
            continue
        for target, edge in graph[node].items():
            arrival = distance + edge["weight"]
            if ready_delay + arrival > edge["contact_end"]:
                continue
            key = (target, hops + 1)
            if key not in labels or arrival < labels[key][0]:
                labels[key] = (arrival, path + [target])
                heappush(queue, (arrival, next(sequence), target, hops + 1))


def delivery_route(env, source, size_mb, links, visited, ready_delay=0.0, *, target_gs_id=None):
    """返回当前可执行的首跳；路径估计不代替逐步传输与资源检查。"""
    if env.cfg.relay_routing == "temporal":
        from sim.temporal_routing import temporal_delivery_route
        return temporal_delivery_route(env, source, size_mb, links, visited, ready_delay,
                                       target_gs_id=target_gs_id)
    if target_gs_id is not None and target_gs_id not in env.ground_stations:
        return None
    graph = nx.DiGraph()
    graph.add_nodes_from(env.satellites)
    blocked = set(visited) - {source}
    remaining_hops = env.cfg.relay_max_hops - max(0, len(visited) - 1)
    contacts = None
    if env.cfg.relay_contact_guard:
        key = (env.time, id(env.contact_cache))
        snapshot = getattr(env, "_relay_contact_snapshot", None)
        if snapshot is None or snapshot[0] != key:
            snapshot = (key, env.contact_cache.windows_at(env.time * env.cfg.dt))
            env._relay_contact_snapshot = snapshot
        contacts = snapshot[1]
    for link in links.values():
        if not link.up or link.bandwidth_mbps <= 0:
            continue
        for src, dst in ((link.i, link.j), (link.j, link.i)):
            if dst in blocked or env.satellites[dst].mem_capacity_gb - env.satellites[dst].mem_used_gb < env.memory_size_gb(size_mb):
                continue
            backlog = sum(tr.remaining_mb for tr in env.transfers if tr.src == src and tr.dst == dst)
            bandwidth = link.bandwidth_mbps
            if contacts is not None:
                bandwidth = min(bandwidth, env.topology.cfg.bandwidth_mbps_min)
            delay = max(env.cfg.dt, math.ceil((backlog + size_mb) * 8 / bandwidth / env.cfg.dt) * env.cfg.dt)
            contact_end = math.inf
            if contacts is not None:
                # 扣去一个扫描周期，避免把采样窗口边界当作精确断链时刻。
                contact_end = next((duration - env.contact_cache.scan_s
                                    for start, duration in contacts.get(link_key(src, dst), [])
                                    if start == 0), 0)
                if delay > contact_end:
                    continue
            graph.add_edge(src, dst, weight=delay, contact_end=contact_end)
    best = None
    for sid, distance, path in _bounded_paths(graph, source, remaining_hops, ready_delay):
        for gs_id, window in env.vis_cache.windows_at(sid, env.time).items():
            if target_gs_id is not None and gs_id != target_gs_id:
                continue
            if window is None:
                continue
            start, duration = window
            gs = env.ground_stations[gs_id]
            backlog = sum(tr.remaining_mb for tr in env.downlink_transfers + env.ground_tile_transfers
                          if tr.src_sat == sid and tr.gs_id == gs_id)
            downlink = max(env.cfg.dt, math.ceil((backlog + size_mb) * 8 / gs.bandwidth_mbps / env.cfg.dt) * env.cfg.dt)
            finish = max(ready_delay + distance, start) + downlink
            if finish > start + duration:
                continue
            candidate = (finish, len(path), sid, gs_id, path)
            if best is None or candidate[:4] < best[:4]:
                best = candidate
    if best is None:
        return None
    path, gs_id = best[4], best[3]
    if len(path) > 1:
        return ("sat", path[1])
    if env.vis_cache.is_visible(source, gs_id, env.ground_stations):
        return ("ground", gs_id)
    return None
