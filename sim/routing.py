"""共享规则路由：当前链路图和星地窗口估计，不读取未来拥塞或任务。"""

import math

import networkx as nx

from sim.topology import link_key


def delivery_route(env, source, size_mb, links, visited, ready_delay=0.0):
    """返回当前可执行的首跳；路径估计不代替逐步传输与资源检查。"""
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
            if dst in blocked or env.satellites[dst].mem_capacity_gb - env.satellites[dst].mem_used_gb < size_mb / 1024:
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
    distances, paths = nx.single_source_dijkstra(graph, source)
    best = None
    for sid, distance in distances.items():
        path = paths[sid]
        if len(path) - 1 > remaining_hops:
            continue
        arrival = ready_delay
        for src, dst in zip(path, path[1:]):
            arrival += graph[src][dst]["weight"]
            if arrival > graph[src][dst]["contact_end"]:
                break
        else:
            arrival = None
        if arrival is not None:
            continue
        for gs_id, window in env.vis_cache.windows_at(sid, env.time).items():
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
