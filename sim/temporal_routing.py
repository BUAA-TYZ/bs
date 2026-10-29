"""公开接触窗口上的存储等待转发适配；不是完整 SABR 协议实现。

只读取当前队列、缓存及轨道预测。等待中的包至多每秒重规划一次，
实际发送仍须当前链路可用，继续经过原执行器的内存、竞争与字节检查。
"""

import math

from sim.scheduler.contact_graph import Contact, ContactPlan, quantize
from sim.cache.contacts import reconcile_windows
from sim.topology import link_key
from sim.roles import can_receive


def temporal_delivery_route(env, source, size_mb, links, visited, ready_delay=0.0, *, target_gs_id=None):
    if target_gs_id is not None and target_gs_id not in env.ground_stations:
        return None
    remaining = env.cfg.relay_max_hops - max(0, len(visited) - 1)
    if remaining < 0:
        return None
    now, dt = env.time * env.cfg.dt, env.cfg.dt
    key = (source, size_mb, tuple(visited), ready_delay, target_gs_id)
    waits = getattr(env, "_temporal_route_waits", None)
    if waits is None:
        waits = env._temporal_route_waits = {}
    if now < waits.get(key, -math.inf) - 1e-9:
        return None
    # 定期丢弃已过期键，避免随历史包数无限增长。
    if len(waits) > 4096:
        waits = env._temporal_route_waits = {k: t for k, t in waits.items() if t > now}
    contacts = []
    executable = set()
    blocked = set(visited) - {source}
    guard = env.contact_cache.scan_s if env.cfg.relay_contact_guard else 0.0
    bandwidth = env.topology.cfg.bandwidth_mbps_min
    windows = env.contact_cache.windows_at(now)
    backlog = {}
    for tr in env.transfers:
        pair = (tr.src, tr.dst)
        backlog[pair] = backlog.get(pair, 0.0) + tr.remaining_mb
    for pair, intervals in windows.items():
        a, b = (int(value) for value in pair.split("-"))
        for src, dst in ((a, b), (b, a)):
            if not can_receive(env.cfg, dst):
                continue
            if dst in blocked or dst == source:
                continue
            satellite = env.satellites[dst]
            if satellite.mem_capacity_gb - satellite.mem_used_gb < env.memory_size_gb(size_mb):
                continue
            link = links.get(pair)
            for start, duration in reconcile_windows(intervals, bool(link and link.up), env.contact_cache.scan_s):
                # 当前预测与执行快照冲突时，不把尚未生效的边当成可立即发送。
                if start == 0 and (link is None or not link.up or link.bandwidth_mbps <= 0):
                    continue
                rate = min(bandwidth, link.bandwidth_mbps) / 8 if start == 0 else bandwidth / 8
                if rate <= 0:
                    continue
                end = start + duration - guard
                ready = start + backlog.get((src, dst), 0.0) / rate
                if ready < end:
                    if start == 0:
                        executable.add(len(contacts))
                    contacts.append(Contact(src, dst, ready, end, rate, (end - ready) * rate))
    ground = set()
    for sid in env.satellites:
        for gid, window in env.vis_cache.windows_at(sid, env.time).items():
            if window is None or (target_gs_id is not None and gid != target_gs_id):
                continue
            start, duration = window
            if start == 0 and not env.vis_cache.is_visible(sid, gid, env.ground_stations):
                continue
            rate = env.ground_stations[gid].bandwidth_mbps / 8
            # 与旧路由同口径，只报价该卫星已知下行积压，不窥视未来竞争。
            queued = sum(tr.remaining_mb for tr in env.downlink_transfers + env.ground_tile_transfers
                         if tr.src_sat == sid and tr.gs_id == gid)
            ready, end = start + queued / rate, start + duration
            if ready < end:
                node = ("ground", gid)
                ground.add(node)
                if start == 0:
                    executable.add(len(contacts))
                contacts.append(Contact(sid, node, ready, end, rate, (end - ready) * rate))
    plan = ContactPlan(contacts)
    route = plan.delivery(source, ground, ready_delay, size_mb, dt=dt, max_hops=remaining + 1)
    if route is not None:
        first = contacts[route[1][0]]
        # backlog 用于估算到达时间，当前有积压仍可加入原执行器共享服务。
        if isinstance(first.target, tuple) and route[1][0] in executable:
            gid = first.target[1]
            if env.vis_cache.is_visible(source, gid, env.ground_stations):
                waits.pop(key, None)
                return "ground", gid
        elif not isinstance(first.target, tuple) and route[1][0] in executable:
            link = links.get(link_key(source, first.target))
            current = any(start == 0 and duration - guard >= max(dt, quantize(size_mb * 8 / bandwidth, dt))
                          for start, duration in windows.get(link_key(source, first.target), []))
            if link is not None and link.up and link.bandwidth_mbps > 0 and current:
                waits.pop(key, None)
                return "sat", first.target
        retry = min(1.0, max(dt, quantize(first.start, dt)))
    else:
        retry = 1.0
    waits[key] = now + max(dt, retry)
    return None
