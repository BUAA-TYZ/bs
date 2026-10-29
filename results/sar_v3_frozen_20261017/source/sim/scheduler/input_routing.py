"""共用有界输入路径：当前接触、整片存储转发和逐跳资源校验。"""

import math
from heapq import heappop, heappush
from itertools import count

from sim.marl.decision import candidate_neighbors
from sim.scheduler.contact_graph import quantize
from sim.topology import link_key


def input_route_nodes(plan, path):
    if not path:
        return ()
    nodes = [plan.contacts[path[0]].source]
    for index in path:
        contact = plan.contacts[index]
        if contact.source != nodes[-1]:
            raise ValueError("Input contacts must form a continuous path")
        nodes.append(contact.target)
    return tuple(nodes)


def _hop_arrival(contact, ready, size, dt, *, future=False):
    if ((contact.start != 0 and not future) or contact.rate <= 0 or
            contact.volume + 1e-9 < size):
        return None
    queued = max(0.0, (contact.end - contact.start) * contact.rate - contact.volume)
    ready = max(quantize(ready, dt), quantize(contact.start, dt))
    # 同一接触按累计字节排队；每跳仍需至少一步，不能给每个小包独占一步。
    finish = max(ready + dt, quantize(max(ready, contact.start + queued / contact.rate)
                                    + size / contact.rate, dt))
    if not math.isfinite(finish) or finish > contact.end + 1e-9:
        return None
    return finish + contact.delay


def input_path_arrival(state, plan, tile, memory, path):
    """按当前剩余资源复核固定路径；返回完整输入到达秒数或 None。"""
    src = tile["location"]
    if not path:
        return 0.0
    ground = {("ground", gid) for gid in state.ground_stations}
    ground_relay = bool(state.config.get("ground_input_relay") and
                        plan.contacts[path[-1]].target in ground)
    max_hops = (state.config.get("relay_max_hops", 8) + 1 if ground_relay else
                state.config.get("input_relay_max_hops", 1))
    if len(path) > max_hops:
        return None
    node, ready, visited = src, 0.0, {src}
    visible_ground = {("ground", row["gs_id"]) for row in state.ground_options.get(src, [])}
    for index in path:
        contact = plan.contacts[index]
        target = contact.target
        if contact.source != node or target in visited:
            return None
        if target in state.satellites:
            if memory.get(target, 0) + 1e-12 < tile["data_size_gb"]:
                return None
            link = state.links.get(link_key(node, target))
            if (contact.start != 0 or not link or not link["up"] or
                    link["bandwidth_mbps"] <= 0):
                return None
            if not ground_relay:
                if target not in candidate_neighbors(state, node):
                    return None
        elif (target not in ground or (ground_relay and index != path[-1]) or
              (not ground_relay and (len(path) != 1 or target not in visible_ground))):
            return None
        ready = _hop_arrival(contact, ready, tile["data_size_mb"], state.config["dt"],
                             future=ground_relay)
        if ready is None:
            return None
        node = target
        visited.add(node)
    return ready


def ground_input_options(state, plan, tile, memory):
    """当前星间图上的地面报价；允许未来地面窗口，不承诺执行器固定路径。"""
    if not state.config.get("ground_input_relay"):
        return {}
    source, size, dt = tile["location"], tile["data_size_mb"], state.config["dt"]
    ground = {("ground", gid) for gid in state.ground_stations}
    max_hops = state.config.get("relay_max_hops", 8) + 1
    serial = count()
    queue = [(0.0, 0, next(serial), source, ())]
    best = {(source, 0): 0.0}
    options = {}
    while queue:
        time, hops, _, node, path = heappop(queue)
        if best[node, hops] != time:
            continue
        if node in ground:
            if node not in options:
                options[node] = (time, path)
            if len(options) == len(ground):
                break
            continue
        if hops >= max_hops:
            continue
        visited = {source, *(plan.contacts[index].target for index in path)}
        for index in plan.outgoing.get(node, []):
            contact = plan.contacts[index]
            target = contact.target
            if target in visited:
                continue
            if target in state.satellites:
                if memory.get(target, 0) + 1e-12 < tile["data_size_gb"]:
                    continue
                # 公共路由器仅在当前连通图选首跳，不承诺跨未来 ISL 接触存储携带。
                link = state.links.get(link_key(node, target))
                if (contact.start != 0 or not link or not link["up"] or
                        link["bandwidth_mbps"] <= 0):
                    continue
            elif target not in ground:
                continue
            finish = _hop_arrival(contact, time, size, dt, future=True)
            if finish is None or any(best.get((target, used), math.inf) <= finish
                                     for used in range(hops + 2)):
                continue
            best[target, hops + 1] = finish
            heappush(queue, (finish, hops + 1, next(serial), target, path + (index,)))
    return options


def current_input_options(state, plan, tile, memory):
    """返回处理节点 -> (到达秒数, 接触索引路径)，不修改计划或内存账本。"""
    src = tile["location"]
    satellite_compute = tile["vram_req_gb"] <= state.config["vram_capacity_gb"]
    options = {src: (0.0, ())} if satellite_compute else {}
    max_hops = state.config.get("input_relay_max_hops", 1)
    frontier = [(src, ())]
    while frontier:
        node, prefix = frontier.pop()
        if len(prefix) >= max_hops:
            continue
        for index in plan.outgoing.get(node, []):
            contact = plan.contacts[index]
            path = prefix + (index,)
            time = input_path_arrival(state, plan, tile, memory, path)
            if time is None:
                continue
            target = contact.target
            if target in state.satellites:
                if not satellite_compute:
                    continue
                frontier.append((target, path))
            if target not in options or time < options[target][0]:
                options[target] = (time, path)
    if state.config.get("ground_input_relay"):
        options.update(ground_input_options(state, plan, tile, memory))
    return options


def reserve_input_memory(memory, plan, path, tile):
    """同轮保守占用中转与最终缓存；源数据已在本地，不重复扣减。"""
    route = input_route_nodes(plan, path)
    if route and isinstance(route[-1], tuple):
        # 地面动态中继没有最终星缓存；未来中间节点仅校验，不永久占用。
        nodes = {route[1]} if plan.contacts[path[0]].start == 0 else set()
    else:
        nodes = set(route) - {tile["location"]}
    for node in nodes:
        if node in memory:
            memory[node] -= tile["data_size_gb"]
