"""文献方法的计算任务适配；边界与公式映射见 docs/thesis/literature_baselines.md。"""

from collections import Counter
import math
import statistics

from sim.entities import Action, ActionType
from sim.marl.decision import candidate_neighbors
from sim.scheduler.contact_graph import Contact, ContactPlan, arrival, choose_opportunity, quantize
from sim.scheduler.heft import Placement, schedule
from sim.scheduler.input_routing import (
    current_input_options, input_path_arrival, input_route_nodes, reserve_input_memory,
)
from sim.topology import link_key


def ground_node(gid):
    return ("ground", gid)


def contact_plan(state):
    contacts = []
    for src, satellite in state.satellites.items():
        for dst in state.satellites:
            if src == dst:
                continue
            link = state.links.get(link_key(src, dst))
            if not link or link["bandwidth_mbps"] <= 0:
                continue
            backlog = satellite.get("isl_backlog_mb", {}).get(dst, 0)
            rate = link["bandwidth_mbps"] / 8
            for start, duration in link.get("contact_windows", []):
                if start == 0 and not link["up"]:
                    continue
                volume = duration * rate
                used = min(backlog, volume)
                backlog -= used
                contacts.append(Contact(src, dst, start, start + duration, rate, volume - used))
        for gid, window in satellite.get("next_gs_windows", {}).items():
            if window is None:
                continue
            start, duration = window
            rate = state.ground_stations[gid]["bandwidth_mbps"] / 8
            backlog = satellite.get("ground_backlog_mb", {}).get(gid, 0)
            contacts.append(Contact(src, ground_node(gid), start, start + duration,
                                    rate, max(0, rate * duration - backlog)))
    return ContactPlan(contacts)


def input_options(state, plan, tile, memory, *, future_ground=False):
    if state.config.get("input_relay_max_hops", 1) > 1 or state.config.get("ground_input_relay"):
        return current_input_options(state, plan, tile, memory)
    src, size, dt = tile["location"], tile["data_size_mb"], state.config["dt"]
    options = {}
    if tile["vram_req_gb"] <= state.config["vram_capacity_gb"]:
        options[src] = (0, ())
        eligible = set(candidate_neighbors(state, src))
    else:
        eligible = set()
    eligible.update(ground_node(o["gs_id"]) for o in state.ground_options.get(src, []))
    if future_ground:
        eligible.update(ground_node(gid) for gid in state.ground_stations)
    for index in plan.outgoing.get(src, []):
        c = plan.contacts[index]
        if (c.start > 0 and not (future_ground and isinstance(c.target, tuple))) or c.target not in eligible:
            continue
        if c.target in state.satellites and memory[c.target] + 1e-12 < tile["data_size_gb"]:
            continue
        time = arrival(c, 0, size, dt)
        if time is not None and (c.target not in options or time < options[c.target][0]):
            options[c.target] = (time, (index,))
    return options


def action_for(tid, src, processor, *, plan=None, path=(), ground_relay=False):
    if processor == src:
        return Action(tid, ActionType.LOCAL)
    if isinstance(processor, tuple):
        if ground_relay:
            return Action(tid, ActionType.OFFLOAD, target_gs_id=processor[1], relay_to_ground=True)
        return Action(tid, ActionType.OFFLOAD, target_gs_id=processor[1])
    if len(path) > 1:
        return Action(tid, ActionType.OFFLOAD, target_sat_id=processor,
                      input_path=input_route_nodes(plan, path))
    return Action(tid, ActionType.OFFLOAD, target_sat_id=processor)


def processing_state(state):
    rate = {sid: s["compute_rate"] for sid, s in state.satellites.items()}
    busy = {sid: s["committed_compute_s"] for sid, s in state.satellites.items()}
    for gid, gs in state.ground_stations.items():
        rate[ground_node(gid)] = gs["compute_rate"]
        busy[ground_node(gid)] = gs["committed_compute_s"]
    return rate, busy


def compute_service(state, seconds):
    return seconds if state.config.get("compute_service_mode") == "work_conserving" else quantize(seconds, state.config["dt"])


class CGSComputeAdapted:
    """将 CGS 的产出机会替换为计算完成事件，按接触容量逐片接纳。"""

    def __init__(self, future_ground=False):
        # 扩展只用于独立命名的实验候选，文献适配基线默认保持即时卸载。
        self.future_ground = future_ground
        self.stats = Counter()

    def _contact_plan(self, state):
        return contact_plan(state)

    def select_actions(self, state):
        if not state.tiles:
            return []
        return self._select_actions_with_plan(state, self._contact_plan(state))

    def _select_actions_with_plan(self, state, plan):
        rate, busy = processing_state(state)
        memory = {sid: s["mem_remaining_gb"] for sid, s in state.satellites.items()}
        ground = {ground_node(gid) for gid in state.ground_stations}
        dt = state.config["dt"]
        ground_relay = bool(state.config.get("ground_input_relay"))
        input_relay = state.config.get("input_relay_max_hops", 1) > 1 or ground_relay
        actions = []
        for tid, tile in sorted(state.tiles.items(), key=lambda x: (-x[1]["waiting_time"], x[0])):
            if tile["in_transfer"]:
                continue
            if tile["state"] == "READY":
                actions.append(Action(tid, ActionType.LOCAL))
                continue
            if tile["task_progress"]["failed"]:
                actions.append(Action(tid, ActionType.WAIT))
                continue
            self.stats["requests"] += 1
            inputs = input_options(state, plan, tile, memory, future_ground=self.future_ground)
            ready = {p: max(busy[p], tx) + compute_service(state, tile["compute_cost"] / rate[p])
                     for p, (tx, _) in inputs.items() if rate[p] > 0}
            hops = state.config.get("relay_max_hops", 8) if state.config.get("result_relay") else 0
            opportunities = [(quantize(t, dt) if state.config.get("compute_service_mode") == "work_conserving" else t, p, p)
                             for p, t in ready.items()]
            if input_relay:
                choice = None
                for opportunity in sorted(opportunities, key=lambda row: row[0]):
                    processor = opportunity[1]
                    input_path = inputs[processor][1]
                    plan.reserve(input_path, tile["data_size_mb"])
                    try:
                        candidate = choose_opportunity([opportunity], plan, ground,
                                                       tile["result_size_mb"], dt=dt,
                                                       max_hops=hops + 1)
                    finally:
                        for index in input_path:
                            plan.contacts[index].volume += tile["data_size_mb"]
                    if candidate is not None and (choice is None or candidate[0] < choice[0]):
                        choice = candidate
            else:
                choice = choose_opportunity(opportunities, plan, ground,
                                            tile["result_size_mb"], dt=dt, max_hops=hops + 1)
            deadline = tile.get("deadline_remaining_s")
            if choice is None or (deadline is not None and
                    choice[0] + state.config.get("postprocessing_delay_s", 0) > deadline):
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_no_feasible_delivery"] += 1
                continue
            _, processor, path = choice
            plan.reserve(inputs[processor][1], tile["data_size_mb"])
            plan.reserve(path, tile["result_size_mb"])
            busy[processor] = ready[processor]
            reserve_input_memory(memory, plan, inputs[processor][1], tile)
            visible_ground = {ground_node(o["gs_id"]) for o in state.ground_options.get(tile["location"], [])}
            if isinstance(processor, tuple) and processor not in visible_ground and not ground_relay:
                # 未来机会只产生 WAIT，实际窗口到来后重新校验资源，不能提前发送。
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_ground_window"] += 1
            else:
                actions.append(action_for(tid, tile["location"], processor,
                                          plan=plan, path=inputs[processor][1],
                                          ground_relay=ground_relay))
                self.stats["accepted"] += 1
        return actions


class HEFTAdapted:
    """滚动构造源数据->当前可调度分片->地面汇聚的 DAG，执行 HEFT。"""

    def __init__(self):
        self.stats = Counter()

    def select_actions(self, state):
        if not state.tiles:
            return []
        plan = contact_plan(state)
        rate, busy = processing_state(state)
        memory = {sid: s["mem_remaining_gb"] for sid, s in state.satellites.items()}
        tiles = {tid: t for tid, t in state.tiles.items() if not t["in_transfer"] and
                 t["state"] == "QUEUED" and not t["task_progress"]["failed"]}
        costs, successors, initial, edges = {}, {}, {}, {}
        dt = state.config["dt"]
        ground_relay = bool(state.config.get("ground_input_relay"))
        input_relay = state.config.get("input_relay_max_hops", 1) > 1 or ground_relay
        input_paths = {}
        result_delays = {}
        for tid, tile in tiles.items():
            root, sink, src = ("source", tid), ("sink", tile["parent_task_id"]), tile["location"]
            costs[root] = {src: 0}
            initial[root] = Placement(src, 0, 0)
            successors[root], successors[tid] = [tid], [sink]
            costs[sink] = {ground_node(gid): 0 for gid in state.ground_stations}
            inputs = input_options(state, plan, tile, memory)
            costs[tid] = {p: compute_service(state, tile["compute_cost"] / rate[p])
                          for p in inputs if rate[p] > 0}
            input_paths.update({(tid, p): path for p, (_, path) in inputs.items()})
            edges[root, tid] = {(src, p): tx for p, (tx, _) in inputs.items()}
            deliveries = {}
            for p in inputs:
                for gid in state.ground_stations:
                    g = ground_node(gid)
                    if p == g:
                        deliveries[p, g] = 0
                    elif p in state.satellites:
                        key = p, g, tile["result_size_mb"]
                        if key not in result_delays:
                            hops = state.config.get("relay_max_hops", 8) if state.config.get("result_relay") else 0
                            route = plan.delivery(p, {g}, 0, tile["result_size_mb"], dt=dt, max_hops=hops + 1)
                            result_delays[key] = route[0] if route else math.inf
                        deliveries[p, g] = result_delays[key]
            edges[tid, sink] = deliveries

        def average(a, b):
            values = [v for v in edges[a, b].values() if math.isfinite(v)]
            return statistics.mean(values) if values else 0

        def feasible(node, p):
            if input_relay and node in tiles:
                return input_path_arrival(state, plan, tiles[node], memory,
                                          input_paths[node, p]) is not None
            if node not in tiles or p not in memory or p == tiles[node]["location"]:
                return True
            return memory[p] + 1e-12 >= tiles[node]["data_size_gb"]

        def assigned(node, p):
            if input_relay and node in tiles:
                path = input_paths[node, p]
                plan.reserve(path, tiles[node]["data_size_mb"])
                reserve_input_memory(memory, plan, path, tiles[node])
                return
            if node in tiles and p in memory and p != tiles[node]["location"]:
                memory[p] -= tiles[node]["data_size_gb"]

        def arrival_time(parent, node, p, q, end):
            if input_relay and node in tiles:
                # 排名使用初始边权；真正插入日历前以剩余容量复核输入路径。
                time = input_path_arrival(state, plan, tiles[node], memory, input_paths[node, q])
                return end + time if time is not None else math.inf
            # 计算日历保留实际秒，结果及汇聚节点只能在步末观察到完成。
            ready = quantize(end, dt) if parent in tiles else end
            return ready + edges[parent, node].get((p, q), math.inf)

        placed, _ = schedule(costs, successors, lambda a, b, p, q: edges[a, b].get((p, q), math.inf),
                             average, initial=initial, occupied={p: [(0, t)] for p, t in busy.items() if t},
                             feasible=feasible, on_assign=assigned,
                             arrival_time=arrival_time if input_relay or state.config.get("compute_service_mode") == "work_conserving" else None)
        actions = []
        for tid, tile in state.tiles.items():
            if tile["in_transfer"]:
                continue
            self.stats["requests"] += 1
            if tile["state"] == "READY":
                actions.append(Action(tid, ActionType.LOCAL))
            elif tid in placed:
                processor = placed[tid].processor
                actions.append(action_for(tid, tile["location"], processor,
                                          plan=plan, path=input_paths[tid, processor],
                                          ground_relay=ground_relay))
                self.stats["accepted"] += 1
            else:
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_no_assignment"] += 1
        return actions
