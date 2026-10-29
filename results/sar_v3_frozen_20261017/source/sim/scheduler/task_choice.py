"""复核整轮本地/CGS两套方案，按整图交付而非单片产出选择。

输入按同轮共享链路总字节保守报价，计算使用实测组服务；结果按处理节点聚合
报价。公开预测不保证未来窗口或未知竞争，部分派发任务仍沿用原门控。
"""

from collections import Counter
from dataclasses import replace
import math

from sim.entities import Action, ActionType
from sim.scheduler.contact_graph import quantize
from sim.scheduler.input_routing import input_route_nodes, reserve_input_memory
from sim.scheduler.literature import ground_node, input_options, processing_state
from sim.scheduler.task_gate import CGSTaskGate, whole_service


def batch_arrivals(plan, paths, sizes, dt):
    """每跳等待整批已知字节，避免把并发流误当独占带宽的FIFO小包。"""
    load = Counter()
    for tid, path in paths.items():
        for index in path:
            load[index] += sizes[tid]
    if any(plan.contacts[i].volume + 1e-9 < size for i, size in load.items()):
        return None
    arrivals = {}
    for tid, path in paths.items():
        ready = 0.0
        for index in path:
            contact = plan.contacts[index]
            if contact.rate <= 0:
                return None
            backlog = max(0, (contact.end - contact.start) * contact.rate - contact.volume)
            ready = (max(quantize(ready, dt), quantize(contact.start, dt))
                     + max(dt, quantize((backlog + load[index]) / contact.rate, dt))
                     + contact.delay)
            if ready > contact.end + 1e-9:
                return None
        arrivals[tid] = ready
    for index, size in load.items():
        plan.reserve((index,), size)
    return arrivals


class CGSTaskChoice(CGSTaskGate):
    def quote(self, state, actions):
        plan = self._contact_plan(state)
        memory = {sid: s["mem_remaining_gb"] for sid, s in state.satellites.items()}
        paths, processors, sizes = {}, {}, {}
        selected = {action.tile_id: action for action in actions}
        if set(selected) != set(state.tiles):
            return None
        for tid, tile in state.tiles.items():
            action = selected[tid]
            if action.action_type == ActionType.LOCAL:
                processor = tile["location"]
            elif action.action_type == ActionType.OFFLOAD:
                processor = (ground_node(action.target_gs_id) if action.target_gs_id is not None
                             else action.target_sat_id)
            else:
                return None
            options = input_options(state, plan, tile, memory)
            if processor not in options:
                return None
            path = options[processor][1]
            if action.input_path and tuple(action.input_path) != input_route_nodes(plan, path):
                return None
            paths[tid], processors[tid], sizes[tid] = path, processor, tile["data_size_mb"]
            reserve_input_memory(memory, plan, path, tile)
        arrivals = batch_arrivals(plan, paths, sizes, state.config["dt"])
        if arrivals is None:
            return None
        rate, busy = processing_state(state)
        groups = {}
        for tid, tile in sorted(state.tiles.items(), key=lambda row: (-row[1]["waiting_time"], row[0])):
            groups.setdefault((tile["parent_task_id"], processors[tid]), []).append(tid)
        ground = {ground_node(gid) for gid in state.ground_stations}
        hops = state.config.get("relay_max_hops", 8) if state.config.get("result_relay") else 0
        latency = {}
        for (task, processor), ids in groups.items():
            tiles = [state.tiles[tid] for tid in ids]
            if rate[processor] <= 0:
                return None
            service = (sum(tile["compute_cost"] for tile in tiles) / rate[processor]
                       if processor in ground else whole_service(state, tiles, processor))
            ready = max(busy[processor], max(arrivals[tid] for tid in ids)) + service
            busy[processor] = ready
            size = sum(tile["result_size_mb"] for tile in tiles)
            route = plan.delivery(processor, ground, quantize(ready, state.config["dt"]), size,
                                  dt=state.config["dt"], max_hops=hops + 1)
            if route is None:
                return None
            plan.reserve(route[1], size)
            end = max(tile["waiting_time"] for tile in tiles) + route[0]
            latency[task] = max(latency.get(task, 0), end + state.config.get("postprocessing_delay_s", 0))
        return latency

    def select_actions(self, state):
        active = {tid: tile for tid, tile in state.tiles.items()
                  if tile["state"] == "QUEUED" and not tile["in_transfer"] and not tile["task_progress"]["failed"]}
        tasks = {tile["parent_task_id"] for tile in active.values()}
        pending = {tid for task, rows in state.pending_task_tiles.items() if task in tasks for tid in rows}
        progress = [tile["task_progress"] for tile in active.values()]
        counts = Counter(tile["parent_task_id"] for tile in active.values())
        if (not active or pending != set(active) or any(
                any(row.get(key, 0) for key in ("done", "running", "awaiting_delivery")) for row in progress)
                or any(tile["task_progress"].get("total", counts[tile["parent_task_id"]])
                       != counts[tile["parent_task_id"]] for tile in active.values())):
            self.stats["choice_fallback"] += 1
            return super().select_actions(state)
        subset = replace(state, tiles=active)
        local = [Action(tid, ActionType.LOCAL) for tid in active]
        cgs = self._select_actions_with_plan(subset, self._contact_plan(subset))
        quotes = {"local": self.quote(subset, local), "cgs": self.quote(subset, cgs)}
        def score(name):
            quote = quotes[name]
            if quote is None:
                return (-1, -math.inf)
            return (sum(value <= self.sla_seconds for value in quote.values()),
                    -sum(min(value, self.sla_seconds) for value in quote.values()))
        winner = max(("local", "cgs"), key=score)
        self.last_prediction = {"quotes": quotes, "selected": winner}
        if quotes[winner] is None:
            self.stats["choice_no_quote"] += 1
            return super().select_actions(state)
        self.stats["choice_" + winner + "_tasks"] += len(tasks)
        chosen = local if winner == "local" else cgs
        rest = {tid: tile for tid, tile in state.tiles.items() if tid not in active}
        chosen.extend(Action(tid, ActionType.LOCAL) for tid, tile in rest.items()
                      if tile["state"] == "READY" and not tile["in_transfer"])
        return chosen
