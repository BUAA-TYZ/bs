"""整图交付候选：比较三种影子方案的整图期限收益，只执行公共动作前缀。

输入、CPU 与结果都使用公共资源模型；每个分片保留原身份和具体落点。
优化范围是本轮可动作任务及其全部已释放兄弟片，不读取尚未释放的任务。
整图影子接纳不是跨轮带宽保证，未派发的未来分配每轮重算。公开状态没有
逐任务在途明细，旧承诺尾部使用过去派发报价的单调最大估计；动作可能未
被执行器接纳，因此这既不是实际完成时刻的下界，也不是期限保证。
"""

from collections import Counter
from dataclasses import dataclass, replace
import math

from sim.entities import Action, ActionType
from sim.marl.decision import decision_state
from sim.scheduler.contact_graph import ContactPlan, quantize
from sim.scheduler.execution_return import execution_delivery
from sim.scheduler.input_routing import _hop_arrival, current_input_options, reserve_input_memory
from sim.scheduler.literature import action_for, compute_service, contact_plan, ground_node, processing_state


@dataclass(frozen=True)
class _Placement:
    processor: object
    input_path: tuple
    result_path: tuple
    compute_end: float
    finish: float


class _Ledger:
    def __init__(self, state, return_mode):
        self.state = state
        self.return_mode = return_mode
        self.plan = contact_plan(state)
        self.rate, self.busy = processing_state(state)
        self.memory = {sid: sat["mem_remaining_gb"] for sid, sat in state.satellites.items()}

    def clone(self):
        other = object.__new__(_Ledger)
        other.state, other.return_mode, other.rate = self.state, self.return_mode, self.rate
        other.plan = ContactPlan([replace(contact) for contact in self.plan.contacts])
        other.busy, other.memory = self.busy.copy(), self.memory.copy()
        return other

    def quotes(self, tile, release=0.0):
        state, dt = self.state, self.state.config["dt"]
        ground = {ground_node(gid) for gid in state.ground_stations}
        hops = state.config.get("relay_max_hops", 8) if state.config.get("result_relay") else 0
        options = current_input_options(state, self.plan, tile, self.memory)
        choices = []
        for processor, (arrival, input_path) in options.items():
            if self.rate.get(processor, 0) <= 0:
                continue
            if release > 0:
                # 隐藏兄弟片受后续决策批次约束，不能假设全部在本轮同时派发。
                arrival = release
                for index in input_path:
                    arrival = _hop_arrival(self.plan.contacts[index], arrival, tile["data_size_mb"], dt,
                                           future=processor in ground)
                    if arrival is None:
                        break
                if arrival is None:
                    continue
            end = max(self.busy[processor], arrival) + compute_service(
                state, tile["compute_cost"] / self.rate[processor])
            saved = {index: self.plan.contacts[index].volume for index in input_path}
            self.plan.reserve(input_path, tile["data_size_mb"])
            try:
                ready = quantize(end, dt)
                if self.return_mode == "execution":
                    route = execution_delivery(state, self.plan, processor, ready, tile["result_size_mb"],
                                               dt=dt, max_hops=hops + 1)
                else:
                    route = self.plan.delivery(processor, ground, ready, tile["result_size_mb"],
                                               dt=dt, max_hops=hops + 1)
            finally:
                for index, volume in saved.items():
                    self.plan.contacts[index].volume = volume
            if route is None:
                continue
            finish, result_path = route
            hard = tile.get("deadline_remaining_s")
            if hard is not None and finish + state.config.get("postprocessing_delay_s", 0) > hard + 1e-9:
                continue
            choices.append(_Placement(processor, input_path, result_path, end, finish))
        return choices

    def place(self, tile, release=0.0):
        choices = self.quotes(tile, release)
        if not choices:
            return None
        # 相同到达时间使用输入枚举的稳定顺序，避免卫星整数与地面 tuple 比较。
        chosen = min(choices, key=lambda row: (row.finish, row.compute_end))
        self.plan.reserve(chosen.input_path, tile["data_size_mb"])
        self.plan.reserve(chosen.result_path, tile["result_size_mb"])
        reserve_input_memory(self.memory, self.plan, chosen.input_path, tile)
        self.busy[chosen.processor] = chosen.compute_end
        return chosen


class TaskDeliveryPolicy:
    def __init__(self, sla_seconds=300.0, return_mode="contact_graph", max_orders=3,
                 decision_interval_s=None):
        if not math.isfinite(sla_seconds) or sla_seconds <= 0:
            raise ValueError("sla_seconds must be finite and positive")
        if return_mode not in ("contact_graph", "execution"):
            raise ValueError("Unknown return prediction mode")
        if type(max_orders) is not int or not 1 <= max_orders <= 3:
            raise ValueError("max_orders must be 1, 2 or 3")
        if decision_interval_s is not None and (not math.isfinite(decision_interval_s) or decision_interval_s <= 0):
            raise ValueError("decision_interval_s must be finite and positive")
        self.sla_seconds, self.return_mode = float(sla_seconds), return_mode
        self.max_orders, self.decision_interval_s = max_orders, decision_interval_s
        self.stats = Counter()
        self.last_prediction = {}
        self._committed_tail = {}
        self._last_time = -math.inf

    def select_actions(self, state):
        interval = (self.decision_interval_s if self.decision_interval_s is not None else
                    state.config.get("decision_interval_seconds", 1.0))
        limit = state.config.get("decision_tile_limit_per_sat", 4)
        active = decision_state(state, limit)
        if not active.tiles:
            return []
        now = state.time * state.config["dt"]
        if now < self._last_time:
            self._committed_tail.clear()
        self._last_time = now
        tasks = {tile["parent_task_id"] for tile in active.tiles.values()
                 if tile["state"] == "QUEUED" and not tile["in_transfer"]}
        pending = {tid: tile for group in state.pending_task_tiles.values() for tid, tile in group.items()
                   if tile["parent_task_id"] in tasks and tile["state"] == "QUEUED" and
                   not tile["in_transfer"] and tile["waiting_time"] >= 0 and not tile["task_progress"]["failed"]}
        pending.update({tid: tile for tid, tile in active.tiles.items()
                        if tile["state"] == "QUEUED" and not tile["in_transfer"] and
                        tile["waiting_time"] >= 0 and not tile["task_progress"]["failed"]})
        ordered = sorted(pending, key=lambda tid: (-pending[tid]["waiting_time"], tid))
        groups = {}
        for tid in ordered:
            groups.setdefault(pending[tid]["parent_task_id"], []).append(tid)
        for task, tids in groups.items():
            progress = pending[tids[0]]["task_progress"]
            # 公开计数证明其余兄弟全部终态时，旧片报价不能继续压住剩余分片。
            if ("total" in progress and "done" in progress and
                    progress["total"] == progress["done"] + progress.get("failed", 0) + len(tids)):
                if self._committed_tail.pop(task, None) is not None:
                    self.stats["cleared_completed_tail"] += 1
        ages = {task: max(pending[tid]["waiting_time"] for tid in tids) for task, tids in groups.items()}
        post = state.config.get("postprocessing_delay_s", 0)
        floors = {task: max(0, self._committed_tail.get(task, now) - now) for task in groups}
        release, hidden_position = {}, Counter()
        for tid in ordered:
            src = pending[tid]["location"]
            if tid in active.tiles:
                release[tid] = 0.0
            else:
                release[tid] = (1 + hidden_position[src] // limit) * interval
                hidden_position[src] += 1
        base = _Ledger(state, self.return_mode)
        public_fifo = [tid for tid in active.tiles if tid in pending]
        public_fifo.extend(tid for tid in pending if tid not in active.tiles)
        public_fifo.sort(key=lambda tid: -pending[tid]["waiting_time"])
        fifo_rank = {tid: index for index, tid in enumerate(public_fifo)}

        def respects_local_fifo(placements):
            seen = {}
            for tid, placement in placements.items():
                tile = pending[tid]
                if placement.processor != tile["location"]:
                    continue
                tasks_seen = seen.setdefault(tile["location"], {})
                task, rank = tile["parent_task_id"], fifo_rank[tid]
                if any(other != task and position > rank for other, position in tasks_seen.items()):
                    return False
                tasks_seen[task] = max(tasks_seen.get(task, -1), rank)
            return True

        def fill(ledger, tids):
            placements = {}
            for tid in tids:
                if (chosen := ledger.place(pending[tid], release[tid])) is not None:
                    placements[tid] = chosen
            return placements

        def task_finish(task, placements):
            if any(tid not in placements for tid in groups[task]):
                return math.inf
            return max(floors[task], max(placements[tid].finish + post for tid in groups[task]))

        def evaluate(placements):
            latency = {task: ages[task] + task_finish(task, placements) for task in groups}
            score = (sum(value <= self.sla_seconds + 1e-9 for value in latency.values()),
                     -sum(min(self.sla_seconds, value) for value in latency.values()))
            return score, latency

        baseline = fill(base.clone(), ordered)
        best, best_name = baseline, "fcfs"
        best_score, best_latency = evaluate(best)
        self.stats["candidate_plans"] += 1
        self.stats["shadow_tiles"] += len(pending)
        if len(groups) > 1 and self.max_orders > 1:
            indices = {task: index for index, task in enumerate(groups)}
            deadlines = sorted(groups, key=lambda task: (self.sla_seconds - ages[task], indices[task]))
            orders = [("deadline", deadlines)]
            if self.max_orders > 2:
                flexibility = {task: len(base.quotes(pending[tids[0]])) for task, tids in groups.items()}
                scarcity = sorted(groups, key=lambda task: (flexibility[task] or math.inf,
                                                            len(groups[task]),
                                                            self.sla_seconds - ages[task], indices[task]))
                if scarcity != deadlines:
                    orders.append(("scarcity", scarcity))
            for name, order in orders:
                ledger, placements, deferred = base.clone(), {}, []
                for task in order:
                    trial = ledger.clone()
                    proposed = fill(trial, groups[task])
                    if ages[task] + task_finish(task, proposed) <= self.sla_seconds + 1e-9:
                        ledger = trial
                        placements.update(proposed)
                    else:
                        # 无法整图按时交付时先回滚；及时任务规划后仍用剩余资源继续。
                        deferred.extend(groups[task])
                placements.update(fill(ledger, sorted(deferred, key=lambda tid: (-pending[tid]["waiting_time"], tid))))
                self.stats["candidate_plans"] += 1
                # LOCAL 只设 READY，不改变已有 sat.queue；不能把不可执行重排算作收益。
                if not respects_local_fifo(placements):
                    self.stats["rejected_local_fifo_order"] += 1
                    continue
                score, latency = evaluate(placements)
                if score > best_score and any(tid in active.tiles for tid in placements):
                    best, best_name, best_score, best_latency = placements, name, score, latency
        else:
            self.stats["single_task_or_fcfs_reuse"] += 1

        self.last_prediction = {"selected": best_name, "on_time_tasks": best_score[0],
                                "truncated_latency_sum": -best_score[1], "task_latency": best_latency,
                                "pending_tiles": len(pending), "active_tiles": len(active.tiles)}
        self.stats[f"selected_{best_name}"] += 1
        self.stats["predicted_on_time_task_observations"] += best_score[0]
        actions = []
        # 按候选方案的真实接纳顺序发动作，保持目标 CPU 的排队假设一致。
        ready = [tid for tid, tile in active.tiles.items() if tile["state"] == "READY"]
        dispatch = [tid for tid in best if tid in active.tiles]
        remainder = [tid for tid in active.tiles if tid not in best and tid not in ready]
        for tid in ready + dispatch + remainder:
            tile = active.tiles[tid]
            if tile["in_transfer"]:
                continue
            if tile["state"] == "READY":
                actions.append(Action(tid, ActionType.LOCAL))
                continue
            if tile["state"] != "QUEUED":
                continue
            if tid not in best:
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_unplanned"] += 1
                continue
            chosen = best[tid]
            actions.append(action_for(tid, tile["location"], chosen.processor,
                                      plan=base.plan, path=chosen.input_path,
                                      ground_relay=bool(state.config.get("ground_input_relay"))))
            task = tile["parent_task_id"]
            self._committed_tail[task] = max(self._committed_tail.get(task, now), now + chosen.finish + post)
            self.stats["accepted"] += 1
        return actions
