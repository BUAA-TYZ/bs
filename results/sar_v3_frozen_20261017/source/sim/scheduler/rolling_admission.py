"""有限承诺接单：保留尚未开算分片的下一轮重选机会。"""

from collections import Counter
from dataclasses import replace
import math

from sim.entities import Action, ActionType
from sim.marl.decision import candidate_neighbors
from sim.scheduler.contact_graph import ContactPlan, choose_opportunity, quantize
from sim.scheduler.execution_return import execution_delivery
from sim.scheduler.input_routing import current_input_options, reserve_input_memory
from sim.scheduler.literature import (
    action_for, compute_service, contact_plan, ground_node, processing_state,
)


class RollingAdmissionPolicy:
    def __init__(self, commit_horizon_s=30, guard_returns=True, input_lookahead_s=0,
                 return_mode="contact_graph", wait_for_sla_only=False, sla_seconds=300):
        if not math.isfinite(commit_horizon_s) or commit_horizon_s < 0:
            raise ValueError("commit_horizon_s must be finite and nonnegative")
        if not math.isfinite(input_lookahead_s) or input_lookahead_s < 0:
            raise ValueError("input_lookahead_s must be finite and nonnegative")
        self.commit_horizon_s = float(commit_horizon_s)
        self.guard_returns = guard_returns
        self.input_lookahead_s = float(input_lookahead_s)
        if return_mode not in ("contact_graph", "execution"):
            raise ValueError("Unknown return prediction mode")
        self.return_mode = return_mode
        if not math.isfinite(sla_seconds) or sla_seconds <= 0:
            raise ValueError("sla_seconds must be finite and positive")
        self.wait_for_sla_only = wait_for_sla_only
        self.sla_seconds = float(sla_seconds)
        self.stats = Counter()

    def select_actions(self, state):
        if not state.tiles:
            return []
        plan = contact_plan(state)
        margin = state.config.get("contact_scan_s", 0) if self.guard_returns else 0
        returns = ContactPlan([
            replace(c, end=c.end - margin, volume=max(0, c.volume - margin * c.rate))
            for c in plan.contacts
        ])
        rate, busy = processing_state(state)
        memory = {sid: sat["mem_remaining_gb"] for sid, sat in state.satellites.items()}
        # 已有积压按接触窗口消化，断链间隔不能算作传输服务时间。
        incoming_mb = {index: max(0, (c.end - c.start) * c.rate - c.volume)
                       for index, c in enumerate(plan.contacts)}
        dt = state.config["dt"]
        ground = {ground_node(gid) for gid in state.ground_stations}
        hops = state.config.get("relay_max_hops", 8) if state.config.get("result_relay") else 0
        post = state.config.get("postprocessing_delay_s", 0)
        ground_relay = state.config.get("ground_input_relay", False)
        multihop_inputs = state.config.get("input_relay_max_hops", 1) > 1 or ground_relay
        actions, risk_tasks = [], set()

        def choose(ready, size):
            opportunities = [(quantize(time, dt), p, p) for p, time in ready.items()]
            if self.return_mode == "contact_graph":
                return choose_opportunity(opportunities, returns, ground, size,
                                          dt=dt, max_hops=hops + 1)
            best = None
            for time, processor, _ in sorted(opportunities, key=lambda row: row[0]):
                if best is not None and time >= best[0]:
                    break
                route = execution_delivery(state, plan, processor, time, size,
                                           dt=dt, max_hops=hops + 1)
                if route is not None and (best is None or route[0] < best[0]):
                    best = route[0], processor, route[1]
            return best

        def reserve(path, size):
            plan.reserve(path, size)
            # 输入可用完整窗口；返回图只保留扣除边界误差后的剩余容量。
            for index, count in Counter(path).items():
                returns.contacts[index].volume = max(0, returns.contacts[index].volume - count * size)

        ordered = sorted(state.tiles.items(), key=lambda row: (-row[1]["waiting_time"], row[0]))
        for tid, tile in ordered:
            if tile["in_transfer"]:
                continue
            if tile["state"] == "READY":
                actions.append(Action(tid, ActionType.LOCAL))
                continue
            if tile["state"] != "QUEUED":
                continue
            self.stats["requests"] += 1
            if tile["task_progress"]["failed"]:
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_failed_task"] += 1
                continue
            src, size = tile["location"], tile["data_size_mb"]
            inputs = ({processor: (arrival, path, False)
                       for processor, (arrival, path) in current_input_options(state, plan, tile, memory).items()}
                      if multihop_inputs else {})
            if not multihop_inputs and tile["vram_req_gb"] <= state.config["vram_capacity_gb"]:
                inputs[src] = (0, (), False)
                eligible = set(candidate_neighbors(state, src))
            else:
                eligible = set()
            if not multihop_inputs:
                eligible.update(ground_node(o["gs_id"]) for o in state.ground_options.get(src, []))
            for index in plan.outgoing.get(src, []):
                c = plan.contacts[index]
                future = (0 < c.start <= self.input_lookahead_s and c.target in memory
                          and tile["vram_req_gb"] <= state.config["vram_capacity_gb"])
                if (not future and (c.start != 0 or c.target not in eligible)) or c.rate <= 0 or c.volume + 1e-9 < size:
                    continue
                if c.target in memory and memory[c.target] + 1e-12 < tile["data_size_gb"]:
                    continue
                # 同一方向先累计字节再量化到达，不能把每个小包各占一个整步。
                arrival = quantize(c.start, dt) + max(
                    dt, quantize((incoming_mb[index] + size) / c.rate, dt))
                if arrival > c.end + 1e-9:
                    continue
                if c.target not in inputs or arrival < inputs[c.target][0]:
                    inputs[c.target] = (arrival, (index,), future)

            starts = {p: max(busy[p], arrival) for p, (arrival, _, _) in inputs.items()}
            admitted = {p: start for p, start in starts.items()
                        if start <= self.commit_horizon_s + 1e-9 and rate[p] > 0}
            ready = {p: start + compute_service(state, tile["compute_cost"] / rate[p])
                     for p, start in admitted.items()}
            choice = choose(ready, tile["result_size_mb"])
            if choice is not None and inputs[choice[1]][2]:
                immediate = choose({p: time for p, time in ready.items() if not inputs[p][2]},
                                   tile["result_size_mb"])
                # 同一回传窗口下没有交付收益时，直接执行优于占住 FIFO 等待。
                if immediate is not None and immediate[0] <= choice[0] + 1e-9:
                    choice = immediate
                elif immediate is not None and self.wait_for_sla_only:
                    remaining = self.sla_seconds - tile["waiting_time"] - post
                    if not (choice[0] <= remaining + 1e-9 < immediate[0]):
                        choice = immediate
                        self.stats["future_wait_rejected_no_sla_gain"] += 1
            deadline = tile.get("deadline_remaining_s")
            if choice is None or (deadline is not None and choice[0] + post > deadline):
                actions.append(Action(tid, ActionType.WAIT))
                reason = "wait_commit_horizon" if starts and not admitted else "wait_no_feasible_delivery"
                self.stats[reason] += 1
                if tile["waiting_time"] >= self.sla_seconds:
                    risk_tasks.add(tile["parent_task_id"])
                continue

            finish, processor, result_path = choice
            reserve(inputs[processor][1], size)
            reserve(result_path, tile["result_size_mb"])
            busy[processor] = ready[processor]
            if processor != src:
                for index in inputs[processor][1]:
                    incoming_mb[index] += size
                if multihop_inputs:
                    reserve_input_memory(memory, plan, inputs[processor][1], tile)
                elif processor in memory:
                    memory[processor] -= tile["data_size_gb"]
            deferred = inputs[processor][2]
            if deferred:
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_future_input"] += 1
            else:
                actions.append(action_for(tid, src, processor, plan=plan, path=inputs[processor][1],
                                          ground_relay=ground_relay)
                               if multihop_inputs else action_for(tid, src, processor))
                self.stats["accepted"] += 1
            if finish + post > self.sla_seconds - tile["waiting_time"]:
                risk_tasks.add(tile["parent_task_id"])
                self.stats["deferred_sla_risk" if deferred else "accepted_sla_risk"] += 1
        # 仅记录本轮可见兄弟片的迟到风险，不把软 SLA 改成物理取消。
        self.stats["task_sla_risk_observations"] += len(risk_tasks)
        return actions
