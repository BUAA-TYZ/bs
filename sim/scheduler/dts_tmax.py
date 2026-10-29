"""DTS-TMAX 选择适配；有界输入路径扩展不等于原文 PropagateSearch。"""

from collections import Counter
import math

from sim.entities import Action, ActionType
from sim.marl.decision import candidate_neighbors
from sim.scheduler.contact_graph import quantize
from sim.scheduler.input_routing import current_input_options, reserve_input_memory
from sim.scheduler.literature import action_for, compute_service, contact_plan, processing_state
from sim.topology import link_key


class DTSTMAXOneHopAdapted:
    def __init__(self, deadline_s=300.0, continue_late=True, include_ground=False):
        if deadline_s is not None and (not math.isfinite(deadline_s) or deadline_s <= 0):
            raise ValueError("Soft deadline must be finite and positive, or None")
        self.deadline_s = deadline_s
        self.continue_late = continue_late
        self.include_ground = include_ground
        self.stats = Counter()

    def select_actions(self, state):
        dt = state.config["dt"]
        busy = {sid: sat["committed_compute_s"] for sid, sat in state.satellites.items()}
        rates = {sid: sat["compute_rate"] for sid, sat in state.satellites.items()}
        if self.include_ground:
            rates, busy = processing_state(state)
        memory = {sid: sat["mem_remaining_gb"] for sid, sat in state.satellites.items()}
        ground_relay = bool(state.config.get("ground_input_relay"))
        input_relay = (state.config.get("input_relay_max_hops", 1) > 1 or
                       ground_relay or self.include_ground)
        plan = contact_plan(state) if input_relay else None
        actions = []
        for tid, tile in sorted(state.tiles.items(), key=lambda item: (-item[1]["waiting_time"], item[0])):
            if tile["in_transfer"] or tile["state"] not in ("QUEUED", "READY"):
                continue
            if tile["state"] == "READY":
                actions.append(Action(tid, ActionType.LOCAL))
                continue
            if tile["task_progress"]["failed"]:
                actions.append(Action(tid, ActionType.WAIT))
                continue
            self.stats["requests"] += 1
            src = tile["location"]
            candidates = []
            if tile["vram_req_gb"] <= state.config["vram_capacity_gb"] or self.include_ground:
                inputs = (current_input_options(state, plan, tile, memory)
                          if input_relay else {})
                processors = ([p for p in inputs if p in state.satellites or self.include_ground]
                              if input_relay else [src, *candidate_neighbors(state, src)])
                for processor in processors:
                    transfer = 0.0
                    path = ()
                    if input_relay:
                        transfer, path = inputs[processor]
                    elif processor != src:
                        link = state.links.get(link_key(src, processor))
                        if (not link or not link["up"] or link["bandwidth_mbps"] <= 0 or
                                memory[processor] + 1e-12 < tile["data_size_gb"]):
                            continue
                        transfer = max(dt, quantize(tile["data_size_mb"] * 8 / link["bandwidth_mbps"], dt))
                        transfer += link.get("latency_ms", 0.0) / 1000.0
                    rate = rates[processor]
                    if rate <= 0:
                        continue
                    service = compute_service(state, tile["compute_cost"] / rate)
                    response = transfer + busy[processor] + service
                    hard_remaining = tile.get("deadline_remaining_s")
                    if not math.isfinite(response) or (hard_remaining is not None and
                                                       response > hard_remaining + 1e-12):
                        continue
                    candidates.append((response, processor != src, processor, service, path))
            if not candidates:
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_resource_or_hard_deadline"] += 1
                continue
            remaining = (math.inf if self.deadline_s is None else
                         self.deadline_s - tile["waiting_time"])
            on_time = [candidate for candidate in candidates if candidate[0] <= remaining + 1e-12]
            if not on_time and not self.continue_late:
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_soft_deadline"] += 1
                continue
            # 保留最短响应选择；有界路径只扩展公共候选，不引入原文逐星传播搜索。
            def preference(candidate):
                response, remote, processor, _, _ = candidate
                target = (1, processor[1]) if isinstance(processor, tuple) else (0, processor)
                return response, remote, target

            _, _, processor, service, path = min(on_time or candidates, key=preference)
            busy[processor] += service
            if input_relay:
                plan.reserve(path, tile["data_size_mb"])
                reserve_input_memory(memory, plan, path, tile)
                actions.append(action_for(tid, src, processor, plan=plan, path=path,
                                          ground_relay=ground_relay))
                if len(path) > 1:
                    self.stats["bounded_path_admission"] += 1
            elif processor != src:
                memory[processor] -= tile["data_size_gb"]
                actions.append(Action(tid, ActionType.OFFLOAD, target_sat_id=processor))
            else:
                actions.append(Action(tid, ActionType.LOCAL))
            self.stats["accepted"] += 1
            self.stats["on_time_admission" if on_time else "late_admission"] += 1
        return actions
