"""跨轮输入到达日历：保留已发出计算承诺的释放时间，不预测未来任务。"""

from dataclasses import dataclass

from sim.entities import Action, ActionType
from sim.scheduler.contact_graph import choose_opportunity, quantize
from sim.scheduler.input_routing import reserve_input_memory
from sim.scheduler.literature import (
    CGSComputeAdapted, action_for, compute_service, contact_plan, ground_node,
    input_options, processing_state,
)


@dataclass
class Commitment:
    tile_id: str | None
    arrival_seconds: float
    remaining_service_seconds: float


class ArrivalCalendarPolicy(CGSComputeAdapted):
    """CGS 机会比较加自有历史日历；到达及完成顺序仍是公开信息下的近似。"""

    def __init__(self):
        super().__init__()
        self.calendar = {}
        self._last_now = None

    def _processing_calendar(self, state):
        now = state.time * state.config["dt"]
        if self._last_now is not None and now < self._last_now:
            self.calendar.clear()
            self.stats["calendar_resets"] += 1
        self._last_now = now
        rate, observed = processing_state(state)
        uncommitted = {tid for rows in state.pending_task_tiles.values() for tid in rows}
        uncommitted.update(tid for tid, tile in state.tiles.items() if tile["state"] == "QUEUED")
        busy = dict(observed)
        for processor in list(self.calendar):
            if processor not in observed:
                self.calendar.pop(processor)
        for processor, public_work in observed.items():
            entries = self.calendar.get(processor, [])
            retained = [entry for entry in entries if entry.tile_id not in uncommitted]
            self.stats["calendar_unaccepted_removed"] += len(entries) - len(retained)
            entries = sorted(retained, key=lambda entry: entry.arrival_seconds)
            if public_work <= 1e-12:
                self.stats["calendar_empty_nodes_cleared"] += bool(entries)
                self.calendar.pop(processor, None)
                continue
            accounted = sum(entry.remaining_service_seconds for entry in entries)
            consumed = max(0.0, accounted - public_work)
            # 公共状态只给总剩余量；按预测到达顺序摊销已完成量，不假装知道真实队列。
            for entry in entries:
                used = min(consumed, entry.remaining_service_seconds)
                entry.remaining_service_seconds -= used
                consumed -= used
                if used > 0 and entry.remaining_service_seconds > 1e-12:
                    entry.arrival_seconds = min(entry.arrival_seconds, now)
            entries = [entry for entry in entries if entry.remaining_service_seconds > 1e-12]
            if public_work > accounted + 1e-12:
                entries.append(Commitment(None, now, public_work - accounted))
                self.stats["calendar_untracked_work_observed"] += 1
            entries.sort(key=lambda entry: entry.arrival_seconds)
            finish = now
            for entry in entries:
                finish = max(finish, entry.arrival_seconds) + entry.remaining_service_seconds
            busy[processor] = max(public_work, finish - now)
            if busy[processor] > public_work + 1e-9:
                self.stats["calendar_release_gap_observations"] += 1
                self.stats["calendar_release_gap_seconds_sum"] += busy[processor] - public_work
            self.calendar[processor] = entries
        return rate, busy

    def select_actions(self, state):
        rate, busy = self._processing_calendar(state)
        if not state.tiles:
            return []
        now, dt = state.time * state.config["dt"], state.config["dt"]
        plan = contact_plan(state)
        memory = {sid: sat["mem_remaining_gb"] for sid, sat in state.satellites.items()}
        ground = {ground_node(gid) for gid in state.ground_stations}
        ground_relay = bool(state.config.get("ground_input_relay"))
        input_relay = state.config.get("input_relay_max_hops", 1) > 1 or ground_relay
        hops = state.config.get("relay_max_hops", 8) if state.config.get("result_relay") else 0
        actions = []
        for tid, tile in sorted(state.tiles.items(), key=lambda row: (-row[1]["waiting_time"], row[0])):
            if tile["in_transfer"]:
                continue
            if tile["state"] == "READY":
                actions.append(Action(tid, ActionType.LOCAL))
                continue
            if tile["task_progress"]["failed"]:
                actions.append(Action(tid, ActionType.WAIT))
                continue
            self.stats["requests"] += 1
            inputs = input_options(state, plan, tile, memory)
            service = {processor: compute_service(state, tile["compute_cost"] / rate[processor])
                       for processor in inputs if rate[processor] > 0}
            ready = {processor: max(busy[processor], inputs[processor][0]) + duration
                     for processor, duration in service.items()}
            opportunities = [(quantize(end, dt) if state.config.get("compute_service_mode") == "work_conserving"
                              else end, processor, processor) for processor, end in ready.items()]
            choice = None
            for opportunity in sorted(opportunities, key=lambda row: row[0]):
                processor = opportunity[1]
                path = inputs[processor][1]
                if input_relay:
                    plan.reserve(path, tile["data_size_mb"])
                try:
                    candidate = choose_opportunity([opportunity], plan, ground, tile["result_size_mb"],
                                                   dt=dt, max_hops=hops + 1)
                finally:
                    if input_relay:
                        for index in path:
                            plan.contacts[index].volume += tile["data_size_mb"]
                if candidate is not None and (choice is None or candidate[0] < choice[0]):
                    choice = candidate
            deadline = tile.get("deadline_remaining_s")
            if choice is None or (deadline is not None and
                    choice[0] + state.config.get("postprocessing_delay_s", 0) > deadline):
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_no_feasible_delivery"] += 1
                continue
            _, processor, result_path = choice
            visible = {ground_node(option["gs_id"]) for option in state.ground_options.get(tile["location"], [])}
            if isinstance(processor, tuple) and processor not in visible and not ground_relay:
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_ground_window"] += 1
                continue
            input_path = inputs[processor][1]
            plan.reserve(input_path, tile["data_size_mb"])
            plan.reserve(result_path, tile["result_size_mb"])
            reserve_input_memory(memory, plan, input_path, tile)
            busy[processor] = ready[processor]
            actions.append(action_for(tid, tile["location"], processor, plan=plan, path=input_path,
                                      ground_relay=ground_relay))
            # 只记录已发出的动作；下一轮公开剩余量和未承诺分片会校正拒收或完成。
            self.calendar.setdefault(processor, []).append(
                Commitment(tid, now + inputs[processor][0], service[processor]))
            self.stats["accepted"] += 1
            self.stats["accepted_ground" if isinstance(processor, tuple) else "accepted_satellite"] += 1
            if isinstance(processor, tuple) and any(plan.contacts[index].start > 0 for index in input_path):
                self.stats["accepted_future_ground_window"] += 1
        self.stats["calendar_entries"] = sum(len(entries) for entries in self.calendar.values())
        return actions
