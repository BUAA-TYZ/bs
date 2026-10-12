"""同步容量预约原型：当前请求串行仲裁，执行仍使用共同传输和中继规则。"""

from collections import Counter
import copy
import heapq
import math

from sim.entities import Action, ActionType
from sim.marl.decision import candidate_neighbors
from sim.marl.observation import MAX_TILES_PER_SAT
from sim.scheduler.greedy import GreedyEarliestFinish
from sim.topology import link_key


class ServiceCalendar:
    """连续服务时间预约；允许独立通道，查询不改变已有预约。"""

    def __init__(self, channels=1):
        self.channels = [[] for _ in range(channels)]

    def quote(self, ready, duration, deadline=math.inf):
        best = None
        for channel, intervals in enumerate(self.channels):
            start = ready
            for left, right in sorted(intervals):
                if start + duration <= left + 1e-9:
                    break
                if start < right:
                    start = right
            end = start + duration
            if end <= deadline + 1e-9:
                option = (end, start, channel)
                if best is None or option < best:
                    best = option
        return best

    def reserve(self, slot):
        end, start, channel = slot
        self.channels[channel].append((start, end))


class _Ledger:
    def __init__(self, state):
        self.state = state
        self.dt = state.config["dt"]
        self.compute = {sid: sat["committed_compute_s"] for sid, sat in state.satellites.items()}
        self.memory = {sid: sat["mem_remaining_gb"] for sid, sat in state.satellites.items()}
        self.ground_compute = {gid: gs["committed_compute_s"] for gid, gs in state.ground_stations.items()}
        self.links = {}
        self.stations = {gid: ServiceCalendar(gs["receive_channels"]) for gid, gs in state.ground_stations.items()}
        self.radios = {sid: ServiceCalendar() for sid in state.satellites}
        for sid, sat in state.satellites.items():
            for nb in state.neighbors.get(sid, []):
                calendar = ServiceCalendar()
                bw = state.links[link_key(sid, nb)]["bandwidth_mbps"]
                busy = sat.get("isl_backlog_mb", {}).get(nb, 0) * 8 / max(bw, 1e-9)
                if busy:
                    calendar.reserve((busy, 0, 0))
                self.links[sid, nb] = calendar
            for gid, size in sat.get("ground_backlog_mb", {}).items():
                slot = self.downlink(sid, gid, 0, size) if size else None
                if slot:
                    self.book_downlink(sid, gid, slot)
        # 已承诺计算和待路由结果也占未来容量；只用当前广播与最近窗口估计。
        for sid, sat in state.satellites.items():
            for ready, size in ((0, sat["unrouted_result_mb"]),
                                (self.compute[sid], sat["committed_result_mb"])):
                options = [(slot[0], gid, slot) for gid in self.stations
                           if size and (slot := self.downlink(sid, gid, ready, size))]
                if options:
                    _, gid, slot = min(options)
                    self.book_downlink(sid, gid, slot)
                elif size:
                    # 连已接单结果都放不下时，不把剩余窗口误报为可承诺容量。
                    for gid, window in sat.get("next_gs_windows", {}).items():
                        if window is not None and ready < sum(window):
                            start, duration = window
                            self.radios[sid].channels[0].append((max(start, ready), start + duration))

    def rounded(self, seconds):
        return math.ceil(max(0, seconds) / self.dt - 1e-12) * self.dt

    def fork(self):
        other = copy.copy(self)
        for name in ("compute", "memory", "ground_compute"):
            setattr(other, name, getattr(self, name).copy())
        for name in ("links", "stations", "radios"):
            calendars = {}
            for key, calendar in getattr(self, name).items():
                cloned = ServiceCalendar(len(calendar.channels))
                cloned.channels = [list(intervals) for intervals in calendar.channels]
                calendars[key] = cloned
            setattr(other, name, calendars)
        return other

    def downlink(self, sid, gid, ready, size):
        window = self.state.satellites[sid].get("next_gs_windows", {}).get(gid)
        if window is None:
            return None
        start, duration = window
        end = math.floor((start + duration) / self.dt + 1e-12) * self.dt
        service = size * 8 / max(self.state.ground_stations[gid]["bandwidth_mbps"], 1e-9)
        ready = max(start, self.rounded(ready))
        # 同时满足地面站通道和本星单个星地终端，跳过两个日历中的冲突区间。
        while ready + service <= end + 1e-9:
            station = self.stations[gid].quote(ready, service, end)
            radio = self.radios[sid].quote(ready, service, end)
            if station is None or radio is None:
                return None
            later = max(station[1], radio[1])
            if abs(station[1] - radio[1]) < 1e-9:
                return station
            ready = later
        return None

    def book_downlink(self, sid, gid, slot):
        self.stations[gid].reserve(slot)
        self.radios[sid].reserve((slot[0], slot[1], 0))

    def transfer(self, src, dst, ready, size):
        link = self.state.links.get(link_key(src, dst))
        if not link or not link["up"] or link["bandwidth_mbps"] <= 0:
            return None
        end = next((duration for start, duration in link.get("contact_windows", []) if start == 0), 0)
        slot = self.links[src, dst].quote(ready, size * 8 / link["bandwidth_mbps"], end)
        if slot is None or self.rounded(slot[0]) > end:
            return None
        return slot

    def options(self, tid, tile):
        state, src = self.state, tile["location"]
        dispatch = tile.get("earliest_dispatch_s", 0.0)
        choices = []
        for sid in self.compute_candidates(src):
            if tile["vram_req_gb"] > state.config["vram_capacity_gb"]:
                continue
            incoming = None
            if sid != src:
                if self.memory[sid] + 1e-12 < tile["data_size_gb"]:
                    continue
                incoming = self.input_transfer(src, sid, dispatch, tile["data_size_mb"])
                if incoming is None:
                    continue
            arrival = self.rounded(incoming[0]) if incoming else dispatch
            ready = max(arrival, self.compute[sid]) + self.rounded(tile["compute_cost"] / state.satellites[sid]["compute_rate"])
            if getattr(self, "prune", False) and choices and ready > min(q["finish"] for q in choices):
                continue
            for result in self.result_options(sid, ready, tile["result_size_mb"]):
                choices.append({**result, "sat": sid, "ready": ready,
                                "incoming": incoming, "ground": False,
                                "defer": incoming is not None and self.should_defer(src, sid, incoming)})
        for option in state.ground_options.get(src, []):
            gid = option["gs_id"]
            upload = self.downlink(src, gid, dispatch, tile["data_size_mb"])
            if upload:
                ready = max(self.rounded(upload[0]), self.ground_compute[gid])
                finish = ready + self.rounded(tile["compute_cost"] / state.ground_stations[gid]["compute_rate"])
                choices.append({"finish": finish, "sat": src, "exit": src, "gs": gid,
                                "ready": finish, "down": upload, "ground": True})
        deadline = tile.get("deadline_remaining_s")
        if deadline is not None:
            choices = [x for x in choices if x["finish"] + state.config.get("postprocessing_delay_s", 0) <= deadline]
        return sorted(choices, key=lambda x: (x["finish"], x["ground"], x["sat"], x["exit"], x["gs"]))

    def compute_candidates(self, src):
        return [src] + candidate_neighbors(self.state, src)

    def input_transfer(self, src, dst, ready, size):
        return self.transfer(src, dst, ready, size)

    def should_defer(self, src, dst, slot):
        return False

    def result_options(self, sid, ready, size):
        choices = []
        for exit_sat in [sid] + candidate_neighbors(self.state, sid):
            relay = None
            if exit_sat != sid:
                if self.memory[exit_sat] < size / 1024:
                    continue
                relay = self.transfer(sid, exit_sat, ready, size)
                if relay is None:
                    continue
            result_ready = self.rounded(relay[0]) if relay else ready
            for gid in self.stations:
                down = self.downlink(exit_sat, gid, result_ready, size)
                if down:
                    choices.append({"finish": self.rounded(down[0]), "exit": exit_sat,
                                    "gs": gid, "relay": relay, "down": down})
        return choices

    def commit(self, tile, quote):
        src, sid, gid = tile["location"], quote["sat"], quote["gs"]
        self.book_downlink(quote["exit"], gid, quote["down"])
        if quote["ground"]:
            self.ground_compute[gid] = quote["ready"]
        else:
            self.compute[sid] = quote["ready"]
            if quote["incoming"]:
                self.memory[sid] -= tile["data_size_gb"]
                self.links[src, sid].reserve(quote["incoming"])
            if quote["relay"]:
                self.links[sid, quote["exit"]].reserve(quote["relay"])
            for src, dst, slot in quote.get("relays", []):
                self.links[src, dst].reserve(slot)


class _ForecastLedger(_Ledger):
    """有限时域接触图报价；只改变接单估计，不修改公共传输和路由。"""

    def __init__(self, state, guard=False, multihop=False, defer=False, prune=True):
        self.guard, self.multihop, self.defer = guard, multihop, defer
        self.prune = prune
        super().__init__(state)
        self.edge_models, self.services = {}, {}
        self.adjacency = {sid: [] for sid in state.satellites}
        for src in state.satellites:
            for dst in state.satellites:
                if src == dst or link_key(src, dst) not in state.links:
                    continue
                link = state.links[link_key(src, dst)]
                if not link.get("contact_windows"):
                    continue
                self.adjacency[src].append(dst)
                calendar = ServiceCalendar()
                backlog = state.satellites[src].get("isl_backlog_mb", {}).get(dst, 0)
                # 在接触日历里消化已有字节，避免把断链等待误当成已服务完毕。
                remaining = backlog
                bw = self.bandwidth(link)
                windows = self.windows(link)
                self.edge_models[src, dst] = (link["up"], bw, tuple((self.rounded(start), end) for start, end in windows))
                if bw > 0:
                    for start, end in windows:
                        duration = min(max(0, end - start), remaining * 8 / bw)
                        if duration > 0:
                            calendar.reserve((start + duration, start, 0))
                            remaining = max(0, remaining - duration * bw / 8)
                self.links[src, dst] = calendar

    def bandwidth(self, link):
        current = link["bandwidth_mbps"]
        return min(current, self.state.config.get("isl_bandwidth_floor_mbps", 0)) if self.guard else current

    def windows(self, link):
        margin = self.state.config.get("contact_scan_s", 0) if self.guard else 0
        return [(start, start + duration - margin) for start, duration in link.get("contact_windows", [])
                if duration > margin and (start > 0 or link["up"])]

    def contact_transfer(self, src, dst, ready, size, future):
        model = self.edge_models.get((src, dst))
        if model is None:
            return None
        up, bw, windows = model
        if bw <= 0 or (not future and not up):
            return None
        key = (src, dst, size)
        service = self.services.get(key)
        if service is None:
            service = self.services[key] = max(self.dt, self.rounded(size * 8 / bw))
        ready = self.rounded(ready)
        for start, end in windows:
            if not future and start > 0:
                continue
            # 至少一个实际步长，按步边界开始/到达，避免多跳在同一步瞬移。
            slot = self.links[src, dst].quote(max(ready, start), service, end)
            if slot is not None:
                return slot
        return None

    def transfer(self, src, dst, ready, size):
        return self.contact_transfer(src, dst, ready, size, future=False)

    def compute_candidates(self, src):
        return [src] + self.adjacency[src] if self.defer else super().compute_candidates(src)

    def input_transfer(self, src, dst, ready, size):
        return self.contact_transfer(src, dst, ready, size, future=self.defer)

    def should_defer(self, src, dst, slot):
        if not self.defer:
            return False
        if dst not in candidate_neighbors(self.state, src):
            return True
        link = self.state.links[link_key(src, dst)]
        return not any(start == 0 and slot[0] <= end for start, end in self.windows(link))

    def result_options(self, sid, ready, size):
        if not self.multihop:
            return [quote for quote in super().result_options(sid, ready, size)
                    if self.state.config.get("result_relay", False) or quote["exit"] == sid]
        limit = self.state.config.get("relay_max_hops", 8) if self.state.config.get("result_relay", False) else 0
        # 固定预约下，较早到达可等待任一后续窗口；按节点和已用跳数保留最早标签。
        queue = [(ready, 0, sid, [])]
        best = {(sid, 0): ready}
        exits = {}
        finish_bound = math.inf
        while queue:
            arrival, hops, node, path = heapq.heappop(queue)
            if self.prune and arrival > finish_bound:
                break
            if arrival != best[node, hops]:
                continue
            for gid in self.stations:
                down = self.downlink(node, gid, arrival, size)
                if down is not None:
                    key = (node, gid)
                    quote = {"finish": self.rounded(down[0]), "exit": node, "gs": gid,
                             "relay": None, "relays": path, "down": down}
                    if key not in exits or (quote["finish"], hops) < (exits[key]["finish"], len(exits[key]["relays"])):
                        exits[key] = quote
                        finish_bound = min(finish_bound, quote["finish"])
            if hops >= limit:
                continue
            for dst in self.adjacency[node]:
                if self.memory[dst] < size / 1024:
                    continue
                slot = self.contact_transfer(node, dst, arrival, size, future=True)
                if slot is None:
                    continue
                end = self.rounded(slot[0])
                if self.prune and end > finish_bound:
                    continue
                if any(best.get((dst, used), math.inf) <= end for used in range(hops + 2)):
                    continue
                best[dst, hops + 1] = end
                heapq.heappush(queue, (end, hops + 1, dst, path + [(node, dst, slot)]))
        choices = list(exits.values())
        # 此账本只供宽度一的试排使用；更晚的出口不会成为该执行目的地的报价。
        return [min(choices, key=lambda q: (q["finish"], q["exit"], q["gs"]))] if self.prune and choices else choices


class ReservationPolicy:
    def __init__(self, reserve=True):
        self.reserve = reserve
        self.stats = Counter()

    def make_ledger(self, state):
        return _Ledger(state)

    def fallback_action(self, ledger, tile, action):
        if action.target_sat_id is not None:
            dst = action.target_sat_id
            if ledger.memory[dst] < tile["data_size_gb"]:
                return Action(action.tile_id, ActionType.WAIT)
            if self.reserve:
                ledger.memory[dst] -= tile["data_size_gb"]
        return action

    def select_actions(self, state):
        if not state.tiles:
            return []
        ledger = self.make_ledger(state)
        fallback = {a.tile_id: a for a in GreedyEarliestFinish().select_actions(state)}
        actions = []
        # 年龄优先、同任务连续处理；下一轮从真实已承诺工作重建账本，不留下过期租约。
        ordered = sorted(state.tiles.items(), key=lambda row: (-row[1]["waiting_time"],
                         row[1]["parent_task_id"], row[0]))
        for tid, tile in ordered:
            if tile["in_transfer"]:
                continue
            self.stats["requests"] += 1
            if tile["state"] == "READY":
                actions.append(Action(tid, ActionType.LOCAL))
                continue
            if tile["task_progress"]["failed"]:
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_failed_task"] += 1
                continue
            options = ledger.options(tid, tile)
            if options:
                choice = options[0]
                if self.reserve:
                    ledger.commit(tile, choice)
                if choice["ground"]:
                    action = Action(tid, ActionType.OFFLOAD, target_gs_id=choice["gs"])
                elif choice["sat"] == tile["location"]:
                    action = Action(tid, ActionType.LOCAL)
                else:
                    action = Action(tid, ActionType.OFFLOAD, target_sat_id=choice["sat"])
                self.stats["accepted"] += 1
            else:
                candidates = {tile["location"], *candidate_neighbors(state, tile["location"])}
                for sid in list(candidates):
                    candidates.update(candidate_neighbors(state, sid))
                forecast = any(w is not None for sid in candidates
                               for w in state.satellites[sid].get("next_gs_windows", {}).values())
                if forecast:
                    action = Action(tid, ActionType.WAIT)
                    self.stats["wait_capacity_or_contact"] += 1
                else:
                    # 预测时域之外不等于不可达；仅此情况退回原启发式继续处理。
                    action = fallback.get(tid, Action(tid, ActionType.WAIT))
                    action = self.fallback_action(ledger, tile, action)
                    self.stats["fallback_no_window"] += 1
            actions.append(action)
        return actions


class JointReservationPolicy(ReservationPolicy):
    """滚动整图预检：先安排所有未分配兄弟分片，再提交当前允许的动作。"""

    def __init__(self, beam_width=4):
        super().__init__()
        if beam_width < 1:
            raise ValueError("beam_width must be positive")
        self.beam_width = beam_width

    def plan(self, ledger, tiles):
        if self.beam_width == 1:
            current, choices, worst, total = ledger.fork(), {}, 0.0, 0.0
            for tid, tile in tiles.items():
                options = current.options(tid, tile)
                if not options:
                    return None
                quote = options[0]
                current.commit(tile, quote)
                choices[tid] = quote
                worst, total = max(worst, quote["finish"]), total + quote["finish"]
            return current, choices, worst, total
        beams = [(ledger, {}, 0.0, 0.0)]
        for tid, tile in tiles.items():
            expanded = []
            for current, choices, worst, total in beams:
                # 每个执行目的地保留最早回传报价，限制搜索开销而不只保留同目的地变体。
                unique = {}
                for quote in current.options(tid, tile):
                    key = (quote["ground"], quote["sat"], quote["gs"] if quote["ground"] else None)
                    unique.setdefault(key, quote)
                for quote in unique.values():
                    child = current.fork()
                    child.commit(tile, quote)
                    expanded.append((child, {**choices, tid: quote},
                                     max(worst, quote["finish"]), total + quote["finish"]))
            if not expanded:
                return None
            expanded.sort(key=lambda beam: (beam[2], beam[3]))
            beams = expanded[:self.beam_width]
        return beams[0]

    def select_actions(self, state):
        if not state.tiles:
            return []
        ledger = self.make_ledger(state)
        all_tiles = {tid: tile for group in state.pending_task_tiles.values() for tid, tile in group.items()}
        all_tiles.update({tid: tile for tid, tile in state.tiles.items()
                          if tile["state"] == "QUEUED" and not tile["in_transfer"]})
        rank = {}
        for sid in state.satellites:
            queue = sorted(((tid, tile) for tid, tile in all_tiles.items() if tile["location"] == sid),
                           key=lambda row: -row[1]["waiting_time"])
            rank.update({tid: index for index, (tid, _) in enumerate(queue)})
        groups = {}
        actions = []
        for tid, tile in sorted(state.tiles.items(), key=lambda row: (-row[1]["waiting_time"],
                               row[1]["parent_task_id"], row[0])):
            if tile["in_transfer"]:
                continue
            if tile["state"] == "READY":
                actions.append(Action(tid, ActionType.LOCAL))
            else:
                groups.setdefault(tile["parent_task_id"], {})[tid] = tile
        fallback = {a.tile_id: a for a in GreedyEarliestFinish().select_actions(state)}
        for task, active in groups.items():
            self.stats["requests"] += len(active)
            self.stats["task_preflights"] += 1
            if any(t["task_progress"]["failed"] for t in active.values()):
                actions.extend(Action(tid, ActionType.WAIT) for tid in active)
                self.stats["wait_failed_task"] += len(active)
                continue
            pending = state.pending_task_tiles.get(task, active)
            tiles = {tid: {**tile, "earliest_dispatch_s": (0 if tid in active else
                          max(1, rank[tid] // MAX_TILES_PER_SAT) * ledger.dt)}
                     for tid, tile in sorted(pending.items(), key=lambda row: rank[row[0]])}
            self.stats["planned_tile_rows"] += len(tiles)
            plan = self.plan(ledger, tiles)
            if plan is not None:
                # 虚拟后续分片也占本轮规划容量；执行仍严格遵守公共的每星四分片限制。
                ledger, choices, _, _ = plan
                deferred = 0
                for tid, tile in active.items():
                    quote = choices[tid]
                    if quote.get("defer", False):
                        actions.append(Action(tid, ActionType.WAIT))
                        deferred += 1
                        continue
                    action = (Action(tid, ActionType.OFFLOAD, target_gs_id=quote["gs"]) if quote["ground"] else
                              Action(tid, ActionType.LOCAL) if quote["sat"] == tile["location"] else
                              Action(tid, ActionType.OFFLOAD, target_sat_id=quote["sat"]))
                    actions.append(action)
                self.stats["accepted"] += len(active) - deferred
                if deferred:
                    self.stats["wait_better_contact"] += deferred
                self.stats["task_preflight_accepted"] += 1
                continue
            candidates = {tile["location"] for tile in tiles.values()}
            for _ in range(2):
                candidates.update(nb for sid in list(candidates) for nb in candidate_neighbors(state, sid))
            forecast = any(w is not None for sid in candidates
                           for w in state.satellites[sid].get("next_gs_windows", {}).values())
            if forecast:
                actions.extend(Action(tid, ActionType.WAIT) for tid in active)
                self.stats["wait_joint_infeasible"] += len(active)
                self.stats["task_preflight_rejected"] += 1
            else:
                for tid, tile in active.items():
                    action = fallback.get(tid, Action(tid, ActionType.WAIT))
                    action = self.fallback_action(ledger, tile, action)
                    actions.append(action)
                self.stats["fallback_no_window"] += len(active)
        return actions


class ForecastReservationPolicy(JointReservationPolicy):
    def __init__(self, guard=False, multihop=False, defer=False):
        super().__init__(beam_width=1)
        self.guard, self.multihop, self.defer = guard, multihop, defer

    def make_ledger(self, state):
        return _ForecastLedger(state, guard=self.guard, multihop=self.multihop, defer=self.defer)

    def fallback_action(self, ledger, tile, action):
        if self.guard and action.target_sat_id is not None:
            dst = action.target_sat_id
            slot = ledger.transfer(tile["location"], dst, 0, tile["data_size_mb"])
            if slot is None or ledger.memory[dst] < tile["data_size_gb"]:
                self.stats["fallback_rejected_contact"] += 1
                return Action(action.tile_id, ActionType.WAIT)
            ledger.links[tile["location"], dst].reserve(slot)
        return super().fallback_action(ledger, tile, action)
