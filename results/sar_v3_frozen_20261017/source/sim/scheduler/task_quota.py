"""整图影子配额候选：把当前可执行配额给公开动作前缀。

配额只按片数重排，不承诺异构分片的原计划计算尾时刻；影子规划也不等于
真实执行日历。实际只派发当前公开候选，重新校验输入链路和接收资源。
"""

from collections import Counter
from dataclasses import replace

from sim.entities import Action, ActionType
from sim.marl.decision import candidate_neighbors, decision_state
from sim.scheduler.contact_graph import quantize
from sim.scheduler.input_routing import current_input_options, reserve_input_memory
from sim.scheduler.literature import action_for, contact_plan, ground_node
from sim.scheduler.rolling_admission import RollingAdmissionPolicy


class TaskQuotaPolicy:
    def __init__(self, input_lookahead_s=300, return_mode="contact_graph",
                 wait_for_sla_only=False, shadow_policy=None):
        self.shadow = shadow_policy if shadow_policy is not None else RollingAdmissionPolicy(
            commit_horizon_s=14400, guard_returns=False, input_lookahead_s=input_lookahead_s,
            return_mode=return_mode, wait_for_sla_only=wait_for_sla_only,
        )
        self.stats = Counter()

    @staticmethod
    def quota_key(tile, action):
        src = tile["location"]
        if action.action_type == ActionType.LOCAL:
            target = src
        elif action.action_type == ActionType.OFFLOAD and action.target_gs_id is not None:
            target = ground_node(action.target_gs_id)
        elif action.action_type == ActionType.OFFLOAD and action.target_sat_id is not None:
            target = action.target_sat_id
        else:
            return None
        return tile["parent_task_id"], src, target

    def select_actions(self, state):
        limit = state.config.get("decision_tile_limit_per_sat", 4)
        active = decision_state(state, limit)
        if not active.tiles:
            return []
        tasks = {tile["parent_task_id"] for tile in active.tiles.values()}
        pending = {tid: tile for task, group in state.pending_task_tiles.items() if task in tasks
                   for tid, tile in group.items()
                   if tile["state"] == "QUEUED" and not tile["in_transfer"]}
        pending.update({tid: tile for tid, tile in active.tiles.items()
                        if tile["state"] == "QUEUED" and not tile["in_transfer"]})
        before = self.shadow.stats.copy()
        shadow_actions = self.shadow.select_actions(replace(state, tiles=pending))
        for name, value in self.shadow.stats.items():
            self.stats[f"shadow_{name}"] += value - before.get(name, 0)
        self.stats["shadow_calls"] += 1
        self.stats["shadow_planned_tiles"] += len(pending)
        quota = Counter()
        original = {}
        for action in shadow_actions:
            if action.tile_id not in pending:
                continue
            key = self.quota_key(pending[action.tile_id], action)
            if key is not None:
                quota[key] += 1
                original[action.tile_id] = key
        self.stats["shadow_immediate_quota"] += sum(quota.values())

        plan = contact_plan(state)
        used_mb = {index: max(0, (c.end - c.start) * c.rate - c.volume)
                   for index, c in enumerate(plan.contacts)}
        memory = {sid: sat["mem_remaining_gb"] for sid, sat in state.satellites.items()}
        neighbors = {sid: set(candidate_neighbors(state, sid)) for sid in state.satellites}
        ground = {sid: {ground_node(row["gs_id"]) for row in state.ground_options.get(sid, [])}
                  for sid in state.satellites}
        dt = state.config["dt"]
        ground_relay = state.config.get("ground_input_relay", False)
        multihop_inputs = state.config.get("input_relay_max_hops", 1) > 1 or ground_relay

        def input_quote(tile, target):
            src, size = tile["location"], tile["data_size_mb"]
            if target in state.satellites:
                if tile["vram_req_gb"] > state.config["vram_capacity_gb"]:
                    return None
                if target == src:
                    return ()
                if target not in neighbors[src] or memory[target] + 1e-12 < tile["data_size_gb"]:
                    return None
            elif target not in ground[src]:
                return None
            for index in plan.outgoing.get(src, []):
                c = plan.contacts[index]
                if c.target != target or c.start != 0 or c.rate <= 0 or c.volume + 1e-9 < size:
                    continue
                arrival = max(dt, quantize((used_mb[index] + size) / c.rate, dt))
                if arrival <= c.end + 1e-9:
                    return (index,)
            return None

        actions = []
        # 与公共候选一样保持等年龄分片的原始顺序，WAIT 不越过动作额度。
        for tid, tile in sorted(active.tiles.items(), key=lambda row: -row[1]["waiting_time"]):
            if tile["in_transfer"]:
                continue
            if tile["state"] == "READY":
                actions.append(Action(tid, ActionType.LOCAL))
                self.stats["ready_continue"] += 1
                continue
            if tile["state"] != "QUEUED":
                continue
            self.stats["requests"] += 1
            if tile["task_progress"]["failed"] or tile.get("deadline_remaining_s") == 0:
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_failed_or_expired"] += 1
                continue
            keys = [key for key, count in quota.items()
                    if count and key[:2] == (tile["parent_task_id"], tile["location"])]
            if original.get(tid) in keys:
                keys.remove(original[tid])
                keys.insert(0, original[tid])
            if multihop_inputs:
                options = current_input_options(state, plan, tile, memory)
                choice = next(((key, options[key[2]][1]) for key in keys if key[2] in options), None)
            else:
                choice = next(((key, path) for key in keys
                               if (path := input_quote(tile, key[2])) is not None), None)
            if choice is None:
                actions.append(Action(tid, ActionType.WAIT))
                self.stats["wait_illegal_quota" if keys else "wait_no_current_quota"] += 1
                continue
            key, path = choice
            src, target = tile["location"], key[2]
            quota[key] -= 1
            plan.reserve(path, tile["data_size_mb"])
            for index in path:
                used_mb[index] += tile["data_size_mb"]
            if multihop_inputs:
                reserve_input_memory(memory, plan, path, tile)
                action = action_for(tid, src, target, plan=plan, path=path, ground_relay=ground_relay)
            elif target == src:
                action = Action(tid, ActionType.LOCAL)
            elif target in state.satellites:
                memory[target] -= tile["data_size_gb"]
                action = Action(tid, ActionType.OFFLOAD, target_sat_id=target)
            else:
                action = Action(tid, ActionType.OFFLOAD, target_gs_id=target[1])
            actions.append(action)
            self.stats["accepted"] += 1
            self.stats["reassigned_current_quota"] += int(original.get(tid) != key)
        self.stats["unused_current_quota"] += sum(quota.values())
        return actions
