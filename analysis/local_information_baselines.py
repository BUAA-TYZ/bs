"""完整静态接触计划与局部动态通告分离；不修改环境、路由或v3报价。"""

from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import replace

from sim.control import ControlPlane
from sim.entities import Action, ActionType
from sim.scheduler.input_routing import reserve_input_memory
from sim.scheduler.literature import action_for, contact_plan, input_options
from sim.scheduler.task_gate import whole_service


DYNAMIC = ("queue_len", "executing_remaining", "executing_remaining_s", "running_count",
           "committed_compute_s", "committed_result_mb", "unrouted_result_mb")


def reachable_helpers(state, source, hops, sources):
    seen, frontier = {source}, {source}
    for _ in range(hops):
        following = {n for sid in frontier for n in state.neighbors.get(sid, [])
                     if n not in sources and n not in seen}
        seen.update(following)
        frontier = following
    return seen - {source}


class LocalInformationPlane(ControlPlane):
    """通告仍占原有链路；历史只积累当前k跳内、已送达且未过期的样本。"""

    estimator = "zero"
    history_hops = 1

    def __init__(self, sources):
        super().__init__(sources)
        self.history = {s: defaultdict(lambda: [Counter(), Counter()]) for s in sources}
        self.seen_samples = {s: set() for s in sources}
        self.observation_stats = Counter()

    def records(self, state, source, hops):
        now = state.time * state.config["dt"]
        scope = reachable_helpers(state, source, hops, self.sources)
        return {sid: r for sid, r in self.delivered[source].items()
                if sid in scope and r["hops"] <= hops and r["received_s"] <= now
                and 0 <= now-r["generated_s"] <= self.ttl_s}

    def enqueue(self, state):
        # 固定5秒采样，包括没有待派发图像的源星；不重复计入同一通告。
        now = state.time * state.config["dt"]
        if now-self.last_sent_s >= self.period_s-1e-9 and self.estimator == "history":
            for source in self.sources:
                for sid, record in self.records(state, source, self.history_hops).items():
                    key = sid, record["generated_s"]
                    if key in self.seen_samples[source]:
                        continue
                    self.seen_samples[source].add(key)
                    row = record["satellite"]
                    values = {k: float(row.get(k, 0)) for k in DYNAMIC}
                    values.update(mem_remaining_gb=row["mem_remaining_gb"],
                                  vram_remaining_gb=row["vram_remaining_gb"])
                    values.update({f"isl:{dst}": v for dst, v in row.get("isl_backlog_mb", {}).items()})
                    values.update({f"ground:{gid}": v for gid, v in row.get("ground_backlog_mb", {}).items()})
                    sums, counts = self.history[source][sid]
                    for name, value in values.items():
                        sums[name] += value
                        counts[name] += 1
        super().enqueue(state)

    def view(self, state, source, hops):
        assert hops == self.history_hops
        records = self.records(state, source, hops)
        now = state.time*state.config["dt"]
        satellites = {}
        for sid, full in state.satellites.items():
            # 白名单复制静态字段，绝不把远端真实动态值作为默认值。
            row = {"compute_rate": full["compute_rate"],
                   "next_gs_windows": deepcopy(full["next_gs_windows"])}
            empty = dict.fromkeys(DYNAMIC, 0.)
            empty.update(mem_remaining_gb=state.config["mem_capacity_gb"],
                         vram_remaining_gb=state.config["vram_capacity_gb"])
            if sid == source or sid in records:
                observed = full if sid == source else records[sid]["satellite"]
                row.update({key: observed.get(key, default) for key, default in empty.items()})
                row["isl_backlog_mb"] = deepcopy(observed.get("isl_backlog_mb", {}))
                row["ground_backlog_mb"] = deepcopy(observed.get("ground_backlog_mb", {}))
                row["dynamic_known"] = True
                row["advertisement_age_s"] = 0. if sid == source else now-records[sid]["generated_s"]
            else:
                sums, counts = self.history[source].get(sid, (Counter(), Counter()))
                def estimate(name, default=0.):
                    return sums[name]/counts[name] if self.estimator == "history" and counts[name] else default
                row.update({key: estimate(key, default) for key, default in empty.items()})
                row["isl_backlog_mb"] = {dst: estimate(f"isl:{dst}") for dst in state.neighbors.get(sid, [])}
                row["ground_backlog_mb"] = {gid: estimate(f"ground:{gid}") for gid in state.ground_stations}
                row["dynamic_known"] = False
                row["dynamic_estimator"] = self.estimator if counts else "zero_no_history"
                self.observation_stats[row["dynamic_estimator"]] += 1
            satellites[sid] = row
        ground = {}
        for gid, full in state.ground_stations.items():
            ground[gid] = {k: full[k] for k in ("compute_rate", "bandwidth_mbps", "min_elevation_deg", "receive_channels")}
            # 无地面动态通告，因此两变体都使用明示的零负载先验，不借用地面真值。
            ground[gid].update(committed_compute_s=0., queue_len=0, running=0,
                               visible_contenders=sum(any(o["gs_id"] == gid for o in opts)
                                                      for opts in state.ground_options.values()),
                               dynamic_known=False, dynamic_estimator="zero_no_ground_history")
        tiles = {tid: deepcopy(t) for tid, t in state.tiles.items() if t["source_sat_id"] == source}
        pending = {task: {tid: deepcopy(t) for tid, t in rows.items() if t["source_sat_id"] == source}
                   for task, rows in state.pending_task_tiles.items()
                   if any(t["source_sat_id"] == source for t in rows.values())}
        links = {key: {k: deepcopy(row[k]) for k in ("up", "bandwidth_mbps", "latency_ms", "contact_windows")}
                 for key, row in state.links.items()}
        config = {**state.config, "local_information": True, "ground_queue_unknown": False,
                  "known_dynamic_nodes": sorted({source, *records}), "observer_source": source,
                  "dynamic_estimator": self.estimator, "observation_hops": hops}
        return replace(state, satellites=satellites, ground_stations=ground, links=links,
                       neighbors=deepcopy(state.neighbors), ground_options=deepcopy(state.ground_options),
                       tiles=tiles, pending_task_tiles=pending, config=config)


class LocalMinLoad:
    """整图首次选择已知积压秒数最少的可达计算星，之后不重选；无候选则等待。"""

    def __init__(self, _raw_hops=None):
        self.assignments, self.decisions = {}, []

    def select_actions(self, state):
        source = state.config["observer_source"]
        known = set(state.config["known_dynamic_nodes"])-set(state.config["task_source_sat_ids"])
        plan = contact_plan(state)
        memory = {sid: s["mem_remaining_gb"] for sid, s in state.satellites.items()}
        busy = {sid: s["committed_compute_s"] for sid, s in state.satellites.items()}
        groups = defaultdict(dict)
        actions = []
        for tid, tile in state.tiles.items():
            if tile["state"] == "READY" and not tile["in_transfer"]:
                actions.append(Action(tid, ActionType.LOCAL))
            elif tile["location"] == source and tile["state"] == "QUEUED" and not tile["in_transfer"] and not tile["task_progress"]["failed"]:
                groups[tile["parent_task_id"]][tid] = tile
        for task, tiles in sorted(groups.items(), key=lambda x: (-max(t["waiting_time"] for t in x[1].values()), x[0])):
            # 最小负载只允许k跳内已知节点，路径也不得经过未知节点。
            first = next(iter(tiles.values()))
            choices = input_options(state, plan, first, memory)
            eligible = {sid: value for sid, value in choices.items() if sid in known
                        and all(plan.contacts[i].target in known for i in value[1])}
            if task not in self.assignments:
                if not eligible:
                    continue
                target = min(eligible, key=lambda s: (busy[s], eligible[s][0], s))
                self.assignments[task] = target
                self.decisions.append({"task_id": task, "time_seconds": state.time*state.config["dt"],
                                       "selected": "least_load", "target": target, "queue_seconds": busy[target]})
            target = self.assignments[task]
            if target not in known:
                continue
            for tid, tile in sorted(tiles.items()):
                options = input_options(state, plan, tile, memory)
                if target not in options:
                    continue
                _, path = options[target]
                if any(plan.contacts[i].target not in known for i in path):
                    continue
                reserve_input_memory(memory, plan, path, tile)
                plan.reserve(path, tile["data_size_mb"])
                busy[target] += whole_service(state, [tile], target)
                actions.append(action_for(tid, source, target, plan=plan, path=path))
        return actions
