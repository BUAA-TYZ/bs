"""三方案整图选择的实验扩展；公共执行器和路由源码保持冻结。"""

from collections import Counter
from copy import deepcopy
from dataclasses import dataclass, replace
import math

from sim.entities import Action, ActionType, TileState
from sim.env import SimulationEnv
from sim.pipeline.distributed import DistributedPipeline
from sim.routing import delivery_route
from sim.scheduler.contact_graph import quantize
from sim.scheduler.literature import CGSComputeAdapted, contact_plan, ground_node, input_options, processing_state
from sim.scheduler.input_routing import input_route_nodes, reserve_input_memory
from sim.scheduler.task_gate import whole_service


@dataclass
class RawImageAction(Action):
    raw_hops: int = 45


class RoutingView:
    """仅为该原图选择已存在的跳数协议，结果中继继续使用环境原协议。"""
    def __init__(self, env, hops):
        self.env = env
        self.cfg = replace(env.cfg, relay_max_hops=hops)
        self._temporal_route_waits = {}

    def __getattr__(self, name):
        return getattr(self.env, name)


class CandidatePipeline(DistributedPipeline):
    def __init__(self, env):
        super().__init__(env)
        self.raw_tasks = {}
        self.routing_views = {}

    def apply_actions(self, actions, links):
        env = self.env
        groups = {}
        normal = []
        for action in actions:
            if isinstance(action, RawImageAction):
                tile = env.tiles[action.tile_id]
                groups.setdefault(tile.parent_task_id, []).append(action)
            else:
                normal.append(action)
        for task_id, commands in groups.items():
            if task_id in self.raw_tasks:
                continue
            task = env.tasks[task_id]
            assert {a.tile_id for a in commands} == set(task.tile_ids)
            tiles = [env.tiles[tid] for tid in task.tile_ids]
            assert all(t.state == TileState.QUEUED and t.location == task.source_sat_id
                       and t.memory_holders == {task.source_sat_id} for t in tiles)
            hops = commands[0].raw_hops
            assert all(a.raw_hops == hops for a in commands)
            blocks = env.image_workload.unique_transport_blocks(len(tiles))
            assert math.isclose(sum(size for _, size in blocks), task.image_size_mb)
            for tile, (_, size) in zip(tiles, blocks):
                env._release_tile_memory(tile, tile.location)
                tile.data_size_mb = size
                assert env._reserve_tile_memory(tile, tile.location)
            self.raw_tasks[task_id] = hops
            self.routing_views.setdefault(hops, RoutingView(env, hops))
        super().apply_actions(normal, links)
        self.route_unique(links)

    def route_unique(self, links):
        env = self.env
        no_routes = set()
        for tile in sorted(env.open_tiles(), key=lambda t: (t.timestamps.created, t.tile_id)):
            hops = self.raw_tasks.get(tile.parent_task_id)
            if hops is None or tile.state not in (TileState.QUEUED, TileState.READY):
                continue
            if not tile.raw_relay_path:
                tile.raw_relay_path = [tile.location]
            key = (hops, tile.location, tile.data_size_mb, tuple(tile.raw_relay_path))
            if key in no_routes:
                continue
            route = delivery_route(self.routing_views[hops], tile.location, tile.data_size_mb,
                                   links, tile.raw_relay_path)
            if route is None:
                no_routes.add(key)
                continue
            no_routes.clear()
            kind, target = route
            if kind == "ground":
                self._offload_to_gs(tile, target)
            else:
                self._offload_to_sat(tile, target, links)

    def advance_ground_tile_transfers(self):
        # 与冻结执行器同一接收语义，仅按任务选择已有的整图到齐屏障。
        env = self.env
        still = []
        for tr in env.ground_tile_transfers:
            tile = env.tiles[tr.tile_id]
            if (env.ground_stations.get(tr.gs_id) is None or
                    not env.vis_cache.is_visible(tr.src_sat, tr.gs_id, env.ground_stations)):
                still.append(tr)
                continue
            if tr.remaining_mb > 1e-6:
                still.append(tr)
                continue
            env._release_tile_memory(tile, tr.src_sat)
            tile.state = TileState.GROUND_QUEUED
            tile.timestamps.end_tx = env.time + 1
            if tile.parent_task_id in self.raw_tasks or env.pipeline_mode == "ground_unique_relay":
                self._assemble_unique_image(tile, tr.gs_id)
            else:
                env.ground_tile_queues[tr.gs_id].append(tile.tile_id)
            env._log_tile_event(tile, "tx_end_ground", sat_from=tr.src_sat, sat_to=tr.gs_id)
        env.ground_tile_transfers = still

    def advance_transfers(self, links):
        super().advance_transfers(links)
        # 公共输入到达会转READY；原图仅存转，不得在等待下一跳时误启动计算。
        for tile in self.env.open_tiles():
            if tile.parent_task_id in self.raw_tasks and tile.state == TileState.READY:
                tile.state = TileState.QUEUED


class CandidateEnv(SimulationEnv):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dist_pipeline = CandidatePipeline(self)

    def export_state(self):
        state = super().export_state()
        raw_tasks = self._dist_pipeline.raw_tasks
        if not raw_tasks:
            return state
        # 独立适配只修正新增原图分支的广播账本，不改变公共环境或物理执行。
        incoming = {tr.tile_id: tr.dst for tr in self.transfers if not tr.is_result}
        windows = Counter()
        seconds = Counter()
        for tile in self.open_tiles():
            if tile.parent_task_id in raw_tasks:
                if tile.tile_id in incoming:
                    sid = incoming[tile.tile_id]
                    state.satellites[sid]["committed_result_mb"] -= self.tile_result_size_mb(tile)
                continue
            if tile.ground_relay_target is not None and tile.state in (TileState.GROUND_RELAY_PENDING, TileState.TRANSFERRING):
                continue
            sid = incoming.get(tile.tile_id, tile.location)
            routed = tile.input_target_sat_id is not None and tile.state in (TileState.TRANSFERRING, TileState.INPUT_RELAY)
            if routed:
                sid = tile.input_target_sat_id
            if routed or tile.tile_id in incoming or tile.state in (TileState.READY, TileState.RUNNING):
                service = float(tile.remaining_compute or 0) if tile.state == TileState.RUNNING else tile.compute_cost / self.satellites[sid].compute_rate
                seconds[sid] += self.compute_service_seconds(service)
                windows[sid] += tile.compute_cost - tile.completed_compute_windows
        for sid, satellite in self.satellites.items():
            if self.cfg.onboard_compute_profile is None:
                state.satellites[sid]["committed_compute_s"] = seconds[sid]
            else:
                pending = max(0, round(windows[sid]) - len(satellite.executing_batch))
                service = self.cfg.onboard_compute_profile["service_s"]
                duration = satellite.batch_remaining_s
                for size in sorted((int(k) for k in service), reverse=True):
                    count, pending = divmod(pending, size)
                    duration += count * service[str(size)]
                state.satellites[sid]["committed_compute_s"] = duration
        # 冻结面板只有一个地面站，因此未到齐原图的后续计算归属无歧义。
        assert len(self.ground_stations) == 1
        gid = next(iter(self.ground_stations))
        counted = set(self.ground_tile_queues[gid])
        counted.add(self.ground_tile_running.get(gid))
        counted.update(tr.tile_id for tr in self.ground_tile_transfers if tr.gs_id == gid)
        extra = sum(t.compute_cost / self.ground_stations[gid].compute_rate for t in self.open_tiles()
                    if t.parent_task_id in raw_tasks and t.tile_id not in counted)
        state.ground_stations[gid]["committed_compute_s"] += extra
        return replace(state, tiles={tid: t for tid, t in state.tiles.items() if t["parent_task_id"] not in raw_tasks},
                       pending_task_tiles={task: rows for task, rows in state.pending_task_tiles.items() if task not in raw_tasks})


class Calendar:
    """公开接触上的FIFO近似日历；每包可在计算结束后立即回传。"""
    def __init__(self, state):
        self.plan = contact_plan(state)
        self.rate, self.busy = processing_state(state)
        self.memory = {sid: row["mem_remaining_gb"] for sid, row in state.satellites.items()}
        self.free = {}
        for i, c in enumerate(self.plan.contacts):
            occupied = max(0, (c.end - c.start) * c.rate - c.volume)
            self.free[i] = c.start + occupied / c.rate if c.rate > 0 else math.inf

    def transport(self, path, size, ready, dt):
        for i in path:
            c = self.plan.contacts[i]
            end = max(quantize(ready, dt), quantize(self.free[i], dt), quantize(c.start, dt))
            end += max(dt, quantize(size / c.rate, dt)) if c.rate > 0 else math.inf
            if end > c.end + 1e-9 or c.volume + 1e-9 < size:
                return None
            self.free[i] = end
            self.plan.reserve((i,), size)
            ready = end + c.delay
        return ready

    def delivery(self, source, destinations, ready, size, dt, hops):
        view = deepcopy(self.plan)
        for i, c in enumerate(view.contacts):
            c.start = max(c.start, self.free[i])
        route = view.delivery(source, destinations, ready, size, dt=dt, max_hops=hops)
        if route is None:
            return None
        end = self.transport(route[1], size, ready, dt)
        return None if end is None else (end, route[1])

    def quote(self, state, actions, *, raw=False, raw_hops=45):
        dt = state.config["dt"]
        ground = {ground_node(gid) for gid in state.ground_stations}
        hops = state.config.get("relay_max_hops", 45)
        selected = {a.tile_id: a for a in actions}
        if set(selected) != set(state.tiles):
            return None
        arrivals, processors = {}, {}
        ordered = sorted(state.tiles, key=lambda tid: (-state.tiles[tid]["waiting_time"], tid))
        for tid in ordered:
            tile, action = state.tiles[tid], selected[tid]
            processor = (tile["location"] if action.action_type == ActionType.LOCAL else
                         ground_node(action.target_gs_id) if action.target_gs_id is not None else action.target_sat_id)
            if raw:
                size = state.config["image_size_mb"] / len(state.tiles)
                route = self.delivery(tile["location"], ground, 0, size, dt, raw_hops + 1)
                if route is None:
                    return None
                arrival, path = route
                processor = self.plan.contacts[path[-1]].target
            elif action.action_type not in (ActionType.LOCAL, ActionType.OFFLOAD):
                return None
            elif processor in ground:
                route = self.delivery(tile["location"], {processor}, 0, tile["data_size_mb"], dt, hops + 1)
                if route is None:
                    return None
                arrival, path = route
            else:
                options = input_options(state, self.plan, tile, self.memory)
                if processor not in options:
                    return None
                path = options[processor][1]
                if action.input_path and tuple(action.input_path) != input_route_nodes(self.plan, path):
                    return None
                arrival = self.transport(path, tile["data_size_mb"], 0, dt)
                if arrival is None:
                    return None
                reserve_input_memory(self.memory, self.plan, path, tile)
            arrivals[tid], processors[tid] = arrival, processor
        if raw:
            processor = processors[max(ordered, key=lambda tid: arrivals[tid])]
            ready = max(self.busy[processor], max(arrivals.values()))
            ready += sum(t["compute_cost"] for t in state.tiles.values()) / self.rate[processor]
            self.busy[processor] = ready
            finish = ready
        else:
            finish = 0
            for tid in sorted(ordered, key=lambda tid: (arrivals[tid], tid)):
                tile, processor = state.tiles[tid], processors[tid]
                if self.rate.get(processor, 0) <= 0:
                    return None
                service = (tile["compute_cost"] / self.rate[processor] if processor in ground else
                           whole_service(state, [tile], processor))
                ready = max(self.busy[processor], arrivals[tid]) + service
                self.busy[processor] = ready
                route = self.delivery(processor, ground, quantize(ready, dt),
                                      tile["result_size_mb"], dt, hops + 1)
                if route is None:
                    return None
                finish = max(finish, route[0])
        return max(t["waiting_time"] for t in state.tiles.values()) + finish + state.config.get("postprocessing_delay_s", 0)


class ThreePlanChoice(CGSComputeAdapted):
    def __init__(self, raw_hops):
        super().__init__()
        self.raw_hops = raw_hops
        self.raw_tasks = set()
        self.decisions = []

    def select_actions(self, state):
        state = replace(state, tiles={tid: t for tid, t in state.tiles.items()
                                      if t["parent_task_id"] not in self.raw_tasks})
        if not state.tiles:
            return []
        groups = {}
        for tid, tile in sorted(state.tiles.items(), key=lambda row: (-row[1]["waiting_time"], row[0])):
            groups.setdefault(tile["parent_task_id"], {})[tid] = tile
        calendar = Calendar(state)
        chosen = []
        for task, tiles in groups.items():
            subset = replace(state, tiles=tiles,
                             satellites={sid: {**s, "committed_compute_s": calendar.busy[sid],
                                                "mem_remaining_gb": calendar.memory[sid]}
                                         for sid, s in state.satellites.items()},
                             ground_stations={gid: {**g, "committed_compute_s": calendar.busy[ground_node(gid)]}
                                              for gid, g in state.ground_stations.items()})
            complete = (len(tiles) == len(state.pending_task_tiles.get(task, {})) and
                        all(t["state"] == "QUEUED" and not t["in_transfer"] and
                            not t["task_progress"]["failed"] and
                            t["task_progress"].get("total", len(tiles)) == len(tiles) and
                            not any(t["task_progress"].get(k, 0) for k in ("done", "running", "awaiting_delivery"))
                            for t in tiles.values()))
            cgs = self._select_actions_with_plan(subset, deepcopy(calendar.plan))
            if not complete:
                chosen.extend(cgs)
                continue
            options = {"local": [Action(tid, ActionType.LOCAL) for tid in tiles], "cgs": cgs,
                       "ground": [RawImageAction(tid, ActionType.OFFLOAD, raw_hops=self.raw_hops) for tid in tiles]}
            trials, quotes = {}, {}
            for name, actions in options.items():
                trial = deepcopy(calendar)
                quotes[name] = trial.quote(subset, actions, raw=name == "ground", raw_hops=self.raw_hops)
                trials[name] = trial
            winner = min(options, key=lambda name: quotes[name] if quotes[name] is not None else math.inf)
            if quotes[winner] is None:
                winner = "cgs"
                self.stats["no_finite_quote"] += 1
            else:
                calendar = trials[winner]
            chosen.extend(options[winner])
            if winner == "ground":
                self.raw_tasks.add(task)
            self.stats["selected_" + winner] += 1
            self.decisions.append({"task_id": task, "time_seconds": state.time * state.config["dt"],
                                   "quotes_seconds": quotes, "selected": winner})
        return chosen
