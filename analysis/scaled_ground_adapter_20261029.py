"""独立多站评估适配；不修改既有冻结源码、模型结构或单站路径。"""
from collections import Counter
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from dataclasses import replace
import math
from unittest.mock import patch

from analysis import main_bc_interface as fixed
from analysis import main_rl_interface as base
from analysis import dynamic_learning_interface as dynamic
from analysis import dynamic_granularity_v2 as granularity
from analysis import local_information_baselines as local_rules
from analysis import sar_three_plan as three
from analysis import run_dynamic_heuristics_v2 as resource_module
from sim import routing
from sim.env import SimulationEnv
from sim.entities import TileState
from sim.scheduler import literature, task_choice
from sim.scheduler.literature import ground_node

OriginalCalendar = three.Calendar
original_observation = fixed.build_observation
original_processing = literature.processing_state
original_inputs = literature.input_options


def public_state(state):
    """选站只用公开几何、容量和输入字节；不使用任何队列或内存真值。"""
    return replace(state, satellites={sid: {**row, 'committed_compute_s': 0.,
        'isl_backlog_mb': {}, 'ground_backlog_mb': {},
        'mem_remaining_gb': state.config['mem_capacity_gb']}
        for sid, row in state.satellites.items()},
        ground_stations={gid: {**row, 'committed_compute_s': 0.}
                         for gid, row in state.ground_stations.items()})


def choose_station(state, *, raw=False, hops=None):
    """按完整输入预测到达时间选站，等时按站ID；不依性能结果改规则。"""
    if len(state.ground_stations) == 1:
        return next(iter(state.ground_stations))
    if not state.tiles:
        return None
    view = public_state(state)
    if not raw:
        plan = literature.contact_plan(view)
        choices = [fixed.options(view, tile, plan) for tile in view.tiles.values()]
        candidates = [(max(opts[ground_node(gid)][0] for opts in choices), gid)
                      for gid in view.ground_stations
                      if all(ground_node(gid) in opts for opts in choices)]
    else:
        candidates = []
        for gid in sorted(view.ground_stations):
            calendar = OriginalCalendar(view)
            arrivals = []
            for tid in sorted(view.tiles, key=lambda t: (-view.tiles[t]['waiting_time'], t)):
                tile = view.tiles[tid]
                size = tile.get('unique_input_size_mb')
                if size is None:
                    size = tile.get('task_image_size_mb', view.config['image_size_mb'])/len(view.tiles)
                result = calendar.delivery(tile['location'], {ground_node(gid)}, 0., size,
                    view.config['dt'], (hops if hops is not None else view.config['relay_max_hops'])+1)
                if result is None:
                    break
                arrivals.append(result[0])
            if len(arrivals) == len(view.tiles):
                candidates.append((max(arrivals), gid))
    return min(candidates)[1] if candidates else None


class PoolBusy(dict):
    """多个接收端口共用一个计算承诺，更新任一端口即同步所有别名。"""
    def __setitem__(self, key, value):
        if isinstance(key, tuple) and key[0] == 'ground':
            for alias in list(self):
                if isinstance(alias, tuple) and alias[0] == 'ground':
                    dict.__setitem__(self, alias, value)
        dict.__setitem__(self, key, value)


def processing_state(state):
    rate, busy = original_processing(state)
    return rate, PoolBusy(busy) if len(state.ground_stations) > 1 else busy


def input_options(state, plan, tile, memory, **kwargs):
    options = original_inputs(state, plan, tile, memory, **kwargs)
    if len(state.ground_stations) == 1:
        return options
    # 分块送地面按该块选站；已提交动作的报价仍允许其固定目标。
    gid = choose_station(replace(state, tiles={'candidate': tile}))
    return {node: value for node, value in options.items()
            if not isinstance(node, tuple) or node == ground_node(gid)}


class SharedCalendar(OriginalCalendar):
    def __init__(self, state):
        super().__init__(state)
        if len(state.ground_stations) > 1:
            self.busy = PoolBusy(self.busy)
        self.raw_destination = None

    def delivery(self, source, destinations, ready, size, dt, hops):
        if self.raw_destination is not None:
            destinations = {ground_node(self.raw_destination)}
        return super().delivery(source, destinations, ready, size, dt, hops)

    def quote(self, state, actions, *, raw=False, raw_hops=45):
        if raw and len(state.ground_stations) > 1:
            target = choose_station(state, raw=True, hops=raw_hops)
            if target is None:
                return None
            for action in actions:
                action.target_gs_id = target
            self.raw_destination = target
        try:
            return super().quote(state, actions, raw=raw, raw_hops=raw_hops)
        finally:
            self.raw_destination = None


def build_observation(state, task, tiles):
    if len(state.ground_stations) == 1:
        return original_observation(state, task, tiles)
    obs = base.build_observation(state, task, tiles)
    gid = choose_station(replace(state, tiles=tiles))
    # 不可达地面槽保持存在但mask关闭，形状和训练时完全一致。
    selected = gid if gid is not None else sorted(state.ground_stations)[0]
    ground = ground_node(selected)
    plan = literature.contact_plan(state)
    choices = [fixed.options(state, tile, plan) for tile in tiles.values()]
    available = gid is not None and all(ground in opts for opts in choices)
    arrival = max((opts[ground][0] for opts in choices if ground in opts), default=600.)
    gs = state.ground_stations[selected]
    row = dict(gs, mem_remaining_gb=state.config['mem_capacity_gb'],
        vram_remaining_gb=state.config['vram_capacity_gb'], running_count=gs.get('running', 0),
        advertisement_age_s=15., next_gs_windows={selected: (0., 600.)})
    obs['node_ids'].insert(1, fixed.GROUND)
    obs['nodes'].insert(1, base.node_features(row, arrival=arrival,
        mem_capacity=state.config['mem_capacity_gb'], vram_capacity=state.config['vram_capacity_gb']))
    obs['node_valid'].insert(1, True)
    obs['position_mask'].insert(1, available)
    for row in obs['adjacency']:
        row.insert(1, True)
    obs['adjacency'].insert(1, [True]*fixed.POSITIONS)
    obs.update(schema=fixed.SCHEMA, ground_id=selected)
    obs['image_mask'][0] = any(obs['position_mask'])
    return obs


class SharedPipeline(three.CandidatePipeline):
    def __init__(self, env):
        super().__init__(env)
        self.raw_stations = {}

    def apply_actions(self, actions, links):
        if len(self.env.ground_stations) > 1:
            for action in actions:
                if isinstance(action, three.RawImageAction):
                    task = self.env.tiles[action.tile_id].parent_task_id
                    assert action.target_gs_id in self.env.ground_stations, 'Raw target must be committed by policy'
                    previous = self.raw_stations.setdefault(task, action.target_gs_id)
                    assert previous == action.target_gs_id
        super().apply_actions(actions, links)

    def route_unique(self, links):
        if len(self.env.ground_stations) == 1:
            return super().route_unique(links)
        env = self.env
        no_routes = set()
        for tile in sorted(env.open_tiles(), key=lambda t: (t.timestamps.created, t.tile_id)):
            hops = self.raw_tasks.get(tile.parent_task_id)
            if hops is None or tile.state not in (TileState.QUEUED, TileState.READY):
                continue
            if not tile.raw_relay_path:
                tile.raw_relay_path = [tile.location]
            gid = self.raw_stations[tile.parent_task_id]
            key = (hops, gid, tile.location, tile.data_size_mb, tuple(tile.raw_relay_path))
            if key in no_routes:
                continue
            route = routing.delivery_route(self.routing_views[hops], tile.location, tile.data_size_mb,
                links, tile.raw_relay_path, target_gs_id=gid)
            if route is None:
                no_routes.add(key)
                continue
            no_routes.clear()
            kind, target = route
            if kind == 'ground':
                self._offload_to_gs(tile, target)
            else:
                self._offload_to_sat(tile, target, links)

    def _assemble_unique_image(self, tile, gs_id):
        super()._assemble_unique_image(tile, self.env.pool_owner)

    def advance_ground_tile_transfers(self):
        env = self.env
        if len(env.ground_stations) == 1:
            return super().advance_ground_tile_transfers()
        # 到达时直接进入唯一FIFO；原图必须等全部唯一块到齐才入队。
        still = []
        for tr in env.ground_tile_transfers:
            tile = env.tiles[tr.tile_id]
            if (not env.vis_cache.is_visible(tr.src_sat, tr.gs_id, env.ground_stations)
                    or tr.remaining_mb > 1e-6):
                still.append(tr)
                continue
            env._release_tile_memory(tile, tr.src_sat)
            tile.state = TileState.GROUND_QUEUED
            tile.timestamps.end_tx = env.time+1
            if tile.parent_task_id in self.raw_tasks:
                self._assemble_unique_image(tile, tr.gs_id)
            else:
                env.ground_tile_queues[env.pool_owner].append(tile.tile_id)
            env._log_tile_event(tile, 'tx_end_ground', sat_from=tr.src_sat, sat_to=tr.gs_id)
        env.ground_tile_transfers = still


class SharedEnv(granularity.GranularityEnv):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pool_owner = next(iter(self.ground_stations))
        assert len({g.compute_rate for g in self.ground_stations.values()}) == 1
        self._dist_pipeline = SharedPipeline(self)

    def export_state(self):
        if len(self.ground_stations) == 1:
            return super().export_state()
        state = SimulationEnv.export_state(self)
        raw_tasks = self._dist_pipeline.raw_tasks
        incoming = {tr.tile_id: tr.dst for tr in self.transfers if not tr.is_result}
        windows, seconds = Counter(), Counter()
        for tile in self.open_tiles():
            if tile.parent_task_id in raw_tasks:
                if tile.tile_id in incoming:
                    state.satellites[incoming[tile.tile_id]]['committed_result_mb'] -= self.tile_result_size_mb(tile)
                continue
            if tile.ground_relay_target is not None and tile.state in (TileState.GROUND_RELAY_PENDING, TileState.TRANSFERRING):
                continue
            sid = incoming.get(tile.tile_id, tile.location)
            routed = tile.input_target_sat_id is not None and tile.state in (TileState.TRANSFERRING, TileState.INPUT_RELAY)
            if routed:
                sid = tile.input_target_sat_id
            if routed or tile.tile_id in incoming or tile.state in (TileState.READY, TileState.RUNNING):
                service = float(tile.remaining_compute or 0) if tile.state == TileState.RUNNING else tile.compute_cost/self.satellites[sid].compute_rate
                seconds[sid] += self.compute_service_seconds(service)
                windows[sid] += tile.compute_cost-tile.completed_compute_windows
        for sid, satellite in self.satellites.items():
            if self.cfg.onboard_compute_profile is None:
                state.satellites[sid]['committed_compute_s'] = seconds[sid]
            else:
                pending = max(0, round(windows[sid])-len(satellite.executing_batch))
                duration = satellite.batch_remaining_s
                service = self.cfg.onboard_compute_profile['service_s']
                for size in sorted(map(int, service), reverse=True):
                    count, pending = divmod(pending, size)
                    duration += count*service[str(size)]
                state.satellites[sid]['committed_compute_s'] = duration
        tids = {tid for queue in self.ground_tile_queues.values() for tid in queue}
        tids.update(self.ground_tile_running.values())
        tids.update(tr.tile_id for tr in self.ground_tile_transfers)
        tids.update(t.tile_id for t in self.open_tiles() if t.parent_task_id in raw_tasks or
                    t.ground_relay_target is not None)
        running = set(self.ground_tile_running.values())
        rate = self.ground_stations[self.pool_owner].compute_rate
        busy = math.fsum(self.compute_service_seconds(float(self.tiles[tid].remaining_compute or 0)
            if tid in running else self.tiles[tid].compute_cost/rate) for tid in sorted(tids)
            if self.tiles[tid].state not in (TileState.DONE, TileState.FAILED))
        for gs in state.ground_stations.values():
            gs.update(committed_compute_s=busy, running=len(running),
                queue_len=sum(map(len, self.ground_tile_queues.values())))
        return replace(state, tiles={tid: t for tid, t in state.tiles.items() if t['parent_task_id'] not in raw_tasks},
            pending_task_tiles={task: rows for task, rows in state.pending_task_tiles.items() if task not in raw_tasks})


class StationPolicy:
    def __init__(self, policy, env):
        self.policy, self.env = policy, env

    def __getattr__(self, name):
        return getattr(self.policy, name)

    def select_actions(self, state):
        actions = self.policy.select_actions(state)
        if len(state.ground_stations) == 1:
            return actions
        targets = {}
        for action in actions:
            if isinstance(action, three.RawImageAction) and action.target_gs_id is None:
                task = self.env.tiles[action.tile_id].parent_task_id
                if task not in targets:
                    rows = {tid: t for tid, t in state.tiles.items() if t['parent_task_id'] == task}
                    targets[task] = choose_station(replace(state, tiles=rows), raw=True, hops=action.raw_hops)
                assert targets[task] is not None
                action.target_gs_id = targets[task]
        return actions


class StationTraffic(resource_module.TrafficAudit):
    def snapshot(self):
        result = super().snapshot()
        result['ground_compute_busy_s'] = dict(self.env.ground_compute_busy_time)
        return result


def resources(snapshot, helpers, duration, cfg):
    result = resource_module.resources(snapshot, helpers, duration, cfg)
    result['stations'] = {g['id']: dict(bytes=snapshot['network_mb'].get('ground:'+g['id'], 0)*1e6,
        utilization=snapshot['network_mb'].get('ground:'+g['id'], 0)/
            (g['bandwidth_mbps']/8*g.get('receive_channels', 1)*duration)) for g in cfg.ground_stations}
    busy = math.fsum(snapshot.get('ground_compute_busy_s', {}).values())
    assert busy <= duration+1e-6
    result.update(ground_pool_busy_s=busy, ground_pool_utilization=busy/duration)
    return result


@contextmanager
def installed(engine):
    """只在本任务单线程worker内替换模块绑定；退出后恢复原入口。"""
    with ExitStack() as stack:
        for module in (three, granularity, base):
            stack.enter_context(patch.object(module, 'Calendar', SharedCalendar))
        for module in (three, literature, task_choice):
            stack.enter_context(patch.object(module, 'processing_state', processing_state))
        stack.enter_context(patch.object(literature, 'input_options', input_options))
        stack.enter_context(patch.object(local_rules, 'input_options', input_options))
        stack.enter_context(patch.object(fixed, 'build_observation', build_observation))
        # teacher内部CGS读literature.input_options；最终整图复核保留提交目标。
        original_fixed, original_dynamic = fixed.CommittedPolicy, dynamic.DynamicPolicy
        original_set = engine.policy_set
        active_env = []
        def environment(cfg):
            env = SharedEnv(cfg); active_env.append(env); return env
        def policies(method, sources, hops):
            plane, values = original_set(method, sources, hops)
            return plane, {sid: StationPolicy(policy, active_env[-1]) for sid, policy in values.items()}
        stack.enter_context(patch.object(engine, 'GranularityEnv', environment))
        stack.enter_context(patch.object(fixed, 'CommittedPolicy', lambda *a: StationPolicy(original_fixed(*a), active_env[-1])))
        stack.enter_context(patch.object(dynamic, 'DynamicPolicy', lambda *a: StationPolicy(original_dynamic(*a), active_env[-1])))
        stack.enter_context(patch.object(engine, 'policy_set', policies))
        stack.enter_context(patch.object(engine, 'TrafficAudit', StationTraffic))
        stack.enter_context(patch.object(engine, 'resources', resources))
        yield
