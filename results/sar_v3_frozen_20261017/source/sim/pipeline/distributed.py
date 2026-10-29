"""分布式星上计算 pipeline。

负责 distributed 模式下的完整数据流：
  调度决策 (apply_actions) →
  ISL 传输推进 (advance_transfers) →
  星上计算推进 (advance_compute) →
  地面站 tile 上传 (advance_ground_tile_transfers) →
  地面站 tile 计算 (advance_ground_tile_compute) →
  下行链路启动 (start_downlinks) →
  下行链路推进 (advance_downlinks)
"""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Dict, List, Optional

from sim.entities import (
    Action,
    ActionType,
    DownlinkTransfer,
    FailureReason,
    GroundTileTransfer,
    TileState,
    Transfer,
)
from sim.topology import link_key
from sim.network import advance_ground_transfers, fair_bytes
from sim.routing import delivery_route

if TYPE_CHECKING:
    from sim.env import SimulationEnv


class DistributedPipeline:
    """封装 distributed 模式的所有推进逻辑，持有对 env 的引用。"""

    def __init__(self, env: "SimulationEnv") -> None:
        self.env = env
        self._input_relay_waiting: set[str] = set()
        self._ground_relay_waiting: set[str] = set()
        # ground_unique_relay：已落地但整图尚未到齐的传输块，按任务记账。
        self._unique_landed: Dict[str, set[str]] = {}
        self._up_cache = None

    def _up_directions(self, links):
        """按 links 原顺序列出已连通链路的两个方向。

        环境只在拓扑采样步通过 `_get_links` 换新字典；同一采样周期内复用缓存。
        测试替身会原地改写 `topology.links`，此时 `_links_cache_time` 不足以发现
        变化，故非环境快照（字典不是 `env._links_cache`）时总是重算。
        """
        env = self.env
        if links is env._links_cache:
            key = (id(links), env._links_cache_time)
            if self._up_cache is not None and self._up_cache[0] == key and not env._links_mutable:
                return self._up_cache[1]
        else:
            key = None
        directions = []
        for lk in links.values():
            if lk.up:
                directions.append((lk.i, lk.j, f"isl:{lk.i}->{lk.j}", lk))
                directions.append((lk.j, lk.i, f"isl:{lk.j}->{lk.i}", lk))
        self._up_cache = (key, directions)
        return directions

    # ------------------------------------------------------------------ #
    # 公共入口（由 env.step 调用）                                          #
    # ------------------------------------------------------------------ #

    def run(self, actions: List[Action], links: Dict[str, object]) -> None:
        if self.env.pipeline_mode in {"ground_relay", "ground_tiles", "ground_unique_relay"}:
            self.route_raw_tiles(links)
        else:
            self.advance_input_relays(links)
            self.advance_ground_input_relays(links)
            self.apply_actions(actions, links)
            self.advance_compute()
        self.advance_ground_tile_compute()
        self.start_downlinks(links)
        self.advance_transfers(links)
        advance_ground_transfers(self.env, self.env.ground_tile_transfers + self.env.downlink_transfers)
        self.advance_ground_tile_transfers()
        self.advance_downlinks()

    def route_raw_tiles(self, links):
        env = self.env
        no_routes = set()
        direct_visible = {}
        for tile in sorted(env.open_tiles(), key=lambda t: (t.timestamps.created, t.tile_id)):
            if tile.state not in (TileState.QUEUED, TileState.READY):
                continue
            if not tile.raw_relay_path:
                tile.raw_relay_path = [tile.location]
            if env.pipeline_mode == "ground_tiles":
                if tile.location not in direct_visible:
                    direct_visible[tile.location] = any(
                        env.vis_cache.is_visible(tile.location, gs_id, env.ground_stations)
                        for gs_id in env.ground_stations
                    )
                if not direct_visible[tile.location]:
                    continue
            route_key = (tile.location, tile.data_size_mb, tuple(tile.raw_relay_path))
            if route_key in no_routes:
                continue
            route = delivery_route(env, tile.location, tile.data_size_mb,
                                   {} if env.pipeline_mode == "ground_tiles" else links,
                                   tile.raw_relay_path)
            if route is None:
                no_routes.add(route_key)
                continue
            # 只复用本次调用内的无路判断；尝试传输可能改变资源或排队状态。
            no_routes.clear()
            kind, target = route
            if kind == "ground":
                self._offload_to_gs(tile, target)
            else:
                self._offload_to_sat(tile, target, links)

    # ------------------------------------------------------------------ #
    # 调度决策                                                              #
    # ------------------------------------------------------------------ #

    def apply_actions(self, actions: List[Action], links: Dict[str, object]) -> None:
        env = self.env
        action_map = {a.tile_id: a for a in actions}
        transfer_tile_ids = {tr.tile_id for tr in env.transfers}
        transfer_tile_ids.update(tr.tile_id for tr in env.ground_tile_transfers)

        for tile_id, action in action_map.items():
            tile = env.tiles.get(tile_id)
            if tile is None or tile.state not in (TileState.QUEUED, TileState.READY):
                continue
            if env.cfg.execution_commitment and tile.state == TileState.READY:
                continue
            if tile_id in transfer_tile_ids:
                continue
            if action.relay_to_ground is not False:
                self._dispatch_ground_input_relay(tile, action, links)
                continue
            if action.input_path != ():
                self._dispatch_input_path(tile, action, links)
                continue
            if action.action_type == ActionType.WAIT:
                continue

            if action.action_type == ActionType.LOCAL:
                tile.state = TileState.READY
                env._log_tile_event(
                    tile, "local_ready", sat_from=tile.location, sat_to=tile.location
                )
                continue

            if action.action_type == ActionType.OFFLOAD:
                if action.target_gs_id is not None:
                    self._offload_to_gs(tile, action.target_gs_id)
                    continue
                if action.target_sat_id is None:
                    continue
                if action.target_sat_id == tile.location:
                    tile.state = TileState.READY
                    continue
                self._offload_to_sat(tile, action.target_sat_id, links)

    def _dispatch_ground_input_relay(self, tile, action: Action, links: Dict[str, object]) -> None:
        env = self.env
        if (not env.cfg.ground_input_relay or action.relay_to_ground is not True
                or tile.state != TileState.QUEUED or action.action_type != ActionType.OFFLOAD
                or action.target_sat_id is not None or action.input_path != ()
                or type(action.target_gs_id) is not str or action.target_gs_id not in env.ground_stations):
            return
        tile.ground_relay_target = action.target_gs_id
        if not tile.raw_relay_path:
            tile.raw_relay_path = [tile.location]
        tile.state = TileState.GROUND_RELAY_PENDING
        queue = env.satellites[tile.location].queue
        if tile.tile_id in queue:
            queue.remove(tile.tile_id)
        self._ground_relay_waiting.add(tile.tile_id)
        env._log_tile_event(tile, "ground_relay_committed", sat_from=tile.location,
                            sat_to=tile.ground_relay_target)
        self._advance_ground_input_relay(tile, links)

    def advance_ground_input_relays(self, links: Dict[str, object]) -> None:
        env = self.env
        no_routes = set()
        for tid in sorted(self._ground_relay_waiting,
                          key=lambda key: (env.tiles[key].timestamps.created, key)):
            tile = env.tiles[tid]
            if tile.state != TileState.GROUND_RELAY_PENDING:
                self._ground_relay_waiting.discard(tid)
                continue
            if tile.timestamps.end_tx is not None and env.time < tile.timestamps.end_tx:
                continue
            key = (tile.location, tile.data_size_mb, tuple(tile.raw_relay_path), tile.ground_relay_target)
            if key in no_routes:
                continue
            if self._advance_ground_input_relay(tile, links):
                no_routes.clear()
            else:
                no_routes.add(key)

    def _advance_ground_input_relay(self, tile, links: Dict[str, object]) -> bool:
        env = self.env
        if tile.timestamps.end_tx is not None and env.time < tile.timestamps.end_tx:
            return False
        route = delivery_route(env, tile.location, tile.data_size_mb, links, tile.raw_relay_path,
                               target_gs_id=tile.ground_relay_target)
        if route is None:
            return False
        kind, target = route
        if kind == "ground":
            self._offload_to_gs(tile, target)
        else:
            self._offload_to_sat(tile, target, links)
        self._ground_relay_waiting.discard(tile.tile_id)
        return True

    def _dispatch_input_path(self, tile, action: Action, links: Dict[str, object]) -> None:
        env = self.env
        path = action.input_path
        if (tile.state != TileState.QUEUED or action.action_type != ActionType.OFFLOAD
                or action.target_gs_id is not None or not isinstance(path, tuple) or not path
                or any(type(sid) is not int or sid not in env.satellites for sid in path)
                or len(set(path)) != len(path) or path[0] != tile.location
                or type(action.target_sat_id) is not int or path[-1] != action.target_sat_id
                or len(path) - 1 > env.cfg.input_relay_max_hops):
            return
        for src, dst in zip(path, path[1:]):
            link = links.get(link_key(src, dst))
            if link is None or not link.up or link.bandwidth_mbps <= 0:
                return
        if len(path) == 1:
            tile.input_route = path
            tile.input_target_sat_id = path[-1]
            tile.raw_relay_path = [tile.location]
            tile.state = TileState.READY
            return

        # 先原子接纳最终缓存和第一跳缓存；最终缓存只是预约，不代表数据已到达。
        holders = list(dict.fromkeys((path[-1], path[1])))
        size_gb = env.memory_size_gb(tile.data_size_mb)
        if any(sid not in tile.memory_holders and
               env.satellites[sid].mem_used_gb + size_gb > env.satellites[sid].mem_capacity_gb
               for sid in holders):
            return
        added = []
        for sid in holders:
            existing = sid in tile.memory_holders
            if not env._reserve_tile_memory(tile, sid):
                for reserved in added:
                    env._release_tile_memory(tile, reserved)
                return
            if not existing:
                added.append(sid)
        tile.input_route = path
        tile.input_target_sat_id = path[-1]
        tile.raw_relay_path = [tile.location]
        self._offload_to_sat(tile, path[1], links)

    def advance_input_relays(self, links: Dict[str, object]) -> None:
        env = self.env
        for tid in sorted(self._input_relay_waiting):
            tile = env.tiles[tid]
            if tile.state != TileState.INPUT_RELAY:
                self._input_relay_waiting.discard(tid)
                continue
            if tile.timestamps.end_tx is None or env.time < tile.timestamps.end_tx:
                continue
            next_sid = tile.input_route[tile.input_route.index(tile.location) + 1]
            link = links.get(link_key(tile.location, next_sid))
            if link is None or not link.up or link.bandwidth_mbps <= 0:
                continue
            if not env._reserve_tile_memory(tile, next_sid):
                continue
            self._offload_to_sat(tile, next_sid, links)
            self._input_relay_waiting.discard(tid)

    def _offload_to_gs(self, tile, target_gs_id: str) -> None:
        env = self.env
        gs = env.ground_stations.get(target_gs_id)
        if gs is None:
            env._fail_tile(tile, FailureReason.NO_ROUTE)
            return
        if not env.vis_cache.is_visible(tile.location, gs.gs_id, env.ground_stations):
            env._fail_tile(tile, FailureReason.LINK_DOWN)
            return
        tr = GroundTileTransfer(
            tile_id=tile.tile_id,
            src_sat=tile.location,
            gs_id=gs.gs_id,
            remaining_mb=tile.data_size_mb,
            start_time=env.time,
        )
        tile.state = TileState.TRANSFERRING
        if tile.ground_relay_target is None or tile.timestamps.start_tx is None:
            tile.timestamps.start_tx = env.time
        src_sat = env.satellites[tile.location]
        if tile.tile_id in src_sat.queue:
            src_sat.queue.remove(tile.tile_id)
        env.ground_tile_transfers.append(tr)
        env._log_tile_event(
            tile,
            "tx_start_ground",
            sat_from=tr.src_sat,
            sat_to=tr.gs_id,
            extra={"remaining_mb": tr.remaining_mb},
        )

    def _offload_to_sat(
        self, tile, target_sat_id: int, links: Dict[str, object]
    ) -> None:
        env = self.env
        lk = links.get(link_key(tile.location, target_sat_id))
        if not lk:
            env._fail_tile(tile, FailureReason.NO_ROUTE)
            return
        if not lk.up:
            env._fail_tile(tile, FailureReason.LINK_DOWN)
            return
        dst = env.satellites[target_sat_id]
        if not env._reserve_tile_memory(tile, target_sat_id):
            env._fail_tile(tile, FailureReason.MEM_FULL)
            return
        transfer = Transfer(
            tile_id=tile.tile_id,
            src=tile.location,
            dst=target_sat_id,
            remaining_mb=tile.data_size_mb,
            start_time=env.time,
            link_key=link_key(tile.location, target_sat_id),
        )
        tile.state = TileState.TRANSFERRING
        if (not tile.input_route and tile.ground_relay_target is None) or tile.timestamps.start_tx is None:
            tile.timestamps.start_tx = env.time
        src_sat = env.satellites[tile.location]
        if tile.tile_id in src_sat.queue:
            src_sat.queue.remove(tile.tile_id)
        env.transfers.append(transfer)
        env._log_tile_event(
            tile,
            "tx_start",
            sat_from=transfer.src,
            sat_to=transfer.dst,
            extra={"remaining_mb": transfer.remaining_mb},
        )

    # ------------------------------------------------------------------ #
    # ISL 传输推进                                                          #
    # ------------------------------------------------------------------ #

    def advance_transfers(self, links: Dict[str, object]) -> None:
        env = self.env
        still_transfers: List[Transfer] = []
        # 每个方向单独计容量；同向 tile 共享带宽，不因切片而扩容。
        groups = {}
        for tr in env.transfers:
            groups.setdefault((tr.src, tr.dst), []).append(tr)
        sent, offered = env.metrics.network_sent_mb, env.metrics.network_capacity_mb
        dt = env.cfg.dt
        # 与逐条 record_network 同序累加：空闲方向记 0 字节与该步容量。
        for src, dst, label, lk in self._up_directions(links):
            capacity = lk.bandwidth_mbps / 8.0 * dt
            flows = groups.get((src, dst))
            if not flows:
                sent[label] = sent.get(label, 0.0) + 0
                offered[label] = offered.get(label, 0.0) + capacity
                continue
            allocations = fair_bytes([tr.remaining_mb for tr in flows], capacity)
            for tr, amount in zip(flows, allocations):
                tr.remaining_mb = max(0.0, tr.remaining_mb - amount)
            sent[label] = sent.get(label, 0.0) + sum(allocations)
            offered[label] = offered.get(label, 0.0) + capacity
        for tr in list(env.transfers):
            lk = links.get(tr.link_key)
            if not lk or not lk.up:
                if env.cfg.transfer_fail_on_link_down:
                    tile = env.tiles[tr.tile_id]
                    env._fail_tile(tile, FailureReason.LINK_DOWN)
                else:
                    still_transfers.append(tr)
                continue
            if tr.remaining_mb <= 1e-6:
                tile = env.tiles[tr.tile_id]
                src_sat = env.satellites[tr.src]
                dst_sat = env.satellites[tr.dst]
                if tr.is_result:
                    env._release_result_memory(tile, tr.src)
                    tile.location = tr.dst
                    tile.state = TileState.COMPUTED
                    tile.result_available_time = env.time + 1
                    tile.result_path.append(tr.dst)
                    env._computed_waiting_downlink.add(tile.tile_id)
                    env._log_tile_event(tile, "result_tx_end", sat_from=tr.src, sat_to=tr.dst,
                                        extra={"arrival_step": env.time + 1})
                    continue
                env._release_tile_memory(tile, tr.src)
                tile.location = tr.dst
                if tile.raw_relay_path:
                    tile.raw_relay_path.append(tr.dst)
                tile.timestamps.end_tx = env.time + 1
                if tile.ground_relay_target is not None:
                    tile.state = TileState.GROUND_RELAY_PENDING
                    self._ground_relay_waiting.add(tile.tile_id)
                elif tile.input_route and tr.dst != tile.input_target_sat_id:
                    tile.state = TileState.INPUT_RELAY
                    self._input_relay_waiting.add(tile.tile_id)
                else:
                    tile.state = TileState.READY
                    dst_sat.queue.append(tile.tile_id)
                env._log_tile_event(tile, "tx_end", sat_from=tr.src, sat_to=tr.dst)
                event = {TileState.INPUT_RELAY: "input_relay_wait",
                         TileState.GROUND_RELAY_PENDING: "ground_relay_wait"}.get(tile.state, "queued")
                env._log_tile_event(tile, event, sat_from=tr.src, sat_to=tr.dst)
            else:
                still_transfers.append(tr)
        env.transfers = still_transfers

    # ------------------------------------------------------------------ #
    # 星上计算推进                                                          #
    # ------------------------------------------------------------------ #

    def advance_compute(self) -> None:
        env = self.env
        if env.cfg.onboard_compute_profile is not None:
            self._advance_compute_cohorts()
            return
        if getattr(env.cfg, "compute_service_mode", "step") == "work_conserving":
            self._advance_compute_budget()
            return
        for sat_id, sat in env.satellites.items():
            if sat.executing is None:
                next_tile_id: Optional[str] = None
                for tid in list(sat.queue):
                    tile = env.tiles[tid]
                    if tile.state == TileState.READY and tile.location == sat_id:
                        next_tile_id = tid
                        break
                if next_tile_id is not None:
                    tile = env.tiles[next_tile_id]
                    if sat.vram_used_gb + tile.vram_req_gb > sat.vram_capacity_gb:
                        if env.cfg.vram_policy == "reject":
                            sat.queue.remove(next_tile_id)
                            env._fail_tile(tile, FailureReason.VRAM_OOM)
                        continue
                    sat.queue.remove(next_tile_id)
                    tile.state = TileState.RUNNING
                    tile.timestamps.start_compute = env.time
                    tile.remaining_compute = tile.compute_cost / sat.compute_rate
                    sat.executing = next_tile_id
                    sat.vram_used_gb += tile.vram_req_gb
                    env.metrics.update_vram_peak(sat_id, sat.vram_used_gb)
                    env._log_tile_event(
                        tile, "compute_start", sat_from=sat_id, sat_to=sat_id
                    )

            if sat.executing is not None:
                tile = env.tiles[sat.executing]
                env.metrics.update_compute_busy(sat_id, min(env.cfg.dt, float(tile.remaining_compute)))
                tile.remaining_compute = max(
                    0.0, float(tile.remaining_compute) - env.cfg.dt
                )
                if tile.remaining_compute <= 1e-6:
                    tile.state = TileState.COMPUTED
                    tile.timestamps.end_compute = env.time + 1
                    env._computed_waiting_downlink.add(tile.tile_id)
                    sat.executing = None
                    sat.vram_used_gb = max(0.0, sat.vram_used_gb - tile.vram_req_gb)
                    env._release_tile_memory(tile, sat_id)
                    tile.result_available_time = env.time + 1
                    tile.result_path = [sat_id]
                    if env.cfg.result_buffering and not env._reserve_result_memory(tile, sat_id):
                        env._fail_tile(tile, FailureReason.MEM_FULL)
                        continue
                    env._log_tile_event(
                        tile, "computed", sat_from=sat_id, sat_to=sat_id
                    )

    def _advance_compute_cohorts(self) -> None:
        """按实测组时长执行；不等待凑批、不按并发数线性加速。"""
        env = self.env
        profile = env.cfg.onboard_compute_profile
        sizes = sorted((int(key) for key in profile["service_s"]), reverse=True)
        for sat_id, sat in env.satellites.items():
            budget = env.cfg.dt
            while budget > 1e-12:
                if not sat.executing_batch:
                    ready = [env.tiles[tid] for tid in sat.queue
                             if env.tiles[tid].state == TileState.READY
                             and env.tiles[tid].location == sat_id
                             and env.tiles[tid].timestamps.created <= env.time
                             and (env.tiles[tid].timestamps.end_tx is None
                                  or env.tiles[tid].timestamps.end_tx <= env.time)]
                    if not ready:
                        break
                    if ready[0].vram_req_gb > sat.vram_capacity_gb:
                        if env.cfg.vram_policy == "reject":
                            env._fail_tile(ready[0], FailureReason.VRAM_OOM)
                            continue
                        break
                    eligible = next((i for i, tile in enumerate(ready)
                                     if tile.vram_req_gb > sat.vram_capacity_gb), len(ready))
                    windows = []
                    for tile in ready[:eligible]:
                        remaining = tile.compute_cost - tile.completed_compute_windows
                        if remaining != int(remaining) or remaining <= 0:
                            raise ValueError("Measured cohorts require positive integer remaining windows")
                        windows.extend([tile] * min(int(remaining), sizes[0] - len(windows)))
                        if len(windows) == sizes[0]:
                            break
                    size = next(n for n in sizes if n <= len(windows))
                    batch = windows[:size]
                    sat.batch_remaining_s = profile["service_s"][str(size)]
                    sat.batch_service_s = sat.batch_remaining_s
                    sat.vram_used_gb = profile["workspace_gb"][str(size)]
                    sat.executing_batch = [tile.tile_id for tile in batch]
                    sat.executing = sat.executing_batch[0]
                    sat.batch_id = f"{sat.executing}:cohort_{len(env.compute_cohort_records)}"
                    env.compute_cohort_records[sat.batch_id] = {
                        "sat_id": sat_id, "tile_ids": list(sat.executing_batch),
                        "service_s": sat.batch_service_s, "remaining_s": sat.batch_remaining_s,
                        "completed_window_tile_ids": []}
                    env.metrics.update_vram_peak(sat_id, sat.vram_used_gb)
                    for tile in {tile.tile_id: tile for tile in batch}.values():
                        sat.queue.remove(tile.tile_id)
                        tile.state = TileState.RUNNING
                        if tile.timestamps.start_compute is None:
                            tile.timestamps.start_compute = env.time
                        tile.remaining_compute = sat.batch_remaining_s
                        env._log_tile_event(tile, "compute_start", sat_from=sat_id, sat_to=sat_id,
                                            extra={"cohort_size": size})
                used = min(budget, sat.batch_remaining_s)
                budget = max(0.0, budget - used)
                sat.batch_remaining_s = max(0.0, sat.batch_remaining_s - used)
                env.compute_cohort_records[sat.batch_id]["remaining_s"] = sat.batch_remaining_s
                env.metrics.update_compute_busy(sat_id, used)
                for tid in sat.executing_batch:
                    env.tiles[tid].remaining_compute = sat.batch_remaining_s
                if sat.batch_remaining_s > 1e-12:
                    break
                completed = list(sat.executing_batch)
                env.compute_cohort_records[sat.batch_id]["completed_window_tile_ids"] = completed
                sat.executing_batch.clear()
                sat.executing = None
                sat.vram_used_gb = 0.0
                unfinished = []
                for tid, count in Counter(completed).items():
                    tile = env.tiles[tid]
                    tile.completed_compute_windows += count
                    if tile.completed_compute_windows < tile.compute_cost:
                        tile.state = TileState.READY
                        tile.remaining_compute = None
                        unfinished.append(tid)
                        continue
                    tile.state = TileState.COMPUTED
                    tile.timestamps.end_compute = env.time + 1
                    tile.result_available_time = env.time + 1
                    tile.result_path = [sat_id]
                    env._computed_waiting_downlink.add(tid)
                    env._release_tile_memory(tile, sat_id)
                    if env.cfg.result_buffering and not env._reserve_result_memory(tile, sat_id):
                        env._fail_tile(tile, FailureReason.MEM_FULL)
                        continue
                    env._log_tile_event(tile, "computed", sat_from=sat_id, sat_to=sat_id)
                sat.queue[:0] = unfinished

    def _advance_compute_budget(self) -> None:
        env = self.env
        for sat_id, sat in env.satellites.items():
            budget = env.cfg.dt
            while budget > 1e-12:
                if sat.executing is None:
                    next_tile = next((env.tiles[tid] for tid in sat.queue
                                      if env.tiles[tid].state == TileState.READY
                                      and env.tiles[tid].location == sat_id
                                      and env.tiles[tid].timestamps.created <= env.time
                                      and (env.tiles[tid].timestamps.end_tx is None
                                           or env.tiles[tid].timestamps.end_tx <= env.time)), None)
                    if next_tile is None:
                        break
                    tile = next_tile
                    if sat.vram_used_gb + tile.vram_req_gb > sat.vram_capacity_gb:
                        if env.cfg.vram_policy == "reject":
                            env._fail_tile(tile, FailureReason.VRAM_OOM)
                            continue
                        break
                    sat.queue.remove(tile.tile_id)
                    tile.state = TileState.RUNNING
                    tile.timestamps.start_compute = env.time
                    tile.remaining_compute = tile.compute_cost / sat.compute_rate
                    sat.executing = tile.tile_id
                    sat.vram_used_gb += tile.vram_req_gb
                    env.metrics.update_vram_peak(sat_id, sat.vram_used_gb)
                    env._log_tile_event(tile, "compute_start", sat_from=sat_id, sat_to=sat_id)
                tile = env.tiles[sat.executing]
                if tile.state != TileState.RUNNING:
                    sat.executing = None
                    sat.vram_used_gb = max(0.0, sat.vram_used_gb - tile.vram_req_gb)
                    continue
                used = min(budget, max(0.0, float(tile.remaining_compute)))
                budget = max(0.0, budget - used)
                tile.remaining_compute = max(0.0, float(tile.remaining_compute) - used)
                env.metrics.update_compute_busy(sat_id, used)
                if tile.remaining_compute > 1e-12:
                    break
                # 复用步内算力，但结果只能在统一步末参与通信。
                tile.state = TileState.COMPUTED
                tile.timestamps.end_compute = env.time + 1
                tile.result_available_time = env.time + 1
                tile.result_path = [sat_id]
                env._computed_waiting_downlink.add(tile.tile_id)
                sat.executing = None
                sat.vram_used_gb = max(0.0, sat.vram_used_gb - tile.vram_req_gb)
                env._release_tile_memory(tile, sat_id)
                if env.cfg.result_buffering and not env._reserve_result_memory(tile, sat_id):
                    env._fail_tile(tile, FailureReason.MEM_FULL)
                    continue
                env._log_tile_event(tile, "computed", sat_from=sat_id, sat_to=sat_id)

    # ------------------------------------------------------------------ #
    # 地面站 tile 上传                                                      #
    # ------------------------------------------------------------------ #

    def advance_ground_tile_transfers(self) -> None:
        env = self.env
        still: List[GroundTileTransfer] = []
        for tr in env.ground_tile_transfers:
            tile = env.tiles[tr.tile_id]
            gs = env.ground_stations.get(tr.gs_id)
            if gs is None:
                still.append(tr)
                continue
            if not env.vis_cache.is_visible(tr.src_sat, tr.gs_id, env.ground_stations):
                still.append(tr)
                continue
            if tr.remaining_mb <= 1e-6:
                src_sat = env.satellites[tr.src_sat]
                env._release_tile_memory(tile, tr.src_sat)
                tile.state = TileState.GROUND_QUEUED
                tile.timestamps.end_tx = env.time + 1
                if env.pipeline_mode == "ground_unique_relay":
                    self._assemble_unique_image(tile, tr.gs_id)
                else:
                    env.ground_tile_queues[tr.gs_id].append(tile.tile_id)
                env._log_tile_event(
                    tile, "tx_end_ground", sat_from=tr.src_sat, sat_to=tr.gs_id
                )
            else:
                still.append(tr)
        env.ground_tile_transfers = still

    def _assemble_unique_image(self, tile, gs_id: str) -> None:
        """全部不重叠块落地后才在最后接收站切出重叠窗并整体排队；地面切窗计 0 秒。"""
        env = self.env
        task = env.tasks[tile.parent_task_id]
        landed = self._unique_landed.setdefault(task.task_id, set())
        landed.add(tile.tile_id)
        if len(landed) < len(task.tile_ids):
            return
        del self._unique_landed[task.task_id]
        env.ground_tile_queues[gs_id].extend(task.tile_ids)
        env._log_task_event(task.task_id, "ground_image_assembled", sat_from=None, sat_to=gs_id)

    # ------------------------------------------------------------------ #
    # 地面站 tile 计算推进                                                   #
    # ------------------------------------------------------------------ #

    def advance_ground_tile_compute(self) -> None:
        env = self.env
        if getattr(env.cfg, "compute_service_mode", "step") == "work_conserving":
            self._advance_ground_tile_compute_budget()
            return
        for gs_id, gs in env.ground_stations.items():
            if gs_id not in env.ground_tile_running:
                queue = env.ground_tile_queues.get(gs_id, [])
                if queue:
                    tid = queue.pop(0)
                    tile = env.tiles[tid]
                    tile.state = TileState.GROUND_RUNNING
                    tile.timestamps.start_compute = env.time
                    tile.remaining_compute = tile.compute_cost / max(
                        1e-9, gs.compute_rate
                    )
                    env.ground_tile_running[gs_id] = tid
                    env._log_tile_event(
                        tile,
                        "ground_compute_start",
                        sat_from=tile.location,
                        sat_to=gs_id,
                    )

            if gs_id in env.ground_tile_running:
                tid = env.ground_tile_running[gs_id]
                tile = env.tiles[tid]
                tile.remaining_compute = max(
                    0.0, float(tile.remaining_compute) - env.cfg.dt
                )
                if tile.remaining_compute <= 1e-6:
                    tile.state = TileState.DONE
                    tile.timestamps.end_compute = env.time + 1
                    tile.timestamps.end_downlink = env.time + 1
                    env.metrics.record_tile_latency(
                        (tile.timestamps.end_downlink or env.time)
                        - tile.timestamps.created
                    )
                    env._log_tile_event(
                        tile,
                        "ground_compute_end",
                        sat_from=tile.location,
                        sat_to=gs_id,
                    )
                    env._log_tile_event(
                        tile, "done", sat_from=tile.location, sat_to=gs_id
                    )
                    env._check_task_done(tile.parent_task_id)
                    env.ground_tile_running.pop(gs_id, None)

    def _advance_ground_tile_compute_budget(self) -> None:
        env = self.env
        for gs_id, gs in env.ground_stations.items():
            budget = env.cfg.dt
            queue = env.ground_tile_queues.setdefault(gs_id, [])
            while budget > 1e-12:
                if gs_id not in env.ground_tile_running:
                    while queue and env.tiles[queue[0]].state != TileState.GROUND_QUEUED:
                        queue.pop(0)
                    if not queue:
                        break
                    tile = env.tiles[queue[0]]
                    if (tile.timestamps.created > env.time or
                            (tile.timestamps.end_tx is not None and tile.timestamps.end_tx > env.time)):
                        break
                    queue.pop(0)
                    tile.state = TileState.GROUND_RUNNING
                    tile.timestamps.start_compute = env.time
                    tile.remaining_compute = tile.compute_cost / max(1e-9, gs.compute_rate)
                    env.ground_tile_running[gs_id] = tile.tile_id
                    env._log_tile_event(tile, "ground_compute_start", sat_from=tile.location, sat_to=gs_id)
                tile = env.tiles[env.ground_tile_running[gs_id]]
                if tile.state != TileState.GROUND_RUNNING:
                    env.ground_tile_running.pop(gs_id, None)
                    continue
                used = min(budget, max(0.0, float(tile.remaining_compute)))
                budget = max(0.0, budget - used)
                tile.remaining_compute = max(0.0, float(tile.remaining_compute) - used)
                env.ground_compute_busy_time[gs_id] = env.ground_compute_busy_time.get(gs_id, 0.0) + used
                if tile.remaining_compute > 1e-12:
                    break
                tile.state = TileState.DONE
                tile.timestamps.end_compute = env.time + 1
                tile.timestamps.end_downlink = env.time + 1
                env.metrics.record_tile_latency(tile.timestamps.end_downlink - tile.timestamps.created)
                env._log_tile_event(tile, "ground_compute_end", sat_from=tile.location, sat_to=gs_id)
                env._log_tile_event(tile, "done", sat_from=tile.location, sat_to=gs_id)
                env._check_task_done(tile.parent_task_id)
                env.ground_tile_running.pop(gs_id, None)

    # ------------------------------------------------------------------ #
    # 下行链路                                                              #
    # ------------------------------------------------------------------ #

    def start_downlinks(self, links=None) -> None:
        env = self.env
        no_routes = set()
        downlinking = {tr.tile_id for tr in env.downlink_transfers}
        pending = list(env._computed_waiting_downlink)
        if env.cfg.result_buffering:
            pending.sort(key=lambda tid: (env.tiles[tid].timestamps.created, tid))
        for tile_id in pending:
            tile = env.tiles.get(tile_id)
            if tile is None or tile.state != TileState.COMPUTED:
                env._computed_waiting_downlink.discard(tile_id)
                continue
            if tile.timestamps.end_compute is not None and tile.timestamps.end_compute > env.time:
                continue
            if tile.result_available_time is not None and tile.result_available_time > env.time:
                continue
            if tile.tile_id in downlinking:
                continue
            result_size_mb = env.tile_result_size_mb(tile)
            gs = env._select_ground_station(tile.location)
            if env.cfg.result_relay:
                route_key = (tile.location, result_size_mb, tuple(tile.result_path))
                if route_key in no_routes:
                    continue
                route = delivery_route(env, tile.location, result_size_mb,
                                       links if links is not None else env._get_links(env.time), tile.result_path)
                if route is None:
                    no_routes.add(route_key)
                    continue
                # 包括资源预留失败的尝试，后续包均重新判断，避免缓存掩盖状态变化。
                no_routes.clear()
                kind, target = route
                if kind == "sat":
                    if not env._reserve_result_memory(tile, target):
                        continue
                    env.transfers.append(Transfer(tile.tile_id, tile.location, target,
                                                  result_size_mb, env.time,
                                                  link_key(tile.location, target), is_result=True))
                    tile.state = TileState.RESULT_TRANSFERRING
                    env._computed_waiting_downlink.discard(tile.tile_id)
                    env._log_tile_event(tile, "result_tx_start", sat_from=tile.location, sat_to=target,
                                        extra={"remaining_mb": result_size_mb})
                    continue
                gs = env.ground_stations[target]
            if gs is None:
                continue
            tr = DownlinkTransfer(
                tile_id=tile.tile_id,
                src_sat=tile.location,
                gs_id=gs.gs_id,
                remaining_mb=result_size_mb,
                start_time=env.time,
            )
            tile.state = TileState.DOWNLINKING
            tile.timestamps.start_downlink = env.time
            env.downlink_transfers.append(tr)
            env._computed_waiting_downlink.discard(tile.tile_id)
            env._log_tile_event(
                tile,
                "downlink_start",
                sat_from=tile.location,
                sat_to=gs.gs_id,
                extra={"remaining_mb": tr.remaining_mb},
            )

    def advance_downlinks(self) -> None:
        env = self.env
        still: List[DownlinkTransfer] = []
        for tr in env.downlink_transfers:
            tile = env.tiles[tr.tile_id]
            gs = env.ground_stations.get(tr.gs_id)
            if gs is None:
                still.append(tr)
                continue
            if not env.vis_cache.is_visible(tr.src_sat, tr.gs_id, env.ground_stations):
                still.append(tr)
                continue
            if tr.remaining_mb <= 1e-6:
                tile.state = TileState.DONE
                env._release_result_memory(tile, tr.src_sat)
                tile.timestamps.end_downlink = env.time + 1
                env.metrics.record_tile_latency(
                    tile.timestamps.end_downlink - tile.timestamps.created
                )
                env._log_tile_event(
                    tile, "downlink_end", sat_from=tr.src_sat, sat_to=tr.gs_id
                )
                env._log_tile_event(tile, "done", sat_from=tr.src_sat, sat_to=tr.gs_id)
                env._check_task_done(tile.parent_task_id)
            else:
                still.append(tr)
        env.downlink_transfers = still
