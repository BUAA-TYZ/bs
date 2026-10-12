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

    # ------------------------------------------------------------------ #
    # 公共入口（由 env.step 调用）                                          #
    # ------------------------------------------------------------------ #

    def run(self, actions: List[Action], links: Dict[str, object]) -> None:
        if self.env.pipeline_mode in {"ground_relay", "ground_tiles"}:
            self.route_raw_tiles(links)
        else:
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
        for tile in sorted(env.tiles.values(), key=lambda t: (t.timestamps.created, t.tile_id)):
            if tile.state not in (TileState.QUEUED, TileState.READY):
                continue
            if not tile.raw_relay_path:
                tile.raw_relay_path = [tile.location]
            route = delivery_route(env, tile.location, tile.data_size_mb,
                                   {} if env.pipeline_mode == "ground_tiles" else links,
                                   tile.raw_relay_path)
            if route is None:
                continue
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
        for key, lk in links.items():
            if not lk.up:
                continue
            capacity = lk.bandwidth_mbps / 8.0 * env.cfg.dt
            for src, dst in ((lk.i, lk.j), (lk.j, lk.i)):
                flows = groups.get((src, dst), [])
                allocations = fair_bytes([tr.remaining_mb for tr in flows], capacity)
                for tr, amount in zip(flows, allocations):
                    tr.remaining_mb = max(0.0, tr.remaining_mb - amount)
                env.metrics.record_network(f"isl:{src}->{dst}", sum(allocations), capacity)
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
                tile.state = TileState.READY
                tile.timestamps.end_tx = env.time + 1
                dst_sat.queue.append(tile.tile_id)
                env._log_tile_event(tile, "tx_end", sat_from=tr.src, sat_to=tr.dst)
                env._log_tile_event(tile, "queued", sat_from=tr.src, sat_to=tr.dst)
            else:
                still_transfers.append(tr)
        env.transfers = still_transfers

    # ------------------------------------------------------------------ #
    # 星上计算推进                                                          #
    # ------------------------------------------------------------------ #

    def advance_compute(self) -> None:
        env = self.env
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
                env.ground_tile_queues[tr.gs_id].append(tile.tile_id)
                env._log_tile_event(
                    tile, "tx_end_ground", sat_from=tr.src_sat, sat_to=tr.gs_id
                )
            else:
                still.append(tr)
        env.ground_tile_transfers = still

    # ------------------------------------------------------------------ #
    # 地面站 tile 计算推进                                                   #
    # ------------------------------------------------------------------ #

    def advance_ground_tile_compute(self) -> None:
        env = self.env
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

    # ------------------------------------------------------------------ #
    # 下行链路                                                              #
    # ------------------------------------------------------------------ #

    def start_downlinks(self, links=None) -> None:
        env = self.env
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
            gs = env._select_ground_station(tile.location)
            if env.cfg.result_relay:
                route = delivery_route(env, tile.location, env.cfg.result_size_mb,
                                       links if links is not None else env._get_links(env.time), tile.result_path)
                if route is None:
                    continue
                kind, target = route
                if kind == "sat":
                    if not env._reserve_result_memory(tile, target):
                        continue
                    env.transfers.append(Transfer(tile.tile_id, tile.location, target,
                                                  env.cfg.result_size_mb, env.time,
                                                  link_key(tile.location, target), is_result=True))
                    tile.state = TileState.RESULT_TRANSFERRING
                    env._computed_waiting_downlink.discard(tile.tile_id)
                    env._log_tile_event(tile, "result_tx_start", sat_from=tile.location, sat_to=target,
                                        extra={"remaining_mb": env.cfg.result_size_mb})
                    continue
                gs = env.ground_stations[target]
            if gs is None:
                continue
            tr = DownlinkTransfer(
                tile_id=tile.tile_id,
                src_sat=tile.location,
                gs_id=gs.gs_id,
                remaining_mb=env.cfg.result_size_mb,
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
