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
        self.apply_actions(actions, links)
        self.advance_transfers(links)
        self.advance_ground_tile_transfers()
        self.advance_compute()
        self.advance_ground_tile_compute()
        self.start_downlinks()
        self.advance_downlinks()

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
        needed_gb = tile.data_size_mb / 1024.0
        if dst.mem_used_gb + needed_gb > dst.mem_capacity_gb:
            env._fail_tile(tile, FailureReason.MEM_FULL)
            return
        dst.mem_used_gb += needed_gb
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
        for tr in env.transfers:
            lk = links.get(tr.link_key)
            if not lk or not lk.up:
                if env.cfg.transfer_fail_on_link_down:
                    tile = env.tiles[tr.tile_id]
                    dst_sat = env.satellites[tr.dst]
                    size_gb = tile.data_size_mb / 1024.0
                    dst_sat.mem_used_gb = max(0.0, dst_sat.mem_used_gb - size_gb)
                    env._fail_tile(tile, FailureReason.LINK_DOWN)
                else:
                    still_transfers.append(tr)
                continue
            rate_mb_s = lk.bandwidth_mbps / 8.0
            sent = rate_mb_s * env.cfg.dt
            tr.remaining_mb -= sent
            if tr.remaining_mb <= 1e-6:
                tile = env.tiles[tr.tile_id]
                src_sat = env.satellites[tr.src]
                dst_sat = env.satellites[tr.dst]
                size_gb = tile.data_size_mb / 1024.0
                src_sat.mem_used_gb = max(0.0, src_sat.mem_used_gb - size_gb)
                tile.location = tr.dst
                tile.state = TileState.READY
                tile.timestamps.end_tx = env.time
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
                tile.remaining_compute = max(
                    0.0, float(tile.remaining_compute) - env.cfg.dt
                )
                if tile.remaining_compute <= 1e-6:
                    tile.state = TileState.COMPUTED
                    tile.timestamps.end_compute = env.time
                    env._computed_waiting_downlink.add(tile.tile_id)
                    sat.executing = None
                    sat.vram_used_gb = max(0.0, sat.vram_used_gb - tile.vram_req_gb)
                    size_gb = tile.data_size_mb / 1024.0
                    sat.mem_used_gb = max(0.0, sat.mem_used_gb - size_gb)
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
            sent = (gs.bandwidth_mbps / 8.0) * env.cfg.dt
            tr.remaining_mb -= sent
            if tr.remaining_mb <= 1e-6:
                src_sat = env.satellites[tr.src_sat]
                size_gb = tile.data_size_mb / 1024.0
                src_sat.mem_used_gb = max(0.0, src_sat.mem_used_gb - size_gb)
                tile.state = TileState.GROUND_QUEUED
                tile.timestamps.end_tx = env.time
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
                    tile.timestamps.end_compute = env.time
                    tile.timestamps.end_downlink = env.time
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

    def start_downlinks(self) -> None:
        env = self.env
        downlinking = {tr.tile_id for tr in env.downlink_transfers}
        for tile_id in list(env._computed_waiting_downlink):
            tile = env.tiles.get(tile_id)
            if tile is None or tile.state != TileState.COMPUTED:
                env._computed_waiting_downlink.discard(tile_id)
                continue
            if tile.tile_id in downlinking:
                continue
            gs = env._select_ground_station(tile.location)
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
            sent = (gs.bandwidth_mbps / 8.0) * env.cfg.dt
            tr.remaining_mb -= sent
            if tr.remaining_mb <= 1e-6:
                tile.state = TileState.DONE
                tile.timestamps.end_downlink = env.time
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
