"""只读运行观测：采样快照、逐流传输量和进度，不修改仿真状态。

main.run_config 在每步 env.step 之后调用 on_step。第 k 步快照的含义：
- links / gs_visible：第 k 步使用的星间链路与星地几何可见性（时刻 k*dt）；
- sats / ground：第 k 步结束时（时刻 (k+1)*dt）的队列、计算和存储状态；
- isl_sent / gs_sent / flows：快照区间 [span[0], span[1]] 内实际发送的数据量，
  来自 Metrics.network_sent_mb 的差分与传输对象剩余量的差分。
"""

from __future__ import annotations

import math
import time
from typing import Callable, Dict, List, Optional

# 传输开始/结束事件 → (流类型, 是否开始)
_FLOW_EVENTS = {
    "tx_start": ("isl_raw", True), "tx_end": ("isl_raw", False),
    "result_tx_start": ("isl_result", True), "result_tx_end": ("isl_result", False),
    "downlink_start": ("downlink", True), "downlink_end": ("downlink", False),
    "tx_start_ground": ("ground_tile", True), "tx_end_ground": ("ground_tile", False),
    "ground_upload_start": ("ground_task", True), "ground_upload_end": ("ground_task", False),
}
SNAPSHOT_FORMAT = "workbench_snapshots_v1"


class RunCancelled(Exception):
    pass


def _r(value: Optional[float], digits: int = 4) -> Optional[float]:
    return None if value is None else round(float(value), digits)


class SnapshotObserver:
    def __init__(self, max_snapshots: int = 1500, queue_head: int = 4,
                 progress: Optional[Callable[[int, Dict], None]] = None,
                 progress_interval_s: float = 0.3,
                 cancelled: Optional[Callable[[], bool]] = None,
                 max_flow_rows: int = 200) -> None:
        self.max_snapshots = max(1, int(max_snapshots))
        self.max_flow_rows = max(0, int(max_flow_rows))
        self.queue_head = queue_head
        self.progress = progress
        self.progress_interval_s = progress_interval_s
        self.cancelled = cancelled or (lambda: False)
        self.snapshots: List[Dict] = []
        self.meta: Dict = {}
        self.last_step = -1
        self.events_seen = 0

    # ------------------------------------------------------------------ #
    def on_start(self, env) -> None:
        if self.cancelled():
            raise RunCancelled()
        cfg = env.cfg
        self.env = env
        self.dt = cfg.dt
        self.sim_steps = cfg.sim_steps
        self.stride = max(1, math.ceil(cfg.sim_steps / self.max_snapshots))
        self.gs_ids = list(env.ground_stations)
        self._gs_index = {gs: i for i, gs in enumerate(self.gs_ids)}
        self._prev_sent = dict(env.metrics.network_sent_mb)
        self._live: Dict[int, list] = {}      # id(transfer) → [obj, kind, ident, src, dst, remaining]
        self._step_events: List[Dict] = []
        self._failed_tasks: set[str] = set()
        self._span_start = 0
        self._reset_span()
        self._last_progress = 0.0
        sources = set(env.task_sources)
        self.meta = {
            "format": SNAPSHOT_FORMAT,
            "dt": cfg.dt, "sim_steps": cfg.sim_steps, "stride": self.stride,
            "start_time_utc": str(cfg.topology.get("start_time_utc", "2025-01-01T00:00:00Z")),
            "num_sats": cfg.num_sats, "gs_ids": self.gs_ids,
            "task_sources": sorted(sources),
            "satellites": [{
                "sat_id": sid, "task_source": sid in sources,
                "compute_rate": sat.compute_rate,
                "mem_capacity_gb": sat.mem_capacity_gb, "vram_capacity_gb": sat.vram_capacity_gb,
                "onboard_compute_profile": env.cfg.onboard_compute_profile,
                "shared_memory_gb": env.cfg.shared_memory_gb,
            } for sid, sat in env.satellites.items()],
            "ground_stations": [{
                "gs_id": gs.gs_id, "bandwidth_mbps": gs.bandwidth_mbps,
                "receive_channels": gs.receive_channels, "compute_rate": gs.compute_rate,
                "min_elevation_deg": gs.min_elevation_deg,
            } for gs in env.ground_stations.values()],
            "fields": {
                "sats": ["queue_len", "executing_tile", "executing_remaining_s", "mem_used_gb", "vram_used_gb",
                         "ground_upload_waiting", "executing_tiles"],
                "links": ["i", "j", "bandwidth_mbps"],
                "isl_sent": ["src", "dst", "sent_mb"],
                "gs_visible": ["sat_id", "gs_index"],
                "flows": ["kind", "id", "src", "dst", "sent_mb", "remaining_mb", "status"],
                "flow_groups": ["kind", "src", "dst", "flows", "moving", "sent_mb", "done", "aborted"],
                "ground": ["tile_queue_len", "tile_running", "task_queue_len", "task_running", "task_remaining_s"],
            },
        }
        logger = env.lifecycle_logger
        if logger.enabled:
            original = logger.log

            def tee(record, _original=original):
                _original(record)
                self._on_event(record)

            logger.log = tee
        self.meta["events_captured"] = logger.enabled

    def _reset_span(self) -> None:
        self._span_flows: Dict[tuple, Dict] = {}
        self._span_actions = {"LOCAL": 0, "OFFLOAD_SAT": 0, "OFFLOAD_GS": 0, "WAIT": 0}
        self._span_event_counts: Dict[str, int] = {}

    def _on_event(self, record: Dict) -> None:
        self.events_seen += 1
        name = record.get("event")
        self._span_event_counts[name] = self._span_event_counts.get(name, 0) + 1
        if name == "failed" or name == "task_failed":
            self._failed_tasks.add(record.get("task_id"))
        if name in _FLOW_EVENTS:
            self._step_events.append(record)

    # ------------------------------------------------------------------ #
    def _initial_size(self, kind: str, ident: str) -> float:
        env = self.env
        if kind in ("isl_raw", "ground_tile"):
            return env.tiles[ident].data_size_mb
        if kind in ("isl_result", "downlink"):
            return env.cfg.result_size_mb
        return env.tasks[ident].image_size_mb

    def _current_transfers(self):
        env = self.env
        for tr in env.transfers:
            yield tr, ("isl_result" if tr.is_result else "isl_raw"), tr.tile_id, tr.src, tr.dst
        for tr in env.downlink_transfers:
            yield tr, "downlink", tr.tile_id, tr.src_sat, tr.gs_id
        for tr in env.ground_tile_transfers:
            yield tr, "ground_tile", tr.tile_id, tr.src_sat, tr.gs_id
        for tr in env.ground_task_transfers:
            yield tr, "ground_task", tr.task_id, tr.src_sat, tr.gs_id

    def _flow(self, kind, ident, src, dst) -> Dict:
        key = (kind, ident, src, dst)
        flow = self._span_flows.get(key)
        if flow is None:
            flow = self._span_flows[key] = {"sent": 0.0, "remaining": None, "status": "active"}
        return flow

    def _track_flows(self) -> None:
        starts, ended = [], set()
        for event in self._step_events:
            kind, is_start = _FLOW_EVENTS[event["event"]]
            ident = event["task_id"] if kind == "ground_task" else event["tile_id"]
            key = (kind, ident, event["sat_from"], event["sat_to"])
            (starts.append(key) if is_start else ended.add(key))
        self._step_events = []
        seen = set()
        current = set()
        for tr, kind, ident, src, dst in self._current_transfers():
            key = (kind, ident, src, dst)
            current.add(id(tr))
            seen.add(key)
            known = self._live.get(id(tr))
            before = known[1] if known is not None else self._initial_size(kind, ident)
            flow = self._flow(*key)
            flow["sent"] += max(0.0, before - tr.remaining_mb)
            flow["remaining"] = tr.remaining_mb
            flow["status"] = "active"
            self._live[id(tr)] = [tr, tr.remaining_mb, key]
        for oid in [oid for oid in self._live if oid not in current]:
            tr, before, key = self._live.pop(oid)
            seen.add(key)
            flow = self._flow(*key)
            flow["sent"] += max(0.0, before - tr.remaining_mb)
            flow["remaining"] = None
            flow["status"] = "done" if key in ended else "aborted"
        # 同一步内开始并结束（或因失败撤销）的传输不会留在传输列表中，由生命周期事件补齐；
        # 撤销时本步已发送量无法从对象恢复，记为未知（None）。
        for key in starts:
            if key in seen:
                continue
            flow = self._flow(*key)
            if key in ended:
                flow["sent"] += self._initial_size(key[0], key[1])
            else:
                flow["sent_unknown"] = True
            flow["remaining"] = None
            flow["status"] = "done" if key in ended else "aborted"

    def _count_actions(self, actions) -> None:
        for action in actions or []:
            kind = getattr(action.action_type, "value", str(action.action_type))
            if kind == "OFFLOAD":
                kind = "OFFLOAD_GS" if action.target_gs_id is not None else "OFFLOAD_SAT"
            if kind in self._span_actions:
                self._span_actions[kind] += 1

    # ------------------------------------------------------------------ #
    def on_step(self, env, step_idx: int, actions) -> None:
        self.last_step = step_idx
        self._count_actions(actions)
        self._track_flows()
        if (step_idx + 1) % self.stride == 0 or step_idx == self.sim_steps - 1:
            self.snapshots.append(self._snapshot(step_idx))
            self._span_start = step_idx + 1
            self._reset_span()
        now = time.monotonic()
        if self.progress is not None and (now - self._last_progress >= self.progress_interval_s
                                          or step_idx == self.sim_steps - 1):
            self._last_progress = now
            self.progress(step_idx + 1, self.counters())
        if self.cancelled():
            raise RunCancelled()

    def counters(self) -> Dict:
        env = self.env
        m = env.metrics
        return {
            "total_tasks": m.total_tasks, "completed_tasks": m.completed_tasks,
            "failed_tasks": len(self._failed_tasks), "aggregating_tasks": len(env._pending_aggregation),
            "total_tiles": m.total_tiles, "completed_tiles": m.completed_tiles, "failed_tiles": m.failed_tiles,
        }

    def _snapshot(self, step_idx: int) -> Dict:
        env = self.env
        sats, queues = [], {}
        for sid, sat in env.satellites.items():
            exec_rem = None
            if sat.executing is not None:
                tile = env.tiles.get(sat.executing)
                exec_rem = _r(tile.remaining_compute) if tile is not None else None
            sats.append([len(sat.queue), sat.executing, exec_rem,
                         _r(sat.mem_used_gb), _r(sat.vram_used_gb),
                         len(env.ground_task_queued.get(sid, [])),
                         list(sat.executing_batch) if sat.executing_batch else
                         ([sat.executing] if sat.executing is not None else [])])
            if sat.queue:
                queues[str(sid)] = sat.queue[:self.queue_head]
        links = [[lk.i, lk.j, _r(lk.bandwidth_mbps, 2)]
                 for lk in env._links_cache.values() if lk.up]
        sent_now = env.metrics.network_sent_mb
        isl_sent, gs_sent = [], {}
        for key, total in sent_now.items():
            delta = total - self._prev_sent.get(key, 0.0)
            if delta <= 1e-9:
                continue
            kind, _, rest = key.partition(":")
            if kind == "isl":
                src, dst = rest.split("->")
                isl_sent.append([int(src), int(dst), _r(delta)])
            elif kind == "ground":
                gs_sent[rest] = _r(delta)
        self._prev_sent = dict(sent_now)
        gs_visible = [[sid, self._gs_index[gs]]
                      for sid, visible in env.vis_cache.visible_gs_by_sat.items()
                      for gs in visible if gs in self._gs_index]
        flows, groups = [], {}
        for (kind, ident, src, dst), f in self._span_flows.items():
            sent = None if f.get("sent_unknown") else f["sent"]
            moved = sent is None or sent > 1e-9
            group = groups.setdefault((kind, src, dst), [kind, src, dst, 0, 0, 0.0, 0, 0])
            group[3] += 1
            group[4] += moved
            group[5] += sent or 0.0
            group[6] += f["status"] == "done"
            group[7] += f["status"] == "aborted"
            flows.append([kind, ident, src, dst, _r(sent), _r(f["remaining"]), f["status"]])
        # 大规模场景同一链路上可有上千个并发分片；逐分片明细按 结束 > 有发送 > 等待、发送量降序截断，
        # 按 (类型, 源, 目的) 的汇总始终完整。
        omitted = max(0, len(flows) - self.max_flow_rows)
        if omitted:
            flows.sort(key=lambda r: (r[6] == "active", r[4] is not None and r[4] <= 1e-9, -(r[4] or 0.0)))
            del flows[self.max_flow_rows:]
        flow_groups = [g[:5] + [_r(g[5])] + g[6:] for g in groups.values()]
        ground = {}
        for gs_id in self.gs_ids:
            comp = env.ground_task_running.get(gs_id)
            ground[gs_id] = [len(env.ground_tile_queues.get(gs_id, [])),
                             env.ground_tile_running.get(gs_id),
                             len(env.ground_compute_queue.get(gs_id, [])),
                             comp.task_id if comp is not None else None,
                             _r(comp.remaining_compute) if comp is not None else None]
        return {
            "step": step_idx, "span": [self._span_start, step_idx],
            "counters": self.counters(), "sats": sats, "queues": queues,
            "links": links, "isl_sent": isl_sent, "gs_visible": gs_visible, "gs_sent": gs_sent,
            "flows": flows, "flows_omitted": omitted, "flow_groups": flow_groups,
            "ground": ground, "actions": dict(self._span_actions),
            "events": dict(self._span_event_counts),
        }

    def export(self, complete: bool) -> Dict:
        return {**self.meta, "complete": complete, "last_step": self.last_step,
                "snapshots": self.snapshots}

    TILE_FIELDS = ["tile_id", "task_id", "state", "location", "failure_reason", "data_size_mb",
                   "created", "start_tx", "end_tx", "start_compute", "end_compute",
                   "start_downlink", "end_downlink", "result_available_time",
                   "raw_relay_path", "result_path"]

    def tile_table(self) -> Dict:
        """运行结束（或中止）时各分片的最终状态；时间字段为步号。"""
        rows = []
        for tile in self.env.tiles.values():
            ts = tile.timestamps
            rows.append([tile.tile_id, tile.parent_task_id, tile.state.value, tile.location,
                         tile.failure_reason.value if tile.failure_reason is not None else None,
                         _r(tile.data_size_mb), ts.created, ts.start_tx, ts.end_tx, ts.start_compute,
                         ts.end_compute, ts.start_downlink, ts.end_downlink, tile.result_available_time,
                         list(tile.raw_relay_path), list(tile.result_path)])
        return {"time_unit": "step", "dt": self.dt, "fields": self.TILE_FIELDS, "rows": rows}
