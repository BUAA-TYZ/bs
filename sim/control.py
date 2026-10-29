"""计算星状态通告：按实际字节共享 ISL，源星只接收控制信息。"""

from collections import Counter
from copy import deepcopy
from dataclasses import dataclass, replace
import json
import math

from sim.topology import link_key


@dataclass
class Advertisement:
    path: tuple
    payload: dict
    size_mb: float
    remaining_mb: float
    created_s: float
    hop: int = 0
    available_step: int = 0

    @property
    def src(self):
        return self.path[self.hop]

    @property
    def dst(self):
        return self.path[self.hop + 1]


class ControlPlane:
    def __init__(self, sources, period_s=5, ttl_s=15):
        self.sources = set(sources)
        self.period_s, self.ttl_s = period_s, ttl_s
        self.messages = []
        self.delivered = {sid: {} for sid in sources}
        self.stats = Counter()
        self.sent_mb_by_edge = Counter()
        self.last_sent_s = -math.inf

    def enqueue(self, state):
        now = state.time * state.config["dt"]
        if now - self.last_sent_s < self.period_s - 1e-9:
            return
        self.last_sent_s = now
        for source in sorted(self.sources):
            paths = {source: (source,)}
            frontier = [source]
            for _ in range(2):
                following = []
                for parent in frontier:
                    for node in state.neighbors[parent]:
                        if node in self.sources or node in paths:
                            continue
                        paths[node] = paths[parent] + (node,)
                        following.append(node)
                frontier = following
            for sender, outward in paths.items():
                if sender == source:
                    continue
                links = {key: deepcopy(row) for key, row in state.links.items()
                         if sender in tuple(map(int, key.split("-")))}
                payload = {"sender": sender, "generated_s": now, "hops": len(outward)-1,
                           "satellite": deepcopy(state.satellites[sender]),
                           "ground_options": deepcopy(state.ground_options[sender]), "links": links}
                # 实际 JSON 序列化长度取代设计预算；包含不可压缩的协议字段，不免费广播。
                size = len(json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()) / 1e6
                self.messages.append(Advertisement(tuple(reversed(outward)), payload, size, size, now,
                                                   available_step=state.time))
                self.stats["messages_sent"] += 1

    def active(self, step, dt):
        now = step * dt
        expired = [m for m in self.messages if now - m.created_s > self.ttl_s]
        self.stats["expired_in_transit"] += len(expired)
        self.messages = [m for m in self.messages if now - m.created_s <= self.ttl_s]
        return [m for m in self.messages if m.available_step <= step]

    def finish_step(self, step, dt):
        still = []
        for message in self.messages:
            if message.remaining_mb > 1e-12:
                still.append(message)
                continue
            message.hop += 1
            if message.hop == len(message.path)-1:
                self.delivered[message.path[-1]][message.path[0]] = {
                    **message.payload, "received_s": (step+1)*dt}
                self.stats["messages_delivered"] += 1
                self.stats["total_delivery_delay_s"] += (step+1)*dt - message.created_s
            else:
                message.remaining_mb = message.size_mb
                message.available_step = step + 1
                still.append(message)
        self.messages = still

    def view(self, state, source, hops):
        now = state.time * state.config["dt"]
        records = {sid: value for sid, value in self.delivered[source].items()
                   if value["hops"] <= hops and now-value["generated_s"] <= self.ttl_s
                   and value["received_s"] <= now}
        satellites = {source: deepcopy(state.satellites[source])}
        links = {}
        options = {source: deepcopy(state.ground_options[source])}
        for sid, record in records.items():
            age = now - record["generated_s"]
            row = deepcopy(record["satellite"])
            row["advertisement_age_s"] = age
            row["next_gs_windows"] = {gid: shift_window(window, age)
                                       for gid, window in row["next_gs_windows"].items()}
            satellites[sid] = row
            options[sid] = deepcopy(record["ground_options"]) if age == 0 else []
            for key, value in record["links"].items():
                value = deepcopy(value)
                value["contact_windows"] = [shift_window(w, age) for w in value["contact_windows"]
                                            if shift_window(w, age) is not None]
                links[key] = value
        known = set(satellites)
        for sid in known - {source}:
            key = link_key(source, sid)
            if key in state.links:
                links[key] = deepcopy(state.links[key])
        links = {key: row for key, row in links.items() if set(map(int, key.split("-"))) <= known}
        neighbors = {sid: [] for sid in known}
        for key, row in links.items():
            if row["up"]:
                a, b = map(int, key.split("-"))
                neighbors[a].append(b)
                neighbors[b].append(a)
        tiles = {tid: deepcopy(row) for tid, row in state.tiles.items() if row["source_sat_id"] == source}
        for row in satellites.values():
            row["isl_backlog_mb"] = {k: v for k, v in row.get("isl_backlog_mb", {}).items() if k in known}
        ground = {gid: {k: value for k, value in row.items()
                       if k in {"compute_rate", "bandwidth_mbps", "min_elevation_deg", "receive_channels"}}
                  for gid, row in state.ground_stations.items()}
        for row in ground.values():
            row.update(committed_compute_s=math.inf, queue_len=None, running=None,
                       visible_contenders=max(1, sum(bool(v) for v in options.values())))
        # 不知道地面队列，不能填真实值或假装空闲；原图地面报价标不可判定。
        config = {**deepcopy(state.config), "local_information": True, "ground_queue_unknown": True}
        pending = {task: {tid: deepcopy(row) for tid, row in values.items() if tid in tiles}
                   for task, values in state.pending_task_tiles.items() if any(tid in tiles for tid in values)}
        return replace(state, satellites=satellites, ground_stations=ground, ground_options=options,
                       neighbors=neighbors, links=links, tiles=tiles, pending_task_tiles=pending, config=config)


def shift_window(window, age):
    if window is None:
        return None
    start, duration = window
    end = start + duration - age
    return None if end <= 0 else (max(0, start-age), end-max(0, start-age))
