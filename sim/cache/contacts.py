"""仅由轨道和终端匹配规则预测接触窗口，不调用带噪声的链路快照。"""

import numpy as np

from sim.orbit import positions_km_at_times
from sim.topology import _visible_from_positions, bounded_edges, link_key


def reconcile_windows(windows, up, scan_s):
    """当前快照否定立即接触时，保留下一扫描周期后的预测机会。

粗采样窗口可能漏掉短时断链；当前down只能否定立即发送，不能否定整段未来。
这里仍是预测，执行时须再次检查真实链路，不能据此提前建立连接。
"""
    result = []
    for start, duration in windows:
        if start == 0 and not up:
            if duration <= scan_s:
                continue
            start, duration = scan_s, duration - scan_s
        result.append((start, duration))
    return result


class ContactCache:
    def __init__(self, topology, horizon_s=1800.0, scan_s=30.0, update_s=60.0):
        if min(horizon_s, scan_s, update_s) <= 0:
            raise ValueError("Contact horizon, scan and update must be positive")
        self.topology = topology
        self.horizon_s, self.scan_s, self.update_s = horizon_s, scan_s, update_s
        self.updated = None
        self.intervals = {}
        self._geometry = {}

    def windows_at(self, now_s):
        if self.updated is None or now_s - self.updated >= self.update_s:
            self._refresh(now_s)
        return {key: [(max(0.0, start - now_s), end - max(start, now_s))
                      for start, end in intervals if end > now_s][:2]
                for key, intervals in self.intervals.items()}

    def _refresh(self, now_s):
        topo = self.topology
        offsets = np.arange(0, self.horizon_s + self.scan_s, self.scan_s)
        offsets = np.minimum(offsets, self.horizon_s)
        # 相邻滚动预测的大部分采样点重叠，只传播新增时刻。
        sample_times = now_s + offsets
        missing = np.array([t for t in sample_times if t not in self._geometry])
        positions = [positions_km_at_times(sat, topo.t0, missing)
                     for sat in topo.sat_recs[:topo.cfg.num_sats]]
        pairs = [(i, j) for i in range(len(positions)) for j in range(i + 1, len(positions))]
        flags = {link_key(i, j): [] for i, j in pairs}
        for k, time_s in enumerate(missing):
            geometry = [_visible_from_positions(tuple(positions[i][k]), tuple(positions[j][k]),
                        topo.cfg.earth_radius_km, topo.cfg.min_elevation_deg, topo.cfg.max_range_km)
                        for i, j in pairs]
            allowed = bounded_edges(pairs, geometry, topo.cfg.max_neighbors,
                                    sources=getattr(topo.cfg, "source_sat_ids", ()),
                                    limits=getattr(topo.cfg, "neighbor_limits", ()),
                                    preferred=getattr(topo.cfg, "preferred_edges", ()))
            self._geometry[time_s] = allowed
        for time_s in sample_times:
            allowed = self._geometry[time_s]
            for i, j in pairs:
                flags[link_key(i, j)].append((i, j) in allowed)
        self.intervals = {}
        for key, values in flags.items():
            intervals, start = [], None
            for offset, up in zip(offsets, values):
                if up and start is None:
                    start = now_s + float(offset)
                if not up and start is not None:
                    intervals.append((start, now_s + float(offset)))
                    start = None
            if start is not None and start < now_s + self.horizon_s:
                intervals.append((start, now_s + self.horizon_s))
            delay = getattr(topo.cfg, "acquisition_delay_s", 0.0)
            pair = tuple(int(value) for value in key.split("-"))
            since = getattr(topo, "_selected_since", {}).get(pair)
            self.intervals[key] = [(max(start, (since if start == now_s and since is not None else start) + delay), end)
                                   for start, end in intervals
                                   if max(start, (since if start == now_s and since is not None else start) + delay) < end]
        self.updated = now_s
        self._geometry = {t: edges for t, edges in self._geometry.items() if t >= now_s}
