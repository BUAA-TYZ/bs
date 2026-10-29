"""地面站可见性缓存与过境窗口预测缓存。

将原 env.py 中的 _refresh_ground_cache / _refresh_window_cache /
_is_ground_visible 抽离到此模块，减少 SimulationEnv 主体代码量。
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import math
import numpy as np

from sim.orbit import next_gs_windows_for_sat, sat_to_ground_geometry, time_grid


class VisibilityCache:
    """维护每个仿真步的地面可见性快照与过境窗口预测。

    Parameters
    ----------
    num_sats:
        卫星数量。
    ground_visibility_update_steps:
        每隔多少步刷新一次缓存（对应 config.ground_visibility_update_steps）。
    gs_window_lookahead_s:
        过境窗口预测时间范围（秒）。
    gs_window_scan_step_s:
        过境窗口扫描步长（秒），默认 30 s。
    """

    def __init__(
        self,
        num_sats: int,
        ground_visibility_update_steps: int,
        gs_window_lookahead_s: float,
        gs_window_scan_step_s: float = 30.0,
        dt: float = 1.0,
    ) -> None:
        self.num_sats = num_sats
        self.update_steps = max(1, ground_visibility_update_steps)
        self.lookahead_s = gs_window_lookahead_s
        self.scan_step_s = gs_window_scan_step_s
        self.dt = dt

        # 可见性快照
        self._cache_time: Optional[int] = None
        self._geom_cache: Dict[Tuple[int, str], Tuple[float, float]] = {}
        self.visible_gs_by_sat: Dict[int, List[str]] = {}
        self.best_gs_by_sat: Dict[int, Optional[str]] = {}

        # 过境窗口预测
        self._window_cache_time: Optional[int] = None
        # {sat_id: {gs_id: (starts_in_s, duration_s) or None}}
        self.window_cache: Dict[int, Dict[str, Optional[tuple]]] = {}
        self._precomputed = None
        self._precomputed_index = None

    def precompute(self, sat_recs, t0, ground_station_objs, sim_steps, chunk_size=20000):
        """只预计算刷新与窗口扫描会读取的步，并分块传播，避免为每个 dt 步建整张轨道网格。"""
        stride = self.scan_step_s / self.dt
        if not stride.is_integer():
            return  # 非整步扫描继续使用原实现，避免改变采样时刻。
        self._scan_stride = int(stride)
        samples = int(self.lookahead_s / self.scan_step_s) + 1
        count = sim_steps + math.ceil(self.lookahead_s / self.dt) + 1
        # refresh 只在 update_steps 的整数倍读取；窗口扫描从这些刷新步按 scan 步长向后读。
        refresh = np.arange(0, sim_steps, self.update_steps, dtype=np.int64)
        offsets = np.arange(samples, dtype=np.int64) * self._scan_stride
        if self._scan_stride % self.update_steps == 0:
            # 刷新步与扫描步同余类有限，按余数枚举即可得到全部被读步。
            residues = np.unique(refresh % self._scan_stride)
            steps = np.unique(np.concatenate([np.arange(r, count, self._scan_stride, dtype=np.int64)
                                              for r in residues]))
        else:
            steps = np.unique((refresh[:, None] + offsets[None, :]).ravel())
        steps = steps[steps < count]
        index = np.full(count, -1, dtype=np.int64)
        index[steps] = np.arange(len(steps), dtype=np.int64)
        self._precomputed = {}
        for sid, sat in enumerate(sat_recs[:self.num_sats]):
            for gs_id, station in ground_station_objs.items():
                altitude = np.empty(len(steps))
                distance = np.empty(len(steps))
                for begin in range(0, len(steps), chunk_size):
                    part = steps[begin:begin + chunk_size]
                    times = time_grid(t0, part * self.dt)
                    alt, _, dist = (sat - station).at(times).altaz()
                    altitude[begin:begin + len(part)] = alt.degrees
                    distance[begin:begin + len(part)] = dist.km
                self._precomputed[sid, gs_id] = (altitude, distance)
        self._precomputed_index = index

    def _sampled_window(self, sid, gs_id, t, min_elev):
        samples = int(self.lookahead_s / self.scan_step_s) + 1
        rows = self._precomputed_index[t:t + samples * self._scan_stride:self._scan_stride]
        if (rows < 0).any():
            raise RuntimeError("Visibility precompute is missing a scanned step")
        altitude = self._precomputed[sid, gs_id][0][rows]
        visible = altitude >= min_elev
        starts = np.flatnonzero(visible)
        if not len(starts):
            return None
        start = int(starts[0])
        ends = np.flatnonzero(~visible[start + 1:])
        end_s = (start + 1 + int(ends[0])) * self.scan_step_s if len(ends) else self.lookahead_s
        return (start * self.scan_step_s, end_s - start * self.scan_step_s)

    def refresh(
        self,
        t: int,
        sat_recs,
        t0,
        ground_station_objs: Dict[str, object],
        ground_stations,
    ) -> None:
        """刷新可见性缓存与过境窗口缓存（按 update_steps 节流）。

        Parameters
        ----------
        t:
            当前仿真时间步。
        sat_recs:
            topology.sat_recs，卫星记录列表。
        t0:
            topology.t0，仿真起始历元。
        ground_station_objs:
            {gs_id: skyfield_ground_station} 字典。
        ground_stations:
            {gs_id: GroundStation} 字典。
        """
        sampled_t = (t // self.update_steps) * self.update_steps

        # 可见性缓存
        if self._cache_time != sampled_t:
            self._cache_time = sampled_t
            self._geom_cache = {}
            self.visible_gs_by_sat = {i: [] for i in range(self.num_sats)}
            self.best_gs_by_sat = {}

            for sat_id in range(self.num_sats):
                best_gs_id: Optional[str] = None
                best_bw = -1.0
                sat = sat_recs[sat_id]
                for gs_id, gs in ground_stations.items():
                    if self._precomputed is not None:
                        altitude, distance = self._precomputed[sat_id, gs_id]
                        row = int(self._precomputed_index[sampled_t])
                        if row < 0:
                            raise RuntimeError("Visibility precompute is missing a refresh step")
                        elev_deg, dist_km = float(altitude[row]), float(distance[row])
                    else:
                        elev_deg, dist_km = sat_to_ground_geometry(
                            sat=sat,
                            ground_station=ground_station_objs[gs_id],
                            t0=t0,
                            t_seconds=sampled_t * self.dt,
                        )
                    self._geom_cache[(sat_id, gs_id)] = (elev_deg, dist_km)
                    if elev_deg < gs.min_elevation_deg:
                        continue
                    self.visible_gs_by_sat[sat_id].append(gs_id)
                    if gs.bandwidth_mbps > best_bw:
                        best_bw = gs.bandwidth_mbps
                        best_gs_id = gs_id
                self.best_gs_by_sat[sat_id] = best_gs_id

        # 过境窗口缓存（与可见性共用 sampled_t）
        if self._window_cache_time != sampled_t:
            self._window_cache_time = sampled_t
            gs_list = [
                (gs_id, ground_station_objs[gs_id], gs.min_elevation_deg)
                for gs_id, gs in ground_stations.items()
            ]
            self.window_cache = {}
            for sat_id in range(self.num_sats):
                if self._precomputed is not None:
                    self.window_cache[sat_id] = {gs_id: self._sampled_window(sat_id, gs_id, sampled_t, gs.min_elevation_deg)
                                                  for gs_id, gs in ground_stations.items()}
                else:
                    self.window_cache[sat_id] = next_gs_windows_for_sat(
                        sat=sat_recs[sat_id],
                        ground_stations=gs_list,
                        t0=t0,
                        t_now_s=sampled_t * self.dt,
                        lookahead_s=self.lookahead_s,
                        scan_step_s=self.scan_step_s,
                    )

    def windows_at(self, sat_id: int, t: int) -> Dict[str, Optional[tuple]]:
        elapsed = max(0.0, (t - (self._window_cache_time or 0)) * self.dt)
        result = {}
        for gs_id, window in self.window_cache.get(sat_id, {}).items():
            if window is None or window[0] + window[1] <= elapsed:
                result[gs_id] = None
            else:
                start, duration = window
                result[gs_id] = (max(0.0, start - elapsed),
                                 duration - max(0.0, elapsed - start))
        return result

    def is_visible(self, sat_id: int, gs_id: str, ground_stations) -> bool:
        """判断卫星 sat_id 当前是否对地面站 gs_id 可见。"""
        geom = self._geom_cache.get((sat_id, gs_id))
        if geom is None:
            return False
        gs = ground_stations.get(gs_id)
        if gs is None:
            return False
        elev_deg, _ = geom
        return elev_deg >= gs.min_elevation_deg
