"""地面站可见性缓存与过境窗口预测缓存。

将原 env.py 中的 _refresh_ground_cache / _refresh_window_cache /
_is_ground_visible 抽离到此模块，减少 SimulationEnv 主体代码量。
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from sim.orbit import next_gs_windows_for_sat, sat_to_ground_geometry


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
    ) -> None:
        self.num_sats = num_sats
        self.update_steps = max(1, ground_visibility_update_steps)
        self.lookahead_s = gs_window_lookahead_s
        self.scan_step_s = gs_window_scan_step_s

        # 可见性快照
        self._cache_time: Optional[int] = None
        self._geom_cache: Dict[Tuple[int, str], Tuple[float, float]] = {}
        self.visible_gs_by_sat: Dict[int, List[str]] = {}
        self.best_gs_by_sat: Dict[int, Optional[str]] = {}

        # 过境窗口预测
        self._window_cache_time: Optional[int] = None
        # {sat_id: {gs_id: (starts_in_s, duration_s) or None}}
        self.window_cache: Dict[int, Dict[str, Optional[tuple]]] = {}

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
                    elev_deg, dist_km = sat_to_ground_geometry(
                        sat=sat,
                        ground_station=ground_station_objs[gs_id],
                        t0=t0,
                        t_seconds=sampled_t,
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
                self.window_cache[sat_id] = next_gs_windows_for_sat(
                    sat=sat_recs[sat_id],
                    ground_stations=gs_list,
                    t0=t0,
                    t_now_s=float(sampled_t),
                    lookahead_s=self.lookahead_s,
                    scan_step_s=self.scan_step_s,
                )

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
