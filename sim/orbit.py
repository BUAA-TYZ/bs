from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

from skyfield.api import EarthSatellite, load, wgs84

_TS = load.timescale() if load is not None else None


def parse_start_time_utc(start_time_utc: str) -> datetime:
    # 允许 "YYYY-MM-DDTHH:MM:SSZ" 或 "YYYY-MM-DD HH:MM:SS" 格式
    if start_time_utc.endswith("Z"):
        return datetime.fromisoformat(start_time_utc.replace("Z", "+00:00"))
    dt = datetime.fromisoformat(start_time_utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_satellites(tle_lines: List[Tuple[str, str]]) -> List[EarthSatellite]:
    if load is None:
        raise ImportError(
            "skyfield is required for ephemeris mode. Install with: pip install skyfield"
        )
    sats: List[EarthSatellite] = []
    for line1, line2 in tle_lines:
        sats.append(EarthSatellite(line1.strip(), line2.strip()))
    return sats


def position_km(
    sat: EarthSatellite, t0: datetime, t_seconds: float
) -> Tuple[float, float, float]:
    # Skyfield 返回地心惯性坐标（km）
    if _TS is None:
        return (0.0, 0.0, 0.0)
    dt = t0 + timedelta(seconds=float(t_seconds))
    t = _TS.from_datetime(dt)
    r = sat.at(t).position.km
    if len(r) != 3:
        return (0.0, 0.0, 0.0)
    return (float(r[0]), float(r[1]), float(r[2]))


def link_geometry_km(
    sat_a: EarthSatellite, sat_b: EarthSatellite, t0: datetime, t_seconds: float
) -> Tuple[Tuple[float, float, float], Tuple[float, float, float], float, float]:
    """Return geocentric vectors, range (km), and central separation (deg)."""
    if _TS is None:
        return (0.0, 0.0, 0.0), (0.0, 0.0, 0.0), 0.0, 0.0
    dt = t0 + timedelta(seconds=float(t_seconds))
    t = _TS.from_datetime(dt)
    pa = sat_a.at(t)
    pb = sat_b.at(t)
    ra = pa.position.km
    rb = pb.position.km
    if len(ra) != 3 or len(rb) != 3:
        return (0.0, 0.0, 0.0), (0.0, 0.0, 0.0), 0.0, 0.0
    # Skyfield-native distance and angular separation.
    range_km = float((sat_b - sat_a).at(t).distance().km)
    separation_deg = float(pa.separation_from(pb).degrees)
    return (
        (float(ra[0]), float(ra[1]), float(ra[2])),
        (float(rb[0]), float(rb[1]), float(rb[2])),
        range_km,
        separation_deg,
    )


def build_ground_station(lat_deg: float, lon_deg: float, alt_m: float = 0.0):
    return wgs84.latlon(
        latitude_degrees=lat_deg, longitude_degrees=lon_deg, elevation_m=alt_m
    )


def sat_to_ground_geometry(
    sat: EarthSatellite,
    ground_station,
    t0: datetime,
    t_seconds: float,
) -> Tuple[float, float]:
    """Return (elevation_deg, distance_km) from ground station to satellite."""
    if _TS is None:
        return -90.0, 0.0
    dt = t0 + timedelta(seconds=float(t_seconds))
    t = _TS.from_datetime(dt)
    topocentric = (sat - ground_station).at(t)
    alt, _az, dist = topocentric.altaz()
    return float(alt.degrees), float(dist.km)


def next_gs_window(
    sat: EarthSatellite,
    ground_station,
    t0: datetime,
    t_now_s: float,
    min_elevation_deg: float,
    lookahead_s: float = 1800.0,
    scan_step_s: float = 30.0,
) -> Optional[Tuple[float, float]]:
    """预测卫星在未来 lookahead_s 秒内，下一次对地面站的过境窗口。

    返回 (starts_in_s, duration_s)：
      - starts_in_s: 距离现在多少秒后窗口开始（若当前已在窗口内则为 0）
      - duration_s:  窗口持续时长（秒）
    若 lookahead_s 内没有过境，返回 None。

    算法：以 scan_step_s 为步长滚动扫描，找到第一个仰角 >= min_elevation_deg
    的时刻（窗口开始），再向后扫找到仰角再次低于阈值的时刻（窗口结束）。
    """
    if _TS is None:
        return None

    # --- 向量化批量采样，避免逐点调用 ---
    steps = int(lookahead_s / scan_step_s) + 1
    t_offsets = [t_now_s + i * scan_step_s for i in range(steps)]
    dts = [t0 + timedelta(seconds=t) for t in t_offsets]
    t_arr = _TS.from_datetimes(dts)
    topos = (sat - ground_station).at(t_arr)
    alts_deg = topos.altaz()[0].degrees  # shape (steps,)

    window_start_s: Optional[float] = None

    for i, (t_s, alt) in enumerate(zip(t_offsets, alts_deg)):
        visible = alt >= min_elevation_deg
        if window_start_s is None:
            if visible:
                # 若第一个点就可见，窗口开始时间设为 0（当前已在窗口内）
                window_start_s = t_s
        else:
            if not visible:
                # 窗口结束
                duration_s = t_s - window_start_s
                starts_in_s = max(0.0, window_start_s - t_now_s)
                return (starts_in_s, duration_s)

    # lookahead 结束时仍在窗口内
    if window_start_s is not None:
        duration_s = (t_now_s + lookahead_s) - window_start_s
        starts_in_s = max(0.0, window_start_s - t_now_s)
        return (starts_in_s, duration_s)

    return None


def next_gs_windows_for_sat(
    sat: EarthSatellite,
    ground_stations: List[Tuple[str, object, float]],
    t0: datetime,
    t_now_s: float,
    lookahead_s: float = 1800.0,
    scan_step_s: float = 30.0,
) -> dict:
    """批量计算一颗卫星对多个地面站的下一次过境窗口。

    ground_stations: List of (gs_id, ground_station_obj, min_elevation_deg)
    返回: {gs_id: (starts_in_s, duration_s) or None}
    """
    result = {}
    for gs_id, gs_obj, min_elev in ground_stations:
        result[gs_id] = next_gs_window(
            sat=sat,
            ground_station=gs_obj,
            t0=t0,
            t_now_s=t_now_s,
            min_elevation_deg=min_elev,
            lookahead_s=lookahead_s,
            scan_step_s=scan_step_s,
        )
    return result
