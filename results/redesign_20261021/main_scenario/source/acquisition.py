"""由区域过境与光照生成确定性的成像事件，不访问网络或未来任务状态。"""

from __future__ import annotations

import calendar
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import math
from typing import Mapping, Sequence

import numpy as np
from skyfield.api import wgs84

from sim.orbit import load_satellites, parse_start_time_utc, time_grid


SOLAR_SOURCE = "https://gml.noaa.gov/grad/solcalc/solareqns.PDF"
EARTH_MEAN_RADIUS_KM = 6371.0088


@dataclass(frozen=True)
class ImagingRegion:
    target_id: str
    lat_deg: float
    lon_deg: float
    radius_km: float = 0.0

    def __post_init__(self):
        values = (self.lat_deg, self.lon_deg, self.radius_km)
        if not self.target_id or not all(math.isfinite(v) for v in values):
            raise ValueError("Region needs an ID and finite coordinates/radius")
        if not -90 <= self.lat_deg <= 90 or not -180 <= self.lon_deg <= 180:
            raise ValueError("Region coordinates outside latitude/longitude bounds")
        if not 0 <= self.radius_km <= 1000:
            raise ValueError("Imaging region radius must be within 0..1000 km")


# 民用农业、植被区域；这些圆形研究区域不代表真实卫星的任务订单。
US_CIVIL_REGIONS = (
    ImagingRegion("central_valley_agriculture", 36.5, -119.5, 125),
    ImagingRegion("great_plains_agriculture", 39.0, -100.0, 400),
    ImagingRegion("corn_belt_agriculture", 42.0, -93.5, 300),
    ImagingRegion("mississippi_delta_agriculture", 33.0, -91.0, 250),
    ImagingRegion("appalachian_vegetation", 38.0, -81.0, 250),
)


def solar_elevation_deg(
    datetimes: Sequence[datetime], lat_deg, lon_deg
) -> np.ndarray:
    """NOAA 简式太阳几何高度，不含大气折射。

    采用公开的 fractional-year/declination/equation-of-time 公式。
    该简式没有此处可保证的逐点误差上界，不能沿用 NOAA 完整 Meeus
    计算器的精度承诺。调度器使用额外角度裕度，并记录这个近似边界。
    """
    if any(t.tzinfo is None for t in datetimes):
        raise ValueError("Solar-position times must include a timezone")
    utc = [t.astimezone(timezone.utc) for t in datetimes]
    hours = np.asarray([
        t.hour + t.minute / 60 + (t.second + t.microsecond / 1e6) / 3600
        for t in utc
    ])
    day = np.asarray([t.timetuple().tm_yday for t in utc])
    year_days = np.asarray([366 if calendar.isleap(t.year) else 365 for t in utc])
    gamma = 2 * np.pi / year_days * (day - 1 + (hours - 12) / 24)
    equation_minutes = 229.18 * (
        0.000075 + 0.001868 * np.cos(gamma) - 0.032077 * np.sin(gamma)
        - 0.014615 * np.cos(2 * gamma) - 0.040849 * np.sin(2 * gamma)
    )
    declination = (
        0.006918 - 0.399912 * np.cos(gamma) + 0.070257 * np.sin(gamma)
        - 0.006758 * np.cos(2 * gamma) + 0.000907 * np.sin(2 * gamma)
        - 0.002697 * np.cos(3 * gamma) + 0.00148 * np.sin(3 * gamma)
    )
    hour_angle = np.deg2rad((hours * 60 + equation_minutes + 4 * np.asarray(lon_deg)) / 4 - 180)
    latitude = np.deg2rad(lat_deg)
    sine_elevation = (
        np.sin(latitude) * np.sin(declination)
        + np.cos(latitude) * np.cos(declination) * np.cos(hour_angle)
    )
    return np.rad2deg(np.arcsin(np.clip(sine_elevation, -1, 1)))


def _closest_region_point(region, sub_lat_deg, sub_lon_deg):
    """用球面大圆选择圆内最近地表点，后续视线计算使用 WGS84 椭球。"""
    lat1, lon1 = np.deg2rad([region.lat_deg, region.lon_deg])
    lat2, lon2 = np.deg2rad(sub_lat_deg), np.deg2rad(sub_lon_deg)
    delta_lon = lon2 - lon1
    central = np.arccos(np.clip(
        np.sin(lat1) * np.sin(lat2) + np.cos(lat1) * np.cos(lat2) * np.cos(delta_lon), -1, 1
    ))
    bearing = np.arctan2(
        np.sin(delta_lon) * np.cos(lat2),
        np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(delta_lon),
    )
    distance = np.minimum(central, region.radius_km / EARTH_MEAN_RADIUS_KM)
    latitude = np.arcsin(np.clip(
        np.sin(lat1) * np.cos(distance) + np.cos(lat1) * np.sin(distance) * np.cos(bearing), -1, 1
    ))
    longitude = lon1 + np.arctan2(
        np.sin(bearing) * np.sin(distance) * np.cos(lat1),
        np.cos(distance) - np.sin(lat1) * np.sin(latitude),
    )
    return np.rad2deg(latitude), (np.rad2deg(longitude) + 180) % 360 - 180


def _sample_geometry(satellite, times, datetimes, region):
    state = satellite.at(times)
    satellite_position = state.position.km.T
    if not np.all(np.isfinite(satellite_position)):
        raise ValueError("Orbit propagation returned non-finite positions")
    subpoint = wgs84.subpoint(state)
    latitudes, longitudes = _closest_region_point(
        region, subpoint.latitude.degrees, subpoint.longitude.degrees
    )
    ground = wgs84.latlon(latitudes, longitudes)
    toward_ground = ground.at(times).position.km.T - satellite_position
    ranges = np.linalg.norm(toward_ground, axis=1)
    # 离轴角是在卫星处相对地心天底方向定义，不是地面仰角的余角。
    cosine = np.sum(toward_ground * -satellite_position, axis=1) / (
        ranges * np.linalg.norm(satellite_position, axis=1)
    )
    off_nadir = np.rad2deg(np.arccos(np.clip(cosine, -1, 1)))
    elevation = (satellite - ground).at(times).altaz()[0].degrees
    return {
        "target_lat_deg": latitudes,
        "target_lon_deg": longitudes,
        "off_nadir_deg": off_nadir,
        "sat_elevation_deg": elevation,
        "slant_range_km": ranges,
        "sun_elevation_deg": solar_elevation_deg(datetimes, latitudes, longitudes),
    }


def generate_acquisition_schedule(
    tle_lines,
    start_time_utc: str,
    source_sat_ids: Sequence[int],
    *,
    duration_s: float,
    dt_s: float = 1,
    targets: Sequence[ImagingRegion | Mapping] | None = None,
    scan_step_s: float = 10,
    max_off_nadir_deg: float = 30,
    min_sun_elevation_deg: float = 20,
    sun_guard_deg: float = 1,
    strips_per_pass: int = 1,
    min_spacing_s: float = 30,
    pass_cooldown_s: float = 600,
    incidence_range_deg: tuple[float, float] | None = None,
) -> dict:
    """返回 JSON 可序列化的 events、windows、metadata 与计数。

    到达区间为 [0, duration_s)。只在与仿真步对齐的采样时刻成像；窗口
    首尾为采样近似，短于 scan_step_s 的窗口可能漏检。任务在采样时刻
    释放，成像本身视为瞬时，min_spacing_s 是可配置的载荷占用代理。
    每个源星/区域的连续可用窗口至多 strips_per_pass 条带；源星的全部
    区域共享 min_spacing_s，两个窗口首条带之间另有 pass_cooldown_s。
    圆内最近点是成像条带中心的代理，不模拟云量、姿态动力学、完整
    条带足迹、区域去重或数据压缩。输出窗口包含未被选择成像的机会。
    """
    scalars = (duration_s, dt_s, scan_step_s, max_off_nadir_deg,
               min_sun_elevation_deg, sun_guard_deg, min_spacing_s, pass_cooldown_s)
    if not all(math.isfinite(x) for x in scalars):
        raise ValueError("Acquisition parameters must be finite")
    if min(duration_s, dt_s, scan_step_s) <= 0 or min(min_spacing_s, pass_cooldown_s, sun_guard_deg) < 0:
        raise ValueError("Time scales must be positive and spacing/guards nonnegative")
    scan_steps = round(scan_step_s / dt_s)
    if scan_steps < 1 or not math.isclose(scan_steps * dt_s, scan_step_s):
        raise ValueError("scan_step_s must be an integer multiple of dt_s")
    if not 0 <= max_off_nadir_deg < 90 or not -90 <= min_sun_elevation_deg <= 90:
        raise ValueError("Invalid imaging or solar elevation threshold")
    if incidence_range_deg is not None and not (len(incidence_range_deg) == 2
            and 0 <= incidence_range_deg[0] < incidence_range_deg[1] < 90):
        raise ValueError("Invalid incidence angle range")
    if type(strips_per_pass) is not int or strips_per_pass < 1:
        raise ValueError("strips_per_pass must be a positive integer")
    if (not source_sat_ids or any(type(s) is not int or not 0 <= s < len(tle_lines) for s in source_sat_ids)
            or len(set(source_sat_ids)) != len(source_sat_ids)):
        raise ValueError("Source IDs must be distinct valid satellite indices")
    regions = [r if isinstance(r, ImagingRegion) else ImagingRegion(**r)
               for r in (US_CIVIL_REGIONS if targets is None else targets)]
    if not regions or len({r.target_id for r in regions}) != len(regions):
        raise ValueError("Imaging regions must have distinct target IDs")
    sources = sorted(source_sat_ids)
    regions.sort(key=lambda r: r.target_id)
    t0 = parse_start_time_utc(start_time_utc)
    steps = np.arange(0, math.ceil(duration_s / dt_s), scan_steps, dtype=np.int64)
    seconds = steps * dt_s
    seconds = seconds[seconds < duration_s]
    steps = steps[:len(seconds)]
    datetimes = [t0 + timedelta(seconds=float(t)) for t in seconds]
    times = time_grid(t0, seconds)
    satellites = load_satellites(tle_lines)
    windows, candidates = [], []
    for sid in sources:
        for region in regions:
            geometry = _sample_geometry(satellites[sid], times, datetimes, region)
            good = ((geometry["off_nadir_deg"] <= max_off_nadir_deg)
                    & (geometry["sat_elevation_deg"] > 0)
                    & (geometry["sun_elevation_deg"] >= min_sun_elevation_deg + sun_guard_deg))
            if incidence_range_deg is not None:
                incidence = 90 - geometry["sat_elevation_deg"]
                good &= (incidence >= incidence_range_deg[0]) & (incidence <= incidence_range_deg[1])
            boundaries = np.flatnonzero(np.diff(np.r_[False, good, False]))
            for start, stop in zip(boundaries[::2], boundaries[1::2]):
                window_id = len(windows)
                windows.append({
                    "window_id": window_id, "source_sat_id": sid, "target_id": region.target_id,
                    "start_s": float(seconds[start]),
                    "end_s": float(min(duration_s, seconds[stop - 1] + scan_step_s)),
                    "sample_count": int(stop - start), "left_censored": bool(start == 0),
                    "right_censored": bool(stop == len(seconds)),
                    "min_off_nadir_deg": float(np.min(geometry["off_nadir_deg"][start:stop])),
                    "max_sun_elevation_deg": float(np.max(geometry["sun_elevation_deg"][start:stop])),
                })
                for index in range(start, stop):
                    candidates.append({
                        "release_step": int(steps[index]), "source_sat_id": sid,
                        "target_id": region.target_id, "window_id": window_id,
                        "geometry": {key: float(value[index]) for key, value in geometry.items()},
                    })
    events, counts, last_emit, last_pass = [], {}, {}, {}
    for candidate in sorted(candidates, key=lambda e: (e["release_step"], e["source_sat_id"], e["target_id"])):
        sid, wid = candidate["source_sat_id"], candidate["window_id"]
        pair = (sid, candidate["target_id"])
        when = candidate["release_step"] * dt_s
        used = counts.get(wid, 0)
        if used >= strips_per_pass or when - last_emit.get(sid, -math.inf) < min_spacing_s:
            continue
        if not used and when - last_pass.get(pair, -math.inf) < pass_cooldown_s:
            continue
        if not used:
            last_pass[pair] = when
        counts[wid], last_emit[sid] = used + 1, when
        events.append(candidate)
    for window in windows:
        window["scheduled_strips"] = counts.get(window["window_id"], 0)
    return {
        "events": events, "windows": windows,
        "event_count_by_source": {str(sid): sum(e["source_sat_id"] == sid for e in events) for sid in sources},
        "window_count_by_source": {str(sid): sum(w["source_sat_id"] == sid for w in windows) for sid in sources},
        "metadata": {
            "start_time_utc": t0.isoformat(), "duration_s": duration_s, "dt_s": dt_s,
            "scan_step_s": scan_step_s, "source_sat_ids": sources,
            "targets": [asdict(r) for r in regions], "max_off_nadir_deg": max_off_nadir_deg,
            "min_sun_elevation_deg": min_sun_elevation_deg, "sun_guard_deg": sun_guard_deg,
            "strips_per_pass": strips_per_pass, "min_spacing_s": min_spacing_s,
            "pass_cooldown_s": pass_cooldown_s, "orbit_model": "SGP4/Skyfield/WGS84",
            "incidence_range_deg": incidence_range_deg,
            "solar_model": "NOAA fractional-year approximate geometric solar elevation",
            "solar_source": SOLAR_SOURCE,
            "solar_error_bound_deg": None,
            "solar_guard_note": "Design margin, not a certified approximation-error bound",
            "acquisition_model": "Instantaneous region-center strip proxy; no clouds or attitude slew",
            "window_sampling_note": "Boundary error up to scan step for resolved crossings; shorter windows can be missed",
        },
    }
