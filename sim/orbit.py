from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, List, Tuple

from skyfield.api import EarthSatellite, load

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
        raise ImportError("skyfield is required for ephemeris mode. Install with: pip install skyfield")
    sats: List[EarthSatellite] = []
    for line1, line2 in tle_lines:
        sats.append(EarthSatellite(line1.strip(), line2.strip()))
    return sats


def position_km(sat: EarthSatellite, t0: datetime, t_seconds: float) -> Tuple[float, float, float]:
    # Skyfield 返回地心惯性坐标（km）
    if _TS is None:
        return (0.0, 0.0, 0.0)
    dt = t0 + timedelta(seconds=float(t_seconds))
    t = _TS.from_datetime(dt)
    r = sat.at(t).position.km
    if len(r) != 3:
        return (0.0, 0.0, 0.0)
    return (float(r[0]), float(r[1]), float(r[2]))
