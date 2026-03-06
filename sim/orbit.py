from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Tuple

try:
    from sgp4.api import Satrec, jday
except Exception:  # pragma: no cover - 仅在缺少依赖时触发
    Satrec = None
    jday = None


@dataclass
class EphemerisConfig:
    start_time_utc: str
    tle_lines: List[Tuple[str, str]]


def parse_start_time_utc(start_time_utc: str) -> datetime:
    # 允许 "YYYY-MM-DDTHH:MM:SSZ" 或 "YYYY-MM-DD HH:MM:SS" 格式
    if start_time_utc.endswith("Z"):
        return datetime.fromisoformat(start_time_utc.replace("Z", "+00:00"))
    dt = datetime.fromisoformat(start_time_utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_satellites(tle_lines: List[Tuple[str, str]]) -> List[Satrec]:
    if Satrec is None:
        raise ImportError("sgp4 is required for ephemeris mode. Install with: pip install sgp4")
    sats: List[Satrec] = []
    for line1, line2 in tle_lines:
        sats.append(Satrec.twoline2rv(line1.strip(), line2.strip()))
    return sats


def position_km(sat: Satrec, t0: datetime, t_seconds: float) -> Tuple[float, float, float]:
    # SGP4 输出 TEME 坐标系下的地心惯性坐标 (km)
    if jday is None:
        return (0.0, 0.0, 0.0)
    dt = t0 + timedelta(seconds=float(t_seconds))
    jd, fr = jday(
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second + dt.microsecond / 1e6
    )
    err, r, _v = sat.sgp4(jd, fr)
    if err != 0:
        # 出错时返回原点，后续会导致链路不可见
        return (0.0, 0.0, 0.0)
    return (r[0], r[1], r[2])
