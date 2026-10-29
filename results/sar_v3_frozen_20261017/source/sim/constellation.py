"""用 SGP4 的轨道根数接口生成合成 Walker Delta 星座，不伪装为实测 TLE。"""

import math

from sgp4.api import Satrec, WGS72, jday
from sgp4.exporter import export_tle

from sim.orbit import parse_start_time_utc


def walker_tle(epoch_utc, planes, satellites_per_plane, altitude_km, inclination_deg,
               raan_offset_deg=0.0, phase_offset_deg=0.0, phasing=1,
               plane_raan_offsets_deg=None, first_satellite_number=1):
    if type(planes) is not int or type(satellites_per_plane) is not int or min(planes, satellites_per_plane) < 1:
        raise ValueError("Walker plane and satellite counts must be positive integers")
    if not 300 <= altitude_km <= 1500 or not 0 < inclination_deg < 180:
        raise ValueError("Expected LEO altitude 300..1500 km and inclination 0..180 degrees")
    if type(phasing) is not int or not 0 <= phasing < planes or planes * satellites_per_plane > 99999:
        raise ValueError("Invalid Walker phasing or satellite count")
    if not all(math.isfinite(v) for v in (raan_offset_deg, phase_offset_deg)):
        raise ValueError("Offsets must be finite")
    if plane_raan_offsets_deg is not None and (
            len(plane_raan_offsets_deg) != planes or
            any(not math.isfinite(v) for v in plane_raan_offsets_deg)):
        raise ValueError("Explicit RAAN offsets must contain one finite value per plane")
    if (type(first_satellite_number) is not int or first_satellite_number < 1 or
            first_satellite_number + planes * satellites_per_plane - 1 > 99999):
        raise ValueError("Synthetic satellite numbers must be in 1..99999")
    time = parse_start_time_utc(epoch_utc)
    jd, fraction = jday(time.year, time.month, time.day, time.hour, time.minute,
                        time.second + time.microsecond / 1e6)
    epoch = jd + fraction - 2433281.5
    # 与 SGP4 WGS72 引力常数保持一致；近圆轨道作为合成研究配置。
    mean_motion = math.sqrt(398600.8 / (6378.135 + altitude_km) ** 3) * 60
    count = planes * satellites_per_plane
    lines = []
    for plane in range(planes):
        # 近极轨双轨道面不能默认把 RAAN 相差 180 度当作正交平面。
        offset = plane * 360 / planes if plane_raan_offsets_deg is None else plane_raan_offsets_deg[plane]
        raan = math.radians((raan_offset_deg + offset) % 360)
        for slot in range(satellites_per_plane):
            anomaly = math.radians((phase_offset_deg + slot * 360 / satellites_per_plane
                                    + plane * phasing * 360 / count) % 360)
            sat = Satrec()
            sat.sgp4init(WGS72, "i", len(lines) + first_satellite_number, epoch, 0, 0, 0, 0.0001, 0,
                         math.radians(inclination_deg), anomaly, mean_motion, raan)
            lines.append(export_tle(sat))
    return lines


def clustered_walker_tle(epoch_utc, planes, groups_per_plane, phase_offsets_deg,
                         altitude_km, inclination_deg, raan_offset_deg=0.0,
                         phase_offset_deg=0.0, phasing=1):
    """保留均匀星座中每组首星轨道，仅移动同组成员的沿轨相位。"""
    if type(groups_per_plane) is not int or groups_per_plane < 1:
        raise ValueError("groups_per_plane must be a positive integer")
    if (not phase_offsets_deg or any(not math.isfinite(v) for v in phase_offsets_deg) or
            len(set(phase_offsets_deg)) != len(phase_offsets_deg) or phase_offsets_deg[0] != 0):
        raise ValueError("phase offsets must be finite, distinct and start with zero")
    if max(phase_offsets_deg) - min(phase_offsets_deg) >= 360 / groups_per_plane:
        raise ValueError("phase offsets overlap neighboring groups")
    members = len(phase_offsets_deg)
    uniform = walker_tle(epoch_utc, planes, groups_per_plane * members, altitude_km,
                         inclination_deg, raan_offset_deg, phase_offset_deg, phasing)
    lines = []
    for group in range(planes * groups_per_plane):
        anchor_lines = uniform[group * members]
        anchor = Satrec.twoline2rv(*anchor_lines)
        for index, offset in enumerate(phase_offsets_deg):
            if index == 0:
                lines.append(anchor_lines)
                continue
            member = Satrec()
            member.sgp4init(WGS72, "i", group * members + index + 1,
                           anchor.jdsatepoch + anchor.jdsatepochF - 2433281.5,
                           anchor.bstar, anchor.ndot, anchor.nddot, anchor.ecco,
                           anchor.argpo, anchor.inclo, (anchor.mo + math.radians(offset)) % (2 * math.pi),
                           anchor.no_kozai, anchor.nodeo)
            lines.append(export_tle(member))
    return lines
