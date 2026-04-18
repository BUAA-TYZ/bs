from __future__ import annotations

import argparse
import base64
import hashlib
import json
import yaml
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
from skyfield.api import EarthSatellite, load, wgs84
from skyfield.framelib import itrs

# ---------------------------------------------------------------------------
# 卫星图标：SVG 内联 data URI（不依赖外部文件/字体，Cesium 原生支持 SVG）
# 图形：简化卫星轮廓（主体矩形 + 两侧太阳能板 + 天线）
# ---------------------------------------------------------------------------
_SAT_SVG = """\
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" width="64" height="64">
  <!-- 太阳能板（左）-->
  <rect x="2" y="26" width="18" height="12" rx="2"
        fill="#4a9eff" stroke="#2060cc" stroke-width="1.5"/>
  <!-- 太阳能板（右）-->
  <rect x="44" y="26" width="18" height="12" rx="2"
        fill="#4a9eff" stroke="#2060cc" stroke-width="1.5"/>
  <!-- 连接杆（左）-->
  <rect x="20" y="30" width="6" height="4" fill="#aaa"/>
  <!-- 连接杆（右）-->
  <rect x="38" y="30" width="6" height="4" fill="#aaa"/>
  <!-- 主体 -->
  <rect x="22" y="20" width="20" height="24" rx="3"
        fill="#e8e8e8" stroke="#888" stroke-width="1.5"/>
  <!-- 天线 -->
  <line x1="32" y1="20" x2="32" y2="10" stroke="#ccc" stroke-width="2"/>
  <circle cx="32" cy="9" r="3" fill="#ffdd44" stroke="#cc9900" stroke-width="1"/>
  <!-- 高亮 -->
  <rect x="25" y="23" width="6" height="8" rx="1" fill="rgba(255,255,255,0.35)"/>
</svg>"""

SAT_ICON_DATA_URI = (
    "data:image/svg+xml;base64," + base64.b64encode(_SAT_SVG.encode()).decode()
)


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------


@dataclass
class TLEEntry:
    name: str
    line1: str
    line2: str


@dataclass
class GroundStationEntry:
    gs_id: str
    lat_deg: float
    lon_deg: float
    alt_m: float
    min_elevation_deg: float
    bandwidth_mbps: float


# ---------------------------------------------------------------------------
# 解析工具
# ---------------------------------------------------------------------------


def parse_tle_file(path: Path) -> List[TLEEntry]:
    raw = [
        ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    out: List[TLEEntry] = []
    i = 0
    while i < len(raw):
        if (
            i + 2 < len(raw)
            and raw[i + 1].startswith("1 ")
            and raw[i + 2].startswith("2 ")
        ):
            out.append(TLEEntry(name=raw[i], line1=raw[i + 1], line2=raw[i + 2]))
            i += 3
            continue
        if i + 1 < len(raw) and raw[i].startswith("1 ") and raw[i + 1].startswith("2 "):
            sat_id = raw[i].split()[1] if len(raw[i].split()) > 1 else f"sat_{len(out)}"
            out.append(TLEEntry(name=sat_id, line1=raw[i], line2=raw[i + 1]))
            i += 2
            continue
        i += 1
    if not out:
        raise ValueError(f"No valid TLE entries found in: {path}")
    return out


def load_config(cfg_path: Path) -> Dict:
    """从仿真 config yaml/json 中读取 TLE、地面站、起始时间等信息。"""
    text = cfg_path.read_text(encoding="utf-8")
    if cfg_path.suffix in (".yaml", ".yml"):
        return yaml.safe_load(text)
    return json.loads(text)


def ground_stations_from_config(cfg: Dict) -> List[GroundStationEntry]:
    out = []
    for idx, item in enumerate(cfg.get("ground_stations", [])):
        out.append(
            GroundStationEntry(
                gs_id=str(item.get("id", f"gs_{idx}")),
                lat_deg=float(item.get("lat_deg", 0.0)),
                lon_deg=float(item.get("lon_deg", 0.0)),
                alt_m=float(item.get("alt_m", 0.0)),
                min_elevation_deg=float(item.get("min_elevation_deg", 5.0)),
                bandwidth_mbps=float(item.get("bandwidth_mbps", 0.0)),
            )
        )
    return out


# ---------------------------------------------------------------------------
# 时间工具
# ---------------------------------------------------------------------------


def parse_utc(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso_z(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


# ---------------------------------------------------------------------------
# 颜色
# ---------------------------------------------------------------------------


def sat_color(name: str) -> List[int]:
    """卫星：亮色，按名称哈希区分。"""
    d = hashlib.sha1(name.encode()).digest()
    return [80 + d[0] % 150, 80 + d[1] % 150, 80 + d[2] % 150, 255]


# 地面站固定用醒目的黄色
GS_COLOR = [255, 220, 50, 255]
GS_CONE_COLOR = [255, 220, 50, 60]  # 半透明仰角锥


# ---------------------------------------------------------------------------
# 轨迹采样
# ---------------------------------------------------------------------------


def sample_positions_m(
    sat: EarthSatellite, ts, start: datetime, duration_s: int, step_s: int
) -> List[float]:
    """向量化采样卫星位置（ECEF，米），用于动画 position。"""
    t_secs = np.arange(0, duration_s + 1, step_s)
    dts = [start + timedelta(seconds=float(t)) for t in t_secs]
    t_arr = ts.from_datetimes(dts)
    xyz_km = sat.at(t_arr).frame_xyz(itrs).km  # shape (3, N)
    xyz_m = xyz_km * 1000.0
    # 交织格式：[t0, x0, y0, z0, t1, x1, y1, z1, ...]
    values: List[float] = []
    for i, t in enumerate(t_secs):
        values.append(float(t))
        values.append(float(xyz_m[0, i]))
        values.append(float(xyz_m[1, i]))
        values.append(float(xyz_m[2, i]))
    return values


def sample_orbit_ring_m(
    sat: EarthSatellite, ts, start: datetime, period_s: int, step_s: int = 30
) -> List[float]:
    """向量化采样完整一个轨道周期的 ECI 位置（米），用于静态 polyline 轨道圆环。
    使用 ECI（INERTIAL）坐标系，Cesium referenceFrame=INERTIAL 下轨道圆环静止不动。
    """
    t_secs = np.arange(0, period_s + step_s, step_s)
    dts = [start + timedelta(seconds=float(t)) for t in t_secs]
    t_arr = ts.from_datetimes(dts)
    xyz_km = sat.at(t_arr).position.km  # shape (3, N)，ECI/GCRS
    xyz_m = xyz_km * 1000.0
    cartesian: List[float] = []
    for i in range(xyz_m.shape[1]):
        cartesian.append(float(xyz_m[0, i]))
        cartesian.append(float(xyz_m[1, i]))
        cartesian.append(float(xyz_m[2, i]))
    # 闭合：最后一点 = 第一点
    cartesian.extend(cartesian[:3])
    return cartesian


# ---------------------------------------------------------------------------
# CZML packet 构建
# ---------------------------------------------------------------------------


def sat_packets(
    idx: int,
    entry: TLEEntry,
    ts,
    start: datetime,
    duration_s: int,
    step_s: int,
    interval: str,
    orbit_period_s: int = 5730,  # LEO 典型轨道周期（秒）
) -> List[dict]:
    """返回两个 packet：卫星动画点 + 静态轨道圆环 polyline。"""
    sat = EarthSatellite(entry.line1, entry.line2, name=entry.name)

    # --- 动画位置（仿真时间段内，ECEF）---
    cartesian_anim = sample_positions_m(
        sat=sat, ts=ts, start=start, duration_s=duration_s, step_s=step_s
    )

    # --- 静态轨道圆环（一个轨道周期，ECI INERTIAL）---
    orbit_ring = sample_orbit_ring_m(
        sat=sat, ts=ts, start=start, period_s=orbit_period_s, step_s=20
    )

    rgba = sat_color(entry.name)
    inc = float(entry.line2[8:16])
    raan = float(entry.line2[17:25])
    label_text = f"{entry.name}\ninc={inc:.1f}° RAAN={raan:.1f}°"

    # packet 1：卫星图标 + 标签（动画）
    sat_pkt = {
        "id": f"sat-{idx}",
        "name": entry.name,
        "availability": interval,
        "label": {
            "text": label_text,
            "font": "11pt sans-serif",
            "fillColor": {"rgba": rgba},
            "outlineColor": {"rgba": [0, 0, 0, 255]},
            "outlineWidth": 2,
            "style": "FILL_AND_OUTLINE",
            "horizontalOrigin": "LEFT",
            "pixelOffset": {"cartesian2": [18, -10]},
            "show": True,
        },
        "billboard": {
            "image": SAT_ICON_DATA_URI,
            "scale": 0.55,
            "show": True,
            "verticalOrigin": "CENTER",
            "horizontalOrigin": "CENTER",
        },
        "position": {
            "epoch": iso_z(start),
            "cartesian": cartesian_anim,
            "interpolationAlgorithm": "LAGRANGE",
            "interpolationDegree": 5,
            "referenceFrame": "FIXED",
        },
    }

    # packet 2：静态轨道圆环（polyline，ECI INERTIAL，始终完整显示）
    orbit_pkt = {
        "id": f"orbit-{idx}",
        "name": f"{entry.name} orbit",
        "polyline": {
            "positions": {
                "referenceFrame": "INERTIAL",
                "cartesian": orbit_ring,
            },
            "width": 1.2,
            "material": {"solidColor": {"color": {"rgba": [*rgba[:3], 160]}}},
            "arcType": "NONE",
            "show": True,
        },
    }

    return [sat_pkt, orbit_pkt]


def gs_packet(gs: GroundStationEntry) -> dict:
    """地面站固定点 packet（始终显示，不受时间限制）。"""
    # WGS84 → ECEF（米）
    lat_r = gs.lat_deg * 3.141592653589793 / 180.0
    lon_r = gs.lon_deg * 3.141592653589793 / 180.0
    a = 6378137.0
    e2 = 6.6943799901414e-3
    import math

    N = a / math.sqrt(1 - e2 * math.sin(lat_r) ** 2)
    x = (N + gs.alt_m) * math.cos(lat_r) * math.cos(lon_r)
    y = (N + gs.alt_m) * math.cos(lat_r) * math.sin(lon_r)
    z = (N * (1 - e2) + gs.alt_m) * math.sin(lat_r)

    label = (
        f"{gs.gs_id}\n"
        f"{gs.lat_deg:.2f}°N {gs.lon_deg:.2f}°E\n"
        f"BW: {gs.bandwidth_mbps:.0f} Mbps  elev≥{gs.min_elevation_deg:.0f}°"
    )
    return {
        "id": f"gs-{gs.gs_id}",
        "name": gs.gs_id,
        "label": {
            "text": label,
            "font": "12pt sans-serif",
            "fillColor": {"rgba": GS_COLOR},
            "outlineColor": {"rgba": [0, 0, 0, 255]},
            "outlineWidth": 2,
            "style": "FILL_AND_OUTLINE",
            "horizontalOrigin": "LEFT",
            "pixelOffset": {"cartesian2": [14, -10]},
            "show": True,
        },
        "billboard": {
            # 地面站用稍大的点
            "image": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wm6QocAAAAASUVORK5CYII=",
            "color": {"rgba": GS_COLOR},
            "scale": 14,
            "show": True,
        },
        "position": {
            "cartesian": [x, y, z],
            "referenceFrame": "FIXED",
        },
    }


def build_packets(
    tles: Iterable[TLEEntry],
    ground_stations: List[GroundStationEntry],
    start: datetime,
    duration_s: int,
    step_s: int,
    trail_s: int = 0,  # 保留参数兼容旧调用，已不使用
) -> List[dict]:
    end = start + timedelta(seconds=duration_s)
    interval = f"{iso_z(start)}/{iso_z(end)}"

    packets: List[dict] = [
        {
            "id": "document",
            "name": "Satellite & Ground Station Visualization",
            "version": "1.0",
            "clock": {
                "interval": interval,
                "currentTime": iso_z(start),
                "multiplier": 60,
                "range": "LOOP_STOP",
                "step": "SYSTEM_CLOCK_MULTIPLIER",
            },
        }
    ]

    ts = load.timescale()
    tles = list(tles)
    for idx, entry in enumerate(tles):
        packets.extend(
            sat_packets(
                idx=idx,
                entry=entry,
                ts=ts,
                start=start,
                duration_s=duration_s,
                step_s=step_s,
                interval=interval,
            )
        )

    for gs in ground_stations:
        packets.append(gs_packet(gs))

    return packets


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert TLE + ground stations to CZML for Cesium visualization."
    )
    # TLE 来源：直接指定文件，或从仿真 config 读取
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--tle-file", help="Path to TLE file (name+line1+line2 format).")
    src.add_argument(
        "--config",
        help="Path to sim config YAML/JSON (reads tle_file + ground_stations).",
    )

    parser.add_argument(
        "--gs-file",
        help="额外的地面站 YAML 文件（格式同 config 的 ground_stations 列表）。",
    )
    parser.add_argument("--out", required=True, help="Output CZML file path.")
    parser.add_argument(
        "--start-utc", default=None, help="UTC start time, e.g. 2026-03-20T17:30:00Z."
    )
    parser.add_argument(
        "--duration-hours",
        type=float,
        default=3.0,
        help="Trajectory duration in hours.",
    )
    parser.add_argument(
        "--step-seconds", type=int, default=60, help="Sampling step in seconds."
    )
    parser.add_argument(
        "--trail-seconds",
        type=int,
        default=3600,
        help="Visible trailing path in seconds.",
    )
    parser.add_argument(
        "--max-sats", type=int, default=0, help="Max satellites (0 = all)."
    )
    args = parser.parse_args()

    # --- 读取 TLE 和地面站 ---
    ground_stations: List[GroundStationEntry] = []
    tle_path: Optional[Path] = None
    start_from_config: Optional[str] = None

    if args.config:
        cfg_path = Path(args.config)
        cfg = load_config(cfg_path)
        topo = cfg.get("topology", {})
        tle_rel = topo.get("tle_file")
        if not tle_rel:
            parser.error("Config 中未找到 topology.tle_file")
        # tle_file 路径：先尝试相对于 config 所在目录，再尝试相对于项目根目录（cwd）
        candidate = cfg_path.parent / tle_rel
        tle_path = candidate if candidate.exists() else Path(tle_rel)
        ground_stations = ground_stations_from_config(cfg)
        start_from_config = topo.get("start_time_utc")
    else:
        tle_path = Path(args.tle_file)

    # 额外地面站文件
    if args.gs_file:
        gs_cfg = yaml.safe_load(Path(args.gs_file).read_text(encoding="utf-8"))
        raw_list = (
            gs_cfg if isinstance(gs_cfg, list) else gs_cfg.get("ground_stations", [])
        )
        for idx, item in enumerate(raw_list):
            ground_stations.append(
                GroundStationEntry(
                    gs_id=str(item.get("id", f"gs_extra_{idx}")),
                    lat_deg=float(item.get("lat_deg", 0.0)),
                    lon_deg=float(item.get("lon_deg", 0.0)),
                    alt_m=float(item.get("alt_m", 0.0)),
                    min_elevation_deg=float(item.get("min_elevation_deg", 5.0)),
                    bandwidth_mbps=float(item.get("bandwidth_mbps", 0.0)),
                )
            )

    # --- 起始时间 ---
    if args.start_utc:
        start = parse_utc(args.start_utc)
    elif start_from_config:
        start = parse_utc(start_from_config)
    else:
        start = datetime.now(timezone.utc)

    duration_s = max(60, int(args.duration_hours * 3600))
    step_s = max(5, args.step_seconds)

    # --- 解析 TLE ---
    tles = parse_tle_file(tle_path)
    if args.max_sats and args.max_sats > 0:
        tles = tles[: args.max_sats]

    # --- 生成 CZML ---
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    packets = build_packets(
        tles=tles,
        ground_stations=ground_stations,
        start=start,
        duration_s=duration_s,
        step_s=step_s,
        trail_s=args.trail_seconds,
    )
    out_path.write_text(json.dumps(packets, indent=2), encoding="utf-8")
    print(
        f"Wrote {len(tles)} satellites + {len(ground_stations)} ground stations → {out_path}"
    )
    print(f"  Start : {iso_z(start)}")
    print(f"  End   : {iso_z(start + timedelta(seconds=duration_s))}")
    print(f"  Step  : {step_s}s  Trail: {args.trail_seconds}s")


if __name__ == "__main__":
    main()
