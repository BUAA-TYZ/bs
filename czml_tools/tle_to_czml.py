from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Tuple

from skyfield.api import EarthSatellite, load
from skyfield.framelib import itrs


@dataclass
class TLEEntry:
    name: str
    line1: str
    line2: str


def parse_tle_file(path: Path) -> List[TLEEntry]:
    raw = [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    out: List[TLEEntry] = []
    i = 0
    while i < len(raw):
        if i + 2 < len(raw) and raw[i + 1].startswith("1 ") and raw[i + 2].startswith("2 "):
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


def parse_utc(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def color_rgba(name: str) -> List[int]:
    digest = hashlib.sha1(name.encode("utf-8")).digest()
    # Keep colors bright and readable.
    return [80 + digest[0] % 150, 80 + digest[1] % 150, 80 + digest[2] % 150, 255]


def sample_positions_m(
    sat: EarthSatellite, ts, start: datetime, duration_s: int, step_s: int
) -> List[float]:
    values: List[float] = []
    for t_sec in range(0, duration_s + 1, step_s):
        dt = start + timedelta(seconds=t_sec)
        t_sf = ts.from_datetime(dt)
        # Cesium fixed frame expects Earth-fixed coordinates.
        xyz_km = sat.at(t_sf).frame_xyz(itrs).km
        values.extend([float(t_sec), float(xyz_km[0] * 1000.0), float(xyz_km[1] * 1000.0), float(xyz_km[2] * 1000.0)])
    return values


def build_packets(
    tles: Iterable[TLEEntry],
    start: datetime,
    duration_s: int,
    step_s: int,
    trail_s: int,
) -> List[dict]:
    end = start + timedelta(seconds=duration_s)
    interval = f"{iso_z(start)}/{iso_z(end)}"
    packets: List[dict] = [
        {
            "id": "document",
            "name": "TLE Visualization",
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
    for idx, entry in enumerate(tles):
        sat = EarthSatellite(entry.line1, entry.line2, name=entry.name)
        cartesian = sample_positions_m(sat=sat, ts=ts, start=start, duration_s=duration_s, step_s=step_s)
        rgba = color_rgba(entry.name)
        packets.append(
            {
                "id": f"sat-{idx}",
                "name": entry.name,
                "availability": interval,
                "label": {
                    "text": entry.name,
                    "font": "12pt sans-serif",
                    "fillColor": {"rgba": rgba},
                    "outlineColor": {"rgba": [0, 0, 0, 255]},
                    "outlineWidth": 2,
                    "style": "FILL_AND_OUTLINE",
                    "horizontalOrigin": "LEFT",
                    "pixelOffset": {"cartesian2": [12, -8]},
                    "show": True,
                },
                "billboard": {
                    "image": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wm6QocAAAAASUVORK5CYII=",
                    "color": {"rgba": rgba},
                    "scale": 8,
                    "show": True,
                },
                "path": {
                    "show": True,
                    "leadTime": 0,
                    "trailTime": max(step_s, trail_s),
                    "width": 1.5,
                    "material": {"solidColor": {"color": {"rgba": rgba}}},
                },
                "position": {
                    "epoch": iso_z(start),
                    "cartesian": cartesian,
                    "interpolationAlgorithm": "LAGRANGE",
                    "interpolationDegree": 5,
                    "referenceFrame": "FIXED",
                },
            }
        )
    return packets


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert TLE file to CZML for Cesium visualization.")
    parser.add_argument("--tle-file", required=True, help="Path to input TLE file.")
    parser.add_argument("--out", required=True, help="Path to output CZML.")
    parser.add_argument("--start-utc", default=None, help="UTC start time, e.g. 2026-03-20T00:00:00Z.")
    parser.add_argument("--duration-hours", type=float, default=3.0, help="Trajectory duration in hours.")
    parser.add_argument("--step-seconds", type=int, default=60, help="Sampling step in seconds.")
    parser.add_argument("--trail-seconds", type=int, default=3600, help="Visible trailing path length.")
    parser.add_argument("--max-sats", type=int, default=24, help="Max satellites to include.")
    args = parser.parse_args()

    start = parse_utc(args.start_utc) if args.start_utc else datetime.now(timezone.utc)
    duration_s = max(60, int(args.duration_hours * 3600))
    step_s = max(5, args.step_seconds)

    tle_path = Path(args.tle_file)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tles = parse_tle_file(tle_path)
    if args.max_sats > 0:
        tles = tles[: args.max_sats]

    packets = build_packets(
        tles=tles, start=start, duration_s=duration_s, step_s=step_s, trail_s=args.trail_seconds
    )
    out_path.write_text(json.dumps(packets, indent=2), encoding="utf-8")
    print(f"Wrote {len(packets) - 1} satellites to {out_path}")


if __name__ == "__main__":
    main()

