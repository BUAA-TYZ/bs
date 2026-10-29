"""只读冻结运行清单，核算轨道出口和瞬时星间连通机会；不读取策略成绩。"""

import argparse
from collections import Counter, deque
import gzip
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys

for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[variable] = "1"

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np

from analysis.audit_experiment_design import windows
from sim.orbit import build_ground_station, load_satellites, parse_start_time_utc, time_grid
from sim.reproducibility import runtime_metadata
from sim.topology import _visible_from_positions, bounded_edges


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def geometry_definition(cfg):
    topo = cfg["topology"]
    return {"num_sats": cfg["num_sats"], "duration_s": cfg["sim_steps"] * cfg["dt"],
            "topology_sample_s": cfg["topology_update_steps"] * cfg["dt"],
            "ground_sample_s": cfg["ground_visibility_update_steps"] * cfg["dt"],
            "sources": cfg["task_source_sat_ids"], "relay_max_hops": cfg["relay_max_hops"],
            "topology": {**{key: topo[key] for key in ("tle_lines", "start_time_utc", "earth_radius_km",
                                                       "max_range_km", "max_neighbors")},
                         "min_elevation_deg": topo.get("min_elevation_deg", 0.0)},
            "stations": [{key: station[key] for key in ("id", "lat_deg", "lon_deg", "alt_m",
                                                         "min_elevation_deg", "receive_channels")}
                         for station in cfg["ground_stations"]]}


def graph_stats(count, edges, exits, max_hops):
    adjacency = [set() for _ in range(count)]
    for left, right in edges:
        adjacency[left].add(right)
        adjacency[right].add(left)
    distances = {node: 0 for node in exits}
    queue = deque(sorted(exits))
    while queue:
        node = queue.popleft()
        if distances[node] >= max_hops:
            continue
        for neighbor in sorted(adjacency[node]):
            if neighbor not in distances:
                distances[neighbor] = distances[node] + 1
                queue.append(neighbor)
    remaining, sizes = set(range(count)), []
    while remaining:
        queue = deque([min(remaining)])
        remaining.remove(queue[0])
        size = 0
        while queue:
            node = queue.popleft()
            size += 1
            for neighbor in adjacency[node] & remaining:
                remaining.remove(neighbor)
                queue.append(neighbor)
        sizes.append(size)
    return distances, sorted(sizes, reverse=True)


def analyze(definition):
    sample = definition["topology_sample_s"]
    if sample != definition["ground_sample_s"] or definition["duration_s"] % sample:
        raise ValueError("This diagnostic requires equal physical geometry periods and complete intervals")
    topo = definition["topology"]
    sats = load_satellites(topo["tle_lines"])
    if len(sats) != definition["num_sats"]:
        raise ValueError("Satellite count mismatch")
    offsets = np.arange(0, definition["duration_s"], sample)
    times = time_grid(parse_start_time_utc(topo["start_time_utc"]), offsets)
    positions = np.stack([sat.at(times).position.km.T for sat in sats])
    visible = np.empty((len(sats), len(definition["stations"]), len(offsets)), dtype=bool)
    station_rows = []
    for gi, station in enumerate(definition["stations"]):
        observer = build_ground_station(station["lat_deg"], station["lon_deg"], station["alt_m"])
        for sid, satellite in enumerate(sats):
            visible[sid, gi] = (satellite - observer).at(times).altaz()[0].degrees >= station["min_elevation_deg"]
        counts = visible[:, gi].sum(axis=0)
        station_rows.append({"id": station["id"],
                             "any_satellite_visible_s": float(np.count_nonzero(counts) * sample),
                             "potential_channel_seconds": float(np.minimum(counts, station["receive_channels"]).sum() * sample),
                             "over_channel_count_s": float(np.count_nonzero(counts > station["receive_channels"]) * sample),
                             "max_simultaneously_visible": int(counts.max())})
    direct = visible.any(axis=1)
    reachable = np.zeros_like(direct)
    pairs = [(i, j) for i in range(len(sats)) for j in range(i + 1, len(sats))]
    component_counts, edge_counts, largest, snapshots = [], [], [], []
    for index, seconds in enumerate(offsets):
        points = [tuple(row) for row in positions[:, index]]
        geometry = [_visible_from_positions(points[i], points[j], topo["earth_radius_km"],
                                            topo["min_elevation_deg"], topo["max_range_km"])
                    for i, j in pairs]
        allowed = bounded_edges(pairs, geometry, topo["max_neighbors"])
        exits = np.flatnonzero(direct[:, index]).tolist()
        distances, sizes = graph_stats(len(sats), allowed, exits, definition["relay_max_hops"])
        reachable[list(distances), index] = True
        component_counts.append(len(sizes))
        edge_counts.append(len(allowed))
        largest.append(sizes[0])
        snapshots.append({"seconds": float(seconds), "edges": sorted(allowed), "ground_exit_satellites": exits,
                          "reachable_hops": distances, "component_sizes": sizes})
    sources = definition["sources"]
    per_source = []
    for sid in sources:
        intervals = windows(direct[sid], sample)
        per_source.append({"sat_id": sid, "direct_contact_s": float(direct[sid].sum() * sample),
                           "instant_relay_exit_s": float(reachable[sid].sum() * sample),
                           "first_direct_contact_s": intervals[0][0] if intervals else None,
                           "direct_windows_s": intervals,
                           "instant_relay_windows_s": windows(reachable[sid], sample)})
    return {"sample_period_s": sample, "sample_count": len(offsets),
            "source_direct_visibility_sha256": digest(direct[sources].tolist()),
            "source_tle_sha256": digest([topo["tle_lines"][sid] for sid in sources]),
            "source_direct_fraction": float(direct[sources].mean()),
            "source_instant_relay_exit_fraction": float(reachable[sources].mean()),
            "component_count_histogram": dict(Counter(component_counts)),
            "mean_component_count": statistics.mean(component_counts),
            "mean_largest_component_size": statistics.mean(largest), "mean_isl_edges": statistics.mean(edge_counts),
            "stations": station_rows, "sources": per_source}, snapshots


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", action="append", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    runtime, geometries, cases, manifests = runtime_metadata(), {}, [], []
    for name in args.manifest:
        path = Path(name)
        manifest = json.loads(path.read_text())
        if manifest["source_sha256"] != runtime["source_sha256"]:
            raise ValueError("Frozen core source mismatch")
        manifests.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        for entry in manifest["cases"]:
            definition = geometry_definition(entry["config"])
            key = digest(definition)
            if key not in geometries:
                result, snapshots = analyze(definition)
                snapshot_name = f"geometry_{key[:12]}.json.gz"
                (out / snapshot_name).write_bytes(gzip.compress(json.dumps(snapshots, separators=(",", ":")).encode(), mtime=0))
                geometries[key] = {"definition": definition, "summary": result, "snapshot_file": snapshot_name,
                                   "snapshot_sha256": hashlib.sha256((out / snapshot_name).read_bytes()).hexdigest()}
                print(json.dumps({"geometry_finished": len(geometries), "sample_count": result["sample_count"]}), flush=True)
            cases.append({"split": manifest["args"]["split"], "case": entry["case"], "geometry_sha256": key})
    source_checks = []
    for split in sorted({row["split"] for row in cases}):
        keys = {row["geometry_sha256"] for row in cases if row["split"] == split}
        equal = all(len({geometries[key]["summary"][field] for key in keys}) == 1
                    for field in ("source_tle_sha256", "source_direct_visibility_sha256"))
        if not equal:
            raise ValueError(f"Source geometry changed across conditions in {split}")
        source_checks.append({"split": split, "source_orbits_and_direct_visibility_equal_across_conditions": equal})
    result = {**runtime, "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "manifests": manifests, "cases": cases, "geometries": geometries, "checks": source_checks,
              "scope": "post-freeze descriptive geometry audit; no policy outcomes read; no configuration changed",
              "limitations": ["Sample-and-hold geometry at the configured 10 second period; not continuous truth.",
                              "Instant relay reachability ignores rates, queues, transfer duration and future store-carry-forward.",
                              "Geometric paths and potential channel seconds do not prove task delivery feasibility.",
                              "Different conditions use different task seeds and link phases; comparisons are not exact paired interventions."]}
    (out / "summary.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    lines = ["# Frozen-Scenario Geometry Audit", "", result["scope"], "",
             "| Split | Layout | Source direct time fraction | Source instantaneous relay-exit fraction | Mean components | Mean largest component |",
             "|---|---|---:|---:|---:|---:|"]
    seen = set()
    for row in cases:
        pair = (row["split"], row["geometry_sha256"])
        if pair in seen:
            continue
        seen.add(pair)
        item = geometries[row["geometry_sha256"]]["summary"]
        lines.append(f"| {row['split']} | {row['case']['role_profile']} | {item['source_direct_fraction']:.6f} | "
                     f"{item['source_instant_relay_exit_fraction']:.6f} | {item['mean_component_count']:.3f} | "
                     f"{item['mean_largest_component_size']:.3f} |")
    lines += ["", *[f"- {value}" for value in result["limitations"]], ""]
    (out / "summary.md").write_text("\n".join(lines))
    if runtime_metadata() != runtime:
        raise ValueError("Core runtime changed during diagnostic")


if __name__ == "__main__":
    main()
