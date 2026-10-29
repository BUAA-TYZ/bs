"""独立场景校准：采样几何/固定配对零代价上界，不运行调度策略。"""

import argparse
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import resource
import sys
import time

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components
from scipy.spatial import cKDTree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.redesign_scenario import ANCHOR, digest, write_json
from sim.constellation import walker_tle
from sim.orbit import load_satellites, parse_start_time_utc, time_grid, build_ground_station
from sim.topology import TopologyConfig, TopologyModel, bounded_edges, _visible_from_positions
from sim.cache.contacts import ContactCache

ROOT = Path("results/redesign_20261021/calibration")
BASE = Path("results/redesign_20261021/M_2026-10-15_k16_86400s/config.json")
SHAPES = {12: (1, 12), 144: (12, 12), 1056: (48, 22)}
DEADLINES = (300, 600, 900, 1800)
SITES = [("Svalbard", 78.23, 15.40), ("Puertollano", 38.68, -4.11),
         ("Troll", -72.01, 2.535), ("Cordoba", -31.52, -64.46)]


def configuration(count):
    cfg = json.loads(BASE.read_text())
    if count != 12:
        planes, per_plane = SHAPES[count]
        cfg["topology"]["tle_lines"] = cfg["topology"]["tle_lines"][:24] + walker_tle(
            ANCHOR, planes, per_plane, 1015, 98.98, raan_offset_deg=15,
            phasing=1, first_satellite_number=25,
            plane_raan_offsets_deg=[180*p/planes for p in range(planes)])
        cfg["topology"]["preferred_edges"] = sorted({tuple(sorted((24+p*per_plane+k,
                24+p*per_plane+(k+1)%per_plane))) for p in range(planes) for k in range(per_plane)})
    cfg["num_sats"] = 24 + count
    cfg["topology"]["neighbor_limits"] = [1]*24 + [4]*count
    return cfg


def geometry_edges(pos, radius=6378.137, max_range=4500):
    pairs = cKDTree(pos).query_pairs(max_range, output_type="ndarray")
    if not len(pairs):
        return pairs, np.empty(0)
    a, b = pos[pairs[:, 0]], pos[pairs[:, 1]]
    na, nb = np.linalg.norm(a, axis=1), np.linalg.norm(b, axis=1)
    sep = np.arccos(np.clip(np.einsum("ij,ij->i", a, b)/(na*nb), -1, 1))
    visible = (na > radius) & (nb > radius) & (sep <= np.arccos(radius/na)+np.arccos(radius/nb))
    distance = np.linalg.norm(a-b, axis=1)
    return pairs[visible], distance[visible]


def select_edges(pairs, distance, n, preferred, source_terminals=1, isolated=True):
    return bounded_edges([tuple(map(int, p)) for p in pairs],
                         [(True, float(d)) for d in distance], 4,
                         sources=tuple(range(24)) if isolated else (),
                         limits=(source_terminals,)*24+(4,)*(n-24), preferred=preferred)


def prepared_graph(pairs, n, isolated=True):
    pairs = np.asarray(pairs, dtype=np.int32).reshape(-1, 2)
    if isolated:
        links = pairs[(pairs[:, 0] >= 24) & (pairs[:, 1] >= 24)]
        outward = pairs[(pairs[:, 0] < 24) & (pairs[:, 1] >= 24)]
    else:
        links, outward = pairs, np.empty((0, 2), dtype=np.int32)
    matrix = coo_matrix((np.ones(len(links), dtype=bool), (links[:, 0], links[:, 1])), shape=(n, n)).tocsr()
    _, labels = connected_components(matrix, directed=False)
    return labels.astype(np.uint16), outward.astype(np.uint16)


def propagate(future, graph, visible, now):
    """反向动态规划，分量内瞬时传播；SAR只有出边，不会连接两个计算分量。"""
    labels, outward = graph
    result = future.copy()
    result[visible] = now
    mins = np.full(int(labels.max())+1, np.inf)
    np.minimum.at(mins, labels, result)
    result = mins[labels]
    if len(outward):
        np.minimum.at(result, outward[:, 0], result[outward[:, 1]])
    return result


def solve(frames, visibility, events, step=5):
    future = np.full((2, visibility.shape[1]), np.inf)
    due = {}
    rows = [{"task_id": f"task_{i}", "source_sat_id": e["source_sat_id"],
             "release_s": e["release_step"]*.1} for i, e in enumerate(events)]
    for i, row in enumerate(rows):
        due.setdefault(int(row["release_s"]//step), []).append(i)
    for k in range(len(frames)-1, -1, -1):
        now = k*step
        masks = (visibility[k//2, :, 0], visibility[k//2].any(axis=1))
        for network, mask in enumerate(masks):
            future[network] = propagate(future[network], frames[k], mask, now)
        for index in due.get(k, []):
            row = rows[index]
            for network, name in enumerate(("single", "five")):
                reached = future[network, row["source_sat_id"]]
                row[name+"_latency_s"] = max(0., float(reached)-row["release_s"]) if np.isfinite(reached) else None
    return {"tasks": rows, "counts": {network: {str(d): sum(r[network+"_latency_s"] is not None and
            r[network+"_latency_s"] <= d+1e-9 for r in rows) for d in DEADLINES} for network in ("single", "five")}}


def positions_and_visibility(cfg, seconds):
    sats = load_satellites(cfg["topology"]["tle_lines"])
    times = time_grid(parse_start_time_utc(ANCHOR), seconds)
    pos = np.stack([sat.at(times).position.km.T for sat in sats], axis=1)
    base = cfg["ground_stations"][0]
    sites = [(base["id"], base["lat_deg"], base["lon_deg"], base["alt_m"])] + [(*s, 0) for s in SITES]
    visible = np.empty((len(seconds), len(sats), len(sites)), dtype=bool)
    for j, (_, lat, lon, alt) in enumerate(sites):
        gs = build_ground_station(lat, lon, alt)
        delta = pos - gs.at(times).position.km.T[:, None, :]
        up = gs.rotation_at(times)[2].T
        sine = np.einsum("tnj,tj->tn", delta, up)/np.linalg.norm(delta, axis=2)
        visible[:, :, j] = sine >= math.sin(math.radians(10))
    return pos, visible


def topo_config(cfg):
    keys = TopologyConfig.__dataclass_fields__
    data = {k: v for k, v in cfg["topology"].items() if k in keys}
    data.update(num_sats=cfg["num_sats"], seed=cfg["seed"], source_sat_ids=tuple(range(24)),
                min_elevation_deg=0.)
    return TopologyConfig(**data)


def probe(count):
    cfg = configuration(count)
    start = time.perf_counter()
    topo = TopologyModel(topo_config(cfg))
    init_s = time.perf_counter()-start
    elapsed = []
    for step in (0, 100, 200):
        start = time.perf_counter()
        topo.snapshot(step, .1)
        elapsed.append(time.perf_counter()-start)
    cache = ContactCache(topo, horizon_s=300, scan_s=30, update_s=60)
    start = time.perf_counter()
    cache.windows_at(20)
    cold = time.perf_counter()-start
    start = time.perf_counter()
    cache.windows_at(80)
    warm = time.perf_counter()-start
    topo.close()
    row = {"compute_count":count, "total_nodes":cfg["num_sats"], "init_s":init_s,
           "snapshot_samples_s":elapsed, "contact_cold_s":cold, "contact_warm_s":warm,
           "peak_rss_bytes":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
           "daily_geometry_s":float(np.median(elapsed))*8640,
           "daily_contact_s":cold+warm*1439,
           "scope":"Measured original geometry/contact code only; excludes actor, queue, step and routing costs"}
    write_json(ROOT/f"cost_{count}.json", row)
    print(json.dumps(row), flush=True)


def run(count):
    out = ROOT/f"bounds_{count}.json"
    if out.exists():
        raise FileExistsError(out)
    cfg = configuration(count)
    events = sorted(cfg["scheduled_task_arrivals"], key=lambda e:e["release_step"])
    end = math.ceil((max(e["release_step"]*.1 for e in events)+1800)/10)*10
    seconds = np.arange(0, end+10, 10, dtype=float)
    start = time.perf_counter()
    pos, visible = positions_and_visibility(cfg, seconds)
    propagation_s = time.perf_counter()-start
    specs = {"base":(1, True, 5)}
    if count == 12:
        specs.update(sar_forward=(1, False, 5), sar_two_terminals=(2, True, 5), no_capture=(1, True, 0))
    frames = {key: [] for key in specs}
    frames["geometric"] = []
    if count == 12:
        frames["geometric_sar_forward"] = []
    since = {key:{} for key in specs}
    preferred = cfg["topology"]["preferred_edges"]
    geometry_s = matching_s = 0.
    intervals, opened = [], {}
    for k, now in enumerate(seconds):
        stamp = time.perf_counter()
        pairs, distance = geometry_edges(pos[k])
        graph = prepared_graph(pairs, cfg["num_sats"])
        frames["geometric"].extend((graph, graph))
        if count == 12:
            graph = prepared_graph(pairs, cfg["num_sats"], False)
            frames["geometric_sar_forward"].extend((graph, graph))
        geometry_s += time.perf_counter()-stamp
        stamp = time.perf_counter()
        for name, (terminals, isolated, delay) in specs.items():
            selected = select_edges(pairs, distance, cfg["num_sats"], preferred, terminals, isolated)
            previous = since[name]
            since[name] = {edge:previous.get(edge, float(now)) for edge in selected}
            if name == "base":
                for edge in set(opened)-selected:
                    a = opened.pop(edge)+delay
                    if a < now:
                        intervals.append((*edge, a, float(now)))
                for edge in selected-set(opened):
                    opened[edge] = float(now)
            for offset in (0, 5):
                ready = [edge for edge in selected if now+offset >= since[name][edge]+delay]
                frames[name].append(prepared_graph(ready, cfg["num_sats"], isolated))
        matching_s += time.perf_counter()-stamp
        if k%600 == 0:
            write_json(ROOT/f"progress_{count}.json", {"status":"RUNNING", "geometry_seconds":float(now),
                       "end_s":end, "elapsed_s":time.perf_counter()-start})
    for edge, opened_at in opened.items():
        if opened_at+5 < end+10:
            intervals.append((*edge, opened_at+5, end+10))
    result = {"compute_count":count,"config_sha256":digest(cfg), "base_file_sha256":hashlib.sha256(BASE.read_bytes()).hexdigest(),
              "protocol_sha256":hashlib.sha256((ROOT/"PROTOCOL.md").read_bytes()).hexdigest(),
              "auditor_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "sampling_s":10,"capture_substep_s":5,"end_s":end,"tasks":len(events),
              "results":{name:solve(graphs,visible,events) for name,graphs in frames.items()},
              "timing":{"orbit_ground_s":propagation_s,"geometry_components_s":geometry_s,
                        "matching_components_s":matching_s,"total_s":time.perf_counter()-start},
              "peak_rss_bytes":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
    # 保存稀疏接触和地面可见性，以便后续中性诊断复用，避免重复几何计算。
    np.savez_compressed(ROOT/f"contacts_{count}.npz", intervals=np.asarray(intervals),
                        times=seconds, ground_visible=visible)
    write_json(out, result)
    write_json(ROOT/f"progress_{count}.json", {"status":"COMPLETE","elapsed_s":time.perf_counter()-start})
    print(json.dumps({"compute_count":count,"counts":{key:v["counts"] for key,v in result["results"].items()},
                      "timing":result["timing"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("probe", "bounds"))
    parser.add_argument("--count", type=int, choices=SHAPES, required=True)
    args = parser.parse_args()
    (probe if args.mode == "probe" else run)(args.count)
