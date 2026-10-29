"""轨道与异构角色开发诊断；先固定布局矩阵，再测几何和可执行基线。"""

import argparse
from dataclasses import asdict, replace
import json
from pathlib import Path

import numpy as np

from main import run_config
from sim.config import load_config
from sim.constellation import clustered_walker_tle, walker_tle
from sim.env import SimulationEnv
from sim.evaluation import paired_task_metrics
from sim.orbit import time_grid
from sim.reproducibility import runtime_metadata
from sim.topology import _visible_from_positions, bounded_edges


def spans(mask, step_s):
    """首尾区间为截尾观测，不能据其长度推断完整窗口或完整断联。"""
    mask = np.asarray(mask, dtype=bool)
    edges = np.flatnonzero(np.diff(np.r_[False, mask, False]))
    return [{"start_s": int(a) * step_s, "end_s": int(b) * step_s,
             "duration_s": int(b - a) * step_s,
             "left_censored": bool(a == 0), "right_censored": bool(b == len(mask))}
            for a, b in zip(edges[::2], edges[1::2])]


def geometry(cfg, duration_s=86400, sample_s=30):
    if duration_s <= 0 or sample_s <= 0 or duration_s % sample_s:
        raise ValueError("Geometry duration must be positive and divisible by sample interval")
    env = SimulationEnv(replace(cfg, topology={**cfg.topology, "precompute_visibility": False,
                                              "predict_contacts": False}))
    try:
        times = time_grid(env.topology.t0, np.arange(0, duration_s, sample_s))
        sats = env.topology.sat_recs[:cfg.num_sats]
        positions = [sat.at(times).position.km.T for sat in sats]
        visible = np.zeros((len(times), cfg.num_sats), dtype=bool)
        station_runs = []
        for sid, sat in enumerate(sats):
            for gs_id, station in env.ground_station_objs.items():
                altitude, _, _ = (sat - station).at(times).altaz()
                up = altitude.degrees >= env.ground_stations[gs_id].min_elevation_deg
                visible[:, sid] |= up
                station_runs.append({"sat_id": sid, "gs_id": gs_id,
                                     "windows": spans(up, sample_s)})
        pairs = [(i, j) for i in range(cfg.num_sats) for j in range(i + 1, cfg.num_sats)]
        degree = np.zeros_like(visible, dtype=int)
        sources = cfg.task_source_sat_ids if cfg.task_source_sat_ids is not None else list(range(cfg.num_sats))
        rates = cfg.satellite_compute_rates if cfg.satellite_compute_rates is not None else [cfg.compute_rate] * cfg.num_sats
        faster_neighbor = np.zeros((len(times), len(sources)), dtype=bool)
        source_index = {sid: i for i, sid in enumerate(sources)}
        for t in range(len(times)):
            geom = [_visible_from_positions(tuple(positions[i][t]), tuple(positions[j][t]),
                    env.topology.cfg.earth_radius_km, env.topology.cfg.min_elevation_deg,
                    env.topology.cfg.max_range_km) for i, j in pairs]
            for i, j in bounded_edges(pairs, geom, env.topology.cfg.max_neighbors):
                degree[t, i] += 1
                degree[t, j] += 1
                for src, nb in ((i, j), (j, i)):
                    if src in source_index and rates[nb] > rates[src]:
                        faster_neighbor[t, source_index[src]] = True
        per_sat = [{"sat_id": sid, "visible_seconds": int(visible[:, sid].sum()) * sample_s,
                    "any_station_windows": spans(visible[:, sid], sample_s),
                    "no_station_intervals": spans(~visible[:, sid], sample_s)}
                   for sid in range(cfg.num_sats)]
        complete_windows = [w["duration_s"] for row in station_runs for w in row["windows"]
                            if not w["left_censored"] and not w["right_censored"]]
        source_gaps = [w["duration_s"] for sid in sources for w in per_sat[sid]["no_station_intervals"]]
        return {"duration_s": duration_s, "sample_s": sample_s,
                "scope": "geometry only; excludes contention, bandwidth, queues and terminal acquisition",
                "station_complete_pass_seconds_median": float(np.median(complete_windows)) if complete_windows else None,
                "station_complete_pass_seconds_p95": float(np.percentile(complete_windows, 95)) if complete_windows else None,
                "source_any_station_visible_fraction": float(visible[:, sources].mean()),
                "source_longest_observed_gap_seconds": max(source_gaps, default=0),
                "source_zero_neighbor_fraction": float((degree[:, sources] == 0).mean()),
                "source_mean_degree": float(degree[:, sources].mean()),
                "source_faster_neighbor_fraction": float(faster_neighbor.mean()),
                "max_degree": int(degree.max()), "satellites": per_sat, "station_passes": station_runs}
    finally:
        env.close()


def layout_config(base, altitude=705, inclination=98.2, clustered=False, heterogeneous=False):
    # 8 颗成像星的轨道不变；仅移动其余 16 颗星，总算力恒为 12。
    epoch = base.topology["start_time_utc"]
    kwargs = dict(epoch_utc=epoch, planes=4, altitude_km=altitude,
                  inclination_deg=inclination, raan_offset_deg=25, phase_offset_deg=15, phasing=1)
    lines = (clustered_walker_tle(groups_per_plane=2, phase_offsets_deg=[0, -2, 2], **kwargs)
             if clustered else walker_tle(satellites_per_plane=6, **kwargs))
    topology = {k: v for k, v in base.topology.items() if k not in ("tle_file", "tle_lines")}
    rates = [0.1 if sid % 3 == 0 else 0.7 for sid in range(24)] if heterogeneous else [0.5] * 24
    return replace(base, num_sats=24, task_source_sat_ids=list(range(0, 24, 3)),
                   satellite_compute_rates=rates, topology={**topology, "tle_lines": lines})


def run_study(args):
    output = Path(args.out_dir)
    output.mkdir(parents=True, exist_ok=False)
    base = load_config(args.config)
    report = {"scope": "predeclared development architecture diagnosis; no MARL selection or reserved test set",
              **runtime_metadata(), "arguments": vars(args), "geometry": [], "runs": [], "paired": []}
    base = replace(base, sim_steps=2160, task_arrival_end_step=720, dt=10,
                   task_arrival_rate=0.08, image_size_mb=128)
    if not args.skip_geometry:
        specs = [(400, 98.2, False), (550, 98.2, False), (705, 98.2, False),
                 (786, 98.2, False), (550, 53, False), (550, 98.2, True), (705, 98.2, True)]
        for altitude, inclination, clustered in specs:
            cfg = layout_config(base, altitude, inclination, clustered, True)
            label = f"{'clustered' if clustered else 'uniform'}_{altitude}_{inclination}"
            result = geometry(cfg, args.geometry_hours * 3600, args.geometry_sample_s)
            report["geometry"].append({"label": label, "altitude_km": altitude,
                                       "inclination_deg": inclination, "clustered": clustered, **result})
            print(json.dumps({"geometry": label, **{k: v for k, v in result.items()
                             if k not in ("satellites", "station_passes")}}), flush=True)
            (output / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    if not args.geometry_only:
        uniform = layout_config(base)
        cluster = layout_config(base, clustered=True)
        assert all(uniform.topology["tle_lines"][sid] == cluster.topology["tle_lines"][sid]
                   for sid in uniform.task_source_sat_ids)
        seeds = [int(seed) for seed in args.seeds.split(",")]
        if not seeds or len(set(seeds)) != len(seeds):
            raise ValueError("Study seeds must be nonempty and unique")
        configs = output / "configs"
        configs.mkdir()
        architectures = [(c, h) for c in (False, True) for h in (False, True)]
        for clustered, heterogeneous in architectures:
            architecture = f"{'clustered' if clustered else 'uniform'}_{'heterogeneous' if heterogeneous else 'homogeneous'}"
            for cost in (6, 60):
                for seed in seeds:
                    cfg = replace(layout_config(base, clustered=clustered, heterogeneous=heterogeneous),
                                  compute_cost_per_tile=cost, seed=seed)
                    assert np.isclose(sum(cfg.satellite_compute_rates), 12)
                    case_id = f"{architecture}_cost{cost}_seed{seed}"
                    (configs / f"{case_id}.json").write_text(json.dumps(asdict(cfg), indent=2) + "\n")
                    case_runs = {}
                    for policy in args.policies.split(","):
                        mode = "ground_compute" if policy == "ground_compute" else "distributed"
                        metrics = run_config(replace(cfg, pipeline_mode=mode), policy)
                        case_runs[policy] = metrics
                        report["runs"].append({"case_id": case_id, "architecture": architecture,
                                               "cost": cost, "seed": seed, "policy": policy, "metrics": metrics})
                        overall = metrics["overall"]
                        print(json.dumps({"case": case_id, "policy": policy,
                                          "completed": overall["completed_tasks"], "total": overall["total_tasks"]}), flush=True)
                    for reference in ("ground_compute", "local"):
                        if reference not in case_runs:
                            continue
                        ref = case_runs[reference]
                        for policy, metrics in case_runs.items():
                            assert ref["experiment"]["task_trace_sha256"] == metrics["experiment"]["task_trace_sha256"]
                            if policy != reference:
                                report["paired"].append({"case_id": case_id, "policy": policy, "reference": reference,
                                    **paired_task_metrics(ref["task_outcomes"], metrics["task_outcomes"])})
                    (output / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    (output / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    return report


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default="examples/config_committed.yaml")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--seeds", default="71001")
    p.add_argument("--policies", default="ground_compute,local,predictive,window_aware")
    p.add_argument("--geometry-hours", type=int, default=24)
    p.add_argument("--geometry-sample-s", type=int, default=30)
    p.add_argument("--skip-geometry", action="store_true")
    p.add_argument("--geometry-only", action="store_true")
    return p


if __name__ == "__main__":
    run_study(parser().parse_args())
