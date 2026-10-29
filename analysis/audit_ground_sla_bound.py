"""只读计算固定输入跳数与实际采样窗口下的乐观 SLA 完成率上界。"""

import argparse
import hashlib
import json
import math
from dataclasses import fields
from pathlib import Path
import sys

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.orbit import build_ground_station, load_satellites, parse_start_time_utc, time_grid
from sim.topology import TopologyConfig, TopologyModel
from sim.workload import ImageTiling


def compute_capacity_bound(cfg, rows, cutoff, visible_by_sat):
    """忽略竞争、带宽、缓存、接触保护和结果跳数，只保留逐跳至少一时间步。"""
    dt = cfg["dt"]
    release = min(row["release_seconds"] for row in rows)
    source_release = {}
    for row in rows:
        src = row["source_sat_id"]
        source_release[src] = min(source_release.get(src, math.inf), row["release_seconds"])
    top = {**cfg["topology"], "num_sats": cfg["num_sats"], "seed": cfg["seed"],
           "min_elevation_deg": 0}
    model = TopologyModel(TopologyConfig(**{field.name: top[field.name]
                                          for field in fields(TopologyConfig) if field.name in top}))
    graph_cache = {}

    def graph_at(step):
        sample = step // cfg["topology_update_steps"] * cfg["topology_update_steps"]
        if sample not in graph_cache:
            graph = {sid: set() for sid in range(cfg["num_sats"])}
            for link in model.snapshot(sample, dt=dt).values():
                if link.up and link.bandwidth_mbps > 0:
                    graph[link.i].add(link.j)
                    graph[link.j].add(link.i)
            graph_cache[sample] = graph
        return graph_cache[sample]

    try:
        start_step = round(release / dt)
        end_step = math.floor(cutoff / dt + 1e-9)
        first_input = {}
        last_result_ready = {}
        result_possible_next = set()
        # 逆向时间展开允许任意合法未来中继，故比实际贪心公共路由更乐观。
        for step in range(end_step - 1, start_step - 1, -1):
            graph = graph_at(step)
            ground = set(np.flatnonzero(visible_by_sat[step // cfg["ground_visibility_update_steps"]]))
            possible = ground | result_possible_next
            possible |= {sid for sid, peers in graph.items() if peers & result_possible_next}
            for sid in possible:
                last_result_ready.setdefault(sid, step * dt)
            result_possible_next = possible
        direct_ground_possible = False
        for step in range(start_step, end_step):
            graph = graph_at(step)
            for src, src_release in source_release.items():
                if step * dt < src_release:
                    continue
                visible = visible_by_sat[step // cfg["ground_visibility_update_steps"]]
                direct_ground_possible |= bool(visible[src])
                reached = {src}
                frontier = {src}
                first_input[src] = min(first_input.get(src, math.inf), step * dt)
                for hops in range(1, cfg["input_relay_max_hops"] + 1):
                    frontier = {peer for sid in frontier for peer in graph[sid]} - reached
                    reached |= frontier
                    for sid in frontier:
                        arrival = (step + hops) * dt
                        first_input[sid] = min(first_input.get(sid, math.inf), arrival)
        rates = cfg.get("satellite_compute_rates") or [cfg["compute_rate"]] * cfg["num_sats"]
        processors = []
        for sid, arrival in sorted(first_input.items()):
            latest = last_result_ready.get(sid)
            capacity = max(0, (latest - arrival) * rates[sid]) if latest is not None else 0
            processors.append({"sat_id": sid, "earliest_input_seconds": arrival,
                               "latest_result_ready_seconds": latest,
                               "compute_capacity_upper_bound": capacity})
        onboard = sum(row["compute_capacity_upper_bound"] for row in processors)
        # 原始输入仅可由源星直达地面；一旦有机会，赠予全部地面算力直至最迟 SLA。
        ground_capacity = (max(row["release_seconds"] + row["sla_seconds"] for row in rows) - release) * sum(
            gs["compute_rate"] for gs in cfg["ground_stations"]) if direct_ground_possible else 0
        work = ImageTiling.from_dict(cfg["image_tiling"]).manifest()["total_compute_cost"]
        capacity = onboard + ground_capacity
        return {"task_ids": [row["task_id"] for row in rows], "earliest_release_seconds": release,
                "last_usable_ground_arrival_seconds": cutoff, "work_per_task": work,
                "total_required_work": len(rows) * work, "processors": processors,
                "direct_source_ground_input_possible": direct_ground_possible,
                "onboard_capacity_upper_bound": onboard,
                "ground_capacity_upper_bound": ground_capacity,
                "total_capacity_upper_bound": capacity,
                "on_time_task_count_upper_bound": min(len(rows), math.floor((capacity + 1e-9) / work))}
    finally:
        model.close()


def ground_samples(cfg):
    dt = cfg["dt"]
    stride = cfg["ground_visibility_update_steps"]
    offsets = np.arange(0, cfg["sim_steps"], stride) * dt
    times = time_grid(parse_start_time_utc(cfg["topology"]["start_time_utc"]), offsets)
    satellites = load_satellites(cfg["topology"]["tle_lines"][:cfg["num_sats"]])
    visible_by_sat = np.zeros((len(offsets), cfg["num_sats"]), dtype=bool)
    for station_cfg in cfg["ground_stations"]:
        station = build_ground_station(station_cfg["lat_deg"], station_cfg["lon_deg"],
                                       station_cfg.get("alt_m", 0))
        for sid, sat in enumerate(satellites):
            visible_by_sat[:, sid] |= (sat - station).at(times).altaz()[0].degrees >= station_cfg["min_elevation_deg"]
    return offsets, visible_by_sat


def classify_window_relaxation(outcomes, windows, observation_end_s, sla):
    """授予零耗时与任意结果放置，仅保留至少一次星地下传机会这一必要条件。"""
    if not outcomes or not math.isfinite(sla) or sla <= 0:
        raise ValueError("Requires nonempty outcomes and a positive finite SLA")
    if len({row["task_id"] for row in outcomes}) != len(outcomes):
        raise ValueError("Duplicate task IDs")
    tasks = []
    for row in outcomes:
        release = row["release_seconds"]
        if release < 0 or not math.isfinite(release) or release + sla >= observation_end_s:
            raise ValueError("Requires visibility coverage beyond every task's SLA endpoint")
        # 窗口右端为开区间，SLA右端为闭区间；恰好截止才开窗也先赠予可行性。
        openings = [max(release, window["start_s"]) for window in windows
                    if window["end_s"] > release and window["start_s"] < observation_end_s]
        opening = min(openings) if openings else None
        possible = opening is not None and opening <= release + sla + 1e-9
        latency = row["latency_seconds"]
        observed = row["status"] == "completed" and latency is not None and latency <= sla
        if observed and not possible:
            raise AssertionError(f"Observed success contradicts window bound: {row['task_id']}")
        tasks.append({"task_id": row["task_id"], "source_sat_id": row["source_sat_id"],
                      "release_seconds": release, "sla_seconds": sla,
                      "next_global_ground_open_seconds": opening,
                      "delivery_latency_lower_bound_seconds": opening - release if opening is not None else None,
                      "no_contact_through_seconds": None if opening is not None else observation_end_s,
                      "potentially_on_time": possible, "observed_on_time": observed,
                      "observed_latency_seconds": latency})
    return tasks


def audit_window_relaxation(path, sla=300):
    raw = path.read_bytes()
    record = json.loads(raw)
    cfg = record["metrics"]["experiment"]["config"]
    if len(cfg["topology"].get("tle_lines", [])) < cfg["num_sats"]:
        raise ValueError("Window bound requires TLE records for all satellites")
    offsets, visible_by_sat = ground_samples(cfg)
    visible = visible_by_sat.any(axis=1)
    edges = np.flatnonzero(np.diff(np.r_[False, visible, False]))
    end = cfg["sim_steps"] * cfg["dt"]
    windows = [{"start_s": float(offsets[start]),
                "end_s": float(offsets[stop]) if stop < len(offsets) else end}
               for start, stop in zip(edges[::2], edges[1::2])]
    tasks = classify_window_relaxation(record["metrics"]["task_outcomes"], windows, end, sla)
    feasible = sum(row["potentially_on_time"] for row in tasks)
    observed = sum(row["observed_on_time"] for row in tasks)
    return {"input": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
            "method": record["method"], "case": record["case"], "sla_seconds": sla,
            "bound_kind": "offline_global_ground_window_relaxation",
            "assumptions": "Conditional on the simulator's sampled-and-held ground visibility. "
                           "Every task is available at all satellites at its release, with zero computation, "
                           "zero transfer/postprocessing duration and unlimited memory/channel capacity. "
                           "A ground contact at the exact SLA endpoint is granted feasibility. "
                           "All tasks are retained, independently of observed outcomes. This is a necessary-condition "
                           "upper bound for both onboard and mixed ground execution, not an achievable schedule.",
            "relaxed_offline_problem": "max sum(x_i), 0 <= x_i <= a_i, "
                                       "a_i=1 iff a sampled ground window intersects [release_i, release_i+SLA]; "
                                       "closed-form optimum sum(a_i), no integer solver required",
            "visibility_sample_seconds": cfg["dt"] * cfg["ground_visibility_update_steps"],
            "visibility_sha256": hashlib.sha256(visible_by_sat.tobytes()).hexdigest(),
            "source_sha256": record["metrics"]["experiment"]["source_sha256"],
            "observation_end_seconds": end, "total_tasks": len(tasks),
            "potentially_on_time_tasks": feasible, "p_sla_upper_bound": feasible / len(tasks),
            "observed_on_time_tasks": observed, "observed_p_sla": observed / len(tasks),
            "gap_upper_bound_percentage_points": 100 * (feasible - observed) / len(tasks),
            "global_ground_windows": windows, "group_capacity_constraints": [], "tasks": tasks}


def attach_execution_witnesses(reports, paths, sla):
    """只选择完整日运行，不拼接各算法最好的单任务结果。"""
    from analysis.evaluate_literature import common_context

    by_case = {report["case"]["id"]: report for report in reports}
    references = {case: json.loads(Path(report["input"]).read_text())
                  for case, report in by_case.items()}
    witnesses = {case: [] for case in by_case}
    for path in dict.fromkeys([Path(report["input"]) for report in reports] + paths):
        raw = path.read_bytes()
        record = json.loads(raw)
        case = record["case"]["id"]
        if (case not in by_case or common_context(record) != common_context(references[case])
                or record["metrics"]["experiment"]["config"] != references[case]["metrics"]["experiment"]["config"]):
            raise ValueError(f"Witness is outside the frozen case's execution context: {path}")
        report = by_case[case]
        reference_tasks = {row["task_id"]: row for row in report["tasks"]}
        outcomes = record["metrics"]["task_outcomes"]
        if ({row["task_id"] for row in outcomes} != set(reference_tasks)
                or len(outcomes) != len(reference_tasks)):
            raise ValueError(f"Witness task set mismatch: {path}")
        on_time = []
        for row in outcomes:
            ref = reference_tasks[row["task_id"]]
            if (row["source_sat_id"], row["release_seconds"]) != (ref["source_sat_id"], ref["release_seconds"]):
                raise ValueError(f"Witness task release mismatch: {path}")
            if row["status"] == "completed" and row["latency_seconds"] is not None and row["latency_seconds"] <= sla:
                if not ref["potentially_on_time"]:
                    raise AssertionError(f"Execution contradicts upper bound: {path}, {row['task_id']}")
                on_time.append(row["task_id"])
        witnesses[case].append({"input": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
                                "method": record["method"], "on_time_tasks": len(on_time),
                                "on_time_task_ids": on_time})
    for case, rows in witnesses.items():
        best = max(rows, key=lambda row: row["on_time_tasks"])
        report = by_case[case]
        report["complete_execution_witnesses"] = rows
        report["best_complete_execution_witness"] = best
        report["offline_optimal_count_certified"] = best["on_time_tasks"] == report["potentially_on_time_tasks"]


def audit(path, sla=300):
    raw = path.read_bytes()
    record = json.loads(raw)
    cfg = record["metrics"]["experiment"]["config"]
    if cfg.get("ground_input_relay", False):
        raise ValueError("Legacy capacity bound excludes raw ground relaying; use --window-only")
    dt = cfg["dt"]
    stride = cfg["ground_visibility_update_steps"]
    offsets, visible_by_sat = ground_samples(cfg)
    visible = visible_by_sat.any(axis=1)
    edges = np.flatnonzero(np.diff(np.r_[False, visible, False]))
    windows = [{"start_s": float(offsets[start]),
                "end_s": float(offsets[end]) if end < len(offsets) else cfg["sim_steps"] * dt}
               for start, end in zip(edges[::2], edges[1::2])]
    post = math.ceil((cfg["ground_relay_delay_s"] + cfg["aggregation_delay_s"]) / dt) * dt
    tasks = []
    for row in record["metrics"]["task_outcomes"]:
        release = row["release_seconds"]
        first = int(release // (stride * dt))
        available = np.flatnonzero(visible[first:])
        opening = max(release, float(offsets[first + available[0]])) if len(available) else None
        lower = opening - release + dt + post if opening is not None else None
        tasks.append({"task_id": row["task_id"], "source_sat_id": row["source_sat_id"],
                      "release_seconds": release, "sla_seconds": sla,
                      "next_global_ground_open_seconds": opening,
                      "delivery_latency_lower_bound_seconds": lower,
                      "potentially_on_time": lower is not None and lower <= sla,
                      "observed_latency_seconds": row["latency_seconds"]})
    feasible = sum(t["potentially_on_time"] for t in tasks)
    constraints = []
    if (cfg["pipeline_mode"] == "distributed" and cfg["execution_commitment"]
            and cfg.get("image_tiling")):
        groups = {}
        for row in tasks:
            if not row["potentially_on_time"]:
                continue
            deadline = row["release_seconds"] + sla
            usable = [window for window in windows
                      if window["end_s"] > row["release_seconds"]
                      and max(window["start_s"], row["release_seconds"]) + dt + post <= deadline]
            if usable and usable[-1]["end_s"] < deadline - post:
                groups.setdefault(usable[-1]["end_s"], []).append(row)
        for cutoff, rows in groups.items():
            # 仅对已有失败的窗口组收紧上界；其他组保持乐观，不据此调度。
            if all(row["observed_latency_seconds"] is not None
                   and row["observed_latency_seconds"] <= sla for row in rows):
                continue
            constraint = compute_capacity_bound(cfg, rows, cutoff, visible_by_sat)
            constraints.append(constraint)
            feasible -= len(rows) - constraint["on_time_task_count_upper_bound"]
    return {"input": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
            "method": record["method"], "case": record["case"], "sla_seconds": sla,
            "assumptions": "Conditional on this simulator, fixed image work, committed input assignments and configured input hop limit. "
                           "Global ground-window bounds optimistically give zero processing and arbitrary result placement. "
                           "For disjoint groups sharing their last usable ground window, capacity bounds retain earliest current-path "
                           "input arrival and latest delivery through an optimistic time-expanded result graph. "
                           "They ignore bandwidth contention, memory, unrelated tasks, packet indivisibility, relay guard, visited nodes "
                           "and result hop limits. Ground computation is zero only when no source has a direct input window; otherwise "
                           "all ground servers are granted full capacity through the latest task deadline. "
                           "The bound concerns maximum count, not which member of a group must fail. "
                           "It does not apply to ground_relay, whose multi-hop raw-input-to-ground paths are a different action space, "
                           "and is not universal scheduling optimality.",
            "total_tasks": len(tasks), "potentially_on_time_tasks": feasible,
            "p_sla_upper_bound": feasible / len(tasks) if tasks else 0,
            "global_ground_windows": windows, "group_capacity_constraints": constraints, "tasks": tasks}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--sla", type=float, default=300)
    parser.add_argument("--window-only", action="store_true",
                        help="Use only the optimistic ground-window relaxation, valid with mixed ground relaying")
    parser.add_argument("--witness-records", nargs="+", default=[],
                        help="Additional complete same-case executions to certify attainable counts (window-only)")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if args.witness_records and not args.window_only:
        parser.error("--witness-records requires --window-only")
    auditor = audit_window_relaxation if args.window_only else audit
    reports = [auditor(Path(path), args.sla) for path in args.inputs]
    if len({report["case"]["id"] for report in reports}) != len(reports):
        raise ValueError("Supply one record per distinct case")
    if args.window_only:
        attach_execution_witnesses(reports, [Path(path) for path in args.witness_records], args.sla)
    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"reports": reports,
                                 "auditor_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                                 "pooled_task_count_upper_bound": sum(r["potentially_on_time_tasks"] for r in reports),
                                 "pooled_total_tasks": sum(r["total_tasks"] for r in reports),
                                 "equal_case_p_sla_upper_bound": sum(r["p_sla_upper_bound"] for r in reports) / len(reports)},
                                indent=2, allow_nan=False))
    for report in reports:
        print(json.dumps({"case": report["case"], "upper_bound": report["p_sla_upper_bound"],
                          "blocked": [t for t in report["tasks"] if not t["potentially_on_time"]],
                          "group_capacity_constraints": report["group_capacity_constraints"]},
                         ensure_ascii=False))


if __name__ == "__main__":
    main()
