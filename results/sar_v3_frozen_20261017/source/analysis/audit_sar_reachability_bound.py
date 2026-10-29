"""冻结SAR场景的逐任务物理可达上界（只读几何，不运行调度）。

松弛：零计算、零传输时长、无限带宽与缓存、不限星间跳数、无接收竞争；数据副本
可在任意已持有卫星上保留并在后续接触继续转发。仅保留仿真器同口径的10 s采样链路
快照与北京站可见性，以及固定聚合时延。由于每项松弛都只扩大可行集，按时可能性
是任何调度方法(含混合计算与原图中继)按时完成的必要条件，与dt无关。

另给源星直达口径（`ground_compute`动作空间）：只能在源星自身可见地面时下传整图。
"""

import argparse
import hashlib
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.config import load_config
from sim.env import SimulationEnv

HORIZON_S = 300.0


def epoch_graphs(env):
    """按仿真器的10 s链路/可见性采样构造每个采样时刻的无向图与可见卫星集合。"""
    cfg = env.cfg
    stride = cfg.topology_update_steps
    if stride != cfg.ground_visibility_update_steps:
        raise ValueError("Bound assumes identical link and ground-visibility sampling")
    epochs = []
    for step in range(0, cfg.sim_steps, stride):
        links = env.topology.snapshot(step, dt=cfg.dt)
        env.vis_cache.refresh(step, env.topology.sat_recs, env.topology.t0,
                              env.ground_station_objs, env.ground_stations)
        graph = {sid: set() for sid in range(cfg.num_sats)}
        for link in links.values():
            if link.up and link.bandwidth_mbps > 0:
                graph[link.i].add(link.j)
                graph[link.j].add(link.i)
        visible = {sid for sid, gids in env.vis_cache.visible_gs_by_sat.items() if gids}
        epochs.append((step * cfg.dt, graph, visible))
    return epochs, stride * cfg.dt


def closure(holders, graph):
    frontier, reached = list(holders), set(holders)
    while frontier:
        node = frontier.pop()
        for peer in graph[node]:
            if peer not in reached:
                reached.add(peer)
                frontier.append(peer)
    return reached


def task_bound(release, source, epochs, period, post_s):
    first = int(release // period)
    holders = {source}
    relay_open = direct_open = None
    hops_at_open = None
    for start, graph, visible in epochs[first:]:
        moment = max(release, start)
        if moment - release > HORIZON_S:
            break
        if direct_open is None and source in visible:
            direct_open = moment
        holders = closure(holders, graph)
        if relay_open is None and holders & visible:
            relay_open = moment
            hops_at_open = len(holders)
        if relay_open is not None and direct_open is not None:
            break
    lower = None if relay_open is None else relay_open - release + post_s
    direct_lower = None if direct_open is None else direct_open - release + post_s
    return {"relay_ground_open_seconds": relay_open, "relay_latency_lower_bound_s": lower,
            "relay_potentially_on_time": lower is not None and lower <= HORIZON_S + 1e-9,
            "direct_ground_open_seconds": direct_open, "direct_latency_lower_bound_s": direct_lower,
            "direct_potentially_on_time": direct_lower is not None and direct_lower <= HORIZON_S + 1e-9,
            "holders_when_ground_reached": hops_at_open}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path,
                        default=Path("results/sar_ground_capacity_frozen_20261015/main_config.json"))
    parser.add_argument("--witnesses", nargs="*", type=Path, default=[],
                        help="run_sar_frozen.py records on the same config used as execution witnesses")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    raw = args.config.read_bytes()
    cfg = load_config(str(args.config))
    env = SimulationEnv(cfg)
    try:
        epochs, period = epoch_graphs(env)
    finally:
        env.close()
    post_s = cfg.ground_relay_delay_s + cfg.aggregation_delay_s
    end_s = cfg.sim_steps * cfg.dt
    tasks = []
    for index, event in enumerate(sorted(cfg.scheduled_task_arrivals, key=lambda e: e["release_step"])):
        release = event["release_step"] * cfg.dt
        if end_s - release < HORIZON_S:
            raise ValueError("Every task needs 300 s of observed geometry")
        row = {"task_id": f"task_{index}", "source_sat_id": event["source_sat_id"],
               "release_seconds": release, "target_id": event.get("target_id")}
        row.update(task_bound(release, event["source_sat_id"], epochs, period, post_s))
        tasks.append(row)
    relay_ok = [t["task_id"] for t in tasks if t["relay_potentially_on_time"]]
    direct_ok = [t["task_id"] for t in tasks if t["direct_potentially_on_time"]]
    report = {"config": str(args.config), "config_sha256": hashlib.sha256(raw).hexdigest(),
              "auditor_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "bound_kind": "relaxed_time_expanded_reachability",
              "assumptions": __doc__.strip(), "sampling_seconds": period,
              "postprocessing_seconds_unrounded": post_s, "total_tasks": len(tasks),
              "relay_upper_bound_tasks": len(relay_ok), "relay_P300_upper_bound": len(relay_ok) / len(tasks),
              "direct_upper_bound_tasks": len(direct_ok), "direct_P300_upper_bound": len(direct_ok) / len(tasks),
              "relay_infeasible_task_ids": [t["task_id"] for t in tasks if not t["relay_potentially_on_time"]],
              "tasks": tasks}
    witnesses = []
    by_id = {t["task_id"]: t for t in tasks}
    for path in args.witnesses:
        record = json.loads(path.read_bytes())
        outcomes = record["metrics"]["task_outcomes"]
        if [(o["task_id"], o["source_sat_id"], o["release_seconds"]) for o in outcomes] != [
                (t["task_id"], t["source_sat_id"], t["release_seconds"]) for t in tasks]:
            raise ValueError(f"Witness task list differs from the bounded config: {path}")
        on_time = record["on_time_task_ids"]
        contradictions = [tid for tid in on_time if not by_id[tid]["relay_potentially_on_time"]]
        if contradictions:
            raise AssertionError(f"Execution contradicts the upper bound: {path} {contradictions}")
        witnesses.append({"input": str(path), "method": record["method"], "dt": record["dt"],
                          "on_time_tasks": len(on_time),
                          "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    if witnesses:
        best = max(witnesses, key=lambda w: w["on_time_tasks"])
        report["execution_witnesses"] = witnesses
        report["best_witness"] = best
        report["gap_to_bound_tasks"] = len(relay_ok) - best["on_time_tasks"]
        report["bound_attained"] = best["on_time_tasks"] == len(relay_ok)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False))
    print(json.dumps({k: report[k] for k in ("total_tasks", "relay_upper_bound_tasks", "relay_P300_upper_bound",
                                             "direct_upper_bound_tasks", "direct_P300_upper_bound",
                                             "relay_infeasible_task_ids")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
