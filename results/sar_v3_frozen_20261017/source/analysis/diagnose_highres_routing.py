"""只读回放原始中继，记录路径选择；全部任务完成后停止，仅用于诊断。"""

import argparse
import ast
from collections import Counter
from dataclasses import asdict
import hashlib
import inspect
import json
import math
from pathlib import Path
import sys
import time

import networkx as nx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import apply_execution_mode
from sim.config import load_config
from sim.env import SimulationEnv
import sim.pipeline.distributed as distributed
from sim.reproducibility import runtime_metadata
from sim.routing import delivery_route


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


class RouteReturns(ast.NodeTransformer):
    def visit_Return(self, node):
        # 只插入返回值观察，原函数分支、顺序和返回表达式保持不变。
        return [ast.Assign(targets=[ast.Name(id="_diagnostic_return", ctx=ast.Store())],
                           value=node.value or ast.Constant(None)),
                ast.Expr(value=ast.Call(func=ast.Name(id="_capture_return", ctx=ast.Load()),
                                       args=[ast.Call(func=ast.Name(id="locals", ctx=ast.Load()), args=[], keywords=[]),
                                             ast.Name(id="_diagnostic_return", ctx=ast.Load())], keywords=[])),
                ast.Return(value=ast.Name(id="_diagnostic_return", ctx=ast.Load()))]


def instrumented_route(capture):
    source = inspect.getsource(delivery_route)
    tree = ast.fix_missing_locations(RouteReturns().visit(ast.parse(source)))
    namespace = {**delivery_route.__globals__, "_capture_return": capture}
    exec(compile(tree, "<diagnostic_delivery_route>", "exec"), namespace)
    return namespace["delivery_route"], hashlib.sha256(source.encode()).hexdigest()


class RoutingTrace:
    def __init__(self):
        self.path_ids = {}
        self.routes = []
        self.route_outcomes = Counter()
        self.snapshots = []
        self.last_snapshot = -1

    def capture(self, values, returned):
        env = values["env"]
        best = values.get("best")
        self.route_outcomes["forward" if returned else "no_path" if best is None else "hold_for_window"] += 1
        if returned is None:
            return
        visited = values["visited"]
        identity = id(visited)
        if identity not in self.path_ids:
            self.path_ids[identity] = next((tile.tile_id for tile in env.tiles.values()
                                            if tile.raw_relay_path is visited), None)
        tid = self.path_ids[identity]
        if tid is None:
            raise AssertionError("Expected a raw-relay path")
        source, kind, target = values["source"], *returned
        relevant_backlog = (sum(tr.remaining_mb for tr in env.transfers
                                if tr.src == source and tr.dst == target) if kind == "sat" else
                            sum(tr.remaining_mb for tr in env.ground_tile_transfers + env.downlink_transfers
                                if tr.src_sat == source and tr.gs_id == target))
        record = {
            "seconds": env.time * env.cfg.dt, "tile_id": tid, "source": source,
            "size_mb": values["size_mb"], "action": [kind, target], "visited": list(visited),
            "remaining_hops": values["remaining_hops"], "selected_edge_backlog_mb": relevant_backlog,
            "predicted_ground_finish_seconds": env.time * env.cfg.dt + best[0],
            "predicted_ground_sender": best[2], "predicted_ground_station": best[3],
            "predicted_satellite_path": best[4],
            "predicted_ground_window": env.vis_cache.windows_at(best[2], env.time).get(best[3]),
        }
        if tid == "task_1_tile_293" and env.time * env.cfg.dt < 200:
            record["diagnostic_graph"] = [
                {"source": src, "target": dst, "weight": data["weight"],
                 "contact_end": data["contact_end"] if math.isfinite(data["contact_end"]) else None}
                for src, dst, data in values["graph"].edges(data=True)]
            paths = values.get("paths")
            if paths is None:
                # 修复后的路由没有无约束路径变量，仅为诊断对照重新计算。
                _, paths = nx.single_source_dijkstra(values["graph"], source)
            record["unconstrained_paths"] = paths
            record["all_ground_windows"] = {str(sid): env.vis_cache.windows_at(sid, env.time)
                                            for sid in env.satellites}
        self.routes.append(record)

    def observe(self, env):
        bucket = int(env.time * env.cfg.dt / 10)
        if bucket <= self.last_snapshot or not env.tasks:
            return
        self.last_snapshot = bucket
        self.snapshots.append({
            "seconds": env.time * env.cfg.dt,
            "completed_tasks": env.metrics.completed_tasks,
            "states": dict(Counter(tile.state.value for tile in env.tiles.values())),
            "pending_locations": dict(Counter(str(tile.location) for tile in env.tiles.values()
                                              if tile.state.value in ("QUEUED", "READY"))),
            "pending_hops": dict(Counter(str(len(tile.raw_relay_path) - 1) for tile in env.tiles.values()
                                         if tile.state.value in ("QUEUED", "READY"))),
            "ground_transfers": len(env.ground_tile_transfers), "isl_transfers": len(env.transfers),
            "ground_windows": {str(sid): env.vis_cache.windows_at(sid, env.time)
                               for sid in sorted({tile.location for tile in env.tiles.values()
                                                  if tile.state.value in ("QUEUED", "READY")})},
        })


def cadence_wrapper(route, env, period_steps):
    def wrapped(links):
        if env.time % period_steps == 0:
            route(links)
    return wrapped


def diagnose(config_path, output, raw_cadence_s=None, max_seconds=None):
    cfg = apply_execution_mode(load_config(str(config_path)), "ground_relay")
    trace = RoutingTrace()
    observed_route, route_sha = instrumented_route(trace.capture)
    original_route = distributed.delivery_route
    source = runtime_metadata()
    env = SimulationEnv(cfg)
    if raw_cadence_s is not None:
        period_steps = round(raw_cadence_s / cfg.dt)
        if period_steps < 1 or not math.isclose(period_steps * cfg.dt, raw_cadence_s):
            env.close()
            raise ValueError("Raw routing cadence must be a positive multiple of dt")
        env._dist_pipeline.route_raw_tiles = cadence_wrapper(
            env._dist_pipeline.route_raw_tiles, env, period_steps)
    started = time.monotonic()
    distributed.delivery_route = observed_route
    try:
        last_arrival = max(event["release_step"] for event in cfg.scheduled_task_arrivals)
        for _ in range(cfg.sim_steps):
            env.step([])
            trace.observe(env)
            if env.time > last_arrival and env.metrics.completed_tasks == len(cfg.scheduled_task_arrivals):
                break
            if max_seconds is not None and env.time * cfg.dt >= max_seconds:
                break
        if runtime_metadata()["source_sha256"] != source["source_sha256"]:
            raise RuntimeError("Core source changed during diagnostic replay")
        metrics = env.summary()
        tiles = [{"tile_id": tile.tile_id, "task_id": tile.parent_task_id,
                  "state": tile.state.value, "location": tile.location,
                  "size_mb": tile.data_size_mb, "path": tile.raw_relay_path,
                  "timestamps_seconds": {key: None if value is None else value * cfg.dt
                                         for key, value in asdict(tile.timestamps).items()}}
                 for tile in env.tiles.values()]
        report = {
            "scope": "diagnostic replay; same core transitions; may stop after all tasks complete or a diagnostic time limit; not a four-hour evaluation",
            "stop_reason": "all_scheduled_tasks_completed" if env.metrics.completed_tasks == len(cfg.scheduled_task_arrivals)
            else "diagnostic_time_limit" if max_seconds is not None else "simulation_horizon",
            "config_path": str(config_path), "config": asdict(cfg), **source,
            "routing_source_sha256": route_sha, "elapsed_wall_seconds": time.monotonic() - started,
            "raw_route_cadence_seconds": cfg.dt if raw_cadence_s is None else raw_cadence_s,
            "cadence_wrapper_sha256": hashlib.sha256(inspect.getsource(cadence_wrapper).encode()).hexdigest()
            if raw_cadence_s is not None else None,
            "elapsed_sim_seconds": env.time * cfg.dt,
            "task_outcomes": metrics["task_outcomes"],
            "network_sent_mb": {k: v for k, v in metrics["network"]["sent_mb"].items() if v > 0},
            "hop_histogram": dict(Counter(str(len(tile["path"]) - 1) for tile in tiles)),
            "path_histogram": dict(Counter("->".join(map(str, tile["path"])) for tile in tiles)),
            "route_outcomes": dict(trace.route_outcomes),
            "routes": trace.routes, "snapshots": trace.snapshots, "tiles": tiles,
        }
        write_json(output, report)
        print(json.dumps({key: report[key] for key in ("config_path", "elapsed_wall_seconds", "elapsed_sim_seconds",
                                                       "task_outcomes", "hop_histogram", "route_outcomes")}), flush=True)
    finally:
        distributed.delivery_route = original_route
        env.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--configs", nargs="+", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--raw-cadence-seconds", type=float)
    parser.add_argument("--max-seconds", type=float)
    args = parser.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for config in args.configs:
        path = Path(config)
        suffix = f"_rawcadence{args.raw_cadence_seconds:g}s" if args.raw_cadence_seconds is not None else ""
        if args.max_seconds is not None:
            suffix += f"_until{args.max_seconds:g}s"
        diagnose(path, out / f"{path.parent.name}{suffix}.json", args.raw_cadence_seconds, args.max_seconds)


if __name__ == "__main__":
    main()
