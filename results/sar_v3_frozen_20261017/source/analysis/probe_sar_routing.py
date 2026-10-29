"""固定开发前缀上的中继组件消融；保留配置、源码哈希、逐任务配对和路径事件。"""

import argparse
from copy import deepcopy
from dataclasses import asdict
import gzip
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_sar_frozen as runner
from sim.reproducibility import runtime_metadata
from sim.scheduler.input_routing import _hop_arrival
import sim.scheduler.task_choice as task_choice
import sim.temporal_routing as temporal_routing


def independent_input_arrivals(plan, paths, sizes, dt):
    """消融：同轮输入不互相排队，仅保留总容量约束；不能当作可执行保证。"""
    arrivals = {}
    for tid, path in paths.items():
        ready = 0.0
        for index in path:
            ready = _hop_arrival(plan.contacts[index], ready, sizes[tid], dt, future=True)
            if ready is None:
                return None
        arrivals[tid] = ready
    try:
        for tid, path in paths.items():
            plan.reserve(path, sizes[tid])
    except ValueError:
        return None
    return arrivals


class RouteObserver(runner.Observer):
    def on_start(self, env):
        super().on_start(env)
        self.last_paths = {}
        self.events = []

    def on_step(self, env, step_idx, actions):
        super().on_step(env, step_idx, actions)
        for tid, tile in env.tiles.items():
            signature = (tuple(tile.raw_relay_path), tuple(tile.result_path), tile.state.value)
            if signature == self.last_paths.get(tid):
                continue
            self.last_paths[tid] = signature
            self.events.append({"time_s": env.time * env.cfg.dt, "tile_id": tid,
                                "location": tile.location, "raw_path": list(tile.raw_relay_path),
                                "result_path": list(tile.result_path), "state": tile.state.value,
                                "ground_windows": env.vis_cache.windows_at(tile.location, env.time)})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--method", required=True, choices=runner.METHODS)
    parser.add_argument("--config", type=Path, default=runner.FROZEN / "main_config.json")
    parser.add_argument("--routing", choices=("snapshot", "temporal"), help="default: inherit config")
    parser.add_argument("--max-hops", type=int, help="default: inherit config")
    parser.add_argument("--stop-seconds", type=float, default=1040)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--capture-times", type=float, nargs="*", default=[])
    parser.add_argument("--route-quote-times", type=float, nargs="*", default=[])
    parser.add_argument("--input-quote-mode", choices=("batch", "independent"), default="batch")
    args = parser.parse_args()
    if args.input_quote_mode != "batch" and args.method != "cgs_task_choice":
        raise ValueError("Input quote ablation is only defined for cgs_task_choice")
    if args.out.exists():
        raise ValueError("Use a new output directory; completed probes are immutable")
    source_path = args.config
    base = json.loads(source_path.read_bytes())
    cfg = runner.derive_config(base, base["dt"], args.stop_seconds)
    if args.routing is not None:
        cfg["relay_routing"] = args.routing
    if args.max_hops is not None:
        cfg["relay_max_hops"] = args.max_hops
    args.out.mkdir(parents=True)
    (args.out / "config.json").write_text(json.dumps(cfg, indent=2))
    # 使用标准批量入口的独立工时/字节/窗口计数审计，额外观察器不改环境。
    observers = []
    def factory(*a, **kw):
        observer = RouteObserver(*a, **kw)
        observers.append(observer)
        return observer
    original = runner.Observer
    original_policy_factory = runner.simulator.make_policy
    original_batch_arrivals = task_choice.batch_arrivals
    original_temporal_route = temporal_routing.temporal_delivery_route
    original_contact_plan = temporal_routing.ContactPlan
    route_quotes = []
    route_quote_keys = set()
    route_context = {}

    class QuotedContactPlan(original_contact_plan):
        def delivery(self, *a, **kw):
            route = super().delivery(*a, **kw)
            if route_context:
                route_quotes.append({**route_context,
                                     "arrival_delay_s": route[0] if route else None,
                                     "contacts": [asdict(self.contacts[i]) for i in route[1]] if route else []})
            return route

    def quoted_temporal_route(env, source, size_mb, links, visited, ready_delay=0.0, **kw):
        now = env.time * env.cfg.dt
        key = (now, source, size_mb, tuple(visited), ready_delay, kw.get("target_gs_id"))
        capture = any(abs(now - value) < 1e-8 for value in args.route_quote_times)
        if capture and key not in route_quote_keys:
            route_quote_keys.add(key)
            route_context.update(time_s=now, source=source, size_mb=size_mb,
                                 visited=list(visited), ready_delay_s=ready_delay,
                                 target_gs_id=kw.get("target_gs_id"))
        try:
            return original_temporal_route(env, source, size_mb, links, visited, ready_delay, **kw)
        finally:
            route_context.clear()

    if args.route_quote_times:
        temporal_routing.ContactPlan = QuotedContactPlan
        temporal_routing.temporal_delivery_route = quoted_temporal_route
    if args.input_quote_mode == "independent":
        task_choice.batch_arrivals = independent_input_arrivals
    decisions = []
    snapshots = {}

    def policy_factory(*a, **kw):
        policy = original_policy_factory(*a, **kw)
        if args.method in ("cgs_task_gate", "cgs_task_choice") or args.capture_times:
            select = policy.select_actions
            def traced_select(state):
                now = state.time * state.config["dt"]
                if any(abs(now - value) < 1e-8 for value in args.capture_times):
                    snapshots[str(now)] = asdict(state)
                actions = select(state)
                if actions and args.method in ("cgs_task_gate", "cgs_task_choice"):
                    decisions.append({"time_seconds": state.time * state.config["dt"],
                                      "prediction": deepcopy(policy.last_prediction)})
                return actions
            policy.select_actions = traced_select
        return policy

    runner.Observer = factory
    runner.simulator.make_policy = policy_factory
    runtime = runtime_metadata()
    try:
        record = runner.run_one(cfg, args.method, runtime["source_sha256"],
                                runner.sha256_bytes(Path(runner.__file__).read_bytes()))
    finally:
        runner.Observer = original
        runner.simulator.make_policy = original_policy_factory
        task_choice.batch_arrivals = original_batch_arrivals
        temporal_routing.ContactPlan = original_contact_plan
        temporal_routing.temporal_delivery_route = original_temporal_route
    record.update({"scope": "development prefix diagnostic; not a main scenario ranking",
                   "routing": cfg.get("relay_routing", "snapshot"), "max_hops": cfg["relay_max_hops"],
                   "input_quote_mode": args.input_quote_mode,
                   "source_config_sha256": runner.sha256_bytes(source_path.read_bytes()),
                   "config_sha256": runner.digest(cfg), **runtime,
                   "probe_sha256": runner.sha256_bytes(Path(__file__).read_bytes())})
    observer = observers[0]
    final = []
    for tile in observer.env.tiles.values():
        final.append({"tile_id": tile.tile_id, "state": tile.state.value, "location": tile.location,
                      "raw_path": tile.raw_relay_path, "result_path": tile.result_path,
                      "completed_windows": tile.completed_compute_windows})
    record["final_tiles"] = final
    record["decision_quotes"] = decisions
    record["route_quotes"] = route_quotes
    if snapshots:
        (args.out / "public_states.json").write_text(json.dumps(snapshots, indent=2, allow_nan=False))
    if args.reference:
        reference = json.loads(args.reference.read_bytes())
        record["paired"] = runner.paired_on_time(reference, record)
        record["outcomes_identical_to_reference"] = reference["metrics"]["task_outcomes"] == record["metrics"]["task_outcomes"]
        record["reference_sha256"] = runner.sha256_bytes(args.reference.read_bytes())
    with gzip.open(args.out / "route_events.json.gz", "wt", encoding="utf-8") as handle:
        json.dump(observer.events, handle, allow_nan=False)
    (args.out / "record.json").write_text(json.dumps(record, indent=2, allow_nan=False))
    print(json.dumps({key: record[key] for key in ("method", "routing", "max_hops", "P300", "R300_seconds", "wall_seconds")}), flush=True)


if __name__ == "__main__":
    main()
