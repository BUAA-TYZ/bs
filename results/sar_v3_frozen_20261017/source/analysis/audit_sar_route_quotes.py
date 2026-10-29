"""对已保存公开状态做报价消融，不重放未来任务，不改变仿真执行。"""

import argparse
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.entities import EnvState
from sim.scheduler.delivery_guard import CGSDeliveryGuard
from sim.scheduler.literature import contact_plan, ground_node
from sim.scheduler.task_gate import whole_service
from sim.scheduler.contact_graph import quantize


def load_state(data):
    data = deepcopy(data)
    for key in ("satellites", "neighbors", "ground_options"):
        data[key] = {int(k): v for k, v in data[key].items()}
    for satellite in data["satellites"].values():
        satellite["isl_backlog_mb"] = {int(k): v for k, v in satellite.get("isl_backlog_mb", {}).items()}
    return EnvState(**data)


def quote(state, task, guard, pipeline):
    plan = CGSDeliveryGuard()._contact_plan(state) if guard else contact_plan(state)
    tiles = [tile for tid, tile in sorted(state.pending_task_tiles[task].items())]
    source = tiles[0]["location"]
    ready = state.satellites[source]["committed_compute_s"]
    ground = {ground_node(gid) for gid in state.ground_stations}
    batches = [[tile] for tile in tiles] if pipeline else [tiles]
    finishes, paths = [], []
    for group in batches:
        ready += whole_service(state, group, source)
        size = sum(tile["result_size_mb"] for tile in group)
        route = plan.delivery(source, ground, quantize(ready, state.config["dt"]), size,
                              dt=state.config["dt"], max_hops=state.config["relay_max_hops"] + 1)
        if route is None:
            return {"latency_seconds": None, "no_route": True}
        plan.reserve(route[1], size)
        finishes.append(route[0])
        paths.append([asdict(plan.contacts[index]) for index in route[1]])
    age = tiles[0]["waiting_time"]
    tail = max(range(len(finishes)), key=finishes.__getitem__)
    return {"latency_seconds": age + finishes[tail] + state.config["postprocessing_delay_s"],
            "compute_tail_seconds": ready, "tail_contact_path": paths[tail]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--states", type=Path, required=True)
    parser.add_argument("--time", default="626.0")
    parser.add_argument("--task", default="task_2")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    state = load_state(json.loads(args.states.read_text())[args.time])
    corrected = deepcopy(state)
    conflicts = []
    scan = state.config["contact_scan_s"]
    for pair, link in corrected.links.items():
        windows = []
        for start, duration in link.get("contact_windows", []):
            if start == 0 and not link["up"]:
                conflicts.append({"pair": pair, "duration": duration})
                if duration <= scan:
                    continue
                start, duration = scan, duration - scan
            windows.append((start, duration))
        link["contact_windows"] = windows
    rows = []
    for reconcile, observed in ((False, state), (True, corrected)):
        for guard in (False, True):
            for pipeline in (False, True):
                rows.append({"reconcile_future": reconcile, "guard": guard, "pipeline": pipeline,
                             **quote(observed, args.task, guard, pipeline)})
    report = {"task": args.task, "time_seconds": float(args.time), "conflicting_windows": conflicts,
              "scope": "public-state quote ablation, not executed outcomes", "rows": rows}
    if args.out.exists():
        raise ValueError("Refusing to replace an existing audit")
    args.out.write_text(json.dumps(report, indent=2, allow_nan=False))
    print(json.dumps([{k: row[k] for k in ("reconcile_future", "guard", "pipeline", "latency_seconds")}
                      for row in rows], indent=2))


if __name__ == "__main__":
    main()
