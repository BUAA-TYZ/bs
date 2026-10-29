"""有限跳数的零代价、多副本可达上界；不改变仿真权限或预知在线决策。"""

import argparse
import hashlib
import heapq
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.audit_sar_reachability_bound import epoch_graphs
from sim.config import load_config
from sim.env import SimulationEnv


def hop_bound(release, source, epochs, period, post, budget):
    best = {source: 0}
    for start, graph, visible in epochs[int(release // period):]:
        moment = max(start, release)
        if moment - release + post > 300:
            break
        queue = [(hops, node) for node, hops in best.items()]
        heapq.heapify(queue)
        while queue:
            hops, node = heapq.heappop(queue)
            if best[node] != hops or hops >= budget:
                continue
            for peer in graph[node]:
                if hops + 1 < best.get(peer, budget + 1):
                    best[peer] = hops + 1
                    heapq.heappush(queue, (hops + 1, peer))
        reached = visible & best.keys()
        if reached:
            return {"potentially_on_time": True, "latency_lower_bound_s": moment - release + post,
                    "hops_at_first_ground": min(best[node] for node in reached)}
    return {"potentially_on_time": False, "latency_lower_bound_s": None, "hops_at_first_ground": None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        raise ValueError("Use a new bound artifact")
    cfg = load_config(str(args.config))
    if not cfg.execution_commitment or not cfg.result_relay:
        raise ValueError("Requires one committed compute placement and result relay")
    env = SimulationEnv(cfg)
    try:
        epochs, period = epoch_graphs(env)
    finally:
        env.close()
    budgets = {"local_or_ground": cfg.relay_max_hops,
               "one_onboard_placement": cfg.input_relay_max_hops + cfg.relay_max_hops}
    post = cfg.aggregation_delay_s + cfg.ground_relay_delay_s
    rows = []
    for i, event in enumerate(sorted(cfg.scheduled_task_arrivals, key=lambda e: e["release_step"])):
        release = event["release_step"] * cfg.dt
        if cfg.sim_steps * cfg.dt - release < 300:
            raise ValueError("Incomplete follow-up")
        rows.append({"task_id": f"task_{i}", "source_sat_id": event["source_sat_id"],
                     "release_seconds": release,
                     **{name: hop_bound(release, event["source_sat_id"], epochs, period, post, budget)
                        for name, budget in budgets.items()}})
    report = {"config_sha256": hashlib.sha256(args.config.read_bytes()).hexdigest(),
              "auditor_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "geometry_auditor_sha256": hashlib.sha256(Path(epoch_graphs.__code__.co_filename).read_bytes()).hexdigest(),
              "scope": "Zero compute/transfer time, unlimited resources, retained copies, perfect future. "
                       "Total-hop relaxation allows waiting during input and any compute split, so it is only an upper bound.",
              "budgets": budgets, "tasks": rows,
              "upper_counts": {name: sum(row[name]["potentially_on_time"] for row in rows) for name in budgets}}
    args.out.write_text(json.dumps(report, indent=2, allow_nan=False))
    print(json.dumps({"tasks": len(rows), "budgets": budgets, "upper_counts": report["upper_counts"]}))


if __name__ == "__main__":
    main()
