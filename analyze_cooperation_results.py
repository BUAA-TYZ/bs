"""从协同诊断原始记录生成瓶颈、重复传输与配对任务证据。"""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

import numpy as np


def distribution(values):
    return {"count": len(values), "mean": float(np.mean(values)) if values else None,
            "p95": float(np.quantile(values, 0.95)) if values else None}


def local_stages(run):
    cfg = run["metrics"]["experiment"]["config"]
    dt, horizon = cfg["dt"], cfg["sim_steps"]
    stages = {key: [] for key in ("arrival_to_compute", "compute", "completed_postcompute", "pending_postcompute")}
    for tile in run["tiles"].values():
        times = tile["timestamps_steps"]
        if times["start_compute"] is not None:
            stages["arrival_to_compute"].append((times["start_compute"] - times["created"]) * dt)
        if times["end_compute"] is not None:
            stages["compute"].append((times["end_compute"] - times["start_compute"]) * dt)
            if tile["state"] == "DONE":
                stages["completed_postcompute"].append((times["end_downlink"] - times["end_compute"]) * dt)
            elif tile["state"] in ("COMPUTED", "DOWNLINKING"):
                # 未完成样本只统计截至观察终点的已等待时间，不估计最终完成时延。
                stages["pending_postcompute"].append((horizon - times["end_compute"]) * dt)
    return stages


def hop_churn(events):
    hops, reversals, previous = Counter(), Counter(), {}
    for event in events:
        if event["event"] != "tx_end":
            continue
        tile, pair = event["tile_id"], (event["sat_from"], event["sat_to"])
        hops[tile] += 1
        if previous.get(tile) == pair[::-1]:
            reversals[tile] += 1
        previous[tile] = pair
    return {"completed_isl_hops": sum(hops.values()), "tiles_with_multiple_hops": sum(n > 1 for n in hops.values()),
            "immediate_reverse_hops": sum(reversals.values()), "max_hops_per_tile": max(hops.values(), default=0)}


def analyze(path):
    report = json.loads(path.read_text(encoding="utf-8"))
    aggregates, stages, groups = {}, {}, []
    for group in report["groups"]:
        if group["scope"] != "development validation only":
            continue
        local = next(run for run in group["runs"] if run["policy"] == "local")
        stage_values = local_stages(local)
        for key, values in stage_values.items():
            stages.setdefault(key, []).extend(values)
        rows, witnesses = [], []
        for run in group["runs"]:
            label = run["policy"]
            aggregates.setdefault(label, []).append(run)
            events_path = path.parent / group["name"] / label / "events.jsonl"
            with events_path.open(encoding="utf-8") as stream:
                churn = hop_churn(json.loads(line) for line in stream)
            rows.append({"policy": label, "overall": run["metrics"]["overall"], "churn": churn,
                         "computed_unique_tiles": run["computed_unique_tiles"],
                         "terminal_tile_states": dict(Counter(t["state"] for t in run["tiles"].values()))})
            if label != "committed_window":
                continue
            for task_id in sorted(set(run["completed_task_ids"]) - set(local["completed_task_ids"])):
                dt = run["metrics"]["experiment"]["config"]["dt"]
                witness = {"task_id": task_id, "plan": run["plans"][task_id],
                           "source_satellite": run["tasks"][task_id]["source_sat_id"],
                           "release_seconds": run["tasks"][task_id]["release_time"] * dt,
                           "candidate_tiles": [], "local_terminal_states": {}}
                for tid in run["tasks"][task_id]["tile_ids"]:
                    tile = run["tiles"][tid]
                    witness["candidate_tiles"].append({"tile_id": tid, "location": tile["location"],
                        "timestamps_seconds": {k: value * dt if value is not None else None
                                               for k, value in tile["timestamps_steps"].items()}})
                    witness["local_terminal_states"][tid] = local["tiles"][tid]["state"]
                witnesses.append(witness)
        groups.append({"name": group["name"], "rows": rows, "paired": group["paired"],
                       "local_stage_seconds": {k: distribution(v) for k, v in stage_values.items()},
                       "gained_task_witnesses": witnesses})
    summary = {}
    for label, runs in aggregates.items():
        count = sum(r["metrics"]["overall"]["completed_tasks"] for r in runs)
        summary[label] = {
            "completion_rate_scenario_mean": float(np.mean([r["metrics"]["overall"]["task_completion_rate"] for r in runs])),
            "completed_tasks": count, "total_tasks": sum(r["metrics"]["overall"]["total_tasks"] for r in runs),
            "completed_task_latency_pooled_seconds": sum((r["metrics"]["latency"]["task_mean"] or 0) *
                r["metrics"]["overall"]["completed_tasks"] for r in runs) / count if count else None,
        }
    return {"source_report": str(path), "source_report_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "scope": "development diagnosis; pending waits are right-censored, not completed latency",
            "aggregate": summary, "pooled_local_stage_seconds": {k: distribution(v) for k, v in stages.items()},
            "groups": groups}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    report = analyze(Path(args.input))
    with Path(args.output).open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
    print(json.dumps(report["aggregate"], indent=2, allow_nan=False))
    print(json.dumps(report["pooled_local_stage_seconds"], indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
