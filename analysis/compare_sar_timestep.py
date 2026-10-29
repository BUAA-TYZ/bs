"""对比同一冻结前缀在两个 dt 下的逐任务结果，按预登记门槛判定数值精度。

输入是 run_sar_frozen.py 的两个输出目录（只允许 dt 及对应步数不同）。不运行仿真。
门槛：按时状态一致、完整交付时延差不超过 --max-latency-diff 秒、两侧不变量均通过。
"""

import argparse
import json
from pathlib import Path

HORIZON_S = 300.0
TEMPORAL = {"dt", "sim_steps", "decision_interval_steps", "topology_update_steps",
            "ground_visibility_update_steps", "task_arrival_end_step", "scheduled_task_arrivals"}


def load(directory: Path, method: str) -> tuple[dict, dict]:
    config = json.loads((directory / "derived_config.json").read_bytes())
    return config, json.loads((directory / f"{method}.json").read_bytes())


def compare(coarse_dir: Path, fine_dir: Path, method: str, max_diff: float) -> dict:
    coarse_cfg, coarse = load(coarse_dir, method)
    fine_cfg, fine = load(fine_dir, method)
    changed = sorted(key for key in coarse_cfg if coarse_cfg[key] != fine_cfg[key])
    if any(key not in TEMPORAL for key in changed):
        raise ValueError(f"Non-temporal fields differ: {changed}")
    seconds = lambda cfg: [(e["source_sat_id"], round(e["release_step"] * cfg["dt"], 9))
                           for e in cfg["scheduled_task_arrivals"]]
    if seconds(coarse_cfg) != seconds(fine_cfg):
        raise ValueError("Physical release times differ")
    if not math_isclose(coarse_cfg["sim_steps"] * coarse_cfg["dt"], fine_cfg["sim_steps"] * fine_cfg["dt"]):
        raise ValueError("Observation horizons differ")
    rows, mismatched, over = [], [], []
    for a, b in zip(coarse["metrics"]["task_outcomes"], fine["metrics"]["task_outcomes"]):
        if a["task_id"] != b["task_id"]:
            raise ValueError("Task order differs")
        on_a = a["status"] == "completed" and a["latency_seconds"] <= HORIZON_S
        on_b = b["status"] == "completed" and b["latency_seconds"] <= HORIZON_S
        diff = (None if a["latency_seconds"] is None or b["latency_seconds"] is None
                else b["latency_seconds"] - a["latency_seconds"])
        if on_a != on_b:
            mismatched.append(a["task_id"])
        if diff is None and a["status"] != b["status"] or diff is not None and abs(diff) > max_diff + 1e-9:
            over.append(a["task_id"])
        rows.append({"task_id": a["task_id"], "release_seconds": a["release_seconds"],
                     "coarse_latency_s": a["latency_seconds"], "fine_latency_s": b["latency_seconds"],
                     "fine_minus_coarse_s": diff, "coarse_on_time": on_a, "fine_on_time": on_b})
    diffs = [abs(r["fine_minus_coarse_s"]) for r in rows if r["fine_minus_coarse_s"] is not None]
    passed = (not mismatched and not over and coarse["invariants"]["passed"] and fine["invariants"]["passed"])
    return {"method": method, "coarse_dt": coarse["dt"], "fine_dt": fine["dt"],
            "coarse_P300": coarse["P300"], "fine_P300": fine["P300"],
            "coarse_R300": coarse["R300_seconds"], "fine_R300": fine["R300_seconds"],
            "max_abs_latency_diff_s": max(diffs, default=0.0),
            "mean_signed_latency_diff_s": sum(r["fine_minus_coarse_s"] for r in rows
                                              if r["fine_minus_coarse_s"] is not None) / max(1, len(diffs)),
            "on_time_mismatch": mismatched, "over_threshold": over, "threshold_s": max_diff,
            "invariants_passed": [coarse["invariants"]["passed"], fine["invariants"]["passed"]],
            "wall_seconds": [coarse["wall_seconds"], fine["wall_seconds"]],
            "passed": passed, "tasks": rows}


def math_isclose(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-9 * max(1.0, abs(a), abs(b))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coarse", type=Path, required=True)
    parser.add_argument("--fine", type=Path, required=True)
    parser.add_argument("--methods", nargs="+", required=True)
    parser.add_argument("--max-latency-diff", type=float, default=1.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = [compare(args.coarse, args.fine, method, args.max_latency_diff) for method in args.methods]
    report = {"scope": "numerical timestep diagnostic on a frozen prefix; not a performance result",
              "coarse": str(args.coarse), "fine": str(args.fine),
              "acceptance": {"on_time_status": "identical", "max_abs_latency_diff_s": args.max_latency_diff,
                             "invariants": "work/byte conservation, timestamp order, no early completion"},
              "all_passed": all(r["passed"] for r in results), "results": results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False))
    for r in results:
        print(json.dumps({k: r[k] for k in ("method", "coarse_P300", "fine_P300", "coarse_R300", "fine_R300",
                                            "max_abs_latency_diff_s", "on_time_mismatch", "passed")}))


if __name__ == "__main__":
    main()
