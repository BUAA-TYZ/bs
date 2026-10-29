"""固定五分钟SLA的候选与文献基线配对实验，记录全部参数和结果。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
import hashlib
import json
import multiprocessing
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main as simulator
from analysis.evaluate_literature import common_context, evaluate
from analysis.report_short_deadline import aggregate
from sim.config import load_config
from sim.reproducibility import runtime_metadata


SPECS = {
    name: {"policy": name, "kwargs": {}}
    for name in ("ground_relay", "greedy", "heft_adapted", "cgs_compute_adapted", "dts_tmax_onehop", "dts_tmax_path", "dts_tmax_hybrid")
}
SPECS.update({f"rolling_h{h}": {"policy": "rolling_admission", "kwargs": {
    "commit_horizon_s": h, "guard_returns": True}} for h in (15, 30, 60, 120)})
SPECS["rolling_no_guard"] = {"policy": "rolling_admission", "kwargs": {
    "commit_horizon_s": 30, "guard_returns": False}}
SPECS["rolling_unbounded"] = {"policy": "rolling_admission", "kwargs": {
    "commit_horizon_s": 14400, "guard_returns": True}}
SPECS["rolling_current"] = {"policy": "rolling_admission", "kwargs": {
    "commit_horizon_s": 14400, "guard_returns": False, "input_lookahead_s": 0}}
SPECS["rolling_future"] = {"policy": "rolling_admission", "kwargs": {
    "commit_horizon_s": 14400, "guard_returns": False, "input_lookahead_s": 300}}
for name, lookahead in (("aligned_current", 0), ("aligned_future", 300)):
    SPECS[name] = {"policy": "rolling_admission", "kwargs": {
        "commit_horizon_s": 14400, "guard_returns": False,
        "input_lookahead_s": lookahead, "return_mode": "execution"}}
SPECS["sla_gate"] = {"policy": "rolling_admission", "kwargs": {
    "commit_horizon_s": 14400, "guard_returns": False, "input_lookahead_s": 300,
    "wait_for_sla_only": True, "sla_seconds": 300}}
SPECS["task_quota"] = {"policy": "task_quota", "kwargs": {}}
SPECS["task_quota_current"] = {"policy": "task_quota", "kwargs": {"input_lookahead_s": 0}}
SPECS["task_delivery"] = {"policy": "task_delivery", "kwargs": {}}
SPECS["task_delivery_fcfs"] = {"policy": "task_delivery", "kwargs": {"max_orders": 1}}
SPECS["arrival_calendar"] = {"policy": "arrival_calendar", "kwargs": {}}


def source_hashes():
    paths = [Path(__file__), Path("analysis/evaluate_literature.py"),
             Path("analysis/report_short_deadline.py")]
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def run_case(cfg, case, name, spec):
    original = simulator.make_policy

    def factory(policy_name, seed, marl_checkpoint=""):
        if policy_name == name and spec["policy"] == "task_delivery":
            from sim.scheduler.task_delivery import TaskDeliveryPolicy
            return TaskDeliveryPolicy(**spec["kwargs"])
        if policy_name == name and spec["policy"] == "task_quota":
            from sim.scheduler.task_quota import TaskQuotaPolicy
            return TaskQuotaPolicy(**spec["kwargs"])
        if policy_name == name and spec["policy"] == "rolling_admission":
            from sim.scheduler.rolling_admission import RollingAdmissionPolicy
            return RollingAdmissionPolicy(**spec["kwargs"])
        return original(spec["policy"] if policy_name == name else policy_name,
                        seed, marl_checkpoint=marl_checkpoint)

    simulator.make_policy = factory
    try:
        record = evaluate(cfg, case, name, "", [300])
        record["method_spec"] = spec
        return record
    finally:
        simulator.make_policy = original


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--methods", nargs="+", choices=list(SPECS), required=True)
    parser.add_argument("--case-ids", nargs="+")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--split", choices=("development", "confirmation"), default="development")
    args = parser.parse_args()
    if args.workers < 1 or len(set(args.methods)) != len(args.methods):
        raise ValueError("Workers must be positive and methods distinct")
    prepared = Path(args.prepared_dir)
    frozen = json.loads((prepared / "run.json").read_text())
    infos = frozen["case_info"]
    if args.split == "confirmation":
        selection = prepared / "selection.json"
        if (not selection.is_file() or
                hashlib.sha256(selection.read_bytes()).hexdigest() != frozen.get("selection_sha256") or
                any(info.get("split") != "confirmation" for info in infos)):
            raise ValueError("Confirmation requires an intact, separately prepared selection manifest")
    if args.case_ids:
        if not set(args.case_ids) <= {info["id"] for info in infos}:
            raise ValueError("Unknown case ID")
        infos = [info for info in infos if info["id"] in args.case_ids]
    cases = [(load_config(str(prepared / f"{info['id']}_config.json")),
              dict(info, split=args.split)) for info in infos]
    if not cases or any(cfg.deadline_steps != 0 for cfg, _ in cases):
        raise ValueError("Requires nonempty cases with no hard expiry")
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    runtime, hashes = runtime_metadata(), source_hashes()
    manifest = {**runtime, "args": vars(args), "method_specs": {n: SPECS[n] for n in args.methods},
                "analysis_source_sha256": hashes, "case_info": [i for _, i in cases],
                "prepared_manifest_sha256": hashlib.sha256((prepared / "run.json").read_bytes()).hexdigest(),
                "selection_sha256": frozen.get("selection_sha256"),
                "provenance": deepcopy(frozen["provenance"]), "primary_seconds": 300,
                "reported_horizons_seconds": [300],
                "scope": "paired short-SLA heuristic research; not MARL or original-paper full reproduction",
                "prepared_configs_sha256": {str(prepared / f"{i['id']}_config.json"):
                    hashlib.sha256((prepared / f"{i['id']}_config.json").read_bytes()).hexdigest() for i in infos}}

    def save(name, data):
        (out / name).write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n")

    save("run.json", manifest)
    records, start = [], time.monotonic()
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        jobs = [pool.submit(run_case, cfg, info, name, SPECS[name])
                for cfg, info in cases for name in args.methods]
        for job in as_completed(jobs):
            record = job.result()
            if (runtime_metadata()["source_sha256"] != runtime["source_sha256"] or
                    record["metrics"]["experiment"]["source_sha256"] != runtime["source_sha256"] or
                    source_hashes() != hashes):
                raise RuntimeError("Source changed during paired experiment")
            for previous in records:
                if previous["case"]["id"] == record["case"]["id"] and common_context(previous) != common_context(record):
                    raise RuntimeError("Unpaired case")
            records.append(record)
            save(f"{record['case']['id']}_{record['method']}.json", record)
            print(json.dumps({"finished": len(records), "total": len(jobs), "case": record["case"]["id"],
                              "method": record["method"], "p300": record["task_age"]["delivery_score"],
                              "elapsed_s": record["elapsed_s"]}), flush=True)
    summary, daily = aggregate(records, args.methods, horizons=[300])
    save("summary.json", {**manifest, "elapsed_s": time.monotonic() - start, "aggregate": summary,
                          "daily": daily, "records": [f"{r['case']['id']}_{r['method']}.json" for r in records]})
    lines = ["# 五分钟SLA配对实验", "", manifest["scope"], "", f"面板：{args.split}。所有到达任务均保留，超SLA不取消。", "",
             "| 方法 | 300秒按时/全部 | 场景等权P300 | 300秒截断均值(s) |", "|---|---:|---:|---:|"]
    for row in summary:
        lines.append(f"| {row['method']} | {row['on_time_tasks']}/{row['total_tasks']} | "
                     f"{row['on_time_rate']:.4%} | {row['capped_mean_latency_300s']:.2f} |")
    lines += ["", "此处比较规则候选和论文适配，不能称为新MARL训练结果。配置、完整任务结果、计时和源码哈希均保留。", ""]
    (out / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
