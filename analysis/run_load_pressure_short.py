"""已批准的三规则短测：只改变负载片段，不修改公共方法与路由。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import math
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.estimate_load_pressure import OUT, PANEL, peak_window, sha
from analysis.redesign_scenario import digest, fixture, measure_merge, write_json
from analysis.run_calibration_capacity import (
    Competition, FifoDiagnostic, METHODS, busiest_window, check_frozen,
)
from analysis.run_sar_frozen import invariants, peak_rss_gb
from analysis.sar_three_plan import CandidateEnv
from sim.config import config_from_dict
from sim.control import ControlPlane
from sim.marl.decision import decision_state
from sim.scheduler.local import LocalOnlyPolicy
from sim.workload import ImageTiling

HERE = Path(__file__).resolve()
TARGET = OUT / "short_600s"


def read(path):
    return json.loads(path.read_text())


def verify(protocol):
    check_frozen(protocol["frozen_protocol"])
    for path, expected in protocol["input_hashes"].items():
        if sha(Path(path)) != expected:
            raise RuntimeError(f"Input changed: {path}")
    if digest(protocol["config"]) != protocol["config_sha256"]:
        raise RuntimeError("Configuration hash mismatch")


def prepare():
    if TARGET.exists():
        raise FileExistsError(TARGET)
    old = read(PANEL / "protocol.json")
    check_frozen(old)
    arrivals_path = OUT / "maritime_extension/maritime80_arrivals.json"
    arrivals = read(arrivals_path)
    peak = peak_window([e["ready_s"] for e in arrivals], [e["raw_bytes"] for e in arrivals], 600)
    assert peak["count"] == 27 and peak["raw_bytes"] == 26.6e9
    # 对齐原10秒几何及60秒预测周期；起点前的业务和终端配对历史不回放。
    origin = int(peak["start_s"] // 60 * 60)
    duration = int(peak["end_s"] + 301 - origin)
    cfg = deepcopy(old["jobs"]["compute_4_local"]["config"])
    cfg["topology"]["start_time_utc"] = (
        datetime(2026, 10, 15, tzinfo=timezone.utc) + timedelta(seconds=origin)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    cfg["sim_steps"] = round(duration / cfg["dt"])
    cfg["task_arrival_end_step"] = round((peak["end_s"] - origin) / cfg["dt"])
    cfg["scheduled_task_arrivals"] = []
    details = []
    TARGET.mkdir(parents=True)
    for index in peak["event_indices"]:
        event = arrivals[index]
        truth = fixture(event["mode"], event["truth_seed"], 16)
        merge = measure_merge(truth)
        work = ImageTiling.from_dict(truth["tiling"])
        assert work.image_size_mb * 1e6 == event["raw_bytes"]
        assert len(truth["windows"]) == event["compute_windows"]
        tid = f"task_{len(details)}"
        write_json(TARGET / "truth" / f"{tid}.json", truth)
        cfg["scheduled_task_arrivals"].append({
            "release_step": round((event["ready_s"] - origin) / cfg["dt"]),
            "source_sat_id": event["source_sat_id"], "image_tiling": truth["tiling"],
            "window_result_bytes": [w["output_bytes"] for w in truth["windows"]],
            "aggregation_delay_s": merge["median_seconds"],
        })
        details.append({"task_id": tid, "daily_event_index": index, **event,
                        "truth_sha256": digest(truth), "merge_reference": merge})
    config_from_dict(cfg)
    permitted = {"topology", "sim_steps", "task_arrival_end_step", "scheduled_task_arrivals"}
    original = old["jobs"]["compute_4_local"]["config"]
    assert all(cfg[k] == v for k, v in original.items() if k not in permitted)
    assert all(cfg["topology"][k] == v for k, v in original["topology"].items() if k != "start_time_utc")
    inputs = [HERE, Path("analysis/redesign_scenario.py"), Path("analysis/run_sar_frozen.py"),
              Path("analysis/estimate_load_pressure.py"), arrivals_path,
              OUT / "maritime_extension/estimate.json", OUT / "PROTOCOL.md", PANEL / "protocol.json"]
    inputs.extend(sorted((TARGET / "truth").glob("*.json")))
    protocol = {
        "purpose": "Cold-start runtime and congestion diagnostic; not main scenario evidence",
        "date": "2026-10-15", "methods": list(METHODS), "workers": 3,
        "origin_absolute_s": origin, "arrival_peak": peak, "sim_duration_s": duration,
        "followup_s": 301, "D_seconds": 300, "compute_multiplier": 4,
        "initial_state": "Empty queues and transfers; fresh terminal acquisition; 35s no-task setup",
        "config": cfg, "config_sha256": digest(cfg), "events": details,
        "frozen_protocol": old, "input_hashes": {str(p): sha(p) for p in inputs},
        "no_automatic_retry": True, "no_automatic_longer_run": True,
        "sealed_test_dates_accessed": False,
    }
    write_json(TARGET / "protocol.json", protocol)
    verify(protocol)
    return protocol


def run(method, protocol):
    started = time.perf_counter()
    env = None
    try:
        verify(protocol)
        cfg = config_from_dict(protocol["config"])
        if method == "ground":
            cfg = replace(cfg, pipeline_mode="ground_unique_relay")
        env = CandidateEnv(cfg)
        env.control_plane = plane = ControlPlane(set(cfg.task_source_sat_ids))
        dispatcher = FifoDiagnostic() if method == "fifo" else LocalOnlyPolicy()
        tracker = Competition(env, method)
        initialized = time.perf_counter()
        ground_mb = np.zeros(protocol["sim_duration_s"])
        per_second_wall = np.zeros(protocol["sim_duration_s"])
        previous_mb, previous_wall, policy_seconds = 0., initialized, 0.
        rate = cfg.ground_stations[0]["bandwidth_mbps"] / 8
        ticks = round(1 / cfg.dt)
        for step in range(cfg.sim_steps):
            actions = []
            if step % cfg.decision_interval_steps == 0:
                state = env.export_state()
                plane.enqueue(state)
                state = decision_state(state, cfg.decision_tile_limit_per_sat)
                before = time.perf_counter()
                if method == "fifo":
                    actions = dispatcher.select_actions(env, state)
                elif method == "local":
                    actions = dispatcher.select_actions(state)
                policy_seconds += time.perf_counter() - before
            env.step(actions)
            tracker.observe(env)
            if env.time % ticks == 0:
                second = round(env.time * cfg.dt) - 1
                total = math.fsum(v for k, v in env.metrics.network_sent_mb.items() if k.startswith("ground:"))
                ground_mb[second] = total - previous_mb
                previous_mb = total
                now = time.perf_counter()
                per_second_wall[second] = now - previous_wall
                previous_wall = now
                if (second + 1) % 60 == 0:
                    write_json(TARGET / "progress" / f"{method}.json", {
                        "status": "RUNNING", "pid": os.getpid(), "sim_seconds": second + 1,
                        "total_sim_seconds": protocol["sim_duration_s"], "wall_seconds": now - started,
                        "updated_utc": datetime.now(timezone.utc).isoformat(),
                    })
        loop_seconds = time.perf_counter() - initialized
        metrics = env.summary()
        checks = invariants(env)
        sources = set(cfg.task_source_sat_ids)
        wrong = [(c["sat_id"], tid) for c in env.compute_cohort_records.values() for tid in c["tile_ids"]
                 if c["sat_id"] in sources and env.tasks[env.tiles[tid].parent_task_id].source_sat_id != c["sat_id"]]
        incoming = [v - plane.sent_mb_by_edge[k] for k, v in env.metrics.network_sent_mb.items()
                    if k.startswith("isl:") and int(k.split("->")[-1]) in sources]
        checks.update(role_errors=wrong, source_incoming_data_mb=math.fsum(incoming))
        checks["passed"] &= not wrong and all(abs(v) <= 1e-6 for v in incoming)
        if method == "fifo":
            wrong_targets = [tid for c in env.compute_cohort_records.values() for tid in c["tile_ids"]
                             if c["sat_id"] != dispatcher.assignments[env.tiles[tid].parent_task_id]["target"]]
            checks["fifo_target_errors"] = wrong_targets
            checks["passed"] &= not wrong_targets and all(env.metrics.compute_busy_time.get(sid, 0) == 0 for sid in sources)
        if method == "ground":
            checks["unique_raw_errors"] = [tid for tid, task in env.tasks.items() if not math.isclose(
                math.fsum(env.tiles[t].data_size_mb for t in task.tile_ids), task.image_size_mb, abs_tol=1e-6)]
            checks["passed"] &= not checks["unique_raw_errors"] and not env.compute_cohort_records
        outcomes = metrics["task_outcomes"]
        checks["full_followup"] = len(outcomes) == len(protocol["events"]) and all(o["observed_age_seconds"] >= 300 for o in outcomes)
        checks["capacity_conservation"] = bool(np.all(ground_mb >= -1e-6) and np.all(ground_mb <= rate + 1e-6))
        checks["passed"] &= checks["full_followup"] and checks["capacity_conservation"]
        verify(protocol)
        ontime = [o["task_id"] for o in outcomes if o["status"] == "completed" and o["latency_seconds"] <= 300]
        restricted = [min(o["latency_seconds"], 300) if o["status"] == "completed" else 300 for o in outcomes]
        competition = tracker.summary()
        # 旧诊断的“完整一图等待”不再作为本轮多源竞争判据。
        competition.pop("criterion_c")
        competition["legacy_complete_image_or_multisource_queue_seconds"] = competition.pop("qualifying_queue_seconds")
        ground_peak = busiest_window(ground_mb / rate)
        ground_peak["utilization"] = ground_peak["busy_s"] / ground_peak["duration_s"]
        left = int(protocol["arrival_peak"]["start_s"] - protocol["origin_absolute_s"])
        right = left + 600
        record = {
            "status": "PASS" if checks["passed"] else "AUDIT_MISMATCH", "method": method,
            "config_sha256": protocol["config_sha256"], "total_tasks": len(outcomes),
            "on_time_tasks": len(ontime), "on_time_task_ids": ontime,
            "P_D": len(ontime) / len(outcomes), "R_D_seconds": math.fsum(restricted) / len(outcomes),
            "wall_seconds": time.perf_counter() - started, "initialization_seconds": initialized - started,
            "loop_seconds": loop_seconds, "policy_seconds": policy_seconds, "peak_rss_gb": peak_rss_gb(),
            "ground_peak_600s": ground_peak, "ground_arrival_window_utilization": float(ground_mb[left:right].sum() / (rate * 600)),
            "ground_total_mb": float(ground_mb.sum()), "competition": competition,
            "criterion_c_diagnostic": competition["peak_queue"]["waiting_source_count"] >= 2 or ground_peak["utilization"] >= .8,
            "audit": checks, "outcomes": outcomes, "assignments": dispatcher.assignments if method == "fifo" else {},
            "cold_start_not_main_result": True,
        }
        np.savez_compressed(TARGET / f"{method}_series.npz", busy=tracker.busy,
                            helper_ids=tracker.helpers, ground_mb=ground_mb, wall_seconds=per_second_wall)
        write_json(TARGET / f"{method}.json", record)
        status = {k: record[k] for k in ("method", "status", "wall_seconds")}
    except Exception:
        status = {"method": method, "status": "FAILED", "traceback": traceback.format_exc()}
        write_json(TARGET / f"{method}_FAILED.json", {**status, "config": protocol["config"]})
    finally:
        if env is not None:
            env.close()
    write_json(TARGET / "progress" / f"{method}.json", status)
    return status


def report(protocol):
    verify(protocol)
    records = {m: read(TARGET / f"{m}.json") for m in METHODS if (TARGET / f"{m}.json").exists()}
    failures = [m for m in METHODS if m not in records or records[m]["status"] != "PASS"]
    audit = []
    for method, record in records.items():
        with np.load(TARGET / f"{method}_series.npz") as series:
            busy = series["busy"]
            ground = series["ground_mb"]
            rate = protocol["config"]["ground_stations"][0]["bandwidth_mbps"] / 8
            ontime = sum(o["status"] == "completed" and o["latency_seconds"] <= 300 for o in record["outcomes"])
            r = math.fsum(min(o["latency_seconds"], 300) if o["status"] == "completed" else 300 for o in record["outcomes"]) / len(record["outcomes"])
            # 独立直接求和复核最忙600秒，不复用前缀和实现。
            peak = max(float(ground[s:s+600].sum()) / (rate * 600) for s in range(len(ground)-599))
            ok = (ontime == record["on_time_tasks"] and abs(r-record["R_D_seconds"]) <= 1e-6
                  and abs(peak-record["ground_peak_600s"]["utilization"]) <= 1e-9
                  and math.isclose(float(busy.sum()), record["competition"]["helper_busy_seconds"], abs_tol=1e-6))
            audit.append({"method": method, "passed": ok})
            if not ok:
                failures.append(method)
    write_json(TARGET / "FINAL_AUDIT.json", {"status": "PASS" if not failures else "BLOCKED",
               "source_file_count": len(protocol["frozen_protocol"]["source_hashes"]), "records": audit,
               "protocol_sha256": sha(TARGET / "protocol.json"), "failures": sorted(set(failures))})
    write_json(TARGET / "summary.json", {"methods": records, "failures": sorted(set(failures)),
               "scope": protocol["purpose"], "no_longer_run_started": True})
    lines = ["# 最忙10分钟冷启动短测", "", "27图，144计算星+24SAR，单站400Mbps，计算量x4，D=300秒。",
             "从UTC 06:16:00空队列及新终端状态启动；到达UTC 06:16:35至06:26:35，追加301秒随访。",
             "本次用于测时与拥塞诊断，不具有全天队列历史，不作主场景验收或方法排名。没有沿用原136图上界。", "",
             "|规则|状态|按时/总数|P_D|R_D(s)|墙钟(s)|地面最忙600秒利用率|同节点等待源星峰值|峰值内存(GB)|",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for m in METHODS:
        if m not in records:
            lines.append(f"|{m}|FAILED|-|-|-|-|-|-|-|")
            continue
        r = records[m]
        lines.append(f"|{m}|{r['status']}|{r['on_time_tasks']}/{r['total_tasks']}|{r['P_D']:.2%}|{r['R_D_seconds']:.3f}|{r['wall_seconds']:.1f}|{r['ground_peak_600s']['utilization']:.2%}|{r['competition']['peak_queue']['waiting_source_count']}|{r['peak_rss_gb']:.3f}|")
    lines += ["", "地面利用率为实际下行字节/单通道容量，包含随访中的滚动600秒窗口；不等同于到达需求利用率88.7%。",
              "FIFO为固定最近计算星的诊断规则。短测不自动触发1小时或6小时运行，后续预算需结合实际分段计时与未清队列评估。"]
    (TARGET / "REPORT.md").write_text("\n".join(lines) + "\n")
    write_json(TARGET / "final_status.json", {"status": "COMPLETE" if not failures else "BLOCKED",
               "terminal": 3, "passed": len(records)-len(set(failures) & set(records)), "failures": sorted(set(failures)),
               "no_automatic_retry": True, "no_longer_run_started": True})


def panel():
    protocol = read(TARGET / "protocol.json")
    verify(protocol)
    statuses = []
    write_json(TARGET / "progress.json", {"pid": os.getpid(), "done": 0, "total": 3})
    try:
        with ProcessPoolExecutor(max_workers=3, mp_context=multiprocessing.get_context("spawn")) as pool:
            futures = {pool.submit(run, m, protocol): m for m in METHODS}
            for future in as_completed(futures):
                try:
                    status = future.result()
                except Exception:
                    status = {"method": futures[future], "status": "FAILED", "traceback": traceback.format_exc()}
                    write_json(TARGET / f"{futures[future]}_FAILED.json", {**status, "config": protocol["config"]})
                statuses.append(status)
                write_json(TARGET / "progress.json", {"pid": os.getpid(), "done": len(statuses), "total": 3, "statuses": statuses})
                print(json.dumps(status), flush=True)
        report(protocol)
    except Exception:
        write_json(TARGET / "final_status.json", {"status": "BLOCKED", "traceback": traceback.format_exc()})
        raise


def launch():
    protocol = read(TARGET / "protocol.json")
    verify(protocol)
    if (TARGET / "launch.json").exists():
        raise FileExistsError("No automatic relaunch")
    with (TARGET / "panel.log").open("xb") as log:
        env = dict(os.environ, OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
        process = subprocess.Popen([sys.executable, str(HERE), "panel"], stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True, env=env)
    guard = subprocess.Popen(["caffeinate", "-i", "-w", str(process.pid)],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    data = {"pid": process.pid, "caffeinate_pid": guard.pid, "workers": 3,
            "command": f"{sys.executable} {HERE} panel", "created_utc": datetime.now(timezone.utc).isoformat(),
            "protocol_sha256": sha(TARGET / "protocol.json"), "host": "mac"}
    write_json(TARGET / "launch.json", data)
    print(json.dumps(data))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "launch", "panel", "report"))
    mode = parser.parse_args().mode
    if mode == "prepare":
        p = prepare()
        print(json.dumps({"tasks": len(p["events"]), "sim_seconds": p["sim_duration_s"], "output": str(TARGET)}))
    elif mode == "launch":
        launch()
    elif mode == "panel":
        panel()
    else:
        report(read(TARGET / "protocol.json"))
