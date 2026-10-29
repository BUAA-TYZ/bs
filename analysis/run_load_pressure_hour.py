"""一小时中性筛选编排；复用冻结短测执行器，仅增加窗口评分与上界审计。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
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
from analysis import run_load_pressure_short as short
from analysis.calibrate_redesign_geometry import prepared_graph, propagate
from analysis.estimate_load_pressure import OUT, PANEL, sha
from analysis.redesign_scenario import digest, fixture, measure_merge, write_json
from analysis.run_calibration_capacity import Competition, METHODS, busiest_window, check_frozen
from sim.config import config_from_dict
from sim.orbit import load_satellites, parse_start_time_utc, position_km

HERE = Path(__file__).resolve()
TARGET = OUT / "hour_3600s"
CACHE = OUT.parent / "contacts_144.npz"


def read(path):
    return json.loads(path.read_text())


def select_window(arrivals):
    lo, hi = 23195 - 3600, 22595
    candidates = sorted({lo, hi} | {e["ready_s"] for e in arrivals if lo <= e["ready_s"] <= hi})
    start = max(candidates, key=lambda s: (sum(e["raw_bytes"] for e in arrivals if s <= e["ready_s"] < s+3600), -s))
    return start, start+3600, int((start-600)//60*60)


def compute_upper(events):
    """固定配对接触图的零代价传播；另以正向集合传播核验每图D内结果。"""
    with np.load(CACHE) as data:
        intervals = data["intervals"]
        grid = data["times"]
        visible = data["ground_visible"][:, :, 0]
    first = int(min(e["ready_s"] for e in events))
    last = int(max(e["ready_s"] for e in events)+300)
    assert all(e["ready_s"] % 5 == 0 for e in events)
    frames, masks = {}, {}
    for now in range(first, last+1, 5):
        pairs = intervals[(intervals[:, 2] <= now) & (now < intervals[:, 3]), :2]
        frames[now] = prepared_graph(pairs, 168, True)
        k = int(np.searchsorted(grid, now, side="right")-1)
        assert k >= 0 and now-grid[k] < 10
        masks[now] = visible[k]
    future = np.full(168, np.inf)
    due = {}
    for index, event in enumerate(events):
        due.setdefault(int(event["ready_s"]), []).append(index)
    reverse = {}
    for now in range(last, first-1, -5):
        future = propagate(future, frames[now], masks[now], now)
        for index in due.get(now, []):
            arrival = float(future[events[index]["source_sat_id"]])
            reverse[index] = arrival if arrival <= now+300 else None
    rows = []
    for index, event in enumerate(events):
        release = int(event["ready_s"])
        reachable = np.zeros(168, dtype=bool)
        reachable[event["source_sat_id"]] = True
        found = None
        for now in range(release, release+301, 5):
            labels, outward = frames[now]
            if len(outward):
                reachable[outward[reachable[outward[:, 0]], 1]] = True
            components = np.unique(labels[reachable])
            reachable |= np.isin(labels, components)
            if np.any(reachable & masks[now]):
                found = now
                break
        if found != reverse[index]:
            raise AssertionError(f"Upper bound directional mismatch at {event['task_id']}: {found}, {reverse[index]}")
        rows.append({"task_id": event["task_id"], "source_sat_id": event["source_sat_id"],
                     "release_absolute_s": release, "earliest_arrival_s": found,
                     "latency_s": found-release if found is not None else None,
                     "reachable_by_D": found is not None})
    return {"scope": "Zero-cost fixed-terminal-pairing protocol relaxation; SAR outward only; single station",
            "D_s": 300, "geometry_step_s": 10, "capture_substep_s": 5,
            "tasks": rows, "reachable": sum(r["reachable_by_D"] for r in rows),
            "total": len(rows), "fraction": sum(r["reachable_by_D"] for r in rows)/len(rows),
            "forward_backward_audit": "PASS", "cache_sha256": sha(CACHE)}


def prepare():
    if TARGET.exists():
        raise FileExistsError(TARGET)
    original_protocol = read(PANEL / "protocol.json")
    check_frozen(original_protocol)
    arrivals_path = OUT / "maritime_extension/maritime80_arrivals.json"
    arrivals = read(arrivals_path)
    start, end, origin = select_window(arrivals)
    selected = [(i, e) for i, e in enumerate(arrivals) if origin <= e["ready_s"] < end]
    cfg = deepcopy(original_protocol["jobs"]["compute_4_local"]["config"])
    cfg["topology"]["start_time_utc"] = (datetime(2026, 10, 15, tzinfo=timezone.utc)+timedelta(seconds=origin)).strftime("%Y-%m-%dT%H:%M:%SZ")
    duration = int(end+301-origin)
    cfg["sim_steps"] = round(duration/cfg["dt"])
    cfg["task_arrival_end_step"] = round((end-origin)/cfg["dt"])
    cfg["scheduled_task_arrivals"] = []
    TARGET.mkdir(parents=True)
    details = []
    for index, event in selected:
        tid = f"task_{len(details)}"
        truth = fixture(event["mode"], event["truth_seed"], 16)
        merge = measure_merge(truth)
        write_json(TARGET / "truth" / f"{tid}.json", truth)
        cfg["scheduled_task_arrivals"].append({
            "release_step": round((event["ready_s"]-origin)/cfg["dt"]), "source_sat_id": event["source_sat_id"],
            "image_tiling": truth["tiling"], "window_result_bytes": [w["output_bytes"] for w in truth["windows"]],
            "aggregation_delay_s": merge["median_seconds"],
        })
        details.append({"task_id": tid, "daily_event_index": index, **event, "scored": event["ready_s"] >= start,
                        "truth_sha256": digest(truth), "merge_reference": merge})
    config_from_dict(cfg)
    old = original_protocol["jobs"]["compute_4_local"]["config"]
    allowed = {"topology", "sim_steps", "task_arrival_end_step", "scheduled_task_arrivals"}
    assert all(cfg[k] == v for k, v in old.items() if k not in allowed)
    assert all(cfg["topology"][k] == v for k, v in old["topology"].items() if k != "start_time_utc")
    before = parse_start_time_utc(old["topology"]["start_time_utc"])
    after = parse_start_time_utc(cfg["topology"]["start_time_utc"])
    sats = load_satellites(cfg["topology"]["tle_lines"])
    for sid in (0, 23, 24, 167):
        for relative in (0, start-origin, end-origin, duration):
            assert position_km(sats[sid], before, origin+relative) == position_km(sats[sid], after, relative)
    scored = [e for e in details if e["scored"]]
    upper = compute_upper(scored)
    write_json(TARGET / "upper_bound.json", upper)
    inputs = [HERE, Path(short.__file__), Path("tests/test_load_pressure_hour.py"), Path("analysis/calibrate_redesign_geometry.py"),
              Path("analysis/redesign_scenario.py"), Path("analysis/run_sar_frozen.py"),
              Path("analysis/estimate_load_pressure.py"), PANEL / "protocol.json", arrivals_path,
              OUT / "HOUR_PROTOCOL.md", TARGET / "upper_bound.json", CACHE]
    inputs += sorted((TARGET / "truth").glob("*.json"))
    protocol = {
        "purpose": "One-hour neutral screening with preceding real arrivals; no automatic longer run",
        "methods": list(METHODS), "workers": 3, "date": "2026-10-15",
        "origin_absolute_s": origin, "score_start_s": int(start-origin), "score_end_s": int(end-origin),
        "score_absolute_start_s": start, "score_absolute_end_s": end,
        "arrival_peak": {"start_s": start, "end_s": end}, "sim_duration_s": duration,
        "warmup_s": start-origin, "warmup_images": len(details)-len(scored),
        "scored_task_ids": [e["task_id"] for e in scored], "scored_raw_bytes": sum(e["raw_bytes"] for e in scored),
        "D_seconds": 300, "followup_s": 301, "compute_multiplier": 4,
        "initial_state": "Empty at warmup start; all real warmup arrivals retained; no pre-origin state",
        "config": cfg, "config_sha256": digest(cfg), "events": details,
        "frozen_protocol": original_protocol, "input_hashes": {str(p): sha(p) for p in inputs},
        "no_automatic_retry": True, "no_automatic_longer_run": True, "sealed_test_dates_accessed": False,
    }
    write_json(TARGET / "protocol.json", protocol)
    short.verify(protocol)
    print(json.dumps({k: protocol[k] for k in ("score_absolute_start_s", "score_absolute_end_s", "warmup_s", "warmup_images", "scored_raw_bytes", "sim_duration_s")}))
    print(json.dumps({"scored_tasks": len(scored), "upper_bound": upper["fraction"], "source_hash_audit": "PASS"}))


class HourCompetition(Competition):
    def __init__(self, env, method, start, end):
        super().__init__(env, method)
        self.start, self.end = start, end

    def observe(self, env):
        # 峰值仅归属于主窗口，预热及随访的队列不混入主窗口竞争判据。
        method = self.method
        if not self.start <= env.time*env.cfg.dt < self.end:
            self.method = "observation_only"
        try:
            super().observe(env)
        finally:
            self.method = method


def worker(method, protocol):
    # 仅在独立子进程绑定输出和观测器；不修改复用入口文件或执行语义。
    short.TARGET = TARGET
    short.Competition = lambda env, m: HourCompetition(env, m, protocol["score_start_s"], protocol["score_end_s"])
    return short.run(method, protocol)


def summarize(protocol):
    short.verify(protocol)
    start, end = protocol["score_start_s"], protocol["score_end_s"]
    wanted = set(protocol["scored_task_ids"])
    upper = read(TARGET / "upper_bound.json")
    bound_by_id = {r["task_id"]: r for r in upper["tasks"]}
    records, audits, failures = {}, [], []
    rate = protocol["config"]["ground_stations"][0]["bandwidth_mbps"]/8
    for method in METHODS:
        path = TARGET / f"{method}.json"
        if not path.exists():
            failures.append(method)
            continue
        raw = read(path)
        outcomes = [r for r in raw["outcomes"] if r["task_id"] in wanted]
        ontime = [r["task_id"] for r in outcomes if r["status"] == "completed" and r["latency_seconds"] <= 300]
        r_d = math.fsum(min(r["latency_seconds"], 300) if r["status"] == "completed" else 300 for r in outcomes)/len(outcomes)
        with np.load(TARGET / f"{method}_series.npz") as data:
            fullbusy = data["busy"]
            busy = fullbusy[start:end]
            ground = data["ground_mb"][start:end]
            helpers = data["helper_ids"]
            wall = data["wall_seconds"]
            groundpeak = busiest_window(ground/rate)
            direct_peak = max(float(ground[i:i+600].sum())/(rate*600) for i in range(len(ground)-599))
            assert abs(direct_peak-groundpeak["busy_s"]/600) < 1e-9
            assert math.isclose(float(fullbusy.sum()), raw["competition"]["helper_busy_seconds"], abs_tol=1e-6)
            cluster = busiest_window(busy.sum(axis=1))
            cluster["utilization"] = cluster["busy_s"]/(144*600)
            hotspot = max(({"sat_id": int(s), **busiest_window(busy[:, i])} for i, s in enumerate(helpers)), key=lambda r: r["busy_s"])
            hotspot["utilization"] = hotspot["busy_s"]/600
            groundpeak["utilization"] = direct_peak
            for row in (cluster, hotspot, groundpeak):
                row["start_s"] += start
                row["end_s"] += start
            per_sat = busy.sum(axis=0)
            total = float(per_sat.sum())
            top = float(np.sort(per_sat)[-15:].sum()/total) if total else None
            segment_wall = {"warmup_s": float(wall[:start].sum()), "main_s": float(wall[start:end].sum()), "followup_s": float(wall[end:].sum())}
            helper_busy = total
        checks = {
            "execution_pass": raw["status"] == "PASS" and raw["audit"]["passed"],
            "config_hash": raw["config_sha256"] == protocol["config_sha256"],
            "scored_set": len(outcomes) == len(wanted) and {r["task_id"] for r in outcomes} == wanted,
            "full_followup": all(r["observed_age_seconds"] >= 300 for r in outcomes),
            "within_upper": all(bound_by_id[tid]["reachable_by_D"] for tid in ontime),
            "ground_capacity": bool(np.all(ground >= -1e-6) and np.all(ground <= rate+1e-6)),
        }
        passed = all(checks.values())
        audits.append({"method": method, "passed": passed, "checks": checks})
        if not passed:
            failures.append(method)
        queue = raw["competition"]["peak_queue"]
        records[method] = {
            "status": "PASS" if passed else "AUDIT_MISMATCH", "P_D": len(ontime)/len(outcomes),
            "R_D_seconds": r_d, "on_time_tasks": len(ontime), "total_tasks": len(outcomes),
            "outcomes": outcomes, "on_time_task_ids": ontime, "upper_bound": upper["fraction"],
            "queue_peak": queue, "queue_witnesses": raw["competition"]["peak_witnesses"],
            "ground_peak_600s": groundpeak, "cluster_peak_600s": cluster, "helper_peak_600s": hotspot,
            "helper_busy_s": helper_busy, "top15_share": top, "wall_seconds": raw["wall_seconds"],
            "segment_wall": segment_wall, "peak_rss_gb": raw["peak_rss_gb"],
            "criterion_c": queue["waiting_source_count"] >= 2 or direct_peak >= .8,
        }
    criteria = None
    if not failures and len(records) == 3:
        u = upper["fraction"]
        criteria = {"a": u >= .85, "b": u-records["local"]["P_D"] >= .15-1e-12,
                    "c": any(r["criterion_c"] for r in records.values()),
                    "d": all(u-r["P_D"] > .05+1e-12 for r in records.values())}
        criteria["candidate"] = all(criteria.values())
    audit = {"status": "PASS" if not failures else "BLOCKED", "source_file_count": len(protocol["frozen_protocol"]["source_hashes"]),
             "protocol_sha256": sha(TARGET / "protocol.json"), "records": audits, "failures": failures,
             "upper_forward_backward": upper["forward_backward_audit"]}
    write_json(TARGET / "FINAL_AUDIT.json", audit)
    write_json(TARGET / "summary.json", {"methods": records, "criteria": criteria, "upper_bound": upper["fraction"],
               "scope": protocol["purpose"], "failures": failures, "no_longer_run_started": True})
    lines = ["# 含高峰一小时中性筛选", "", f"主窗口UTC秒[{protocol['score_absolute_start_s']},{protocol['score_absolute_end_s']})；{len(wanted)}图，{protocol['scored_raw_bytes']/1e9:.1f}GB。",
             f"前置{protocol['warmup_s']}秒预热，真实预热图{protocol['warmup_images']}张；随访301秒。更早积压未恢复，主窗口后没有新到达。",
             "预热没有图时仅预热几何/终端与预测状态，不能声称获得了带历史积压的稳态。FIFO仅诊断规则，不作方法排名。",
             f"单站、144计算星、24SAR、计算量x4、D300。固定终端配对零代价上界：{upper['reachable']}/{upper['total']}={upper['fraction']:.2%}，正反向逐图核验通过。",
             "", "|规则|状态|按时/总数|P_D|R_D(s)|地面忙600s|同节点等待源星峰值|等待块/窗/完整图峰值|集群忙600s|单星忙600s|前15星份额|墙钟(s)|",
             "|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|"]
    for method in METHODS:
        if method not in records:
            lines.append(f"|{method}|FAILED|-|-|-|-|-|-|-|-|-|-|")
            continue
        r = records[method]; q = r["queue_peak"]
        share = f"{r['top15_share']:.2%}" if r["top15_share"] is not None else "N/A"
        lines.append(f"|{method}|{r['status']}|{r['on_time_tasks']}/{r['total_tasks']}|{r['P_D']:.2%}|{r['R_D_seconds']:.3f}|{r['ground_peak_600s']['utilization']:.2%}|{q['waiting_source_count']}|{q['blocks']}/{q['windows']}/{q['complete_waiting_images']}|{r['cluster_peak_600s']['utilization']:.2%}|{r['helper_peak_600s']['utilization']:.2%}|{share}|{r['wall_seconds']:.1f}|")
    lines += ["", f"预登记a/b/c/d判定：{json.dumps(criteria, ensure_ascii=False)}。",
              "", "所有拥塞指标仅使用主窗口；P_D/R_D仅评分主窗口到达图。每秒原始序列及逐图结果保留，最忙窗口起止和峰值证据见summary.json。",
              "c要求至少两个不同源星在同计算星等待，或单站实际接收利用率>=80%；同源完整图积压不能替代。局部排队不等于集群饱和或学习收益。",
              "", "完成后停止：不自动启动六小时、扩负载、训练或访问封存日期。"]
    (TARGET / "REPORT.md").write_text("\n".join(lines)+"\n")
    write_json(TARGET / "final_status.json", {"status": "COMPLETE" if not failures else "BLOCKED", "terminal": 3,
               "failures": failures, "criteria": criteria, "no_longer_run_started": True})


def panel():
    protocol = read(TARGET / "protocol.json")
    statuses = []
    started = time.perf_counter()
    try:
        short.verify(protocol)
        write_json(TARGET / "progress.json", {"pid": os.getpid(), "done": 0, "total": 3})
        with ProcessPoolExecutor(max_workers=3, mp_context=multiprocessing.get_context("spawn")) as pool:
            futures = {pool.submit(worker, m, protocol): m for m in METHODS}
            for future in as_completed(futures):
                try:
                    status = future.result()
                except Exception:
                    status = {"method": futures[future], "status": "FAILED", "traceback": traceback.format_exc()}
                    write_json(TARGET / f"{futures[future]}_FAILED.json", {**status, "config": protocol["config"]})
                statuses.append(status)
                write_json(TARGET / "progress.json", {"pid": os.getpid(), "done": len(statuses), "total": 3,
                           "statuses": statuses, "wall_seconds": time.perf_counter()-started})
                print(json.dumps(status), flush=True)
        summarize(protocol)
    except Exception:
        write_json(TARGET / "final_status.json", {"status": "BLOCKED", "traceback": traceback.format_exc()})
        raise


def launch():
    protocol = read(TARGET / "protocol.json")
    short.verify(protocol)
    if (TARGET / "launch.json").exists():
        raise FileExistsError("No automatic relaunch")
    with (TARGET / "panel.log").open("xb") as log:
        env = dict(os.environ, OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
        process = subprocess.Popen([sys.executable, str(HERE), "panel"], stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=env)
    guard = subprocess.Popen(["caffeinate", "-i", "-w", str(process.pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    data = {"pid": process.pid, "caffeinate_pid": guard.pid, "workers": 3, "host": "mac",
            "command": f"{sys.executable} {HERE} panel", "created_utc": datetime.now(timezone.utc).isoformat(),
            "protocol_sha256": sha(TARGET / "protocol.json")}
    write_json(TARGET / "launch.json", data)
    print(json.dumps(data))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "launch", "panel", "report"))
    mode = parser.parse_args().mode
    if mode == "prepare":
        prepare()
    elif mode == "launch":
        launch()
    elif mode == "panel":
        panel()
    else:
        summarize(read(TARGET / "protocol.json"))
