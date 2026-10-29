"""六小时两阶段面板；不改冻结方法，只有中性门控通过才运行信息对照。"""

from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace
import argparse
import json
import math
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import run_load_pressure_hour as hour
from analysis import run_load_pressure_short as short
from analysis.estimate_load_pressure import OUT, peak_window, sha
from analysis.redesign_scenario import digest, write_json
from analysis.run_calibration_capacity import Competition, FifoDiagnostic, busiest_window, waiting_snapshot
from analysis.run_sar_frozen import invariants, peak_rss_gb
from analysis.sar_three_plan import CandidateEnv, ThreePlanChoice
from main import make_policy
from sim.config import config_from_dict
from sim.control import ControlPlane
from sim.marl.decision import decision_state

HERE = Path(__file__).resolve()
ROOT = OUT / "six_21600s"
NEUTRAL = ("local", "ground", "fifo")
INFORMATION = ("v3", "local_1", "local_2", "cgs", "heft")


def read(path):
    return json.loads(path.read_text())


def select_six(arrivals):
    peak = peak_window([e["ready_s"] for e in arrivals], [e["raw_bytes"] for e in arrivals], 21600)
    start, end = peak["start_s"], peak["end_s"]
    return start, end, int((start-600)//60*60)


def distribution(events):
    durations = [r["duration_s"] for r in events]
    bins = (1, 5, 30, 60, 300, math.inf)
    counts, previous = {}, 0
    for edge in bins:
        counts[f"({previous},{edge}]"] = sum(previous < d <= edge for d in durations)
        previous = edge
    return {"count": len(durations), "total_s": math.fsum(durations),
            "mean_s": float(np.mean(durations)) if durations else 0.,
            "median_s": float(np.median(durations)) if durations else 0.,
            "p90_s": float(np.percentile(durations, 90)) if durations else 0.,
            "max_s": max(durations, default=0.), "duration_bins": counts}


class Episodes:
    def __init__(self, start, end):
        self.start, self.end = start, end
        self.open = {}
        self.events = []
        self.union_start = None
        self.union = []

    def update(self, now, rows):
        now = min(self.end, round(now, 9))
        active = {r["sat_id"]: r for r in rows if r["waiting_source_count"] >= 2} if now < self.end else {}
        for sid in set(self.open)-set(active):
            event = self.open.pop(sid)
            event.update(end_s=now, duration_s=round(now-event["start_s"], 9), right_censored=now == self.end)
            event["source_ids"] = sorted(event["source_ids"])
            event["task_ids"] = sorted(event["task_ids"])
            if event["duration_s"] > 0:
                self.events.append(event)
        for sid, row in active.items():
            event = self.open.setdefault(sid, {"sat_id": sid, "start_s": now, "left_censored": now == self.start,
                                               "source_ids": set(), "task_ids": set(), "peak_sources": 0})
            event["source_ids"].update(row["source_ids"])
            event["task_ids"].update(row["task_ids"])
            event["peak_sources"] = max(event["peak_sources"], row["waiting_source_count"])
        if active and self.union_start is None:
            self.union_start = now
        elif not active and self.union_start is not None:
            if now > self.union_start:
                self.union.append({"start_s": self.union_start, "end_s": now,
                                   "duration_s": round(now-self.union_start, 9),
                                   "left_censored": self.union_start == self.start, "right_censored": now == self.end})
            self.union_start = None

    def finish(self):
        self.update(self.end, [])
        return {"node_events": self.events, "network_events": self.union,
                "node_distribution": distribution(self.events), "network_distribution": distribution(self.union),
                "definition": "Continuous >=2 distinct sources truly waiting on one helper; dt=0.1s; no split on identity change"}


class SixObserver(Competition):
    def __init__(self, env, method, protocol):
        super().__init__(env, "fifo")
        self.start, self.end = protocol["score_start_s"], protocol["score_end_s"]
        self.episodes = Episodes(self.start, self.end)

    def observe(self, env):
        now = round(env.time*env.cfg.dt, 9)
        self.method = "fifo" if self.start <= now < self.end else "observation_only"
        super().observe(env)
        if self.start <= now <= self.end:
            self.episodes.update(now, waiting_snapshot(env, self.helpers) if now < self.end else [])


def prepare():
    # 复用已核验的片段准备流程；只在本进程换选择函数与输出，原入口文件不变。
    hour.TARGET = ROOT
    hour.select_window = select_six
    hour.prepare()
    p = read(ROOT / "protocol.json")
    arrivals_path = OUT / "maritime_extension/maritime80_arrivals.json"
    arrivals = read(arrivals_path)
    counts = {str(s): sum(e["source_sat_id"] == s for e in arrivals) for s in range(24)}
    p.update(purpose="Six-hour neutral confirmation then conditional information comparison",
             methods=list(NEUTRAL+INFORMATION), stages={"neutral": list(NEUTRAL), "information": list(INFORMATION)},
             worker_counts={"neutral": 3, "information": 5}, no_automatic_longer_run=False,
             automatic_second_stage_only_if_all_neutral_pass_and_abcd=True,
             stop_after="Both stages or failed neutral gate; no training or additional runs",
             workload={"daily_images": len(arrivals), "daily_raw_bytes": sum(e["raw_bytes"] for e in arrivals),
                       "images_per_source_mean": len(arrivals)/24, "per_source_counts": counts})
    for path in (HERE, OUT / "SIX_HOUR_PROTOCOL.md", Path("tests/test_load_pressure_six.py")):
        p["input_hashes"][str(path)] = sha(path)
    write_json(ROOT / "protocol.json", p)
    short.verify(p)
    print(json.dumps({"warmup_images": p["warmup_images"], "scored_images": len(p["scored_task_ids"]), "workload": p["workload"]}))


def run_job(method, stage, p):
    out = ROOT / stage
    env = None
    started = time.perf_counter()
    try:
        short.verify(p)
        cfg = config_from_dict(p["config"])
        if method == "ground":
            cfg = replace(cfg, pipeline_mode="ground_unique_relay")
        env = CandidateEnv(cfg)
        sources = set(cfg.task_source_sat_ids)
        env.control_plane = plane = ControlPlane(sources)
        local_policies = {sid: ThreePlanChoice(cfg.relay_max_hops) for sid in sources} if method.startswith("local_") else {}
        policy = (FifoDiagnostic() if method == "fifo" else ThreePlanChoice(cfg.relay_max_hops) if method == "v3" else
                  make_policy({"cgs": "cgs_compute_adapted", "heft": "heft_adapted"}.get(method, "local"), cfg.seed))
        observer = SixObserver(env, method, p)
        ground = np.zeros(p["sim_duration_s"])
        wall = np.zeros(p["sim_duration_s"])
        previous_mb, previous_wall = 0., time.perf_counter()
        init_s = previous_wall-started
        policy_s = 0.
        for step in range(cfg.sim_steps):
            actions = []
            if step % cfg.decision_interval_steps == 0:
                state = env.export_state()
                plane.enqueue(state)
                state = decision_state(state, cfg.decision_tile_limit_per_sat)
                stamp = time.perf_counter()
                if local_policies:
                    active_sources = {t["source_sat_id"] for t in state.tiles.values()}
                    for sid in sorted(active_sources):
                        actions.extend(local_policies[sid].select_actions(plane.view(state, sid, int(method[-1]))))
                elif method == "fifo":
                    actions = policy.select_actions(env, state)
                elif method != "ground":
                    actions = policy.select_actions(state)
                policy_s += time.perf_counter()-stamp
            env.step(actions)
            observer.observe(env)
            if env.time % round(1/cfg.dt) == 0:
                sec = round(env.time*cfg.dt)-1
                sent = math.fsum(v for k, v in env.metrics.network_sent_mb.items() if k.startswith("ground:"))
                ground[sec], previous_mb = sent-previous_mb, sent
                now = time.perf_counter()
                wall[sec], previous_wall = now-previous_wall, now
                if (sec+1) % 60 == 0:
                    write_json(out / "progress" / f"{method}.json", {"status": "RUNNING", "pid": os.getpid(),
                               "sim_seconds": sec+1, "total_sim_seconds": p["sim_duration_s"],
                               "wall_seconds": now-started, "updated_utc": datetime.now(timezone.utc).isoformat()})
        checks = invariants(env)
        checks["foreign_compute"] = [(c["sat_id"], tid) for c in env.compute_cohort_records.values() for tid in c["tile_ids"]
                                      if c["sat_id"] in sources and env.tasks[env.tiles[tid].parent_task_id].source_sat_id != c["sat_id"]]
        incoming = [v-plane.sent_mb_by_edge[k] for k, v in env.metrics.network_sent_mb.items()
                    if k.startswith("isl:") and int(k.split("->")[-1]) in sources]
        checks["sar_incoming_data_mb"] = math.fsum(incoming)
        checks["passed"] &= not checks["foreign_compute"] and all(abs(v) <= 1e-6 for v in incoming)
        raw_tasks = set(env.tasks) if method == "ground" else set(env._dist_pipeline.raw_tasks)
        checks["raw_unique_errors"] = [tid for tid in raw_tasks if not math.isclose(
            sum(env.tiles[t].data_size_mb for t in env.tasks[tid].tile_ids), env.tasks[tid].image_size_mb, abs_tol=1e-6)]
        checks["passed"] &= not checks["raw_unique_errors"]
        if method == "fifo":
            checks["fifo_target_errors"] = [tid for c in env.compute_cohort_records.values() for tid in c["tile_ids"]
                                           if c["sat_id"] != policy.assignments[env.tiles[tid].parent_task_id]["target"]]
            checks["passed"] &= not checks["fifo_target_errors"] and not any(env.metrics.compute_busy_time.get(s, 0) for s in sources)
        if method == "ground":
            checks["passed"] &= not env.compute_cohort_records
        outcomes = env.summary()["task_outcomes"]
        wanted = set(p["scored_task_ids"])
        scored = [o for o in outcomes if o["task_id"] in wanted]
        checks["followup"] = len(scored) == len(wanted) and len(outcomes) == len(p["events"]) and all(o["observed_age_seconds"] >= 300 for o in scored)
        checks["passed"] &= checks["followup"] and bool(np.all(ground >= -1e-6) and np.all(ground <= 50+1e-6))
        short.verify(p)
        ontime = [o["task_id"] for o in scored if o["status"] == "completed" and o["latency_seconds"] <= 300]
        record = {"status": "PASS" if checks["passed"] else "AUDIT_MISMATCH", "method": method, "stage": stage,
                  "config_sha256": p["config_sha256"], "P_D": len(ontime)/len(scored), "on_time_task_ids": ontime,
                  "on_time_tasks": len(ontime), "total_tasks": len(scored), "outcomes": outcomes,
                  "R_D_seconds": math.fsum(min(o["latency_seconds"], 300) if o["status"] == "completed" else 300 for o in scored)/len(scored),
                  "wall_seconds": time.perf_counter()-started, "initialization_seconds": init_s, "policy_seconds": policy_s,
                  "peak_rss_gb": peak_rss_gb(), "audit": checks, "control": dict(plane.stats),
                  "competition": observer.summary(), "multisource": observer.episodes.finish(),
                  "assignments": policy.assignments if method == "fifo" else {},
                  "decisions": [d for pol in local_policies.values() for d in pol.decisions] if local_policies else getattr(policy, "decisions", [])}
        np.savez_compressed(out / f"{method}_series.npz", busy=observer.busy, helper_ids=observer.helpers,
                            ground_mb=ground, wall_seconds=wall)
        write_json(out / f"{method}.json", record)
        status = {k: record[k] for k in ("method", "status", "wall_seconds")}
    except Exception:
        status = {"method": method, "status": "FAILED", "traceback": traceback.format_exc()}
        write_json(out / f"{method}_FAILED.json", {**status, "config": p["config"]})
    finally:
        if env is not None:
            env.close()
    write_json(out / "progress" / f"{method}.json", status)
    return status


def audit_record(method, stage, p, upper):
    out = ROOT / stage
    raw = read(out / f"{method}.json")
    wanted = set(p["scored_task_ids"])
    outcomes = [o for o in raw["outcomes"] if o["task_id"] in wanted]
    ontime = [o["task_id"] for o in outcomes if o["status"] == "completed" and o["latency_seconds"] <= 300]
    r_d = math.fsum(min(o["latency_seconds"], 300) if o["status"] == "completed" else 300 for o in outcomes)/len(outcomes)
    assert len(outcomes) == len(wanted) and {o["task_id"] for o in outcomes} == wanted
    assert raw["config_sha256"] == p["config_sha256"] and raw["status"] == "PASS" and raw["audit"]["passed"]
    assert ontime == raw["on_time_task_ids"] and len(ontime)/len(wanted) == raw["P_D"] and abs(r_d-raw["R_D_seconds"]) <= 1e-6
    assert all(o["observed_age_seconds"] >= 300 for o in outcomes)
    reachable = {o["task_id"] for o in upper["tasks"] if o["reachable_by_D"]}
    assert set(ontime) <= reachable
    start, end = p["score_start_s"], p["score_end_s"]
    with np.load(out / f"{method}_series.npz") as series:
        fullbusy = series["busy"]
        busy = fullbusy[start:end]
        ground = series["ground_mb"][start:end]
        assert abs(float(fullbusy.sum())-raw["competition"]["helper_busy_seconds"]) < 1e-6
        assert np.all(ground >= -1e-6) and np.all(ground <= 50+1e-6)
        gpeak = busiest_window(ground/50)
        gpeak["utilization"] = gpeak["busy_s"]/600
        # 卷积独立复算地面600秒，而非沿用前缀和。
        assert abs(float(np.convolve(ground, np.ones(600), "valid").max()/30000)-gpeak["utilization"]) < 1e-9
        cluster = busiest_window(busy.sum(axis=1))
        cluster["utilization"] = cluster["busy_s"]/(144*600)
        hotspot = max(({"sat_id": int(s), **busiest_window(busy[:, i])} for i, s in enumerate(series["helper_ids"])), key=lambda r: r["busy_s"])
        hotspot["utilization"] = hotspot["busy_s"]/600
        for window in (gpeak, cluster, hotspot):
            window["start_s"] += start
            window["end_s"] += start
        total = float(busy.sum())
        top = float(np.sort(busy.sum(axis=0))[-15:].sum()/total) if total else None
        ground_util = float(ground.sum()/(50*(end-start)))
    multi = raw["multisource"]
    for kind in ("node", "network"):
        events = multi[f"{kind}_events"]
        assert distribution(events) == multi[f"{kind}_distribution"]
        for e in events:
            assert start <= e["start_s"] < e["end_s"] <= end
            assert abs(e["duration_s"]-(e["end_s"]-e["start_s"])) < 1e-7
            if kind == "node":
                assert e["peak_sources"] >= 2 and len(e["source_ids"]) >= e["peak_sources"]
    merged = []
    for e in sorted(multi["node_events"], key=lambda e: e["start_s"]):
        if merged and e["start_s"] <= merged[-1][1]+1e-8:
            merged[-1][1] = max(merged[-1][1], e["end_s"])
        else:
            merged.append([e["start_s"], e["end_s"]])
    assert merged == [[e["start_s"], e["end_s"]] for e in multi["network_events"]]
    return {"status": "PASS", "P_D": raw["P_D"], "R_D_seconds": r_d, "on_time_tasks": len(ontime),
            "total_tasks": len(outcomes), "on_time_task_ids": ontime, "wall_seconds": raw["wall_seconds"],
            "queue_peak": raw["competition"]["peak_queue"], "queue_witnesses": raw["competition"]["peak_witnesses"],
            "multisource": multi, "ground_peak_600s": gpeak, "ground_whole_utilization": ground_util,
            "cluster_peak_600s": cluster, "helper_peak_600s": hotspot, "helper_busy_s": total, "top15_share": top,
            "policy_seconds": raw["policy_seconds"], "decision_count": len(raw["decisions"])}


def audit_stage(stage, p):
    short.verify(p)
    upper = read(ROOT / "upper_bound.json")
    rows, failures = {}, []
    for method in p["stages"][stage]:
        try:
            rows[method] = audit_record(method, stage, p, upper)
        except Exception:
            failures.append(method)
            write_json(ROOT / stage / f"{method}_AUDIT_FAILURE.json", {"traceback": traceback.format_exc()})
    criteria = None
    if stage == "neutral" and not failures:
        u = upper["fraction"]
        criteria = {"a": u >= .85, "b": u-rows["local"]["P_D"] >= .15-1e-12,
                    "c": any(r["multisource"]["node_distribution"]["count"] > 0 or r["ground_peak_600s"]["utilization"] >= .8 for r in rows.values()),
                    "d": all(u-r["P_D"] > .05+1e-12 for r in rows.values())}
        criteria["candidate"] = all(criteria.values())
    summary = {"stage": stage, "methods": rows, "failures": failures, "criteria": criteria, "upper_bound": upper["fraction"]}
    write_json(ROOT / stage / "summary.json", summary)
    write_json(ROOT / stage / "FINAL_AUDIT.json", {"status": "PASS" if not failures else "BLOCKED",
               "methods": list(rows), "failures": failures, "source_file_count": len(p["frozen_protocol"]["source_hashes"]),
               "protocol_sha256": sha(ROOT / "protocol.json")})
    write_json(ROOT / stage / "final_status.json", {"status": "COMPLETE" if not failures else "BLOCKED", "criteria": criteria, "failures": failures})
    return summary


def stage_run(stage, p):
    out = ROOT / stage
    out.mkdir(exist_ok=False)
    statuses = []
    started = time.perf_counter()
    write_json(ROOT / "progress.json", {"stage": stage, "pid": os.getpid(), "done": 0, "total": len(p["stages"][stage])})
    with ProcessPoolExecutor(max_workers=p["worker_counts"][stage], mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = {pool.submit(run_job, method, stage, p): method for method in p["stages"][stage]}
        for future in as_completed(futures):
            try:
                status = future.result()
            except Exception:
                status = {"method": futures[future], "status": "FAILED", "traceback": traceback.format_exc()}
                write_json(out / f"{futures[future]}_FAILED.json", {**status, "config": p["config"]})
            statuses.append(status)
            progress = {"stage": stage, "pid": os.getpid(), "done": len(statuses), "total": len(futures),
                        "statuses": statuses, "wall_seconds": time.perf_counter()-started}
            write_json(out / "progress.json", progress)
            write_json(ROOT / "progress.json", progress)
            print(json.dumps(status), flush=True)
    return audit_stage(stage, p)


def report(p, first, second=None):
    rows = {**first["methods"], **(second["methods"] if second else {})}
    upper = read(ROOT / "upper_bound.json")
    pairs = {}
    if "v3" in rows:
        v = rows["v3"]
        for name in ("local_1", "local_2"):
            if name in rows:
                r = rows[name]
                pairs[name] = {"P_D_difference_pp": 100*(v["P_D"]-r["P_D"]),
                               "R_D_difference_s": v["R_D_seconds"]-r["R_D_seconds"],
                               "gained": sorted(set(v["on_time_task_ids"])-set(r["on_time_task_ids"])),
                               "lost": sorted(set(r["on_time_task_ids"])-set(v["on_time_task_ids"]))}
        lower_r = math.fsum(r["latency_s"] if r["reachable_by_D"] else 300 for r in upper["tasks"])/upper["total"]
        pairs["upper_minus_v3"] = {"P_D_gap_pp": 100*(upper["fraction"]-v["P_D"]),
                                    "zero_cost_R_D_lower_bound_s": lower_r, "v3_R_D_above_lower_bound_s": v["R_D_seconds"]-lower_r}
    summary = {"neutral": first, "information": second, "paired": pairs, "workload": p["workload"],
               "upper_bound": upper["fraction"], "no_training_or_additional_runs": True}
    write_json(ROOT / "summary.json", summary)
    lines = ["# 六小时中性确认与信息对照", "", f"主窗口UTC秒[{p['score_absolute_start_s']},{p['score_absolute_end_s']})，{len(p['scored_task_ids'])}图、{p['scored_raw_bytes']/1e9:.1f}GB；预热{p['warmup_s']}秒、{p['warmup_images']}图，随访301秒。",
             f"零代价固定终端配对协议上界{upper['reachable']}/{upper['total']}={upper['fraction']:.2%}，正反向传播核验通过；不代表有限带宽与算力下可达到。",
             "", "|方法|P_D|R_D(s)|按时/总数|地面忙600s/全片段|多源节点事件/全网事件|节点秒/全网秒|墙钟(s)|",
             "|---|---:|---:|---:|---|---|---|---:|"]
    for name, r in rows.items():
        n, g = r["multisource"]["node_distribution"], r["multisource"]["network_distribution"]
        lines.append(f"|{name}|{r['P_D']:.3%}|{r['R_D_seconds']:.3f}|{r['on_time_tasks']}/{r['total_tasks']}|{r['ground_peak_600s']['utilization']:.2%}/{r['ground_whole_utilization']:.2%}|{n['count']}/{g['count']}|{n['total_s']:.1f}/{g['total_s']:.1f}|{r['wall_seconds']:.1f}|")
    lines += ["", f"第一阶段a–d：{json.dumps(first['criteria'], ensure_ascii=False)}；失败：{first['failures']}。",
              f"第二阶段：{'未运行' if second is None else '已运行，失败'+str(second['failures'])}。", "", "## 持续时间分布"]
    for name, r in rows.items():
        for kind in ("node", "network"):
            lines.append(f"- {name}/{kind}：`{json.dumps(r['multisource'][kind+'_distribution'], ensure_ascii=False)}`")
    lines += ["", "每个连续事件的起止、节点、源星与任务集合在各方法JSON的multisource中；源星组合变化不拆事件，全网时长为节点事件并集。0.1秒采样且片段边界截断，不声称连续物理时间精确测量。",
              "", "## 信息与协议缺口", "", f"`{json.dumps(pairs, ensure_ascii=False)}`",
              "", "全局与局部差距含观测范围、通告时效、未知地面队列造成的报价/回退限制，不等于已证明的学习收益；零代价上界缺口还含有限带宽与服务时间，不全部归因于调度。",
              "", "## 全天工作量与现实性", "", f"maritime80全天{p['workload']['daily_images']}图，{p['workload']['daily_raw_bytes']/1e9:.1f}GB，每源星平均{p['workload']['images_per_source_mean']:.1f}图/日。",
              "公开依据与逐项限制见父目录SIX_HOUR_PROTOCOL.md及REPORT.md：ICEYE Gen4最多500幅/星/日、Multi-Spot几十星每日数千幅，只支持幅数量级可讨论，非实际平均。约0.97TB/日为本研究幅度产品假设；ESA Sentinel-1不同产品级别分别提供TB量级参照，不能混加或赋给小星。尚未验证姿态机动、热、电与载荷占空比，不能宣称工程可执行性已证明。",
              "", "只运行批准开发片段，保留更早积压未恢复和截止后无新到达的局限；停止，不自动训练或追加实验。"]
    (ROOT / "REPORT.md").write_text("\n".join(lines)+"\n")


def panel():
    p = read(ROOT / "protocol.json")
    try:
        short.verify(p)
        first = stage_run("neutral", p)
        report(p, first)
        allowed = not first["failures"] and bool(first["criteria"] and first["criteria"]["candidate"])
        write_json(ROOT / "gate.json", {"passed": allowed, "criteria": first["criteria"],
                   "neutral_summary_sha256": sha(ROOT / "neutral/summary.json"), "protocol_sha256": sha(ROOT / "protocol.json")})
        second = stage_run("information", p) if allowed else None
        report(p, first, second)
        short.verify(p)
        failures = first["failures"]+(second["failures"] if second else [])
        write_json(ROOT / "FINAL_AUDIT.json", {"status": "PASS" if not failures else "BLOCKED",
                   "source_hashes_pass": True, "input_hashes_pass": True, "failures": failures,
                   "gate_passed": allowed, "stages": ["neutral"]+(["information"] if second else [])})
        write_json(ROOT / "final_status.json", {"status": "BLOCKED" if failures else "COMPLETE" if allowed else "STOPPED_NEUTRAL_GATE",
                   "failures": failures, "gate_passed": allowed, "stop": "Await user; no training or additional runs"})
    except Exception:
        write_json(ROOT / "final_status.json", {"status": "BLOCKED", "traceback": traceback.format_exc()})
        raise


def launch():
    p = read(ROOT / "protocol.json")
    short.verify(p)
    if (ROOT / "launch.json").exists():
        raise FileExistsError("No automatic relaunch")
    with (ROOT / "panel.log").open("xb") as log:
        env = dict(os.environ, OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
        proc = subprocess.Popen([sys.executable, str(HERE), "panel"], stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True)
    guard = subprocess.Popen(["caffeinate", "-i", "-w", str(proc.pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    data = {"pid": proc.pid, "caffeinate_pid": guard.pid, "command": f"{sys.executable} {HERE} panel",
            "created_utc": datetime.now(timezone.utc).isoformat(), "protocol_sha256": sha(ROOT / "protocol.json"),
            "workers": p["worker_counts"], "host": "mac"}
    write_json(ROOT / "launch.json", data)
    print(json.dumps(data))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "launch", "panel"))
    args = parser.parse_args()
    {"prepare": prepare, "launch": launch, "panel": panel}[args.mode]()
