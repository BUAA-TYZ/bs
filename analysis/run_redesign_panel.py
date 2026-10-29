"""M 档开发面板；基线/局部信息对照完成后停止，不启动其他档位或训练。"""

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import sys
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.redesign_scenario import digest, write_json
from analysis.run_sar_frozen import invariants
from analysis.sar_three_plan import CandidateEnv, ThreePlanChoice
from main import make_policy
from sim.config import config_from_dict
from sim.control import ControlPlane
from sim.entities import TileState
from sim.marl.decision import decision_state


METHODS = ("local", "ground", "cgs", "heft", "v3", "local_1", "local_2")


def source_hashes():
    paths = list(Path("sim").rglob("*.py")) + [Path("main.py")]
    paths += [Path("analysis") / name for name in ("run_redesign_panel.py", "redesign_scenario.py",
                                                  "sar_three_plan.py", "run_sar_frozen.py")]
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}


def neighborhood(state, source, sources, hops=2):
    known, frontier = {source}, {source}
    for _ in range(hops):
        following = {nb for sid in frontier for nb in state.neighbors.get(sid, [])
                     if nb not in known and nb not in sources}
        known.update(following)
        frontier = following
    return known - {source}


def run_job(config, method, out, frozen):
    out = Path(out)
    started = time.monotonic()
    env = None
    try:
        if source_hashes() != frozen:
            raise RuntimeError("Source hashes changed before run")
        cfg = config_from_dict(config)
        if cfg.topology["start_time_utc"][:10] not in {"2026-10-15", "2026-10-16", "2026-10-17"}:
            raise ValueError("This runner only permits the approved M development days")
        if cfg.num_sats != 36 or cfg.task_source_sat_ids != list(range(24)):
            raise ValueError("Stop boundary: this panel only permits the M tier")
        if method == "ground":
            cfg = replace(cfg, pipeline_mode="ground_unique_relay")
        env = CandidateEnv(cfg)
        sources = set(cfg.task_source_sat_ids)
        helpers = sorted(set(env.satellites)-sources)
        plane = ControlPlane(sources)
        env.control_plane = plane
        policies = ({sid: ThreePlanChoice(cfg.relay_max_hops) for sid in sources}
                    if method.startswith("local_") else {})
        policy = (ThreePlanChoice(cfg.relay_max_hops) if method == "v3" else
                  make_policy({"cgs": "cgs_compute_adapted", "heft": "heft_adapted"}.get(method, "local"), cfg.seed))
        queue_peak = {sid: {"blocks": 0, "windows": 0, "images": 0} for sid in helpers}
        indegrees, indegree_peak, structural_peak = Counter(), Counter(), Counter()
        decision_samples = 0
        decision_seconds = 0.0
        for step in range(cfg.sim_steps):
            actions = []
            if step % cfg.decision_interval_steps == 0:
                state = env.export_state()
                plane.enqueue(state)
                state = decision_state(state, cfg.decision_tile_limit_per_sat)
                degree, structural = Counter(), Counter()
                active_sources = {tile["source_sat_id"] for tile in state.tiles.values()}
                for source in sources:
                    for node in neighborhood(state, source, sources):
                        structural[node] += 1
                        degree[node] += int(source in active_sources)
                for sid in helpers:
                    indegrees[sid] += degree[sid]
                    indegree_peak[sid] = max(indegree_peak[sid], degree[sid])
                    structural_peak[sid] = max(structural_peak[sid], structural[sid])
                decision_samples += 1
                before = time.perf_counter()
                if method.startswith("local_"):
                    hops = int(method[-1])
                    for source in sorted(active_sources):
                        actions.extend(policies[source].select_actions(plane.view(state, source, hops)))
                elif method != "ground":
                    actions = policy.select_actions(state)
                decision_seconds += time.perf_counter() - before
            env.step(actions)
            for sid in helpers:
                sat = env.satellites[sid]
                tids = {tid for tid in sat.queue if env.tiles[tid].state in (TileState.READY, TileState.RUNNING)}
                tids.update(sat.executing_batch)
                queue_peak[sid]["blocks"] = max(queue_peak[sid]["blocks"], len(tids))
                queue_peak[sid]["windows"] = max(queue_peak[sid]["windows"], sum(
                    env.tiles[tid].compute_cost-env.tiles[tid].completed_compute_windows for tid in tids))
                queue_peak[sid]["images"] = max(queue_peak[sid]["images"], len({env.tiles[t].parent_task_id for t in tids}))
            if (step+1) % 6000 == 0:
                write_json(out / "progress" / f"{method}.json", {"status": "RUNNING", "pid": os.getpid(),
                    "sim_seconds": env.time*cfg.dt, "wall_seconds": time.monotonic()-started,
                    "updated_utc": datetime.now(timezone.utc).isoformat()})
        metrics = env.summary()
        checks = invariants(env)
        foreign_compute = [(record["sat_id"], tid) for record in env.compute_cohort_records.values()
                           for tid in record["tile_ids"] if record["sat_id"] in sources
                           and env.tasks[env.tiles[tid].parent_task_id].source_sat_id != record["sat_id"]]
        incoming = {key: value-plane.sent_mb_by_edge[key] for key,value in env.metrics.network_sent_mb.items()
                    if key.startswith("isl:") and int(key.split("->")[-1]) in sources}
        checks["roles"] = {"foreign_compute_windows": len(foreign_compute),
                           "sar_received_task_data_mb": sum(incoming.values()),
                           "role_rejection_failures": env.metrics.failure_reasons.get("role_forbidden", 0)}
        checks["passed"] &= not foreign_compute and all(abs(v) <= 1e-6 for v in incoming.values())
        unique_errors = []
        raw_tasks = set(env._dist_pipeline.raw_tasks)
        if method == "ground":
            raw_tasks = set(env.tasks)
        for task_id in raw_tasks:
            task = env.tasks[task_id]
            if not math.isclose(sum(env.tiles[t].data_size_mb for t in task.tile_ids), task.image_size_mb, abs_tol=1e-6):
                unique_errors.append(task_id)
        checks["unique_raw_errors"] = unique_errors
        checks["passed"] &= not unique_errors and source_hashes() == frozen
        if not checks["passed"]:
            write_json(out / f"{method}_audit_failure.json", {"checks": checks, "metrics": metrics})
            raise RuntimeError("Execution audit failed; see audit_failure.json")
        outcomes = metrics["task_outcomes"]
        on_time = [row["task_id"] for row in outcomes if row["status"] == "completed" and row["latency_seconds"] <= 300]
        restricted = [min(row["latency_seconds"],300) if row["status"] == "completed" else 300 for row in outcomes]
        work = {sid: env.metrics.compute_busy_time.get(sid,0) for sid in helpers}
        total_work = sum(work.values())
        top = math.ceil(len(helpers)*.1)
        competition = {"candidate_indegree_definition": "Current <=2-hop role-legal graph from distinct sources with queued work; topology candidates, before capacity/time-window admission",
            "mean_candidate_indegree": sum(indegrees.values())/(len(helpers)*decision_samples),
            "peak_candidate_indegree": max(indegree_peak.values(), default=0),
            "peak_structural_indegree": max(structural_peak.values(), default=0),
            "top10_percent_nodes": top,
            "top10_percent_compute_share": sum(sorted(work.values(), reverse=True)[:top])/total_work if total_work else None,
            "helper_utilization": total_work/(len(helpers)*env.time*cfg.dt),
            "peak_queue": {key: max(row[key] for row in queue_peak.values()) for key in ("blocks","windows","images")},
            "per_compute_satellite": {sid: {"busy_seconds": work[sid], "peak_queue": queue_peak[sid],
                                           "mean_candidate_indegree": indegrees[sid]/decision_samples,
                                           "peak_candidate_indegree": indegree_peak[sid]} for sid in helpers}}
        decisions = ([row for p in policies.values() for row in p.decisions] if policies else getattr(policy,"decisions",[]))
        record = {"status":"PASS", "method":method, "config_sha256":digest(config), "D_seconds":300,
                  "total_tasks": len(outcomes), "on_time_tasks":len(on_time), "on_time_task_ids":on_time,
                  "P_D":len(on_time)/len(outcomes) if outcomes else None,
                  "R_D_seconds":sum(restricted)/len(restricted) if restricted else None,
                  "wall_seconds":time.monotonic()-started, "decision_seconds":decision_seconds,
                  "competition":competition, "control":dict(plane.stats), "audit":checks,
                  "outcomes":outcomes, "decisions":decisions, "source_hashes":frozen}
        write_json(out / f"{method}.json", record)
        write_json(out / "progress" / f"{method}.json", {"status":"PASS", "wall_seconds":record["wall_seconds"]})
        return {k: record[k] for k in ("method","status","P_D","R_D_seconds","wall_seconds")}
    except Exception:
        failure = {"status":"FAILED", "method":method, "traceback":traceback.format_exc(), "config":config}
        write_json(out / f"{method}_FAILED.json", failure)
        return {k: failure[k] for k in ("method","status","traceback")}
    finally:
        if env is not None:
            env.close()


def summarize(out, methods):
    records = {name: json.loads((out/f"{name}.json").read_text()) for name in methods if (out/f"{name}.json").exists()}
    failed = [name for name in methods if name not in records]
    pairs = {}
    if "v3" in records:
        candidate = records["v3"]
        for name, row in records.items():
            if name == "v3":
                continue
            pairs[name] = {"P_D_difference":candidate["P_D"]-row["P_D"],
                           "R_D_difference_seconds":candidate["R_D_seconds"]-row["R_D_seconds"],
                           "gained":sorted(set(candidate["on_time_task_ids"])-set(row["on_time_task_ids"])),
                           "lost":sorted(set(row["on_time_task_ids"])-set(candidate["on_time_task_ids"]))}
    write_json(out/"summary.json", {"failed":failed, "v3_paired":pairs,
        "rows":{name:{k:v for k,v in row.items() if k not in ("source_hashes","outcomes","decisions")}
                for name,row in records.items()}})
    lines = ["# M 档开发日结果", "", "仅开发日；D=300 s，全部图像至少随访 D。R_D 是未交付图计 D 的截断均值。",
             "", "| 方法 | 按时/总数 | P_D | R_D(s) | 候选入度均值/峰值 | 前10%负载份额 | 峰值队列 块/窗/图 |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for name,row in records.items():
        c=row["competition"]; q=c["peak_queue"]; share=c["top10_percent_compute_share"]
        lines.append(f"|{name}|{row['on_time_tasks']}/{row['total_tasks']}|{row['P_D']:.4%}|{row['R_D_seconds']:.3f}|"
                     f"{c['mean_candidate_indegree']:.4f}/{c['peak_candidate_indegree']}|"
                     f"{f'{share:.2%}' if share is not None else 'N/A'}|{q['blocks']}/{q['windows']}/{q['images']}|")
    lines += ["", "## 解释边界", "",
              "候选入度按当前两跳拓扑中的活跃源星数统计，是可用资源准入前的候选，不等于成功卸载。前10%向上取整为2颗计算星；无计算负载时份额为N/A。队列峰值包含就绪和正在执行，排除仅存转的数据。",
              "局部规则仅读取已经送达且未过期的状态通告；未知地面计算队列使地面处理报价不可判定，三案无完整报价时固定本地回退。公共多跳数据路由相同。全局与局部的差距同时包含信息可见范围与可报价动作的差异，不能直接称为可学习提升空间或最优上界。",
              "模型识别与预处理沿用既有4096窗推理代理；新整图预处理与流式展开内存峰值待Orin可用后验证。合并采用本机参考测量，并使用统一串行合并队列。",
              "", "逐图结果、报价及源码/配置哈希见各方法JSON；增减任务见summary.json。",
              "", f"失败任务：{failed}。失败不作为P_D=0。测试日期未访问；未训练；在M档停止。"]
    (out/"REPORT.md").write_text("\n".join(lines)+"\n")


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--methods",nargs="+",choices=METHODS,default=METHODS)
    parser.add_argument("--prefix",type=int)
    parser.add_argument("--workers",type=int,default=4)
    args=parser.parse_args()
    cfg=json.loads(args.config.read_text())
    if args.prefix is not None:
        cfg["task_arrival_end_step"]=round(args.prefix/cfg["dt"])
        cfg["sim_steps"]=round((args.prefix+301)/cfg["dt"])
        cfg["scheduled_task_arrivals"]=[e for e in cfg["scheduled_task_arrivals"] if e["release_step"]<cfg["task_arrival_end_step"]]
    args.output.mkdir(parents=True,exist_ok=False)
    frozen=source_hashes()
    write_json(args.output/"protocol.json", {"config":cfg,"config_sha256":digest(cfg),"source_hashes":frozen,
        "methods":args.methods,"workers":args.workers,"pid":os.getpid(),"scope":"M development only",
        "hypothesis":"Global v3 may improve delivery over local rules under scarce compute access; equal/worse performance does not support that hypothesis.",
        "sealed_tests":True,"stop_after":"M report; no S/L/XL baseline or training"})
    with ProcessPoolExecutor(max_workers=args.workers,mp_context=multiprocessing.get_context("spawn")) as pool:
        jobs={pool.submit(run_job,cfg,name,str(args.output),frozen):name for name in args.methods}
        statuses=[]
        for future in as_completed(jobs):
            status=future.result()
            statuses.append(status)
            write_json(args.output/"progress.json",{"done":len(statuses),"total":len(jobs),"statuses":statuses})
            print(json.dumps(status),flush=True)
    summarize(args.output,args.methods)
    write_json(args.output/"final_status.json",{"status":"COMPLETE" if all(s["status"]=="PASS" for s in statuses) else "FAILED",
                                               "done":len(statuses),"source_hashes_match":source_hashes()==frozen})


if __name__ == "__main__":
    main()
