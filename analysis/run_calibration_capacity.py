"""独立单站容量诊断：本地、强地面、首次接入最近计算星FIFO。"""

import argparse
from collections import Counter, defaultdict
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
import subprocess
import sys
import time
import traceback

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.calibrate_redesign_geometry import configuration
from analysis.redesign_scenario import digest, write_json
from analysis.run_redesign_panel import source_hashes
from analysis.run_sar_frozen import invariants
from analysis.sar_three_plan import CandidateEnv
from sim.config import config_from_dict
from sim.control import ControlPlane
from sim.entities import Action, ActionType, TileState
from sim.marl.decision import decision_state
from sim.orbit import position_km
from sim.scheduler.local import LocalOnlyPolicy
from sim.topology import link_key

ROOT = Path("results/redesign_20261021/calibration/capacity_20261021")
METHODS = ("local", "ground", "fifo")
MULTIPLIERS = (1, 4, 10)
IMPLEMENTATION = (Path(__file__), Path("analysis/calibrate_redesign_geometry.py"),
                  Path("tests/test_calibration_capacity.py"), ROOT/"PROTOCOL.md")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def implementation_hashes():
    return {str(p):sha(p) for p in IMPLEMENTATION}


def scaled_config(multiplier):
    cfg = configuration(144)
    cfg["compute_rate"] /= multiplier
    if cfg.get("satellite_compute_rates") is not None:
        cfg["satellite_compute_rates"] = [r/multiplier for r in cfg["satellite_compute_rates"]]
    for size in cfg["onboard_compute_profile"]["service_s"]:
        cfg["onboard_compute_profile"]["service_s"][size] *= multiplier
    for gs in cfg["ground_stations"]:
        gs["compute_rate"] /= multiplier
    return cfg


class FifoDiagnostic:
    def __init__(self):
        self.assignments = {}

    def select_actions(self, env, state):
        links = env._get_links(env.time)
        sources = set(env.task_sources)
        available = {sid:sat.mem_capacity_gb-sat.mem_used_gb for sid,sat in env.satellites.items()}
        actions, blocked = [], set()
        pending = defaultdict(list)
        for tid,row in state.tiles.items():
            tile = env.tiles[tid]
            task = env.tasks[tile.parent_task_id]
            if tile.location == task.source_sat_id and tile.state == TileState.QUEUED and not row["in_transfer"]:
                pending[task.task_id].append(tid)
        for task_id in sorted(pending, key=lambda tid:(env.tasks[tid].release_time, int(tid.split("_")[-1]))):
            task = env.tasks[task_id]
            source = task.source_sat_id
            if task_id not in self.assignments:
                neighbors = [sid for sid in state.neighbors[source] if sid not in sources
                             and links[link_key(source,sid)].up and links[link_key(source,sid)].bandwidth_mbps>0]
                if not neighbors:
                    continue
                origin=np.array(position_km(env.topology.sat_recs[source],env.topology.t0,env.time*env.cfg.dt))
                choices=[(float(np.linalg.norm(np.array(position_km(env.topology.sat_recs[sid],env.topology.t0,
                                                   env.time*env.cfg.dt))-origin)),sid) for sid in neighbors]
                distance,target=min(choices)
                self.assignments[task_id]={"target":target,"source":source,"selected_s":env.time*env.cfg.dt,
                                           "distance_km":distance,"rule":"first_live_contact_nearest_no_reselection"}
            target=self.assignments[task_id]["target"]
            link=links.get(link_key(source,target))
            if target in blocked or link is None or not link.up:
                continue
            eligible=set(pending[task_id])
            for tid in task.tile_ids:
                if tid not in eligible:
                    continue
                tile=env.tiles[tid]
                size=0. if target in tile.memory_holders else env.memory_size_gb(tile.data_size_mb)
                if size>available[target]+1e-12:
                    blocked.add(target)
                    break
                available[target]-=size
                actions.append(Action(tid,ActionType.OFFLOAD,target_sat_id=target))
        return actions


def waiting_snapshot(env, helper_ids, enabled=True):
    """只统计已落地等待计算的数据，严格整图判据排除运行中图的剩余块。"""
    if not enabled:
        return []
    rows=[]
    for sid in helper_ids:
        sat=env.satellites[sid]
        if not sat.queue:
            continue
        running=set(sat.executing_batch)
        waiting={tid for tid in sat.queue if tid not in running and env.tiles[tid].state==TileState.READY
                 and env.tiles[tid].location==sid and (env.tiles[tid].timestamps.end_tx is None
                     or env.tiles[tid].timestamps.end_tx<env.time)}
        if not waiting:
            continue
        tasks={env.tiles[tid].parent_task_id for tid in waiting}
        full=[tid for tid in sorted(tasks) if set(env.tasks[tid].tile_ids)<=waiting
              and all(env.tiles[t].completed_compute_windows==0 for t in env.tasks[tid].tile_ids)]
        sources=sorted({env.tasks[tid].source_sat_id for tid in tasks})
        rows.append({"sat_id":sid,"blocks":len(waiting),
                     "windows":sum(env.tiles[tid].compute_cost-env.tiles[tid].completed_compute_windows for tid in waiting),
                     "waiting_images":len(tasks),"complete_waiting_images":len(full),
                     "waiting_source_count":len(sources),"source_ids":sources,
                     "task_ids":sorted(tasks),"complete_task_ids":full})
    return rows


def busiest_window(series, width=600):
    width=min(width,len(series))
    if width<1:
        return {"start_s":0,"end_s":0,"busy_s":0.,"duration_s":0}
    prefix=np.concatenate(([0.],np.cumsum(series,dtype=np.float64)))
    sums=prefix[width:]-prefix[:-width]
    start=int(np.argmax(sums))
    return {"start_s":start,"end_s":start+width,"busy_s":float(sums[start]),"duration_s":width}


class Competition:
    def __init__(self, env, method):
        self.helpers=sorted(set(env.satellites)-set(env.task_sources))
        self.indices={sid:i for i,sid in enumerate(self.helpers)}
        self.busy=np.zeros((min(86400,math.ceil(env.cfg.sim_steps*env.cfg.dt)),len(self.helpers)),dtype=np.float64)
        self.previous={}
        self.method=method
        self.peak={k:0 for k in ("blocks","windows","waiting_images","complete_waiting_images","waiting_source_count")}
        self.witness={}
        self.seconds_observed=0
        self.qualifying_queue_seconds=0.
        self.any_queue_seconds=0.

    def observe(self, env):
        now=env.time*env.cfg.dt
        if now>86400+1e-9:
            return
        queue_rows=waiting_snapshot(env,self.helpers,enabled=self.method=="fifo")
        self.any_queue_seconds+=env.cfg.dt*bool(queue_rows)
        self.qualifying_queue_seconds+=env.cfg.dt*any(r["waiting_source_count"]>=2 or r["complete_waiting_images"]>=1 for r in queue_rows)
        for row in queue_rows:
            for key in self.peak:
                if row[key]>self.peak[key]:
                    self.peak[key]=row[key]
                    self.witness[key]={"observed_s":now,**row}
        if env.time % round(1/env.cfg.dt)==0:
            second=round(now)-1
            for sid,value in env.metrics.compute_busy_time.items():
                if sid in self.indices:
                    self.busy[second,self.indices[sid]]=value-self.previous.get(sid,0.)
                    self.previous[sid]=value
            self.seconds_observed=second+1

    def summary(self):
        values=self.busy[:self.seconds_observed]
        per_sat=values.sum(axis=0)
        total=float(per_sat.sum())
        window=busiest_window(values.sum(axis=1))
        window["utilization"]=window["busy_s"]/(len(self.helpers)*window["duration_s"]) if window["duration_s"] else 0.
        hotspot=max(({"sat_id":sid,**busiest_window(values[:,i])} for i,sid in enumerate(self.helpers)),
                    key=lambda r:(r["busy_s"],-r["start_s"],-r["sat_id"]))
        hotspot["utilization"]=hotspot["busy_s"]/hotspot["duration_s"] if hotspot["duration_s"] else 0.
        top=math.ceil(len(self.helpers)*.1)
        return {"peak_queue":self.peak,"peak_witnesses":self.witness,
                "any_queue_seconds":self.any_queue_seconds,"qualifying_queue_seconds":self.qualifying_queue_seconds,
                "criterion_c":self.peak["waiting_source_count"]>=2 or self.peak["complete_waiting_images"]>=1,
                "observation_duration_s":self.seconds_observed,"helper_busy_seconds":total,
                "mean_utilization":total/(len(self.helpers)*self.seconds_observed) if self.seconds_observed else 0.,
                "busiest_600s_cluster":window,"busiest_600s_single_helper":hotspot,
                "top10_count":top,"top10_share":float(np.sort(per_sat)[-top:].sum()/total) if total else None,
                "per_helper_busy_seconds":{str(sid):float(per_sat[i]) for i,sid in enumerate(self.helpers)}}


def check_frozen(protocol):
    if source_hashes()!=protocol["source_hashes"] or implementation_hashes()!=protocol["implementation_hashes"]:
        raise RuntimeError("Frozen source or diagnostic implementation changed")


def run_job(config, method, key, out, protocol):
    out=Path(out)
    start=time.monotonic()
    env=None
    try:
        check_frozen(protocol)
        assert method in METHODS and config["num_sats"]==168
        assert config["topology"]["start_time_utc"]=="2026-10-15T00:00:00Z"
        assert len(config["ground_stations"])==1 and config["sar_source_isolation"]
        cfg=config_from_dict(config)
        if method=="ground":
            cfg=replace(cfg,pipeline_mode="ground_unique_relay")
        env=CandidateEnv(cfg)
        env.control_plane=plane=ControlPlane(set(cfg.task_source_sat_ids))
        dispatcher=FifoDiagnostic() if method=="fifo" else LocalOnlyPolicy()
        tracker=Competition(env,method)
        decisions_s=0.
        for step in range(cfg.sim_steps):
            actions=[]
            if step%cfg.decision_interval_steps==0:
                state=env.export_state()
                plane.enqueue(state)
                state=decision_state(state,cfg.decision_tile_limit_per_sat)
                before=time.perf_counter()
                if method=="fifo":
                    actions=dispatcher.select_actions(env,state)
                elif method=="local":
                    actions=dispatcher.select_actions(state)
                decisions_s+=time.perf_counter()-before
            env.step(actions)
            tracker.observe(env)
            if env.time%6000==0:
                write_json(out/"progress"/f"{key}.json",{"status":"RUNNING","pid":os.getpid(),
                    "sim_seconds":env.time*cfg.dt,"wall_seconds":time.monotonic()-start,
                    "updated_utc":datetime.now(timezone.utc).isoformat()})
        metrics=env.summary()
        checks=invariants(env)
        sources=set(cfg.task_source_sat_ids)
        wrong=[(c["sat_id"],tid) for c in env.compute_cohort_records.values() for tid in c["tile_ids"]
               if c["sat_id"] in sources and env.tasks[env.tiles[tid].parent_task_id].source_sat_id!=c["sat_id"]]
        incoming=[v-plane.sent_mb_by_edge[k] for k,v in env.metrics.network_sent_mb.items()
                  if k.startswith("isl:") and int(k.split("->")[-1]) in sources]
        checks["role_errors"]=wrong
        checks["source_incoming_data_mb"]=math.fsum(incoming)
        checks["passed"] &= not wrong and all(abs(x)<=1e-6 for x in incoming)
        if method=="fifo":
            checks["fifo_source_compute_seconds"]=sum(env.metrics.compute_busy_time.get(sid,0) for sid in sources)
            checks["passed"] &= checks["fifo_source_compute_seconds"]==0
            wrong_targets=[tid for c in env.compute_cohort_records.values() for tid in c["tile_ids"]
                           if c["sat_id"]!=dispatcher.assignments[env.tiles[tid].parent_task_id]["target"]]
            checks["fifo_target_errors"]=wrong_targets
            checks["passed"] &= not wrong_targets
        if method=="ground":
            checks["unique_raw_errors"]=[tid for tid,t in env.tasks.items() if not math.isclose(
                sum(env.tiles[i].data_size_mb for i in t.tile_ids),t.image_size_mb,abs_tol=1e-6)]
            checks["passed"] &= not checks["unique_raw_errors"] and not env.compute_cohort_records
        check_frozen(protocol)
        outcomes=metrics["task_outcomes"]
        assert len(outcomes)==len(config["scheduled_task_arrivals"])
        if protocol["scope"]=="full_day":
            assert all(o["observed_age_seconds"]>=300 for o in outcomes)
        on_time=[o["task_id"] for o in outcomes if o["status"]=="completed" and o["latency_seconds"]<=300]
        restricted=[min(o["latency_seconds"],300) if o["status"]=="completed" else 300 for o in outcomes]
        record={"status":"PASS" if checks["passed"] else "AUDIT_MISMATCH","key":key,"method":method,
                "config_sha256":digest(config),"total_tasks":len(outcomes),"on_time_tasks":len(on_time),
                "on_time_task_ids":on_time,"P_D":len(on_time)/len(outcomes) if outcomes else None,
                "R_D_seconds":sum(restricted)/len(restricted) if restricted else None,
                "wall_seconds":time.monotonic()-start,"decision_seconds":decisions_s,
                "competition":tracker.summary(),"audit":checks,"outcomes":outcomes,
                "assignments":dispatcher.assignments if method=="fifo" else {},
                "control":dict(plane.stats),"source_hashes":protocol["source_hashes"],
                "implementation_hashes":protocol["implementation_hashes"]}
        np.savez_compressed(out/f"{key}_busy_1s.npz",busy=tracker.busy[:tracker.seconds_observed],helper_ids=tracker.helpers)
        write_json(out/f"{key}.json",record)
        write_json(out/"progress"/f"{key}.json",{"status":record["status"],"wall_seconds":record["wall_seconds"]})
        return {k:record[k] for k in ("key","status","wall_seconds")}
    except Exception:
        record={"status":"FAILED","key":key,"method":method,"traceback":traceback.format_exc(),"config":config}
        write_json(out/f"{key}_FAILED.json",record)
        return {"key":key,"status":"FAILED","traceback":record["traceback"]}
    finally:
        if env is not None:
            env.close()


def summarize(out, protocol):
    check_frozen(protocol)
    records={k:json.loads((out/f"{k}.json").read_text()) for k in protocol["jobs"] if (out/f"{k}.json").exists()}
    failures=[k for k in protocol["jobs"] if k not in records or records[k]["status"]!="PASS"]
    if failures:
        write_json(out/"final_status.json",{"status":"BLOCKED","failed":failures,"completed":len(records),
                                            "total":len(protocol["jobs"]),"no_automatic_retries":True})
        return
    rows={}
    upper=protocol["upper_bound_fraction"]
    for mult in protocol["multipliers"]:
        local=records[f"compute_{mult}_local"]
        fifo=records[f"compute_{mult}_fifo"]
        rows[str(mult)]={"upper_bound":upper,"local_gap_pp":100*(upper-local["P_D"]),
                        "criterion_a":upper>=.85,"criterion_b":upper-local["P_D"]>=.15-1e-12,
                        "criterion_c":fifo["competition"]["criterion_c"]}
        rows[str(mult)]["eligible"]=all(rows[str(mult)][k] for k in ("criterion_a","criterion_b","criterion_c"))
    need_load=not any(row["criterion_c"] for row in rows.values())
    status="SMOKE_PASS" if protocol["scope"]=="smoke" else "NEEDS_AUTHORIZED_LOAD_EXPANSION" if need_load else "COMPLETE"
    summary={"status":status,"criteria":rows,"load_expansion_triggered":need_load if protocol["scope"]=="full_day" else False,
             "candidate_multiplier":next((int(k) for k,r in rows.items() if r["eligible"]),None),
             "methods":{k:{f:r[f] for f in ("method","total_tasks","on_time_tasks","P_D","R_D_seconds","competition","wall_seconds")}
                        for k,r in records.items()},"sealed_tests_accessed":False}
    write_json(out/"summary.json",summary)
    lines=["# 单站144计算星容量诊断", "", "FIFO为诊断规则，不参与方法排名。D=300秒；计算倍率同时作用于星地。上界复用同轨道、同到达136图的零代价松弛值100%。",
           "", "|倍率|规则|按时/图数|P_D|R_D(s)|等待块/窗峰值|完整等待图峰值|同节点等待源星峰值|集群最忙10分钟利用率|单星最忙10分钟利用率|前15星份额|", "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for mult in protocol["multipliers"]:
        for method in METHODS:
            r=records[f"compute_{mult}_{method}"]
            c=r["competition"]; q=c["peak_queue"]
            share="N/A" if c["top10_share"] is None else f"{c['top10_share']:.2%}"
            lines.append(f"|{mult}|{method}|{r['on_time_tasks']}/{r['total_tasks']}|{r['P_D']:.2%}|{r['R_D_seconds']:.3f}|"
                         f"{q['blocks']}/{q['windows']}|{q['complete_waiting_images']}|{q['waiting_source_count']}|"
                         f"{c['busiest_600s_cluster']['utilization']:.3%}|{c['busiest_600s_single_helper']['utilization']:.2%}|{share}|")
    lines += ["", "|倍率|上界-本地(pp)|a:覆盖|b:差距|c:排队|三项同时满足|", "|---:|---:|---|---|---|---|"]
    for k,r in rows.items():
        lines.append(f"|{k}|{r['local_gap_pp']:.3f}|{r['criterion_a']}|{r['criterion_b']}|{r['criterion_c']}|{r['eligible']}|")
    lines += ["", f"状态：{status}；候选倍率：{summary['candidate_multiplier']}。",
              "", "完整等待图要求所有分片到达且未开始计算；READY队列的窗口数不含运行批次。局部热点存在不等于全144星算力饱和，也不证明学习方法有收益。峰值证据的时刻、源星/任务ID与最忙窗口起止见各记录competition；busy_1s.npz保存每星每秒忙时以独立复算。",
              "", "所有原源码和新诊断入口哈希一致，工作量/网络/角色审计通过。未实施千星加速、未运行多站、未训练、未访问测试。"]
    (out/"REPORT.md").write_text("\n".join(lines)+"\n")
    write_json(out/"final_status.json",{"status":status,"completed":len(records),"total":len(protocol["jobs"]),
                                        "candidate_multiplier":summary["candidate_multiplier"]})


def prepare(out, smoke=False):
    out.mkdir(parents=True,exist_ok=False)
    jobs={}
    for mult in ((10,) if smoke else MULTIPLIERS):
        cfg=scaled_config(mult)
        if smoke:
            cfg["scheduled_task_arrivals"]=[e for e in cfg["scheduled_task_arrivals"] if e["release_step"]<12000]
            cfg["task_arrival_end_step"]=12000
            cfg["sim_steps"]=18000
        for method in METHODS:
            key=f"compute_{mult}_{method}"
            jobs[key]={"method":method,"config":cfg,"config_sha256":digest(cfg)}
    upper_path=Path("results/redesign_20261021/calibration/bounds_144.json")
    upper=json.loads(upper_path.read_text())
    assert upper["results"]["base"]["counts"]["single"]["300"]==136
    protocol={"scope":"smoke" if smoke else "full_day","jobs":jobs,"multipliers":[10] if smoke else list(MULTIPLIERS),
              "source_hashes":source_hashes(),"implementation_hashes":implementation_hashes(),
              "upper_bound_fraction":1.,"upper_bound_file_sha256":sha(upper_path),"sealed_tests":True,
              "created_utc":datetime.now(timezone.utc).isoformat()}
    write_json(out/"protocol.json",protocol)
    return protocol


def panel(out, workers, smoke=False):
    protocol=prepare(out,smoke)
    started=time.monotonic()
    try:
        with ProcessPoolExecutor(max_workers=workers,mp_context=multiprocessing.get_context("spawn")) as pool:
            futures={pool.submit(run_job,j["config"],j["method"],key,str(out),protocol):key for key,j in protocol["jobs"].items()}
            statuses=[]
            for future in as_completed(futures):
                try:
                    status=future.result()
                except Exception:
                    status={"key":futures[future],"status":"FAILED","traceback":traceback.format_exc()}
                    write_json(out/f"{futures[future]}_FAILED.json",{**status,"job":protocol["jobs"][futures[future]]})
                statuses.append(status)
                write_json(out/"progress.json",{"done":len(statuses),"total":len(futures),"statuses":statuses,
                    "wall_seconds":time.monotonic()-started,"pid":os.getpid()})
                print(json.dumps(status),flush=True)
        summarize(out,protocol)
    except Exception:
        write_json(out/"final_status.json",{"status":"BLOCKED","traceback":traceback.format_exc()})
        raise


def launch(out,workers):
    smoke_status=json.loads((ROOT/"smoke_v2"/"final_status.json").read_text())
    if smoke_status["status"]!="SMOKE_PASS":
        raise RuntimeError("Smoke must pass before full-day launch")
    if out.exists():
        raise FileExistsError(out)
    log=ROOT/"panel.log"
    with log.open("xb") as f:
        process=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),"panel","--out",str(out),"--workers",str(workers)],
                                 stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
    guard=subprocess.Popen(["caffeinate","-i","-w",str(process.pid)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    data={"pid":process.pid,"caffeinate_pid":guard.pid,"output":str(out),"log":str(log),
          "command":"run_calibration_capacity.py panel", "created_utc":datetime.now(timezone.utc).isoformat()}
    write_json(ROOT/"launch.json",data)
    print(json.dumps(data))


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode",choices=("smoke","panel","launch","report"))
    p.add_argument("--out",type=Path)
    p.add_argument("--workers",type=int,default=3)
    args=p.parse_args()
    target=args.out or ROOT/("smoke_v2" if args.mode=="smoke" else "load_1")
    if args.mode=="launch":
        launch(target,args.workers)
    elif args.mode=="report":
        summarize(target,json.loads((target/"protocol.json").read_text()))
    else:
        panel(target,args.workers,args.mode=="smoke")
