"""冻结 v3 的离线反事实试点；不训练，不读取测试轨迹，不修改冻结源码。"""

import argparse
import ast
from collections import Counter
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import pickle
import random
import subprocess
import sys
import textwrap
import threading
import time
import traceback

PROJECT = Path(__file__).resolve().parents[1]
FROZEN = PROJECT / "results/sar_v3_frozen_20261017"
OUT = PROJECT / "results/sar_selector_pilot_20261019"
sys.path.insert(0, str(FROZEN / "source"))

import numpy as np
from analysis import sar_three_plan as candidate
from analysis.sar_condition_scan import derive
from sim.config import config_from_dict
from sim.marl.decision import decision_state

CONDITIONS = {"base": (1, 1, 2), "compute_4": (4, 1, 2), "compute_10": (10, 1, 2),
              "load_2": (1, 2, 2), "load_4": (1, 4, 2)}
PLANS = ("local", "cgs", "ground")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    tmp.replace(path)


def read(path):
    return json.loads(Path(path).read_text())


def frozen_audit():
    freeze = read(FROZEN / "freeze.json")
    for name, expected in freeze["files"].items():
        if sha(FROZEN / "source" / name) != expected:
            raise ValueError(f"Frozen snapshot changed: {name}")
    return len(freeze["files"])


def choose_label(branches):
    """删失分支只有在其下界不可能击败已完成赢家时才允许确定标签。"""
    if any(r["status"] == "FAILED" for r in branches.values()):
        return {"status": "ERROR", "label": None, "ties": []}
    complete = [p for p in PLANS if branches[p]["completion_seconds"] is not None]
    if not complete:
        return {"status": "INDETERMINATE", "label": None, "ties": []}
    best = min(complete, key=lambda p: branches[p]["completion_seconds"])
    finish = branches[best]["completion_seconds"]
    if any(r["completion_seconds"] is None and r["censor_seconds"] <= finish
           for r in branches.values()):
        return {"status": "INDETERMINATE", "label": None, "ties": []}
    ties = [p for p in complete if branches[p]["completion_seconds"] == finish]
    return {"status": "DETERMINATE", "label": best, "ties": ties}


def instrumented_select():
    # 在冻结函数中仅插入离线选择钩子；保留同轮影子日历，避免事后替换动作。
    tree = ast.parse(textwrap.dedent(inspect.getsource(candidate.ThreePlanChoice.select_actions)))
    inserted = [0, 0]

    class Hook(ast.NodeTransformer):
        def visit_Assign(self, node):
            if (len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                    and node.targets[0].id == "winner" and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Name) and node.value.func.id == "min"):
                inserted[0] += 1
                return [ast.parse("prior_calendar = calendar").body[0], node]
            return node

        def visit_Expr(self, node):
            if ast.unparse(node) == "chosen.extend(options[winner])":
                inserted[1] += 1
                hook = ast.parse("winner, calendar = self.intervene(task, state, subset, options, "
                                 "quotes, details, trials, winner, prior_calendar, calendar)").body[0]
                return [hook, node]
            return node

    Hook().visit(tree)
    assert inserted == [1, 1], inserted
    ast.fix_missing_locations(tree)
    namespace = dict(vars(candidate))
    exec(compile(tree, "<frozen-v3-offline-intervention>", "exec"), namespace)
    return namespace["select_actions"], ast.unparse(tree) + "\n"


class PilotChoice(candidate.ThreePlanChoice):
    select_actions, instrumented_source = instrumented_select()

    def intervene(self, *args):
        return self.controller.intervene(*args)


def prepare():
    if OUT.exists():
        raise ValueError("Pilot directory already exists; do not overwrite preregistration")
    count = frozen_audit()
    OUT.mkdir()
    blind = {"registered_at_utc": datetime.now(timezone.utc).isoformat(),
             "dates": ["2026-12-15", "2026-12-16", "2026-12-17"],
             "conditions": list(CONDITIONS), "status": "SEALED_NOT_GENERATED_NOT_RUN",
             "selection_reason": "Three calendar days starting one week after the week containing the consumed Dec 1-6 tests; no geometry or outcomes inspected",
             "exclude_from_training_validation_labels": True,
             "forbidden_consumed_test_dates": [f"2026-12-{d:02d}" for d in range(1, 7)],
             "forbidden_old_optical_test_dates": [f"2026-11-{d:02d}" for d in range(21, 27)],
             "train_dates": [f"2026-10-{d}" for d in range(15, 22)],
             "validation_dates": [f"2026-10-{d}" for d in range(22, 25)],
             "future_test_requires_frozen_learned_selector_and_user_authorization": True}
    write(OUT / "blind_dates.json", blind)
    base_path = PROJECT / "results/sar_loopfree_20261017/main_config.json"
    base = read(base_path)
    assert base["topology"]["start_time_utc"].startswith("2026-10-15")
    assert base["topology"]["visibility_workers"] == 1 and not base["tile_lifecycle_log"]
    freeze = read(FROZEN / "freeze.json")
    jobs = []
    for ci, (condition, factors) in enumerate(CONDITIONS.items()):
        cfg = derive(base, *factors)
        # 标签回放允许末图继续观察两小时；到达截止仍是原开发日24:00。
        cfg["sim_steps"] = round((86400 + 7200 + 1) / cfg["dt"])
        events = sorted(cfg["scheduled_task_arrivals"], key=lambda e: e["release_step"])
        rng = np.random.default_rng(20261019 + ci)
        bins = np.array_split(np.arange(len(events)), 8)
        indices = [int(rng.choice(b)) for b in bins]
        config_path = OUT / "configs" / f"{condition}.json"
        write(config_path, cfg)
        jobs.append({"condition": condition, "config": str(config_path), "config_sha256": sha(config_path),
                     "raw_hops": freeze["ground_hops"][condition], "sampling_seed": 20261019 + ci,
                     "samples": [{"task_id": f"task_{i}", "arrival_index": i,
                                  "release_seconds": events[i]["release_step"] * cfg["dt"]} for i in indices]})
    manifest = {"registered_at_utc": datetime.now(timezone.utc).isoformat(), "pilot_date": "2026-10-15",
                "blind_dates_sha256": sha(OUT / "blind_dates.json"), "base_config_sha256": sha(base_path),
                "frozen_snapshot_files_verified": count, "freeze_sha256": sha(FROZEN / "freeze.json"),
                "runner_sha256": sha(__file__), "jobs": jobs, "states": 40, "branches": 120,
                "branch_horizon_seconds": 7200, "max_condition_workers": 5,
                "sampling": "8 equal-count strata over arrival-ordered images, one fixed-seed uniform draw each; first whole-image selection only; no replacement if missing",
                "intervention": "Fork complete process at first whole-image selection after all three quotes, before commit; force only this decision, including its shadow calendar; all subsequent choices remain frozen v3",
                "null_quote": "Force the existing plan even when its quote is None; retain pre-selection calendar if no finite quote, do not treat prediction failure as physical infeasibility",
                "state_clone": "POSIX copy-on-write fork of environment, local call stack, calendar, policy, caches and RNG; global Python RNG restored after at-fork reseeding; each branch starts with same fingerprint",
                "audit": "Unforced instrumented selector compared with original at every parent decision; chosen-plan branch target completion must exactly match parent, or both censored at same bound",
                "agreement": "number of determinate states whose fastest label (exact ties local,cgs,ground) equals v3 / determinate states; also report tie-aware agreement",
                "indeterminate": "no uniquely selectable fastest observed plan after censor lower-bound check / all 40 sampled states; execution errors are separate and block a pass",
                "stop": "agreement >= 0.90 OR indeterminate_fraction > 0.40 => STOP; otherwise PILOT_PASS_AWAIT_CONFIRMATION; never automatically generate full labels or train",
                "missing_or_failed_state": "audit failure, missing first whole-image decision, branch exception or missing record => BLOCKED; do not silently replace samples",
                "scope": "pilot only; no learner, no training, no new blind-test geometry, no method/routing/physics changes"}
    write(OUT / "manifest.json", manifest)
    write(OUT / "registration_hashes.json", {"blind_dates.json": sha(OUT / "blind_dates.json"),
                                           "manifest.json": sha(OUT / "manifest.json")})
    (OUT / "instrumented_select.py.txt").write_text(PilotChoice.instrumented_source)
    print(json.dumps({"status": "PREREGISTERED", "blind_sha256": manifest["blind_dates_sha256"],
                      "sample_tasks": {j["condition"]: [s["task_id"] for s in j["samples"]] for j in jobs}}), flush=True)


class Controller:
    def __init__(self, job, env, policy):
        self.job, self.env, self.policy = job, env, policy
        self.samples = {s["task_id"]: s for s in job["samples"]}
        self.captured = {}
        self.branch = None
        self.started = time.monotonic()
        self.cpu_started = time.process_time()
        self.audit_calls = 0

    def fingerprint(self, state, options, quotes, calendar):
        payload = (asdict(state), self.env.tasks, self.env.tiles, self.env.satellites,
                   self.env.transfers, self.env.downlink_transfers, self.env.ground_tile_transfers,
                   self.env.ground_tile_queues, self.env.ground_tile_running, self.env.metrics,
                   self.env.rng.bit_generator.state, self.env.py_rng.getstate(),
                   self.env.topology.rng.getstate(), np.random.get_state(), random.getstate(),
                   self.policy.raw_tasks, options, quotes, calendar)
        return hashlib.sha256(pickle.dumps(payload, protocol=5)).hexdigest()

    def intervene(self, task, state, subset, options, quotes, details, trials, winner, prior, calendar):
        if self.branch is not None or task not in self.samples or task in self.captured:
            return winner, calendar
        assert threading.active_count() == 1, "Fork must originate in a single Python thread"
        assert self.env.topology._executor is None
        started = time.monotonic()
        now = self.env.time * self.env.cfg.dt
        destination = OUT / "states" / self.job["condition"] / task
        destination.mkdir(parents=True, exist_ok=False)
        rng_state = random.getstate()
        fingerprint = self.fingerprint(state, options, quotes, prior)
        meta = {"task_id": task, "condition": self.job["condition"], "decision_seconds": now,
                "source_sat_id": self.env.tasks[task].source_sat_id,
                "release_seconds": self.env.tasks[task].release_time * self.env.cfg.dt,
                "rule_choice": winner, "quotes_seconds": quotes, "quote_components": details,
                "state_fingerprint": fingerprint, "parent_pid": os.getpid(),
                "public_features": {"image_age_s": max(t["waiting_time"] for t in subset.tiles.values()),
                                    "work_windows": sum(t["compute_cost"] for t in subset.tiles.values()),
                                    "quotes": quotes, "finite_mask": {k: v is not None for k, v in quotes.items()}},
                "actions": {p: [asdict(a) for a in actions] for p, actions in options.items()}}
        # Enum是str枚举，可直接序列化；私有真值仅属于回放器，不进入public_features。
        write(destination / "state.json", meta)
        self.captured[task] = meta
        children = []
        sys.stdout.flush()
        sys.stderr.flush()
        for plan in PLANS:
            pid = os.fork()
            if pid == 0:
                random.setstate(rng_state)
                self.branch = {**meta, "plan": plan, "output": str(destination / f"{plan}.json"),
                               "start_wall": time.monotonic(), "start_cpu": time.process_time(),
                               "censor_step": self.env.time + round(7200 / self.env.cfg.dt),
                               "child_pid": os.getpid()}
                assert self.fingerprint(state, options, quotes, prior) == fingerprint
                return plan, trials[plan] if quotes[plan] is not None else prior
            children.append((plan, pid))
        for plan, pid in children:
            _, status = os.waitpid(pid, 0)
            path = destination / f"{plan}.json"
            if status != 0 or not path.exists():
                raise RuntimeError(f"Counterfactual child {pid}/{plan} failed, status={status}")
        branches = {p: read(destination / f"{p}.json") for p in PLANS}
        label = choose_label(branches)
        label.update(condition=self.job["condition"], task_id=task, rule_choice=winner,
                     agrees=None if label["label"] is None else label["label"] == winner,
                     tie_aware_agrees=None if label["label"] is None else winner in label["ties"],
                     branch_wall_seconds=time.monotonic() - started)
        write(destination / "label.json", label)
        self.progress()
        return winner, calendar

    def finish_branch(self, status="PASS", error=None):
        branch = self.branch
        task = self.env.tasks[branch["task_id"]]
        dt = self.env.cfg.dt
        row = {k: v for k, v in branch.items() if k not in ("start_wall", "start_cpu", "output")}
        row.update(status=status, completion_seconds=None if task.completion_time is None else task.completion_time * dt,
                   latency_seconds=None if task.completion_time is None else (task.completion_time - task.release_time) * dt,
                   censor_seconds=branch["censor_step"] * dt,
                   simulated_replay_seconds=self.env.time * dt - branch["decision_seconds"],
                   wall_seconds=time.monotonic() - branch["start_wall"],
                   cpu_seconds=time.process_time() - branch["start_cpu"], traceback=error)
        write(branch["output"], row)
        os._exit(0 if status == "PASS" else 1)

    def progress(self):
        write(OUT / "progress" / f"{self.job['condition']}.json",
              {"status": "RUNNING", "pid": os.getpid(), "simulated_seconds": self.env.time * self.env.cfg.dt,
               "captured_states": len(self.captured), "expected_states": 8,
               "wall_seconds": time.monotonic() - self.started})

    def audit_controls(self):
        audits = []
        for task_id, meta in self.captured.items():
            branch = read(OUT / "states" / self.job["condition"] / task_id / f"{meta['rule_choice']}.json")
            task = self.env.tasks[task_id]
            completion = None if task.completion_time is None else task.completion_time * self.env.cfg.dt
            branch_completion = branch["completion_seconds"]
            matched = (completion == branch_completion if branch_completion is not None else
                       completion is None or completion > branch["censor_seconds"])
            audits.append({"task_id": task_id, "rule_choice": meta["rule_choice"], "passed": matched,
                           "parent_completion_seconds": completion, "branch_completion_seconds": branch_completion})
        assert len(audits) == 8 and all(a["passed"] for a in audits), audits
        return audits


def worker(condition):
    frozen_audit()
    manifest = read(OUT / "manifest.json")
    assert manifest["runner_sha256"] == sha(__file__)
    job = next(j for j in manifest["jobs"] if j["condition"] == condition)
    assert sha(job["config"]) == job["config_sha256"]
    cfg = config_from_dict(read(job["config"]))
    policy = PilotChoice(job["raw_hops"])
    env = candidate.CandidateEnv(cfg)
    ctl = Controller(job, env, policy)
    policy.controller = ctl
    ctl.progress()
    try:
        for step in range(cfg.sim_steps):
            if step % cfg.decision_interval_steps == 0:
                state = decision_state(env.export_state(), cfg.decision_tile_limit_per_sat)
                reference = None
                if ctl.branch is None and state.tiles:
                    reference = candidate.ThreePlanChoice(job["raw_hops"])
                    reference.raw_tasks = policy.raw_tasks.copy()
                    reference.stats = deepcopy(policy.stats)
                    expected = reference.select_actions(deepcopy(state))
                    old_count = len(policy.decisions)
                actions = policy.select_actions(state)
                if ctl.branch is None and reference is not None:
                    assert actions == expected, "Unforced actions differ from frozen v3"
                    assert policy.decisions[old_count:] == reference.decisions, "Unforced decisions differ"
                    assert policy.raw_tasks == reference.raw_tasks and policy.stats == reference.stats
                    ctl.audit_calls += 1
            else:
                actions = []
            env.step(actions)
            if ctl.branch is not None:
                if env.tasks[ctl.branch["task_id"]].completion_time is not None or env.time >= ctl.branch["censor_step"]:
                    ctl.finish_branch()
            elif env.time % round(1800 / cfg.dt) == 0:
                ctl.progress()
        assert ctl.branch is None, "Branch ended before its censor bound"
        audits = ctl.audit_controls()
        row = {"condition": condition, "status": "PASS", "captured_states": len(ctl.captured),
               "unforced_selector_audit_calls": ctl.audit_calls, "control_audits": audits,
               "wall_seconds": time.monotonic() - ctl.started,
               "parent_cpu_seconds": time.process_time() - ctl.cpu_started,
               "frozen_snapshot_files_verified": frozen_audit()}
        write(OUT / "conditions" / f"{condition}.json", row)
        write(OUT / "progress" / f"{condition}.json", row)
    except BaseException:
        if ctl.branch is not None:
            ctl.finish_branch("FAILED", traceback.format_exc())
        write(OUT / "conditions" / f"{condition}.json",
              {"status": "FAILED", "condition": condition, "captured_states": len(ctl.captured),
               "traceback": traceback.format_exc(), "job": job})
        raise
    finally:
        env.close()


def summarize():
    manifest = read(OUT / "manifest.json")
    conditions, states = [], []
    for job in manifest["jobs"]:
        path = OUT / "conditions" / f"{job['condition']}.json"
        conditions.append(read(path) if path.exists() else {"condition": job["condition"], "status": "MISSING"})
        for sample in job["samples"]:
            directory = OUT / "states" / job["condition"] / sample["task_id"]
            if (directory / "label.json").exists():
                row = read(directory / "label.json")
                row["branches"] = {p: read(directory / f"{p}.json") for p in PLANS}
                states.append(row)
    counts = Counter(s["status"] for s in states)
    determinate = [s for s in states if s["status"] == "DETERMINATE"]
    agreement = sum(s["agrees"] for s in determinate) / len(determinate) if determinate else None
    indeterminate = counts["INDETERMINATE"] / 40
    valid = (len(states) == 40 and not counts["ERROR"] and all(c["status"] == "PASS" for c in conditions))
    stop = agreement is not None and agreement >= .9 or indeterminate > .4
    status = "BLOCKED" if not valid else "STOP" if stop else "PILOT_PASS_AWAIT_CONFIRMATION"
    result = {"status": status, "completed_at_utc": datetime.now(timezone.utc).isoformat(),
              "expected_states": 40, "observed_states": len(states), "status_counts": dict(counts),
              "determinate": len(determinate), "agreement": agreement,
              "tie_aware_agreement": sum(s["tie_aware_agrees"] for s in determinate) / len(determinate) if determinate else None,
              "indeterminate_fraction": indeterminate, "conditions": conditions, "states": states,
              "branch_worker_seconds": sum(r["wall_seconds"] for s in states for r in s["branches"].values()),
              "branch_cpu_seconds": sum(r["cpu_seconds"] for s in states for r in s["branches"].values()),
              "frozen_source_files_verified": frozen_audit(), "manifest_sha256": sha(OUT / "manifest.json"),
              "blind_dates_sha256": sha(OUT / "blind_dates.json"), "training_started": False,
              "next_action": "Wait for user confirmation; do not generate full labels or train"}
    write(OUT / "summary.json", result)
    lines = ["# 反事实选择器试点", "", f"状态：{status}。未训练，未启封新盲测。", "",
             f"完成 {len(states)}/40 状态；可判定 {len(determinate)}；不可判定 {counts['INDETERMINATE']}/40。",
             f"严格标签一致率：{agreement}；不可判定率：{indeterminate}。",
             "停止规则：一致率≥90% 或不可判定率>40%；任何审计/执行失败单列并阻止通过。", "",
             "|条件|可判定|一致|不可判定|", "|---|---:|---:|---:|"]
    for condition in CONDITIONS:
        group = [s for s in states if s["condition"] == condition]
        lines.append(f"|{condition}|{sum(s['status']=='DETERMINATE' for s in group)}|"
                     f"{sum(s['agrees'] is True for s in group)}|{sum(s['status']=='INDETERMINATE' for s in group)}|")
    lines += ["", "三分支是同状态单决策干预，后续固定v3；标签是局部反事实最快方案，不是全局最优策略。",
              "每状态三分支上限7200仿真秒；完成分支必须早于其他删失分支的下界。未完成不按300秒平局处理。",
              "到达仅原开发日24小时；标签观察延至末图后两小时，未生成次日任务。",
              "选择器无干预时逐决策与原v3比较，原选择对应分支与母轨迹完成时间比较；详见summary.json。",
              f"新盲测2026-12-15至12-17；日期SHA256：`{result['blind_dates_sha256']}`。",
              "试点后停止，等待是否进入全量标签生成的确认。"]
    (OUT / "REPORT.md").write_text("\n".join(lines) + "\n")
    return result


def panel():
    manifest = read(OUT / "manifest.json")
    assert manifest["runner_sha256"] == sha(__file__)
    logs = []
    processes = []
    for job in manifest["jobs"]:
        log = (OUT / f"{job['condition']}.log").open("x")
        logs.append(log)
        proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "worker", "--condition", job["condition"]],
                                cwd=PROJECT, stdout=log, stderr=subprocess.STDOUT)
        processes.append((job["condition"], proc))
    write(OUT / "worker_launch.json", {"coordinator_pid": os.getpid(),
                                      "workers": {c: p.pid for c, p in processes}})
    exits = {c: p.wait() for c, p in processes}
    for log in logs:
        log.close()
    write(OUT / "exits.json", exits)
    result = summarize()
    print(json.dumps({k: result[k] for k in ("status", "agreement", "indeterminate_fraction")}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "worker", "panel", "summarize"))
    parser.add_argument("--condition", choices=list(CONDITIONS))
    args = parser.parse_args()
    if args.command == "prepare":
        prepare()
    elif args.command == "worker":
        worker(args.condition)
    elif args.command == "panel":
        panel()
    else:
        summarize()
