"""授权后的监督式选择器实验；仅开发/验证日期，冻结核心与盲测不变。"""

import argparse
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
from analysis import sar_selector_pilot as pilot
from analysis import run_sar_formal as geometry_runner
from analysis import run_sar_frozen as frozen_runner
from sim.config import config_from_dict
from sim.marl.decision import decision_state
import numpy as np

OUT = PROJECT / "results/sar_selector_learning_20261019"
PILOT_OUT = pilot.OUT
PLANS = pilot.PLANS
CONDITIONS = pilot.CONDITIONS
TRAIN = [f"2026-10-{d}" for d in range(15, 22)]
VALIDATION = [f"2026-10-{d}" for d in range(22, 25)]
SEEDS = [1701, 1702, 1703]
read, write, sha = pilot.read, pilot.write, pilot.sha
DEADLINE = datetime.fromisoformat("2026-10-26T23:59:59+08:00")


def now():
    return datetime.now(timezone.utc).isoformat()


def guard_date(date):
    if date not in TRAIN + VALIDATION:
        raise ValueError(f"Date outside registered development/validation allowlist: {date}")


def audit_sources():
    freeze = read(pilot.FROZEN / "freeze.json")
    for relative, digest in freeze["files"].items():
        assert sha(pilot.FROZEN / "source" / relative) == digest, relative
        assert sha(PROJECT / relative) == digest, relative
    registered = read(PILOT_OUT / "approved_registration_hashes.json")
    for name, digest in registered.items():
        assert sha(PILOT_OUT / name) == digest, name
    if (OUT / "implementation_hashes.json").exists():
        for relative, digest in read(OUT / "implementation_hashes.json").items():
            assert sha(PROJECT / relative) == digest, relative
    if (OUT / "registration_hashes.json").exists():
        for relative, digest in read(OUT / "registration_hashes.json").items():
            assert sha(OUT / relative) == digest, relative
    return {"frozen_files": len(freeze["files"]), "blind_registration": registered}


def features(subset, options, quotes, details, calendar):
    """只读取公开决策状态和报价日历，不接受环境或输出真值。"""
    source = next(iter(subset.tiles.values()))["location"]
    values = {}

    def add(name, value):
        value = float(value)
        assert np.isfinite(value) and value >= -1e-8, (name, value)
        values[name] = float(np.log1p(max(0, value)))

    add("image_age_s", max(t["waiting_time"] for t in subset.tiles.values()))
    add("work_windows", sum(t["compute_cost"] for t in subset.tiles.values()))
    add("input_mb", sum(t["data_size_mb"] for t in subset.tiles.values()))
    add("tiles", len(subset.tiles))
    for key in ("queue_len", "committed_compute_s", "mem_remaining_gb", "vram_remaining_gb", "compute_rate"):
        add("source_" + key, subset.satellites[source][key])
    add("ground_committed_s", sum(g["committed_compute_s"] for g in subset.ground_stations.values()))
    add("ground_rate", sum(g["compute_rate"] for g in subset.ground_stations.values()))
    for plan in PLANS:
        values[plan + "_quote_valid"] = float(quotes[plan] is not None)
        add(plan + "_quote_s", quotes[plan] or 0)
        for name in ("pipeline", "group"):
            value = details[plan].get(name)
            values[f"{plan}_{name}_valid"] = float(value is not None)
            add(f"{plan}_{name}_s", value or 0)
        processors = set()
        for action in options[plan]:
            if plan == "ground" or action.target_gs_id is not None:
                continue
            sid = action.target_sat_id if action.target_sat_id is not None else source
            processors.add(sid)
        add(plan + "_processors", len(processors))
        for field in ("committed_compute_s", "mem_remaining_gb", "queue_len"):
            entries = [subset.satellites[s][field] for s in processors]
            add(f"{plan}_{field}_mean", np.mean(entries) if entries else 0)
            add(f"{plan}_{field}_max", max(entries, default=0))
        nodes = processors or {source}
        contacts = [c for c in calendar.plan.contacts if c.source in nodes and c.volume > 0]
        add(plan + "_contact_volume_mb", sum(c.volume for c in contacts))
        add(plan + "_contact_wait_s", min((max(0, c.start) for c in contacts), default=7200))
        add(plan + "_contact_duration_s", max((max(0, c.end - c.start) for c in contacts), default=0))
    return values


def select_samples(eligible, seed, limit=8):
    ordered = list(eligible)
    assert len({r["task_id"] for r in ordered}) == len(ordered)
    if len(ordered) <= limit:
        return ordered
    rng = np.random.default_rng(seed)
    return [ordered[int(rng.choice(part))] for part in np.array_split(np.arange(len(ordered)), limit)]


def deadline_loss(branches, choice, fastest):
    if fastest is None:
        return None
    a, b = branches[choice], branches[fastest]
    fastest_on_time = b["latency_seconds"] is not None and b["latency_seconds"] <= 300
    chosen_on_time = a["latency_seconds"] is not None and a["latency_seconds"] <= 300
    if a["latency_seconds"] is None and a["censor_seconds"] - a["release_seconds"] < 300:
        return None
    return bool(fastest_on_time and not chosen_on_time)


def outcomes(env):
    dt = env.cfg.dt
    rows = []
    for task in env.tasks.values():
        latency = None if task.completion_time is None else (task.completion_time - task.release_time) * dt
        rows.append({"task_id": task.task_id, "release_seconds": task.release_time * dt,
                     "completion_seconds": None if task.completion_time is None else task.completion_time * dt,
                     "latency_seconds": latency})
    assert all(env.time * dt - r["release_seconds"] >= 300 for r in rows)
    return {"outcomes": rows, "total_tasks": len(rows),
            "on_time_tasks": sum(r["latency_seconds"] is not None and r["latency_seconds"] <= 300 for r in rows),
            "P300": sum(r["latency_seconds"] is not None and r["latency_seconds"] <= 300 for r in rows) / len(rows),
            "R300_seconds": sum(min(300, r["latency_seconds"]) if r["latency_seconds"] is not None else 300 for r in rows) / len(rows)}


class Census:
    def __init__(self):
        self.eligible = {}

    def intervene(self, task, state, subset, options, quotes, details, trials, winner, prior, calendar):
        if task not in self.eligible and state.time * state.config["dt"] < 86400:
            self.eligible[task] = {"task_id": task, "decision_seconds": state.time * state.config["dt"],
                                   "rule_choice": winner,
                                   "features": features(subset, options, quotes, details, prior)}
        return winner, calendar


class Labels(pilot.Controller):
    def intervene(self, task, state, subset, options, quotes, details, trials, winner, prior, calendar):
        selected = self.branch is None and task in self.samples and task not in self.captured
        if selected:
            observed = features(subset, options, quotes, details, prior)
            assert observed == self.samples[task]["features"], "Census/replay feature mismatch"
            assert winner == self.samples[task]["rule_choice"]
            assert state.time * state.config["dt"] == self.samples[task]["decision_seconds"]
        result = super().intervene(task, state, subset, options, quotes, details, trials, winner, prior, calendar)
        if selected and self.branch is None:
            folder = OUT / "states" / self.job["condition"] / task
            state_record = read(folder / "state.json")
            state_record["features"] = observed
            write(folder / "state.json", state_record)
            branches = {p: read(folder / f"{p}.json") for p in PLANS}
            label = read(folder / "label.json")
            label["v3_caused_deadline_miss"] = deadline_loss(branches, winner, label["label"])
            write(folder / "label.json", label)
        return result

    def progress(self):
        write(OUT / "progress" / (self.job["condition"] + "_labels.json"),
              {"status": "RUNNING", "pid": os.getpid(), "simulated_seconds": self.env.time * self.env.cfg.dt,
               "captured_states": len(self.captured), "expected_states": len(self.samples),
               "wall_seconds": time.monotonic() - self.started})

    def audit_controls(self):
        checks = []
        for task, meta in self.captured.items():
            branch = read(OUT / "states" / self.job["condition"] / task / (meta["rule_choice"] + ".json"))
            completion = self.env.tasks[task].completion_time
            completion = None if completion is None else completion * self.env.cfg.dt
            matched = (completion == branch["completion_seconds"] if branch["completion_seconds"] is not None
                       else completion is None or completion > branch["censor_seconds"])
            checks.append({"task_id": task, "passed": matched, "completion_seconds": completion,
                           "branch_completion_seconds": branch["completion_seconds"]})
        assert len(checks) == len(self.samples) and all(c["passed"] for c in checks), checks
        return checks


class LearnedChoice:
    def __init__(self, method, seed):
        from analysis.sar_selector_model import predict
        self.predict = predict
        self.rng = np.random.default_rng(seed)
        self.method = method
        self.model = read(OUT / "models" / f"{method}_{seed}.json") if method != "random" else None
        self.timings = []

    def intervene(self, task, state, subset, options, quotes, details, trials, winner, prior, calendar):
        start = time.perf_counter()
        x = features(subset, options, quotes, details, prior)
        if self.method == "random":
            selected = PLANS[int(self.rng.integers(3))]
        else:
            selected = PLANS[self.predict(self.model, x)]
        self.timings.append(time.perf_counter() - start)
        return selected, trials[selected] if quotes[selected] is not None else prior


def prepare():
    assert not OUT.exists(), "Do not overwrite registered experiment"
    verification = audit_sources()
    OUT.mkdir()
    write(OUT / "implementation_hashes.json", {str(p.relative_to(PROJECT)): sha(p) for p in
          [Path(__file__), PROJECT / "analysis/sar_selector_model.py", PROJECT / "analysis/sar_selector_pilot.py"]})
    spec = {"created_at_utc": now(), "authorization": "User accepts 39-state pilot; full labels, weighted training, validation and freeze; no blind test",
            "train_dates": TRAIN, "validation_dates": VALIDATION, "conditions": list(CONDITIONS), "seeds": SEEDS,
            "sampling": "Census first actual whole-image decisions during [0,86400) of each unmodified v3 day; 8 equal-count temporal strata with one seeded draw; use all if fewer than 8; register each sampled set before branch replay",
            "sampling_seed": 20261019, "max_states_per_cell": 8, "planned_cells": 50,
            "gate": {"maximum_indeterminate_fraction": .4, "minimum_coverage": .9, "execution_audit_failures_block": True},
            "deadline_miss": "fastest latency <=300 and v3 latency >300 (or censored beyond300); null if indeterminate",
            "features": "Explicit public features() only, log1p nonnegative scalars, no date/source ID/condition label/future arrivals/output truth; training-only standardization",
            "actions": "Three existing proposals; a None quote is prediction failure, not physical infeasibility. All selectors may select any proposal, including WAIT-containing CGS; execution resource checks unchanged. Random uniform over same three proposals.",
            "model": {"variants": ["classifier", "regressor"], "hidden": [32, 16], "epochs": 200,
                      "optimizer": "Adam", "learning_rate": .003, "weight_decay": .001, "batch": "full",
                      "class_weight": "N/(3*n_class), training only; missing class blocks training",
                      "classifier_loss": "class-weighted cross entropy",
                      "regressor_loss": "per-state class-weighted SmoothL1 on log1p latency; censored branches one-sided lower-bound loss; same determinate states as classifier",
                      "checkpoint": "fixed final epoch, no tuning or early stopping"},
            "selection": "Each of six trained candidates evaluated on all15 validation date/condition cells. Rank by five-condition mean of three-day equal-weight P300 descending then R300 ascending; exact tie classifier before regressor then seed1701/1702/1703. Report all seeds.",
            "validation": "15 cells x (v3 + six learned checkpoints + random three seeds)=150 full-day records; v3 census reused for P300/R300 after >=300s follow-up. Separate same-state counterfactual oracle table, never presented as achievable full-day oracle.",
            "censor_horizon_seconds": 7200, "label_simulation_seconds": 93601, "evaluation_seconds": 86701,
            "workers": 4, "deadline": DEADLINE.isoformat(), "source_audit": verification,
            "pilot_deviation_accepted": {"states": 39, "planned": 40, "agreement": 23 / 39,
                                         "indeterminate": 0, "missing": "load_4/task_535", "rerun_pilot": False},
            "old_pilot_usage": "diagnostic only; full cohort uses corrected census sampling and expanded shared features, not a repair/replacement of the missing pilot state",
            "freeze_sha256": sha(pilot.FROZEN / "freeze.json")}
    write(OUT / "protocol.json", spec)
    write(OUT / "registration_hashes.json", {"protocol.json": sha(OUT / "protocol.json")})
    base = read(pilot.FROZEN / "main_config.json")
    geometry = geometry_runner.generate(10, base)
    write(OUT / "development_geometry.json", geometry)
    jobs = []
    for di, date in enumerate(TRAIN + VALIDATION):
        guard_date(date)
        daily = geometry_runner.daily_config(base, geometry["events"], date)
        if date == TRAIN[0]:
            normalize = lambda entries: [{k: v for k, v in e.items() if k != "window_id"} for e in entries]
            assert normalize(daily["scheduled_task_arrivals"]) == normalize(base["scheduled_task_arrivals"])
        for ci, (condition, factors) in enumerate(CONDITIONS.items()):
            cfg = pilot.derive(daily, *factors)
            cfg["sim_steps"] = round(93601 / cfg["dt"])
            key = f"{date}_{condition}"
            path = OUT / "configs" / f"{key}.json"
            write(path, cfg)
            jobs.append({"key": key, "date": date, "condition": condition, "config": str(path),
                         "config_sha256": sha(path), "seed": 20261019 + di * 5 + ci,
                         "raw_hops": geometry_runner.GROUND[condition]})
    write(OUT / "manifest.json", {"jobs": jobs, "protocol_sha256": sha(OUT / "protocol.json"),
                                  "geometry_sha256": sha(OUT / "development_geometry.json")})


def worker(stage, key, method="v3", seed=1701):
    manifest = read(OUT / "manifest.json")
    job = next(j for j in manifest["jobs"] if j["key"] == key)
    guard_date(job["date"])
    audit_sources()
    assert sha(job["config"]) == job["config_sha256"]
    cfg_dict = read(job["config"])
    if stage == "evaluate":
        assert job["date"] in VALIDATION
        cfg_dict["sim_steps"] = round(86701 / cfg_dict["dt"])
    cfg = config_from_dict(cfg_dict)
    env = pilot.candidate.CandidateEnv(cfg)
    policy = pilot.PilotChoice(job["raw_hops"])
    started = time.monotonic()
    name = f"{key}_{method}_{seed}" if stage == "evaluate" else key
    destination = OUT / stage / f"{name}.json"
    assert not destination.exists(), "Existing terminal record must not be rerun"
    if stage == "labels":
        sample_path = OUT / "samples" / f"{key}.json"
        assert read(sample_path.with_suffix(".sha256.json"))[sample_path.name] == sha(sample_path)
        samples = read(sample_path)
        assert samples["census_sha256"] == sha(OUT / "census" / f"{key}.json")
        pilot.OUT = OUT
        ctl = Labels({**job, "condition": key, "samples": samples["states"]}, env, policy)
    elif stage == "census":
        ctl = Census()
    else:
        ctl = LearnedChoice(method, seed)
    policy.controller = ctl
    try:
        for step in range(cfg.sim_steps):
            if step % cfg.decision_interval_steps == 0:
                state = decision_state(env.export_state(), cfg.decision_tile_limit_per_sat)
                actions = policy.select_actions(state)
            else:
                actions = []
            env.step(actions)
            if stage == "labels" and ctl.branch is not None:
                if env.tasks[ctl.branch["task_id"]].completion_time is not None or env.time >= ctl.branch["censor_step"]:
                    ctl.finish_branch()
            elif step % round(3600 / cfg.dt) == 0:
                write(OUT / "progress" / f"{stage}_{name}.json", {"status": "RUNNING", "pid": os.getpid(),
                      "simulated_seconds": env.time * cfg.dt, "wall_seconds": time.monotonic() - started})
        audit = frozen_runner.invariants(env)
        assert audit["passed"], audit
        record = {"status": "PASS", **job, "method": method, "seed": seed, "stage": stage,
                  "config_used": cfg_dict, "wall_seconds": time.monotonic() - started,
                  "audit": audit, **outcomes(env), "selection_counts": dict(policy.stats),
                  "decisions": policy.decisions, "source_audit": audit_sources()}
        if stage == "census":
            record["eligible"] = list(ctl.eligible.values())
        elif stage == "labels":
            record["controls"] = ctl.audit_controls()
            census = read(OUT / "census" / f"{key}.json")
            assert record["outcomes"] == census["outcomes"], "Census/replay entire trajectory mismatch"
            assert record["decisions"] == census["decisions"], "Census/replay decisions mismatch"
            record["census_replay_equal"] = True
        else:
            record["selector_extra_seconds"] = sum(ctl.timings)
            record["selector_extra_calls"] = len(ctl.timings)
        write(destination, record)
    except BaseException:
        if stage == "labels" and ctl.branch is not None:
            ctl.finish_branch("FAILED", traceback.format_exc())
        write(destination, {"status": "FAILED", "job": job, "config_used": cfg_dict, "traceback": traceback.format_exc()})
        raise
    finally:
        env.close()


def sample_cell(job):
    census_path = OUT / "census" / f"{job['key']}.json"
    census = read(census_path)
    assert census["status"] == "PASS"
    states = select_samples(census["eligible"], job["seed"])
    registration = {"registered_at_utc": now(), "job": job, "census_sha256": sha(census_path),
                    "eligible_count": len(census["eligible"]), "planned_count": len(states),
                    "under_eight": len(states) < 8, "states": states}
    path = OUT / "samples" / f"{job['key']}.json"
    assert not path.exists()
    write(path, registration)
    write(path.with_suffix(".sha256.json"), {path.name: sha(path)})


def label_audit():
    manifest = read(OUT / "manifest.json")
    rows, cells, errors = [], [], []
    for job in manifest["jobs"]:
        sample_path = OUT / "samples" / f"{job['key']}.json"
        assert read(sample_path.with_suffix(".sha256.json"))[sample_path.name] == sha(sample_path)
        sample = read(sample_path)
        record = read(OUT / "labels" / f"{job['key']}.json")
        if record["status"] != "PASS":
            errors.append(job["key"])
        count = 0
        for state in sample["states"]:
            folder = OUT / "states" / job["key"] / state["task_id"]
            if not (folder / "label.json").exists():
                continue
            label = read(folder / "label.json")
            branches = {p: read(folder / f"{p}.json") for p in PLANS}
            assert all(b["status"] == "PASS" for b in branches.values())
            assert pilot.choose_label(branches)["label"] == label["label"]
            rows.append({**label, "date": job["date"], "condition": job["condition"],
                         "features": read(folder / "state.json")["features"], "branches": branches,
                         "split": "train" if job["date"] in TRAIN else "validation"})
            count += 1
        cells.append({"key": job["key"], "eligible": sample["eligible_count"],
                      "planned": sample["planned_count"], "observed": count})
    planned = sum(c["planned"] for c in cells)
    indeterminate = sum(r["status"] == "INDETERMINATE" for r in rows)
    coverage = len(rows) / planned if planned else 0
    unknown_fraction = indeterminate / len(rows) if rows else 1
    errors += [r["task_id"] for r in rows if r["status"] == "ERROR"]
    result = {"passed": coverage >= .9 and unknown_fraction <= .4 and not errors,
              "planned": planned, "observed": len(rows), "coverage": coverage,
              "indeterminate": indeterminate, "indeterminate_fraction": unknown_fraction, "errors": errors,
              "cells": cells, "labels": rows, "source_audit": audit_sources(),
              "deadline_misses_caused_by_v3": sum(r["v3_caused_deadline_miss"] is True for r in rows),
              "class_counts": {s: dict(Counter(r["label"] for r in rows if r["split"] == s and r["label"] is not None))
                               for s in ("train", "validation")}}
    write(OUT / "label_audit.json", result)
    return result


def run_batch(stage, jobs):
    pending = list(jobs)
    active = []
    failures = []
    while pending or active:
        if datetime.now(timezone.utc) > DEADLINE:
            for proc, handle, terminal, name in active:
                # 每个worker独立进程组，连同反事实分支一起停止，避免越过时间盒。
                import signal
                os.killpg(proc.pid, signal.SIGTERM)
                proc.wait()
                handle.close()
                if not terminal.exists():
                    write(terminal, {"status": "FAILED", "reason": "Registered timebox expired", "job": name})
            raise TimeoutError("Registered selection/freezing timebox expired")
        while pending and len(active) < 4:
            job = pending.pop(0)
            key, method, seed = job["key"], job.get("method", "v3"), job.get("seed", 1701)
            name = f"{key}_{method}_{seed}" if stage == "evaluate" else key
            terminal = OUT / stage / f"{name}.json"
            if terminal.exists():
                if read(terminal)["status"] != "PASS":
                    failures.append(str(terminal))
                continue
            log_path = OUT / "logs" / f"{stage}_{name}.log"
            log_path.parent.mkdir(exist_ok=True)
            handle = log_path.open("a")
            proc = subprocess.Popen([sys.executable, __file__, "worker", "--stage", stage, "--key", key,
                                     "--method", method, "--seed", str(seed)], cwd=PROJECT,
                                    stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
            active.append((proc, handle, terminal, name))
        for entry in list(active):
            proc, handle, terminal, name = entry
            code = proc.poll()
            if code is None:
                continue
            handle.close()
            if not terminal.exists():
                write(terminal, {"status": "FAILED", "exit_code": code, "job": name,
                                 "reason": "Worker exited without terminal record; inspect matching log"})
            if code or read(terminal)["status"] != "PASS":
                failures.append(str(terminal))
            active.remove(entry)
        write(OUT / "progress.json", {"stage": stage, "updated_at_utc": now(), "pending": len(pending),
              "active": [{"pid": p.pid, "job": name} for p, _, _, name in active], "failures": failures})
        if active:
            time.sleep(5)
    if failures:
        raise RuntimeError(f"{stage} has failed records, no automatic retry: {failures}")


def panel():
    try:
        if not (OUT / "manifest.json").exists():
            prepare()
        jobs = read(OUT / "manifest.json")["jobs"]
        run_batch("census", jobs)
        for job in jobs:
            if not (OUT / "samples" / f"{job['key']}.json").exists():
                sample_cell(job)
        run_batch("labels", jobs)
        audit = label_audit()
        assert audit["passed"], "Label audit failed; training not authorized by gate"
        subprocess.run([sys.executable, str(PROJECT / "analysis/sar_selector_model.py"), "train"], check=True, cwd=PROJECT)
        evaluation = [{**j, "method": method, "seed": seed} for j in jobs if j["date"] in VALIDATION
                      for method in ("classifier", "regressor", "random") for seed in SEEDS]
        run_batch("evaluate", evaluation)
        subprocess.run([sys.executable, str(PROJECT / "analysis/sar_selector_model.py"), "report"], check=True, cwd=PROJECT)
        write(OUT / "final_status.json", {"status": "FROZEN_AWAITING_USER", "completed_at_utc": now(),
                                          "source_audit": audit_sources(), "blind_test_opened": False})
    except BaseException:
        write(OUT / "final_status.json", {"status": "BLOCKED", "time_utc": now(), "traceback": traceback.format_exc(),
                                          "blind_test_opened": False})
        raise


def smoke():
    """训练日短前缀核验公开特征及无干预钩子，不混入正式标签或验证成绩。"""
    audit_sources()
    cfg_dict = read(pilot.FROZEN / "main_config.json")
    events = cfg_dict["scheduled_task_arrivals"]
    first = min(e["release_step"] for e in events)
    cfg_dict["scheduled_task_arrivals"] = [e for e in events if e["release_step"] == first]
    cfg_dict["sim_steps"] = first + round(301 / cfg_dict["dt"])
    cfg_dict["task_arrival_end_step"] = first + 1
    cfg = config_from_dict(cfg_dict)
    env = pilot.candidate.CandidateEnv(cfg)
    policy = pilot.PilotChoice(8)
    ctl = Census()
    policy.controller = ctl
    checked = 0
    try:
        for step in range(cfg.sim_steps):
            if step % cfg.decision_interval_steps == 0:
                state = decision_state(env.export_state(), cfg.decision_tile_limit_per_sat)
                reference = pilot.candidate.ThreePlanChoice(8)
                reference.raw_tasks = policy.raw_tasks.copy()
                reference.stats = deepcopy(policy.stats)
                expected = reference.select_actions(deepcopy(state))
                old_count = len(policy.decisions)
                actions = policy.select_actions(state)
                assert actions == expected
                assert policy.decisions[old_count:] == reference.decisions
                assert policy.raw_tasks == reference.raw_tasks and policy.stats == reference.stats
                checked += bool(state.tiles)
            else:
                actions = []
            env.step(actions)
        assert ctl.eligible, "Smoke did not exercise feature extraction"
        assert frozen_runner.invariants(env)["passed"]
        result = {"status": "PASS", "audited_decisions": checked, "eligible": list(ctl.eligible.values()),
                  "config": cfg_dict, "score": outcomes(env), "source_audit": audit_sources()}
        path = PROJECT / "results/sar_selector_learning_smoke_20261019.json"
        assert not path.exists(), "Do not overwrite smoke evidence"
        write(path, result)
        print(json.dumps({"status": "PASS", "audited_decisions": checked,
                          "feature_count": len(next(iter(ctl.eligible.values()))["features"])}), flush=True)
    finally:
        env.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "panel", "worker", "audit", "smoke"))
    parser.add_argument("--stage", choices=("census", "labels", "evaluate"))
    parser.add_argument("--key")
    parser.add_argument("--method", default="v3")
    parser.add_argument("--seed", type=int, default=1701)
    args = parser.parse_args()
    if args.command == "worker":
        worker(args.stage, args.key, args.method, args.seed)
    elif args.command == "audit":
        label_audit()
    else:
        globals()[args.command]()
