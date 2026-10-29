"""仅修复冻结正式测试的审计、失败隔离与断点续跑；原159份源码不变。"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stdout, redirect_stderr
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback

PROJECT = next(p for p in Path(__file__).resolve().parents
               if (p / "results/sar_v3_frozen_20261017/freeze.json").exists())
OUT = PROJECT / "results/sar_v3_frozen_20261017"
sys.path.insert(0, str(OUT / "source"))
from analysis import run_sar_formal as old

REPAIR = "orchestration_repair_20261018"
read, save, now, sha = old.read, old.save, old.now, old.file_hash


def metric_audit(record):
    outcomes = record["metrics"]["task_outcomes"]
    score = old.task_age_score(outcomes, [300.0], record["observation_end_seconds"])
    curve = score["delivery_curve"][0]
    expected_ids = sorted(t["task_id"] for t in outcomes
                          if t["status"] == "completed" and t["latency_seconds"] <= 300.0)
    expected_r, recorded_r = score["restricted_mean_latency_seconds"], record["R300_seconds"]
    delta = None if expected_r is None or recorded_r is None else abs(expected_r - recorded_r)
    counts_ok = (type(record["on_time_tasks"]) is int and type(record["total_tasks"]) is int
                 and record["on_time_tasks"] == curve["delivered_within_horizon"]
                 and record["total_tasks"] == len(outcomes)
                 and sorted(record["on_time_task_ids"]) == expected_ids)
    # P300以整数分子/分母核验；比例字段还须忠实记录该计数的商。
    p_ok = counts_ok and record["P300"] == curve["delivery_rate"]
    r_ok = ((expected_r is None and recorded_r is None) or
            (delta is not None and math.isfinite(delta) and delta <= 1e-6))
    return {"status": "PASS" if p_ok and r_ok else "AUDIT_MISMATCH",
            "P300_count_exact": counts_ok, "P300_ratio_consistent": p_ok,
            "expected": {"on_time_tasks": curve["delivered_within_horizon"], "total_tasks": len(outcomes),
                         "P300": curve["delivery_rate"], "R300_seconds": expected_r},
            "recorded": {k: record[k] for k in ("on_time_tasks", "total_tasks", "P300", "R300_seconds")},
            "R300_absolute_error_seconds": delta, "R300_absolute_tolerance_seconds": 1e-6}


def audit(record, job, frozen):
    assert sha(Path(job["config"])) == job["config_sha256"]
    old.validate_record(record, read(job["config"]), frozen["source_sha256"], job["method_code"])
    assert record["formal_job"] == job["key"]
    assert record["formal_freeze_sha256"] == sha(OUT / "freeze.json")
    if job["method"] == "three_plan":
        assert record["unique_ground_audit"]["passed"]
        assert record["extension_sha256"] == frozen["files"]["analysis/sar_three_plan.py"]
        assert record["raw_ground_hops"] == job["ground_hops"]
    return metric_audit(record)


def verify(out):
    assert Path(old.__file__).resolve() == out / "source/analysis/run_sar_formal.py"
    frozen = old.verify_freeze(out)
    repair = read(out / REPAIR / "repair.json")
    assert sha(Path(__file__)) == repair["orchestrator_after_sha256"]
    for path, digest in repair["preserved_sha256"].items():
        assert sha(Path(path)) == digest, f"Preserved evidence changed: {path}"
    manifest = read(out / "manifest.json")
    for job in manifest["jobs"]:
        assert sha(Path(job["config"])) == job["config_sha256"]
    return frozen, manifest


def prepare(out):
    frozen = old.verify_freeze(out)
    repair_dir = out / REPAIR
    assert not (repair_dir / "repair.json").exists(), "Repair already registered"
    manifest = read(out / "manifest.json")
    preserved = {str(out / n): sha(out / n) for n in
                 ("FAILED.json", "FAILURE_REPORT.md", "freeze.json", "manifest.json", "acquisition.json",
                  "main_config.json", "README.md", "launch.json")}
    retained = []
    for job in manifest["jobs"]:
        assert sha(Path(job["config"])) == job["config_sha256"]
        path = Path(job["record"])
        if path.exists():
            assert audit(read(path), job, frozen)["status"] == "PASS"
            preserved[str(path)] = sha(path)
            retained.append(job["key"])
    assert len(retained) == 6 and len(manifest["jobs"]) == 180
    repair_dir.mkdir(exist_ok=True)
    target = repair_dir / "resume_sar_formal.py"
    shutil.copyfile(__file__, target)
    save(repair_dir / "repair.json", {"registered_at_utc": now(),
         "scope": "User-authorized orchestration repair only; no method, routing, parameter or test-config changes",
         "orchestrator_before_path": str(out / "source/analysis/run_sar_formal.py"),
         "orchestrator_before_sha256": frozen["files"]["analysis/run_sar_formal.py"],
         "orchestrator_after_path": str(target), "orchestrator_after_sha256": sha(target),
         "frozen_files_verified_unchanged": len(frozen["files"]),
         "frozen_file_sha256": frozen["files"], "preserved_sha256": preserved,
         "retained_jobs": retained, "remaining_jobs": 174,
         "smoke_job": "2026-12-01_base_local", "workers": 4})
    print(json.dumps({"frozen_files_unchanged": len(frozen["files"]), "retained": len(retained),
                      "remaining": 174, "before": frozen["files"]["analysis/run_sar_formal.py"], "after": sha(target)}), flush=True)


def result_path(out, job):
    return out / REPAIR / "job_status" / f"{job['key']}.json"


def failure(out, job, trace, record=None):
    value = {"status": "FAILED", "job": job, "failed_at_utc": now(), "traceback": trace,
             "config": read(job["config"]), "partial_record": record}
    save(out / REPAIR / "failures" / f"{job['key']}.json", value)
    save(result_path(out, job), {"status": "FAILED", "job": job["key"], "failed_at_utc": now()})
    return "FAILED"


def worker(out, job):
    record = None
    try:
        frozen, _ = verify(out)
        assert not Path(job["record"]).exists(), "Never rerun a saved record"
        cfg = read(job["config"])
        print(json.dumps({"started_at_utc": now(), "pid": os.getpid(), "job": job["key"]}), flush=True)
        if job["method"] == "three_plan":
            record = old.run_job(job["condition"], job["ground_hops"], cfg, frozen["source_sha256"],
                                 frozen["files"]["analysis/sar_three_plan.py"], "v3")
        else:
            record = old.measured_job(cfg, job["method_code"], frozen["source_sha256"],
                                      frozen["files"]["analysis/run_sar_frozen.py"], 6)
        record.update(derived_config_sha256=old.runner.digest(cfg), formal_job=job["key"],
                      formal_freeze_sha256=sha(out / "freeze.json"), finished_at_utc=now(),
                      orchestration_repair_sha256=sha(Path(__file__)))
        check = audit(record, job, frozen)
        record["metric_audit"] = check
        # 数值超容差也保存完整记录，后续任务继续；汇总时明确排除该成绩。
        save(job["record"], record)
        if check["status"] == "AUDIT_MISMATCH":
            save(out / REPAIR / "audit_mismatches" / f"{job['key']}.json",
                 {"status": "AUDIT_MISMATCH", "job": job, "config": cfg, "audit": check, "record": record})
        save(result_path(out, job), {"status": check["status"], "job": job["key"],
             "finished_at_utc": now(), "record_sha256": sha(Path(job["record"]))})
        print(json.dumps({"job": job["key"], "status": check["status"], "wall_seconds": record["wall_seconds"]}), flush=True)
        return check["status"]
    except Exception:
        trace = traceback.format_exc()
        print(trace, flush=True)
        return failure(out, job, trace, record)


def run_isolated(out, job):
    log = out / REPAIR / "logs" / f"{job['key']}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    try:
        with log.open("a") as handle:
            process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "worker", "--out", str(out),
                                        "--job", job["key"]], cwd=out / "source", stdin=subprocess.DEVNULL,
                                       stdout=handle, stderr=handle)
            save(out / REPAIR / "active" / f"{job['key']}.json", {"pid": process.pid, "started_at_utc": now()})
            code = process.wait()
        status_path = result_path(out, job)
        if code != 0 or not status_path.exists():
            # 原生崩溃没有Python栈时保留退出码和完整日志，同时保留协调层调用栈。
            raise RuntimeError(f"Worker exit={code}; status_exists={status_path.exists()}; log:\n{log.read_text()}")
        return read(status_path)["status"]
    except Exception:
        return failure(out, job, traceback.format_exc())


def inventory(out, manifest, frozen):
    statuses = {}
    for job in manifest["jobs"]:
        path, state = Path(job["record"]), result_path(out, job)
        if state.exists() and read(state)["status"] == "FAILED":
            statuses[job["key"]] = "FAILED"
        elif path.exists():
            statuses[job["key"]] = audit(read(path), job, frozen)["status"]
        elif state.exists():
            raise RuntimeError(f"Status exists without record: {job['key']}")
    return statuses


def write_progress(out, statuses, phase, **extra):
    save(out / "progress.json", {"status": phase, "done": len(statuses), "total": 180,
         **{name: sum(s == label for s in statuses.values()) for name, label in
            (("passed", "PASS"), ("audit_mismatch", "AUDIT_MISMATCH"), ("failed", "FAILED"))},
         "updated_at_utc": now(), "orchestration_repair": REPAIR, **extra})


def smoke(out):
    frozen, manifest = verify(out)
    repair = read(out / REPAIR / "repair.json")
    statuses = inventory(out, manifest, frozen)
    assert set(statuses) == set(repair["retained_jobs"])
    job = next(j for j in manifest["jobs"] if j["key"] == repair["smoke_job"])
    write_progress(out, statuses, "smoke_running", smoke_job=job["key"])
    status = run_isolated(out, job)
    statuses[job["key"]] = status
    save(out / REPAIR / "smoke.json", {"job": job["key"], "status": status, "finished_at_utc": now(),
         "config_sha256": job["config_sha256"], "counts_as_formal_run": True,
         "audit": read(job["record"]).get("metric_audit") if Path(job["record"]).exists() else None})
    write_progress(out, statuses, "smoke_passed" if status == "PASS" else "smoke_failed")
    print(json.dumps(read(out / REPAIR / "smoke.json")), flush=True)
    return status


def summarize(out):
    frozen, manifest = verify(out)
    statuses = inventory(out, manifest, frozen)
    assert len(statuses) == 180, "Report only after every job reaches a terminal status"
    records, daily, issues, hashes = {}, [], [], {}
    for job in manifest["jobs"]:
        status = statuses[job["key"]]
        row = {k: job[k] for k in ("condition", "date", "method")}
        row.update(status=status, P300=None, R300_seconds=None, on_time_tasks=None, total_tasks=None)
        if status == "PASS":
            record = read(job["record"])
            row.update({k: record[k] for k in ("P300", "R300_seconds", "on_time_tasks", "total_tasks")})
            records[job["condition"], job["date"], job["method"]] = record
            hashes[job["key"]] = sha(Path(job["record"]))
        else:
            issues.append({"job": job["key"], "status": status,
                           "details": str(out / REPAIR / ("failures" if status == "FAILED" else "audit_mismatches") / f"{job['key']}.json")})
        daily.append(row)
    means, pairs, wins = [], [], []
    for condition in old.GROUND:
        for method in old.METHODS:
            group = [r for r in daily if r["condition"] == condition and r["method"] == method]
            complete = all(r["status"] == "PASS" for r in group)
            means.append({"condition": condition, "method": method, "valid_days": sum(r["status"] == "PASS" for r in group),
                          "defined_days": sum(r["P300"] is not None for r in group),
                          "P300_day_equal": old.average([r["P300"] for r in group]) if complete else None,
                          "R300_day_equal_s": old.average([r["R300_seconds"] for r in group]) if complete else None})
            if method == "three_plan":
                continue
            group_pairs = []
            for date in old.DATES:
                new, ref = records.get((condition, date, "three_plan")), records.get((condition, date, method))
                p = r = None
                gained, lost = [], []
                valid = new is not None and ref is not None
                if valid:
                    identity = lambda x: [(t["task_id"], t["source_sat_id"], t["release_seconds"])
                                          for t in x["metrics"]["task_outcomes"]]
                    assert identity(new) == identity(ref)
                    if new["P300"] is not None:
                        p = 100 * (new["P300"] - ref["P300"])
                        r = new["R300_seconds"] - ref["R300_seconds"]
                    gained = sorted(set(new["on_time_task_ids"]) - set(ref["on_time_task_ids"]))
                    lost = sorted(set(ref["on_time_task_ids"]) - set(new["on_time_task_ids"]))
                pair = {"condition": condition, "date": date, "baseline": method, "valid_pair": valid,
                        "P300_result": "unavailable" if not valid else "undefined" if p is None else
                            "win" if p > 0 else "loss" if p < 0 else "tie",
                        "delta_P300_pp": p, "delta_R300_s": r, "gained_task_ids": gained, "lost_task_ids": lost}
                pairs.append(pair)
                group_pairs.append(pair)
            complete = all(p["valid_pair"] for p in group_pairs)
            wins.append({"condition": condition, "baseline": method,
                         **{s: sum(p["P300_result"] == s for p in group_pairs) for s in
                            ("win", "tie", "loss", "undefined", "unavailable")},
                         "delta_P300_day_equal_pp": old.average([p["delta_P300_pp"] for p in group_pairs]) if complete else None,
                         "delta_R300_day_equal_s": old.average([p["delta_R300_s"] for p in group_pairs]) if complete else None})
    save(out / "summary.json", {"completed_at_utc": now(), "daily": daily, "day_equal_means": means,
         "daily_pairs": pairs, "wins_losses": wins, "issues": issues, "record_sha256": hashes,
         "freeze_sha256": sha(out / "freeze.json"), "repair_sha256": sha(out / REPAIR / "repair.json"),
         "method_changes_after_test": False, "frozen_source_files_unchanged": len(frozen["files"])})
    fmt = lambda x, scale=1: "NA" if x is None else f"{x * scale:.4f}"
    lines = ["# v3 正式测试：编排修复后汇总", "", f"180项均已结束；通过{180-len(issues)}项，异常{len(issues)}项。",
             "原159份源码、参数及测试配置未变；原6份记录复用。原失败证据保留。P300按整数计数核验，R300绝对容差1e-6秒。",
             "异常项不算算法失败，也不进入指标均值；任一日期异常时对应六日日均置NA，禁止只选成功日期平均。零任务日单独记不可定义。", "",
             "## 异常单列", "", "|任务|状态|证据文件|", "|---|---|---|"]
    lines += [f"|{i['job']}|{i['status']}|{i['details']}|" for i in issues] or ["|无|全部通过|见summary.json哈希|"]
    lines += ["", "## 日等权均值", "", "|条件|方法|P300/%|R300/秒|有效/可定义日数|", "|---|---|---:|---:|---|"]
    lines += [f"|{r['condition']}|{r['method']}|{fmt(r['P300_day_equal'],100)}|{fmt(r['R300_day_equal_s'])}|{r['valid_days']}/{r['defined_days']}|" for r in means]
    lines += ["", "## 三方案逐日胜负汇总", "", "P300胜负独立统计，R300差值为三方案减基线。", "",
              "|条件|基线|胜/平/负/空日/异常|日均P300差/百分点|日均R300差/秒|", "|---|---|---|---:|---:|"]
    lines += [f"|{r['condition']}|{r['baseline']}|{r['win']}/{r['tie']}/{r['loss']}/{r['undefined']}/{r['unavailable']}|{fmt(r['delta_P300_day_equal_pp'])}|{fmt(r['delta_R300_day_equal_s'])}|" for r in wins]
    lines += ["", "## 逐日指标", "", "|条件|日期|方法|状态|按时/总图|P300/%|R300/秒|", "|---|---|---|---|---|---:|---:|"]
    lines += [f"|{r['condition']}|{r['date']}|{r['method']}|{r['status']}|{r['on_time_tasks']}/{r['total_tasks']}|{fmt(r['P300'],100)}|{fmt(r['R300_seconds'])}|" for r in daily]
    lines += ["", "## 逐日配对差值", "", "|条件|日期|基线|胜负|P300差/百分点|R300差/秒|", "|---|---|---|---|---:|---:|"]
    lines += [f"|{r['condition']}|{r['date']}|{r['baseline']}|{r['P300_result']}|{fmt(r['delta_P300_pp'])}|{fmt(r['delta_R300_s'])}|" for r in pairs]
    lines += ["", "逐图新增/丢失见summary.json；开发最强基线差值见DEVELOPMENT_REPORT.md。六日属于同一合成星座的相位留出，不是独立星座。不据测试结果修方法，学习未实施。"]
    (out / "REPORT.md").write_text("\n".join(lines) + "\n")
    return statuses


def panel(out):
    frozen, manifest = verify(out)
    assert read(out / REPAIR / "smoke.json")["status"] == "PASS"
    statuses = inventory(out, manifest, frozen)
    todo = [j for j in manifest["jobs"] if j["key"] not in statuses]
    start = time.monotonic()
    write_progress(out, statuses, "running", remaining=len(todo))
    # 每项独立子进程；即使某项原生崩溃，也不会破坏其他工作进程。
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(run_isolated, out, j): j for j in todo}
        for future in as_completed(futures):
            job = futures[future]
            try:
                status = future.result()
            except Exception:
                status = failure(out, job, traceback.format_exc())
            statuses[job["key"]] = status
            write_progress(out, statuses, "running", last_job=job["key"], elapsed_seconds=time.monotonic()-start)
            print(json.dumps(read(out / "progress.json")), flush=True)
    statuses = summarize(out)
    write_progress(out, statuses, "complete_with_issues" if any(s != "PASS" for s in statuses.values()) else "complete",
                   elapsed_seconds=time.monotonic()-start)


def launch(out, action):
    verify(out)
    if action == "panel":
        assert read(out / REPAIR / "smoke.json")["status"] == "PASS"
    launch_path = out / REPAIR / f"{action}_launch.json"
    assert not launch_path.exists(), "Already launched; inspect rather than duplicate"
    script = out / REPAIR / "resume_sar_formal.py"
    command = [sys.executable, str(script), action, "--out", str(out)]
    with (out / REPAIR / f"{action}.log").open("ab") as log:
        process = subprocess.Popen(command, cwd=out / "source", stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=log, start_new_session=True)
    awake = subprocess.Popen(["/usr/bin/caffeinate", "-i", "-w", str(process.pid)],
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
    value = {"started_at_utc": now(), "pid": process.pid, "caffeinate_pid": awake.pid, "command": command}
    save(launch_path, value)
    print(json.dumps(value), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "smoke", "panel", "worker", "report", "launch-smoke", "launch-panel"))
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--job")
    args = parser.parse_args()
    out = args.out.resolve()
    assert out == OUT
    if args.action.startswith("launch-"):
        launch(out, args.action.removeprefix("launch-"))
    elif args.action == "worker":
        manifest = read(out / "manifest.json")
        worker(out, next(j for j in manifest["jobs"] if j["key"] == args.job))
    else:
        try:
            {"prepare": prepare, "smoke": smoke, "panel": panel, "report": summarize}[args.action](out)
        except Exception:
            save(out / REPAIR / "CONTROLLER_FAILED.json", {"status": "controller_failed", "at_utc": now(),
                                                          "traceback": traceback.format_exc()})
            raise


if __name__ == "__main__":
    main()
