"""冻结 v3 后的一次性正式测试编排；不修改方法、公共路由或训练模型。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stdout, redirect_stderr
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import math
import multiprocessing
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from analysis import audit_daynight_coverage as geometry
from analysis import audit_sar_coastal_coverage as coastal
from analysis import prepare_sar_dispatch as output_fixture
from analysis import run_sar_frozen as runner
from analysis.run_sar_three_plan import run_job
from analysis.sar_condition_scan import CONDITIONS, derive, file_hash, measured_job, strong_ground, validate_record
from sim.evaluation import task_age_score
from sim.reproducibility import runtime_metadata

OUT = ROOT / "results/sar_v3_frozen_20261017"
GROUND = {"base": 8, "compute_4": 8, "compute_10": 8, "load_2": 45, "load_4": 8}
METHODS = {"local": "local", "ground": "ground_unique_relay", "cgs": "cgs_compute_adapted",
           "heft": "heft_adapted", "two_plan": "cgs_task_choice", "three_plan": "three_plan_v3"}
DATES = [f"2026-12-{i:02d}" for i in range(1, 7)]


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    temp.replace(path)


def now():
    return datetime.now(timezone.utc).isoformat()


def freeze(out):
    if (out / "freeze.json").exists():
        raise ValueError("Freeze already exists; do not overwrite")
    scan = read(ROOT / "results/sar_condition_scan_20261017/manifest.json")
    previous = ROOT / "results/sar_choice_revision_20261017/v3"
    frozen = read(previous / "manifest.json")
    assert runtime_metadata()["source_sha256"] == scan["source_sha256"] == frozen["source_sha256"]
    assert file_hash(ROOT / "analysis/sar_three_plan.py") == file_hash(previous / "candidate_source.py")
    assert file_hash(ROOT / "analysis/run_sar_three_plan.py") == file_hash(previous / "runner_source.py")
    dev_rows, seconds = [], 0
    for condition in CONDITIONS:
        old = {j["variant"]: read(ROOT / j["path"]) for j in scan["jobs"] if j["condition"] == condition}
        ground = strong_ground(old)
        if condition in GROUND:
            assert GROUND[condition] == (8 if ground == "ground_8" else 45)
        new = read(previous / f"{condition}.json")
        references = {k: old[k] for k in ("local", "cgs", "heft", "choice", ground)}
        best = max(references, key=lambda k: (references[k]["P300"], -references[k]["R300_seconds"]))
        ref = references[best]
        dev_rows.append({"condition": condition, "strongest_baseline": best,
                        "baseline_on_time": ref["on_time_tasks"], "v3_on_time": new["on_time_tasks"],
                        "tasks": new["total_tasks"], "delta_tasks": new["on_time_tasks"] - ref["on_time_tasks"],
                        "delta_P300_pp": 100 * (new["P300"] - ref["P300"]),
                        "delta_R300_s": new["R300_seconds"] - ref["R300_seconds"]})
        if condition in GROUND:
            seconds += sum(r["wall_seconds"] for r in references.values()) + new["wall_seconds"]
    hours = seconds * (86701 / 21600) * 6 / 3600
    save(out / "runtime_estimate.json", {"created_at_utc": now(), "runs": 180, "workers": 4,
         "worker_hours_linear": hours, "wall_hours_linear": hours / 4,
         "planning_wall_hours": [hours * 1.5 / 4 + 1, hours * 2 / 4 + 1],
         "basis": "Same-host six-hour development worker wall times, 86701/21600 horizon scaling, six dates; 1.5-2 allowance and 1h preparation are planning assumptions, not confidence intervals."})
    lines = ["# 冻结时开发成绩：相对最强对照", "", "对照含本地、逐条件强地面、CGS、HEFT、旧两方案消融；按 P300 高、R300 低选完整面板，不能逐图拼接。", "",
             "差值为 v3 减对照：P300 正值更好，R300 负值更好；带宽条件不进入正式测试。", "",
             "|条件|最强对照|对照按时数|v3 按时数|净增图数|P300 差/百分点|R300 差/秒|",
             "|---|---|---:|---:|---:|---:|---:|"]
    for r in dev_rows:
        lines.append(f"|{r['condition']}|{r['strongest_baseline']}|{r['baseline_on_time']}/{r['tasks']}|{r['v3_on_time']}/{r['tasks']}|{r['delta_tasks']:+d}|{r['delta_P300_pp']:+.4f}|{r['delta_R300_s']:+.4f}|")
    lines += ["", "冻结是用户明确的方法选择，不意味着旧跨条件不退化验收已通过。既有负结果继续保留。"]
    (out / "DEVELOPMENT_REPORT.md").write_text("\n".join(lines) + "\n")
    save(out / "development_differences.json", dev_rows)
    snapshot = out / "source"
    paths = sorted(ROOT.glob("*.py")) + sorted((ROOT / "sim").rglob("*.py")) + sorted((ROOT / "analysis").glob("*.py"))
    hashes = {}
    for src in paths:
        relative = src.relative_to(ROOT)
        dst = snapshot / relative
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        hashes[str(relative)] = file_hash(dst)
    main = ROOT / "results/sar_loopfree_20261017/main_config.json"
    shutil.copyfile(main, out / "main_config.json")
    save(out / "freeze.json", {"frozen_at_utc": now(), "decision": "User freezes v3; no further tuning",
         **runtime_metadata(), "files": hashes, "main_config_sha256": file_hash(main),
         "spec_sha256": file_hash(out / "README.md"), "ground_hops": GROUND, "dates": DATES,
         "methods": METHODS, "conditions": list(GROUND), "runs": 180,
         "development_evidence": {str(p.relative_to(ROOT)): file_hash(p) for p in
             [previous / "manifest.json", previous / "summary.json", ROOT / "results/sar_condition_scan_20261017/manifest.json"]}})
    print(json.dumps(read(out / "runtime_estimate.json")), flush=True)


def verify_freeze(out):
    frozen = read(out / "freeze.json")
    assert runtime_metadata()["source_sha256"] == frozen["source_sha256"]
    for relative, digest in frozen["files"].items():
        assert file_hash(ROOT / relative) == digest, f"Frozen source changed: {relative}"
    assert file_hash(out / "main_config.json") == frozen["main_config_sha256"]
    assert file_hash(out / "README.md") == frozen["spec_sha256"]
    return frozen


def generate(days, base):
    old_duration, old_select = geometry.DURATION_S, geometry.select_candidates
    duration = days * 86400
    tle_map = {sid: tle for sid, tle in coastal.sar_tles_by_source().items() if sid in range(24, 72, 2)}
    assert list(tle_map.values()) == [tuple(t) for t in base["topology"]["tle_lines"][:24]] or list(tle_map.values()) == base["topology"]["tle_lines"][:24]
    geometry.DURATION_S = duration
    geometry.select_candidates = lambda candidates: old_select(candidates, duration_s=duration)
    try:
        return geometry.generate_geometry(coastal.selection(), tle_map=tle_map)
    finally:
        geometry.DURATION_S, geometry.select_candidates = old_duration, old_select


def daily_config(base, events, date):
    offset = (datetime.fromisoformat(date) - datetime(2026, 10, 15)).days * 86400
    cfg = deepcopy(base)
    cfg["topology"]["start_time_utc"] = date + "T00:00:00Z"
    cfg["sim_steps"] = round(86701 / cfg["dt"])
    cfg["task_arrival_end_step"] = round(86400 / cfg["dt"])
    cfg["scheduled_task_arrivals"] = [
        {"release_step": round((e["release_s"] + 60 - offset) / cfg["dt"]),
         "source_sat_id": (e["source_sat_id"] - 24) // 2, "target_id": e["target_id"], "window_id": e["window_id"]}
        for e in events if offset <= e["release_s"] + 60 < offset + 86400]
    cfg, _ = output_fixture.derive(cfg, 2)
    return cfg


def check_development(out):
    base = read(out / "main_config.json") if (out / "main_config.json").exists() else read(ROOT / "results/sar_loopfree_20261017/main_config.json")
    geometry_data = generate(1, base)
    cfg = daily_config(base, geometry_data["events"], "2026-10-15")
    # 窗口编号随传播跨度和入选源星集合变化；比较物理任务身份及完整输出真值。
    normalize = lambda events: [{k: v for k, v in e.items() if k != "window_id"} for e in events]
    assert normalize(cfg["scheduled_task_arrivals"]) == normalize(base["scheduled_task_arrivals"])
    assert {k: v for k, v in cfg.items() if k != "scheduled_task_arrivals"} == {k: v for k, v in base.items() if k != "scheduled_task_arrivals"}
    result = {"passed": True, "tasks": len(cfg["scheduled_task_arrivals"]), "output_bytes_identical": True,
              "nonarrival_config_identical": True, "geometry": geometry_data["verification"]}
    save(out / "development_generator_check.json", result)
    print(json.dumps(result), flush=True)


def prepare(out):
    verify_freeze(out)
    if (out / "manifest.json").exists():
        return read(out / "manifest.json")
    assert not (out / "generation_started.json").exists(), "Interrupted one-time generation; report instead of silently regenerating"
    assert read(out / "development_generator_check.json")["passed"]
    save(out / "generation_started.json", {"started_at_utc": now(), "days": 53})
    base = read(out / "main_config.json")
    all_geometry = generate(53, base)
    save(out / "acquisition.json", all_geometry)
    jobs, dates = [], {}
    for date in DATES:
        daily = daily_config(base, all_geometry["events"], date)
        dates[date] = {"tasks": len(daily["scheduled_task_arrivals"]), "start_time_utc": daily["topology"]["start_time_utc"]}
        for condition, hops in GROUND.items():
            cfg = derive(daily, *CONDITIONS[condition])
            for method, code in METHODS.items():
                config = deepcopy(cfg)
                if method == "ground":
                    config["relay_max_hops"] = hops
                key = f"{date}_{condition}_{method}"
                config_path = out / "configs" / f"{key}.json"
                save(config_path, config)
                jobs.append({"key": key, "date": date, "condition": condition, "method": method,
                             "method_code": code, "ground_hops": hops, "config": str(config_path),
                             "config_sha256": file_hash(config_path), "record": str(out / "records" / f"{key}.json")})
    manifest = {"prepared_at_utc": now(), "freeze_sha256": file_hash(out / "freeze.json"),
                "acquisition_sha256": file_hash(out / "acquisition.json"), "dates": dates, "jobs": jobs}
    assert len(jobs) == 180
    save(out / "manifest.json", manifest)
    return manifest


def audit_record(record, job, frozen):
    assert file_hash(Path(job["config"])) == job["config_sha256"]
    cfg = read(job["config"])
    validate_record(record, cfg, frozen["source_sha256"], job["method_code"])
    score = task_age_score(record["metrics"]["task_outcomes"], [300], record["observation_end_seconds"])
    p, r = score["delivery_curve"][0]["delivery_rate"], score["restricted_mean_latency_seconds"]
    assert p == record["P300"] and r == record["R300_seconds"]
    assert record["on_time_tasks"] == score["delivery_curve"][0]["delivered_within_horizon"]
    if job["method"] == "three_plan":
        assert record["unique_ground_audit"]["passed"]
        assert record["extension_sha256"] == frozen["files"]["analysis/sar_three_plan.py"]
        assert record["raw_ground_hops"] == job["ground_hops"]


def execute(job, out):
    frozen = verify_freeze(out)
    log = out / "logs" / f"{job['key']}.log"
    log.parent.mkdir(exist_ok=True)
    with log.open("a", buffering=1) as handle, redirect_stdout(handle), redirect_stderr(handle):
        print(json.dumps({"started_at_utc": now(), "job": job["key"]}), flush=True)
        cfg = read(job["config"])
        assert file_hash(Path(job["config"])) == job["config_sha256"]
        if job["method"] == "three_plan":
            record = run_job(job["condition"], job["ground_hops"], cfg, frozen["source_sha256"],
                             frozen["files"]["analysis/sar_three_plan.py"], "v3")
        else:
            record = measured_job(cfg, job["method_code"], frozen["source_sha256"],
                                  frozen["files"]["analysis/run_sar_frozen.py"], 6)
        record.update(derived_config_sha256=runner.digest(cfg), formal_job=job["key"],
                      formal_freeze_sha256=file_hash(out / "freeze.json"), finished_at_utc=now())
        audit_record(record, job, frozen)
        save(job["record"], record)
        print(json.dumps({"finished_at_utc": now(), "wall_seconds": record["wall_seconds"]}), flush=True)
    return job["key"]


def average(values):
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else None


def report(out):
    frozen = verify_freeze(out)
    manifest = read(out / "manifest.json")
    assert manifest["freeze_sha256"] == file_hash(out / "freeze.json")
    assert manifest["acquisition_sha256"] == file_hash(out / "acquisition.json")
    rows, records, hashes = [], {}, {}
    for job in manifest["jobs"]:
        r = read(job["record"])
        audit_record(r, job, frozen)
        assert r["formal_freeze_sha256"] == manifest["freeze_sha256"] and r["formal_job"] == job["key"]
        hashes[job["key"]] = file_hash(Path(job["record"]))
        records[job["condition"], job["date"], job["method"]] = r
        rows.append({k: job[k] for k in ("condition", "date", "method")} | {
            k: r[k] for k in ("P300", "R300_seconds", "on_time_tasks", "total_tasks", "wall_seconds")})
    means, pairs, wins = [], [], []
    for condition in GROUND:
        for method in METHODS:
            group = [r for r in rows if r["condition"] == condition and r["method"] == method]
            means.append({"condition": condition, "method": method, "days": 6,
                          "defined_days": sum(r["P300"] is not None for r in group),
                          "P300_day_equal": average([r["P300"] for r in group]),
                          "R300_day_equal_s": average([r["R300_seconds"] for r in group])})
            if method == "three_plan":
                continue
            per_method = []
            for date in DATES:
                new, ref = records[condition, date, "three_plan"], records[condition, date, method]
                def identities(r):
                    return [(t["task_id"], t["source_sat_id"], t["release_seconds"]) for t in r["metrics"]["task_outcomes"]]
                assert identities(new) == identities(ref)
                p = None if new["P300"] is None else 100 * (new["P300"] - ref["P300"])
                r = None if new["R300_seconds"] is None else new["R300_seconds"] - ref["R300_seconds"]
                pair = {"condition": condition, "date": date, "baseline": method,
                        "delta_P300_pp": p, "delta_R300_s": r,
                        "P300_result": "undefined" if p is None else "win" if p > 0 else "loss" if p < 0 else "tie",
                        "gained_task_ids": sorted(set(new["on_time_task_ids"]) - set(ref["on_time_task_ids"])),
                        "lost_task_ids": sorted(set(ref["on_time_task_ids"]) - set(new["on_time_task_ids"]))}
                pairs.append(pair)
                per_method.append(pair)
            wins.append({"condition": condition, "baseline": method,
                         **{k: sum(p["P300_result"] == k for p in per_method) for k in ("win", "tie", "loss", "undefined")},
                         "delta_P300_day_equal_pp": average([p["delta_P300_pp"] for p in per_method]),
                         "delta_R300_day_equal_s": average([p["delta_R300_s"] for p in per_method])})
    save(out / "summary.json", {"completed_at_utc": now(), "daily": rows, "day_equal_means": means,
         "daily_pairs": pairs, "wins_losses": wins, "record_sha256": hashes,
         "freeze_sha256": manifest["freeze_sha256"], "method_changes_after_test": False})
    fmt = lambda x, scale=1: "NA" if x is None else f"{x * scale:.4f}"
    lines = ["# v3 冻结正式测试", "", "180 次全部完成并通过来源、配置、执行与独立评分审计。测试仅用于报告，不用于改方法。",
             "P300 越高越好，R300 越低越好；主均值按日等权。逐日任务定义在六方法间逐一核验相同。", "",
             "## 日等权均值", "", "|条件|方法|P300 / %|R300 / 秒|可定义日数|", "|---|---|---:|---:|---:|"]
    for r in means:
        lines.append(f"|{r['condition']}|{r['method']}|{fmt(r['P300_day_equal'],100)}|{fmt(r['R300_day_equal_s'])}|{r['defined_days']}/6|")
    lines += ["", "## 三方案对每个基线的逐日胜负汇总", "", "胜/平/负仅按 P300；R300 独立列出，不将 P300 平局改写为胜。差值均为三方案减基线。", "",
              "|条件|基线|胜/平/负/NA|日均 P300 差/百分点|日均 R300 差/秒|", "|---|---|---|---:|---:|"]
    for r in wins:
        lines.append(f"|{r['condition']}|{r['baseline']}|{r['win']}/{r['tie']}/{r['loss']}/{r['undefined']}|{fmt(r['delta_P300_day_equal_pp'])}|{fmt(r['delta_R300_day_equal_s'])}|")
    lines += ["", "## 每条件逐日成绩", "", "|条件|日期|方法|按时/总图|P300 / %|R300 / 秒|", "|---|---|---|---:|---:|---:|"]
    for r in rows:
        lines.append(f"|{r['condition']}|{r['date']}|{r['method']}|{r['on_time_tasks']}/{r['total_tasks']}|{fmt(r['P300'],100)}|{fmt(r['R300_seconds'])}|")
    lines += ["", "## 逐日配对差值", "", "|条件|日期|基线|P300胜负|P300差/百分点|R300差/秒|", "|---|---|---|---|---:|---:|"]
    for r in pairs:
        lines.append(f"|{r['condition']}|{r['date']}|{r['baseline']}|{r['P300_result']}|{fmt(r['delta_P300_pp'])}|{fmt(r['delta_R300_s'])}|")
    lines += ["", "逐图新增/丢失与每份记录哈希见 summary.json。六日是同一合成星座的相位留出，不是六个独立星座；不据此声称普遍最优。",
              "开发最强基线差值见 DEVELOPMENT_REPORT.md，报价低估与地面贡献限制见 README.md。学习提案待用户确认，未实施或训练。"]
    (out / "REPORT.md").write_text("\n".join(lines) + "\n")


def run(out):
    start = time.monotonic()
    save(out / "progress.json", {"status": "generating", "done": 0, "total": 180, "started_at_utc": now()})
    try:
        manifest = prepare(out)
        frozen = verify_freeze(out)
        todo = []
        for job in manifest["jobs"]:
            if Path(job["record"]).exists():
                audit_record(read(job["record"]), job, frozen)
            else:
                todo.append(job)
        done = 180 - len(todo)
        save(out / "progress.json", {"status": "running", "done": done, "total": 180, "updated_at_utc": now()})
        with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context("spawn"), max_tasks_per_child=1) as pool:
            futures = {pool.submit(execute, j, out): j for j in todo}
            for future in as_completed(futures):
                job = futures[future]
                try:
                    key = future.result()
                except Exception:
                    for pending in futures:
                        pending.cancel()
                    raise RuntimeError(f"Job failed: {job['key']}\n{traceback.format_exc()}")
                done += 1
                save(out / "progress.json", {"status": "running", "done": done, "total": 180, "last_job": key,
                     "updated_at_utc": now(), "elapsed_seconds": time.monotonic() - start})
                print(json.dumps(read(out / "progress.json")), flush=True)
        report(out)
        save(out / "progress.json", {"status": "complete", "done": 180, "total": 180,
             "finished_at_utc": now(), "elapsed_seconds": time.monotonic() - start})
    except Exception:
        failure = {"status": "failed", "failed_at_utc": now(), "traceback": traceback.format_exc()}
        save(out / "FAILED.json", failure)
        save(out / "progress.json", failure)
        raise


def launch(out):
    assert not (out / "launch.json").exists(), "Already launched; inspect process and records first"
    verify_freeze(out)
    command = [sys.executable, str(out / "source/analysis/run_sar_formal.py"), "run", "--out", str(out)]
    # 脱离交互终端；独立快照避免工作区后续编辑影响测试。
    with (out / "background.log").open("ab") as log:
        process = subprocess.Popen(command, cwd=out / "source", stdout=log, stderr=log,
                                   stdin=subprocess.DEVNULL, start_new_session=True)
    awake = subprocess.Popen(["/usr/bin/caffeinate", "-i", "-w", str(process.pid)],
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
    save(out / "launch.json", {"started_at_utc": now(), "pid": process.pid, "caffeinate_pid": awake.pid,
         "command": command, "workers": 4, "runs": 180})
    print(json.dumps(read(out / "launch.json")), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check-development", "freeze", "launch", "run", "report"))
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    {"check-development": check_development, "freeze": freeze, "launch": launch,
     "run": run, "report": report}[args.action](out)


if __name__ == "__main__":
    main()
