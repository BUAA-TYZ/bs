"""主场景局部基线独立编排；复用冻结执行器，旧产物不修改。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import run_load_pressure_six as six
from analysis import run_load_pressure_short as short
from analysis.freeze_main_scenario import ROOT as MAIN
from analysis.local_information_baselines import LocalInformationPlane, LocalMinLoad
from analysis.redesign_scenario import write_json
from analysis.sar_three_plan import ThreePlanChoice
from analysis.estimate_load_pressure import sha, OUT

ORIGINAL = six.ROOT
ROOT = MAIN/"local_baselines"
JOBS = [(variant, f"local_{k}") for variant in ("zero", "history", "minload") for k in (1, 2)]


def read(path):
    return json.loads(path.read_text())


def verify_main():
    manifest = read(MAIN/"manifest.json")
    for name in ("files", "evidence_hashes", "source_entry_hashes"):
        for path, expected in manifest[name].items():
            assert sha(Path(path)) == expected, path


def prepare():
    if ROOT.exists():
        raise FileExistsError(ROOT)
    verify_main()
    p = read(ORIGINAL/"protocol.json")
    short.verify(p)
    p.update(purpose="Complete static plan plus k-hop dynamics: frozen main M local baseline strengthening",
             stages={v: ["local_1", "local_2"] for v in ("zero", "history", "minload")},
             workers=6, jobs=[list(x) for x in JOBS], no_automatic_retry=True,
             reused_records={"fifo": str(ORIGINAL/"neutral/fifo.json"), "v3": str(ORIGINAL/"information/v3.json")})
    for path in [Path(__file__), Path("analysis/local_information_baselines.py"),
                 Path("tests/test_local_information_baselines.py"), MAIN/"manifest.json", MAIN/"LOCAL_PROTOCOL.md",
                 ORIGINAL/"upper_bound.json", ORIGINAL/"neutral/fifo.json", ORIGINAL/"information/v3.json"]:
        p["input_hashes"][str(path)] = sha(path)
    for phase in p["stages"]:
        (ROOT/phase/"progress").mkdir(parents=True)
    write_json(ROOT/"protocol.json", p)
    write_json(ROOT/"upper_bound.json", read(ORIGINAL/"upper_bound.json"))
    short.verify(p)
    print(json.dumps({"jobs": JOBS, "workers": 6, "protocol_sha256": sha(ROOT/"protocol.json")}))


def worker(variant, method, p):
    verify_main()
    six.ROOT = ROOT
    # 仅替换本子进程的观测提供者/策略工厂；冻结执行器、三报价公式和路由不变。
    class Plane(LocalInformationPlane):
        estimator = "history" if variant == "history" else "zero"
        history_hops = int(method[-1])
        last_instance = None

        def __init__(self, sources):
            super().__init__(sources)
            Plane.last_instance = self
    six.ControlPlane = Plane
    six.ThreePlanChoice = LocalMinLoad if variant == "minload" else ThreePlanChoice
    status = six.run_job(method, variant, p)
    file = ROOT/variant/f"{method}.json"
    if file.exists():
        record = read(file)
        plane = Plane.last_instance
        record["observation_audit"] = {"estimator": plane.estimator, "hops": plane.history_hops,
            "history_samples_per_source": {str(s): len(v) for s, v in plane.seen_samples.items()},
            "unknown_field_estimator_counts": dict(plane.observation_stats),
            "three_plan_quotes_present": variant != "minload" and all(set(d["quotes_seconds"]) == {"local", "cgs", "ground"} for d in record["decisions"]),
            "ground_finite_quote_count": sum(d.get("quotes_seconds", {}).get("ground") is not None for d in record["decisions"])}
        write_json(file, record)
    return {**status, "variant": variant}


def collect():
    p = read(ROOT/"protocol.json")
    short.verify(p); verify_main()
    assert read(ROOT/"launch.json")["protocol_sha256"] == sha(ROOT/"protocol.json")
    upper = read(ROOT/"upper_bound.json")
    rows, failures = {}, []
    six.ROOT = ROOT
    for variant, method in JOBS:
        key = f"{variant}_{method}"
        try:
            row = six.audit_record(method, variant, p, upper)
            row["observation_audit"] = read(ROOT/variant/f"{method}.json")["observation_audit"]
            rows[key] = row
        except Exception:
            failure = {"job": key, "traceback": traceback.format_exc()}
            failures.append(failure)
            write_json(ROOT/variant/f"{method}_AUDIT_FAILURE.json", failure)
    six.ROOT = ORIGINAL
    old = read(ORIGINAL/"protocol.json")
    rows["fifo"] = six.audit_record("fifo", "neutral", old, upper)
    rows["v3"] = six.audit_record("v3", "information", old, upper)
    paired = {}
    for key, row in rows.items():
        if key == "v3":
            continue
        v = rows["v3"]
        paired[key] = {"P_D_difference_pp": 100*(v["P_D"]-row["P_D"]),
                       "R_D_difference_s": v["R_D_seconds"]-row["R_D_seconds"],
                       "gained": sorted(set(v["on_time_task_ids"])-set(row["on_time_task_ids"])),
                       "lost": sorted(set(row["on_time_task_ids"])-set(v["on_time_task_ids"]))}
    best = best_with_fifo = None
    if not failures:
        rank = lambda key: (-rows[key]["P_D"], rows[key]["R_D_seconds"], key)
        best = min((key for key in rows if key not in {"v3", "fifo"}), key=rank)
        best_with_fifo = min((key for key in rows if key != "v3"), key=rank)
    summary = {"methods": rows, "failures": failures, "best_local": best,
               "best_including_fifo_diagnostic": best_with_fifo, "paired": paired,
               "upper_bound": upper["fraction"], "upper_minus_v3_pp": 100*(upper["fraction"]-rows["v3"]["P_D"]),
               "small_learning_space": paired[best]["P_D_difference_pp"] < 3 if best else None,
               "no_training": True, "sealed_dates_accessed": False}
    write_json(ROOT/"summary.json", summary)
    lines = ["# 主场景M：补强局部信息基线", "", "同一六小时、340评分图；P_D越高越好，R_D为截断D的整图时延均值（秒），越低越好。",
             "", "|规则|按时/340|P_D|R_D(s)|v3减该规则(pp)|v3减该规则(s)|", "|---|---:|---:|---:|---:|---:|"]
    for name, row in rows.items():
        pair = paired.get(name, {"P_D_difference_pp": 0, "R_D_difference_s": 0})
        lines.append(f"|{name}|{row['on_time_tasks']}|{row['P_D']:.4%}|{row['R_D_seconds']:.3f}|{pair['P_D_difference_pp']:.4f}|{pair['R_D_difference_s']:.3f}|")
    lines.extend(["", f"失败/审计异常：{json.dumps(failures, ensure_ascii=False)}。异常不当作0分，不从不完整面板选最强规则。",
                  f"最强局部规则：{best}；含FIFO诊断的保守参照：{best_with_fifo}。",
                  f"固定终端配对零代价上界：{upper['fraction']:.4%}；上界-v3={summary['upper_minus_v3_pp']:.4f}个百分点。"])
    if best:
        pair = paired[best]
        lines.append(f"v3相对最强局部：新增{len(pair['gained'])}图、丢失{len(pair['lost'])}图；详见summary.json逐图名单。")
        lines.append("v3与最强局部差距不足3个百分点，学习方法在P_D上的空间很小。" if summary["small_learning_space"] else
                     "信息差距达到3个百分点，但仍不是已证明可实现的学习收益；需考虑局部信息可辨识性与训练成本。")
    lines.extend(["", "静态计划完整保留；动态通告仍有实际链路开销、延迟及TTL。history只使用本源星以前合法获得的逐节点样本均值；从未观测节点和无历史的地面队列均回退零负载。有限地面报价数、历史样本数见各原始记录的observation_audit。",
                  "FIFO保持诊断身份；上界差含实际传输与计算成本。此开发片段用于基线补强，不是新盲测。",
                  "", "主场景与日期登记见../manifest.json、../date_split.json；成本评估见../RL_COST_ASSESSMENT.md。完成后停止，不训练、不改论文、不追加实验。"])
    (ROOT/"REPORT.md").write_text("\n".join(lines)+"\n")
    audit = {"status": "PASS" if not failures else "BLOCKED", "source_file_count": len(p["frozen_protocol"]["source_hashes"]),
             "frozen_hashes_pass": True, "main_manifest_sha256": sha(MAIN/"manifest.json"),
             "protocol_sha256": sha(ROOT/"protocol.json"), "audited_methods": list(rows), "failures": failures}
    write_json(ROOT/"FINAL_AUDIT.json", audit)
    write_json(ROOT/"final_status.json", {"status": "COMPLETE" if not failures else "BLOCKED", "failures": failures,
               "stop": "Await user; no training or additional simulation"})
    return summary


def panel():
    p = read(ROOT/"protocol.json")
    started = time.perf_counter()
    try:
        short.verify(p); verify_main()
        statuses = []
        with ProcessPoolExecutor(max_workers=6, mp_context=multiprocessing.get_context("spawn")) as pool:
            pending = {pool.submit(worker, v, m, p): (v, m) for v, m in JOBS}
            for future in as_completed(pending):
                v, m = pending[future]
                try:
                    status = future.result()
                except Exception:
                    status = {"variant": v, "method": m, "status": "FAILED", "traceback": traceback.format_exc()}
                    write_json(ROOT/v/f"{m}_FAILED.json", {**status, "config": p["config"]})
                statuses.append(status)
                write_json(ROOT/"progress.json", {"pid": os.getpid(), "done": len(statuses), "total": 6,
                           "statuses": statuses, "wall_seconds": time.perf_counter()-started})
        collect()
    except Exception:
        write_json(ROOT/"final_status.json", {"status": "BLOCKED", "traceback": traceback.format_exc()})
        raise


def launch():
    p = read(ROOT/"protocol.json")
    short.verify(p); verify_main()
    if (ROOT/"launch.json").exists():
        raise FileExistsError("No automatic relaunch")
    with (ROOT/"panel.log").open('xb') as log:
        env = dict(os.environ, OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1", VECLIB_MAXIMUM_THREADS="1")
        proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "panel"], env=env,
                                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    guard = subprocess.Popen(["caffeinate", "-i", "-w", str(proc.pid)], stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
    data = {"pid": proc.pid, "caffeinate_pid": guard.pid, "workers": 6,
            "command": f"{sys.executable} {Path(__file__).resolve()} panel",
            "created_utc": datetime.now(timezone.utc).isoformat(), "protocol_sha256": sha(ROOT/"protocol.json")}
    write_json(ROOT/"launch.json", data)
    print(json.dumps(data))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'launch', 'panel', 'collect'])
    args = parser.parse_args()
    globals()[args.mode]()
