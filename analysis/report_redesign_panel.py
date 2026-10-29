"""只读复核新 M 面板记录，输出逐图配对和解释；不实例化仿真环境。"""

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.redesign_scenario import digest, write_json


def report(directory, scenario):
    protocol = json.loads((directory / "protocol.json").read_text())
    manifest = json.loads((scenario / "manifest.json").read_text())
    config = protocol["config"]
    assert digest(config) == protocol["config_sha256"] == manifest["config_sha256"]
    assert all(hashlib.sha256(Path(path).read_bytes()).hexdigest() == expected
               for path, expected in protocol["source_hashes"].items())
    events = {row["task_id"]: row for row in manifest["events"]}
    for row in events.values():
        truth = json.loads((scenario / "truth" / f"image_{row['image_id']}.json").read_text())
        assert digest(truth) == row["truth_sha256"]
    records, rows, per_mode, comparisons = {}, [], {}, {}
    for method in protocol["methods"]:
        record = json.loads((directory / f"{method}.json").read_text())
        assert record["status"] == "PASS" and record["audit"]["passed"]
        assert record["config_sha256"] == protocol["config_sha256"]
        assert record["source_hashes"] == protocol["source_hashes"]
        outcomes = {o["task_id"]: o for o in record["outcomes"]}
        assert len(outcomes) == record["total_tasks"] == len(events)
        assert outcomes.keys() == events.keys()
        on_time, restricted = [], []
        groups = defaultdict(list)
        last_quote = {r["task_id"]: r for r in record["decisions"]}
        for task_id, event in events.items():
            outcome = outcomes[task_id]
            assert outcome["source_sat_id"] == event["source_sat_id"]
            assert abs(outcome["release_seconds"] - event["ready_s"]) < 1e-6
            latency = outcome["latency_seconds"] if outcome["status"] == "completed" else None
            timely = latency is not None and latency <= 300
            restricted.append(min(latency, 300) if latency is not None else 300)
            if timely:
                on_time.append(task_id)
            quote = last_quote.get(task_id, {})
            selected = quote.get("selected")
            predicted = quote.get("quotes_seconds", {}).get(selected)
            row = {"method": method, "task_id": task_id, "source_sat_id": event["source_sat_id"],
                   "mode": event["mode"], "ready_s": event["ready_s"], "on_time": int(timely),
                   "status": outcome["status"], "latency_s": latency, "restricted_latency_s": restricted[-1],
                   "last_recorded_plan": selected, "last_quote_s": predicted,
                   "actual_minus_last_quote_s": latency-predicted if latency is not None and predicted is not None else None}
            rows.append(row)
            groups[event["mode"]].append(row)
        assert set(on_time) == set(record["on_time_task_ids"])
        assert len(on_time) == record["on_time_tasks"]
        assert abs(record["P_D"] - len(on_time)/len(events)) < 1e-12
        assert abs(record["R_D_seconds"] - sum(restricted)/len(events)) <= 1e-6
        per_mode[method] = {mode: {"images": len(group), "on_time":sum(r["on_time"] for r in group),
                                  "R_D_seconds":sum(r["restricted_latency_s"] for r in group)/len(group)}
                            for mode, group in groups.items()}
        records[method] = record
    with (directory / "per_image.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    for method, other in records.items():
        if method == "v3":
            continue
        own = records["v3"]
        comparisons[method] = {"P_D_pp":100*(own["P_D"]-other["P_D"]),
                               "R_D_seconds":own["R_D_seconds"]-other["R_D_seconds"],
                               "gained":sorted(set(own["on_time_task_ids"])-set(other["on_time_task_ids"])),
                               "lost":sorted(set(other["on_time_task_ids"])-set(own["on_time_task_ids"]))}
    audit = {"passed": True, "methods":len(records), "images_per_method":len(events),
             "source_hashes_match": True, "config_and_truth_hashes_match":True,
             "score_recomputed":True, "paired_v3":comparisons,"by_mode":per_mode,
             "record_hashes":{method:hashlib.sha256((directory/f"{method}.json").read_bytes()).hexdigest()
                              for method in records}}
    windows = sum(len(event["window_result_bytes"]) for event in config["scheduled_task_arrivals"])
    helper_count = config["num_sats"] - len(config["task_source_sat_ids"])
    slow_service = config["onboard_compute_profile"]["service_s"]["1"]
    capacity_bound = windows * slow_service / (helper_count * config["sim_steps"] * config["dt"])
    audit["workload_capacity_bound"] = {
        "windows": windows, "single_window_seconds": slow_service,
        "all_windows_on_helpers_busy_seconds_upper_bound": windows * slow_service,
        "helper_mean_utilization_upper_bound": capacity_bound,
        "assumptions": "Every inference window executes at most once; all assigned to helpers; single-window service (no batching benefit); excludes forwarding and host simulator CPU"}
    write_json(directory / "FINAL_AUDIT.json", audit)
    lines = ["# M 档逐图配对复核", "", "同一开发日136图配对，不能据单日结果给出跨日期统计显著性。",
             "", "| v3相对方法 | P_D差（百分点） | R_D差（秒，负值更好） | 新增按时 | 丢失按时 |",
             "|---|---:|---:|---:|---:|"]
    for method, pair in comparisons.items():
        lines.append(f"|{method}|{pair['P_D_pp']:+.4f}|{pair['R_D_seconds']:+.4f}|{len(pair['gained'])}|{len(pair['lost'])}|")
    lines += ["", "逐图数据见 [per_image.csv](per_image.csv)，每一条件的增减ID及模式统计见 [FINAL_AUDIT.json](FINAL_AUDIT.json)。",
              "", "v3报价为最后一次完整整图复核的记录；报价不可行、等待或部分派发时可能没有有限报价。不能把空报价当作0，也不能把该误差归为单一预测原因。",
              "", f"总工作量为{windows}个推理窗口。即使全部交给{helper_count}颗计算星并全部按单窗服务计时，计算星全时段平均计算利用率也至多{capacity_bound:.4%}。因此本基准不能声称检验全天整体算力饱和；短时热点、接入与回传竞争仍须由实际指标判断。",
              "", "M档12颗计算星位于单一轨道面。资源比例、候选可见性和实际任务接入需要分别解释；低忙时不能当作已证明星上计算协同无价值。",
              "", "本轮只报告G-v3与L1/L2的信息权限对照；同动作集G2及反事实尚未执行，不能将差距直接声称为学习方法可以取得的改进。测试仍封存。"]
    (directory / "PAIRED_REPORT.md").write_text("\n".join(lines)+"\n")
    print(json.dumps({"audit_passed":True,"paired_v3":comparisons},ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory",type=Path)
    parser.add_argument("scenario",type=Path)
    args = parser.parse_args()
    report(args.directory,args.scenario)
