"""只复核容量诊断记录、忙时积分和判据，输出可复用表格。"""

import argparse
import csv
import hashlib
import json
from pathlib import Path
import sys

import numpy as np

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from analysis.redesign_scenario import digest, write_json
from analysis.run_calibration_capacity import busiest_window, check_frozen


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def csv_write(path,rows):
    with path.open("w",newline="") as handle:
        writer=csv.DictWriter(handle,fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def audit(directory):
    protocol=json.loads((directory/"protocol.json").read_text())
    check_frozen(protocol)
    assert protocol["scope"]=="full_day"
    upper_file=Path("results/redesign_20261021/calibration/bounds_144.json")
    assert sha(upper_file)==protocol["upper_bound_file_sha256"]
    summary=json.loads((directory/"summary.json").read_text())
    table,images,hashes=[],[],{}
    for key,job in protocol["jobs"].items():
        path=directory/f"{key}.json"
        r=json.loads(path.read_text())
        assert r["status"]=="PASS" and r["audit"]["passed"]
        assert r["source_hashes"]==protocol["source_hashes"]
        assert r["implementation_hashes"]==protocol["implementation_hashes"]
        assert r["config_sha256"]==job["config_sha256"]==digest(job["config"])
        n=len(job["config"]["scheduled_task_arrivals"])
        assert r["total_tasks"]==len(r["outcomes"])==n
        assert len({o["task_id"] for o in r["outcomes"]})==n
        on_time=[]
        times=[]
        for i,o in enumerate(r["outcomes"]):
            task_index=int(o["task_id"].split("_")[-1])
            e=job["config"]["scheduled_task_arrivals"][task_index]
            assert o["source_sat_id"]==e["source_sat_id"]
            assert abs(o["release_seconds"]-e["release_step"]*job["config"]["dt"])<=1e-8
            assert o["observed_age_seconds"]>=300
            latency=o["latency_seconds"] if o["status"]=="completed" else None
            timely=latency is not None and latency<=300
            if timely:
                on_time.append(o["task_id"])
            restricted=min(latency,300) if latency is not None else 300
            times.append(restricted)
            images.append({"key":key,"task_id":o["task_id"],"source_sat_id":o["source_sat_id"],
                           "release_s":o["release_seconds"],"latency_s":latency,"on_time":int(timely),
                           "restricted_latency_s":restricted,"status":o["status"]})
        assert set(on_time)==set(r["on_time_task_ids"])
        assert len(on_time)==r["on_time_tasks"]
        assert abs(len(on_time)/n-r["P_D"])<=1e-12
        assert abs(sum(times)/n-r["R_D_seconds"])<=1e-6
        busy_file=directory/f"{key}_busy_1s.npz"
        with np.load(busy_file) as arrays:
            values=arrays["busy"]
            helpers=arrays["helper_ids"]
        assert values.shape==(86400,144) and len(helpers)==144
        assert np.all(np.isfinite(values)) and values.min()>=-1e-6 and values.max()<=1+1e-6
        c=r["competition"]
        per_sat=values.sum(axis=0)
        total=float(per_sat.sum())
        assert abs(total-c["helper_busy_seconds"])<=1e-6
        cluster=busiest_window(values.sum(axis=1))
        for f in ("start_s","end_s","duration_s"):
            assert cluster[f]==c["busiest_600s_cluster"][f]
        assert abs(cluster["busy_s"]-c["busiest_600s_cluster"]["busy_s"])<=1e-6
        assert abs(cluster["busy_s"]/(144*600)-c["busiest_600s_cluster"]["utilization"])<=1e-10
        hotspot=max(({"sat_id":int(sid),**busiest_window(values[:,i])} for i,sid in enumerate(helpers)),
                    key=lambda x:(x["busy_s"],-x["start_s"],-x["sat_id"]))
        assert hotspot["sat_id"]==c["busiest_600s_single_helper"]["sat_id"]
        assert abs(hotspot["busy_s"]-c["busiest_600s_single_helper"]["busy_s"])<=1e-6
        if total:
            assert abs(float(np.sort(per_sat)[-15:].sum()/total)-c["top10_share"])<=1e-12
        else:
            assert c["top10_share"] is None
        q=c["peak_queue"]
        assert c["criterion_c"]==(q["complete_waiting_images"]>=1 or q["waiting_source_count"]>=2)
        for metric,value in q.items():
            if value:
                witness=c["peak_witnesses"][metric]
                assert witness[metric]==value and witness["sat_id"] in helpers
                assert witness["waiting_source_count"]==len(set(witness["source_ids"]))
                assert all(t in witness["task_ids"] for t in witness["complete_task_ids"])
        if job["method"]!="fifo":
            assert total==0 and all(value==0 for value in q.values())
        table.append({"key":key,"method":job["method"],"images":n,"on_time":len(on_time),
                      "P_D":r["P_D"],"R_D_s":r["R_D_seconds"],"upper_bound":protocol["upper_bound_fraction"],
                      **q,"qualifying_queue_seconds":c["qualifying_queue_seconds"],
                      "mean_helper_utilization":c["mean_utilization"],
                      "cluster_peak600_utilization":c["busiest_600s_cluster"]["utilization"],
                      "cluster_peak600_start_s":cluster["start_s"],
                      "hotspot_peak600_utilization":c["busiest_600s_single_helper"]["utilization"],
                      "hotspot_id":hotspot["sat_id"],"hotspot_peak600_start_s":hotspot["start_s"],
                      "top15_load_share":c["top10_share"],"wall_seconds":r["wall_seconds"]})
        hashes[key]={"record":sha(path),"busy_series":sha(busy_file)}
    csv_write(directory/"main_table.csv",table)
    csv_write(directory/"per_image.csv",images)
    write_json(directory/"FINAL_AUDIT.json",{"status":"PASS","records":len(table),"per_image_rows":len(images),
        "source_files_verified":len(protocol["source_hashes"]),"protocol_sha256":sha(directory/"protocol.json"),
        "auditor_sha256":sha(Path(__file__)),"record_hashes":hashes,"sealed_test_dates_accessed":False,
        "queue_audit_scope":"Threshold/witness consistency and tested observation logic; no full independent queue-event replay",
        "panel_status":summary["status"],"criteria":summary["criteria"]})
    print(json.dumps({"audit":"PASS","records":len(table),"criteria":summary["criteria"]}),flush=True)


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("directory",type=Path)
    audit(p.parse_args().directory)
