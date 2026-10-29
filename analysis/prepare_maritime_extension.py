"""登记全美国沿海目标扩展，纯几何工作负载及成本预检。"""

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import multiprocessing
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.audit_sar_coastal_coverage import COASTAL_TARGETS
from analysis.estimate_load_pressure import OUT, PANEL, peak_window, sha
from analysis.prepare_load_pressure import arrivals, distance_km, source_windows
from analysis.redesign_scenario import write_json


def main():
    directory = OUT/"maritime_extension"
    directory.mkdir(exist_ok=False)
    data = json.loads((OUT/"noaa_waterlevels.json").read_text())
    chosen = [dict(r) for r in COASTAL_TARGETS]
    candidates = [{"target_id":"noaa_"+r["id"],"lat_deg":r["lat"],"lon_deg":r["lng"],"radius_km":0,
                   "noaa_station_id":r["id"],"noaa_station_name":r["name"],"source_url":r["self"]}
                  for r in data["stations"] if r.get("tidal") and not r.get("greatlakes")
                  and r["id"] not in {x["noaa_station_id"] for x in chosen}]
    while candidates and len(chosen)<160:
        ranked = sorted(((min(distance_km(r,c) for c in chosen),r["target_id"],r) for r in candidates),
                        key=lambda r:(-r[0],r[1]))
        if ranked[0][0]<20:
            break
        chosen.append(ranked[0][2])
        candidates.remove(ranked[0][2])
    registration = {"created_utc":datetime.now(timezone.utc).isoformat(),"targets":chosen,
                    "sets":{"maritime80":[r["target_id"] for r in chosen[:80]],
                            "maritime160":[r["target_id"] for r in chosen]},
                    "rule":"All US NOAA tidal non-Great-Lakes stations; farthest point, at least 20km; original five retained",
                    "input_hashes":{str(p):sha(p) for p in (OUT/"MARITIME_EXTENSION.md",OUT/"noaa_waterlevels.json",
                         Path(__file__),Path("analysis/prepare_load_pressure.py"),PANEL/"protocol.json")}}
    write_json(directory/"registration.json",registration)
    cfg=json.loads((PANEL/"protocol.json").read_text())["jobs"]["compute_4_local"]["config"]
    windows=[]
    with ProcessPoolExecutor(max_workers=8,mp_context=multiprocessing.get_context("spawn")) as pool:
        futures=[pool.submit(source_windows,sid,cfg["topology"]["tle_lines"][:24],chosen) for sid in range(24)]
        for done,future in enumerate(as_completed(futures),1):
            sid,rows=future.result()
            windows.extend(rows)
            print(json.dumps({"done":done,"sid":sid}),flush=True)
    windows.sort(key=lambda w:(w["start_s"],w["source_sat_id"],w["target_id"]))
    for i,w in enumerate(windows):
        w["window_id"]=i
    write_json(directory/"windows.json",windows)
    rows={}
    wall=max(json.loads((PANEL/f"compute_4_{m}.json").read_text())["wall_seconds"] for m in ("local","ground","fifo"))
    for name,ids in registration["sets"].items():
        events=arrivals(windows,set(ids))
        write_json(directory/f"{name}_arrivals.json",events)
        t,b=[e["ready_s"] for e in events],[e["raw_bytes"] for e in events]
        peak,six=peak_window(t,b,600),peak_window(t,b,21600)
        subset=[events[i] for i in six["event_indices"]]
        ratios={"simulation_time":21901/86701,"images":len(subset)/136,
                "windows":sum(e["compute_windows"] for e in subset)/7988,"bytes":six["raw_bytes"]/118.6e9}
        rows[name]={"target_count":len(ids),"daily_images":len(events),"daily_bytes":sum(b),
                    "image_multiplier":len(events)/136,"byte_multiplier":sum(b)/118.6e9,
                    "peak_600s":peak,"peak_demand_utilization":peak["raw_bytes"]/30e9,
                    "peak_21600s":six,"six_hour_windows":sum(e["compute_windows"] for e in subset),
                    "runtime_basis_wall_s":wall,"ratios":ratios,
                    "fixed_time_projection_h":wall*ratios["simulation_time"]/3600,
                    "conservative_projection_h":wall*max(ratios.values())/3600}
    eligible=[name for name,r in rows.items() if r["peak_demand_utilization"]>=.8]
    choice=min(eligible,key=lambda name:(abs(rows[name]["byte_multiplier"]-4),rows[name]["daily_bytes"])) if eligible else None
    report={"rows":rows,"recommended":choice,"planned_runs":3 if choice else 0,
            "stop_for_runtime_confirmation":bool(choice and rows[choice]["conservative_projection_h"]>4),
            "no_scheduler_run":True,"sealed_test_dates_accessed":False}
    write_json(directory/"estimate.json",report)
    print(json.dumps(report,indent=2),flush=True)


if __name__=="__main__":
    main()
