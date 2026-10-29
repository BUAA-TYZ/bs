"""复核解析量、扩展机会和未改动的公共源码；不运行环境。"""

from collections import Counter
import json
import math
from pathlib import Path
import sys

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.audit_sar_coastal_coverage import COASTAL_TARGETS
from analysis.estimate_load_pressure import OUT, PANEL, sha
from analysis.prepare_load_pressure import arrivals, distance_km
from analysis.redesign_scenario import write_json
from analysis.run_calibration_capacity import check_frozen


def independent_peak(events, width):
    endpoints = sorted({float(width), 86400., *[min(86400., max(float(width), e["ready_s"]+.1)) for e in events]})
    return max(math.fsum(e["raw_bytes"] for e in events if end-width <= e["ready_s"] < end) for end in endpoints)


def main():
    protocol = json.loads((PANEL/"protocol.json").read_text())
    check_frozen(protocol)
    analytic = json.loads((OUT/"analytic.json").read_text())
    for path, value in analytic["input_hashes"].items():
        assert sha(Path(path)) == value
    assert analytic["raw_bytes"] == 32*200e6+75*800e6+29*1800e6
    assert analytic["ground"]["geometry_capacity_bytes"] == 86400*400e6/8
    assert analytic["peak_600s"]["raw_bytes"] == 8.8e9
    ext = OUT/"maritime_extension"
    reg = json.loads((ext/"registration.json").read_text())
    for path, value in reg["input_hashes"].items():
        assert sha(Path(path)) == value
    points = reg["targets"]
    assert min(distance_km(a,b) for i,a in enumerate(points) for b in points[i+1:]) >= 20
    windows = json.loads((ext/"windows.json").read_text())
    old = protocol["jobs"]["compute_4_local"]["config"]["scheduled_task_arrivals"]
    recovered = arrivals(windows, {r["target_id"] for r in COASTAL_TARGETS})
    assert [(e["ready_s"],e["source_sat_id"],int(math.sqrt(e["raw_bytes"]/2))) for e in recovered] == [
        (e["release_step"]*.1,e["source_sat_id"],e["image_tiling"]["width_px"]) for e in old]
    estimate = json.loads((ext/"estimate.json").read_text())
    verification = {}
    for name in reg["sets"]:
        events = json.loads((ext/f"{name}_arrivals.json").read_text())
        row = estimate["rows"][name]
        assert len(events) == row["daily_images"]
        assert math.fsum(e["raw_bytes"] for e in events) == row["daily_bytes"]
        counts = Counter(e["source_sat_id"] for e in events)
        for sid in counts:
            times = sorted(e["capture_start_s"] for e in events if e["source_sat_id"] == sid)
            assert all(b-a>=60 for a,b in zip(times,times[1:]))
        for e in events:
            w = windows[e["window_id"]]
            assert e["source_sat_id"] == w["source_sat_id"] and e["target_id"] == w["target_id"]
            assert e["capture_start_s"] >= w["start_s"] and e["capture_end_s"] <= w["end_s"]-10
            assert e["ready_s"] == e["capture_end_s"]+60
        for width,key in ((600,"peak_600s"),(21600,"peak_21600s")):
            assert independent_peak(events,width) == row[key]["raw_bytes"]
            start,end = row[key]["start_s"],row[key]["end_s"]
            selected = [i for i,e in enumerate(events) if start<=e["ready_s"]<end]
            assert selected == row[key]["event_indices"]
        verification[name] = {"images":len(events),"per_source_min":min(counts.values()),"per_source_max":max(counts.values()),
                              "spacing_and_capture_bounds_pass":True,"independent_600s_and_6h_peaks_pass":True}
    files = [p for p in OUT.rglob("*") if p.is_file() and p.name!="AUDIT.json"]
    write_json(OUT/"AUDIT.json", {"status":"PASS","source_files_verified":len(protocol["source_hashes"]),
               "original_releases_sources_modes_reproduced":len(old),"verification":verification,
               "files":{str(p):sha(p) for p in sorted(files)},"auditor_sha256":sha(Path(__file__)),
               "no_scheduler_run":True,"sealed_test_dates_accessed":False,
               "known_limits":["Geometry is sampled; no slew/thermal/energy duty-cycle validation",
                               "Demand utilization is not realized downlink utilization",
                               "Runtime is a scaling budget, not a high-load benchmark"]})
    print(json.dumps({"status":"PASS","frozen_sources":len(protocol["source_hashes"]),"verification":verification}))


if __name__=="__main__":
    main()
