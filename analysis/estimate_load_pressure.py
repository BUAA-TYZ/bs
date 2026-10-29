"""只读已有开发任务与可见性缓存，估算单站接收容量及高负载需求。"""

import hashlib
import json
from pathlib import Path
import sys

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.redesign_scenario import write_json
from sim.workload import ImageTiling

ROOT = Path("results/redesign_20261021/calibration")
OUT = ROOT / "load_pressure_20261022"
PANEL = ROOT / "capacity_20261021/load_1_distributed"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def peak_window(times, amounts, width, duration=86400):
    """非负到达量的极大窗口可取某次到达为左端，日末截断另列候选。"""
    times, amounts = np.asarray(times), np.asarray(amounts)
    order = np.argsort(times, kind="stable")
    times, amounts = times[order], amounts[order]
    starts = np.unique(np.clip(np.r_[0, times, duration-width], 0, duration-width))
    sums = np.r_[0., np.cumsum(amounts)]
    left = np.searchsorted(times, starts, side="left")
    right = np.searchsorted(times, starts+width, side="left")
    index = int(np.argmax(sums[right]-sums[left]))
    return {"start_s": float(starts[index]), "end_s": float(starts[index]+width),
            "raw_bytes": float(sums[right[index]]-sums[left[index]]),
            "count": int(right[index]-left[index]),
            "event_indices": order[left[index]:right[index]].tolist()}


def main():
    output = OUT / "analytic.json"
    if output.exists():
        raise FileExistsError(output)
    protocol_path = PANEL / "protocol.json"
    protocol = json.loads(protocol_path.read_text())
    cfg = protocol["jobs"]["compute_4_local"]["config"]
    events = cfg["scheduled_task_arrivals"]
    times = [e["release_step"]*cfg["dt"] for e in events]
    work = [ImageTiling.from_dict(e["image_tiling"]) for e in events]
    raw = [w.image_size_mb*1e6 for w in work]
    cache_path = ROOT / "contacts_144.npz"
    with np.load(cache_path) as cache:
        grid = cache["times"]
        visible = cache["ground_visible"][:, 24:, 0].any(axis=1)
    lengths = np.maximum(0, np.minimum(grid[1:], 86400)-grid[:-1])
    available = float(np.sum(lengths*visible[:-1]))
    rate = cfg["ground_stations"][0]["bandwidth_mbps"]*1e6/8
    peak = peak_window(times, raw, 600)
    six = peak_window(times, raw, 21600)
    start, end = peak["start_s"], peak["end_s"]
    cap_peak = float(np.sum(np.maximum(0, np.minimum(grid[1:], end)-np.maximum(grid[:-1], start))*visible[:-1]))*rate
    counts = {}
    for w in work:
        key = str(w.width_px)
        counts[key] = counts.get(key, 0)+1
    result = {
        "input_hashes": {str(p): sha(p) for p in (protocol_path, cache_path, OUT/"PROTOCOL.md", Path(__file__))},
        "date": "2026-10-15", "duration_s": 86400, "images": len(events), "mode_size_counts": counts,
        "raw_bytes": sum(raw), "dispatch_bytes_with_overlap": sum(sum(t.data_size_mb for t in w.tiles())*1e6 for w in work),
        "inference_windows": sum(len(e["window_result_bytes"]) for e in events),
        "result_bytes": sum(sum(e["window_result_bytes"]) for e in events),
        "ground": {"receive_channels": cfg["ground_stations"][0]["receive_channels"],
                   "rate_bytes_s": rate, "helper_visibility_union_s": available,
                   "geometry_capacity_bytes": available*rate, "continuous_capacity_bytes": 86400*rate,
                   "sampling_seconds": 10, "peak_600s_geometry_capacity_bytes": cap_peak},
        "peak_600s": peak, "peak_21600s": six,
        "scaled_loads": [{"factor": f, "images": len(events)*f, "daily_raw_bytes": sum(raw)*f,
                          "images_per_source_mean": len(events)*f/24,
                          "peak_600s_raw_bytes": peak["raw_bytes"]*f,
                          "peak_demand_utilization": peak["raw_bytes"]*f/cap_peak,
                          "daily_demand_utilization": sum(raw)*f/(available*rate)} for f in (1,4,8,16)],
        "scope": "Arrival demand / geometric single-channel capacity; not realized receiving utilization or scheduling simulation",
        "sealed_test_dates_accessed": False,
    }
    write_json(output, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
