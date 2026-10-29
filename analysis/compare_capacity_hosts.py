"""只读核对短片段跨主机结果；主机墙钟耗时不作为仿真指标。"""

import argparse
import json
import math
from pathlib import Path
import sys

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.redesign_scenario import write_json
from analysis.distribute_capacity import portable


def compare_value(a, b, path="root"):
    if isinstance(a, dict):
        assert isinstance(b, dict) and a.keys() == b.keys(), path
        for key in a:
            compare_value(a[key], b[key], f"{path}.{key}")
    elif isinstance(a, list):
        assert isinstance(b, list) and len(a) == len(b), path
        for i, (left, right) in enumerate(zip(a, b)):
            compare_value(left, right, f"{path}[{i}]")
    elif isinstance(a, float) or isinstance(b, float):
        assert a is not None and b is not None and math.isclose(a, b, abs_tol=1e-6, rel_tol=1e-12), (path, a, b)
    else:
        assert a == b, (path, a, b)


def compare(reference, candidate):
    left_launch = json.loads((reference / "launch.json").read_text())
    right_launch = json.loads((candidate / "launch.json").read_text())
    assert left_launch["source_hashes"] == right_launch["source_hashes"]
    assert portable(left_launch["implementation_hashes"]) == portable(right_launch["implementation_hashes"])
    timings = []
    for i, method in enumerate(("local", "ground", "fifo")):
        assert left_launch["config_hashes"][i] == right_launch["config_hashes"][i]
        key = f"probe_{i}_{method}"
        a = json.loads((reference / f"{key}.json").read_text())
        b = json.loads((candidate / f"{key}.json").read_text())
        assert a["status"] == b["status"] == "PASS"
        for field in ("config_sha256", "total_tasks", "on_time_tasks", "on_time_task_ids", "P_D",
                      "R_D_seconds", "competition", "audit", "outcomes", "assignments", "control"):
            compare_value(a[field], b[field], field)
        with np.load(reference / f"{key}_busy_1s.npz") as x, np.load(candidate / f"{key}_busy_1s.npz") as y:
            assert np.array_equal(x["helper_ids"], y["helper_ids"])
            assert np.allclose(x["busy"], y["busy"], atol=1e-6, rtol=0)
        timings.append({"method": method, "reference_wall_s": a["wall_seconds"], "candidate_wall_s": b["wall_seconds"]})
    result = {"status": "PASS", "reference": str(reference), "candidate": str(candidate),
              "scope": "900 simulation seconds, 3 released images, all three diagnostic rules",
              "absolute_tolerance": 1e-6, "timings": timings,
              "limitation": "Short-prefix equivalence is not proof of all-day equivalence."}
    write_json(candidate / "CROSS_HOST_AUDIT.json", result)
    print(json.dumps(result))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("reference", type=Path)
    p.add_argument("candidate", type=Path)
    args = p.parse_args()
    compare(args.reference, args.candidate)
