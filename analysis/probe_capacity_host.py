"""隔离的多进程主机测速；不写正式面板，不改变仿真规则。"""

import argparse
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import platform
import sys
import tarfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.run_calibration_capacity import implementation_hashes, run_job
from analysis.redesign_scenario import digest, write_json

ROOT = Path("results/redesign_20261021/calibration/capacity_20261021")
PROBE = ROOT / "host_probe"


def pack():
    protocol = json.loads((ROOT / "smoke_v2/protocol.json").read_text())
    paths = set(protocol["source_hashes"])
    paths.update(("analysis/probe_capacity_host.py", "analysis/run_calibration_capacity.py",
                  "analysis/calibrate_redesign_geometry.py", "analysis/audit_sar_coastal_coverage.py",
                  "analysis/audit_daynight_coverage.py", "analysis/benchmark_sar_aggregation.py",
                  "tests/test_calibration_capacity.py", str(ROOT / "PROTOCOL.md"),
                  str(ROOT / "smoke_v2/protocol.json")))
    PROBE.mkdir(parents=True, exist_ok=True)
    with tarfile.open(PROBE / "snapshot.tar.gz", "w:gz") as archive:
        for name in sorted(paths):
            archive.add(name, arcname=name)
    write_json(PROBE / "snapshot_hashes.json", {
        name: hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in sorted(paths)})


def run(out, workers):
    out.mkdir(parents=True, exist_ok=False)
    protocol = json.loads((ROOT / "smoke_v2/protocol.json").read_text())
    current = implementation_hashes()
    # 只重新定位绝对路径，文件内容必须与原冒烟冻结哈希相同。
    expected = {Path(k).name: v for k, v in protocol["implementation_hashes"].items()}
    assert {Path(k).name: v for k, v in current.items()} == expected
    protocol["implementation_hashes"] = current
    jobs = []
    for i in range(workers):
        key, job = list(protocol["jobs"].items())[i % 3]
        config = deepcopy(job["config"])
        config["sim_steps"] = 9000
        config["scheduled_task_arrivals"] = [x for x in config["scheduled_task_arrivals"]
                                             if x["release_step"] < 6000]
        config["task_arrival_end_step"] = 6000
        jobs.append((config, job["method"], f"probe_{i}_{job['method']}", str(out), protocol))
    write_json(out / "launch.json", {"pid": os.getpid(), "workers": workers,
        "platform": platform.platform(), "python": sys.version, "purpose": "host throughput only",
        "config_hashes": [digest(job[0]) for job in jobs],
        "implementation_hashes": current, "source_hashes": protocol["source_hashes"]})
    start = time.monotonic()
    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = [pool.submit(run_job, *job) for job in jobs]
        statuses = [future.result() for future in futures]
    write_json(out / "timing.json", {"wall_seconds": time.monotonic() - start,
                                      "workers": workers, "statuses": statuses})
    print(json.dumps({"out": str(out), "statuses": statuses}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("pack", "run"))
    parser.add_argument("--out", type=Path)
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()
    if args.mode == "pack":
        pack()
    else:
        run(args.out, args.workers)
