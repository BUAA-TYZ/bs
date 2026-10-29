"""独立的容量诊断多机编排；保留原始远端记录与路径归一化证据。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import multiprocessing
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.run_calibration_capacity import check_frozen, implementation_hashes, run_job, summarize
from analysis.redesign_scenario import digest, write_json

ROOT = Path("results/redesign_20261021/calibration/capacity_20261021")
OUT = ROOT / "load_1_distributed"
REMOTE = "/data00/home/tianyuzhe/bs_capacity_20261021"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def portable(hashes):
    result = {}
    for name, value in hashes.items():
        key = "analysis/" + name.split("/analysis/", 1)[1] if "/analysis/" in name else name
        if key in result:
            raise ValueError(f"Duplicate normalized path: {key}")
        result[key] = value
    return result


def validate_assignment(jobs, assignments, reused):
    keys = [key for group in assignments.values() for key in group]
    assert len(keys) == len(set(keys)), "Duplicate job ownership"
    assert not set(keys) & set(reused), "Completed job must not be rerun"
    assert set(keys) | set(reused) == set(jobs), "Missing or unexpected job"


def prepare(assignment_file):
    original = ROOT / "load_1"
    protocol = json.loads((original / "protocol.json").read_text())
    check_frozen(protocol)
    assignments = json.loads(Path(assignment_file).read_text())
    reused = {}
    for key, job in protocol["jobs"].items():
        if (original / f"{key}_FAILED.json").exists():
            raise RuntimeError(f"No retry allowed: {key}")
        path = original / f"{key}.json"
        if path.exists():
            record = json.loads(path.read_text())
            assert record["status"] == "PASS" and record["audit"]["passed"]
            assert record["config_sha256"] == job["config_sha256"]
            reused[key] = {"record": sha(path), "busy": sha(original / f"{key}_busy_1s.npz")}
    validate_assignment(protocol["jobs"], assignments, reused)
    OUT.mkdir(exist_ok=False)
    shutil.copy2(original / "protocol.json", OUT / "protocol.json")
    for key in reused:
        for suffix in (".json", "_busy_1s.npz"):
            shutil.copy2(original / f"{key}{suffix}", OUT / f"{key}{suffix}")
    manifest = {"assignments": assignments, "reused": reused,
                "protocol_sha256": sha(OUT / "protocol.json"), "entry_sha256": sha(__file__),
                "created_utc": datetime.now(timezone.utc).isoformat(),
                "no_retries": True, "original_run": str(original)}
    write_json(OUT / "manifest.json", manifest)
    paths = set(protocol["source_hashes"])
    paths.update(str(Path(p).relative_to(Path.cwd())) if Path(p).is_absolute() else p
                 for p in protocol["implementation_hashes"])
    paths.update((str(OUT / "protocol.json"), str(OUT / "manifest.json"),
                  "analysis/distribute_capacity.py", "analysis/probe_capacity_host.py",
                  "analysis/audit_calibration_capacity.py", "analysis/audit_sar_coastal_coverage.py",
                  "analysis/audit_daynight_coverage.py", "analysis/benchmark_sar_aggregation.py",
                  str(ROOT / "smoke_v2/protocol.json"),
                  "results/redesign_20261021/calibration/bounds_144.json"))
    hashes = {name: sha(name) for name in sorted(paths)}
    write_json(OUT / "package_hashes.json", hashes)
    with tarfile.open(OUT / "snapshot.tar.gz", "w:gz") as archive:
        for name in sorted(paths | {str(OUT / "package_hashes.json")}):
            archive.add(name, arcname=name)


def verify_package():
    for name, value in json.loads((OUT / "package_hashes.json").read_text()).items():
        assert sha(name) == value, f"Package changed: {name}"


def group(host):
    directory = OUT / "hosts" / host
    try:
        verify_package()
        protocol = json.loads((OUT / "protocol.json").read_text())
        manifest = json.loads((OUT / "manifest.json").read_text())
        current = implementation_hashes()
        assert portable(current) == portable(protocol["implementation_hashes"])
        runtime = deepcopy(protocol)
        runtime["implementation_hashes"] = current
        check_frozen(runtime)
        write_json(directory / "runtime.json", {"python": sys.version, "platform": platform.platform(),
            "versions": {name: importlib.metadata.version(name) for name in
                         ("numpy", "scipy", "skyfield", "sgp4", "shapely", "tqdm", "jplephem")},
            "implementation_hashes": current, "protocol_sha256": sha(OUT / "protocol.json"),
            "entry_sha256": sha(__file__)})
        keys = manifest["assignments"][host]
        statuses = []
        with ProcessPoolExecutor(max_workers=len(keys), mp_context=multiprocessing.get_context("spawn")) as pool:
            futures = {pool.submit(run_job, protocol["jobs"][key]["config"],
                       protocol["jobs"][key]["method"], key, str(directory), runtime): key for key in keys}
            for future in as_completed(futures):
                key = futures[future]
                try:
                    result = future.result()
                except Exception:
                    result = {"key": key, "status": "FAILED", "traceback": traceback.format_exc(),
                              "config": protocol["jobs"][key]["config"]}
                    write_json(directory / f"{key}_FAILED.json", result)
                statuses.append(result)
                write_json(directory / "progress.json", {"done": len(statuses), "total": len(keys),
                                                           "statuses": statuses})
        verify_package()
        write_json(directory / "final_status.json", {"status": "COMPLETE", "statuses": statuses,
                                                      "all_pass": all(r["status"] == "PASS" for r in statuses)})
    except Exception:
        write_json(directory / "final_status.json", {"status": "BLOCKED", "traceback": traceback.format_exc()})
        raise


def launch(host):
    verify_package()
    directory = OUT / "hosts" / host
    directory.mkdir(parents=True, exist_ok=False)
    with (directory / "group.log").open("xb") as log:
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "group", "--host", host],
                                   stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                   start_new_session=True,
                                   env={**os.environ, "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1"})
    launch_info = {"pid": process.pid, "host": host, "directory": str(Path.cwd()),
                   "created_utc": datetime.now(timezone.utc).isoformat()}
    if sys.platform == "darwin":
        guard = subprocess.Popen(["caffeinate", "-i", "-w", str(process.pid)],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        launch_info["caffeinate_pid"] = guard.pid
    write_json(directory / "launch.json", launch_info)
    print(json.dumps(launch_info), flush=True)


def collect():
    verify_package()
    protocol = json.loads((OUT / "protocol.json").read_text())
    manifest = json.loads((OUT / "manifest.json").read_text())
    statuses, evidence = [], {}
    for host, keys in manifest["assignments"].items():
        directory = OUT / "hosts" / host
        final = json.loads((directory / "final_status.json").read_text())
        assert final["status"] == "COMPLETE", f"Host blocked: {host}"
        runtime = json.loads((directory / "runtime.json").read_text())
        assert runtime["protocol_sha256"] == manifest["protocol_sha256"]
        assert runtime["entry_sha256"] == manifest["entry_sha256"]
        assert portable(runtime["implementation_hashes"]) == portable(protocol["implementation_hashes"])
        for key in keys:
            failed = directory / f"{key}_FAILED.json"
            if failed.exists():
                shutil.copy2(failed, OUT / failed.name)
                statuses.append({"key": key, "status": "FAILED"})
                continue
            raw = directory / f"{key}.json"
            record = json.loads(raw.read_text())
            assert record["key"] == key and record["source_hashes"] == protocol["source_hashes"]
            assert record["implementation_hashes"] == runtime["implementation_hashes"]
            assert record["config_sha256"] == protocol["jobs"][key]["config_sha256"]
            record["execution_provenance"] = {"host": host, "raw_record": str(raw),
                                              "raw_sha256": sha(raw), "runtime": str(directory / "runtime.json")}
            # 原始记录保留，仅归一化源码路径键，供原审计器复算；不改任何结果数值。
            record["implementation_hashes"] = protocol["implementation_hashes"]
            write_json(OUT / raw.name, record)
            busy = directory / f"{key}_busy_1s.npz"
            shutil.copy2(busy, OUT / busy.name)
            evidence[key] = {"host": host, "raw": sha(raw), "normalized": sha(OUT / raw.name), "busy": sha(busy)}
            statuses.append({"key": key, "status": record["status"]})
    for key, hashes in manifest["reused"].items():
        assert sha(OUT / f"{key}.json") == hashes["record"]
        assert sha(OUT / f"{key}_busy_1s.npz") == hashes["busy"]
    write_json(OUT / "COLLECTION_AUDIT.json", {"status": "PASS", "records": evidence,
                                               "reused": manifest["reused"], "statuses": statuses})
    summarize(OUT, protocol)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "launch", "group", "collect"))
    parser.add_argument("--host")
    parser.add_argument("--assignments", type=Path)
    args = parser.parse_args()
    if args.mode == "prepare":
        prepare(args.assignments)
    elif args.mode == "launch":
        launch(args.host)
    elif args.mode == "group":
        group(args.host)
    else:
        collect()
