#!/usr/bin/env python3
"""Run isolated calibration processes with read-only Jetson telemetry."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", required=True, type=Path)
    parser.add_argument("--weights", required=True, type=Path)
    parser.add_argument("--workload-json", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"started_at_utc": utc_now(), "status": "running", "runs": []}
    monitor = None
    monitor_thread = None
    telemetry_file = (args.output / "tegrastats.jsonl").open("w", encoding="utf-8")
    monitor_stderr = (args.output / "tegrastats.stderr.log").open("w", encoding="utf-8")

    def collect():
        for line in monitor.stdout:
            telemetry_file.write(json.dumps({"utc": utc_now(), "monotonic_s": time.monotonic(),
                                             "raw": line.rstrip()}) + "\n")
            telemetry_file.flush()

    try:
        executable = shutil.which("tegrastats")
        if executable:
            monitor = subprocess.Popen([executable, "--interval", "1000"], stdout=subprocess.PIPE,
                                       stderr=monitor_stderr, text=True)
            monitor_thread = threading.Thread(target=collect, daemon=True)
            monitor_thread.start()
        report["tegrastats_available"] = bool(executable)
        for name in ("round1", "round2", "round3", "sustained"):
            command = [sys.executable, str(args.runner), "--weights", str(args.weights),
                       "--workload-json", str(args.workload_json), "--output", str(args.output / name),
                       "--warmup", "50"]
            command += ["--duration-sec", "900"] if name == "sustained" else ["--repeats", "64"]
            print("Starting {} at {}".format(name, utc_now()), flush=True)
            run = {"name": name, "command": command, "started_at_utc": utc_now()}
            with (args.output / (name + ".log")).open("w", encoding="utf-8") as log:
                with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      text=True) as process:
                    for line in process.stdout:
                        log.write(line)
                        log.flush()
                        print(line, end="", flush=True)
                    run["returncode"] = process.wait()
            run["finished_at_utc"] = utc_now()
            report["runs"].append(run)
            if run["returncode"]:
                raise RuntimeError("Calibration subprocess failed: " + name)
        report["status"] = "passed"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = "{}: {}".format(type(exc).__name__, exc)
    finally:
        if monitor:
            monitor.terminate()
            try:
                monitor.wait(timeout=5)
            except subprocess.TimeoutExpired:
                monitor.kill()
                monitor.wait(timeout=5)
            if monitor_thread:
                monitor_thread.join(timeout=5)
            report["tegrastats_returncode_after_stop"] = monitor.returncode
        telemetry_file.close()
        monitor_stderr.close()
        report["finished_at_utc"] = utc_now()
        (args.output / "series.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
