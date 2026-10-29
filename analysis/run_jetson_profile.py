"""Bounded Nsight/tegrastats diagnostic runs; does not modify device settings."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import threading
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--weights", type=Path, required=True)
    parser.add_argument("--workload-json", type=Path, required=True)
    parser.add_argument("--modes", nargs="+", choices=("batch1", "batch2", "batch4", "streams2"),
                        default=["batch1", "batch2", "batch4", "streams2"])
    parser.add_argument("--windows", type=int, default=16)
    parser.add_argument("--telemetry-only", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {"purpose": "Diagnostic, not replacement service calibration", "runs": []}
    for mode in args.modes:
        print("Starting " + mode, flush=True)
        telemetry = subprocess.Popen(["tegrastats", "--interval", "100"],
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        def collect():
            with (args.output / (mode + "_telemetry.jsonl")).open("w") as output:
                for line in telemetry.stdout:
                    row = {"received_epoch_s": time.time(), "raw": line.strip()}
                    try:
                        row["gpu_frequency_hz"] = int(Path("/sys/class/devfreq/17000000.gpu/cur_freq").read_text())
                    except (OSError, ValueError):
                        row["gpu_frequency_hz"] = None
                    output.write(json.dumps(row) + "\n")

        thread = threading.Thread(target=collect, daemon=True)
        thread.start()
        target = [sys.executable, str(Path(__file__).with_name("benchmark_jetson_concurrency.py")),
                  "--mode", mode, "--warmup", "12", "--windows", str(args.windows),
                  "--weights", str(args.weights), "--workload-json", str(args.workload_json),
                  "--output", str(args.output / (mode + "_service"))]
        command = target if args.telemetry_only else ["nsys", "profile", "--trace=cuda,nvtx", "--sample=none", "--cpuctxsw=none",
                   "--capture-range=cudaProfilerApi", "--capture-range-end=stop", "--export=sqlite",
                   "--output", str(args.output / mode), *target, "--profile"]
        try:
            with (args.output / (mode + ".log")).open("w") as log:
                result = subprocess.run(["timeout", "180", *command], stdout=log, stderr=subprocess.STDOUT)
            manifest["runs"].append({"mode": mode, "returncode": result.returncode, "command": command})
        finally:
            telemetry.terminate()
            telemetry.wait(timeout=10)
            thread.join(timeout=10)
            telemetry.stdout.close()
        (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps(manifest["runs"][-1]), flush=True)
        if result.returncode:
            return result.returncode
    return 0


if __name__ == "__main__":
    sys.exit(main())
