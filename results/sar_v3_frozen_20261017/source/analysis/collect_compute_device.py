"""Collect a read-only compute-device inventory using Python 3.8+ stdlib."""

import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess


def command(program, arguments=(), candidates=()):
    executable = shutil.which(program)
    if executable is None:
        executable = next((path for path in candidates
                           if Path(path).is_file() and os.access(path, os.X_OK)), None)
    if executable is None:
        return {"status": "not_found"}
    try:
        result = subprocess.run([executable, *arguments], capture_output=True,
                                text=True, errors="replace", timeout=3, check=False)
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "timeout_s": 3}
    except OSError as error:
        return {"status": "unavailable", "error_type": type(error).__name__}
    return {"status": "ok" if result.returncode == 0 else "command_failed",
            "returncode": result.returncode,
            "stdout": result.stdout.strip()[:16384],
            "stderr": result.stderr.strip()[:4096]}


def read_text(path):
    try:
        with Path(path).open(encoding="utf-8", errors="replace") as handle:
            value = handle.read(16384).strip("\x00\n ")
        return {"status": "ok", "value": value}
    except FileNotFoundError:
        return {"status": "not_found"}
    except OSError as error:
        return {"status": "unavailable", "error_type": type(error).__name__}


def linux_inventory():
    information = {
        "board_model": read_text("/proc/device-tree/model"),
        "l4t_release": read_text("/etc/nv_tegra_release"),
        "os_release": {"status": "not_found"},
        "cpu": {"status": "not_found"},
        "ram": {"status": "not_found"},
    }
    release = read_text("/etc/os-release")
    if release["status"] == "ok":
        values = {}
        for line in release["value"].splitlines():
            key, separator, value = line.partition("=")
            if separator and key in {"NAME", "ID", "VERSION", "VERSION_ID"}:
                try:
                    values[key] = " ".join(shlex.split(value))
                except ValueError:
                    values[key] = value
        information["os_release"] = {"status": "ok", "values": values}
    else:
        information["os_release"] = release
    cpu = read_text("/proc/cpuinfo")
    if cpu["status"] == "ok":
        names = set()
        for line in cpu["value"].splitlines():
            key, _, value = line.partition(":")
            if key.strip() in {"model name", "Hardware", "Processor"} and value.strip():
                names.add(value.strip())
        information["cpu"] = {"status": "ok" if names else "unavailable",
                              "model_names": sorted(names)}
    else:
        information["cpu"] = cpu
    memory = read_text("/proc/meminfo")
    if memory["status"] == "ok":
        for line in memory["value"].splitlines():
            if line.startswith("MemTotal:"):
                information["ram"] = {"status": "ok", "bytes": int(line.split()[1]) * 1024,
                                      "scope": "OS-visible total, excluding reserved memory"}
    else:
        information["ram"] = memory
    packages = ("nvidia-l4t-core", "nvidia-jetpack", "libnvinfer8", "libnvinfer10",
                "libcudnn8", "libcudnn9-cuda-12", "libcudnn9-cuda-13")
    information["debian_packages"] = {
        package: command("dpkg-query", ("-W", "-f=${Status}\t${Version}\n", package))
        for package in packages
    }
    information["power_mode"] = command("nvpmodel", ("-q",), ("/usr/sbin/nvpmodel",))
    information["clocks"] = command("jetson_clocks", ("--show",),
                                    ("/usr/bin/jetson_clocks", "/usr/sbin/jetson_clocks"))
    return information


def collect():
    system = platform.system()
    report = {
        "schema_version": 1,
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "Read-only hardware/software inventory; no performance measurements",
        "platform": {"system": system, "machine": platform.machine(),
                     "release": platform.release(), "python_version": platform.python_version(),
                     "logical_cpu_count": os.cpu_count()},
        "limitations": [
            "Missing packages or commands do not prove that a component is absent.",
            "Python package versions describe only the Python environment running this script.",
            "No serial numbers, GPU UUIDs, network addresses, hostname, or environment dump collected.",
            "Jetson nvidia-smi may be unavailable; board model and L4T are separate evidence.",
            "The CUDA version shown by a driver is not the installed CUDA toolkit version.",
        ],
    }
    if system == "Linux":
        report["linux"] = linux_inventory()
    elif system == "Darwin":
        report["macos"] = {"cpu": command("sysctl", ("-n", "machdep.cpu.brand_string")),
                           "ram_bytes": command("sysctl", ("-n", "hw.memsize"))}
    elif system == "Windows":
        report["windows"] = {
            "cpu": command("powershell", ("-NoProfile", "-NonInteractive", "-Command",
                                          "(Get-CimInstance Win32_Processor).Name")),
            "ram_bytes": command("powershell", ("-NoProfile", "-NonInteractive", "-Command",
                  "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory")),
        }
    report["nvidia_gpu"] = command("nvidia-smi", (
        "--query-gpu=name,memory.total,driver_version", "--format=csv,nounits"))
    report["cuda_toolkit"] = command("nvcc", ("--version",), ("/usr/local/cuda/bin/nvcc",))
    report["cuda_version_file"] = {"status": "not_found"}
    version_file = read_text("/usr/local/cuda/version.json")
    if version_file["status"] == "ok":
        try:
            versions = json.loads(version_file["value"])
            selected = {key: value.get("version") for key, value in versions.items()
                        if key in {"cuda", "cuda_nvcc", "cuda_cudart"} and isinstance(value, dict)}
            report["cuda_version_file"] = {"status": "ok", "versions": selected}
        except (ValueError, AttributeError):
            report["cuda_version_file"] = {"status": "invalid_json"}
    elif version_file["status"] == "not_found":
        report["cuda_version_file"] = read_text("/usr/local/cuda/version.txt")
    else:
        report["cuda_version_file"] = version_file
    versions = {}
    for package in ("torch", "torchvision", "tensorrt", "tensorrt-cu12", "tensorrt-cu13",
                    "onnx", "onnxruntime", "onnxruntime-gpu", "numpy", "ultralytics"):
        try:
            versions[package] = {"status": "ok", "version": importlib.metadata.version(package)}
        except importlib.metadata.PackageNotFoundError:
            versions[package] = {"status": "not_found_in_current_python"}
    report["python_packages"] = versions
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True,
                        help="JSON destination (must not already exist)")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output already exists; choose a new filename")
    report = collect()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    print("Inventory written to {}. No device settings were changed.".format(args.output))


if __name__ == "__main__":
    main()
