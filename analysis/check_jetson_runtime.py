"""检查当前Jetson Python环境的CUDA执行能力，不安装依赖或测量性能。"""

import sys

if sys.version_info < (3, 8):
    print("Python 3.8+ required; keep your system Python/JetPack unchanged.")
    sys.exit(2)

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import subprocess


MARKER = "JETSON_RUNTIME_RESULT="
CUDA_PROBE = '''
import numpy as np
import torch
information = {
    "torch": torch.__version__, "numpy": np.__version__,
    "torch_cuda_build": torch.version.cuda,
    "cuda_available": torch.cuda.is_available(),
    "cudnn_version": torch.backends.cudnn.version(),
    "cudnn_available": torch.backends.cudnn.is_available(),
    "cudnn_enabled": torch.backends.cudnn.enabled,
}
if not information["cuda_available"]:
    raise RuntimeError("CUDA unavailable in this Python environment: " + repr(information))
torch.set_num_threads(1)
device = torch.device("cuda:0")
properties = torch.cuda.get_device_properties(device)
information.update({"gpu_name": properties.name,
                    "compute_capability": list(torch.cuda.get_device_capability(device)),
                    "cuda_visible_memory_bytes": properties.total_memory})
checks = {}
with torch.inference_mode():
    for dtype, label in ((torch.float32, "fp32"), (torch.float16, "fp16")):
        matrix = torch.ones((64, 64), device=device, dtype=dtype)
        product = matrix @ matrix
        image = torch.ones((1, 3, 64, 64), device=device, dtype=dtype)
        kernel = torch.ones((8, 3, 3, 3), device=device, dtype=dtype)
        convolution = torch.nn.functional.conv2d(image, kernel)
        torch.cuda.synchronize(device)
        if not torch.equal(product, torch.full_like(product, 64)):
            raise RuntimeError(label + " matrix multiplication result mismatch")
        if not torch.equal(convolution, torch.full_like(convolution, 27)):
            raise RuntimeError(label + " convolution result mismatch")
        checks[label] = {"matmul": "pass", "conv2d": "pass"}
    source = np.arange(16, dtype=np.float32)
    returned = torch.from_numpy(source).to(device).cpu().numpy()
    if not np.array_equal(source, returned):
        raise RuntimeError("NumPy/CUDA roundtrip result mismatch")
    checks["numpy_cuda_roundtrip"] = "pass"
information["checks"] = checks
emit(information)
'''

TORCHVISION_PROBE = '''
import torch
import torchvision
boxes = torch.tensor([[0., 0., 10., 10.], [1., 1., 9., 9.],
                      [20., 20., 30., 30.]], device="cuda:0")
scores = torch.tensor([0.9, 0.8, 0.7], device="cuda:0")
kept = torchvision.ops.nms(boxes, scores, 0.5)
torch.cuda.synchronize()
indices = kept.cpu().tolist()
if indices != [0, 2]:
    raise RuntimeError("torchvision CUDA NMS result mismatch: " + repr(indices))
emit({"torchvision": torchvision.__version__, "cuda_nms": "pass", "indices": indices,
      "scope": "axis-aligned extension smoke check, not OBB model validation"})
'''

TENSORRT_PROBE = '''
import tensorrt
emit({"tensorrt": tensorrt.__version__, "scope": "Python import only, no engine execution"})
'''


def probe(code, timeout_s=60):
    prefix = "import json\ndef emit(value):\n    print(" + repr(MARKER) + " + json.dumps(value))\n"
    try:
        process = subprocess.run([sys.executable, "-c", prefix + code],
                                 capture_output=True, text=True, errors="replace",
                                 timeout=timeout_s, check=False)
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "timeout_s": timeout_s}
    except OSError as error:
        return {"status": "error", "error": str(error)}
    report = {"status": "pass" if process.returncode == 0 else "failed",
              "returncode": process.returncode,
              "stdout": process.stdout[-12000:], "stderr": process.stderr[-12000:]}
    for line in process.stdout.splitlines():
        if line.startswith(MARKER):
            report["details"] = json.loads(line[len(MARKER):])
    if report["status"] == "pass" and "details" not in report:
        report["status"] = "missing_result"
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="New JSON file; default: script-local results directory")
    args = parser.parse_args()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    output = args.output or Path(__file__).resolve().parent / "results" / ("jetson_runtime_" + stamp + ".json")
    if output.exists():
        parser.error("Output exists; choose a new filename")
    report = {
        "schema_version": 1, "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "Small runtime smoke checks; no model training or performance calibration",
        "python": platform.python_version(), "system": platform.system(),
        "machine": platform.machine(), "checks": {},
    }
    print("1/3 Checking PyTorch CUDA FP32/FP16 and NumPy bridge...", flush=True)
    report["checks"]["pytorch_cuda"] = probe(CUDA_PROBE)
    print("2/3 Checking torchvision CUDA extension...", flush=True)
    if report["checks"]["pytorch_cuda"]["status"] == "pass":
        report["checks"]["torchvision_cuda"] = probe(TORCHVISION_PROBE)
    else:
        report["checks"]["torchvision_cuda"] = {"status": "skipped", "reason": "PyTorch CUDA check did not pass"}
    print("3/3 Checking optional TensorRT Python import...", flush=True)
    report["checks"]["tensorrt_python"] = probe(TENSORRT_PROBE)
    required = ("pytorch_cuda", "torchvision_cuda")
    report["status"] = ("pytorch_path_ready" if all(report["checks"][key]["status"] == "pass"
                                                  for key in required) else "needs_attention")
    report["limitations"] = [
        "Passing CUDA checks does not validate the full OBB model, latency or sustained performance.",
        "TensorRT Python import is optional for the PyTorch path; system libraries are separate.",
        "Small synthetic tensors validate execution only; no images or model weights are downloaded.",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    print("Status: " + report["status"])
    print("Return this report: " + str(output.resolve()))
    return 0 if report["status"] == "pytorch_path_ready" else 1


if __name__ == "__main__":
    sys.exit(main())
