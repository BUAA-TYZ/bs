"""运行小规模真实遥感负载 CPU/MPS 代理测量，禁止覆盖已有结果。"""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workload_benchmark.runner import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assets", default="data/workload_assets")
    parser.add_argument("--output", default="results/image_workload_calibration_20261014")
    parser.add_argument("--devices", nargs="+", choices=("cpu", "mps"), default=["cpu", "mps"])
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--include-mirror", action="store_true")
    args = parser.parse_args()
    if min(args.warmup, args.repeats, args.threads) < 1:
        parser.error("warmup/repeats/threads must be positive")
    run(args)


if __name__ == "__main__":
    main()
