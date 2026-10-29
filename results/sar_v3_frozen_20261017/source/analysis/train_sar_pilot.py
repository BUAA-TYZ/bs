"""固定一次小样本 SAR 船型检测预实验，不读取封存测试集。"""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workload_benchmark.sar_pilot import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default="data/workload_assets/sar_pilot_20261015/dataset.yaml")
    parser.add_argument("--output", default="results/sar_pilot_20261015")
    parser.add_argument("--project", default="checkpoints/sar_pilot_20261015")
    parser.add_argument("--weights", default="data/workload_assets/yolo11n-obb.pt")
    parser.add_argument("--epochs", type=int, choices=(10, 40), default=10)
    parser.add_argument("--reference-checkpoint", help="Optional prior checkpoint for training-set diagnostics only")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
