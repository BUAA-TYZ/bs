"""绘制已归档的昼夜机会分布，不重新传播或修改实验配置。"""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("coverage", type=Path)
    args = parser.parse_args()
    report = json.loads(args.coverage.read_text(encoding="utf-8"))
    figure, axes = plt.subplots(len(report["groups"]), 1, figsize=(12, 8), sharex=True, sharey=True)
    for axis, group in zip(np.atleast_1d(axes), report["groups"]):
        summary = group["summary"]
        rows = summary["hourly_counts"]
        optical = np.array([row["optical"] for row in rows])
        night = np.array([row["night"] for row in rows])
        sar_day = np.array([row["sar"] for row in rows]) - night
        hours = np.arange(len(rows))
        axis.bar(hours, night, color="#3b70a4", label="SAR at night")
        axis.bar(hours, sar_day, bottom=night, color="#438b70", label="SAR outside night")
        optical_count = group.get("optical_count", report["selection"].get("optical", {}).get("count", 0))
        if optical_count:
            axis.bar(hours, optical, bottom=night + sar_day, color="#d28a35", label="Optical")
        gap = summary["longest_gap_including_start_end"]["duration_s"] / 60
        architecture = f"{group['sar_count']} SAR"
        if optical_count:
            architecture += f" + {optical_count} optical"
        architecture += f" + {report['selection']['helpers']['count']} compute"
        axis.set_title(f"{architecture} | "
                       f"{summary['total_acquisitions']} opportunities | longest gap {gap:.1f} min", loc="left", fontsize=11)
        axis.set_ylabel("Opportunities")
        axis.set_axisbelow(True)
        axis.grid(axis="y", alpha=0.2)
        axis.spines[["top", "right"]].set_visible(False)
    axes[0].legend(loc="upper right", frameon=False, ncols=3)
    axes[-1].set_xticks(np.arange(24))
    axes[-1].set_xlabel("UTC hour on 2026-10-15 (each bin is one fixed clock hour)")
    scope = "US harbor reference points" if "application" in report["selection"] else "Fixed US target centers"
    figure.suptitle(f"{scope}: preliminary day/night geometry", fontsize=15)
    figure.text(0.5, 0.015, "Acquisition opportunities only. Squint, attitude, full footprint and image formation are not validated.",
                ha="center", fontsize=10)
    figure.tight_layout(rect=(0, 0.04, 1, 0.96))
    output = args.coverage.with_name("hourly_opportunities.png")
    figure.savefig(output, dpi=160)
    plt.close(figure)
    print(output)


if __name__ == "__main__":
    main()
