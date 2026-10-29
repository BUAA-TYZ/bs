"""Render the fixed SAR development pilot; never substitute best-epoch metrics."""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def percent(value):
    return "N/A" if value is None else f"{100 * value:.2f}%"


def report(root):
    summary = json.loads((root / "summary.json").read_text())
    data = json.loads((root / "data_summary.json").read_text())
    before, after = summary["before"], summary["after"]
    rows = after["per_class"]
    destination = root / "REPORT.md"
    if destination.exists():
        raise FileExistsError("A pilot report already exists; preserve the recorded experiment")

    fig, (quality, timing) = plt.subplots(1, 2, figsize=(13, 8),
                                        gridspec_kw={"width_ratios": [2.3, 1]}, layout="constrained")
    positions = np.arange(len(rows))
    initial = [0 if row["ap50"] is None else row["ap50"] * 100 for row in before["per_class"]]
    final = [0 if row["ap50"] is None else row["ap50"] * 100 for row in rows]
    quality.barh(positions - .18, initial, height=.34, color="#999999", label="Initialized head")
    quality.barh(positions + .18, final, height=.34, color="#008c7a", label="Last epoch")
    quality.set_yticks(positions, [f'{row["name"]} (n={row["ground_truth_instances"]})'
                                  for row in rows])
    quality.invert_yaxis()
    quality.set_xlim(0, 100)
    quality.set_xlabel("Development AP50 (%) using OBB probIoU")
    quality.set_title("Per-class results; n is validation object count", fontsize=12)
    quality.legend(loc="lower right")
    quality.grid(axis="x", alpha=.2)
    quality.set_axisbelow(True)
    for index, row in enumerate(rows):
        if row["ap50"] is None:
            quality.text(1, index, "Not evaluated", va="center", fontsize=9)

    service = summary["service_timing"]["devices"]
    devices = list(service)
    x = np.arange(len(devices))
    timing.bar(x - .18, [service[d]["seconds"]["p50"] * 1000 for d in devices],
               width=.34, color="#008c7a", label="P50")
    timing.bar(x + .18, [service[d]["seconds"]["p95"] * 1000 for d in devices],
               width=.34, color="#cf6748", label="P95")
    timing.set_xticks(x, [d.upper() for d in devices])
    timing.set_ylabel("Milliseconds / 800 x 800 patch")
    timing.set_title("Local service timing, batch 1", fontsize=12)
    timing.legend()
    timing.grid(axis="y", alpha=.2)
    timing.set_axisbelow(True)
    fig.suptitle("SAR development pilot: 128 training / 64 validation patches", fontsize=15)
    fig.savefig(root / "pilot_results.png", dpi=150)
    plt.close(fig)

    lines = ["# SAR细船型识别开发试跑", "", f"运行状态：{summary['status']}；"
             f"完成{summary['epochs_completed']}/{summary['epochs_requested']}轮；"
             f"训练耗时{summary['training_seconds'] / 60:.2f}分钟。", "",
             "## 固定比较", "", "| 开发验证指标 | 正确初始化epoch0 | 固定最后epoch |",
             "| --- | ---: | ---: |",
             f"| AP50（有真值类均值） | {percent(before['mAP50_supported_classes'])} | "
             f"{percent(after['mAP50_supported_classes'])} |",
             f"| AP50:95（有真值类均值） | {percent(before['mAP50_95_supported_classes'])} | "
             f"{percent(after['mAP50_95_supported_classes'])} |", "",
             f"验证支持{after['supported_class_count']}/{after['total_class_count']}类。"
             "这是Ultralytics OBB probIoU指标，不能直接与原论文polygon IoU AP比较，"
             "也不是军民二分类正确率。单次小样本试跑不构成可靠性或显著性证据。", "",
             "| 原生类别 | 训练目标 | 验证目标 | 最后epoch AP50 | AP50:95 |",
             "| --- | ---: | ---: | ---: | ---: |"]
    training_counts = data["summary"]["class_counts"]["train"]
    for row in rows:
        lines.append(f"| {row['name']} | {training_counts.get(row['name'], 0)} | "
                     f"{row['ground_truth_instances']} | {percent(row['ap50'])} | "
                     f"{percent(row['ap50_95'])} |")
    lines += ["", "## 本机服务耗时", "", "| 设备 | 平均 | P50 | P95 | 平均输出字节 |",
              "| --- | ---: | ---: | ---: | ---: |"]
    for device, values in service.items():
        times = values["seconds"]
        lines.append(f"| {device} | {times['mean'] * 1000:.2f} ms | "
                     f"{times['p50'] * 1000:.2f} ms | {times['p95'] * 1000:.2f} ms | "
                     f"{values['serialized_bytes']['mean']:.1f} |")
    lines += ["", "输入为已解码800×800图像、网络尺寸832；包含预测、CPU坐标和OBB1序列化，"
              "不含整图解码、滑窗提取、跨窗合并和传输。低质量模型可能漏检并产生偏小输出；"
              "这些字节数不能直接冻结为正式仿真负载。CPU/MPS是同一本机的代理，"
              "不是星上与地面服务器硬件标定。", "", "## 数据与边界", "",
              f"- 数据下载：{data['network_bytes_this_run'] / 1024 ** 2:.1f} MiB；"
              "没有下载完整ZIP。",
              "- 来源使用明确标记为临时分组的文件名前缀，训练与开发验证没有前缀交集；"
              "文件/像素哈希和局部相关筛查通过，不等于已证明真实原图或船舶身份独立。",
              "- 首轮整图排除含越界、退化或不支持难度标注的样本，结果不能推广到边缘目标。",
              "- 无最终识别测试集，未运行调度封存日期；没有新P_300或调度算法优胜结论。",
              "- 光学预训练只用作初始化，分类末层已重置并核对训练入口哈希。",
              f"- 检查点：`{summary['checkpoint']['path']}`；"
              f"SHA256 `{summary['checkpoint']['sha256']}`。", "",
              "![开发结果与本机耗时](pilot_results.png)", ""]
    destination.write_text("\n".join(lines), encoding="utf-8")
    print(destination)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("results/sar_pilot_20261015"))
    report(parser.parse_args().output)


if __name__ == "__main__":
    main()
