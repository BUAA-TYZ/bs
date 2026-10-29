"""汇总已完成的高分仿真，不训练模型或筛选策略胜出的任务。"""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.evaluate_literature import common_context, summarize
from sim.evaluation import paired_task_metrics


ROOT = Path("results")
OUT = ROOT / "highres_study_20261015"
NOMINAL = ["highres_nominal_ground_20261015", "highres_nominal_star_20261015",
           "highres_nominal_literature_20261015"]
ORDER = ["ground_compute", "ground_tiles", "ground_relay", "local", "greedy",
         "window_aware", "heft_adapted", "cgs_compute_adapted"]
LABELS = {"ground_compute": "源星整图直传+地面计算", "ground_tiles": "源星分片直传+地面计算",
          "ground_relay": "原始分片中继+地面计算", "local": "源星本地计算+结果中继",
          "greedy": "贪心协同+结果中继", "window_aware": "窗口感知协同+结果中继",
          "heft_adapted": "HEFT适配+结果中继", "cgs_compute_adapted": "接触图计算适配+结果中继"}
FIXED = False


def read_run(name):
    if FIXED:
        name = name.replace("highres_", "highres_fixed_", 1)
    directory = ROOT / name
    summary = json.loads((directory / "summary.json").read_text())
    records = [json.loads((directory / filename).read_text()) for filename in summary["records"]]
    return summary, records


def table(records, methods):
    aggregate, _ = summarize(records, methods)
    lines = ["| 方法 | 完整交付/到达 | 5分钟 | 10分钟 | 30分钟 | 60分钟 | 240分钟 | 截断平均时延(s) |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for row in aggregate:
        selected = [r for r in records if r["method"] == row["method"]]
        rates = [sum(r["task_age"]["delivery_curve"][i]["delivery_rate"] for r in selected) / len(selected)
                 for i in (0, 1, 2, 3, 5)]
        lines.append(f"| {LABELS[row['method']]} | {row['completed_tasks']}/{row['total_tasks']} | " +
                     " | ".join(f"{100 * rate:.2f}%" for rate in rates) +
                     f" | {row['restricted_mean_latency_seconds']:.2f} |")
    return lines


def main():
    global FIXED, OUT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixed", action="store_true", help="Read only runs repeated after the bounded-route correction")
    args = parser.parse_args()
    FIXED = args.fixed
    if FIXED:
        OUT = ROOT / "highres_fixed_study_20261015"
    records = []
    for name in NOMINAL:
        _, rows = read_run(name)
        records.extend(rows)
    if {r["method"] for r in records} != set(ORDER) or len(records) != len(ORDER):
        raise ValueError("Nominal panel incomplete or duplicated")
    if any(common_context(r) != common_context(records[0]) for r in records):
        raise ValueError("Nominal inputs are not paired")
    daily_summary, daily = read_run("highres_three_days_20261015")
    if len(daily) != 9:
        raise ValueError("Three-day panel is incomplete")
    for row in daily:
        if row["case"]["id"] == "day01":
            original = next(r for r in records if r["method"] == row["method"])
            contexts = [common_context(r) for r in (row, original)]
            # 窗口编号只是全扫描列表索引；增加日期会改变编号，不改变事件几何。
            for context in contexts:
                for event in context["config_except_pipeline_mode"]["scheduled_task_arrivals"]:
                    event.pop("window_id", None)
            if (contexts[0] != contexts[1] or
                    row["metrics"]["task_outcomes"] != original["metrics"]["task_outcomes"]):
                raise ValueError("Repeated first-day results changed")
    sensitivity = {}
    for variant in ("eta08", "ground10x", "isl2500", "dt1", "dt05", "dt025", "dt01"):
        _, sensitivity[variant] = read_run(f"highres_{variant}_20261015")
    if FIXED:
        _, sensitivity["dt005"] = read_run("highres_dt005_20261015")
    timestep = []
    for method in ("ground_relay", "greedy"):
        coarse = next(r for r in sensitivity["dt1"] if r["method"] == method)
        fine = next(r for r in sensitivity["dt01"] if r["method"] == method)
        timestep.append({"method": method, **paired_task_metrics(coarse["metrics"]["task_outcomes"], fine["metrics"]["task_outcomes"]),
                         "coarse_latency_seconds": coarse["task_age"]["restricted_mean_latency_seconds"],
                         "fine_latency_seconds": fine["task_age"]["restricted_mean_latency_seconds"],
                         "coarse_delivery_score": coarse["task_age"]["delivery_score"],
                         "fine_delivery_score": fine["task_age"]["delivery_score"]})
    OUT.mkdir(exist_ok=True)
    data = {"nominal": summarize(records, ORDER), "three_day": daily_summary["aggregate"],
            "sensitivity": {k: [{"method": r["method"], "task_age": r["task_age"]} for r in rows]
                            for k, rows in sensitivity.items()}, "timestep": timestep,
            "sources": NOMINAL + ["highres_three_days_20261015"] + [f"highres_{k}_20261015" for k in sensitivity]}
    if FIXED:
        data["sources"] = [name.replace("highres_", "highres_fixed_", 1) for name in data["sources"]]
    (OUT / "summary.json").write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    lines = ["# 高分遥感场景实验报告", "", "日期：2026-10-15。公开设备通信规格支持的网络层仿真，计算和图像输出仍为受控代理。没有在这组新场景训练或评估MARL，不能把规则策略成绩写成强化学习成绩。", "",
             ("已修复带跳数预算的最短路漏路错误；本报告全部策略、日期和敏感性数字都由修复后版本重跑。旧失效结果保存在[修复前诊断报告](../highres_study_20261015/REPORT.md)，不混入本表。数值检查仍只覆盖首个窗口，见第5节。" if FIXED else
              "**验收结论：当前未通过关键时间步稳定性检查。下列1秒主表仅用于开发诊断，不能作为论文中的稳定加速结论。原始中继在0.1秒步长出现路径绕行，必须先处理并重跑。**"), "",
             "## 1. 场景与证据", "",
             "2颗620 km、97.9度成像星；22颗550 km、53度计算中继星；美国五个民用区域成像，北京坐标单接收站。保持固定日期和全部合格成像事件，首日21幅，连续三天共53幅。各日独立从空队列开始，每个任务都跟踪至少4小时。", "",
             "- 星间10 Gbps，另测同系列2.5 Gbps；星地双通道合计1.2 Gbps。效率1.0为上限抽象，0.8为敏感性假设。",
             "- 每幅原生PAN/MS约6.417 GB，3:1压缩后2.139 GB；固定200像素overlap产生4617窗口、315包，分片输入合计3.328 GB。",
             "- 每包最多16窗口、11.534 MB。完整窗口容量充足不等于有空闲接入或可持续路由；每一步仍执行共享带宽、缓存、断链和计算队列。",
             "- 通信证据：[设备参数依据](../../docs/thesis/highres_link_evidence.md)；配置理由与边界：[实验协议](../../docs/thesis/highres_scene_protocol.md)；[轨道几何审计](../../docs/thesis/highres_geometry_audit.md)；[包长与窗口审计](../highres_contact_audit_20261015.md)。", "",
             "首日几何审计中，源星在观察期没有北京直传窗口，各次任务到下一直接窗口等待约8.03至11.28小时。故两个直传控制组为0不能证明协同计算优势；有意义的地面对手是允许多星中继的ground_relay。", "",
             "## 2. 首日完整基线", ""]
    lines += table(records, ORDER)
    lines += ["", "所有星上策略采用共同的结果中继；地面中继也执行相同的实际链路和竞争约束。HEFT/CGS是文献方法适配，不能写成原论文端到端复现。最终交付数相同，不代表早期任务年龄交付率和时延相同。", "",
              "## 3. 连续日期面板", ""]
    lines += table(daily, ["ground_relay", "greedy", "heft_adapted"])
    lines += ["", "上述比例和时延按日等权；完成数汇总。三个日期分别21、15、17个任务，不是53个独立轨道样本，也没有统计显著性结论。", "",
              "| 日期 | 方法 | 10分钟交付率 | 截断平均时延(s) |", "|---|---|---:|---:|"]
    for row in sorted(daily, key=lambda r: (r["case"]["id"], r["method"])):
        lines.append(f"| {row['case']['calendar_day']} | {LABELS[row['method']]} | "
                     f"{100 * row['task_age']['delivery_curve'][1]['delivery_rate']:.2f}% | "
                     f"{row['task_age']['restricted_mean_latency_seconds']:.2f} |")
    lines += ["", "![连续日期交付曲线](delivery_curves.png)", "", "## 4. 单因素敏感性", "",
              "均采用完整首日21幅；同一变体内各方法仍有一致的任务、轨道和配置。", "",
              "| 变体 | 方法 | 5分钟交付率 | 10分钟交付率 | 六时限J | 截断平均时延(s) |",
              "|---|---|---:|---:|---:|---:|"]
    names = {"eta08": "通信净荷效率0.8", "ground10x": "地面参考能力乘10", "isl2500": "仅星间降至设备2.5 Gbps档"}
    for variant, name in names.items():
        for row in sensitivity[variant]:
            age = row["task_age"]
            lines.append(f"| {name} | {LABELS[row['method']]} | {100 * age['delivery_curve'][0]['delivery_rate']:.2f}% | "
                         f"{100 * age['delivery_curve'][1]['delivery_rate']:.2f}% | {100 * age['delivery_score']:.2f}% | "
                         f"{age['restricted_mean_latency_seconds']:.2f} |")
    lines += ["", "地面能力乘10是代理算力敏感性，不是新增服务器实测。应比较各时限而不只挑一个均值；较快地面可在紧时限下优于某些协同策略。仅降低星间速率时，原始中继均值反而略降，说明当前自适应选路和离散执行的结果不保证随带宽单调变化；本轮未完成逐任务因果归因，不能解释成低带宽更优。", "",
              "## 5. 时间步检查", "",
              "固定首个成像窗口3幅图，保持星上策略决策每1 s、几何每10 s，比较执行dt=1 s和0.1 s。原始中继当前每个执行步重新选路，其控制频率也随dt变化。这个小面板不代表全日已收敛。", "",
              "| 方法 | dt=1s平均时延(s) | dt=0.1s平均时延(s) | 两步长共同完成 |", "|---|---:|---:|---:|"]
    for row in timestep:
        lines.append(f"| {LABELS[row['method']]} | {row['coarse_latency_seconds']:.2f} | "
                     f"{row['fine_latency_seconds']:.2f} | {row['common_completed_tasks']}/{row['total_tasks']} |")
    lines += ["", "原始中继补充步长结果：", "", "| dt(s) | 平均时延(s) | J |", "|---:|---:|---:|"]
    for variant, dt in (("dt1", 1), ("dt05", .5), ("dt025", .25), ("dt01", .1)):
        row = next(r for r in sensitivity[variant] if r["method"] == "ground_relay")
        lines.append(f"| {dt} | {row['task_age']['restricted_mean_latency_seconds']:.2f} | {100 * row['task_age']['delivery_score']:.2f}% |")
    if FIXED:
        row = sensitivity["dt005"][0]
        lines.append(f"| 0.05 | {row['task_age']['restricted_mean_latency_seconds']:.2f} | {100 * row['task_age']['delivery_score']:.2f}% |")
    lines += ["", ("修复前0.1秒原始中继曾发生大范围路径改变，而接收总字节不变；这里使用的是约束最短路修复后的重跑数据。不能把一个首窗口的小面板外推为全场景收敛。" if FIXED else
                    "这不是可以忽略的毫秒舍入：0.1秒原始中继发生了大范围路径改变，而接收总字节不变。不能仅凭1、0.5、0.25秒接近就宣布收敛，也不能把0.1秒下更大的协同优势用作宣传。"), "",
              "## 6. 能支持与不能支持的结论", "",
              "真实设备规格量级的高速链路并不自动消除协同空间。该场景中，多星中继首先解决源星接收窗口的缺失，计算放置再影响并行度、地面队列和结果回传时机。不能只与无中继直传组比较。", "",
              "本轮还不能支持：MARL优于启发式；实际星载GPU上有相同比例加速；完整终端能在所有几何边上持续满速；图像压缩后精度不变；全年或所有星座都获益。", "",
              "后续优先补齐：", "",
              ("1. 扩大时间步、几何分辨率与路由控制周期的独立敏感性面板；保持约束最短路回归测试。" if FIXED else
               "1. 修复或重新定义原始中继的控制时序和路径重选，完成时间步稳定性验证；当前主表暂不用于方法优越性论证。"),
              "2. 原生PAN/MS到模型输入的预处理、实际边缘硬件与地面服务器计时，以及高分条带的结果体积和全局合并开销。",
              "3. 原始影像无重复分块中继、地面重组后再生成overlap的更强地面对照；当前ground_relay仍传输重叠像元。",
              "4. 激光捕获/跟踪限制、几何时间分辨率、跨轨失联与更广日期/相位面板；不能仅靠固定效率0.8代替物理验证。",
              "5. 在该协议中重新构建多场景训练分布，统一MARL和规则策略的可决策吞吐，使用已测强教师并保留独立轨道日期测试。", "",
              "## 7. 产物与复现", "",
              "所有输入、逐任务输出和源码哈希保存在下列目录；没有把较差结果从面板移除。", ""]
    lines += [f"- [{name}](../{name}/summary.md)" for name in data["sources"]]
    lines += ["", "执行入口：`analysis/evaluate_highres_scene.py`；敏感性配置生成：`analysis/prepare_highres_sensitivity.py`；本汇总：`analysis/report_highres_scene.py`。" +
              ("修复后全套271项测试通过。" if FIXED else "修复前核心测试256项通过，但未覆盖此次发现的约束路径问题。"), ""]
    (OUT / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(7.2, 4.2), constrained_layout=True)
    for method, label, color in (("ground_relay", "Raw relay + ground compute", "#b45309"),
                                  ("greedy", "Greedy + result relay", "#047857"),
                                  ("heft_adapted", "HEFT adapted + result relay", "#7c3aed")):
        selected = [r for r in daily if r["method"] == method]
        horizons = [row["horizon_seconds"] / 60 for row in selected[0]["task_age"]["delivery_curve"]]
        rates = [100 * sum(r["task_age"]["delivery_curve"][i]["delivery_rate"] for r in selected) / len(selected)
                 for i in range(len(horizons))]
        ax.plot(horizons, rates, marker="o", label=label, color=color, linewidth=1.8)
    ax.set(xscale="log", xlabel="Task age (minutes)", ylabel="Complete tasks delivered (%)",
           ylim=(-2, 103), title=("Three dates, 53 acquisitions; bounded-route correction" if FIXED else
                                 "Three dates, 53 acquisitions; dt=1 s (not converged)"))
    ax.set_xticks(horizons, [str(int(h)) for h in horizons])
    ax.grid(alpha=.2)
    ax.legend(fontsize=8, loc="lower right")
    fig.savefig(OUT / "delivery_curves.png", dpi=180)
    plt.close(fig)
    print(json.dumps({"report": str(OUT / "REPORT.md"), "three_day": data["three_day"], "timestep": timestep}, indent=2))


if __name__ == "__main__":
    main()
