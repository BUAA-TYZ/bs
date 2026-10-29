"""只读正式180份结果，生成配对统计与论文图；绝不调用仿真。"""

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import itertools
import json
import math
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PROJECT = Path(__file__).resolve().parents[1]
SOURCE = PROJECT / "results/sar_v3_frozen_20261017"
OUT = PROJECT / "results/sar_formal_statistics_20261019"
CONDITIONS = ["base", "compute_4", "compute_10", "load_2", "load_4"]
METHODS = ["local", "ground", "cgs", "heft", "two_plan", "three_plan"]
LABELS = {"local": "Local + relay", "ground": "Strong ground", "cgs": "CGS", "heft": "HEFT",
          "two_plan": "Two-plan", "three_plan": "Three-plan (v3)"}
COND_LABELS = ["Base", "Compute x4", "Compute x10", "Load x2", "Load x4"]
PLANS = ["local", "cgs", "ground"]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


def write(path, obj):
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def exact_sign_test(values, tolerance=0):
    positive = sum(x > tolerance for x in values)
    negative = sum(x < -tolerance for x in values)
    n = positive + negative
    p = min(1.0, 2 * sum(math.comb(n, k) for k in range(min(positive, negative) + 1)) / 2**n) if n else 1.0
    return {"positive": positive, "zero": len(values) - n, "negative": negative,
            "nonzero_days": n, "p_two_sided": p}


def bootstrap(values):
    values = np.asarray(values, dtype=float)
    indices = np.asarray(list(itertools.product(range(len(values)), repeat=len(values))))
    means = values[indices].mean(axis=1)
    return np.quantile(means, [.025, .975], method="linear").tolist()


def holm(pvalues):
    order = sorted(range(len(pvalues)), key=pvalues.__getitem__)
    adjusted = [0.] * len(pvalues)
    previous = 0.
    for rank, i in enumerate(order):
        previous = max(previous, min(1., (len(order) - rank) * pvalues[i]))
        adjusted[i] = previous
    return adjusted


def load_inputs():
    summary = read(SOURCE / "summary.json")
    manifest = read(SOURCE / "manifest.json")
    freeze = read(SOURCE / "freeze.json")
    assert len(manifest["jobs"]) == 180 and not summary["issues"]
    for name, expected in freeze["files"].items():
        assert sha(SOURCE / "source" / name) == expected, name
    records, inputs = {}, {}
    for job in manifest["jobs"]:
        path = Path(job["record"])
        hashed = sha(path)
        assert hashed == summary["record_sha256"][job["key"]], job["key"]
        assert sha(Path(job["config"])) == job["config_sha256"]
        r = read(path)
        outcomes = r["metrics"]["task_outcomes"]
        ids = [o["task_id"] for o in outcomes if o["latency_seconds"] is not None and o["latency_seconds"] <= 300]
        assert set(ids) == set(r["on_time_task_ids"]) and len(ids) == r["on_time_tasks"]
        assert len(outcomes) == r["total_tasks"] and r["P300"] == len(ids) / len(outcomes)
        restricted = math.fsum(min(300., o["latency_seconds"]) if o["latency_seconds"] is not None else 300.
                               for o in outcomes) / len(outcomes)
        assert abs(restricted - r["R300_seconds"]) <= 1e-6
        records[(job["condition"], job["date"], job["method"])] = r
        inputs[job["key"]] = hashed
    return manifest["dates"], records, inputs, len(freeze["files"])


def analyze(dates, records):
    main, pairs, statistics, choices, transitions = [], [], [], [], []
    for condition in CONDITIONS:
        for method in METHODS:
            group = [records[(condition, date, method)] for date in dates]
            main.append({"condition": condition, "method": method,
                         "P300_percent": math.fsum(r["P300"] * 100 for r in group) / len(dates),
                         "R300_seconds": math.fsum(r["R300_seconds"] for r in group) / len(dates)})
        for baseline in METHODS[:-1]:
            daily = []
            for date in dates:
                v3, other = records[(condition, date, "three_plan")], records[(condition, date, baseline)]
                assert v3["total_tasks"] == other["total_tasks"]
                assert [(o["task_id"], o["source_sat_id"], o["release_seconds"]) for o in v3["metrics"]["task_outcomes"]] == [
                    (o["task_id"], o["source_sat_id"], o["release_seconds"]) for o in other["metrics"]["task_outcomes"]]
                a, b = set(v3["on_time_task_ids"]), set(other["on_time_task_ids"])
                row = {"condition": condition, "date": date, "baseline": baseline,
                       "task_count": v3["total_tasks"], "delta_on_time": len(a) - len(b),
                       "delta_P300_pp": 100 * (len(a) - len(b)) / v3["total_tasks"],
                       "delta_R300_seconds": v3["R300_seconds"] - other["R300_seconds"],
                       "gained_on_time_ids": sorted(a - b), "lost_on_time_ids": sorted(b - a)}
                pairs.append(row)
                daily.append(row)
            stats = {"condition": condition, "baseline": baseline, "days": len(dates)}
            for metric in ("P300_pp", "R300_seconds"):
                diffs = [r["delta_" + metric] for r in daily]
                sign_values = [r["delta_on_time"] for r in daily] if metric == "P300_pp" else diffs
                stats[metric] = {"mean_difference": math.fsum(diffs) / len(dates), "bootstrap_ci95": bootstrap(diffs),
                                 **exact_sign_test(sign_values, 0 if metric == "P300_pp" else 1e-6)}
            statistics.append(stats)
        for date in dates:
            r = records[(condition, date, "three_plan")]
            first = {}
            for d in r["decisions"]:
                first.setdefault(d["task_id"], d)
            ids = {o["task_id"] for o in r["metrics"]["task_outcomes"]}
            assert set(first) <= ids
            counts = Counter(d["selected"] for d in first.values())
            missing = len(ids - set(first))
            assert sum(counts.values()) + missing == r["total_tasks"]
            choices.append({"condition": condition, "date": date, "tasks": r["total_tasks"],
                            "first_choice_counts": {p: counts[p] for p in PLANS},
                            "fractions_all_images": {p: counts[p] / r["total_tasks"] for p in PLANS},
                            "without_whole_image_choice": missing, "decision_events": len(r["decisions"]),
                            "unclassified_outcomes": [o for o in r["metrics"]["task_outcomes"] if o["task_id"] not in first],
                            "repeated_choice_events": len(r["decisions"]) - len(first),
                            "no_finite_quote_events": r["selection_counts"].get("no_finite_quote", 0)})
            other = records[(condition, date, "two_plan")]
            a, b = set(r["on_time_task_ids"]), set(other["on_time_task_ids"])
            for plan in PLANS + ["unclassified"]:
                group = {tid for tid in ids if first.get(tid, {}).get("selected", "unclassified") == plan}
                transitions.append({"condition": condition, "date": date, "v3_first_plan": plan,
                                    "images": len(group), "gained": len(group & (a - b)), "lost": len(group & (b - a)),
                                    "both_on_time": len(group & a & b), "neither_on_time": len(group - a - b)})
    corrected = holm([s[m]["p_two_sided"] for s in statistics for m in ("P300_pp", "R300_seconds")])
    for i, s in enumerate(statistics):
        for mi, metric in enumerate(("P300_pp", "R300_seconds")):
            s[metric]["p_holm_50"] = corrected[2 * i + mi]
    mean_choices = []
    for condition in CONDITIONS:
        rows = [r for r in choices if r["condition"] == condition]
        mean_choices.append({"condition": condition,
                             "day_equal_fraction": {p: math.fsum(r["fractions_all_images"][p] for r in rows) / len(dates) for p in PLANS},
                             "image_pooled_fraction": {p: sum(r["first_choice_counts"][p] for r in rows) / sum(r["tasks"] for r in rows) for p in PLANS},
                             "counts": {p: sum(r["first_choice_counts"][p] for r in rows) for p in PLANS},
                             "tasks": sum(r["tasks"] for r in rows),
                             "without_whole_image_choice": sum(r["without_whole_image_choice"] for r in rows),
                             "repeated_choice_events": sum(r["repeated_choice_events"] for r in rows)})
    return {"main_table": main, "paired_daily": pairs, "statistics": statistics,
            "daily_choices": choices, "mean_choices": mean_choices, "two_plan_transitions": transitions}


def export_figure(fig, name):
    for suffix in ("pdf", "svg", "png"):
        fig.savefig(OUT / f"{name}.{suffix}", dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def figures(result):
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "pdf.fonttype": 42,
                         "svg.fonttype": "none", "axes.spines.top": False, "axes.spines.right": False})
    by_key = {(r["condition"], r["method"]): r for r in result["main_table"]}
    fig, ax = plt.subplots(figsize=(12.7, 4.4))
    ax.set_axis_off()
    table_rows = [[f"{by_key[c,m]['P300_percent']:.2f}%\n{by_key[c,m]['R300_seconds']:.2f} s" for m in METHODS] for c in CONDITIONS]
    table = ax.table(cellText=table_rows, rowLabels=COND_LABELS, colLabels=[LABELS[m] for m in METHODS],
                     cellLoc="center", bbox=[.10, .18, .90, .72])
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    for (r, c), cell in table.get_celld().items():
        cell.set_edgecolor("#d3d8dd")
        cell.set_linewidth(.6)
        if r == 0:
            cell.set_facecolor("#edf1f4")
            cell.set_text_props(weight="bold")
        elif r % 2 == 0:
            cell.set_facecolor("#f7f8f9")
        if r > 0 and c >= 0:
            condition = CONDITIONS[r - 1]
            best = max(METHODS, key=lambda m: by_key[condition, m]["P300_percent"])
            if METHODS[c] == best:
                cell.set_text_props(weight="bold")
    fig.text(.13, .94, "Formal test: six-day equal-weight means", fontsize=14, weight="bold")
    fig.text(.13, .055, "Cell: P300 (higher is better) / R300 seconds (lower is better). Bold: highest mean P300 in each condition.\n"
             "Dates: 2026-12-01 to 12-06. All 180 runs passed audit; identical image arrivals within each date and condition.", fontsize=9)
    export_figure(fig, "a_main_results")
    colors = ["#0072B2", "#D55E00", "#009E73", "#CC79A7", "#7A64AB", "#202020"]
    markers = ["o", "s", "^", "D", "v", "*"]
    fig, ax = plt.subplots(figsize=(9.2, 5.1))
    for method, color, marker in zip(METHODS, colors, markers):
        ax.plot(range(5), [by_key[c, method]["P300_percent"] for c in CONDITIONS], label=LABELS[method],
                color=color, marker=marker, markersize=9 if method == "three_plan" else 6,
                linewidth=2.4 if method == "three_plan" else 1.5, linestyle="-" if method == "three_plan" else "--")
    ax.set(xticks=range(5), xticklabels=COND_LABELS, ylabel="P300 (%)", ylim=(-3, 103),
           title="Frozen formal test across five conditions")
    ax.grid(axis="y", alpha=.22)
    ax.legend(ncols=3, loc="upper center", frameon=False, fontsize=9)
    fig.subplots_adjust(bottom=.17)
    fig.text(.13, .025, "Six-day equal-weight means. Conditions are categorical; connecting lines do not imply a continuous sweep.", fontsize=8)
    export_figure(fig, "b_condition_p300")
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    bottom = np.zeros(5)
    for plan, label, color in zip(PLANS, ["Local", "CGS", "Ground"], ["#0072B2", "#009E73", "#D55E00"]):
        values = np.array([r["day_equal_fraction"][plan] * 100 for r in result["mean_choices"]])
        ax.bar(range(5), values, bottom=bottom, width=.62, label=label, color=color)
        for x, (height, start) in enumerate(zip(values, bottom)):
            if height >= 4:
                ax.text(x, start + height / 2, f"{height:.1f}%", ha="center", va="center", color="white", fontsize=10)
        bottom += values
    missing = np.maximum(0, 100 - bottom)
    ax.bar(range(5), missing, bottom=bottom, width=.62, label="No whole-image log", color="#c5c9ce", hatch="///")
    if max(missing) > .05:
        ax.annotate(f"{missing[-1]:.2f}% unclassified", (4, 100), xytext=(3.42, 109),
                    fontsize=8, ha="center", arrowprops={"arrowstyle": "-", "color": "#555555"})
    ax.set(xticks=range(5), xticklabels=COND_LABELS, ylim=(0, 114), ylabel="Share of all images (%)",
           title="v3 plan selection by condition")
    ax.legend(ncols=4, loc="upper center", bbox_to_anchor=(.5, 1.14), frameon=False, fontsize=9)
    fig.subplots_adjust(bottom=.19)
    fig.text(.13, .03, "Six-day equal-weight fractions of all images; first whole-image choice only, missing logs retained.\n"
             "A selected plan is not a causal attribution of its performance gain.", fontsize=8)
    export_figure(fig, "c_plan_shares")
    tex = [r"\begin{tabular}{lrrrrrr}", r"\hline",
           "Condition & " + " & ".join(LABELS[m].replace(" + ", r" $+$ ") for m in METHODS) + r" \\", r"\hline"]
    for c, label in zip(CONDITIONS, COND_LABELS):
        cells = [f"{by_key[c,m]['P300_percent']:.2f} / {by_key[c,m]['R300_seconds']:.2f}" for m in METHODS]
        tex.append(label + " & " + " & ".join(cells) + r" \\")
    tex += [r"\hline", r"\end{tabular}", "% Entries: P300 percent / R300 seconds; equal-weight six-day means."]
    (OUT / "main_table.tex").write_text("\n".join(tex) + "\n")


def report(result):
    rows = ["# 正式测试配对统计与论文图表", "", "仅复用180份已完成正式记录，无新增仿真、方法修改或参数调整。",
            "", "## 统计口径", "",
            "每个条件在同一日期配对，差值统一为 v3 减基线：P300用百分点（正值更好），R300用秒（负值更好）。",
            "以6日为重采样单位，枚举6^6=46,656种有放回日期序列，报告配对均值的percentile bootstrap 95%区间（linear分位数）。不按单图重采样，不把各方法独立重采样。",
            "双侧精确符号检验：P300按按时计数差判符号；R300绝对差≤1e-6秒视为平，剔除平局后进行二项检验。另列50次比较（5条件×5基线×2指标）的Holm校正p。",
            "样本只有6个相邻仿真日，最小双侧p=0.03125；相邻日期并非已证明独立的星座样本。bootstrap区间依赖日期可交换假设，只反映这些日期的经验变动，不代表跨星座或真实任务总体保证。",
            "同时比较较多，不能只摘取未校正p<0.05宣称普遍显著；区间与符号检验针对不同统计量，二者不必给出相同判断。",
            "", "## 主表：日等权均值", "", "单元格为P300% / R300秒。", "",
            "|条件|" + "|".join(LABELS[m] for m in METHODS) + "|", "|---|" + "---:|" * 6]
    index = {(r["condition"], r["method"]): r for r in result["main_table"]}
    for c in CONDITIONS:
        rows.append("|" + c + "|" + "|".join(f"{index[c,m]['P300_percent']:.4f} / {index[c,m]['R300_seconds']:.4f}" for m in METHODS) + "|")
    rows += ["", "图表： [主表](a_main_results.pdf)、[条件P300](b_condition_p300.pdf)、[方案占比](c_plan_shares.pdf)。同名SVG/PNG和可粘入论文的main_table.tex均已导出。条件图连接线仅引导阅读，计算倍率与负载倍率不是同一连续自变量。", "",
             "## 配对差与区间", "", "P列为胜/平/负；R列为改善/平/退化。p为双侧符号检验，括号内为50项Holm校正。", "",
             "|条件|对照|ΔP均值 [95%CI] pp|P胜/平/负|P p (Holm)|ΔR均值 [95%CI] s|R改善/平/退化|R p (Holm)|",
             "|---|---|---:|---|---:|---:|---|---:|"]
    for s in result["statistics"]:
        p, r = s["P300_pp"], s["R300_seconds"]
        fmt = lambda x: f"{x['mean_difference']:+.3f} [{x['bootstrap_ci95'][0]:+.3f}, {x['bootstrap_ci95'][1]:+.3f}]"
        rows.append(f"|{s['condition']}|{LABELS[s['baseline']]}|{fmt(p)}|{p['positive']}/{p['zero']}/{p['negative']}|"
                    f"{p['p_two_sided']:.5f} ({p['p_holm_50']:.3f})|{fmt(r)}|{r['negative']}/{r['zero']}/{r['positive']}|"
                    f"{r['p_two_sided']:.5f} ({r['p_holm_50']:.3f})|")
    rows += ["", "## 两方案对照：17胜13负的分布", "",
             "|条件|P300胜/平/负|ΔP均值 pp|ΔR均值 s|赢的日期|输的日期|", "|---|---|---:|---:|---|---|"]
    for c in CONDITIONS:
        s = next(s for s in result["statistics"] if s["condition"] == c and s["baseline"] == "two_plan")
        d = [r for r in result["paired_daily"] if r["condition"] == c and r["baseline"] == "two_plan"]
        wins = ", ".join(r["date"][5:] for r in d if r["delta_on_time"] > 0)
        losses = ", ".join(r["date"][5:] for r in d if r["delta_on_time"] < 0)
        p, r = s["P300_pp"], s["R300_seconds"]
        rows.append(f"|{c}|{p['positive']}/{p['zero']}/{p['negative']}|{p['mean_difference']:+.4f}|{r['mean_difference']:+.4f}|{wins or '-'}|{losses or '-'}|")
    rows += ["", "收益集中在重计算与高负载：compute_10与load_4六日全胜，compute_4五胜一负；base与load_2六日全负。",
             "三方案增加地面选项并不保证实际交付单调改善：报价低估与跨图资源竞争是已知机制；正式记录的关联分组只能定位落差，不能在未做反事实干预时断言每个丢失任务的因果原因。不得据此重新调参。", "",
             "以下按v3的首次选择分组，只读统计相对旧两方案新增/丢失按时图；不同策略具有不同资源历史，不能把该表当成单独某方案的因果效应。", "",
             "|条件|v3选项|图数|新增按时|丢失按时|", "|---|---|---:|---:|---:|"]
    for c in CONDITIONS:
        for p in PLANS + ["unclassified"]:
            group = [r for r in result["two_plan_transitions"] if r["condition"] == c and r["v3_first_plan"] == p]
            rows.append(f"|{c}|{p}|{sum(r['images'] for r in group)}|{sum(r['gained'] for r in group)}|{sum(r['lost'] for r in group)}|")
    rows += ["", "## 首次方案选择比例", "", "主图采用六日日等权比例；括号内为跨日按图合并比例。分母是每一天所有图像，重复报价不重复计数。", "",
             "|条件|本地%|CGS%|地面%|总图数|未出现整图决策|重复决策事件|", "|---|---:|---:|---:|---:|---:|---:|"]
    for r in result["mean_choices"]:
        shares = "|".join(f"{r['day_equal_fraction'][p]*100:.3f} ({r['image_pooled_fraction'][p]*100:.3f})" for p in PLANS)
        rows.append(f"|{r['condition']}|{shares}|{r['tasks']}|{r['without_whole_image_choice']}|{r['repeated_choice_events']}|")
    unclassified = [o for day in result["daily_choices"] for o in day["unclassified_outcomes"]]
    reasons = Counter((o["status"], o["failure_reason"]) for o in unclassified)
    rows += ["", f"未记录整图选择的{len(unclassified)}图按现有结局统计为：{dict(reasons)}。"
             "选择日志只在完整可选图上写入；不完整可见图会回退CGS，另有准入失败图。因此灰色部分不强行计入CGS整图选项，也不从分母剔除。",
             "", "## 全部逐日配对", "", "新增/丢失的具体task_id见statistics.json的paired_daily；此处列150行每日差。", "",
             "|条件|日期|对照|Δ按时图|ΔP pp|ΔR s|新增|丢失|", "|---|---|---|---:|---:|---:|---:|---:|"]
    for r in result["paired_daily"]:
        rows.append(f"|{r['condition']}|{r['date']}|{r['baseline']}|{r['delta_on_time']:+d}|{r['delta_P300_pp']:+.4f}|"
                    f"{r['delta_R300_seconds']:+.4f}|{len(r['gained_on_time_ids'])}|{len(r['lost_on_time_ids'])}|")
    (OUT / "REPORT.md").write_text("\n".join(rows) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    OUT.mkdir(exist_ok=True)
    dates, records, hashes, frozen_files = load_inputs()
    result = analyze(dates, records)
    result["provenance"] = {"created_at_utc": datetime.now(timezone.utc).isoformat(), "dates": dates,
                            "source_summary_sha256": sha(SOURCE / "summary.json"), "record_sha256": hashes,
                            "frozen_source_files_verified": frozen_files, "script_sha256": sha(Path(__file__)),
                            "bootstrap": "paired day resampling, enumerate 6^6 sequences; percentile CI, linear quantile",
                            "sign_test": "exact two-sided, exclude zero count differences / R300 abs <=1e-6s",
                            "holm_family_size": 50, "new_simulations": 0}
    write(OUT / "statistics.json", result)
    report(result)
    figures(result)
    write(OUT / "artifact_hashes.json", {p.name: sha(p) for p in sorted(OUT.iterdir()) if p.is_file() and p.name != "artifact_hashes.json"})
    print(json.dumps({"records_verified": len(hashes), "paired_days": len(result["paired_daily"]),
                      "comparison_rows": len(result["statistics"]), "output": str(OUT)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
