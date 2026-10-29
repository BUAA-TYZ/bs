"""只读开发日整图记录，输出含右删失区间的事后时限曲线。"""

import csv
import hashlib
import json
import math
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parents[1]
MAIN = REPO / "results/redesign_20261021/main_scenario"
LOCAL = MAIN / "local_baselines"
OLD = REPO / "results/redesign_20261021/calibration/load_pressure_20261022/six_21600s"
OUT = MAIN / "posthoc_20261022"
DEADLINES = (60, 90, 120, 180, 240, 300, 600)
LABELS = {
    "v3": "全局 v3", "cgs": "CGS", "heft": "HEFT",
    "zero_1": "局部 v3 零负载 1跳", "zero_2": "局部 v3 零负载 2跳",
    "history_1": "局部 v3 历史均值 1跳", "history_2": "局部 v3 历史均值 2跳",
    "minload_1": "局部最小负载 1跳", "minload_2": "局部最小负载 2跳",
    "fifo": "最近计算星 FIFO（诊断）", "local": "本地", "ground": "强地面",
}
LOCAL_KEYS = tuple(k for k in LABELS if k.startswith(("zero", "history", "minload")))


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def score(outcomes, deadline):
    ontime = sorted(o["task_id"] for o in outcomes
                    if o["status"] == "completed" and o["latency_seconds"] <= deadline)
    # 未完成且随访不足D的图不能当作确定失败；保留全体图像分母。
    unknown = sorted(o["task_id"] for o in outcomes
                     if o["status"] != "completed" and o["observed_age_seconds"] < deadline)
    n = len(outcomes)
    return {"D_seconds": deadline, "on_time_count": len(ontime), "on_time_ids": ontime,
            "unknown_count": len(unknown), "unknown_ids": unknown, "total": n,
            "P_lower": len(ontime) / n, "P_upper": (len(ontime) + len(unknown)) / n}


def latency_stats(outcomes):
    values = [o["latency_seconds"] for o in outcomes if o["status"] == "completed"]
    unfinished = [o for o in outcomes if o["status"] != "completed"]
    return {"completed": len(values), "unfinished": len(unfinished),
            "unfinished_ids": [o["task_id"] for o in unfinished],
            "mean_completed_s": math.fsum(values) / len(values) if values else None,
            "P50_completed_s": float(np.percentile(values, 50, method="linear")) if values else None,
            "P90_completed_s": float(np.percentile(values, 90, method="linear")) if values else None,
            "max_completed_s": max(values) if values else None,
            "min_observed_age_s": min(o["observed_age_seconds"] for o in outcomes)}


def gap(rows, deadline, local_keys=LOCAL_KEYS):
    scores = {k: score(v, deadline) for k, v in rows.items()}
    v = scores["v3"]
    best_lower = max(scores[k]["P_lower"] for k in local_keys)
    best_upper = max(scores[k]["P_upper"] for k in local_keys)
    return {"D_seconds": deadline,
            "best_local_keys": [k for k in local_keys if scores[k]["P_lower"] == best_lower],
            "best_local_P_lower": best_lower, "best_local_P_upper": best_upper,
            "gap_lower_pp": 100 * (v["P_lower"] - best_upper),
            "gap_upper_pp": 100 * (v["P_upper"] - best_lower)}


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def write_csv(path, rows):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def plot_curves(rows, intervals):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 1, figsize=(10.5, 8), layout="constrained",
                             gridspec_kw={"height_ratios": [2.2, 1]})
    # 四个局部v3的按时集合曲线相同，合并图例；表格仍逐项列出。
    groups = [("v3", "Global v3"), ("cgs", "CGS"), ("heft", "HEFT"),
              ("zero_1", "Local v3 (all 4 variants)"),
              ("minload_1", "Min-load 1-hop"), ("minload_2", "Min-load 2-hop"),
              ("fifo", "FIFO diagnostic"), ("local", "Local processing"), ("ground", "Ground")]
    for i, (key, label) in enumerate(groups):
        xs = sorted({60., 600., *[o["latency_seconds"] for o in rows[key]
                                  if o["status"] == "completed" and 60 <= o["latency_seconds"] <= 600]})
        ys = [100 * score(rows[key], x)["P_lower"] for x in xs]
        axes[0].step(xs, ys, where="post", label=label, linewidth=2 if key == "v3" else 1.3,
                     linestyle=["-", "--", "-."][i % 3])
        last = score(rows[key], 600)
        if last["unknown_count"]:
            axes[0].plot([600, 600], [100 * last["P_lower"], 100 * last["P_upper"]], color="black", lw=2)
    for k in ("zero_2", "history_1", "history_2"):
        a = sorted(o["latency_seconds"] for o in rows["zero_1"]
                   if o["status"] == "completed" and o["latency_seconds"] >= 60)
        b = sorted(o["latency_seconds"] for o in rows[k]
                   if o["status"] == "completed" and o["latency_seconds"] >= 60)
        # 历史1跳的少量时延差均在60秒以下，不影响本图显示区间。
        assert a == b and score(rows[k], 60) == score(rows["zero_1"], 60)
    axes[0].set(ylabel="P(D), %", ylim=(0, 102), title="Development day 2026-10-15: fixed six-hour trajectories")
    axes[0].legend(ncol=3, fontsize=8, loc="lower right")
    xs = [i["start_inclusive_s"] for i in intervals] + [600]
    lower = [i["gap_lower_pp"] for i in intervals] + [gap(rows, 600)["gap_lower_pp"]]
    upper = [i["gap_upper_pp"] for i in intervals] + [gap(rows, 600)["gap_upper_pp"]]
    axes[1].step(xs, lower, where="post", color="#246a52", label="Global v3 - best local envelope")
    axes[1].fill_between(xs, lower, upper, step="post", color="#246a52", alpha=.2, label="Censoring bounds")
    axes[1].axhline(0, color="#777777", linewidth=.7)
    axes[1].set(xlabel="Post-hoc deadline D (seconds)", ylabel="Difference (pp)")
    axes[1].legend(fontsize=8)
    for ax in axes:
        ax.set_xlim(60, 600)
        ax.set_xticks(DEADLINES)
        ax.grid(alpha=.2)
    fig.savefig(OUT / "deadline_curves.png", dpi=180)
    fig.savefig(OUT / "deadline_curves.svg")
    plt.close(fig)


def main():
    paths = {m: OLD / stage / f"{m}.json" for stage, methods in
             [("neutral", ["local", "ground", "fifo"]), ("information", ["v3", "cgs", "heft"])] for m in methods}
    paths.update({f"{v}_{k}": LOCAL / v / f"local_{k}.json" for v in ["zero", "history", "minload"] for k in (1, 2)})
    p, old = read(LOCAL / "protocol.json"), read(OLD / "protocol.json")
    assert p["config"] == old["config"] and p["scored_task_ids"] == old["scored_task_ids"]
    wanted = set(p["scored_task_ids"])
    assert len(wanted) == 340
    inputs = {str(f): sha(f) for f in [*paths.values(), LOCAL / "protocol.json", OLD / "protocol.json",
                                     MAIN / "manifest.json", MAIN / "date_split.json"]}
    for table in (p["input_hashes"], p["frozen_protocol"]["source_hashes"],
                  p["frozen_protocol"]["implementation_hashes"]):
        for path, expected in table.items():
            assert sha(REPO / path) == expected, path
    rows, summary, curves, latencies, per_image = {}, {}, [], [], []
    for key in LABELS:
        raw = read(paths[key])
        assert raw["status"] == "PASS" and raw["config_sha256"] == p["config_sha256"]
        outcomes = [o for o in raw["outcomes"] if o["task_id"] in wanted]
        assert len(outcomes) == 340 and {o["task_id"] for o in outcomes} == wanted
        for o in outcomes:
            assert o["deadline_seconds"] is None
            if o["status"] == "completed":
                assert 0 <= o["latency_seconds"] <= o["observed_age_seconds"] + 1e-7
                assert abs(o["latency_seconds"] - (o["completion_seconds"] - o["release_seconds"])) < 1e-6
        rows[key] = outcomes
        scored = [score(outcomes, d) for d in DEADLINES]
        assert scored[5]["P_lower"] == raw["P_D"] and scored[5]["unknown_count"] == 0
        assert scored[5]["on_time_ids"] == sorted(raw["on_time_task_ids"])
        stats = latency_stats(outcomes)
        summary[key] = {"label": LABELS[key], "curve": scored, "latency": stats}
        curves.extend({"method": key, **{k: v for k, v in s.items() if not k.endswith("_ids")}} for s in scored)
        latencies.append({"method": key, **{k: v for k, v in stats.items() if not k.endswith("_ids")}})
        per_image.extend({"method": key, **{k: o[k] for k in ("task_id", "status", "release_seconds", "completion_seconds", "latency_seconds", "observed_age_seconds")}} for o in outcomes)
    gaps = [gap(rows, d) for d in DEADLINES]
    breaks = {60., 600.}
    for k in ("v3", *LOCAL_KEYS):
        for o in rows[k]:
            t = o["latency_seconds"] if o["status"] == "completed" else o["observed_age_seconds"]
            if 60 < t < 600:
                breaks.add(t)
    breaks = sorted(breaks)
    intervals = [{"start_inclusive_s": a, "end_exclusive_s": b, **gap(rows, a)} for a, b in zip(breaks, breaks[1:])]
    largest = max(r["gap_lower_pp"] for r in intervals)
    maxima = [r for r in intervals if abs(r["gap_lower_pp"] - largest) < 1e-9]
    if (OUT / "AUDIT.json").exists():
        raise FileExistsError("Completed report exists; preserve it")
    OUT.mkdir(exist_ok=True)
    write_json(OUT / "summary.json", {"scope": "development-only fixed trajectories, not reoptimized for D",
                                     "methods": summary, "gaps": gaps, "maximum_gap_intervals_60_to_600": maxima,
                                     "best_local_includes": LOCAL_KEYS, "no_simulation": True})
    write_csv(OUT / "deadline_curve.csv", curves)
    write_csv(OUT / "latency_summary.csv", latencies)
    write_csv(OUT / "per_image.csv", per_image)
    write_json(OUT / "gap_intervals.json", intervals)
    plot_curves(rows, intervals)
    def fmt(s):
        lo, hi = 100*s["P_lower"], 100*s["P_upper"]
        return f"{lo:.2f}" if s["unknown_count"] == 0 else f"[{lo:.2f}, {hi:.2f}]"
    lines = ["# 主场景M：开发日事后时限曲线", "",
             "仅复算2026-10-15同一六小时的12份已有记录，每种方法340张主窗口图像；排除2张预热图。没有新仿真、训练或测试日期访问。",
             "", "## 评分口径", "",
             "P_D分母固定340；已完成且真实整图交付时延≤D计成功。未完成且观察年龄<D记不可判定，报告[已知成功/340, (已知成功+不可判定)/340]，不删除末尾图像、不将短随访当失败。",
             "原实验无硬截止淘汰，D=300用于评分而非超时杀任务；本表只是同一轨迹的事后阈值，不代表针对新D重新优化。原尾随301秒，实际最末图随访346秒。",
             "P600中，全局v3、强地面、四个局部v3均有task_341一张不可判定（346秒）；其余方法该图已交付，所以P600可确定。区间宽度1/340=0.2941个百分点。",
             "平均/P50/P90只对截至仿真结束已完整交付的图像计算，不截断；未完成另列，分位数用线性插值。不能把这些条件均值称作包含未完成图像的总体平均，不能用不同完成子集的均值单独判断优劣。存在未完成图像的方法，其全体图像平均时延仍未知；本地340张均已完成。",
             "", "## P_D曲线（%）", "", "|方法|" + "|".join(f"{d}s" for d in DEADLINES) + "|",
             "|---|" + "---:|"*len(DEADLINES)]
    for k, r in summary.items():
        lines.append("|" + LABELS[k] + "|" + "|".join(fmt(s) for s in r["curve"]) + "|")
    lines += ["", "## 完整已交付图像时延（秒）", "", "|方法|完成|未完成|均值|P50|P90|", "|---|---:|---:|---:|---:|---:|"]
    for k, r in summary.items():
        s = r["latency"]
        lines.append(f"|{LABELS[k]}|{s['completed']}|{s['unfinished']}|{s['mean_completed_s']:.2f}|{s['P50_completed_s']:.2f}|{s['P90_completed_s']:.2f}|")
    lines += ["", "## 与最强局部规则的差距", "",
              "逐D取六项局部规则的最大P_D，形成事后包络，不是一套预先选定的可执行策略。四个局部v3在七个阈值均并列；FIFO为诊断不参与此六项排名，将FIFO纳入不会改变任何阈值的最强值。", "",
              "|D(s)|最强局部规则|v3减最强局部(pp)|", "|---:|---|---:|"]
    for g in gaps:
        label = "局部v3四变体并列" if len(g["best_local_keys"]) == 4 else "/".join(LABELS[k] for k in g["best_local_keys"])
        a, b = g["gap_lower_pp"], g["gap_upper_pp"]
        delta = f"{a:+.4f}" if abs(a-b)<1e-9 else f"[{a:+.4f}, {b:+.4f}]"
        lines.append(f"|{g['D_seconds']}|{label}|{delta}|")
    lines += ["", "七个指定阈值中最大差距在D=60秒：v3 264/340、最强局部175/340，+89图、+26.1765个百分点。60/90/120秒分别差26.18/19.71/17.06个百分点；短时限60–120秒是差距较明显区间，不能据此只报告60秒。", "",
              "逐个真实交付时刻计算60–600秒阶梯包络，最大差距及其半开区间（不是插值）为："]
    lines.extend(f"- [{r['start_inclusive_s']:.1f}, {r['end_exclusive_s']:.1f})秒：{r['gap_lower_pp']:.4f}个百分点。" for r in maxima)
    lines += ["", "实施计划暂建议D=60秒作按时奖励阈值，依据仅为开发日七档探索；这不是业务SLA或独立测试证据，需用户确认后固定。正式汇报报告整条预定曲线，不能只报择优D。", "",
              "全局v3已完成334张，平均133.80秒；2跳最小负载也完成334张，平均190.15秒（完成集合不同）。此前84.86秒差对应R300；当前未截断条件均值差为56.35秒。局部v3的R300≈98.82秒，但完整已交付均值≈479.67秒，说明截断指标隐藏了长尾，不应笼统称其平均时延更优。", "",
              "未来评价需同时报告P_D整条曲线、完成/未完成数、已完成均值和分位数，并补充统一随访下R600，避免以丢弃慢任务改善均值。全局v3是信息更充分的经验参考，不是数学上界；原99.7059%协议上界仅对应D300，不能用于其他D。", "",
              "![全部方法开发日时限曲线](deadline_curves.png)", "",
              "文件：deadline_curve.csv（84行）、latency_summary.csv（12行）、per_image.csv（4080行）、summary.json（含逐D按时/不可判定任务ID）、gap_intervals.json（完整阶梯差距）、AUDIT.json（输入输出哈希）。"]
    (OUT / "REPORT.md").write_text("\n".join(lines) + "\n")
    assert all(sha(Path(f)) == expected for f, expected in inputs.items())
    for d in DEADLINES:
        assert gap(rows, d, (*LOCAL_KEYS, "fifo"))["best_local_P_lower"] == gap(rows, d)["best_local_P_lower"]
    write_json(OUT / "AUDIT.json", {"status": "PASS", "input_hashes_before_and_after": inputs,
                                    "script_sha256": sha(Path(__file__)), "methods": 12, "scored_per_method": 340,
                                    "source_hashes_verified": len(p["frozen_protocol"]["source_hashes"]),
                                    "output_hashes": {f.name: sha(f) for f in OUT.iterdir() if f.is_file()}})
    print(json.dumps({"output": str(OUT), "maximum_intervals": maxima, "gaps": gaps}, ensure_ascii=False))


if __name__ == "__main__":
    main()
