"""两个固定监督式变体的训练、验证汇总及冻结；不运行盲测。"""

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "results/sar_selector_learning_20261019"
PLANS = ("local", "cgs", "ground")
SEEDS = (1701, 1702, 1703)


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def class_weights(labels):
    counts = np.bincount(labels, minlength=3)
    if np.any(counts == 0):
        raise ValueError(f"Missing training class: {counts.tolist()}; cannot satisfy three-class weighted training")
    return len(labels) / (3 * counts)


def forward(model, features):
    x = np.asarray([features[k] for k in model["feature_names"]], dtype=np.float64)
    x = (x - np.asarray(model["mean"])) / np.asarray(model["std"])
    for i, layer in enumerate(model["layers"]):
        x = np.asarray(layer["weight"]) @ x + np.asarray(layer["bias"])
        if i < len(model["layers"]) - 1:
            x = np.maximum(x, 0)
    return x


def predict(model, features):
    y = forward(model, features)
    return int(np.argmax(y) if model["variant"] == "classifier" else np.argmin(y))


def recalls(targets, predictions):
    confusion = np.zeros((3, 3), dtype=int)
    for actual, prediction in zip(targets, predictions):
        confusion[actual, prediction] += 1
    return {"matrix_actual_rows_predicted_columns": confusion.tolist(),
            "recall": {plan: float(confusion[i, i] / confusion[i].sum()) if confusion[i].sum() else None
                       for i, plan in enumerate(PLANS)},
            "support": {plan: int(confusion[i].sum()) for i, plan in enumerate(PLANS)}}


def rows_and_scaler(audit):
    train_rows = [r for r in audit["labels"] if r["split"] == "train" and r["status"] == "DETERMINATE"]
    validation = [r for r in audit["labels"] if r["split"] == "validation" and r["status"] == "DETERMINATE"]
    names = list(train_rows[0]["features"])
    assert all(list(r["features"]) == names for r in train_rows + validation)
    x = np.asarray([[r["features"][k] for k in names] for r in train_rows])
    mean, std = x.mean(axis=0), x.std(axis=0)
    std = np.where(std < 1e-6, 1, std)
    return train_rows, validation, names, x, mean, std


def train():
    import torch
    from torch import nn
    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    audit = read(OUT / "label_audit.json")
    assert audit["passed"]
    assert not (OUT / "training_complete.json").exists(), "Training already completed; do not rerun"
    protocol = read(OUT / "protocol.json")
    spec = protocol["model"]
    train_rows, val_rows, names, x, mean, std = rows_and_scaler(audit)
    y = np.asarray([PLANS.index(r["label"]) for r in train_rows])
    weights = class_weights(y)
    tx = torch.tensor((x - mean) / std, dtype=torch.float64)
    ty = torch.tensor(y)
    tw = torch.tensor(weights, dtype=torch.float64)
    target, censored = [], []
    for r in train_rows:
        target.append([np.log1p(b["latency_seconds"] if b["latency_seconds"] is not None
                               else b["censor_seconds"] - b["release_seconds"])
                       for b in (r["branches"][p] for p in PLANS)])
        censored.append([r["branches"][p]["latency_seconds"] is None for p in PLANS])
    target = torch.tensor(target, dtype=torch.float64)
    censored = torch.tensor(censored, dtype=torch.bool)
    records = []
    for variant in spec["variants"]:
        for seed in SEEDS:
            torch.manual_seed(seed)
            net = nn.Sequential(nn.Linear(len(names), 32), nn.ReLU(), nn.Linear(32, 16), nn.ReLU(), nn.Linear(16, 3)).double()
            optimizer = torch.optim.Adam(net.parameters(), lr=spec["learning_rate"], weight_decay=spec["weight_decay"])
            curve = []
            for epoch in range(1, spec["epochs"] + 1):
                optimizer.zero_grad()
                prediction = net(tx)
                if variant == "classifier":
                    loss = nn.functional.cross_entropy(prediction, ty, weight=tw)
                else:
                    # 删失只有下界；预测超过下界不惩罚，也不伪造完成时延。
                    error = torch.where(censored, torch.minimum(prediction - target, torch.zeros_like(prediction)), prediction - target)
                    per_row = nn.functional.smooth_l1_loss(error, torch.zeros_like(error), reduction="none").mean(1)
                    loss = (per_row * tw[ty]).sum() / tw[ty].sum()
                assert torch.isfinite(loss)
                loss.backward()
                optimizer.step()
                with torch.no_grad():
                    scores = net(tx)
                    selected = scores.argmax(1) if variant == "classifier" else scores.argmin(1)
                    accuracy = float((selected == ty).double().mean())
                curve.append({"epoch": epoch, "weighted_training_loss": float(loss.detach()), "training_accuracy": accuracy})
            model = {"variant": variant, "seed": seed, "feature_names": names, "mean": mean.tolist(), "std": std.tolist(),
                     "class_weights": dict(zip(PLANS, weights.tolist())), "epochs": spec["epochs"],
                     "layers": [{"weight": layer.weight.detach().numpy().tolist(), "bias": layer.bias.detach().numpy().tolist()}
                                for layer in net if isinstance(layer, nn.Linear)]}
            for i, row in enumerate(train_rows):
                np.testing.assert_allclose(forward(model, row["features"]), net(tx[i]).detach().numpy(), rtol=1e-10, atol=1e-10)
            predictions = [predict(model, r["features"]) for r in val_rows]
            validation_recall = recalls([PLANS.index(r["label"]) for r in val_rows], predictions)
            model["parameter_count"] = sum(p.numel() for p in net.parameters())
            write(OUT / "models" / f"{variant}_{seed}.json", model)
            report = {"variant": variant, "seed": seed, "curve": curve, "validation_recall": validation_recall,
                      "train_recall": recalls(y, [predict(model, r["features"]) for r in train_rows]),
                      "train_states": len(train_rows), "validation_states": len(val_rows), "parameter_count": model["parameter_count"]}
            write(OUT / "training" / f"{variant}_{seed}.json", report)
            records.append(report)
    write(OUT / "training_complete.json", {"status": "PASS", "records": records,
                                          "finished_at_utc": datetime.now(timezone.utc).isoformat()})
    curves(records)


def curves(records):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(2, 2, figsize=(10, 6), layout="constrained")
    for col, variant in enumerate(("classifier", "regressor")):
        for row in records:
            if row["variant"] != variant:
                continue
            epochs = [r["epoch"] for r in row["curve"]]
            axes[0, col].plot(epochs, [r["weighted_training_loss"] for r in row["curve"]], label=str(row["seed"]))
            axes[1, col].plot(epochs, [r["training_accuracy"] for r in row["curve"]], label=str(row["seed"]))
        axes[0, col].set_title(variant.capitalize())
        axes[0, col].set_ylabel("Weighted training loss")
        axes[0, col].legend(title="Seed")
        axes[1, col].set_ylabel("Training label accuracy")
        axes[1, col].set_xlabel("Epoch")
        axes[1, col].set_ylim(0, 1)
    for extension in ("pdf", "svg", "png"):
        fig.savefig(OUT / f"training_curves.{extension}", dpi=180)
    plt.close(fig)


def latency_score(latencies):
    assert latencies
    return {"P300": sum(x is not None and x <= 300 for x in latencies) / len(latencies),
            "R300_seconds": sum(min(300, x) if x is not None else 300 for x in latencies) / len(latencies)}


def report():
    sys.path.insert(0, str(ROOT))
    from analysis.sar_selector_learning import audit_sources, sha, DEADLINE
    assert datetime.now(timezone.utc) <= DEADLINE, "Registered freeze deadline exceeded"
    audit = read(OUT / "label_audit.json")
    protocol = read(OUT / "protocol.json")
    manifest = read(OUT / "manifest.json")
    assert audit["passed"]
    assert not (OUT / "freeze.json").exists(), "Already frozen; no reselection"
    jobs = [j for j in manifest["jobs"] if j["date"] in protocol["validation_dates"]]
    daily = []
    for job in jobs:
        v3 = read(OUT / "census" / f"{job['key']}.json")
        daily.append({"date": job["date"], "condition": job["condition"], "method": "v3", "seed": None,
                      **{k: v3[k] for k in ("P300", "R300_seconds", "on_time_tasks", "total_tasks")}})
        for method in ("classifier", "regressor", "random"):
            for seed in SEEDS:
                record = read(OUT / "evaluate" / f"{job['key']}_{method}_{seed}.json")
                assert record["status"] == "PASS" and record["audit"]["passed"]
                assert [r["task_id"] for r in record["outcomes"]] == [r["task_id"] for r in v3["outcomes"]]
                daily.append({"date": job["date"], "condition": job["condition"], "method": method, "seed": seed,
                              **{k: record[k] for k in ("P300", "R300_seconds", "on_time_tasks", "total_tasks")}})
    aggregate = []
    for method, seeds in (("v3", [None]), ("classifier", SEEDS), ("regressor", SEEDS), ("random", SEEDS)):
        for seed in seeds:
            group = [r for r in daily if r["method"] == method and r["seed"] == seed]
            assert len(group) == 15
            aggregate.append({"method": method, "seed": seed,
                              **{k: float(np.mean([r[k] for r in group])) for k in ("P300", "R300_seconds")}})
    candidates = [r for r in aggregate if r["method"] in ("classifier", "regressor")]
    chosen = min(candidates, key=lambda r: (-r["P300"], r["R300_seconds"], r["method"], r["seed"]))
    models = {(m, s): read(OUT / "models" / f"{m}_{s}.json") for m in ("classifier", "regressor") for s in SEEDS}
    sampled = []
    for job in jobs:
        rows = [r for r in audit["labels"] if r["date"] == job["date"] and r["condition"] == job["condition"]
                and r["status"] == "DETERMINATE"]
        if not rows:
            raise ValueError("No determinate validation states in cell")
        methods = [("v3", None), ("hindsight_local_oracle", None)] + list(models) + [("random", s) for s in SEEDS]
        for method, seed in methods:
            if method == "random":
                # 均匀随机的反事实期望，三方案等权，不添加蒙特卡洛误差。
                latency = [r["branches"][p]["latency_seconds"] for r in rows for p in PLANS]
            else:
                selections = [r["rule_choice"] if method == "v3" else r["label"] if method == "hindsight_local_oracle"
                              else PLANS[predict(models[(method, seed)], r["features"])] for r in rows]
                latency = [r["branches"][p]["latency_seconds"] for r, p in zip(rows, selections)]
            sampled.append({"date": job["date"], "condition": job["condition"], "method": method, "seed": seed,
                            "states": len(rows), **latency_score(latency)})
    summary = {"selected": chosen, "full_day_daily": daily, "full_day_equal_weight": aggregate,
               "same_state_local_counterfactual": sampled,
               "label_statistics": {k: v for k, v in audit.items() if k != "labels"},
               "source_audit": audit_sources(), "blind_test_opened": False,
               "oracle_scope": "Local state-conditioned bound with v3 continuation; not a global full-day optimum"}
    write(OUT / "summary.json", summary)
    lines = ["# 监督式整图选择器：开发、验证与冻结", "",
             f"标签覆盖 {audit['observed']}/{audit['planned']}={audit['coverage']:.2%}，不可判定 {audit['indeterminate_fraction']:.2%}。",
             f"训练/验证类别：{audit['class_counts']}；v3选错造成300秒超时：{audit['deadline_misses_caused_by_v3']}状态。",
             "原试点接受39/40用于诊断，缺失来自到达抽样而非实际决策；历史证据保留，未补跑该缺项。",
             "本轮先统计实际首次决策再分层抽样，每格不足8取全部。训练只用10-15至21，验证10-22至24；新盲测未启封。", "",
             "## 全天闭环（五条件、三日等权；每个种子单列）", "",
             "|方法|种子|P300|R300/秒|", "|---|---:|---:|---:|"]
    for row in aggregate:
        lines.append(f"|{row['method']}|{row['seed']}|{row['P300']:.4%}|{row['R300_seconds']:.4f}|")
    lines += ["", f"冻结选择：{chosen['method']} / {chosen['seed']}；按预登记P300优先、R300次之选择，不再调参。",
              "随机策略对同样三套方案均匀抽样；报价None不代表物理不可行，执行器仍执行资源检查。",
              "v3复用26小时标签母轨迹的前24小时到达队列评分；全部图像随访至少300秒，其他方法24小时+301秒。", "",
              "## 各条件日等权（学习/随机另对三种子等权）", "",
              "|条件|方法|P300|R300/秒|", "|---|---|---:|---:|"]
    for condition in protocol["conditions"]:
        for method in ("v3", "classifier", "regressor", "random"):
            rows = [r for r in daily if r["condition"] == condition and r["method"] == method]
            lines.append(f"|{condition}|{method}|{np.mean([r['P300'] for r in rows]):.4%}|{np.mean([r['R300_seconds'] for r in rows]):.4f}|")
    lines += ["", "## 同状态局部反事实（包括事后最优）", "",
              "这里的分母是已抽中且可判定的验证决策，不是全天所有图像；三分支随后均继续v3，不能据此宣称全天上界。", "",
              "|方法|P300|R300/秒|", "|---|---:|---:|"]
    for method in ("v3", "classifier", "regressor", "random", "hindsight_local_oracle"):
        rows = [r for r in sampled if r["method"] == method]
        lines.append(f"|{method}|{np.mean([r['P300'] for r in rows]):.4%}|{np.mean([r['R300_seconds'] for r in rows]):.4f}|")
    lines += ["", "## 验证标签各类别召回率", "", "|方法|种子|本地|CGS|地面|", "|---|---:|---:|---:|---:|"]
    for row in read(OUT / "training_complete.json")["records"]:
        recall = row["validation_recall"]["recall"]
        cells = ["N/A" if recall[p] is None else f"{recall[p]:.2%}" for p in PLANS]
        lines.append(f"|{row['variant']}|{row['seed']}|" + "|".join(cells) + "|")
    lines += ["", "训练曲线见training_curves.pdf/png；完整逐日成绩、同状态对照见summary.json；逐状态时限损失见label_audit.json。",
              "监督式方案选择器，不称MARL；类别权重与标准化只用训练日期，固定200轮，无验证选轮或额外变体。",
              "若未超过v3，如实保留负结果。冻结后停止，等待用户指令，不运行12-15至17盲测。"]
    (OUT / "REPORT.md").write_text("\n".join(lines) + "\n")
    paths = [ROOT / "analysis/sar_selector_learning.py", ROOT / "analysis/sar_selector_model.py",
             ROOT / "analysis/sar_selector_pilot.py", OUT / "protocol.json", OUT / "manifest.json", OUT / "label_audit.json",
             OUT / "summary.json", OUT / "REPORT.md"]
    paths += sorted((OUT / "models").glob("*.json")) + sorted((OUT / "configs").glob("*.json"))
    write(OUT / "freeze.json", {"frozen_at_utc": datetime.now(timezone.utc).isoformat(), "selected": chosen,
          "selected_model": str(OUT / "models" / f"{chosen['method']}_{chosen['seed']}.json"),
          "files": {str(p.relative_to(ROOT)): sha(p) for p in paths}, "blind_test_opened": False})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("train", "report"))
    globals()[parser.parse_args().command]()
