"""汇总已完成的固定 overlap 配对评估；同名面板仅在协议完全一致时合并。"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.evaluation import task_age_score


HORIZONS = [1800, 3600, 7200, 14400]
GROUND_MODES = {"ground_compute", "ground_tiles", "ground_relay"}
LABELS = {"ground_compute": "Ground image", "ground_tiles": "Ground tiles",
          "ground_relay": "Ground relay", "local": "Local + result relay",
          "greedy": "Greedy", "window_aware": "Window aware",
          "reservation_calendar": "Reservation", "heft_adapted": "HEFT adapted",
          "cgs_compute_adapted": "CGS adapted", "bc": "BC", "ppo": "PPO"}
RESOURCE_SUMS = ("source_compute_s", "non_source_compute_s", "source_capacity_s",
                 "non_source_capacity_s", "ground_compute_s", "ground_capacity_s",
                 "isl_sent_mb", "ground_sent_mb", "other_sent_mb", "raw_relay_hops",
                 "result_relay_hops")


def read_json(path):
    def invalid(value):
        raise ValueError(f"Non-finite JSON number in {path}: {value}")

    return json.loads(Path(path).read_text(encoding="utf-8"), parse_constant=invalid)


def close(left, right):
    return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-9)


def condition(case_id):
    return re.sub(r"_\d+$", "", re.sub(r"^(validation|test)_", "", case_id))


def without_pipeline(config):
    return {key: value for key, value in config.items() if key != "pipeline_mode"}


def context(record):
    exp = record["metrics"]["experiment"]
    return {"case": record["case"], "config": without_pipeline(exp["config"]),
            **{key: exp[key] for key in ("source_sha256", "task_trace_sha256", "tle_sha256",
                                        "workload_sha256", "elapsed_sim_seconds")},
            "task_definitions": sorted(
                (r["task_id"], r["source_sat_id"], r["release_seconds"], r["deadline_seconds"])
                for r in record["metrics"]["task_outcomes"])}


def validate_record(record, manifest, case_spec):
    case_id, method = record["case"]["id"], record["method"]
    tag = f"{case_id}/{method}"
    exp, overall = record["metrics"]["experiment"], record["metrics"]["overall"]
    cfg = exp["config"]
    if record["case"] != case_spec["case"] or without_pipeline(cfg) != without_pipeline(case_spec["config"]):
        raise ValueError(f"Case/config mismatch: {tag}")
    expected_pipeline = method if method in GROUND_MODES else case_spec["config"]["pipeline_mode"]
    if cfg["pipeline_mode"] != expected_pipeline:
        raise ValueError(f"Unexpected execution mode: {tag}")
    if exp["source_sha256"] != manifest["source_sha256"]:
        raise ValueError(f"Source mismatch: {tag}")
    expected_checkpoint = manifest.get("checkpoint_sha256", {}).get(method)
    if record.get("checkpoint_sha256") != expected_checkpoint:
        raise ValueError(f"Checkpoint mismatch: {tag}")
    if not close(exp["elapsed_sim_seconds"], cfg["sim_steps"] * cfg["dt"]):
        raise ValueError(f"Incomplete simulation: {tag}")
    end_step = cfg.get("task_arrival_end_step")
    if end_step is None:
        end_step = cfg["sim_steps"]
    if (cfg["sim_steps"] - end_step) * cfg["dt"] < max(HORIZONS):
        raise ValueError(f"Incomplete maximum-age follow-up: {tag}")
    outcomes = record["metrics"]["task_outcomes"]
    age = task_age_score(outcomes, HORIZONS, exp["elapsed_sim_seconds"])
    if not outcomes or overall["total_tasks"] != len(outcomes):
        raise ValueError(f"Empty/inconsistent task population: {tag}")
    if overall["completed_tasks"] != sum(r["status"] == "completed" for r in outcomes):
        raise ValueError(f"Inconsistent completed task count: {tag}")
    reported = record["task_age"]
    if reported["delivery_curve"] != age["delivery_curve"] or any(
            not close(reported[key], age[key]) for key in ("delivery_score", "restricted_mean_latency_seconds")):
        raise ValueError(f"Age metrics disagree with task outcomes: {tag}")
    return age


def load_directory(directory):
    root = Path(directory).resolve()
    if not (root / "summary.json").is_file():
        raise ValueError(f"Completed summary.json required: {root}")
    manifest = read_json(root / "summary.json")
    run = read_json(root / "run.json")
    if any(manifest.get(key) != value for key, value in run.items()):
        raise ValueError(f"Summary/run manifest mismatch: {root}")
    if manifest["delivery_horizons_seconds"] != HORIZONS:
        raise ValueError(f"Expected full 30/60/120/240 minute horizons: {root}")
    methods = list(manifest["args"]["methods"]) + list(manifest.get("checkpoint_sha256", {}))
    cases = {entry["case"]["id"]: entry for entry in manifest["cases"]}
    if not cases or len(cases) != len(manifest["cases"]) or not methods or len(methods) != len(set(methods)):
        raise ValueError(f"Duplicate/empty manifest cases or methods: {root}")
    expected = {(case_id, method) for case_id in cases for method in methods}
    filenames = manifest["records"]
    if len(filenames) != len(expected) or len(set(filenames)) != len(filenames):
        raise ValueError(f"Incomplete/duplicate record list: {root}")
    records, references = {}, {}
    for name in filenames:
        path = (root / name).resolve()
        if path.parent != root or path.suffix != ".json":
            raise ValueError(f"Invalid record path: {name}")
        record = read_json(path)
        pair = (record["case"]["id"], record["method"])
        if pair not in expected or pair in records or name != f"{pair[0]}_{pair[1]}.json":
            raise ValueError(f"Unexpected/duplicate case-method record: {name}")
        validate_record(record, manifest, cases[pair[0]])
        evidence = context(record)
        if pair[0] in references and references[pair[0]] != evidence:
            raise ValueError(f"Unpaired task trace/configuration: {pair[0]}")
        references[pair[0]] = evidence
        records[pair] = record
    if records.keys() != expected:
        raise ValueError(f"Incomplete case-method matrix: {root}")
    return {"directory": str(root), "manifest": manifest, "methods": methods,
            "references": references, "records": records,
            "summary_sha256": hashlib.sha256((root / "summary.json").read_bytes()).hexdigest()}


def resource_metrics(record):
    metrics = record["metrics"]
    cfg = metrics["experiment"]["config"]
    duration = metrics["experiment"]["elapsed_sim_seconds"]
    satellites = set(range(cfg["num_sats"]))
    sources = set(cfg["task_source_sat_ids"] if cfg.get("task_source_sat_ids") is not None else satellites)
    if not sources <= satellites:
        raise ValueError("Source IDs outside satellite set")
    busy = {int(key): value for key, value in metrics["resource"]["compute_busy_time"].items()}
    if not busy.keys() <= satellites or any(not math.isfinite(value) or value < 0 or value > duration + 1e-6
                                           for value in busy.values()):
        raise ValueError("Invalid satellite busy-time accounting")
    ground_busy = metrics["resource"].get("ground_compute_busy_time")
    ground_ids = {station["id"] for station in cfg["ground_stations"]}
    if ground_busy is not None and (not ground_busy.keys() <= ground_ids or any(
            not math.isfinite(value) or value < 0 or value > duration + 1e-6 for value in ground_busy.values())):
        raise ValueError("Invalid ground busy-time accounting")
    sent = metrics["network"]["sent_mb"]
    if any(not math.isfinite(value) or value < 0 for value in sent.values()):
        raise ValueError("Invalid transmitted-byte accounting")
    outcomes = metrics["task_outcomes"]
    return {"source_compute_s": sum(value for sid, value in busy.items() if sid in sources),
            "non_source_compute_s": sum(value for sid, value in busy.items() if sid not in sources),
            "source_capacity_s": len(sources) * duration,
            "non_source_capacity_s": len(satellites - sources) * duration,
            "ground_compute_s": sum(ground_busy.values()) if ground_busy is not None else None,
            "ground_capacity_s": len(ground_ids) * duration,
            "isl_sent_mb": sum(value for key, value in sent.items() if key.startswith("isl:")),
            "ground_sent_mb": sum(value for key, value in sent.items() if key.startswith("ground:")),
            "other_sent_mb": sum(value for key, value in sent.items()
                                 if not key.startswith(("isl:", "ground:"))),
            **{key: sum(row[key] for row in outcomes) if all(key in row for row in outcomes) else None
               for key in ("raw_relay_hops", "result_relay_hops")}}


def aggregate(records, method):
    resources = [resource_metrics(row) for row in records]
    resource = {key: sum(row[key] for row in resources) if all(row[key] is not None for row in resources) else None
                for key in RESOURCE_SUMS}
    for group in ("source", "non_source", "ground"):
        numerator, denominator = resource[f"{group}_compute_s"], resource[f"{group}_capacity_s"]
        resource[f"{group}_utilization"] = numerator / denominator if numerator is not None and denominator else None
    total_busy = resource["source_compute_s"] + resource["non_source_compute_s"]
    resource["fleet_utilization"] = total_busy / (resource["source_capacity_s"] + resource["non_source_capacity_s"])
    return {"method": method, "cases": len(records),
            "delivery_curve": [{"horizon_seconds": horizon,
                                "delivery_rate": statistics.mean(row["task_age"]["delivery_curve"][i]["delivery_rate"]
                                                                 for row in records)}
                               for i, horizon in enumerate(HORIZONS)],
            **{key: statistics.mean(row["task_age"][key] for row in records)
               for key in ("delivery_score", "restricted_mean_latency_seconds")},
            **{key: sum(row["metrics"]["overall"][key] for row in records)
               for key in ("completed_tasks", "total_tasks")},
            "resource": resource}


def paired_comparisons(records, methods):
    result = []
    for reference in ("local", "cgs_compute_adapted"):
        if reference not in methods:
            continue
        baseline = {row["case"]["id"]: row for row in records if row["method"] == reference}
        for method in methods:
            if method == reference:
                continue
            differences = [{"case": row["case"]["id"],
                            "score_difference": row["task_age"]["delivery_score"] -
                            baseline[row["case"]["id"]]["task_age"]["delivery_score"]}
                           for row in records if row["method"] == method]
            values = [row["score_difference"] for row in differences]
            result.append({"reference": reference, "method": method,
                           "mean_score_difference": statistics.mean(values),
                           "wins": sum(value > 1e-12 for value in values),
                           "ties": sum(abs(value) <= 1e-12 for value in values),
                           "losses": sum(value < -1e-12 for value in values), "cases": differences})
    return result


def build_panel(label, directories):
    loaded = [load_directory(path) for path in directories]
    first = loaded[0]
    records, methods = {}, []
    for panel in loaded:
        if panel["references"] != first["references"]:
            raise ValueError(f"Cannot merge {label}: case set/configuration/source/task trace differs")
        if panel["manifest"]["args"]["split"] != first["manifest"]["args"]["split"]:
            raise ValueError(f"Cannot merge {label}: split differs")
        if records.keys() & panel["records"].keys():
            raise ValueError(f"Cannot merge {label}: duplicate case-method records")
        records.update(panel["records"])
        methods.extend(panel["methods"])
    ordered = [records[key] for key in sorted(records)]
    groups = sorted({condition(row["case"]["id"]) for row in ordered})
    def summarize(rows):
        return [aggregate([row for row in rows if row["method"] == method], method) for method in methods]
    return {"label": label, "split": first["manifest"]["args"]["split"],
            "directories": [{key: panel[key] for key in ("directory", "summary_sha256")} for panel in loaded],
            "source_sha256": first["manifest"]["source_sha256"], "horizons_seconds": HORIZONS,
            "case_count": len(first["references"]), "record_count": len(records), "methods": methods,
            "observation_hours": sorted({r["metrics"]["experiment"]["elapsed_sim_seconds"] / 3600 for r in ordered}),
            "checks": {"complete_case_method_matrix": True, "paired_context_and_task_definitions": True,
                       "age_metrics_recomputed_from_outcomes": True, "cross_panel_pooling": False},
            "aggregate": summarize(ordered),
            "conditions": [{"condition": group,
                            "aggregate": summarize([row for row in ordered if condition(row["case"]["id"]) == group])}
                           for group in groups],
            "comparisons": paired_comparisons(ordered, methods),
            "cases": [{"case": row["case"], "method": row["method"], "task_age": row["task_age"],
                       "completed_tasks": row["metrics"]["overall"]["completed_tasks"],
                       "total_tasks": row["metrics"]["overall"]["total_tasks"], "resource": resource_metrics(row),
                       "source_sha256": row["metrics"]["experiment"]["source_sha256"],
                       "task_trace_sha256": row["metrics"]["experiment"]["task_trace_sha256"],
                       "checkpoint_sha256": row.get("checkpoint_sha256")}
                      for row in ordered]}


def compact_validation(validation):
    return None if validation is None else {key: value for key, value in validation.items() if key != "cases"}


def training_report(directory):
    root = Path(directory).resolve()
    run = read_json(root / "run.json") if (root / "run.json").exists() else {}
    warm = read_json(root / "warmstart.json") if (root / "warmstart.json").exists() else {}
    records = []
    if (root / "train_log.jsonl").exists():
        with (root / "train_log.jsonl").open(encoding="utf-8") as stream:
            for line in stream:
                record = json.loads(line)
                records.append({key: record[key] for key in (
                    "episode", "scenario", "ep_reward", "task_age", "action_counts", "decision_agent_steps",
                    "active_agent_entropy_mean", "update_diagnostics", "elapsed_s", "is_best") if key in record})
                records[-1]["validation"] = compact_validation(record.get("validation"))
    episodes = [record["episode"] for record in records]
    if episodes != list(range(1, len(episodes) + 1)):
        raise ValueError("Training report requires a contiguous, non-resumed log starting at episode 1")
    validations = ([{"episode": 0, **compact_validation(warm["validation"])}] if warm.get("validation") else [])
    validations.extend({"episode": row["episode"], **row["validation"]} for row in records if row["validation"])
    selected = 0 if warm.get("validation") else None
    for row in records:
        if row.get("is_best"):
            selected = row["episode"]
    args = run.get("training_args", {})
    counts = Counter()
    bc_episodes = []
    for row in warm.get("episodes", []):
        counts.update(row.get("action_counts", {}))
        actions = row.get("action_counts", {})
        total = sum(actions.values())
        bc_episodes.append({"episode": len(bc_episodes) + 1, "seed": row.get("seed"),
                            "samples": row.get("samples", 0), "action_counts": actions,
                            "tile_labels": total,
                            "wait_fraction": actions.get("0", actions.get(0, 0)) / total if total else None,
                            "teacher_outcomes": row.get("metrics", {}).get("overall")})
    total_samples = sum(row["samples"] for row in bc_episodes)
    for row in bc_episodes:
        row["sample_fraction"] = row["samples"] / total_samples if total_samples else None
    fitted = [{key: fit[key] for key in ("samples", "loss", "agreement", "class_recall") if key in fit}
              for fit in warm.get("pooled_fits", [])]
    diagnostics = [diagnostic for row in records for diagnostic in row.get("update_diagnostics", [])]
    rows = sum(row.get("rows", 0) for row in diagnostics)
    active = sum(row.get("active_rows", 0) for row in diagnostics)
    status = "complete" if args.get("episodes") == len(records) and records else "in_progress_or_unavailable"
    is_bc = bool(warm.get("episodes"))
    selection_note = ("Best episode 0 is BC/warmstart; no PPO improvement was selected."
                      if selected == 0 and is_bc else
                      "Best episode inferred from validation selection in the training log; this alone is not test improvement.")
    return {"directory": str(root), "status": status, "source_sha256": run.get("source_sha256"),
            "training_args": args, "parameter_count": run.get("parameter_count"),
            "bc": {"episodes": len(warm.get("episodes", [])),
                   "samples": total_samples, "tile_labels": sum(counts.values()),
                   "teacher_action_counts": dict(counts), "pooled_fits": fitted,
                   "per_episode": bc_episodes},
            "selected_episode_from_log": selected, "selection_note": selection_note,
            "selected_checkpoint_binary_read": False, "validations": validations, "episodes": records,
            "ppo_diagnostics": {"rows": rows, "active_rows": active,
                                "active_fraction": active / rows if rows else None,
                                "kl_stop_count": sum(bool(row.get("kl_early_stop")) for row in diagnostics)}}


def number(value, digits=3, scale=1):
    return "n/a" if value is None else f"{scale * value:.{digits}f}"


def result_table(rows):
    lines = ["| Method | F30 | F60 | F120 | F240 | J | Restricted latency (s) | Completed / total |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        values = [number(item["delivery_rate"], scale=100) + "%" for item in row["delivery_curve"]]
        lines.append(f"| {row['method']} | " + " | ".join(values) +
                     f" | {100 * row['delivery_score']:.3f}% | {row['restricted_mean_latency_seconds']:.2f}"
                     f" | {row['completed_tasks']} / {row['total_tasks']} |")
    return lines


def markdown(report):
    lines = ["# Fixed-Overlap Study", "", "Each panel is a separate paired experiment; no scores are pooled across panels.",
             "F30/F60/F120/F240 and J use equal case weights; J is the equal mean of the four age horizons. "
             "Restricted latency retains every arrived task, assigning undelivered tasks 14,400 seconds. "
             "Completed/total counts are pooled within each panel only.", "",
             "Busy time and sent MB are sums across cases. Utilization is busy seconds divided by total available "
             "node-seconds over the full simulation (including drain), not only the arrival interval. "
             "Source/non-source identifies imaging roles, not the originating satellite of each individual task. "
             "ISL bytes include repeated-hop and result traffic; they are not a direct measure of raw-input offload. "
             "The raw-relay counter excludes ordinary distributed OFFLOAD transfers; zero raw_relay_hops does not mean "
             "zero raw-input offload. Executor-level traces are needed for exact task-locality. "
             "These are fixed-time-step measurements. Stable age-delivery scores do not establish convergence of "
             "short-task latency or communication volume; numerical sensitivity must be reported separately."]
    for panel in report["panels"]:
        lines += ["", f"## {panel['label']}", "", f"Split: {panel['split']}; {panel['case_count']} cases, "
                  f"{panel['record_count']} records. Full observation: {panel['observation_hours']} hours. "
                  f"Source SHA256: `{panel['source_sha256']}`.", "", *result_table(panel["aggregate"]), "",
                  "### Compute and Communication", "",
                  "| Method | Source busy (s) | Non-source busy (s) | Ground busy (s) | Source use (%) | Non-source use (%) | Fleet use (%) | ISL sent (MB) | Ground sent (MB) | Raw / result relay hops |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for row in panel["aggregate"]:
            r = row["resource"]
            values = [number(r[key]) for key in ("source_compute_s", "non_source_compute_s", "ground_compute_s")]
            values += [number(r[key], digits=6, scale=100) for key in ("source_utilization", "non_source_utilization", "fleet_utilization")]
            values += [number(r[key], digits=6) for key in ("isl_sent_mb", "ground_sent_mb")]
            lines.append(f"| {row['method']} | " + " | ".join(values) +
                         f" | {number(r['raw_relay_hops'], 0)} / {number(r['result_relay_hops'], 0)} |")
        lines += ["", "### Paired Differences", "", "| Method | Reference | Mean J difference (pp) | Wins / ties / losses |",
                  "|---|---|---:|---:|"]
        for row in panel["comparisons"]:
            lines.append(f"| {row['method']} | {row['reference']} | {100 * row['mean_score_difference']:.4f}"
                         f" | {row['wins']} / {row['ties']} / {row['losses']} |")
        for group in panel["conditions"]:
            lines += ["", f"### {group['condition']}", "", *result_table(group["aggregate"])]
    if report.get("training"):
        training = report["training"]
        lines += ["", "## Training Diagnostics", "", f"Status: {training['status']}. {training['selection_note']}",
                  "Selection is inferred from JSON logs; the checkpoint binary is not loaded by this reporting script.",
                  f"BC episodes: {training['bc']['episodes']}; labelled agent rows: {training['bc']['samples']}; "
                  f"teacher action counts: `{training['bc']['teacher_action_counts']}`.", "",
                  "| BC fit | Samples | Loss | Agreement | Class recall |", "|---|---:|---:|---:|---|"]
        for i, fit in enumerate(training["bc"]["pooled_fits"], 1):
            lines.append(f"| {i} | {fit.get('samples')} | {number(fit.get('loss'), 6)} | "
                         f"{number(fit.get('agreement'), 4)} | `{fit.get('class_recall', {})}` |")
        lines += ["", "| BC episode | Seed | Agent rows | Row share (%) | Tile labels | WAIT labels (%) |",
                  "|---|---:|---:|---:|---:|---:|"]
        for row in training["bc"]["per_episode"]:
            lines.append(f"| {row['episode']} | {row['seed']} | {row['samples']} | "
                         f"{number(row['sample_fraction'], scale=100)} | {row['tile_labels']} | "
                         f"{number(row['wait_fraction'], scale=100)} |")
        lines += ["", "Repeated WAIT decisions are distinct training samples. Agent-row counts and tile-label "
                  "counts use different denominators; class recall and per-case evaluation are needed when WAIT dominates."]
        lines += ["", "| Validation episode | J | Restricted latency (s) |", "|---|---:|---:|"]
        for row in training["validations"]:
            lines.append(f"| {row['episode']} | {number(row.get('delivery_score'), scale=100)}% | "
                         f"{number(row.get('restricted_mean_latency_seconds'))} |")
        lines += ["", f"PPO active agent rows: {training['ppo_diagnostics']['active_rows']} / "
                  f"{training['ppo_diagnostics']['rows']}; KL early stops: {training['ppo_diagnostics']['kl_stop_count']}.",
                  "Training episodes use different sampled scenarios, so their scores are not a paired improvement curve. "
                  "BC fit agreement is measured on demonstrations, not held-out policy performance. "
                  "One training seed does not establish robustness."]
    return "\n".join(lines) + "\n"


def plot_report(report, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
    artifacts = []
    def save(fig, stem):
        for suffix in ("png", "pdf"):
            name = f"{stem}.{suffix}"
            fig.savefig(out / name, dpi=180, bbox_inches="tight")
            artifacts.append(name)
        plt.close(fig)
    for i, panel in enumerate(report["panels"], 1):
        stem = f"panel_{i:02d}"
        rows = panel["aggregate"]
        names = [LABELS.get(row["method"], row["method"]) for row in rows]
        colors = plt.get_cmap("tab20")(np.linspace(0, 1, len(rows)))
        fig, ax = plt.subplots(figsize=(9, 4.8), layout="constrained")
        for row, name, color in zip(rows, names, colors):
            ax.plot([30, 60, 120, 240], [100 * p["delivery_rate"] for p in row["delivery_curve"]],
                    marker="o", label=name, color=color)
        ax.set(xlabel="Task age horizon (minutes)", ylabel="Complete-task delivery (%)", ylim=(-2, 102),
               xticks=[30, 60, 120, 240], title=f"Panel {i}: {panel['split']} (equal case weights)")
        ax.grid(alpha=.2)
        ax.legend(loc="center left", bbox_to_anchor=(1, .5), fontsize=8)
        save(fig, f"{stem}_delivery")
        groups = panel["conditions"]
        data = np.array([[100 * next(row["delivery_score"] for row in group["aggregate"]
                                    if row["method"] == method) for group in groups] for method in panel["methods"]])
        fig, ax = plt.subplots(figsize=(max(8, 1.4 * len(groups)), max(4, .43 * len(rows) + 1.5)), layout="constrained")
        im = ax.imshow(data, cmap="YlGnBu", vmin=0, vmax=100, aspect="auto")
        ax.set(yticks=range(len(names)), yticklabels=names, xticks=range(len(groups)),
               xticklabels=[group["condition"].replace("_", "\n") for group in groups],
               title=f"Panel {i}: age delivery score by condition (%)")
        for y in range(data.shape[0]):
            for x in range(data.shape[1]):
                ax.text(x, y, f"{data[y, x]:.2f}", ha="center", va="center", color="white" if data[y, x] > 55 else "black", fontsize=8)
        fig.colorbar(im, ax=ax, shrink=.8, label="Age delivery score (%)")
        save(fig, f"{stem}_conditions")
        fig, axes = plt.subplots(2, 2, figsize=(13, max(8, .45 * len(rows) + 4)), layout="constrained")
        y = np.arange(len(rows))
        r = [row["resource"] for row in rows]
        for ax, keys, title, xlabel in (
            (axes[0, 0], ["source_compute_s", "non_source_compute_s", "ground_compute_s"], "Compute service (all cases)", "Busy seconds"),
            (axes[1, 0], ["isl_sent_mb", "ground_sent_mb", "other_sent_mb"], "Actual link transmissions (all cases)", "Sent MB, symlog scale")):
            left = np.zeros(len(rows))
            for key, color in zip(keys, ["#287f68", "#427cb4", "#bf6e32"]):
                values = np.array([row[key] if row[key] is not None else np.nan for row in r])
                ax.barh(y, values, left=left, label=key.replace("_", " "), color=color)
                left += np.nan_to_num(values)
            ax.set(yticks=y, yticklabels=names, title=title, xlabel=xlabel)
            ax.invert_yaxis()
            ax.legend(fontsize=7)
        axes[1, 0].set_xscale("symlog", linthresh=.01)
        for offset, key, color in ((-.2, "source_utilization", "#287f68"), (.2, "non_source_utilization", "#427cb4")):
            axes[0, 1].barh(y + offset, [100 * row[key] if row[key] is not None else np.nan for row in r],
                           height=.38, label=key.replace("_", " "), color=color)
        axes[0, 1].set(yticks=y, yticklabels=names, xlabel="Busy / full node-time capacity (%)",
                       title=f"Full observation denominator: {panel['observation_hours']} hours per case")
        axes[0, 1].invert_yaxis()
        axes[0, 1].legend(fontsize=7)
        for offset, key, color in ((-.2, "raw_relay_hops", "#bf6e32"), (.2, "result_relay_hops", "#427cb4")):
            axes[1, 1].barh(y + offset, [row[key] if row[key] is not None else np.nan for row in r],
                           height=.38, label=key.replace("_", " "), color=color)
        axes[1, 1].set(yticks=y, yticklabels=names, xlabel="Summed packet-hop counts; ordinary OFFLOAD excluded from raw", title="Recorded relay-path counters")
        axes[1, 1].invert_yaxis()
        axes[1, 1].legend(fontsize=7)
        save(fig, f"{stem}_resources")
    training = report.get("training")
    if training and (training["episodes"] or training["validations"]):
        fig, axes = plt.subplots(1, 2, figsize=(10, 4), layout="constrained")
        valid = [row for row in training["validations"] if row.get("delivery_score") is not None]
        sampled = [row for row in training["episodes"] if (row.get("task_age") or {}).get("delivery_score") is not None]
        axes[0].plot([row["episode"] for row in sampled], [100 * row["task_age"]["delivery_score"] for row in sampled],
                     "o--", label="Training (different scenarios)", alpha=.6)
        axes[0].plot([row["episode"] for row in valid], [100 * row["delivery_score"] for row in valid],
                     "o-", label="Fixed validation", color="#287f68")
        axes[0].set(xlabel="PPO episode (0 = warmstart)", ylabel="Age delivery score (%)", ylim=(-2, 102),
                    title=f"Selected episode from log: {training['selected_episode_from_log']}")
        axes[0].legend(fontsize=8)
        diagnostic = [(row["episode"], item) for row in training["episodes"] for item in row.get("update_diagnostics", [])]
        axes[1].plot([episode for episode, _ in diagnostic], [item.get("joint_kl_mean", 0) for _, item in diagnostic], "o-")
        threshold = training["training_args"].get("target_kl")
        if threshold:
            axes[1].axhline(threshold, color="#bf6e32", linestyle="--", label="KL stop threshold")
            axes[1].legend(fontsize=8)
        axes[1].set(xlabel="PPO episode", ylabel="Mean active-row joint KL", title="Policy update diagnostic")
        save(fig, "training_diagnostics")
    return artifacts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", action="append", required=True, metavar="LABEL=DIR",
                        help="Repeat a label to merge strictly paired, disjoint method sets")
    parser.add_argument("--training-dir")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args(argv)
    grouped = defaultdict(list)
    for item in args.panel:
        if "=" not in item or not all(item.split("=", 1)):
            parser.error("--panel must be LABEL=DIR")
        label, directory = item.split("=", 1)
        grouped[label].append(directory)
    report = {"scope": "Separate paired controlled-proxy panels; no flight hardware or detection generalization claim",
              "panels": [build_panel(label, paths) for label, paths in grouped.items()],
              "training": training_report(args.training_dir) if args.training_dir else None}
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    report["figures"] = plot_report(report, out)
    (out / "summary.json").write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    (out / "summary.md").write_text(markdown(report), encoding="utf-8")
    print(json.dumps({"output": str(out.resolve()), "panels": len(report["panels"]), "figures": report["figures"]}))


if __name__ == "__main__":
    main()
