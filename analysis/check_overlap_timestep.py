"""固定物理到达轨迹与观测周期，比较重叠窗口负载的时间步敏感性。"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, replace
import gzip
import hashlib
import json
import math
import multiprocessing
from pathlib import Path
import random
import sys
import time
from types import SimpleNamespace

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import run_config
from sim.config import load_config
from sim.evaluation import paired_task_metrics, task_age_delivery_curve, task_age_score
from sim.marl.scenarios import ScenarioDistribution
from sim.reproducibility import runtime_metadata
from sim.workload import ImageTiling


POLICIES = ("ground_compute", "local", "greedy", "cgs_compute_adapted")
HORIZONS = [1800.0, 3600.0, 7200.0, 14400.0]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def steps(seconds, dt):
    count = seconds / dt
    if not math.isfinite(count) or count < 0 or not math.isclose(count, round(count), abs_tol=1e-9):
        raise ValueError(f"Physical period {seconds} s is not divisible by dt={dt}")
    return round(count)


def rescale_config(cfg, dt):
    if not math.isfinite(dt) or dt <= 0:
        raise ValueError("dt must be finite and positive")
    if cfg.image_tiling is None or cfg.compute_service_mode != "work_conserving":
        raise ValueError("Requires image_tiling and work_conserving compute service")
    expected = {"decision_interval_steps": 1, "topology_update_steps": 10,
                "ground_visibility_update_steps": 10}
    for name, seconds in expected.items():
        if not math.isclose(getattr(cfg, name) * cfg.dt, seconds):
            raise ValueError(f"Base {name} must represent {seconds} physical seconds")
    if not math.isclose(cfg.topology.get("contact_scan_s", 30), 30):
        raise ValueError("Contact scan must stay at 30 physical seconds")
    if not math.isclose(cfg.topology["bandwidth_period"] * cfg.dt, 600):
        raise ValueError("Bandwidth period must represent 600 physical seconds")
    if cfg.topology.get("bandwidth_noise", 0) != 0:
        raise ValueError("This deterministic numerical protocol requires zero bandwidth noise")
    if cfg.task_arrival_end_step is None:
        raise ValueError("Explicit arrival cutoff is required for complete task follow-up")
    topology = {**cfg.topology, "bandwidth_period": steps(600, dt)}
    return replace(cfg, dt=dt, sim_steps=steps(cfg.sim_steps * cfg.dt, dt),
                   task_arrival_end_step=steps(cfg.task_arrival_end_step * cfg.dt, dt),
                   task_arrival_rate=cfg.task_arrival_rate / cfg.dt * dt,
                   deadline_steps=steps(max(0, cfg.deadline_steps) * cfg.dt, dt),
                   topology=topology, tile_lifecycle_log="",
                   **{name: steps(seconds, dt) for name, seconds in expected.items()})


def arrival_events(cfg):
    """复用新几何模式原有的一秒 Poisson 与独立源星 RNG 序列。"""
    canonical = rescale_config(cfg, 1.0)
    rng, sources_rng = np.random.default_rng(canonical.seed), random.Random(canonical.seed)
    sources = canonical.task_source_sat_ids or list(range(canonical.num_sats))
    events = []
    for second in range(canonical.task_arrival_end_step):
        count = int(rng.poisson(canonical.task_arrival_rate))
        for _ in range(count):
            source_index = sources_rng.randrange(len(sources))
            events.append({"task_id": f"task_{len(events)}", "release_seconds": float(second),
                           "source_index": source_index, "source_sat_id": sources[source_index]})
    return events


def expected_task_trace(cfg, events):
    workload = ImageTiling.from_dict(cfg.image_tiling)
    tiles = [{"data_size_mb": tile.data_size_mb, "compute_cost": tile.compute_cost,
              "result_size_mb": tile.result_size_mb,
              "windows_xyxy": [list(box) for box in tile.windows_xyxy]} for tile in workload.tiles()]
    return [{"task_id": row["task_id"], "source_sat_id": row["source_sat_id"],
             "release_seconds": row["release_seconds"],
             "deadline_seconds": row["release_seconds"] + cfg.deadline_steps * cfg.dt
             if cfg.deadline_steps > 0 else None,
             "image_size_mb": workload.image_size_mb, "tiles": tiles} for row in events]


def actual_task_trace(env):
    rows = []
    for task in env.tasks.values():
        tiles = [env.tiles[tid] for tid in task.tile_ids]
        rows.append({"task_id": task.task_id, "source_sat_id": task.source_sat_id,
                     "release_seconds": task.release_time * env.cfg.dt,
                     "deadline_seconds": task.deadline * env.cfg.dt if task.deadline is not None else None,
                     "image_size_mb": task.image_size_mb,
                     "tiles": [{"data_size_mb": tile.data_size_mb, "compute_cost": tile.compute_cost,
                                "result_size_mb": tile.result_size_mb,
                                "windows_xyxy": [list(box) for box in tile.windows_xyxy]} for tile in tiles]})
    return rows


class ReplayObserver:
    def __init__(self, cfg, events):
        self.cfg, self.events = cfg, events
        self.expected = expected_task_trace(cfg, events)
        self.task_trace = None
        self.context = []
        self.executed_seconds = set()

    def on_start(self, env):
        if env.image_workload is None:
            raise ValueError("Legacy random tile sizes cannot use this replay mechanism")
        grouped = {}
        for row in self.events:
            grouped.setdefault(steps(row["release_seconds"], env.cfg.dt), []).append(row)
        original = env._task_arrivals

        def replay(step):
            rows = grouped.get(step)
            if not rows:
                return
            if step in self.executed_seconds:
                raise AssertionError("Arrival step executed twice")
            self.executed_seconds.add(step)
            sources = iter(row["source_index"] for row in rows)
            saved_rng, saved_py_rng = env.rng, env.py_rng
            before = len(env.tasks)

            def choose_source(count):
                index = next(sources)
                if count != len(env.task_sources) or not 0 <= index < count:
                    raise AssertionError("Source inventory changed during replay")
                return index

            # 仅在原任务构造函数内部替换随机接口；异常也恢复，不干预调度器。
            env.rng = SimpleNamespace(poisson=lambda _rate: len(rows))
            env.py_rng = SimpleNamespace(randrange=choose_source)
            try:
                original(step)
            finally:
                env.rng, env.py_rng = saved_rng, saved_py_rng
            if len(env.tasks) - before != len(rows) or next(sources, None) is not None:
                raise AssertionError("Replay did not construct exactly the prescribed tasks")

        env._task_arrivals = replay

    def on_step(self, env, step_idx, actions):
        if step_idx % self.cfg.topology_update_steps == 0:
            self.context.append({
                "seconds": step_idx * self.cfg.dt,
                "links": {key: [link.up, link.bandwidth_mbps] for key, link in sorted(env._links_cache.items())},
                "source_visible_stations": {str(sid): list(env.vis_cache.visible_gs_by_sat.get(sid, []))
                                            for sid in env.task_sources},
            })
        if step_idx == self.cfg.sim_steps - 1:
            self.task_trace = actual_task_trace(env)
            if self.task_trace != self.expected:
                raise AssertionError("Physical task trace differs from the frozen replay trace")


def full_age_curve(outcomes, elapsed):
    # 保留每个实际交付事件的阶跃点，另附 30 s 网格，最大年龄仍统一 4 h。
    horizons = set(float(t) for t in range(30, int(max(HORIZONS)) + 1, 30))
    horizons.update(row["latency_seconds"] for row in outcomes
                    if row["status"] == "completed" and 0 < row["latency_seconds"] <= max(HORIZONS))
    curve = task_age_delivery_curve(outcomes, sorted(horizons), elapsed)
    zero = sum(row["status"] == "completed" and row["latency_seconds"] == 0 for row in outcomes)
    return [{"horizon_seconds": 0.0, "total_tasks": len(outcomes), "delivered_within_horizon": zero,
             "not_delivered_within_horizon": len(outcomes) - zero,
             "delivery_rate": zero / len(outcomes) if outcomes else None}] + curve


def throughput(result):
    elapsed = result["experiment"]["elapsed_sim_seconds"]
    sent = result["network"]["sent_mb"]
    return {"completed_tasks_per_hour": result["overall"]["completed_tasks"] * 3600 / elapsed,
            "completed_tiles_per_hour": result["overall"]["completed_tiles"] * 3600 / elapsed,
            "ground_sent_mb": sum(value for key, value in sent.items() if key.startswith("ground:")),
            "isl_sent_mb": sum(value for key, value in sent.items() if key.startswith("isl:")),
            "ground_actual_mbps": 8 * sum(value for key, value in sent.items() if key.startswith("ground:")) / elapsed,
            "isl_actual_mbps": 8 * sum(value for key, value in sent.items() if key.startswith("isl:")) / elapsed}


def relative_deviation(candidate, reference):
    return candidate / reference - 1 if reference != 0 else None


def compare(reference, candidate):
    ref, cand = reference["metrics"], candidate["metrics"]
    pairing = paired_task_metrics(ref["task_outcomes"], cand["task_outcomes"])
    ref_tasks = {row["task_id"]: row for row in ref["task_outcomes"]}
    common = []
    for row in cand["task_outcomes"]:
        previous = ref_tasks[row["task_id"]]
        if row["status"] == previous["status"] == "completed":
            common.append({"task_id": row["task_id"], "reference_latency_seconds": previous["latency_seconds"],
                           "candidate_latency_seconds": row["latency_seconds"],
                           "candidate_minus_reference_seconds": row["latency_seconds"] - previous["latency_seconds"]})
    changes = {key: {"reference": reference["throughput"][key], "candidate": value,
                     "absolute_difference": value - reference["throughput"][key],
                     "relative_deviation": relative_deviation(value, reference["throughput"][key])}
               for key, value in candidate["throughput"].items()}
    return {"method": candidate["method"], "reference_dt": reference["dt"], "candidate_dt": candidate["dt"],
            "pairing": pairing, "common_completed_tasks": common, "throughput": changes,
            "delivery_score_difference": candidate["task_age"]["delivery_score"] - reference["task_age"]["delivery_score"],
            "restricted_latency_difference_seconds": candidate["task_age"]["restricted_mean_latency_seconds"] -
            reference["task_age"]["restricted_mean_latency_seconds"]}


def evaluate_run(cfg, case, policy, events, source_sha, context_path):
    if runtime_metadata()["source_sha256"] != source_sha:
        raise RuntimeError("Simulation source changed before numerical comparison")
    run_cfg = replace(cfg, pipeline_mode="ground_compute" if policy == "ground_compute" else "distributed")
    observer = ReplayObserver(run_cfg, events)
    started = time.monotonic()
    metrics = run_config(run_cfg, policy, observer=observer)
    if metrics["experiment"]["source_sha256"] != source_sha:
        raise RuntimeError("Simulation source changed during numerical comparison")
    assert observer.task_trace == expected_task_trace(cfg, events)
    if context_path:
        with gzip.open(context_path, "wt", encoding="utf-8") as handle:
            json.dump(observer.context, handle, allow_nan=False)
    return {"case": case, "method": policy, "dt": cfg.dt,
            "wall_seconds": time.monotonic() - started,
            "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "physical_task_trace_sha256": digest(observer.task_trace),
            "physical_link_context_sha256": digest(observer.context), "metrics": metrics,
            "task_age": task_age_score(metrics["task_outcomes"], HORIZONS,
                                       metrics["experiment"]["elapsed_sim_seconds"]),
            "complete_age_curve": full_age_curve(metrics["task_outcomes"],
                                                  metrics["experiment"]["elapsed_sim_seconds"]),
            "throughput": throughput(metrics),
            "file": f"{case['id']}_{policy}_dt{cfg.dt:g}.json"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="examples/overlap_proxy/config.json")
    parser.add_argument("--distribution", default="examples/overlap_proxy/distribution.json")
    parser.add_argument("--split", choices=("validation", "test"), default="validation")
    parser.add_argument("--case", default="validation_near_high_1")
    parser.add_argument("--dts", nargs="+", type=float, default=[1.0, 0.5, 0.25])
    parser.add_argument("--policies", nargs="+", choices=POLICIES, default=list(POLICIES))
    parser.add_argument("--workers", type=int, choices=(1, 2), default=1)
    parser.add_argument("--resume", action="store_true", help="Reuse only matching source/config/trace results")
    parser.add_argument("--output-dir", default="results/overlap_timestep_20261014")
    args = parser.parse_args()
    if len(set(args.dts)) != len(args.dts) or len(set(args.policies)) != len(args.policies):
        raise ValueError("Time steps and policies must be unique")
    distribution = ScenarioDistribution(args.distribution, load_config(args.config))
    matches = [(cfg, info) for cfg, info in distribution.cases(args.split) if info["id"] == args.case]
    if len(matches) != 1:
        raise ValueError(f"Expected one matching case: {args.case}")
    base, case = matches[0]
    configs = {dt: rescale_config(base, dt) for dt in args.dts}
    if (base.sim_steps - base.task_arrival_end_step) * base.dt < max(HORIZONS):
        raise ValueError("The protocol must follow every task for at least four hours")
    events = arrival_events(base)
    if not events:
        raise ValueError("Cannot compare an empty arrival trace")
    expected = expected_task_trace(base, events)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=args.resume)
    source = runtime_metadata()
    manifest = {"args": vars(args), "case": case, **source,
                "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "distribution_sha256": distribution.sha256, "base_config": asdict(base),
                "physical_task_trace_sha256": digest(expected), "arrival_events_sha256": digest(events),
                "planned_runs": len(configs) * len(args.policies), "reference_dt": min(args.dts),
                "physical_periods_seconds": {"arrival_sampling": 1, "policy_decision": 1,
                                             "topology": 10, "ground_visibility": 10,
                                             "contact_scan": 30, "bandwidth_sinusoid": 600},
                "delivery_horizons_seconds": HORIZONS,
                "scope": "one selected development case; timestep sensitivity, not statistical robustness",
                "limits": ["Finest dt is a numerical reference, not an exact continuous-time solution.",
                           "Ranking stability does not establish accuracy of individual service times.",
                           "Compute timestamps, transfer completion, ground arbitration and fixed merge delay remain quantized.",
                           "Actual link throughput counts transmitted bytes including relay hops, not unique useful payload.",
                           "Network nominal capacity counters are not used as visibility-conditioned capacity evidence.",
                           "Common-completion latency retains success-subset bias; complete-population age metrics are primary.",
                           "Native task_trace_sha256 includes step indices; cross-dt equality uses physical_task_trace_sha256."]}
    if args.resume:
        old = json.loads((output / "run.json").read_text())
        for key in ("source_sha256", "distribution_sha256", "physical_task_trace_sha256", "arrival_events_sha256"):
            if old[key] != manifest[key]:
                raise RuntimeError(f"Cannot resume: {key} changed")
        for key in ("case", "base_config", "reference_dt", "planned_runs", "delivery_horizons_seconds"):
            if digest(old[key]) != digest(manifest[key]):
                raise RuntimeError(f"Cannot resume: {key} changed")
        for key in ("dts", "policies"):
            if old["args"][key] != manifest["args"][key]:
                raise RuntimeError(f"Cannot resume: {key} changed")
        history = old.get("runner_history", [old["runner_sha256"]])
        manifest["runner_history"] = list(dict.fromkeys(history + [manifest["runner_sha256"]]))
    else:
        dump(output / "arrivals.json", events)
        dump(output / "task_trace.json", expected)
    dump(output / "run.json", manifest)
    records, context_hash, tle_hash = [], None, None
    started = time.monotonic()
    pending = []
    for dt, cfg in configs.items():
        for policy in args.policies:
            path = output / f"{args.case}_{policy}_dt{dt:g}.json"
            if args.resume and path.exists():
                record = json.loads(path.read_text())
                expected_cfg = replace(cfg, pipeline_mode="ground_compute" if policy == "ground_compute" else "distributed")
                if (record["method"] != policy or record["dt"] != dt or
                        record["physical_task_trace_sha256"] != manifest["physical_task_trace_sha256"] or
                        record["metrics"]["experiment"]["source_sha256"] != source["source_sha256"] or
                        digest(record["metrics"]["experiment"]["config"]) != digest(asdict(expected_cfg))):
                    raise RuntimeError(f"Cannot resume mismatched record: {path}")
                records.append(record)
            else:
                pending.append((cfg, policy))
    if records:
        context_hash = records[0]["physical_link_context_sha256"]
        tle_hash = records[0]["metrics"]["experiment"]["tle_sha256"]
    with ProcessPoolExecutor(max_workers=args.workers,
                             mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = [pool.submit(evaluate_run, cfg, case, policy, events, source["source_sha256"],
                               str(output / "physical_link_context.json.gz") if index == 0 and not records else None)
                   for index, (cfg, policy) in enumerate(pending)]
        for future in as_completed(futures):
            record = future.result()
            if runtime_metadata()["source_sha256"] != source["source_sha256"]:
                raise RuntimeError("Simulation source changed during numerical comparison")
            context, tle = record["physical_link_context_sha256"], record["metrics"]["experiment"]["tle_sha256"]
            if context_hash is None:
                context_hash, tle_hash = context, tle
            assert context == context_hash and tle == tle_hash, "Orbit or physical link context changed"
            assert record["physical_task_trace_sha256"] == manifest["physical_task_trace_sha256"]
            dump(output / record["file"], record)
            records.append(record)
            print(json.dumps({"finished": len(records), "total": manifest["planned_runs"], "dt": record["dt"],
                              "method": record["method"], "score": record["task_age"]["delivery_score"],
                              "seconds": record["wall_seconds"]}), flush=True)
    for record in records:
        assert record["physical_link_context_sha256"] == context_hash
        assert record["metrics"]["experiment"]["tle_sha256"] == tle_hash
    records.sort(key=lambda row: (args.dts.index(row["dt"]), args.policies.index(row["method"])))
    finest = {row["method"]: row for row in records if row["dt"] == min(args.dts)}
    comparisons = [compare(finest[row["method"]], row) for row in records]
    ratios = []
    if "ground_compute" in args.policies:
        for row in records:
            ground = next(other for other in records if other["dt"] == row["dt"] and other["method"] == "ground_compute")
            value = row["throughput"]["completed_tasks_per_hour"]
            denom = ground["throughput"]["completed_tasks_per_hour"]
            fine_value = finest[row["method"]]["throughput"]["completed_tasks_per_hour"]
            fine_denom = finest["ground_compute"]["throughput"]["completed_tasks_per_hour"]
            ratio, reference = (value / denom if denom else None), (fine_value / fine_denom if fine_denom else None)
            ratios.append({"method": row["method"], "dt": row["dt"], "task_throughput_over_ground": ratio,
                           "finest_dt_ratio": reference,
                           "ratio_relative_deviation": relative_deviation(ratio, reference)
                           if ratio is not None and reference is not None else None})
    summary = {**manifest, "wall_seconds": time.monotonic() - started,
               "all_physical_task_traces_equal": True, "all_physical_link_contexts_equal": True,
               "physical_link_context_sha256": context_hash, "tle_sha256": tle_hash,
               "records": [row["file"] for row in records], "comparisons": comparisons,
               "task_throughput_ratios": ratios}
    dump(output / "summary.json", summary)
    lines = ["# Overlap Timestep Sensitivity", "", manifest["scope"], "",
             "| Method | dt (s) | Age score | Completed / arrived | Restricted latency (s) | Actual ground Mbps |",
             "|---|---:|---:|---:|---:|---:|"]
    for row in records:
        overall = row["metrics"]["overall"]
        lines.append(f"| {row['method']} | {row['dt']:g} | {row['task_age']['delivery_score']:.6f} | "
                     f"{overall['completed_tasks']} / {overall['total_tasks']} | "
                     f"{row['task_age']['restricted_mean_latency_seconds']:.3f} | "
                     f"{row['throughput']['ground_actual_mbps']:.6f} |")
    lines += ["", "Every run has the identical frozen physical task trace and sampled physical link context.", ""]
    lines.extend(f"- {note}" for note in manifest["limits"])
    (output / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
