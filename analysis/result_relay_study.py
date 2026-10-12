"""结果中继开发对照；合成正负例和可选的既有轨道验证案例。"""

import argparse
from dataclasses import replace
from collections import Counter
import json
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from main import make_policy
from sim.config import load_config
from sim.entities import Link, Task, Tile, TileState, TileTimestamps
from sim.env import SimulationEnv
from sim.evaluation import task_age_delivery_curve
from sim.marl.decision import decision_state
from sim.marl.scenarios import ScenarioDistribution


class StudyTopology:
    def __init__(self, cfg):
        self.cfg = cfg
        self.sat_recs = [None] * cfg.num_sats
        self.t0 = None
        self.links = {"0-1": Link(0, 1, True, cfg.bandwidth_mbps_max, 0),
                      "1-2": Link(1, 2, True, cfg.bandwidth_mbps_max, 0)}

    def snapshot(self, t, dt=1):
        return self.links

    def close(self):
        pass


class StudyVisibility:
    def __init__(self, **kwargs):
        self.dt = kwargs["dt"]
        self.lookahead_s = 60

    def refresh(self, t, recs, t0, station_objs, stations):
        self.now = t * self.dt
        self.visible_gs_by_sat = {sid: ["gs"] if sid == 2 and self.now < 60 else [] for sid in range(len(recs))}
        self.best_gs_by_sat = {sid: next(iter(gids), None) for sid, gids in self.visible_gs_by_sat.items()}

    def windows_at(self, sid, t):
        return {"gs": (0, 60 - t * self.dt) if sid == 2 and t * self.dt < 60 else None}

    def is_visible(self, sid, gid, stations):
        return sid == 2 and self.now < 60


def inject_image(env):
    task = Task("image", 0, 0, 16, 4, None)
    env.tasks[task.task_id] = task
    env.metrics.total_tasks += 1
    tiles = []
    for index in range(4):
        tile = Tile(f"image_{index}", task.task_id, 4, 1, .1, TileState.CREATED,
                    0, TileTimestamps(0))
        env.tiles[tile.tile_id] = tile
        task.tile_ids.append(tile.tile_id)
        env.metrics.total_tiles += 1
        tiles.append(tile)
        env._log_tile_event(tile, "created", sat_from=None, sat_to=0)
    env._task_tile_index[task.task_id] = task.tile_ids
    if env.pipeline_mode == "ground_compute":
        env._ground_pipeline.enqueue_ground_task(task, tiles)
    else:
        for tile in tiles:
            env._enqueue_tile(0, tile)


def run(cfg, label, directory, synthetic, experiment_extra=None):
    variants = {"ground_direct": ("ground_compute", None, False),
                "ground_tiles": ("ground_tiles", None, False),
                "ground_relay": ("ground_relay", None, False),
                "local_direct": ("distributed", "local", False),
                "local_relay": ("distributed", "local", True),
                "window_direct": ("distributed", "window_aware", False),
                "window_relay": ("distributed", "window_aware", True)}
    mode, policy_name, relay = variants[label]
    directory.mkdir(parents=True, exist_ok=False)
    cfg = replace(cfg, pipeline_mode=mode, result_buffering=True, result_relay=relay,
                  tile_lifecycle_log=str(directory / "events.jsonl"))
    if synthetic:
        with patch("sim.env.TopologyModel", StudyTopology), patch("sim.env.VisibilityCache", StudyVisibility):
            env = SimulationEnv(cfg)
    else:
        env = SimulationEnv(cfg)
    policy = make_policy(policy_name, cfg.seed) if policy_name else None
    try:
        if synthetic:
            inject_image(env)
        for _ in range(cfg.sim_steps):
            actions = policy.select_actions(decision_state(env.export_state())) if policy else []
            env.step(actions)
        result = env.summary()
        result["experiment"]["policy"] = label
        result["experiment"].update(experiment_extra or {})
        result["task_age_delivery_curve"] = task_age_delivery_curve(
            result["task_outcomes"], [5, 10, 30, 60] if synthetic else [1800, 3600, 7200, 14400],
            result["experiment"]["elapsed_sim_seconds"])
        result["result_paths"] = {tid: tile.result_path for tid, tile in env.tiles.items() if len(tile.result_path) > 1}
        result["raw_relay_paths"] = {tid: tile.raw_relay_path for tid, tile in env.tiles.items() if len(tile.raw_relay_path) > 1}
        result["end_state_diagnostic"] = {
            "tile_states": dict(Counter(tile.state.value for tile in env.tiles.values())),
            "isl_inflight": [{"tile_id": tr.tile_id, "src": tr.src, "dst": tr.dst,
                              "remaining_mb": tr.remaining_mb, "started_step": tr.start_time}
                             for tr in env.transfers],
            "ground_inflight": [{"tile_id": tr.tile_id, "src": tr.src_sat, "gs": tr.gs_id,
                                 "remaining_mb": tr.remaining_mb, "started_step": tr.start_time}
                                for tr in env.ground_tile_transfers + env.downlink_transfers],
        }
        for sid, sat in env.satellites.items():
            held = sum(tile.data_size_mb / 1024 for tile in env.tiles.values() if sid in tile.memory_holders)
            held += sum(cfg.result_size_mb / 1024 for tile in env.tiles.values() if sid in tile.result_memory_holders)
            held += sum(size for tid, size in env.task_source_mem_gb.items() if env.tasks[tid].source_sat_id == sid)
            if abs(sat.mem_used_gb - held) > 1e-8 or sat.mem_used_gb > sat.mem_capacity_gb + 1e-8:
                raise RuntimeError("Memory accounting mismatch")
        for key, sent in env.metrics.network_sent_mb.items():
            if sent > env.metrics.network_capacity_mb[key] + 1e-8:
                raise RuntimeError("Network exceeded capacity")
    finally:
        env.close()
    with (directory / "metrics.json").open("x") as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
    print(f"{label}: {result['overall']['completed_tasks']}/{result['overall']['total_tasks']}; "
          f"latency={result['latency']['task_mean']}", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--config", default="examples/config_roles.yaml")
    parser.add_argument("--distribution", default="examples/scenario_roles.json")
    parser.add_argument("--orbit-case", help="One existing validation case; never reads the test split")
    parser.add_argument("--variants", default="ground_direct,ground_tiles,ground_relay,local_direct,local_relay,window_direct,window_relay")
    args = parser.parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=False)
    base = load_config(args.config)
    if args.orbit_case:
        cases = ScenarioDistribution(args.distribution, base).cases("validation")
        cfg, info = next((cfg, info) for cfg, info in cases if info["id"] == args.orbit_case)
        scenarios = [(args.orbit_case, cfg, False)]
    else:
        base = replace(base, num_sats=3, sim_steps=60, dt=1, task_arrival_end_step=0,
                       task_arrival_rate=0, num_tiles=4, image_size_mb=16, compute_cost_per_tile=1,
                       compute_rate=1, result_size_mb=1, ground_relay_delay_s=0, aggregation_delay_s=2,
                       ground_stations=[{"id": "gs", "bandwidth_mbps": 8, "compute_rate": 1}],
                       topology={**base.topology, "predict_contacts": False, "precompute_visibility": False})
        scenarios = [(name, replace(base, topology={**base.topology, "bandwidth_mbps_max": bandwidth}), True)
                     for name, bandwidth in (("two_hop", 8), ("insufficient_capacity", .08))]
    records = []
    for name, cfg, synthetic in scenarios:
        trace = None
        for label in args.variants.split(","):
            result = run(cfg, label, out / name / label, synthetic)
            if trace is not None and trace != result["experiment"]["task_trace_sha256"]:
                raise RuntimeError("Methods used different task traces")
            trace = result["experiment"]["task_trace_sha256"]
            records.append({"case": name, "label": label, "metrics_path": str(out / name / label / "metrics.json"),
                            "overall": result["overall"], "latency": result["latency"],
                            "task_age_delivery_curve": result["task_age_delivery_curve"],
                            "completed_tasks_with_result_relay": sum(r["status"] == "completed" and r["result_relay_hops"] > 0
                                                                      for r in result["task_outcomes"])})
    with (out / "summary.json").open("x") as stream:
        json.dump({"scope": "Routing and buffering development diagnostic, no learned policy comparison",
                   "runs": records}, stream, indent=2, allow_nan=False)


if __name__ == "__main__":
    main()
