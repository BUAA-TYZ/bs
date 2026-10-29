"""冻结SAR场景失败任务的事件归因：从时刻0重跑一个方法，导出逐分片时间线并分类。

只读观察，不改执行器。重跑结果须与主表记录逐任务一致（确定性检查），否则拒绝归因。
分类只依据本次运行的事件证据：
- physical_no_window：松弛可达上界判定300 s内无任何到北京站的时序路径；
- cache_failure：任务分片以 MEM_FULL 失败；
- 其余按关键尾片（最晚交付分片）的阶段时长取最大者：
  source_wait（源星等待派发/本地排队）、input_transfer（原图/输入在途，含地面中继逐跳）、
  input_assembly_wait（唯一像素模式等待其他原图块）、compute_queue（输入齐备后排队）、
  compute_service、result_delivery（结果中继与下行）。
  接收占用比例只是伴随证据，不能单独证明接收竞争或物理无窗。
  若尾片是已承诺地面中继(`ground_relay_target`)的原图，且同图星上分片早已在300 s内
  交付，另记 `committed_ground_relay_stranded`：这是承诺锁定地面目的地后路径失效的
  延迟现象，是否路径失效、绕路或可由其他承诺避免仍需反事实重放。
“过早承诺”需要反事实重放，单次运行无法给出事件证据，不作为自动类别。
"""

import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import sys
import time

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import main as simulator
from run_sar_frozen import derive_config, digest, sha256_bytes
from sim.config import config_from_dict
from sim.reproducibility import runtime_metadata

HORIZON_S = 300.0
STAGES = ("source_wait", "input_transfer", "input_assembly_wait", "compute_queue",
          "compute_service", "result_delivery")


class ChannelObserver:
    """逐步记录北京站接收通道是否有字节下传，用于区分接收竞争与等窗口。"""

    def on_start(self, env):
        self.env = env
        self.busy = np.zeros(env.cfg.sim_steps, dtype=bool)
        self.keys = [f"ground:{gid}" for gid in env.ground_stations]
        self.last = 0.0

    def on_step(self, env, step_idx, actions):
        sent = sum(env.metrics.network_sent_mb.get(key, 0.0) for key in self.keys)
        self.busy[step_idx] = sent > self.last + 1e-12
        self.last = sent


def tile_record(tile, dt):
    ts = tile.timestamps
    s = lambda value: None if value is None else value * dt
    return {"tile_id": tile.tile_id, "state": tile.state.value,
            "failure_reason": tile.failure_reason.value if tile.failure_reason else None,
            "processor": ("ground" if not tile.result_path else int(tile.result_path[0])),
            "result_hops": max(0, len(tile.result_path) - 1),
            "raw_relay_hops": max(0, len(tile.raw_relay_path) - 1),
            "input_route": list(tile.input_route), "ground_relay_target": tile.ground_relay_target,
            "created": s(ts.created), "start_tx": s(ts.start_tx), "end_tx": s(ts.end_tx),
            "start_compute": s(ts.start_compute), "end_compute": s(ts.end_compute),
            "start_downlink": s(ts.start_downlink), "end_downlink": s(ts.end_downlink)}


def stages(row, input_ready_seconds=None):
    created, start_tx, end_tx = row["created"], row["start_tx"], row["end_tx"]
    start_c, end_c, done = row["start_compute"], row["end_compute"], row["end_downlink"]
    if start_c is None or end_c is None or done is None:
        return None
    first_action = start_tx if start_tx is not None else start_c
    arrived = end_tx if end_tx is not None else first_action
    ready = max(arrived, first_action, input_ready_seconds if input_ready_seconds is not None else arrived)
    if ready > start_c + 1e-6:
        raise ValueError("Compute starts before all required input is ready")
    return {"source_wait": first_action - created,
            "input_transfer": 0.0 if start_tx is None else max(0.0, arrived - start_tx),
            "input_assembly_wait": max(0.0, ready - arrived),
            "compute_queue": max(0.0, start_c - ready),
            "compute_service": end_c - start_c,
            "result_delivery": max(0.0, done - end_c)}


def channel_share(busy, start_s, end_s, dt):
    if busy is None:
        return None
    a, b = int(round(start_s / dt)), int(round(end_s / dt))
    if b <= a:
        return None
    return float(busy[a:b].mean())


def classify(task, tiles, bound_row, busy, dt, *, unique_image=False):
    if bound_row is not None and not bound_row["relay_potentially_on_time"]:
        return "physical_no_window", {"bound_relay_open_s": bound_row["relay_ground_open_seconds"]}
    if any(t["failure_reason"] == "MEM_FULL" for t in tiles):
        return "cache_failure", {}
    finished = [t for t in tiles if t["end_downlink"] is not None]
    if len(finished) < len(tiles):
        tail = max((t for t in tiles), key=lambda t: (t["end_downlink"] is None, t["end_downlink"] or 0))
        return "unfinished_at_horizon", {"tail_tile": tail}
    tail = max(finished, key=lambda t: t["end_downlink"])
    input_tail = max(finished, key=lambda t: t["end_tx"]) if unique_image else None
    input_ready = input_tail["end_tx"] if input_tail is not None else None
    parts = stages(tail, input_ready)
    dominant = max(STAGES, key=lambda key: parts[key])
    evidence = {"tail_tile": tail, "stage_seconds": parts}
    if input_tail is not None:
        evidence.update({"last_input_tile": input_tail, "all_input_ready_seconds": input_ready,
                         "first_compute_seconds": min(t["start_compute"] for t in finished)})
    if dominant == "input_assembly_wait":
        return "input_delivery_wait", evidence
    if dominant == "result_delivery" or (dominant == "input_transfer" and tail["processor"] == "ground"):
        start = tail["end_compute"] if dominant == "result_delivery" else tail["start_tx"]
        end = tail["end_downlink"] if dominant == "result_delivery" else tail["end_tx"]
        share = channel_share(busy, start, end, dt)
        evidence["receiver_busy_share"] = share
        onboard = [t["end_downlink"] for t in finished if t["processor"] != "ground"]
        release = task.release_time * dt
        stranded = (dominant == "input_transfer" and tail["ground_relay_target"] is not None
                    and onboard and max(onboard) - release <= HORIZON_S)
        evidence["ground_relay_tiles_late"] = sum(
            1 for t in finished if t["processor"] == "ground" and t["end_downlink"] - release > HORIZON_S)
        evidence["onboard_tiles_last_delivery_age_s"] = (max(onboard) - release) if onboard else None
        if stranded and (share is None or share < 0.5):
            dominant = "committed_ground_relay_stranded"
        else:
            dominant = "result_delivery_wait" if dominant == "result_delivery" else "input_delivery_wait"
    return dominant, evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel-record", type=Path, required=True, help="run_sar_frozen.py method record")
    parser.add_argument("--bound", type=Path, default=Path("results/sar_reachability_bound_20261015/bound.json"))
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    panel = json.loads(args.panel_record.read_bytes())
    method = panel["method"]
    cfg_dict = json.loads((args.panel_record.parent / "derived_config.json").read_bytes())
    if digest(cfg_dict) != panel["derived_config_sha256"]:
        raise ValueError("Derived config does not match the panel record")
    runtime = runtime_metadata()
    if runtime["source_sha256"] != panel["metrics"]["experiment"]["source_sha256"]:
        raise ValueError("Core source differs from the panel record; diagnosis would not replay it")
    cfg = simulator.apply_execution_mode(config_from_dict(cfg_dict), method)
    observer = ChannelObserver()
    started = time.monotonic()
    metrics = simulator.run_config(cfg, method, observer=observer)
    wall = time.monotonic() - started
    if metrics["task_outcomes"] != panel["metrics"]["task_outcomes"]:
        raise RuntimeError("Replay differs from the panel record; refusing to attribute")
    env, dt = observer.env, cfg.dt
    bound = {row["task_id"]: row for row in json.loads(args.bound.read_bytes())["tasks"]}
    on_time = set(panel["on_time_task_ids"])
    rows, timelines = [], {}
    for outcome in metrics["task_outcomes"]:
        task = env.tasks[outcome["task_id"]]
        tiles = [tile_record(env.tiles[tid], dt) for tid in task.tile_ids]
        timelines[task.task_id] = tiles
        if task.task_id in on_time:
            continue
        label, evidence = classify(task, tiles, bound.get(task.task_id), observer.busy, dt,
                                   unique_image=method == "ground_unique_relay")
        processors = {}
        for tile in tiles:
            key = "ground" if tile["processor"] == "ground" else (
                "source" if tile["processor"] == task.source_sat_id else "helper")
            processors[key] = processors.get(key, 0) + 1
        rows.append({"task_id": task.task_id, "source_sat_id": task.source_sat_id,
                     "release_seconds": outcome["release_seconds"], "status": outcome["status"],
                     "latency_seconds": outcome["latency_seconds"], "cause": label,
                     "processor_split": processors, "evidence": evidence})
    counts = {}
    for row in rows:
        counts[row["cause"]] = counts.get(row["cause"], 0) + 1
    args.out.mkdir(parents=True, exist_ok=True)
    timeline_path = args.out / f"{method}_tile_timelines.json.gz"
    with gzip.open(timeline_path, "wt", encoding="utf-8") as handle:
        json.dump(timelines, handle, allow_nan=False)
    report = {"scope": __doc__.strip().splitlines()[0], "method": method,
              "panel_record": str(args.panel_record),
              "panel_record_sha256": sha256_bytes(args.panel_record.read_bytes()),
              "bound_sha256": sha256_bytes(args.bound.read_bytes()),
              "script_sha256": sha256_bytes(Path(__file__).read_bytes()), **runtime,
              "replay_identical_to_panel": True, "wall_seconds": wall,
              "receiver_busy_steps_share": float(observer.busy.mean()),
              "late_or_failed_tasks": len(rows), "cause_counts": counts,
              "timeline_file": timeline_path.name,
              "timeline_sha256": hashlib.sha256(timeline_path.read_bytes()).hexdigest(),
              "tasks": rows}
    (args.out / f"{method}_failures.json").write_text(json.dumps(report, indent=2, ensure_ascii=False,
                                                                 allow_nan=False))
    print(json.dumps({"method": method, "late_or_failed": len(rows), "cause_counts": counts,
                      "wall_s": round(wall, 1)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
