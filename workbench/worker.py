"""单次工作台运行的子进程入口：python -m workbench.worker <run_dir>

读取 run_dir/request.json，经 main.run_config（与 CLI 相同的执行流程）运行仿真，写出：
status.json（状态/阶段/错误）、progress.json（步数与计数）、result.json（与 CLI --output 相同的 summary）、
snapshots.json（采样过程快照）、tiles.json（分片最终状态）、events.jsonl（生命周期事件）、czml.json。
取消：server 创建 run_dir/cancel 标志文件，观测器在下一步结束时中止；SIGTERM 作为兜底。
"""

from __future__ import annotations

import json
import math
import os
import signal
import sys
import time
import traceback
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from workbench import ROOT

STATUS_FILE, PROGRESS_FILE, CANCEL_FILE = "status.json", "progress.json", "cancel"


def write_json(path: Path, data, indent=None) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
    os.replace(tmp, path)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def build_czml(raw_cfg: dict, num_sats: int, sim_steps: int, dt: float) -> list:
    """用 czml_tools.tle_to_czml 生成与仿真同一 TLE 子集、同一起始时间的 CZML。

    卫星编号 i 对应 TLE 文件中的第 i 条记录（与 sim.env._resolve_tle_lines 的顺序一致），
    CZML 实体 id 为 sat-{i}。采样多覆盖一个步长，避免末段插值越界。
    """
    from czml_tools.tle_to_czml import (TLEEntry, build_packets, ground_stations_from_config,
                                        iso_z, parse_tle_file, parse_utc)
    from datetime import timedelta

    topology = raw_cfg.get("topology") or {}
    if topology.get("tle_lines"):
        tles = [TLEEntry(name=f"sat_{i}", line1=l1, line2=l2)
                for i, (l1, l2) in enumerate(topology["tle_lines"])]
    else:
        path = Path(str(topology["tle_file"])).expanduser()
        tles = parse_tle_file(path if path.is_absolute() else ROOT / path)
    tles = tles[:num_sats]
    start = parse_utc(str(topology.get("start_time_utc", "2025-01-01T00:00:00Z")))
    duration = max(1, math.ceil(sim_steps * dt))
    step_s = int(min(60, max(10, math.ceil(duration / 300))))
    packets = build_packets(tles, ground_stations_from_config(raw_cfg), start,
                            duration + step_s, step_s)
    interval = f"{iso_z(start)}/{iso_z(start + timedelta(seconds=sim_steps * dt))}"
    for pkt in packets:
        if pkt["id"] == "document":
            pkt["clock"]["interval"] = interval
            pkt["clock"]["multiplier"] = 10
        elif pkt["id"].startswith("sat-"):
            idx = int(pkt["id"].split("-", 1)[1])
            pkt["availability"] = interval
            pkt["label"]["text"] = f"S{idx}"
            pkt["properties"] = {"sat_index": idx, "tle_name": pkt["name"]}
            pkt["name"] = f"S{idx} · {pkt['name']}"
    return packets


class _Status:
    def __init__(self, run_dir: Path) -> None:
        self.path = run_dir / STATUS_FILE
        self.data = read_json(self.path, {}) or {}

    def update(self, **fields) -> None:
        self.data.update(fields, updated_at=now_iso())
        write_json(self.path, self.data, indent=2)


def run(run_dir: Path) -> int:
    from main import apply_execution_mode, run_config
    from sim.config import config_from_dict
    from workbench.observer import RunCancelled, SnapshotObserver

    request = read_json(run_dir / "request.json")
    status = _Status(run_dir)
    started = time.monotonic()
    status.update(state="running", phase="initializing", pid=os.getpid(), started_at=now_iso())
    cancel_flag = run_dir / CANCEL_FILE

    def on_sigterm(signum, frame):
        raise RunCancelled()

    signal.signal(signal.SIGTERM, on_sigterm)

    raw = request["config"]
    policy, checkpoint = request["policy"], request.get("checkpoint") or ""
    observer = None
    sim_steps = int(raw.get("sim_steps", 200))

    def on_progress(steps_done, counters):
        if status.data.get("phase") != "simulating":
            status.update(phase="simulating")
        elapsed = time.monotonic() - started
        write_json(run_dir / PROGRESS_FILE, {
            "steps_done": steps_done, "sim_steps": sim_steps, "elapsed_wall_s": round(elapsed, 2),
            "counters": counters, "updated_at": now_iso()})

    try:
        cfg = apply_execution_mode(config_from_dict(raw), policy)
        cfg = replace(cfg, tile_lifecycle_log=str(run_dir / "events.jsonl"))
        observer = SnapshotObserver(progress=on_progress, cancelled=cancel_flag.exists)
        summary = run_config(cfg, policy, checkpoint, observer=observer)
        status.update(phase="writing")
        write_json(run_dir / "result.json", summary, indent=2)
        write_json(run_dir / "snapshots.json", observer.export(complete=True))
        write_json(run_dir / "tiles.json", observer.tile_table())
        status.update(phase="czml")
        try:
            write_json(run_dir / "czml.json", build_czml(raw, cfg.num_sats, cfg.sim_steps, cfg.dt))
            czml_error = None
        except Exception as exc:  # 轨道可视化失败不影响仿真结果
            czml_error = f"{type(exc).__name__}: {exc}"
        status.update(state="completed", phase="done", finished_at=now_iso(),
                      wall_seconds=round(time.monotonic() - started, 2), czml_error=czml_error)
        return 0
    except RunCancelled:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        partial = observer is not None and observer.meta
        czml_error = None
        if partial:
            write_json(run_dir / "snapshots.json", observer.export(complete=False))
            write_json(run_dir / "tiles.json", observer.tile_table())
            try:  # 部分快照的诊断回放仍需要轨道
                write_json(run_dir / "czml.json", build_czml(raw, cfg.num_sats, cfg.sim_steps, cfg.dt))
            except Exception as exc:
                czml_error = f"{type(exc).__name__}: {exc}"
        status.update(state="cancelled", phase="done", finished_at=now_iso(),
                      wall_seconds=round(time.monotonic() - started, 2), czml_error=czml_error,
                      cancelled_at_step=(observer.last_step + 1) if partial else 0,
                      error="用户取消；未生成 result.json，部分过程快照仅用于诊断")
        return 0
    except BaseException as exc:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        status.update(state="failed", phase="done", finished_at=now_iso(),
                      wall_seconds=round(time.monotonic() - started, 2),
                      error=f"{type(exc).__name__}: {exc}", traceback=traceback.format_exc())
        return 1


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m workbench.worker <run_dir>")
    sys.exit(run(Path(sys.argv[1]).resolve()))


if __name__ == "__main__":
    main()
