"""工作台 HTTP 服务（标准库 ThreadingHTTPServer，无额外依赖）。

  .venv/bin/python -m workbench.server [--host 127.0.0.1] [--port 8765]

仿真在子进程（workbench.worker）中执行，API 线程只读写运行目录，不会被长任务阻塞。
首版一次只允许一个运行中的实验；每次运行使用 runs/ 下的新目录，不覆盖已有结果。
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import mimetypes
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import zipfile
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import yaml

from sim.evaluation import paired_task_metrics
from workbench import ROOT, catalog
from workbench.worker import CANCEL_FILE, PROGRESS_FILE, STATUS_FILE, now_iso, read_json, write_json

STATIC_DIR = Path(__file__).resolve().parent / "static"
SETTINGS_FILE = Path(__file__).resolve().parent / "settings.json"
DEFAULT_CESIUM_URL = "https://cesium.com/downloads/cesiumjs/releases/1.120/Build/Cesium/"
FINAL_STATES = {"completed", "failed", "cancelled"}
RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
DOWNLOADS = {
    "config.yaml": "application/x-yaml", "request.json": "application/json",
    "result.json": "application/json", "events.jsonl": "application/x-ndjson",
    "snapshots.json": "application/json", "tiles.json": "application/json",
    "czml.json": "application/json", "status.json": "application/json", "worker.log": "text/plain",
}


class ApiError(Exception):
    def __init__(self, status: int, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.status, self.message, self.extra = status, message, extra


def load_settings() -> Dict[str, Any]:
    """访问令牌只从环境变量或未纳入版本库的 workbench/settings.json 读取。"""
    settings = read_json(SETTINGS_FILE, {}) or {}
    return {
        "cesium_ion_token": os.environ.get("CESIUM_ION_TOKEN") or settings.get("cesium_ion_token") or "",
        "cesium_base_url": os.environ.get("WORKBENCH_CESIUM_URL") or settings.get("cesium_base_url")
        or DEFAULT_CESIUM_URL,
    }


def _pid_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _update_status(run_dir: Path, **fields: Any) -> Dict[str, Any]:
    data = read_json(run_dir / STATUS_FILE, {}) or {}
    data.update(fields, updated_at=now_iso())
    write_json(run_dir / STATUS_FILE, data, indent=2)
    return data


def _log_tail(path: Path, limit: int = 4000) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")[-limit:]
    except OSError:
        return ""


# ── 运行管理 ────────────────────────────────────────────────────────────────
def cli_command(run_dir: Path, policy: str, checkpoint: str = "") -> str:
    """复现该运行的 CLI 命令（main.py 与 worker 共用 run_config）。"""
    try:
        cfg_path = str((run_dir / "config.yaml").relative_to(ROOT))
    except ValueError:
        cfg_path = str(run_dir / "config.yaml")
    return (f".venv/bin/python main.py --config {cfg_path} --policy {policy}"
            + (f" --marl-checkpoint {checkpoint}" if checkpoint else "") + " --output /tmp/cli_result.json")


class RunManager:
    def __init__(self, runs_dir: Path, python: str = sys.executable, cancel_grace_s: float = 5.0) -> None:
        self.runs_dir = runs_dir
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.python = python
        self.cancel_grace_s = cancel_grace_s
        self.lock = threading.RLock()
        self.active_id: Optional[str] = None
        self.proc: Optional[subprocess.Popen] = None
        self._summary_cache: Dict[str, tuple] = {}
        self._recover()

    def _recover(self) -> None:
        """服务重启：仍存活的 worker 继续视为活动运行，已退出但未写终态的标记为中断。"""
        for run_dir in sorted(self.runs_dir.iterdir()):
            status = read_json(run_dir / STATUS_FILE)
            if not status or status.get("state") in FINAL_STATES:
                continue
            if _pid_alive(status.get("pid")) and self.active_id is None:
                self.active_id = run_dir.name
            else:
                _update_status(run_dir, state="failed", phase="done", finished_at=now_iso(),
                               error="运行在工作台服务重启前中断，未生成结果")

    def run_dir(self, run_id: str) -> Path:
        if not RUN_ID_RE.match(run_id or "") or not (self.runs_dir / run_id).is_dir():
            raise ApiError(404, f"运行不存在：{run_id}")
        return self.runs_dir / run_id

    def _active_alive(self) -> bool:
        if self.active_id is None:
            return False
        if self.proc is not None:
            return self.proc.poll() is None
        status = read_json(self.runs_dir / self.active_id / STATUS_FILE, {}) or {}
        return status.get("state") not in FINAL_STATES and _pid_alive(status.get("pid"))

    def active(self) -> Optional[str]:
        with self.lock:
            if not self._active_alive():
                self.active_id, self.proc = None, None
            return self.active_id

    # ------------------------------------------------------------------ #
    def create(self, body: Dict[str, Any]) -> Dict[str, Any]:
        policy = str(body.get("policy") or "")
        checkpoint = str(body.get("checkpoint") or "") if policy == "marl" else ""
        result = catalog.validate(body.get("config"), policy, checkpoint)
        if not result["ok"]:
            raise ApiError(400, "配置校验未通过", errors=result["errors"], warnings=result["warnings"])
        with self.lock:
            if self.active() is not None:
                raise ApiError(409, f"已有运行中的实验 {self.active_id}；首版一次只允许运行一个实验")
            cfg = result["config"]
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            base_id = f"{stamp}-{policy}-s{cfg.get('seed', 0)}"
            run_id, n = base_id, 1
            while True:
                try:
                    (self.runs_dir / run_id).mkdir()
                    break
                except FileExistsError:
                    n += 1
                    run_id = f"{base_id}-{n}"
            run_dir = self.runs_dir / run_id
            cli = cli_command(run_dir, policy, checkpoint)
            request = {
                "run_id": run_id, "created_at": now_iso(), "policy": policy, "checkpoint": checkpoint,
                "base_config": body.get("base_config") or None, "rerun_of": body.get("rerun_of"),
                "warnings": result["warnings"], "cli_command": cli, "config": cfg,
            }
            write_json(run_dir / "request.json", request, indent=2)
            header = (f"# 由仿真工作台生成（运行 {run_id}，基于 {request['base_config'] or '页面编辑'}）。\n"
                      f"# CLI 复现：{cli}\n")
            (run_dir / "config.yaml").write_text(
                header + yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
            write_json(run_dir / STATUS_FILE, {"state": "queued", "phase": "starting",
                                               "created_at": request["created_at"]}, indent=2)
            log = (run_dir / "worker.log").open("wb")
            env = dict(os.environ, PYTHONUNBUFFERED="1")
            self.proc = subprocess.Popen([self.python, "-m", "workbench.worker", str(run_dir)], cwd=ROOT,
                                         stdout=log, stderr=subprocess.STDOUT, env=env,
                                         start_new_session=True)
            log.close()
            _update_status(run_dir, pid=self.proc.pid)
            self.active_id = run_id
            threading.Thread(target=self._watch, args=(run_id, self.proc), daemon=True).start()
            return self.detail(run_id)

    def _watch(self, run_id: str, proc: subprocess.Popen) -> None:
        code = proc.wait()
        run_dir = self.runs_dir / run_id
        status = read_json(run_dir / STATUS_FILE, {}) or {}
        if status.get("state") not in FINAL_STATES:
            if (run_dir / CANCEL_FILE).exists():
                _update_status(run_dir, state="cancelled", phase="done", finished_at=now_iso(),
                               error="用户取消；worker 已被强制结束，未生成结果")
            else:
                _update_status(run_dir, state="failed", phase="done", finished_at=now_iso(),
                               error=f"worker 异常退出（返回码 {code}）",
                               traceback=_log_tail(run_dir / "worker.log"))
        with self.lock:
            if self.active_id == run_id:
                self.active_id, self.proc = None, None

    def cancel(self, run_id: str) -> Dict[str, Any]:
        run_dir = self.run_dir(run_id)
        with self.lock:
            if self.active() != run_id:
                raise ApiError(409, "该运行不在执行中，无法取消")
            (run_dir / CANCEL_FILE).touch()
            _update_status(run_dir, cancel_requested_at=now_iso())
            proc = self.proc
        threading.Thread(target=self._escalate, args=(run_id, proc), daemon=True).start()
        return self.detail(run_id)

    def _escalate(self, run_id: str, proc: Optional[subprocess.Popen]) -> None:
        """取消标志在每步结束时检查；初始化阶段等长时间无步进时依次发送 SIGTERM、SIGKILL。"""
        status_path = self.runs_dir / run_id / STATUS_FILE
        pid = proc.pid if proc is not None else (read_json(status_path, {}) or {}).get("pid")
        for sig in (signal.SIGTERM, signal.SIGKILL):
            deadline = time.monotonic() + self.cancel_grace_s
            while time.monotonic() < deadline:
                if (proc.poll() is not None) if proc is not None else not _pid_alive(pid):
                    return
                time.sleep(0.1)
            try:
                if sig == signal.SIGKILL:
                    os.killpg(pid, sig)
                else:
                    os.kill(pid, sig)
            except (ProcessLookupError, PermissionError):
                return

    def rerun(self, run_id: str) -> Dict[str, Any]:
        request = read_json(self.run_dir(run_id) / "request.json")
        if not request:
            raise ApiError(404, "运行缺少 request.json，无法重新运行")
        return self.create({"config": request["config"], "policy": request["policy"],
                            "checkpoint": request.get("checkpoint"), "base_config": request.get("base_config"),
                            "rerun_of": run_id})

    # ------------------------------------------------------------------ #
    def _result_summary(self, run_dir: Path) -> Optional[Dict[str, Any]]:
        path = run_dir / "result.json"
        if not path.exists():
            return None
        mtime = path.stat().st_mtime
        cached = self._summary_cache.get(str(path))
        if cached and cached[0] == mtime:
            return cached[1]
        result = read_json(path)
        if result is None:
            return None
        exp = result.get("experiment", {})
        summary = {"overall": result.get("overall"), "latency": result.get("latency"),
                   "failures": result.get("failures"), "policy": exp.get("policy"),
                   "task_trace_sha256": exp.get("task_trace_sha256"),
                   "elapsed_sim_seconds": exp.get("elapsed_sim_seconds")}
        self._summary_cache[str(path)] = (mtime, summary)
        return summary

    def _config_hash(self, run_dir: Path, request: Dict[str, Any]) -> Optional[str]:
        """规范化配置（含种子，不含执行模式）的哈希，用于在历史中筛选同配置同种子的运行。"""
        path = run_dir / "request.json"
        mtime = path.stat().st_mtime
        cached = self._summary_cache.get(str(path))
        if cached and cached[0] == mtime:
            return cached[1]
        try:
            text = json.dumps(catalog.normalized_config(request["config"]), sort_keys=True, default=str)
            digest = hashlib.sha256(text.encode()).hexdigest()[:12]
        except Exception:
            digest = None
        self._summary_cache[str(path)] = (mtime, digest)
        return digest

    def detail(self, run_id: str) -> Dict[str, Any]:
        run_dir = self.run_dir(run_id)
        request = read_json(run_dir / "request.json", {}) or {}
        if request.get("policy") and not request.get("cli_command"):
            request["cli_command"] = cli_command(run_dir, request["policy"], request.get("checkpoint") or "")
        status = read_json(run_dir / STATUS_FILE, {}) or {}
        files = sorted(name for name in DOWNLOADS if (run_dir / name).exists())
        return {"id": run_id, "request": request, "status": status,
                "progress": read_json(run_dir / PROGRESS_FILE), "files": files,
                "config_hash": self._config_hash(run_dir, request) if request else None,
                "summary": self._result_summary(run_dir), "active": self.active() == run_id}

    def list(self) -> List[Dict[str, Any]]:
        out = []
        for run_dir in sorted(self.runs_dir.iterdir(), reverse=True):
            if not run_dir.is_dir() or not (run_dir / "request.json").exists():
                continue
            request = read_json(run_dir / "request.json", {}) or {}
            cfg = request.get("config") or {}
            status = read_json(run_dir / STATUS_FILE, {}) or {}
            out.append({
                "id": run_dir.name, "created_at": request.get("created_at"),
                "policy": request.get("policy"), "checkpoint": request.get("checkpoint") or None,
                "base_config": request.get("base_config"), "rerun_of": request.get("rerun_of"),
                "seed": cfg.get("seed", 0), "sim_steps": cfg.get("sim_steps", 200), "dt": cfg.get("dt", 1.0),
                "num_sats": cfg.get("num_sats", 4), "state": status.get("state"),
                "config_hash": self._config_hash(run_dir, request),
                "wall_seconds": status.get("wall_seconds"), "error": status.get("error"),
                "progress": read_json(run_dir / PROGRESS_FILE) if status.get("state") not in FINAL_STATES else None,
                "summary": self._result_summary(run_dir),
            })
        return out

    # ------------------------------------------------------------------ #
    def compare(self, ids: List[str]) -> Dict[str, Any]:
        if len(ids) < 2:
            raise ApiError(400, "至少选择两个运行进行对比")
        runs = []
        for run_id in ids:
            run_dir = self.run_dir(run_id)
            request = read_json(run_dir / "request.json", {}) or {}
            result = read_json(run_dir / "result.json")
            if result is None:
                raise ApiError(400, f"运行 {run_id} 没有结果（状态：{(read_json(run_dir / STATUS_FILE, {}) or {}).get('state')}）")
            runs.append((run_id, request, result))
        flat = [catalog.flatten(catalog.normalized_config(req["config"])) for _, req, _ in runs]
        keys = sorted(set().union(*flat))
        diffs = [{"key": key, "values": [f.get(key) for f in flat]} for key in keys
                 if any(f.get(key) != flat[0].get(key) for f in flat[1:])]
        exp = [res["experiment"] for _, _, res in runs]
        traces = {e.get("task_trace_sha256") for e in exp}
        tles = {e.get("tle_sha256") for e in exp}
        sources = {e.get("source_sha256") for e in exp}
        notes = []
        if len(sources) > 1:
            notes.append("各运行的代码版本（source_sha256）不同，结果可能受代码修改影响。")
        if len(traces) > 1 and not diffs:
            notes.append("配置相同但任务到达轨迹哈希不同。")
        labels = [f"{req.get('policy')}" + (f"（{Path(req['checkpoint']).parent.name}）" if req.get("checkpoint") else "")
                  for _, req, _ in runs]
        comparable = not diffs and len(traces) == 1 and len(tles) == 1
        rows = []
        for (run_id, req, res), label in zip(runs, labels):
            rows.append({"id": run_id, "label": label, "policy": res["experiment"].get("policy"),
                         "checkpoint": res["experiment"].get("checkpoint"),
                         "overall": res["overall"], "latency": res["latency"], "failures": res["failures"],
                         "elapsed_sim_seconds": res["experiment"].get("elapsed_sim_seconds"),
                         "task_trace_sha256": res["experiment"].get("task_trace_sha256"),
                         "source_sha256": res["experiment"].get("source_sha256"),
                         "delivery": [[o["release_seconds"], o["completion_seconds"]] for o in res["task_outcomes"]]})
        paired = []
        if comparable:
            reference = runs[0][2]["task_outcomes"]
            for run_id, _, res in runs[1:]:
                paired.append({"reference": runs[0][0], "candidate": run_id,
                               **paired_task_metrics(reference, res["task_outcomes"])})
        return {"comparable": comparable, "config_diffs": diffs, "notes": notes, "runs": rows,
                "paired": paired, "seeds": sorted({req["config"].get("seed", 0) for _, req, _ in runs})}

    # ------------------------------------------------------------------ #
    def events(self, run_id: str, task_id: str, limit: int = 20000) -> Dict[str, Any]:
        path = self.run_dir(run_id) / "events.jsonl"
        if not path.exists():
            raise ApiError(404, "该运行没有生命周期事件文件")
        if not re.match(r"^[\w.-]+$", task_id or ""):
            raise ApiError(400, "需要 task_id")
        needle = f'"task_id": "{task_id}"'
        rows, truncated = [], False
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if needle in line:
                    if len(rows) >= limit:
                        truncated = True
                        break
                    rows.append(json.loads(line))
        return {"task_id": task_id, "time_unit": "step", "events": rows, "truncated": truncated}

    def tasks_csv(self, run_id: str) -> bytes:
        result = read_json(self.run_dir(run_id) / "result.json")
        if result is None:
            raise ApiError(404, "该运行没有结果")
        buf = io.StringIO()
        outcomes = result.get("task_outcomes") or []
        fields = list(outcomes[0].keys()) if outcomes else ["task_id"]
        writer = csv.DictWriter(buf, fieldnames=fields)
        writer.writeheader()
        writer.writerows(outcomes)
        return ("\ufeff" + buf.getvalue()).encode("utf-8")

    def bundle(self, run_id: str):
        run_dir = self.run_dir(run_id)
        spool = tempfile.SpooledTemporaryFile(max_size=32 * 1024 * 1024)
        with zipfile.ZipFile(spool, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in DOWNLOADS:
                if (run_dir / name).exists():
                    zf.write(run_dir / name, f"{run_id}/{name}")
            if (run_dir / "result.json").exists():
                zf.writestr(f"{run_id}/tasks.csv", self.tasks_csv(run_id))
        size = spool.tell()
        spool.seek(0)
        return spool, size


# ── HTTP ───────────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    server_version = "SatWorkbench/1.0"
    manager: RunManager

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("WORKBENCH_ACCESS_LOG"):
            super().log_message(fmt, *args)

    # ------------------------------------------------------------------ #
    def _send(self, status: int, body: bytes, content_type: str, headers: Optional[Dict[str, str]] = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, data: Any, status: int = 200) -> None:
        self._send(status, json.dumps(data, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

    def _file(self, path: Path, content_type: str, download: Optional[str] = None) -> None:
        if not path.is_file():
            raise ApiError(404, f"文件不存在：{path.name}")
        headers = {"Content-Disposition": f'attachment; filename="{download}"'} if download else {}
        size = path.stat().st_size
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(size))
        self.send_header("Cache-Control", "no-store")
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            with path.open("rb") as f:
                shutil.copyfileobj(f, self.wfile)

    def _body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length > 20 * 1024 * 1024:
            raise ApiError(413, "请求体过大")
        try:
            data = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            raise ApiError(400, "请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise ApiError(400, "请求体必须是 JSON 对象")
        return data

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        url = urlparse(self.path)
        query = {k: v[-1] for k, v in parse_qs(url.query).items()}
        parts = [p for p in url.path.split("/") if p]
        try:
            if not parts or parts[0] != "api":
                if method != "GET":
                    raise ApiError(405, "不支持的方法")
                return self._static(url.path)
            self._api(method, parts[1:], query)
        except ApiError as exc:
            self._json({"error": exc.message, **exc.extra}, exc.status)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:
            traceback.print_exc()
            self._json({"error": f"服务内部错误：{type(exc).__name__}: {exc}"}, 500)

    def _static(self, path: str) -> None:
        rel = "index.html" if path in ("", "/") else path.lstrip("/")
        target = (STATIC_DIR / rel).resolve()
        if STATIC_DIR.resolve() not in target.parents and target != STATIC_DIR.resolve():
            raise ApiError(404, "not found")
        content_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        if target.suffix == ".js":
            content_type = "text/javascript"
        self._file(target, content_type + ("; charset=utf-8" if content_type.startswith("text/") else ""))

    def _api(self, method: str, parts: List[str], query: Dict[str, str]) -> None:
        m = self.manager
        route = parts[0] if parts else ""
        if method == "GET" and route == "meta":
            settings = load_settings()
            return self._json({
                "policies": catalog.POLICIES, "policy_groups": catalog.POLICY_GROUPS,
                "contact_window_policies": sorted(catalog.CONTACT_WINDOW_POLICIES),
                "fields": catalog.FIELDS, "field_groups": catalog.FIELD_GROUPS, "gs_fields": catalog.GS_FIELDS,
                "configs": catalog.list_configs(), "tles": catalog.list_tles(),
                "cesium": {"base_url": settings["cesium_base_url"], "ion_token": settings["cesium_ion_token"]},
                "active_run": m.active(), "runs_dir": str(m.runs_dir),
            })
        if method == "GET" and route == "config":
            try:
                return self._json(catalog.load_config_file(query.get("path", "")))
            except FileNotFoundError as exc:
                raise ApiError(404, str(exc))
        if method == "GET" and route == "checkpoints":
            return self._json({"checkpoints": catalog.list_checkpoints()})
        if method == "GET" and route == "checkpoint":
            return self._json(catalog.inspect_checkpoint(query.get("path", "")))
        if method == "POST" and route == "validate":
            body = self._body()
            policy = str(body.get("policy") or "")
            result = catalog.validate(body.get("config"), policy,
                                      str(body.get("checkpoint") or "") if policy == "marl" else "")
            result.pop("config", None)
            return self._json(result)
        if method == "GET" and route == "compare":
            ids = [i for i in query.get("ids", "").split(",") if i]
            return self._json(m.compare(ids))
        if route != "runs":
            raise ApiError(404, "未知接口")
        if len(parts) == 1:
            if method == "GET":
                return self._json({"runs": m.list(), "active_run": m.active()})
            return self._json(m.create(self._body()), 201)
        run_id = parts[1]
        action = parts[2] if len(parts) > 2 else ""
        if method == "POST" and action == "cancel":
            return self._json(m.cancel(run_id))
        if method == "POST" and action == "rerun":
            return self._json(m.rerun(run_id), 201)
        if method != "GET":
            raise ApiError(405, "不支持的方法")
        run_dir = m.run_dir(run_id)
        if action == "":
            return self._json(m.detail(run_id))
        if action in ("result", "snapshots", "czml", "tiles"):
            name = f"{action}.json"
            if not (run_dir / name).exists():
                state = (read_json(run_dir / STATUS_FILE, {}) or {}).get("state")
                raise ApiError(404, f"该运行没有 {name}（状态：{state}）")
            return self._file(run_dir / name, "application/json; charset=utf-8")
        if action == "events":
            return self._json(m.events(run_id, query.get("task_id", "")))
        if action == "download":
            name = parts[3] if len(parts) > 3 else ""
            if name == "tasks.csv":
                return self._send(200, m.tasks_csv(run_id), "text/csv; charset=utf-8",
                                  {"Content-Disposition": f'attachment; filename="{run_id}_tasks.csv"'})
            if name == "bundle.zip":
                spool, size = m.bundle(run_id)
                self.send_response(200)
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Length", str(size))
                self.send_header("Content-Disposition", f'attachment; filename="{run_id}.zip"')
                self.end_headers()
                shutil.copyfileobj(spool, self.wfile)
                spool.close()
                return
            if name not in DOWNLOADS:
                raise ApiError(404, "不支持下载该文件")
            return self._file(run_dir / name, DOWNLOADS[name], download=f"{run_id}_{name}")
        raise ApiError(404, "未知接口")


def make_server(host: str, port: int, runs_dir: Path) -> ThreadingHTTPServer:
    handler = type("BoundHandler", (Handler,), {"manager": RunManager(runs_dir)})
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="卫星任务调度仿真工作台")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--runs-dir", default=str(ROOT / "runs"))
    args = parser.parse_args()
    server = make_server(args.host, args.port, Path(args.runs_dir).resolve())
    print(f"仿真工作台：http://{args.host}:{server.server_address[1]}/  （运行目录 {args.runs_dir}）", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
