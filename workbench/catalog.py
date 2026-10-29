"""工作台可选项与提交前校验：配置文件、策略、checkpoint 与字段定义。

校验尽量调用引擎自身的解析与检查（config_from_dict、地面站与 TLE 解析、MARLPolicy 加载），
只把引擎的错误信息定位到表单字段，不另立一套参数约束。
"""

from __future__ import annotations

import copy
import math
import re
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from main import GROUND_MODES, POLICY_NAMES
from sim.config import config_from_dict
from sim.env import _load_ground_stations, _resolve_tle_lines
from workbench import ROOT

EXAMPLES_DIR = ROOT / "examples"
TLE_DIR = ROOT / "data" / "tle"
CHECKPOINT_DIR = ROOT / "checkpoints"

# ── 策略目录：名称与 main.make_policy / 地面 pipeline 一致 ─────────────────────
POLICIES: List[Dict[str, Any]] = [
    {"name": "local", "group": "onboard", "label": "全部本地计算",
     "description": "所有分片在源星本地执行，用于区分卸载协作收益。"},
    {"name": "greedy", "group": "onboard", "label": "贪心最早完成",
     "description": "GreedyEarliestFinish：估算本地、一跳/两跳卸载与直接上地面的完成时间并取最早者。"},
    {"name": "load_aware", "group": "onboard", "label": "负载感知资源匹配",
     "description": "LoadAwareResourceFit：本地队列较长时向资源更空闲的邻星分流。"},
    {"name": "random", "group": "onboard", "label": "随机",
     "description": "RandomPolicy：在合法动作中随机选择，随机数种子取配置 seed。"},
    {"name": "window_aware", "group": "onboard", "label": "窗口感知贪心",
     "description": "WindowAwareGreedy：在贪心估算中使用地面站过境窗口预测下传等待时间。"},
    {"name": "predictive", "group": "onboard", "label": "保守预测启发式",
     "description": "PredictivePolicy：考虑分片到达后的接触窗口，READY 分片继续执行。"},
    {"name": "reservation", "group": "reservation", "label": "容量预约",
     "description": "ReservationPolicy：同步预约计算与回传容量，请求串行仲裁。"},
    {"name": "reservation_independent", "group": "reservation", "label": "容量预约（独立报价消融）",
     "description": "相同预测下不累计预约的消融对照。"},
    {"name": "reservation_joint", "group": "reservation", "label": "整图预检（beam=4）",
     "description": "JointReservationPolicy：预检所有未分配兄弟分片，束宽 4。"},
    {"name": "reservation_joint_sequential", "group": "reservation", "label": "整图预检（顺序）",
     "description": "JointReservationPolicy：束宽 1 的顺序预检。"},
    {"name": "reservation_calendar", "group": "reservation", "label": "服务日历预约",
     "description": "ForecastReservationPolicy：有限时域接触图报价。"},
    {"name": "reservation_guarded", "group": "reservation", "label": "保守接单预约",
     "description": "ForecastReservationPolicy(guard)：接单前检查传输时隙与内存。"},
    {"name": "reservation_multihop", "group": "reservation", "label": "多段回传预约",
     "description": "ForecastReservationPolicy(multihop)：报价允许多段结果回传。"},
    {"name": "reservation_forecast", "group": "reservation", "label": "保守接单 + 多段回传",
     "description": "ForecastReservationPolicy(guard, multihop)。"},
    {"name": "reservation_deferred", "group": "reservation", "label": "等待机会预约",
     "description": "ForecastReservationPolicy(multihop, defer)：允许等待更好的接触机会。"},
    {"name": "heft_adapted", "group": "literature", "label": "HEFT（滚动任务图适配）",
     "description": "向上排序与插空最早完成；静态边代价，不保证按预计路径交付。"},
    {"name": "cgs_compute_adapted", "group": "literature", "label": "CGS（计算机会适配）",
     "description": "比较计算产出后的接触图交付机会，按接触总量接纳。非原文完整系统复现。"},
    {"name": "dts_tmax_onehop", "group": "literature", "label": "DTS-TMAX（一跳选择适配）",
     "description": "输入传输、承诺队列和计算响应最小化；不含原文的多跳搜索与动态优先级。"},
    {"name": "dts_tmax_path", "group": "literature", "label": "DTS-TMAX（有界路径适配）",
     "description": "相同TMAX报价扩展到公共输入路径，跳数由配置控制；不等同原文PropagateSearch。"},
    {"name": "dts_tmax_hybrid", "group": "literature", "label": "DTS-TMAX（星地混合适配）",
     "description": "TMAX响应最小化加入共享地面输入中继候选；是额外适配，不是原论文完整算法。"},
    {"name": "rolling_admission", "group": "onboard", "label": "有限承诺接单",
     "description": "研究候选：仅接下预计30秒内开算的分片；当前开发实验未优于文献适配。"},
    {"name": "task_quota", "group": "onboard", "label": "整图机会配额",
     "description": "研究候选：规划同图全部未承诺分片，将即时配额优先分给公共动作前缀。"},
    {"name": "task_delivery", "group": "onboard", "label": "整图试排调度",
     "description": "比较FCFS、整图期限与稀缺资源顺序，以完整任务按时数及截断时延选优。"},
    {"name": "arrival_calendar", "group": "onboard", "label": "跨轮到达日历",
     "description": "保留已发出分片的预计输入到达时间，用公开剩余工作量校正跨轮计算承诺。"},
    {"name": "cgs_delivery_guard", "group": "onboard", "label": "CGS + 交付窗口余量",
     "description": "研究候选：分配计算前扣除星间接触尾部保护期，执行规则不变。"},
    {"name": "cgs_task_gate", "group": "onboard", "label": "CGS + 整图源端门控",
     "description": "研究候选：源端预计可完整按时交付时保留本地，否则由CGS分配。尚未优于本地对照。"},
    {"name": "cgs_task_choice", "group": "onboard", "label": "CGS + 整图方案复核",
     "description": "研究候选：计入同轮输入竞争和实测并发工时，比较整图本地与CGS分配。"},
    {"name": "marl", "group": "learned", "label": "MARL（IPPO/MAPPO checkpoint）",
     "description": "MARLPolicy：加载训练好的 checkpoint，每星使用局部观测确定性推理。"},
    {"name": "ground_compute", "group": "ground", "label": "全地面整图计算",
     "description": "执行模式：整图上传地面站计算，不调用调度策略。"},
    {"name": "ground_relay", "group": "ground", "label": "原始分片中继至地面计算",
     "description": "执行模式：原始分片经星间中继下传，在地面站计算，不调用调度策略。"},
    {"name": "ground_tiles", "group": "ground", "label": "原始分片直接下传地面计算",
     "description": "执行模式：原始分片仅直接下传地面站计算（不经星间中继），不调用调度策略。"},
    {"name": "ground_unique_relay", "group": "ground", "label": "唯一像素原图中继至地面计算",
     "description": "执行模式：原图按不重叠块经星间中继下传，全部到齐后在地面切窗计算，不调用调度策略。"},
]
assert [p["name"] for p in POLICIES] == list(POLICY_NAMES + GROUND_MODES)
POLICY_GROUPS = {"onboard": "星上调度策略", "reservation": "预约类策略",
                 "learned": "学习策略", "ground": "地面执行模式", "literature": "文献方法适配"}
# 读取链路 contact_windows 的策略；未开启 topology.predict_contacts 时该输入为空。
CONTACT_WINDOW_POLICIES = {"predictive", "marl", "heft_adapted", "cgs_compute_adapted", "cgs_delivery_guard", "cgs_task_gate", "cgs_task_choice", "dts_tmax_path", "dts_tmax_hybrid", "rolling_admission", "task_quota", "task_delivery", "arrival_calendar"} | {p for p in POLICY_NAMES if p.startswith("reservation")}

# ── 表单字段：key 为点分路径；default 与 config_from_dict / env.py 的读取默认值一致 ──
_F = lambda key, label, group, type_, default=None, **kw: {  # noqa: E731
    "key": key, "label": label, "group": group, "type": type_, "default": default, **kw}
FIELDS: List[Dict[str, Any]] = [
    _F("seed", "随机种子", "basic", "int", 0, help="任务到达、分片大小与链路带宽噪声均由该种子决定"),
    _F("sim_steps", "仿真步数", "basic", "int", 200, min=1),
    _F("dt", "步长 dt", "basic", "float", 1.0, unit="s", min_exclusive=0),
    _F("num_sats", "卫星数量", "basic", "int", 4, min=1, help="取 TLE 文件前 N 颗卫星"),
    _F("task_arrival_rate", "任务到达率", "load", "float", 0.3, unit="个/步", min=0, help="每步泊松到达均值"),
    _F("task_arrival_end_step", "停止到达步号", "load", "int_or_null", None, min=0,
       help="首个不再产生任务的步号；留空表示全程到达"),
    _F("image_size_mb", "整图大小", "load", "float", 512.0, unit="MB", min_exclusive=0),
    _F("num_tiles", "分片数", "load", "int", 16, min=1),
    _F("compute_cost_per_tile", "单分片计算量", "load", "float", 4.0, min_exclusive=0,
       help="计算时长 = 计算量 / 算力（秒）"),
    _F("result_size_mb", "单分片结果大小", "load", "float", 1.0, unit="MB", min=0),
    _F("deadline_steps", "截止步数", "load", "int", 0, min=0, help="0 表示无截止期"),
    _F("compute_rate", "单窗服务率", "compute", "float", 1.0, min_exclusive=0),
    _F("mem_capacity_gb", "数据缓存配额", "compute", "float", 32.0, unit="GB", min_exclusive=0),
    _F("vram_capacity_gb", "执行内存配额", "compute", "float", 8.0, unit="GB", min_exclusive=0),
    _F("vram_base_gb", "单片执行预留", "compute", "float", 0.8, unit="GB"),
    _F("vram_alpha_per_mb", "分片显存系数", "compute", "float", 0.002, unit="GB/MB"),
    _F("vram_policy", "显存不足处理", "compute", "enum", "wait", options=["wait", "reject"]),
    _F("topology.bandwidth_mbps_min", "星间带宽下限", "comm", "float", 50.0, unit="Mbps"),
    _F("topology.bandwidth_mbps_max", "星间带宽上限", "comm", "float", 300.0, unit="Mbps"),
    _F("topology.max_range_km", "星间最大距离", "comm", "float", 0.0, unit="km", min=0, help="0 表示不限制"),
    _F("topology.max_neighbors", "最大邻居数", "comm", "int", 0, min=0, help="0 表示不限制"),
    _F("topology.predict_contacts", "接触预测", "comm", "bool", False,
       help="启用确定性几何接触计划（链路 contact_windows）"),
    _F("topology.window_lookahead_s", "窗口预测时域", "comm", "float", 1800.0, unit="s"),
    _F("ground_stations", "地面站", "comm", "ground_stations", []),
    # ── 高级 ──
    _F("decision_interval_steps", "决策间隔", "advanced", "int", 1, min=1, unit="步"),
    _F("topology_update_steps", "拓扑更新间隔", "advanced", "int", 1, min=1, unit="步"),
    _F("ground_visibility_update_steps", "星地可见性更新间隔", "advanced", "int", 1, min=1, unit="步"),
    _F("transfer_fail_on_link_down", "链路中断即失败", "advanced", "bool", False, help="仅作用于星间传输"),
    _F("execution_commitment", "执行承诺", "advanced", "bool", False),
    _F("result_buffering", "结果缓存", "advanced", "bool", False),
    _F("result_relay", "结果中继", "advanced", "bool", False, help="需要同时开启结果缓存"),
    _F("relay_max_hops", "最大中继跳数", "advanced", "int", 8, min=1),
    _F("relay_contact_guard", "中继接触保护", "advanced", "bool", False, help="需要开启接触预测"),
    _F("relay_routing", "中继路由", "advanced", "enum", "snapshot", options=["snapshot", "temporal"]),
    _F("ground_relay_delay_s", "地面转发延迟", "advanced", "float", 0.0, unit="s", min=0),
    _F("aggregation_delay_s", "整图聚合延迟", "advanced", "float", 0.0, unit="s", min=0),
    _F("task_source_sat_ids", "成像源卫星", "advanced", "int_list_or_null", None,
       help="逗号分隔的卫星编号；留空表示全部卫星产生任务"),
    _F("satellite_compute_rates", "逐星算力", "advanced", "float_list_or_null", None,
       help="逗号分隔，长度等于卫星数；留空使用统一算力"),
    _F("topology.tle_file", "TLE 文件", "advanced", "tle", None),
    _F("topology.start_time_utc", "起始时间 (UTC)", "advanced", "str", "2025-01-01T00:00:00Z"),
    _F("topology.bandwidth_period", "带宽变化周期", "advanced", "int", 50, unit="步"),
    _F("topology.bandwidth_noise", "带宽噪声幅度", "advanced", "float", 5.0, unit="Mbps"),
    _F("topology.bandwidth_distance_scale_km", "带宽距离尺度", "advanced", "float", 0.0, unit="km"),
    _F("topology.latency_ms", "星间时延", "advanced", "float", 20.0, unit="ms"),
    _F("topology.min_elevation_deg", "星间最小仰角", "advanced", "float", 0.0, unit="°"),
    _F("topology.earth_radius_km", "地球半径", "advanced", "float", 6378.137, unit="km"),
    _F("topology.precompute_visibility", "预计算星地可见性", "advanced", "bool", False),
    _F("topology.contact_scan_s", "接触扫描步长", "advanced", "float", 30.0, unit="s"),
    _F("topology.contact_update_s", "接触计划更新间隔", "advanced", "float", 60.0, unit="s"),
    _F("topology.visibility_workers", "可见性计算进程数", "advanced", "int", 1, min=1),
]
FIELD_GROUPS = {"basic": "基本参数", "load": "任务负载", "compute": "星上算力与存储",
                "comm": "通信与地面站", "advanced": "高级配置"}
GS_FIELDS = [
    {"key": "id", "label": "ID", "type": "str", "readonly": True},
    {"key": "lat_deg", "label": "纬度", "type": "float", "default": 0.0, "unit": "°"},
    {"key": "lon_deg", "label": "经度", "type": "float", "default": 0.0, "unit": "°"},
    {"key": "alt_m", "label": "海拔", "type": "float", "default": 0.0, "unit": "m"},
    {"key": "min_elevation_deg", "label": "最小仰角", "type": "float", "default": 5.0, "unit": "°"},
    {"key": "bandwidth_mbps", "label": "每通道带宽", "type": "float", "default": 200.0, "unit": "Mbps"},
    {"key": "receive_channels", "label": "接收通道", "type": "int", "default": 1},
    {"key": "compute_rate", "label": "地面算力", "type": "float", "default": 8.0},
]
_FIELD_BY_KEY = {f["key"]: f for f in FIELDS}
# 未写入配置时引擎使用的拓扑默认值，仅用于对比时归一化显示。
TOPOLOGY_DEFAULTS = {f["key"].split(".", 1)[1]: f["default"] for f in FIELDS
                     if f["key"].startswith("topology.") and f["key"] != "topology.tle_file"}


# ── 配置文件 ────────────────────────────────────────────────────────────────
def resolve_path(value: str) -> Path:
    path = Path(str(value)).expanduser()
    return path if path.is_absolute() else ROOT / path


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def _description(text: str) -> str:
    lines = []
    for line in text.splitlines():
        if not line.startswith("#"):
            break
        body = line.lstrip("#").strip()
        if body and not set(body) <= set("=-"):
            lines.append(body)
    return " ".join(lines[:2])


def list_configs() -> List[Dict[str, Any]]:
    out = []
    for path in sorted(EXAMPLES_DIR.glob("*.yaml")):
        text = path.read_text(encoding="utf-8")
        raw = yaml.safe_load(text) or {}
        problems = environment_errors(raw)
        out.append({
            "path": _relative(path), "name": path.stem, "description": _description(text),
            "num_sats": raw.get("num_sats"), "sim_steps": raw.get("sim_steps"), "dt": raw.get("dt"),
            "available": not problems, "problems": [p["message"] for p in problems],
        })
    return out


def load_config_file(rel_path: str) -> Dict[str, Any]:
    path = resolve_path(rel_path)
    if path.suffix not in (".yaml", ".yml", ".json") or not _inside_root(path) or not path.is_file():
        raise FileNotFoundError(f"配置文件不存在：{rel_path}")
    text = path.read_text(encoding="utf-8")
    raw = yaml.safe_load(text) or {}
    return {"path": _relative(path), "description": _description(text), "config": raw}


def list_tles() -> List[Dict[str, Any]]:
    out = []
    for path in sorted(TLE_DIR.glob("*.tle")):
        try:
            count = len(_resolve_tle_lines({"tle_file": str(path)}))
        except ValueError:
            count = 0
        out.append({"path": _relative(path), "count": count})
    return out


def _inside_root(path: Path) -> bool:
    try:
        path.resolve().relative_to(ROOT)
        return True
    except ValueError:
        return False


# ── checkpoint ─────────────────────────────────────────────────────────────
def list_checkpoints() -> List[Dict[str, Any]]:
    out = []
    for path in sorted(CHECKPOINT_DIR.glob("**/*.pt")):
        if path.name not in ("best.pt", "last.pt"):
            continue
        stat = path.stat()
        out.append({"path": _relative(path), "dir": _relative(path.parent), "name": path.name,
                    "size_mb": round(stat.st_size / 1e6, 2), "mtime": stat.st_mtime})
    return out


_ckpt_cache: Dict[tuple, Dict[str, Any]] = {}
_ckpt_lock = threading.Lock()


def inspect_checkpoint(rel_path: str) -> Dict[str, Any]:
    """用 MARLPolicy 实际加载 checkpoint，返回元数据或引擎给出的错误。"""
    if not rel_path:
        return {"ok": False, "error": "MARL 策略需要选择 checkpoint（--marl-checkpoint）"}
    path = resolve_path(rel_path)
    if not _inside_root(path):
        return {"ok": False, "error": "checkpoint 必须位于仓库目录内"}
    if not path.is_file():
        return {"ok": False, "error": f"checkpoint 文件不存在：{rel_path}"}
    stat = path.stat()
    key = (str(path.resolve()), stat.st_mtime, stat.st_size)
    with _ckpt_lock:
        if key in _ckpt_cache:
            return _ckpt_cache[key]
        try:
            from sim.scheduler.marl_policy import MARLPolicy

            meta = MARLPolicy(str(path)).metadata
            info = {
                "ok": True, "path": _relative(path),
                "algorithm": meta.get("algorithm"), "model_version": meta.get("model_version"),
                "observation_version": meta.get("observation_version"),
                "ground_station_ids": list(meta.get("ground_station_ids") or []),
                "model_config": meta.get("model_config"),
                "training_config": (meta.get("training_args") or {}).get("config"),
                "seed": meta.get("seed"), "episode": meta.get("episode"),
                "training_seeds": list(meta.get("training_seeds") or []),
                "validation_seeds": list(meta.get("validation_seeds") or []),
                "selection": meta.get("selection"),
            }
        except Exception as exc:  # 引擎拒绝旧版/不兼容 checkpoint 时给出原始原因
            info = {"ok": False, "path": _relative(path), "error": f"{type(exc).__name__}: {exc}"}
        _ckpt_cache[key] = info
        return info


# ── 校验 ───────────────────────────────────────────────────────────────────
def get_path(raw: Dict[str, Any], key: str, default: Any = None) -> Any:
    node: Any = raw
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node


def _type_error(value: Any, field: Dict[str, Any]) -> Optional[str]:
    type_ = field["type"]
    is_int = type(value) is int
    is_num = type(value) in (int, float) and math.isfinite(value)
    if type_ == "int" and not is_int:
        return "需要整数"
    if type_ == "int_or_null" and value is not None and not is_int:
        return "需要整数或留空"
    if type_ == "float" and not is_num:
        return "需要有限数值"
    if type_ == "bool" and type(value) is not bool:
        return "需要布尔值"
    if type_ == "enum" and value not in field["options"]:
        return "可选值：" + " / ".join(field["options"])
    if type_ in ("str", "tle") and not isinstance(value, str):
        return "需要文本"
    if type_ == "int_list_or_null" and value is not None and (
            not isinstance(value, list) or any(type(v) is not int for v in value)):
        return "需要逗号分隔的整数列表或留空"
    if type_ == "float_list_or_null" and value is not None and (
            not isinstance(value, list) or any(type(v) not in (int, float) or not math.isfinite(v) for v in value)):
        return "需要逗号分隔的数值列表或留空"
    if is_num and "min" in field and value < field["min"]:
        return f"不能小于 {field['min']}"
    if is_num and "min_exclusive" in field and value <= field["min_exclusive"]:
        return f"必须大于 {field['min_exclusive']}"
    return None


# 引擎错误信息 → 字段
_ENGINE_FIELD_PATTERNS = [
    (r"^(\w+) must be positive$", None),
    (r"^(\w+) must be finite and nonnegative$", None),
    (r"^result_relay requires result_buffering$", "result_relay"),
    (r"^relay_contact_guard requires predict_contacts$", "relay_contact_guard"),
    (r"^(relay_routing must be|temporal relay routing requires)", "relay_routing"),
    (r"^relay_max_hops must be", "relay_max_hops"),
    (r"^vram_policy must be", "vram_policy"),
    (r"^task_arrival_end_step must be", "task_arrival_end_step"),
    (r"^task_source_sat_ids must be", "task_source_sat_ids"),
    (r"^satellite_compute_rates must", "satellite_compute_rates"),
    (r"^ground station", "ground_stations"),
    (r"^At least one ground station", "ground_stations"),
    (r"^TLE file not found", "topology.tle_file"),
    (r"^No valid TLE entries", "topology.tle_file"),
    (r"^Skyfield topology requires", "topology.tle_file"),
    (r"^num_sats exceeds", "num_sats"),
    (r"^max_neighbors must be", "topology.max_neighbors"),
    (r"^pipeline_mode must be", "policy"),
]


def _engine_field(message: str, raw: Dict[str, Any]) -> str:
    if message.startswith("task_arrival_rate and result_size_mb"):
        rate = raw.get("task_arrival_rate", 0.3)
        return "task_arrival_rate" if isinstance(rate, (int, float)) and rate < 0 else "result_size_mb"
    for pattern, field in _ENGINE_FIELD_PATTERNS:
        match = re.match(pattern, message)
        if match:
            return field or match.group(1)
    return "config"


def environment_errors(raw: Dict[str, Any]) -> List[Dict[str, str]]:
    """依赖运行环境的检查：TLE 文件与条数、地面站参数（与 SimulationEnv 初始化相同）。"""
    errors = []
    topology = dict(raw.get("topology") or {})
    if topology.get("tle_file") and not topology.get("tle_lines"):
        topology["tle_file"] = str(resolve_path(topology["tle_file"]))
    try:
        tle_lines = _resolve_tle_lines(topology)
        if not tle_lines:
            raise ValueError("Skyfield topology requires non-empty topology.tle_lines or topology.tle_file")
        num_sats = raw.get("num_sats", 4)
        if type(num_sats) is int and len(tle_lines) < num_sats:
            raise ValueError(f"num_sats exceeds the number of TLE records (TLE 共 {len(tle_lines)} 条)")
    except (ValueError, FileNotFoundError) as exc:
        message = str(exc).replace(str(ROOT) + "/", "")
        errors.append({"field": _engine_field(message, raw), "message": message})
    try:
        if not _load_ground_stations(list(raw.get("ground_stations") or [])):
            raise ValueError("At least one ground station is required in config: ground_stations")
    except (ValueError, TypeError) as exc:
        errors.append({"field": _engine_field(str(exc), raw), "message": str(exc)})
    max_neighbors = topology.get("max_neighbors", 0)
    if type(max_neighbors) is int and max_neighbors < 0:
        errors.append({"field": "topology.max_neighbors",
                       "message": "max_neighbors must be nonnegative; zero means unlimited"})
    return errors


def ground_station_ids(raw: Dict[str, Any]) -> List[str]:
    return [str(item.get("id", f"gs_{idx}")) for idx, item in enumerate(raw.get("ground_stations") or [])]


def prepare_config(raw: Dict[str, Any], policy: str) -> Dict[str, Any]:
    """执行模式由策略选择决定，写入 pipeline_mode，使保存的配置可直接用于 CLI 复现。"""
    cfg = copy.deepcopy(raw)
    cfg["pipeline_mode"] = policy if policy in GROUND_MODES else "distributed"
    cfg["tile_lifecycle_log"] = ""
    return cfg


def validate(raw: Any, policy: str, checkpoint: str = "") -> Dict[str, Any]:
    errors: List[Dict[str, str]] = []
    warnings: List[str] = []
    if not isinstance(raw, dict):
        return {"ok": False, "errors": [{"field": "config", "message": "配置必须是对象"}], "warnings": []}
    if policy not in POLICY_NAMES + GROUND_MODES:
        errors.append({"field": "policy", "message": f"未知策略：{policy}"})
    for field in FIELDS:
        if field["type"] == "ground_stations":
            continue
        value = get_path(raw, field["key"], field["default"])
        if field["type"] == "tle" and value is None:
            continue
        problem = _type_error(value, field)
        if problem:
            errors.append({"field": field["key"], "message": problem})
    for idx, item in enumerate(raw.get("ground_stations") or []):
        if not isinstance(item, dict):
            errors.append({"field": f"ground_stations.{idx}", "message": "地面站必须是对象"})
            continue
        for gs_field in GS_FIELDS[1:]:
            value = item.get(gs_field["key"], gs_field["default"])
            problem = _type_error(value, gs_field)
            if problem:
                errors.append({"field": f"ground_stations.{idx}.{gs_field['key']}", "message": problem})
    if errors:
        return {"ok": False, "errors": errors, "warnings": warnings}

    prepared = prepare_config(raw, policy)
    try:
        config_from_dict(prepared)
    except (ValueError, TypeError) as exc:
        errors.append({"field": _engine_field(str(exc), prepared), "message": str(exc)})
    errors.extend(environment_errors(prepared))

    if policy == "marl":
        info = inspect_checkpoint(checkpoint)
        if not info["ok"]:
            errors.append({"field": "checkpoint", "message": info["error"]})
        else:
            if info["ground_station_ids"] != ground_station_ids(prepared):
                errors.append({"field": "checkpoint", "message":
                               "Ground station order/IDs differ from the training checkpoint（checkpoint: "
                               + ", ".join(info["ground_station_ids"]) + "）"})
            seed = prepared.get("seed", 0)
            if seed in info["training_seeds"] or seed in info["validation_seeds"]:
                warnings.append(f"种子 {seed} 属于该 checkpoint 的训练/验证种子，结果不能作为独立测试结论。")
    if policy in CONTACT_WINDOW_POLICIES and not get_path(prepared, "topology.predict_contacts", False):
        warnings.append("该策略会读取链路接触窗口（topology.predict_contacts）；当前未开启，窗口输入为空。")
    if policy in GROUND_MODES:
        warnings.append("地面执行模式不调用调度策略，pipeline_mode 将设为 " + policy + "。")
    if prepared.get("decision_interval_steps", 1) != 1:
        warnings.append("decision_interval_steps ≠ 1：与 evaluate_policies 的可比评估设置不同。")
    num_sats, steps = prepared.get("num_sats", 4), prepared.get("sim_steps", 200)
    if isinstance(num_sats, int) and isinstance(steps, int) and num_sats >= 24 and steps >= 1000:
        warnings.append(f"规模较大（{num_sats} 星 × {steps} 步），运行可能需要数分钟以上。")
    return {"ok": not errors, "errors": errors, "warnings": warnings, "config": prepared}


# ── 可比性 ─────────────────────────────────────────────────────────────────
COMPARE_IGNORED = {"tile_lifecycle_log", "pipeline_mode"}


def normalized_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    """按引擎默认值展开配置，忽略日志路径与执行模式（后者属于被比较的方法）。"""
    cfg = asdict(config_from_dict(raw))
    for key in COMPARE_IGNORED:
        cfg.pop(key, None)
    topology = {**TOPOLOGY_DEFAULTS, **(cfg.get("topology") or {})}
    if topology.get("tle_lines"):
        topology["tle_lines"] = [list(pair) for pair in topology["tle_lines"]]
    cfg["topology"] = topology
    gs_defaults = {f["key"]: f["default"] for f in GS_FIELDS[1:]}
    cfg["ground_stations"] = [{**gs_defaults, "latency_ms": 20.0, **gs} for gs in cfg["ground_stations"]]
    return cfg


def flatten(value: Any, prefix: str = "") -> Dict[str, Any]:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            out.update(flatten(item, f"{prefix}.{key}" if prefix else str(key)))
        return out
    if isinstance(value, list) and value and all(isinstance(v, dict) for v in value):
        out = {}
        for idx, item in enumerate(value):
            out.update(flatten(item, f"{prefix}.{idx}"))
        return out
    return {prefix: value}
