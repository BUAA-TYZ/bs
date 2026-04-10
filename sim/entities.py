from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class TileState(str, Enum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    TRANSFERRING = "TRANSFERRING"
    READY = "READY"
    RUNNING = "RUNNING"
    COMPUTED = "COMPUTED"
    DOWNLINKING = "DOWNLINKING"
    DONE = "DONE"
    FAILED = "FAILED"


class ActionType(str, Enum):
    LOCAL = "LOCAL"
    OFFLOAD = "OFFLOAD"
    WAIT = "WAIT"


class FailureReason(str, Enum):
    MEM_FULL = "mem_full"
    VRAM_OOM = "vram_oom"
    LINK_DOWN = "link_down"
    DEADLINE_MISS = "deadline_miss"
    NO_ROUTE = "no_route"


@dataclass
class Action:
    tile_id: str
    action_type: ActionType
    target_sat_id: Optional[int] = None


@dataclass
class TileTimestamps:
    created: int
    start_tx: Optional[int] = None
    end_tx: Optional[int] = None
    start_compute: Optional[int] = None
    end_compute: Optional[int] = None
    start_downlink: Optional[int] = None
    end_downlink: Optional[int] = None


@dataclass
class Tile:
    tile_id: str
    parent_task_id: str
    data_size_mb: float
    compute_cost: float
    vram_req_gb: float
    state: TileState
    location: int
    timestamps: TileTimestamps
    deadline: Optional[int] = None
    remaining_compute: Optional[float] = None
    failure_reason: Optional[FailureReason] = None


@dataclass
class Task:
    task_id: str
    source_sat_id: int
    release_time: int
    image_size_mb: float
    num_tiles: int
    deadline: Optional[int]
    tile_ids: List[str] = field(default_factory=list)


@dataclass
class Satellite:
    sat_id: int
    compute_rate: float  # tiles per second (or cost units per second)
    mem_capacity_gb: float
    vram_capacity_gb: float
    queue: List[str] = field(default_factory=list)
    executing: Optional[str] = None
    mem_used_gb: float = 0.0
    vram_used_gb: float = 0.0
    energy: Optional[float] = None
    busy_time: float = 0.0
    queue_time_acc: float = 0.0
    mem_peak_gb: float = 0.0
    vram_peak_gb: float = 0.0


@dataclass
class Link:
    i: int
    j: int
    up: bool
    bandwidth_mbps: float
    latency_ms: float


@dataclass
class Transfer:
    tile_id: str
    src: int
    dst: int
    remaining_mb: float
    start_time: int
    link_key: str


@dataclass
class GroundStation:
    gs_id: str
    lat_deg: float
    lon_deg: float
    alt_m: float
    min_elevation_deg: float
    bandwidth_mbps: float
    latency_ms: float


@dataclass
class DownlinkTransfer:
    tile_id: str
    src_sat: int
    gs_id: str
    remaining_mb: float
    start_time: int


@dataclass
class EnvState:
    time: int
    satellites: Dict[int, Dict]
    neighbors: Dict[int, List[int]]
    links: Dict[str, Dict]
    tiles: Dict[str, Dict]
    config: Dict
