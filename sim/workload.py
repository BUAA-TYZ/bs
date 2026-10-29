"""二维重叠窗口及调度打包；只描述工作量，不执行图像模型。"""

from dataclasses import asdict, dataclass
import math
from typing import Any


@dataclass(frozen=True)
class TransportComponent:
    name: str
    downsample: int
    bands: int
    bits_per_sample: int

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("transport component name must be a nonempty string")
        for name in ("downsample", "bands", "bits_per_sample"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"transport component {name} must be a positive integer")
        if self.bits_per_sample > 64:
            raise ValueError("transport component bits_per_sample must not exceed 64")

    def packed_bytes(self, box: tuple[int, int, int, int]) -> int:
        x0, y0, x1, y1 = box
        scale = self.downsample
        # 向外取整保留窗口覆盖到的低分辨率像元，位流只在组件末尾补齐字节。
        width = (x1 + scale - 1) // scale - x0 // scale
        height = (y1 + scale - 1) // scale - y0 // scale
        return (width * height * self.bands * self.bits_per_sample + 7) // 8


@dataclass(frozen=True)
class ImageTiling:
    width_px: int
    height_px: int
    bands: int
    bits_per_sample: int
    window_px: int
    stride_px: int
    windows_per_tile_axis: int
    compute_cost_per_window: float
    result_size_mb_per_window: float
    transport_components: tuple[TransportComponent, ...] | None = None
    transport_compression_ratio: float = 1.0
    deduplicate_within_tile: bool = False
    requested_num_tiles: int | None = None

    def __post_init__(self) -> None:
        if type(self.deduplicate_within_tile) is not bool:
            raise ValueError("deduplicate_within_tile must be a boolean")
        if self.requested_num_tiles is not None and (
                type(self.requested_num_tiles) is not int or self.requested_num_tiles not in {1, 4, 16, 64}):
            raise ValueError("requested_num_tiles must be one of 1, 4, 16, 64")
        for name in ("width_px", "height_px", "bands", "bits_per_sample", "window_px",
                     "stride_px", "windows_per_tile_axis"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"image_tiling.{name} must be a positive integer")
        if self.bits_per_sample not in {8, 16, 32, 64}:
            raise ValueError("image_tiling.bits_per_sample must be a byte-aligned storage type (8/16/32/64)")
        if self.window_px > min(self.width_px, self.height_px):
            raise ValueError("image_tiling window must fit the image; padding is not modeled")
        if self.stride_px > self.window_px:
            raise ValueError("image_tiling.stride_px must not exceed window_px (would leave gaps)")
        for name in ("compute_cost_per_window", "result_size_mb_per_window"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError(f"image_tiling.{name} must be finite and nonnegative")
        if self.compute_cost_per_window == 0:
            raise ValueError("image_tiling.compute_cost_per_window must be positive")
        ratio = self.transport_compression_ratio
        if type(ratio) not in (int, float) or not math.isfinite(ratio) or ratio <= 0:
            raise ValueError("image_tiling.transport_compression_ratio must be positive and finite")
        if self.transport_components is not None:
            if not isinstance(self.transport_components, (tuple, list)) or not self.transport_components:
                raise ValueError("image_tiling.transport_components must be a nonempty list")
            try:
                components = tuple(
                    value if isinstance(value, TransportComponent) else TransportComponent(**value)
                    for value in self.transport_components
                )
            except TypeError as exc:
                raise ValueError(f"Invalid transport component fields: {exc}") from exc
            if len({component.name for component in components}) != len(components):
                raise ValueError("transport component names must be unique")
            object.__setattr__(self, "transport_components", components)

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "ImageTiling":
        if not isinstance(values, dict):
            raise ValueError("image_tiling must be a mapping")
        try:
            return cls(**values)
        except TypeError as exc:
            raise ValueError(f"Invalid image_tiling fields: {exc}") from exc

    @property
    def image_size_mb(self) -> float:
        return self._transport_size_mb((0, 0, self.width_px, self.height_px))

    @property
    def window_size_mb(self) -> float:
        # 这是推理输入/显存口径，不随原生遥感载荷格式或传输压缩变化。
        return self.window_px ** 2 * self.bands * self.bits_per_sample / 8e6

    def _transport_size_mb(self, box: tuple[int, int, int, int]) -> float:
        if self.transport_components is None:
            x0, y0, x1, y1 = box
            size_mb = (x1 - x0) * (y1 - y0) * self.bands * self.bits_per_sample / 8e6
        else:
            size_mb = sum(component.packed_bytes(box) for component in self.transport_components) / 1e6
        return size_mb / self.transport_compression_ratio

    def unique_transport_blocks(self, count: int) -> tuple[tuple[tuple[int, int, int, int], float], ...]:
        """原图按 n×n 不重叠块传输，块字节和必须等于整图，不含窗口重叠。"""
        n = math.isqrt(count) if type(count) is int and count > 0 else 0
        if n * n != count or n == 0:
            raise ValueError("unique transport blocks require a positive square block count")
        if n > min(self.width_px, self.height_px):
            raise ValueError("unique transport blocks must not be smaller than one pixel")
        xs = [i * self.width_px // n for i in range(n + 1)]
        ys = [i * self.height_px // n for i in range(n + 1)]
        blocks = tuple(((xs[c], ys[r], xs[c + 1], ys[r + 1]),
                        self._transport_size_mb((xs[c], ys[r], xs[c + 1], ys[r + 1])))
                       for r in range(n) for c in range(n))
        if not math.isclose(math.fsum(size for _, size in blocks), self.image_size_mb,
                            rel_tol=1e-12, abs_tol=1e-12):
            # 降采样组件按块向外取整时会重复边界像元，不能称为唯一像素传输。
            raise ValueError("unique transport blocks must partition the image bytes exactly")
        return blocks

    def tiles(self) -> tuple["ImageTile", ...]:
        xs = window_starts(self.width_px, self.window_px, self.stride_px)
        ys = window_starts(self.height_px, self.window_px, self.stride_px)
        group = self.windows_per_tile_axis
        if self.requested_num_tiles is not None:
            q = max(n for n in (1, 2, 4, 8)
                    if n * n <= self.requested_num_tiles and n <= min(len(xs), len(ys)))
            columns = [xs[i * len(xs) // q:(i + 1) * len(xs) // q] for i in range(q)]
            rows = [ys[i * len(ys) // q:(i + 1) * len(ys) // q] for i in range(q)]
        else:
            columns = [xs[i:i + group] for i in range(0, len(xs), group)]
            rows = [ys[i:i + group] for i in range(0, len(ys), group)]
        tiles = []
        for row in rows:
            for col in columns:
                boxes = tuple((x, y, x + self.window_px, y + self.window_px)
                              for y in row for x in col)
                count = len(boxes)
                size_mb = (count * self.window_size_mb if self.transport_components is None
                           and self.transport_compression_ratio == 1 else
                           math.fsum(self._transport_size_mb(box) for box in boxes))
                if self.deduplicate_within_tile:
                    # 连续网格窗口无缝覆盖包围框，包内每个原生像元只传一次。
                    size_mb = self._transport_size_mb((boxes[0][0], boxes[0][1],
                                                       boxes[-1][2], boxes[-1][3]))
                tiles.append(ImageTile(boxes, size_mb,
                                       count * self.compute_cost_per_window,
                                       count * self.result_size_mb_per_window))
        return tuple(tiles)

    def manifest(self) -> dict[str, Any]:
        tiles = self.tiles()
        count = sum(len(tile.windows_xyxy) for tile in tiles)
        parameters = asdict(self)
        parameters.pop("transport_components")
        parameters.pop("transport_compression_ratio")
        manifest = {
            "model": "overlapping_window_packets_v1",
            "parameters": parameters,
            "coordinate_convention": "xyxy, upper bounds exclusive",
            "edge_rule": "shift_last_window_to_edge_no_padding",
            "packet_rule": "independent_full_windows_no_pixel_deduplication",
            "result_rule": "per_window_outputs_before_global_merge",
            "image_size_mb": self.image_size_mb,
            "num_windows": count,
            "num_tiles": len(tiles),
            "processed_pixels": count * self.window_px ** 2,
            "coverage_multiplier": count * self.window_px ** 2 / (self.width_px * self.height_px),
            "packet_input_mb": math.fsum(tile.data_size_mb for tile in tiles),
            "total_compute_cost": math.fsum(tile.compute_cost for tile in tiles),
            "unmerged_result_mb": math.fsum(tile.result_size_mb for tile in tiles),
            "tiles": [asdict(tile) for tile in tiles],
        }
        if self.transport_components is not None or self.transport_compression_ratio != 1:
            components = ([asdict(component) for component in self.transport_components]
                          if self.transport_components is not None else None)
            parameters.update(transport_components=components,
                              transport_compression_ratio=self.transport_compression_ratio)
            manifest["model"] = "overlapping_native_window_packets_v2"
            manifest["transport"] = {
                "format": ("native_components_packed_bitstream" if components is not None
                           else "inference_raster_with_transport_compression"),
                "pixel_mapping": "floor(start/downsample), ceil(end/downsample)",
                "packing": "per_component_contiguous_bits_ceil_to_byte; no row padding or headers",
                "compression_ratio": self.transport_compression_ratio,
                "compression_assumption": "Uncompressed/compressed size ratio; fixed modeling assumption, not measured codec performance",
                "unique_uncompressed_mb": self.image_size_mb * self.transport_compression_ratio,
                "inference_window_mb": self.window_size_mb,
                "inference_rule": "bands/bits_per_sample define model input and VRAM proxy, independently of native transport",
                "compute_rule": "Per-window processing proxy; not a measured native PAN/MS pipeline; decode/pansharpen cost not modeled",
            }
            if components is not None:
                manifest["transport"]["component_rasters"] = [
                    {**asdict(component),
                     "width_px": (self.width_px + component.downsample - 1) // component.downsample,
                     "height_px": (self.height_px + component.downsample - 1) // component.downsample,
                     "packed_bytes": component.packed_bytes((0, 0, self.width_px, self.height_px))}
                    for component in self.transport_components
                ]
        if self.deduplicate_within_tile:
            manifest["model"] = "deduplicated_dispatch_packets_v3"
            manifest["packet_rule"] = "one_native_bounding_rectangle_per_packet; inference_windows_unchanged"
        return manifest


@dataclass(frozen=True)
class ImageTile:
    windows_xyxy: tuple[tuple[int, int, int, int], ...]
    data_size_mb: float
    compute_cost: float
    result_size_mb: float


def window_starts(length: int, window: int, stride: int) -> tuple[int, ...]:
    """末窗贴边，避免补零或漏掉右侧/底部；边缘实际重叠可能更大。"""
    if any(type(value) is not int or value <= 0 for value in (length, window, stride)):
        raise ValueError("Window geometry must use positive integers")
    if not stride <= window <= length:
        raise ValueError("Expected stride <= window <= image length")
    starts = list(range(0, length - window + 1, stride))
    if starts[-1] != length - window:
        starts.append(length - window)
    return tuple(starts)
