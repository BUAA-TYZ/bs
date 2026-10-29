"""公开 SAR 小样本试验的数据校验、分组和有界 ZIP 读取。"""

from collections import Counter
from dataclasses import dataclass
import hashlib
import io
import math
from pathlib import Path, PurePosixPath
import threading
import time
import urllib.request

from shapely.geometry import Polygon


ARCHIVE_BYTES = 27_935_453_810
CENTRAL_OFFSET = 27_927_051_260
CENTRAL_BYTES = 8_402_452
CENTRAL_SHA256 = "21280176160cd716066a16fa229a6f2cd9254a0d9b9c313ec6638518c40f2481"
ARCHIVE_URL = (
    "https://drive.usercontent.google.com/download?"
    "id=16gKlS9-CLuTIEUJpel3fbslsTI8uCVhG&export=download&confirm=t"
    "&uuid=8a420255-2b36-43c7-873d-58dd838ca830"
)
EXCLUDED_GROUPS = frozenset({"ABS001", "HEB012", "MHD003", "UBA102", "UBA114"})


def digest(data):
    return hashlib.sha256(data).hexdigest()


def stable_rank(value, seed=1701):
    return hashlib.sha256(f"{seed}:{value}".encode()).hexdigest()


def provisional_group(member):
    stem = PurePosixPath(member).stem
    if "__" not in stem or not stem.split("__", 1)[0]:
        raise ValueError(f"Missing provisional grouping prefix: {member}")
    return stem.split("__", 1)[0]


def assigned_split(group, seed=1701):
    return "val" if int(stable_rank(group, seed), 16) % 5 == 0 else "train"


@dataclass(frozen=True)
class Annotation:
    points: tuple
    class_name: str
    difficulty: int


def parse_annotations(text):
    annotations = []
    for index, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        fields = line.split()
        if len(fields) != 10:
            raise ValueError(f"Line {index}: expected ten fields")
        coordinates = tuple(float(value) for value in fields[:8])
        if not all(math.isfinite(value) for value in coordinates):
            raise ValueError(f"Line {index}: non-finite coordinate")
        difficulty = int(fields[9])
        if difficulty < 0:
            raise ValueError(f"Line {index}: negative difficulty")
        polygon = Polygon(list(zip(coordinates[::2], coordinates[1::2])))
        if not polygon.is_valid or polygon.area <= 0:
            raise ValueError(f"Line {index}: invalid polygon")
        annotations.append(Annotation(coordinates, fields[8], difficulty))
    return annotations


def contained_labels(annotations, names, width=800, height=800):
    """首轮只保留目标完全位于图内的影像，不修改原始目标框。"""
    if any(item.difficulty != 0 for item in annotations):
        raise ValueError("Difficult annotation requires excluding the whole image")
    class_ids = {name: index for index, name in enumerate(names)}
    rows, counts, stats = [], Counter(), Counter()
    for annotation in annotations:
        points = list(zip(annotation.points[::2], annotation.points[1::2]))
        values = [coordinate / size for x, y in points
                  for coordinate, size in ((x, width), (y, height))]
        if not all(0 <= value <= 1 for value in values):
            raise ValueError("Out-of-bounds annotation requires excluding the whole image")
        rows.append(str(class_ids[annotation.class_name]) + " " +
                    " ".join(f"{value:.9f}" for value in values))
        counts[annotation.class_name] += 1
        stats["unmodified_objects"] += 1
    return "\n".join(rows) + ("\n" if rows else ""), dict(counts), dict(stats)


def select_candidates(candidates, names, targets=None, seed=1701):
    targets = targets or {"train": 128, "val": 64}
    chosen = []
    for split, target in targets.items():
        pool = sorted((candidate for candidate in candidates
                       if candidate["provisional_group_id"] not in EXCLUDED_GROUPS
                       and assigned_split(candidate["provisional_group_id"], seed) == split),
                      key=lambda candidate: stable_rank(candidate["label_member"], seed))
        frequencies = Counter(name for candidate in pool for name in candidate["class_counts"])
        uncovered, used_groups, selected_members = set(names), Counter(), set()
        selected = []
        for group_limit in (1, 2):
            while len(selected) < target:
                remaining = [candidate for candidate in pool
                             if candidate["label_member"] not in selected_members
                             and used_groups[candidate["provisional_group_id"]] < group_limit]
                if not remaining:
                    break
                scores = [sum(1 / frequencies[name] for name in sorted(uncovered)
                              if name in candidate["class_counts"]) for candidate in remaining]
                best = remaining[max(range(len(remaining)), key=scores.__getitem__)]
                selected.append({**best, "split": split})
                selected_members.add(best["label_member"])
                used_groups[best["provisional_group_id"]] += 1
                uncovered.difference_update(best["class_counts"])
        chosen.extend(selected)
    check_group_isolation(chosen)
    return chosen


def check_group_isolation(records):
    owners = {}
    for record in records:
        group, split = record["provisional_group_id"], record["split"]
        if group in owners and owners[group] != split:
            raise ValueError(f"Group crosses splits: {group}")
        owners[group] = split


class BoundedRemoteZip(io.RawIOBase):
    """用标准 zipfile 解析固定归档，网络读取严格要求 HTTP Range。"""

    def __init__(self, cache_root, audit_cache, tail_path, budget=300 * 1024 * 1024,
                 max_request=10 * 1024 * 1024, time_limit=600, opener=None):
        self.pos = 0
        self.cache_root = Path(cache_root)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.budget, self.max_request = budget, max_request
        self.deadline = time.monotonic() + time_limit
        self.opener = opener or urllib.request.urlopen
        self.downloaded = 0
        self.reserved = 0
        self.requests = []
        self.lock = threading.Lock()
        self.ranges = []
        self.memory = []
        central = Path(audit_cache) / f"range_{CENTRAL_OFFSET}_{CENTRAL_BYTES}.bin"
        central_data = central.read_bytes()
        if len(central_data) != CENTRAL_BYTES or digest(central_data) != CENTRAL_SHA256:
            raise ValueError("Pinned central directory checksum mismatch")
        self.memory.append((CENTRAL_OFFSET, central_data))
        tail = Path(tail_path).read_bytes()
        self.memory.append((ARCHIVE_BYTES - len(tail), tail))
        for directory in (Path(audit_cache), self.cache_root):
            for path in directory.glob("range_*_*.bin"):
                _, start, length = path.stem.split("_")
                if path.stat().st_size != int(length):
                    raise ValueError(f"Truncated cached range: {path}")
                self.ranges.append((int(start), int(length), path))

    def seekable(self):
        return True

    def readable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, offset, whence=0):
        self.pos = (0 if whence == 0 else self.pos if whence == 1 else ARCHIVE_BYTES) + offset
        if self.pos < 0 or self.pos > ARCHIVE_BYTES:
            raise ValueError("Invalid archive seek")
        return self.pos

    def _cached(self, start, length):
        for cached_start, data in self.memory:
            offset = start - cached_start
            if offset >= 0 and offset + length <= len(data):
                return data[offset:offset + length]
        with self.lock:
            ranges = list(self.ranges)
        for cached_start, cached_length, path in ranges:
            offset = start - cached_start
            if offset >= 0 and offset + length <= cached_length:
                with path.open("rb") as source:
                    source.seek(offset)
                    data = source.read(length)
                if len(data) != length:
                    raise ValueError("Truncated cached range")
                return data
        return None

    def fetch(self, start, length):
        if start < 0 or length < 0 or start + length > ARCHIVE_BYTES:
            raise ValueError("Invalid archive range")
        if length == 0:
            return b""
        cached = self._cached(start, length)
        if cached is not None:
            return cached
        with self.lock:
            if (length > self.max_request or self.reserved + length > self.budget
                    or time.monotonic() >= self.deadline):
                raise ValueError("Archive request exceeds byte or time budget")
            self.reserved += length
        request = urllib.request.Request(ARCHIVE_URL, headers={
            "Range": f"bytes={start}-{start + length - 1}",
            "User-Agent": "sar-offline-pilot/1"})
        with self.opener(request, timeout=25) as response:
            expected = f"bytes {start}-{start + length - 1}/{ARCHIVE_BYTES}"
            if response.status != 206 or response.headers.get("Content-Range") != expected:
                raise ValueError("Server ignored bounded archive range")
            data = response.read(length + 1)
            if len(data) != length:
                raise ValueError("Unexpected range response length")
        path = self.cache_root / f"range_{start}_{length}.bin"
        path.write_bytes(data)
        with self.lock:
            self.downloaded += len(data)
            self.ranges.append((start, length, path))
            self.requests.append({"offset": start, "length": length, "sha256": digest(data)})
        return data

    def read(self, length=-1):
        if length < 0:
            length = ARCHIVE_BYTES - self.pos
        length = min(length, ARCHIVE_BYTES - self.pos)
        result = self.fetch(self.pos, length)
        self.pos += len(result)
        return result


def validate_member(info, limit):
    path = PurePosixPath(info.filename)
    if (path.is_absolute() or ".." in path.parts or info.file_size > limit
            or info.flag_bits & 1 or (info.external_attr >> 16) & 0o170000 == 0o120000):
        raise ValueError(f"Unsafe or oversized archive member: {info.filename}")
