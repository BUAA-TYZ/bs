"""固定官方资源地址和文件哈希，避免下载完整 DOTA 数据。"""

import hashlib
import io
import json
from pathlib import Path
import tarfile
import urllib.request
import zipfile

from PIL import Image


ASSETS = {
    "yolo11n-obb.pt": {
        "url": "https://github.com/ultralytics/assets/releases/download/v8.3.0/yolo11n-obb.pt",
        "sha256": "b62898ebf38940ca4df323863e45ee9d84a1a46d5d11ebdde529fb33aa9f3a32",
    },
    "dota8.zip": {
        "url": "https://github.com/ultralytics/assets/releases/download/v0.0.0/dota8.zip",
        "sha256": "6f901955660fc5c613a0389f44240db359f07741effb48afc99d17155ee3e7ec",
    },
    "boats.jpg": {
        "url": "https://github.com/ultralytics/assets/releases/download/v0.0.0/boats.jpg",
        "sha256": "8c5ada657cf8110a9f8aaac954c1dd96cde0187315b581276c32b0d1863e756f",
    },
}


def sha256(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def prepare_assets(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    for name, asset in ASSETS.items():
        path = root / name
        if not path.exists():
            request = urllib.request.Request(asset["url"], headers={"User-Agent": "workload-benchmark/1"})
            with urllib.request.urlopen(request, timeout=60) as response, path.open("wb") as output:
                total = 0
                while chunk := response.read(1024 * 1024):
                    total += len(chunk)
                    if total > 20 * 1024 * 1024:
                        raise ValueError("Unexpected asset larger than 20 MiB")
                    output.write(chunk)
        if sha256(path) != asset["sha256"]:
            raise ValueError(f"Asset checksum mismatch: {name}")
    with zipfile.ZipFile(root / "dota8.zip") as archive:
        for entry in archive.infolist():
            path = (root / entry.filename).resolve()
            if not path.is_relative_to(root.resolve()):
                raise ValueError("Unsafe archive member")
            # 不加载或提取上游 pickle cache，只保留实际影像、标签和授权说明。
            if entry.is_dir() or path.suffix == ".cache":
                continue
            if entry.file_size > 10 * 1024 * 1024:
                raise ValueError("Unexpected archive member size")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(archive.read(entry))
    return [{"name": name, **asset, "bytes": (root / name).stat().st_size}
            for name, asset in ASSETS.items()]


def prepare_dota_mirror(root, count=2):
    """流式读取第三方公开镜像前部，压缩网络数据硬限 100 MB。"""
    root = Path(root) / "dota_mirror"
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "provenance.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        for item in manifest["images"]:
            for kind in ("image", "label"):
                if sha256(root / item[kind]) != item[f"{kind}_sha256"]:
                    raise ValueError("Mirror asset checksum mismatch")
        return manifest
    revision = "672e63236622f7da6ee37fca44c50ac368b77cab"
    base = f"https://huggingface.co/datasets/isaaccorley/dota/resolve/{revision}/"

    class LimitedReader:
        def __init__(self, response):
            self.response, self.count = response, 0

        def read(self, size=-1):
            size = min(65536 if size < 0 else size, 100_000_000 - self.count + 1)
            chunk = self.response.read(size)
            self.count += len(chunk)
            if self.count > 100_000_000:
                raise ValueError("Mirror network budget exceeded")
            return chunk

    images = []
    image_url = base + "dotav1.0_images_val.tar.gz"
    with urllib.request.urlopen(image_url, timeout=30) as response:
        etag = response.headers.get("ETag")
        stream = LimitedReader(response)
        with tarfile.open(fileobj=stream, mode="r|gz") as archive:
            for item in archive:
                if not item.isfile() or not item.name.lower().endswith(".png"):
                    continue
                if item.size > 50_000_000:
                    continue
                data = archive.extractfile(item).read()
                with Image.open(io.BytesIO(data)) as image:
                    width, height = image.size
                if min(width, height) < 1024:
                    continue
                path = root / Path(item.name).name
                path.write_bytes(data)
                images.append({"image": path.name, "image_sha256": sha256(path),
                               "width": width, "height": height, "archive_member": item.name})
                if len(images) == count:
                    break
        read_bytes = stream.count
    label_url = base + "dotav1.0_annotations_val.tar.gz"
    with urllib.request.urlopen(label_url, timeout=30) as response:
        label_archive = response.read(1_000_001)
    if len(label_archive) > 1_000_000:
        raise ValueError("Unexpected annotation archive size")
    wanted = {Path(item["image"]).stem: item for item in images}
    with tarfile.open(fileobj=io.BytesIO(label_archive), mode="r:gz") as archive:
        for member in archive:
            stem = Path(member.name).stem
            if member.isfile() and stem in wanted and member.name.endswith(".txt"):
                path = root / (stem + ".txt")
                path.write_bytes(archive.extractfile(member).read())
                wanted[stem].update(label=path.name, label_sha256=sha256(path))
    if not images or any("label" not in item for item in images):
        raise ValueError("No matching full-image DOTA labels")
    manifest = {"repository": "isaaccorley/dota", "revision": revision,
                "provenance": "third-party mirror of DOTA val; not an official DOTA host",
                "image_archive_url": image_url, "image_archive_etag": etag,
                "compressed_image_bytes_read": read_bytes, "network_limit_bytes": 100_000_000,
                "label_archive_url": label_url,
                "label_archive_sha256": hashlib.sha256(label_archive).hexdigest(), "images": images}
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest
