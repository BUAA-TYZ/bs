"""记录源码和运行时版本，避免把不同实现的实验混在一起。"""

import hashlib
from importlib.metadata import version
from pathlib import Path
import platform


def runtime_metadata() -> dict:
    root = Path(__file__).resolve().parent.parent
    sources = sorted((root / "sim").rglob("*.py")) + sorted(root.glob("*.py"))
    digest = hashlib.sha256()
    for source in sources:
        digest.update(str(source.relative_to(root)).encode())
        digest.update(b"\0")
        digest.update(source.read_bytes())
        digest.update(b"\0")
    return {"source_sha256": digest.hexdigest(), "python_version": platform.python_version(),
            "packages": {name: version(name) for name in ("numpy", "torch", "skyfield", "PyYAML")}}
