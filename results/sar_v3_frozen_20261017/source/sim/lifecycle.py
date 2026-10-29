from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


class TileLifecycleLogger:
    """Append-only JSONL logger for tile lifecycle events."""

    def __init__(self, path: Optional[str]) -> None:
        self.path = path or ""
        self._fp = None
        if self.path:
            p = Path(self.path)
            p.parent.mkdir(parents=True, exist_ok=True)
            self._fp = p.open("w", encoding="utf-8")

    @property
    def enabled(self) -> bool:
        return self._fp is not None

    def log(self, record: Dict[str, Any]) -> None:
        if self._fp is None:
            return
        self._fp.write(json.dumps(record, ensure_ascii=False) + "\n")

    def close(self) -> None:
        if self._fp is not None:
            self._fp.close()
            self._fp = None

