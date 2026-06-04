from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class JsonlSpoolManager:
    """
    Rotating JSONL writer keyed by logical bucket.
    """

    def __init__(self, base_dir: Path, prefix: str, max_records: int = 250_000):
        self.base_dir = base_dir
        self.prefix = prefix
        self.max_records = max_records
        self.handles: dict[str, tuple[Any, int, int]] = {}
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def write(self, bucket: str, obj: dict) -> None:
        if bucket not in self.handles:
            self.handles[bucket] = self._open(bucket, part=0)

        fh, part, count = self.handles[bucket]

        if count >= self.max_records:
            fh.close()
            self.handles[bucket] = self._open(bucket, part=part + 1)
            fh, part, count = self.handles[bucket]

        fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self.handles[bucket] = (fh, part, count + 1)

    def _open(self, bucket: str, part: int):
        path = self.base_dir / f"{self.prefix}_{bucket}_{part:05d}.jsonl"
        fh = open(path, "w", encoding="utf-8")
        return fh, part, 0

    def close(self) -> None:
        for fh, _, _ in self.handles.values():
            fh.close()
        self.handles.clear()
