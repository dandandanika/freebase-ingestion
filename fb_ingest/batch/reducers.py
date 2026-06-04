from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator


def iter_jsonl(path: Path) -> Iterator[dict]:
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def iter_jsonl_dir(path: Path) -> Iterator[tuple[Path, dict]]:
    if not path.exists():
        return
    for file_path in sorted(path.glob("*.jsonl")):
        for row in iter_jsonl(file_path):
            yield file_path, row


def group_jsonl_by_key(path: Path, key: str) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    if not path.exists():
        return grouped

    for _, row in iter_jsonl_dir(path):
        value = row.get(key)
        if value is None:
            continue
        grouped.setdefault(value, []).append(row)
    return grouped


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
