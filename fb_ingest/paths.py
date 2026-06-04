from __future__ import annotations

from pathlib import Path


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def phase1_paths(work_dir: Path) -> dict[str, Path]:
    base = ensure_dir(work_dir / "phase1")
    return {
        "base": base,
        "schema": ensure_dir(base / "schema"),
        "partitions": ensure_dir(base / "partitions"),
        "stats": ensure_dir(base / "stats"),
        "manifests": ensure_dir(base / "manifests"),
        "logs": ensure_dir(base / "logs"),
    }