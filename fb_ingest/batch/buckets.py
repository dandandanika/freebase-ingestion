from __future__ import annotations

from pathlib import Path

from fb_ingest.batch.reducers import iter_jsonl


def bucket_from_spool_filename(path: Path) -> str:
    """Extract the partition bucket id from a spool file name (prefix_bucket_part.jsonl)."""
    parts = path.stem.split("_")
    if len(parts) < 2:
        raise ValueError(f"Unexpected spool filename: {path.name}")
    return parts[-2]


def discover_buckets(stage_dirs: list[Path], partition_count: int) -> list[str]:
    """
    Collect partition bucket ids present in staged spool directories.

    Falls back to the full partition range when no spool files exist yet.
    """
    buckets: set[str] = set()
    for stage_dir in stage_dirs:
        if not stage_dir.exists():
            continue
        for path in stage_dir.glob("*_*.jsonl"):
            buckets.add(bucket_from_spool_filename(path))

    if not buckets:
        return [f"{idx:03d}" for idx in range(partition_count)]
    return sorted(buckets)


def iter_spool_bucket(stage_dir: Path, prefix: str, bucket: str):
    """Yield rows from all spool parts for one logical bucket."""
    if not stage_dir.exists():
        return
    for file_path in sorted(stage_dir.glob(f"{prefix}_{bucket}_*.jsonl")):
        yield from iter_jsonl(file_path)
