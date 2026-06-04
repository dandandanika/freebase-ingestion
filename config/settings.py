from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    input_path: Path
    work_dir: Path
    partition_count: int = 256
    spool_max_records: int = 250_000
    log_every: int = 1_000_000
    sample_count: int = 0

    @property
    def phase1_dir(self) -> Path:
        return self.work_dir / "phase1"


def build_settings(
    input_path: str,
    work_dir: str,
    partition_count: int = 256,
    spool_max_records: int = 250_000,
    log_every: int = 1_000_000,
    sample_count: int = 0,
) -> Settings:
    return Settings(
        input_path=Path(input_path),
        work_dir=Path(work_dir),
        partition_count=partition_count,
        spool_max_records=spool_max_records,
        log_every=log_every,
        sample_count=sample_count,
    )
