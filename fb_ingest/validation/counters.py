from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from fb_ingest.models import Phase1Stats


def write_stats(stats: Phase1Stats, out_path: Path) -> None:
    out_path.write_text(
        json.dumps(asdict(stats), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
