from __future__ import annotations

import json
from pathlib import Path


def write_manifest(out_path: Path, payload: dict) -> None:
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
