from __future__ import annotations

import json
from pathlib import Path

from .registry import PredicateCatalog


def write_predicate_catalog(catalog: PredicateCatalog, out_path: Path) -> None:
    payload = {
        "predicate_counts": catalog.predicate_counts,
        "literal_predicate_counts": catalog.literal_predicate_counts,
        "object_predicate_counts": catalog.object_predicate_counts,
        "datatype_counts": catalog.datatype_counts,
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )