from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fb_ingest.logging_utils import get_logger
from fb_ingest.paths import ensure_dir


def truncate_embedding(record: dict, preview_dims: int = 8) -> dict:
    """Return a copy with embedding truncated for readable sample files."""
    out = json.loads(json.dumps(record, ensure_ascii=False))

    def _truncate_obj(obj: Any) -> Any:
        if not isinstance(obj, dict):
            return obj
        result = dict(obj)
        props = result.get("properties")
        if isinstance(props, dict) and "embedding" in props:
            embedding = props["embedding"]
            if isinstance(embedding, list):
                props = dict(props)
                props["embedding_preview"] = embedding[:preview_dims]
                props["embedding_dims"] = len(embedding)
                props.pop("embedding", None)
                result["properties"] = props
        return result

    if isinstance(out, list):
        return [_truncate_obj(item) for item in out]
    return _truncate_obj(out)


class SampleCollector:
    """
    Collects a capped number of example records per category for inspection.
    """

    def __init__(self, max_per_category: int = 5):
        self.max_per_category = max_per_category
        self._buckets: dict[str, list[Any]] = {}

    @property
    def enabled(self) -> bool:
        return self.max_per_category > 0

    def add(self, category: str, record: Any) -> None:
        if not self.enabled:
            return
        bucket = self._buckets.setdefault(category, [])
        if len(bucket) >= self.max_per_category:
            return
        bucket.append(record)

    def counts(self) -> dict[str, int]:
        return {category: len(rows) for category, rows in self._buckets.items()}

    def write(
        self,
        out_dir: Path,
        phase: str,
        *,
        filename: str = "samples.json",
        truncate_embeddings: bool = True,
    ) -> Path | None:
        if not self.enabled or not self._buckets:
            return None

        samples_dir = ensure_dir(out_dir / "samples")
        out_path = samples_dir / filename
        categories = self._buckets
        if truncate_embeddings:
            categories = {
                category: truncate_embedding(rows) for category, rows in self._buckets.items()
            }

        payload = {
            "phase": phase,
            "sample_count_per_category": self.max_per_category,
            "categories": categories,
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        logger = get_logger("fb_ingest.samples")
        summary = ", ".join(f"{k}={v}" for k, v in sorted(self.counts().items()))
        logger.info("Wrote samples (%s) -> %s", summary, out_path)
        return out_path


def cvt_record_snapshot(record, decision, flattened_edge: dict | None = None) -> dict:
    return {
        "cvt_mid": record.cvt_mid,
        "types": list(record.types),
        "incoming": [
            {
                "source_mid": fact.source_mid,
                "predicate": fact.predicate,
                "line_no": fact.line_no,
            }
            for fact in record.incoming
        ],
        "outgoing_entities": [
            {
                "predicate": fact.predicate,
                "target_mid": fact.target_mid,
                "line_no": fact.line_no,
            }
            for fact in record.outgoing_entities
        ],
        "outgoing_literals": [
            {
                "predicate": fact.predicate,
                "parsed_value": fact.parsed_value,
                "value_kind": fact.value_kind,
                "line_no": fact.line_no,
            }
            for fact in record.outgoing_literals
        ],
        "chained_cvts": list(record.chained_cvts),
        "decision": {"status": decision.status, "reason": decision.reason},
        "flattened_edge": flattened_edge,
    }
