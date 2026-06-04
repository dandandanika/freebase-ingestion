from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fb_ingest.cvt.models import CVTRecord


@dataclass(frozen=True)
class FlattenDecision:
    status: str
    reason: str
    source_mid: str | None = None
    target_mid: str | None = None
    rel_type: str | None = None
    predicate: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)


def _normalize_rel_type(predicate: str) -> str:
    token = predicate.strip("/").replace("/", "_").replace(".", "_")
    return token.upper()


def decide_cvt_flatten(
    record: CVTRecord,
    rel_type: str | None = None,
) -> FlattenDecision:
    """
    Safe flattening rule:

    Flatten only if:
      - exactly one incoming fact,
      - exactly one outgoing entity fact,
      - zero chained CVTs,
      - any remaining outgoing literals become relationship properties.

    Otherwise retain as explicit/reified CVT.
    """
    if len(record.chained_cvts) > 0:
        return FlattenDecision(
            status="retain",
            reason="chained_cvts",
        )

    if len(record.incoming) != 1:
        return FlattenDecision(
            status="retain",
            reason="incoming_arity_not_1",
        )

    if len(record.outgoing_entities) != 1:
        return FlattenDecision(
            status="retain",
            reason="outgoing_entity_arity_not_1",
        )

    incoming = record.incoming[0]
    outgoing = record.outgoing_entities[0]

    props: dict[str, Any] = {"cvt_mid": record.cvt_mid}

    for lit in record.outgoing_literals:
        key = lit.predicate.strip("/").replace("/", ".")
        if key in props:
            existing = props[key]
            if isinstance(existing, list):
                if lit.parsed_value not in existing:
                    existing.append(lit.parsed_value)
            else:
                if existing != lit.parsed_value:
                    props[key] = [existing, lit.parsed_value]
        else:
            props[key] = lit.parsed_value

    return FlattenDecision(
        status="flatten",
        reason="safe_binary_cvt",
        source_mid=incoming.source_mid,
        target_mid=outgoing.target_mid,
        rel_type=rel_type or _normalize_rel_type(incoming.predicate),
        predicate=incoming.predicate,
        properties=props,
    )
