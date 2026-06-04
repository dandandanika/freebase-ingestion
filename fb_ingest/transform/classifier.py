from __future__ import annotations

from dataclasses import dataclass

from config.special_predicates import (
    TOPIC_ALIAS,
    TOPIC_DESCRIPTION,
    TYPE_OBJECT_KEY,
    TYPE_OBJECT_NAME,
    TYPE_OBJECT_TYPE,
)
from fb_ingest.cvt.detector import Phase1Artifacts
from fb_ingest.models import Triple


@dataclass(frozen=True)
class Classification:
    kind: str
    reason: str


SPECIAL_PREDICATES = {
    TYPE_OBJECT_TYPE,
    TYPE_OBJECT_NAME,
    TOPIC_ALIAS,
    TOPIC_DESCRIPTION,
    TYPE_OBJECT_KEY,
}


def classify_triple(
    triple: Triple,
    artifacts: Phase1Artifacts,
) -> Classification:
    if triple.p == TYPE_OBJECT_TYPE:
        return Classification(kind="type_fact", reason="type_assignment")

    if triple.p in SPECIAL_PREDICATES:
        return Classification(kind="special_fact", reason="special_predicate")

    if triple.is_literal:
        if artifacts.is_cvt_instance(triple.s):
            return Classification(kind="cvt_literal_fact", reason="literal_from_cvt")
        return Classification(kind="direct_literal_fact", reason="direct_literal")

    if artifacts.is_cvt_instance(triple.s):
        if artifacts.is_cvt_instance(triple.o, via_predicate=triple.p):
            return Classification(kind="cvt_chain_fact", reason="cvt_to_cvt")
        return Classification(kind="cvt_entity_out_fact", reason="entity_from_cvt")

    if artifacts.is_cvt_instance(triple.o, via_predicate=triple.p):
        return Classification(kind="cvt_incoming_fact", reason="entity_to_cvt")

    return Classification(kind="direct_edge_fact", reason="entity_to_entity")
