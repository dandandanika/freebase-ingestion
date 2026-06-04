from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fb_ingest.transform.canonicalize import CanonicalPredicate


PROMOTED_RELATIONSHIPS = {
    "/people/person/place_of_birth": "PLACE_OF_BIRTH",
    "/people/person/nationality": "NATIONALITY",
    "/people/person/gender": "GENDER",
    "/people/person/profession": "PROFESSION",
    "/organization/founder/organizations_founded": "FOUNDER_OF",
    "/people/person/education": "EDUCATED_AT",
    "/people/person/employment_history": "EMPLOYED_AT",
    "/film/film/directed_by": "DIRECTED_BY",
    "/film/film/produced_by": "PRODUCED_BY",
    "/music/recording/artist": "PERFORMED_BY",
}


@dataclass
class EdgeRecord:
    source_mid: str
    target_mid: str
    rel_type: str
    predicate: str
    properties: dict[str, Any] = field(default_factory=dict)


def build_direct_edge(
    source_mid: str,
    target_mid: str,
    canonical: CanonicalPredicate,
) -> EdgeRecord:
    rel_type = PROMOTED_RELATIONSHIPS.get(
        canonical.canonical_predicate,
        "FB_REL",
    )

    props = {}
    if rel_type == "FB_REL":
        props["predicate"] = canonical.canonical_predicate

    if canonical.direction == "reverse":
        source_mid, target_mid = target_mid, source_mid

    return EdgeRecord(
        source_mid=source_mid,
        target_mid=target_mid,
        rel_type=rel_type,
        predicate=canonical.canonical_predicate,
        properties=props,
    )


def build_flattened_cvt_edge(
    source_mid: str,
    target_mid: str,
    predicate: str,
    rel_type: str,
    properties: dict[str, Any],
) -> EdgeRecord:
    return EdgeRecord(
        source_mid=source_mid,
        target_mid=target_mid,
        rel_type=rel_type,
        predicate=predicate,
        properties=dict(properties),
    )
