from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CanonicalPredicate:
    predicate: str
    canonical_predicate: str
    direction: str  # "forward" or "reverse"
    rel_type: str


def default_rel_type(predicate: str) -> str:
    return predicate.strip("/").replace("/", "_").replace(".", "_").upper()


class PredicateCanonicalizer:
    """
    Canonicalizes Freebase predicates using Phase 1 reverse/master-property maps.
    """

    def __init__(
        self,
        master_property: dict[str, str] | None = None,
        reverse_property: dict[str, str] | None = None,
    ):
        self.master_property = master_property or {}
        self.reverse_property = reverse_property or {}

    @classmethod
    def from_schema_dir(cls, schema_dir: Path) -> "PredicateCanonicalizer":
        master_path = schema_dir / "master_property.json"
        reverse_path = schema_dir / "reverse_property.json"

        master = {}
        reverse = {}

        if master_path.exists():
            master = json.loads(master_path.read_text(encoding="utf-8"))
        if reverse_path.exists():
            reverse = json.loads(reverse_path.read_text(encoding="utf-8"))

        return cls(master_property=master, reverse_property=reverse)

    def canonicalize(self, predicate: str) -> CanonicalPredicate:
        if predicate in self.master_property:
            canonical = self.master_property[predicate]
            return CanonicalPredicate(
                predicate=predicate,
                canonical_predicate=canonical,
                direction="reverse",
                rel_type=default_rel_type(canonical),
            )

        return CanonicalPredicate(
            predicate=predicate,
            canonical_predicate=predicate,
            direction="forward",
            rel_type=default_rel_type(predicate),
        )
