from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SchemaRegistry:
    mediator_types: set[str] = field(default_factory=set)
    expected_type: dict[str, str] = field(default_factory=dict)
    master_property: dict[str, str] = field(default_factory=dict)
    reverse_property: dict[str, str] = field(default_factory=dict)


@dataclass
class CVTIndex:
    node_types: dict[str, list[str]] = field(default_factory=dict)

    def add_type(self, node: str, fb_type: str) -> None:
        self.node_types.setdefault(node, []).append(fb_type)

    def is_cvt_instance(self, node: str, mediator_types: set[str]) -> bool:
        return any(fb_type in mediator_types for fb_type in self.node_types.get(node, []))


@dataclass
class PredicateCatalog:
    predicate_counts: dict[str, int] = field(default_factory=dict)
    literal_predicate_counts: dict[str, int] = field(default_factory=dict)
    object_predicate_counts: dict[str, int] = field(default_factory=dict)
    datatype_counts: dict[str, int] = field(default_factory=dict)

    def observe(self, predicate: str, is_literal: bool, datatype: str | None) -> None:
        self.predicate_counts[predicate] = self.predicate_counts.get(predicate, 0) + 1

        if is_literal:
            self.literal_predicate_counts[predicate] = (
                self.literal_predicate_counts.get(predicate, 0) + 1
            )
            if datatype is not None:
                self.datatype_counts[datatype] = self.datatype_counts.get(datatype, 0) + 1
        else:
            self.object_predicate_counts[predicate] = (
                self.object_predicate_counts.get(predicate, 0) + 1
            )
