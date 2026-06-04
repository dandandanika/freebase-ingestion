from __future__ import annotations

from pathlib import Path

from fb_ingest.cvt.node_type_index import NodeTypeIndex


class Phase1Artifacts:
    """
    Loader and lookup helper for Phase 1 schema artifacts.

    Expected files:
      - mediator_types.json
      - expected_type.json
      - node_types.json and/or phase1/partitions/node_types/*.jsonl
    """

    def __init__(self, type_index: NodeTypeIndex):
        self.type_index = type_index

    @classmethod
    def from_schema_dir(
        cls,
        schema_dir: Path,
        work_dir: Path | None = None,
    ) -> "Phase1Artifacts":
        return cls(NodeTypeIndex.from_schema_dir(schema_dir, work_dir=work_dir))

    @property
    def mediator_types(self) -> set[str]:
        return self.type_index.mediator_types

    @property
    def node_types(self) -> dict[str, list[str]]:
        return self.type_index.node_types

    def is_mediator_type(self, fb_type: str) -> bool:
        return fb_type in self.mediator_types

    def get_node_types(self, node: str) -> list[str]:
        return self.type_index.get_node_types(node)

    def add_type(self, node: str, fb_type: str) -> None:
        self.type_index.add_type(node, fb_type)

    def is_cvt_instance(self, node: str, via_predicate: str | None = None) -> bool:
        return self.type_index.is_cvt_instance(node, via_predicate=via_predicate)

    def classify_node_kind(self, node: str) -> str:
        if self.is_cvt_instance(node):
            return "cvt"
        return "entity"
