from __future__ import annotations

import json
from pathlib import Path

from fb_ingest.batch.reducers import iter_jsonl_dir


class NodeTypeIndex:
    """
    Scalable node-type lookup for CVT detection.

    Loads mediator types and predicate expected types from Phase 1 schema artifacts,
    node types from the monolithic file and/or partitioned spool files, and supports
    incremental updates while streaming Phase 2 triples.
    """

    def __init__(
        self,
        mediator_types: set[str],
        expected_type: dict[str, str] | None = None,
        node_types: dict[str, list[str]] | None = None,
    ):
        self.mediator_types = mediator_types
        self.expected_type = expected_type or {}
        self.node_types = node_types or {}
        self.known_cvt_instances: set[str] = set()

    @classmethod
    def from_schema_dir(cls, schema_dir: Path, work_dir: Path | None = None) -> "NodeTypeIndex":
        mediator_types: set[str] = set()
        expected_type: dict[str, str] = {}
        node_types: dict[str, list[str]] = {}

        mediator_path = schema_dir / "mediator_types.json"
        if mediator_path.exists():
            mediator_types = set(json.loads(mediator_path.read_text(encoding="utf-8")))

        expected_path = schema_dir / "expected_type.json"
        if expected_path.exists():
            expected_type = json.loads(expected_path.read_text(encoding="utf-8"))

        node_types_path = schema_dir / "node_types.json"
        if node_types_path.exists():
            node_types = json.loads(node_types_path.read_text(encoding="utf-8"))

        index = cls(
            mediator_types=mediator_types,
            expected_type=expected_type,
            node_types=node_types,
        )

        if work_dir is not None:
            partitions_dir = work_dir / "phase1" / "partitions" / "node_types"
            if partitions_dir.exists():
                index.load_partitioned_types(partitions_dir)

        return index

    def load_partitioned_types(self, partitions_dir: Path) -> None:
        for _, row in iter_jsonl_dir(partitions_dir):
            node = row.get("node")
            fb_type = row.get("type")
            if node and fb_type:
                self.add_type(node, fb_type)

    def add_type(self, node: str, fb_type: str) -> None:
        types = self.node_types.setdefault(node, [])
        if fb_type not in types:
            types.append(fb_type)
        if fb_type in self.mediator_types:
            self.known_cvt_instances.add(node)

    def get_node_types(self, node: str) -> list[str]:
        return self.node_types.get(node, [])

    def predicate_targets_mediator(self, predicate: str | None) -> bool:
        if not predicate:
            return False
        expected = self.expected_type.get(predicate)
        return expected is not None and expected in self.mediator_types

    def mark_cvt_instance(self, node: str) -> None:
        self.known_cvt_instances.add(node)

    def is_cvt_instance(
        self,
        node: str,
        *,
        via_predicate: str | None = None,
    ) -> bool:
        if node in self.known_cvt_instances:
            return True

        if any(fb_type in self.mediator_types for fb_type in self.get_node_types(node)):
            self.known_cvt_instances.add(node)
            return True

        if self.predicate_targets_mediator(via_predicate):
            self.known_cvt_instances.add(node)
            return True

        return False
