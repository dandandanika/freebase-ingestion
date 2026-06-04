from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path

from fb_ingest.parse.literals import parse_typed_literal
from fb_ingest.parse.ntriples import Triple
from .registry import SchemaRegistry, CVTIndex, PredicateCatalog


TYPE_OBJECT_TYPE = "/type/object/type"
TYPE_OBJECT_KEY = "/type/object/key"
MEDIATOR_HINT = "/freebase/type_hints/mediator"
EXPECTED_TYPE = "/type/property/expected_type"
MASTER_PROPERTY = "/type/property/master_property"
REVERSE_PROPERTY = "/type/property/reverse_property"
BOOL_TRUE = "/type/boolean/true"


@dataclass
class Phase1Stats:
    triples_read: int = 0
    parse_errors: int = 0
    literal_triples: int = 0
    entity_object_triples: int = 0
    mediator_hint_object_true: int = 0
    mediator_hint_literal_true: int = 0
    mediator_types_found: int = 0
    type_assertions: int = 0
    expected_type_triples: int = 0
    master_property_triples: int = 0
    reverse_property_triples: int = 0
    key_predicate_triples: int = 0
    unrecognized_datatypes: int = 0


def apply_schema_observation(
    triple: Triple,
    registry: SchemaRegistry,
    cvt_index: CVTIndex,
    catalog: PredicateCatalog,
    stats: Phase1Stats,
) -> None:
    stats.triples_read += 1

    if triple.is_literal:
        stats.literal_triples += 1
    else:
        stats.entity_object_triples += 1

    catalog.observe(triple.p, triple.is_literal, triple.datatype)

    if triple.p == MEDIATOR_HINT:
        if not triple.is_literal and triple.o == BOOL_TRUE:
            registry.mediator_types.add(triple.s)
            stats.mediator_hint_object_true += 1
            return

        if triple.is_literal:
            lit = parse_typed_literal(
                lexical=triple.lexical or "",
                datatype=triple.datatype,
                lang=triple.lang,
            )
            if lit.value is True:
                registry.mediator_types.add(triple.s)
                stats.mediator_hint_literal_true += 1
            return

    if triple.p == TYPE_OBJECT_TYPE and not triple.is_literal:
        cvt_index.add_type(triple.s, triple.o)
        stats.type_assertions += 1
        return

    if triple.p == EXPECTED_TYPE and not triple.is_literal:
        registry.expected_type[triple.s] = triple.o
        stats.expected_type_triples += 1
        return

    if triple.p == MASTER_PROPERTY and not triple.is_literal:
        registry.master_property[triple.s] = triple.o
        stats.master_property_triples += 1
        return

    if triple.p == REVERSE_PROPERTY and not triple.is_literal:
        registry.reverse_property[triple.s] = triple.o
        stats.reverse_property_triples += 1
        return

    if triple.p == TYPE_OBJECT_KEY:
        stats.key_predicate_triples += 1

    if triple.is_literal and triple.datatype is not None:
        lit = parse_typed_literal(
            lexical=triple.lexical or "",
            datatype=triple.datatype,
            lang=triple.lang,
        )
        if lit.value_kind == "unknown_typed_literal":
            stats.unrecognized_datatypes += 1


def finalize_registry_stats(registry: SchemaRegistry, stats: Phase1Stats) -> None:
    stats.mediator_types_found = len(registry.mediator_types)


def write_registry_artifacts(
    registry: SchemaRegistry,
    cvt_index: CVTIndex,
    out_dir: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "mediator_types.json").write_text(
        json.dumps(sorted(registry.mediator_types), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out_dir / "expected_type.json").write_text(
        json.dumps(registry.expected_type, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out_dir / "master_property.json").write_text(
        json.dumps(registry.master_property, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out_dir / "reverse_property.json").write_text(
        json.dumps(registry.reverse_property, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out_dir / "node_types.json").write_text(
        json.dumps(cvt_index.node_types, ensure_ascii=False),
        encoding="utf-8",
    )


def write_phase1_stats(stats: Phase1Stats, out_path: Path) -> None:
    out_path.write_text(
        json.dumps(asdict(stats), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
