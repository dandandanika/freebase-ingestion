from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Triple:
    s: str
    p: str
    o: str
    is_literal: bool
    lexical: Optional[str] = None
    lang: Optional[str] = None
    datatype: Optional[str] = None
    line_no: int = 0


@dataclass(frozen=True)
class TypedLiteral:
    lexical: str
    value_kind: str
    value: object
    datatype: Optional[str] = None
    lang: Optional[str] = None


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


@dataclass
class Phase2StageStats:
    triples_read: int = 0
    parse_errors: int = 0
    direct_literal_facts: int = 0
    direct_edge_facts: int = 0
    special_facts: int = 0
    type_facts: int = 0
    cvt_incoming_facts: int = 0
    cvt_entity_out_facts: int = 0
    cvt_literal_out_facts: int = 0
    cvt_chain_facts: int = 0
    fallback_raw_records: int = 0


@dataclass
class Phase2ReduceStats:
    node_records_written: int = 0
    edge_records_written: int = 0
    flattened_cvts: int = 0
    retained_cvts: int = 0
    orphan_cvts: int = 0
    cvt_chains: int = 0
    fallback_records_written: int = 0
    partitions_processed: int = 0
