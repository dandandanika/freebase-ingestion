from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from config.settings import Settings
from config.special_predicates import (
    TOPIC_ALIAS,
    TOPIC_DESCRIPTION,
    TYPE_OBJECT_KEY,
    TYPE_OBJECT_NAME,
    TYPE_OBJECT_TYPE,
)
from fb_ingest.batch.manifest import write_manifest
from fb_ingest.batch.partitioning import stable_partition
from fb_ingest.batch.spool import JsonlSpoolManager
from fb_ingest.cvt.detector import Phase1Artifacts
from fb_ingest.logging_utils import get_logger
from fb_ingest.models import Phase2StageStats
from fb_ingest.parse.literals import parse_typed_literal
from fb_ingest.parse.ntriples import TripleParseError, parse_line_auto
from fb_ingest.parse.reader import iter_lines
from fb_ingest.paths import ensure_dir
from fb_ingest.transform.canonicalize import PredicateCanonicalizer
from fb_ingest.transform.classifier import classify_triple
from fb_ingest.transform.property_mapper import predicate_to_property_key
from fb_ingest.validation.counters import write_stats
from fb_ingest.validation.samples import SampleCollector


SPECIAL_PREDICATES = {
    TYPE_OBJECT_NAME,
    TOPIC_ALIAS,
    TOPIC_DESCRIPTION,
    TYPE_OBJECT_KEY,
}


def phase2_paths(work_dir: Path) -> dict[str, Path]:
    base = ensure_dir(work_dir / "phase2")
    stage = ensure_dir(base / "stage")
    reduce_dir = ensure_dir(base / "reduce")
    neo4j = ensure_dir(base / "neo4j")
    stats = ensure_dir(base / "stats")
    manifests = ensure_dir(base / "manifests")

    return {
        "base": base,
        "stage": stage,
        "reduce": reduce_dir,
        "neo4j": neo4j,
        "stats": stats,
        "manifests": manifests,
        "stage_direct_literals": ensure_dir(stage / "direct_literals"),
        "stage_direct_edges": ensure_dir(stage / "direct_edges"),
        "stage_cvt_facts": ensure_dir(stage / "cvt_facts"),
        "stage_type_facts": ensure_dir(stage / "type_facts"),
        "stage_special_facts": ensure_dir(stage / "special_facts"),
        "stage_fallback_raw": ensure_dir(stage / "fallback_raw"),
        "reduce_nodes": ensure_dir(reduce_dir / "nodes"),
        "reduce_edges": ensure_dir(reduce_dir / "edges"),
        "reduce_retained_cvts": ensure_dir(reduce_dir / "retained_cvts"),
        "reduce_fallback": ensure_dir(reduce_dir / "fallback"),
        "reduce_audit": ensure_dir(reduce_dir / "audit"),
    }


def _bucket_for_mid(mid: str, partition_count: int) -> str:
    return f"{stable_partition(mid, partition_count):03d}"


def _stage_sample(
    samples: SampleCollector,
    category: str,
    triple,
    classification,
    extra: dict | None = None,
) -> None:
    record = {
        "classification": classification.kind,
        "reason": classification.reason,
        "subject_mid": triple.s,
        "predicate": triple.p,
        "object": triple.o,
        "is_literal": triple.is_literal,
        "line_no": triple.line_no,
    }
    if extra:
        record.update(extra)
    samples.add(category, record)


def run_phase2_stage(settings: Settings) -> dict:
    logger = get_logger("fb_ingest.phase2_stage")
    paths = phase2_paths(settings.work_dir)

    phase1_schema_dir = settings.work_dir / "phase1" / "schema"
    artifacts = Phase1Artifacts.from_schema_dir(
        phase1_schema_dir,
        work_dir=settings.work_dir,
    )
    canonicalizer = PredicateCanonicalizer.from_schema_dir(phase1_schema_dir)
    stats = Phase2StageStats()
    samples = SampleCollector(settings.sample_count)

    direct_literal_spool = JsonlSpoolManager(
        paths["stage_direct_literals"], "direct_literals", settings.spool_max_records
    )
    direct_edge_spool = JsonlSpoolManager(
        paths["stage_direct_edges"], "direct_edges", settings.spool_max_records
    )
    cvt_fact_spool = JsonlSpoolManager(
        paths["stage_cvt_facts"], "cvt_facts", settings.spool_max_records
    )
    type_fact_spool = JsonlSpoolManager(
        paths["stage_type_facts"], "type_facts", settings.spool_max_records
    )
    special_fact_spool = JsonlSpoolManager(
        paths["stage_special_facts"], "special_facts", settings.spool_max_records
    )
    fallback_spool = JsonlSpoolManager(
        paths["stage_fallback_raw"], "fallback_raw", settings.spool_max_records
    )

    try:
        for line_no, line in iter_lines(settings.input_path):
            try:
                triple = parse_line_auto(line, line_no)
            except TripleParseError:
                stats.parse_errors += 1
                continue

            stats.triples_read += 1
            classification = classify_triple(triple, artifacts)

            if classification.kind == "type_fact":
                stats.type_facts += 1
                artifacts.add_type(triple.s, triple.o)
                bucket = _bucket_for_mid(triple.s, settings.partition_count)
                type_fact_spool.write(bucket, {"node": triple.s, "type": triple.o})
                _stage_sample(samples, "type_fact", triple, classification)
                continue

            if classification.kind == "special_fact":
                stats.special_facts += 1
                payload = {}
                if triple.p in {TYPE_OBJECT_NAME, TOPIC_ALIAS, TOPIC_DESCRIPTION}:
                    payload = {"text": triple.lexical, "lang": triple.lang}
                elif triple.p == TYPE_OBJECT_KEY:
                    payload = {"value": triple.lexical or triple.o}
                else:
                    payload = {"raw": triple.o}

                bucket = _bucket_for_mid(triple.s, settings.partition_count)
                special_fact_spool.write(
                    bucket,
                    {
                        "subject_mid": triple.s,
                        "predicate": triple.p,
                        "payload": payload,
                        "line_no": triple.line_no,
                    },
                )
                _stage_sample(samples, "special_fact", triple, classification, {"payload": payload})
                continue

            if classification.kind == "direct_literal_fact":
                stats.direct_literal_facts += 1
                lit = parse_typed_literal(
                    lexical=triple.lexical or "",
                    datatype=triple.datatype,
                    lang=triple.lang,
                )
                bucket = _bucket_for_mid(triple.s, settings.partition_count)
                direct_literal_spool.write(
                    bucket,
                    {
                        "subject_mid": triple.s,
                        "predicate": triple.p,
                        "property_key": predicate_to_property_key(triple.p),
                        "lexical": lit.lexical,
                        "parsed_value": lit.value,
                        "value_kind": lit.value_kind,
                        "datatype": lit.datatype,
                        "lang": lit.lang,
                        "line_no": triple.line_no,
                    },
                )
                _stage_sample(
                    samples,
                    "direct_literal",
                    triple,
                    classification,
                    {"parsed_value": lit.value, "value_kind": lit.value_kind},
                )
                continue

            if classification.kind == "direct_edge_fact":
                stats.direct_edge_facts += 1
                canonical = canonicalizer.canonicalize(triple.p)
                bucket = _bucket_for_mid(triple.s, settings.partition_count)
                direct_edge_spool.write(
                    bucket,
                    {
                        "source_mid": triple.s,
                        "predicate": triple.p,
                        "target_mid": triple.o,
                        "canonical_predicate": canonical.canonical_predicate,
                        "direction": canonical.direction,
                        "rel_type": canonical.rel_type,
                        "line_no": triple.line_no,
                    },
                )
                _stage_sample(
                    samples,
                    "direct_edge",
                    triple,
                    classification,
                    {
                        "target_mid": triple.o,
                        "rel_type": canonical.rel_type,
                    },
                )
                continue

            if classification.kind == "cvt_incoming_fact":
                stats.cvt_incoming_facts += 1
                bucket = _bucket_for_mid(triple.o, settings.partition_count)
                cvt_fact_spool.write(
                    bucket,
                    {
                        "kind": "incoming",
                        "source_mid": triple.s,
                        "predicate": triple.p,
                        "cvt_mid": triple.o,
                        "line_no": triple.line_no,
                    },
                )
                _stage_sample(
                    samples,
                    "cvt_incoming",
                    triple,
                    classification,
                    {"cvt_mid": triple.o},
                )
                continue

            if classification.kind == "cvt_entity_out_fact":
                stats.cvt_entity_out_facts += 1
                bucket = _bucket_for_mid(triple.s, settings.partition_count)
                cvt_fact_spool.write(
                    bucket,
                    {
                        "kind": "entity_out",
                        "cvt_mid": triple.s,
                        "predicate": triple.p,
                        "target_mid": triple.o,
                        "line_no": triple.line_no,
                    },
                )
                _stage_sample(
                    samples,
                    "cvt_entity_out",
                    triple,
                    classification,
                    {"cvt_mid": triple.s, "target_mid": triple.o},
                )
                continue

            if classification.kind == "cvt_literal_fact":
                stats.cvt_literal_out_facts += 1
                lit = parse_typed_literal(
                    lexical=triple.lexical or "",
                    datatype=triple.datatype,
                    lang=triple.lang,
                )
                bucket = _bucket_for_mid(triple.s, settings.partition_count)
                cvt_fact_spool.write(
                    bucket,
                    {
                        "kind": "literal_out",
                        "cvt_mid": triple.s,
                        "predicate": triple.p,
                        "lexical": lit.lexical,
                        "parsed_value": lit.value,
                        "value_kind": lit.value_kind,
                        "datatype": lit.datatype,
                        "lang": lit.lang,
                        "line_no": triple.line_no,
                    },
                )
                _stage_sample(
                    samples,
                    "cvt_literal_out",
                    triple,
                    classification,
                    {"cvt_mid": triple.s, "parsed_value": lit.value},
                )
                continue

            if classification.kind == "cvt_chain_fact":
                stats.cvt_chain_facts += 1
                bucket = _bucket_for_mid(triple.s, settings.partition_count)
                cvt_fact_spool.write(
                    bucket,
                    {
                        "kind": "cvt_chain",
                        "cvt_mid": triple.s,
                        "predicate": triple.p,
                        "target_cvt_mid": triple.o,
                        "line_no": triple.line_no,
                    },
                )
                _stage_sample(
                    samples,
                    "cvt_chain",
                    triple,
                    classification,
                    {"cvt_mid": triple.s, "target_cvt_mid": triple.o},
                )
                continue

            stats.fallback_raw_records += 1
            fallback_spool.write(
                _bucket_for_mid(triple.s, settings.partition_count),
                {
                    "category": "unclassified",
                    "subject": triple.s,
                    "predicate": triple.p,
                    "object_value": triple.o,
                    "context": {
                        "is_literal": triple.is_literal,
                        "line_no": triple.line_no,
                    },
                },
            )
            _stage_sample(samples, "unclassified", triple, classification)

            if stats.triples_read and stats.triples_read % settings.log_every == 0:
                logger.info("Phase 2 stage processed %s triples", stats.triples_read)

    finally:
        direct_literal_spool.close()
        direct_edge_spool.close()
        cvt_fact_spool.close()
        type_fact_spool.close()
        special_fact_spool.close()
        fallback_spool.close()

    stats_path = paths["stats"] / "phase2_stage_stats.json"
    write_stats(stats, stats_path)
    sample_path = samples.write(paths["base"], "phase2_stage", filename="stage_samples.json")

    manifest = {
        "phase": "phase2_stage",
        "input_path": str(settings.input_path),
        "phase1_schema_dir": str(phase1_schema_dir),
        "partition_count": settings.partition_count,
        "spool_max_records": settings.spool_max_records,
        "stats": asdict(stats),
        "artifacts": {
            "direct_literals_dir": str(paths["stage_direct_literals"]),
            "direct_edges_dir": str(paths["stage_direct_edges"]),
            "cvt_facts_dir": str(paths["stage_cvt_facts"]),
            "type_facts_dir": str(paths["stage_type_facts"]),
            "special_facts_dir": str(paths["stage_special_facts"]),
            "fallback_raw_dir": str(paths["stage_fallback_raw"]),
            "stats_file": str(stats_path),
            "samples_file": str(sample_path) if sample_path else None,
        },
    }
    write_manifest(paths["manifests"] / "phase2_stage_manifest.json", manifest)
    logger.info("Phase 2 stage complete: %s triples read", stats.triples_read)
    return manifest
