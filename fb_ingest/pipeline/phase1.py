from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from config.settings import Settings
from config.special_predicates import TYPE_OBJECT_TYPE
from fb_ingest.batch.manifest import write_manifest
from fb_ingest.batch.partitioning import stable_partition
from fb_ingest.batch.spool import JsonlSpoolManager
from fb_ingest.logging_utils import get_logger
from fb_ingest.models import Phase1Stats
from fb_ingest.parse.ntriples import TripleParseError, parse_preprocessed_line
from fb_ingest.parse.reader import iter_lines
from fb_ingest.paths import phase1_paths
from fb_ingest.schema.predicate_catalog import write_predicate_catalog
from fb_ingest.schema.registry import CVTIndex, PredicateCatalog, SchemaRegistry
from fb_ingest.schema.schema_pass import (
    apply_schema_observation,
    finalize_registry_stats,
    write_registry_artifacts,
)
from fb_ingest.validation.counters import write_stats
from fb_ingest.validation.samples import SampleCollector


def run_phase1(settings: Settings) -> dict:
    logger = get_logger("fb_ingest.phase1")
    paths = phase1_paths(settings.work_dir)
    samples = SampleCollector(settings.sample_count)

    registry = SchemaRegistry()
    cvt_index = CVTIndex()
    catalog = PredicateCatalog()
    stats = Phase1Stats()

    type_spool = JsonlSpoolManager(
        paths["partitions"] / "node_types",
        prefix="node_types",
        max_records=settings.spool_max_records,
    )

    try:
        for line_no, line in iter_lines(Path(settings.input_path)):
            try:
                triple = parse_preprocessed_line(line, line_no)
            except TripleParseError:
                stats.parse_errors += 1
                continue

            apply_schema_observation(
                triple=triple,
                registry=registry,
                cvt_index=cvt_index,
                catalog=catalog,
                stats=stats,
            )

            if triple.p == TYPE_OBJECT_TYPE and not triple.is_literal:
                bucket = f"{stable_partition(triple.s, settings.partition_count):03d}"
                type_row = {"node": triple.s, "type": triple.o}
                type_spool.write(bucket, type_row)
                samples.add("type_assertions", type_row)

            if stats.triples_read and stats.triples_read % settings.log_every == 0:
                logger.info("Processed %s triples", stats.triples_read)

    finally:
        type_spool.close()

    finalize_registry_stats(registry, stats)

    for mediator_type in sorted(registry.mediator_types):
        samples.add("mediator_types", {"type": mediator_type})

    for predicate, expected in sorted(registry.expected_type.items()):
        samples.add("expected_type", {"predicate": predicate, "expected_type": expected})

    sample_path = samples.write(paths["base"], "phase1")
    write_registry_artifacts(registry, cvt_index, paths["schema"])
    write_predicate_catalog(catalog, paths["schema"] / "predicate_catalog.json")
    write_stats(stats, paths["stats"] / "phase1_stats.json")

    manifest = {
        "phase": "phase1",
        "input_path": str(settings.input_path),
        "work_dir": str(settings.work_dir),
        "partition_count": settings.partition_count,
        "spool_max_records": settings.spool_max_records,
        "stats": asdict(stats),
        "artifacts": {
            "schema_dir": str(paths["schema"]),
            "stats_file": str(paths["stats"] / "phase1_stats.json"),
            "predicate_catalog_file": str(paths["schema"] / "predicate_catalog.json"),
            "node_type_partitions_dir": str(paths["partitions"] / "node_types"),
            "samples_file": str(sample_path) if sample_path else None,
        },
    }
    write_manifest(paths["manifests"] / "phase1_manifest.json", manifest)

    logger.info(
        "Phase 1 complete: %s triples read, %s mediator types",
        stats.triples_read,
        stats.mediator_types_found,
    )
    return manifest
