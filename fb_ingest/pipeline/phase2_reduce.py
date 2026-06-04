from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from config.settings import Settings
from fb_ingest.batch.buckets import discover_buckets, iter_spool_bucket
from fb_ingest.batch.manifest import write_manifest
from fb_ingest.batch.parallel import map_parallel, merge_sample_dicts
from fb_ingest.batch.partitioning import stable_partition
from fb_ingest.batch.reducers import write_jsonl
from fb_ingest.cvt.flatten import decide_cvt_flatten
from fb_ingest.cvt.stage import CVTStager
from fb_ingest.logging_utils import get_logger
from fb_ingest.models import Phase2ReduceStats
from fb_ingest.pipeline.phase2_stage import phase2_paths
from fb_ingest.transform.canonicalize import PredicateCanonicalizer
from fb_ingest.transform.edge_builder import (
    EdgeRecord,
    build_direct_edge,
    build_flattened_cvt_edge,
)
from fb_ingest.transform.fallback import make_fallback
from fb_ingest.transform.node_builder import NodeRecord
from fb_ingest.validation.counters import write_stats
from fb_ingest.validation.samples import SampleCollector, cvt_record_snapshot


@dataclass
class ReduceBucketTask:
    bucket: str
    work_dir: str
    partition_count: int
    sample_count: int


@dataclass
class ReduceBucketResult:
    stats: dict
    samples: dict[str, list]


def _bucket_for_mid(mid: str, partition_count: int) -> str:
    return f"{stable_partition(mid, partition_count):03d}"


def _node_to_dict(node: NodeRecord) -> dict:
    return {
        "mid": node.mid,
        "labels": node.labels,
        "fb_types": node.fb_types,
        "properties": node.properties,
        "multi_properties": node.multi_properties,
    }


def _edge_to_dict(edge: EdgeRecord) -> dict:
    return {
        "source_mid": edge.source_mid,
        "target_mid": edge.target_mid,
        "rel_type": edge.rel_type,
        "predicate": edge.predicate,
        "properties": edge.properties,
    }


def _retained_cvt_row(record, decision) -> dict:
    return {
        "cvt_mid": record.cvt_mid,
        "reason": decision.reason,
        "types": record.types,
        "incoming": [
            {
                "source_mid": f.source_mid,
                "predicate": f.predicate,
                "line_no": f.line_no,
            }
            for f in record.incoming
        ],
        "outgoing_entities": [
            {
                "predicate": f.predicate,
                "target_mid": f.target_mid,
                "line_no": f.line_no,
            }
            for f in record.outgoing_entities
        ],
        "outgoing_literals": [
            {
                "predicate": f.predicate,
                "lexical": f.lexical,
                "parsed_value": f.parsed_value,
                "value_kind": f.value_kind,
                "datatype": f.datatype,
                "lang": f.lang,
                "line_no": f.line_no,
            }
            for f in record.outgoing_literals
        ],
        "chained_cvts": list(record.chained_cvts),
    }


def _reduce_bucket(task: ReduceBucketTask) -> ReduceBucketResult:
    """
    Reduce one partition bucket from staged spool files into node/edge JSONL.

    Only loads facts for the target bucket, keeping memory bounded per worker.
    """
    paths = phase2_paths(Path(task.work_dir))
    bucket = task.bucket
    partition_count = task.partition_count
    sample_cap = task.sample_count

    phase1_schema_dir = Path(task.work_dir) / "phase1" / "schema"
    canonicalizer = PredicateCanonicalizer.from_schema_dir(phase1_schema_dir)

    stats = Phase2ReduceStats()
    samples: dict[str, list] = {}

    def add_sample(category: str, record) -> None:
        if sample_cap <= 0:
            return
        bucket_rows = samples.setdefault(category, [])
        if len(bucket_rows) >= sample_cap:
            return
        bucket_rows.append(record)

    nodes: dict[str, NodeRecord] = {}
    edges: list[dict] = []
    retained_cvts: list[dict] = []
    fallback_rows: list[dict] = []
    audit_rows: list[dict] = []

    for row in iter_spool_bucket(paths["stage_type_facts"], "type_facts", bucket):
        mid = row["node"]
        if _bucket_for_mid(mid, partition_count) != bucket:
            continue
        node = nodes.setdefault(mid, NodeRecord(mid=mid))
        node.add_type(row["type"])

    for row in iter_spool_bucket(paths["stage_special_facts"], "special_facts", bucket):
        mid = row["subject_mid"]
        if _bucket_for_mid(mid, partition_count) != bucket:
            continue
        node = nodes.setdefault(mid, NodeRecord(mid=mid))
        node.add_special_fact(row["predicate"], row["payload"])

    for row in iter_spool_bucket(paths["stage_direct_literals"], "direct_literals", bucket):
        mid = row["subject_mid"]
        if _bucket_for_mid(mid, partition_count) != bucket:
            continue
        node = nodes.setdefault(mid, NodeRecord(mid=mid))
        node.add_literal_property(row["predicate"], row["parsed_value"])

    for row in iter_spool_bucket(paths["stage_direct_edges"], "direct_edges", bucket):
        if _bucket_for_mid(row["source_mid"], partition_count) != bucket:
            continue
        canonical = canonicalizer.canonicalize(row["predicate"])
        edge = build_direct_edge(
            source_mid=row["source_mid"],
            target_mid=row["target_mid"],
            canonical=canonical,
        )
        edge_dict = _edge_to_dict(edge)
        edges.append(edge_dict)
        stats.edge_records_written += 1
        add_sample("direct_edge", edge_dict)
        nodes.setdefault(edge.source_mid, NodeRecord(mid=edge.source_mid))

    cvt_stager = CVTStager()
    for row in iter_spool_bucket(paths["stage_cvt_facts"], "cvt_facts", bucket):
        cvt_mid = row["cvt_mid"]
        if _bucket_for_mid(cvt_mid, partition_count) != bucket:
            continue
        kind = row["kind"]
        if kind == "incoming":
            cvt_stager.add_incoming(
                source_mid=row["source_mid"],
                predicate=row["predicate"],
                cvt_mid=cvt_mid,
                line_no=row.get("line_no", 0),
            )
            nodes.setdefault(row["source_mid"], NodeRecord(mid=row["source_mid"]))
        elif kind == "entity_out":
            cvt_stager.add_entity_out(
                cvt_mid=cvt_mid,
                predicate=row["predicate"],
                target_mid=row["target_mid"],
                line_no=row.get("line_no", 0),
            )
        elif kind == "literal_out":
            cvt_stager.add_literal_out(
                cvt_mid=cvt_mid,
                predicate=row["predicate"],
                lexical=row["lexical"],
                parsed_value=row["parsed_value"],
                value_kind=row["value_kind"],
                datatype=row.get("datatype"),
                lang=row.get("lang"),
                line_no=row.get("line_no", 0),
            )
        elif kind == "cvt_chain":
            cvt_stager.add_chained_cvt(
                cvt_mid=cvt_mid,
                predicate=row["predicate"],
                target_cvt_mid=row["target_cvt_mid"],
            )

    for record in cvt_stager.iter_records():
        decision = decide_cvt_flatten(record)
        if decision.status == "flatten":
            edge = build_flattened_cvt_edge(
                source_mid=decision.source_mid or "",
                target_mid=decision.target_mid or "",
                predicate=decision.predicate or "",
                rel_type=decision.rel_type or "FB_CVT",
                properties=decision.properties,
            )
            edge_dict = _edge_to_dict(edge)
            edges.append(edge_dict)
            stats.edge_records_written += 1
            stats.flattened_cvts += 1
            add_sample(
                "flattened_cvt_edge",
                cvt_record_snapshot(record, decision, flattened_edge=edge_dict),
            )
            continue

        retained_row = _retained_cvt_row(record, decision)
        retained_cvts.append(retained_row)
        stats.retained_cvts += 1
        add_sample("retained_cvt", cvt_record_snapshot(record, decision))

        if not record.incoming:
            stats.orphan_cvts += 1
            orphan_row = {
                "kind": "orphan_cvt",
                "cvt_mid": record.cvt_mid,
                "evidence_count": len(record.outgoing_entities)
                + len(record.outgoing_literals)
                + len(record.chained_cvts),
            }
            audit_rows.append(orphan_row)
            add_sample("orphan_cvt", orphan_row)

        if record.chained_cvts:
            stats.cvt_chains += 1
            fallback = make_fallback(
                category="cvt_chain",
                subject=record.cvt_mid,
                predicate=record.chained_cvts[0][1],
                object_value=record.chained_cvts[0][2],
                chain_count=len(record.chained_cvts),
            )
            fallback_rows.append(
                {
                    "category": fallback.category,
                    "subject": fallback.subject,
                    "predicate": fallback.predicate,
                    "object_value": fallback.object_value,
                    "context": fallback.context,
                }
            )
            stats.fallback_records_written += 1

    node_rows = []
    for node in nodes.values():
        node_dict = _node_to_dict(node)
        node_rows.append(node_dict)
        stats.node_records_written += 1
        category = "entity_node_with_name" if node.properties.get("name") else "entity_node"
        add_sample(category, node_dict)

    if node_rows:
        write_jsonl(paths["reduce_nodes"] / f"nodes_{bucket}.jsonl", node_rows)
    if edges:
        write_jsonl(paths["reduce_edges"] / f"edges_{bucket}.jsonl", edges)
    if retained_cvts:
        write_jsonl(
            paths["reduce_retained_cvts"] / f"retained_cvts_{bucket}.jsonl",
            retained_cvts,
        )
    if fallback_rows:
        write_jsonl(
            paths["reduce_fallback"] / f"fallback_{bucket}.jsonl",
            fallback_rows,
        )
    if audit_rows:
        write_jsonl(paths["reduce_audit"] / f"audit_{bucket}.jsonl", audit_rows)

    stats.partitions_processed = 1 if node_rows or edges or retained_cvts else 0
    return ReduceBucketResult(stats=asdict(stats), samples=samples)


def _merge_reduce_stats(results: list[ReduceBucketResult]) -> Phase2ReduceStats:
    merged = Phase2ReduceStats()
    for result in results:
        stats = result.stats
        merged.node_records_written += stats.get("node_records_written", 0)
        merged.edge_records_written += stats.get("edge_records_written", 0)
        merged.flattened_cvts += stats.get("flattened_cvts", 0)
        merged.retained_cvts += stats.get("retained_cvts", 0)
        merged.orphan_cvts += stats.get("orphan_cvts", 0)
        merged.cvt_chains += stats.get("cvt_chains", 0)
        merged.fallback_records_written += stats.get("fallback_records_written", 0)
        merged.partitions_processed += stats.get("partitions_processed", 0)
    return merged


def run_phase2_reduce(settings: Settings) -> dict:
    logger = get_logger("fb_ingest.phase2_reduce")
    paths = phase2_paths(settings.work_dir)
    samples = SampleCollector(settings.sample_count)

    phase1_schema_dir = settings.work_dir / "phase1" / "schema"

    buckets = discover_buckets(
        [
            paths["stage_type_facts"],
            paths["stage_special_facts"],
            paths["stage_direct_literals"],
            paths["stage_direct_edges"],
            paths["stage_cvt_facts"],
        ],
        settings.partition_count,
    )

    tasks = [
        ReduceBucketTask(
            bucket=bucket,
            work_dir=str(settings.work_dir),
            partition_count=settings.partition_count,
            sample_count=settings.sample_count,
        )
        for bucket in buckets
    ]

    logger.info(
        "Phase 2 reduce: processing %s buckets (workers=%s)",
        len(tasks),
        settings.workers,
    )

    results = map_parallel(
        tasks,
        _reduce_bucket,
        workers=settings.workers,
        label="reduce buckets",
    )

    stats = _merge_reduce_stats(results)
    merged_samples: dict[str, list] = {}
    for result in results:
        merge_sample_dicts(merged_samples, result.samples)

    for category, records in merged_samples.items():
        for record in records[: settings.sample_count]:
            samples.add(category, record)

    stats.partitions_processed = len(
        {p.stem.split("_")[-1] for p in paths["reduce_nodes"].glob("nodes_*.jsonl")}
    )

    stats_path = paths["stats"] / "phase2_reduce_stats.json"
    write_stats(stats, stats_path)
    sample_path = samples.write(paths["base"], "phase2_reduce", filename="reduce_samples.json")

    manifest = {
        "phase": "phase2_reduce",
        "phase1_schema_dir": str(phase1_schema_dir),
        "workers": settings.workers,
        "stats": asdict(stats),
        "artifacts": {
            "nodes_dir": str(paths["reduce_nodes"]),
            "edges_dir": str(paths["reduce_edges"]),
            "retained_cvts_dir": str(paths["reduce_retained_cvts"]),
            "fallback_dir": str(paths["reduce_fallback"]),
            "audit_dir": str(paths["reduce_audit"]),
            "stats_file": str(stats_path),
            "samples_file": str(sample_path) if sample_path else None,
        },
    }
    write_manifest(paths["manifests"] / "phase2_reduce_manifest.json", manifest)
    logger.info(
        "Phase 2 reduce complete: %s nodes, %s edges, %s flattened CVTs (%s buckets)",
        stats.node_records_written,
        stats.edge_records_written,
        stats.flattened_cvts,
        stats.partitions_processed,
    )
    return manifest
