from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from config.settings import Settings
from fb_ingest.batch.manifest import write_manifest
from fb_ingest.batch.partitioning import stable_partition
from fb_ingest.batch.reducers import iter_jsonl_dir, write_jsonl
from fb_ingest.cvt.flatten import decide_cvt_flatten
from fb_ingest.cvt.stage import CVTStager
from fb_ingest.logging_utils import get_logger
from fb_ingest.models import Phase2ReduceStats
from fb_ingest.pipeline.phase2_stage import phase2_paths
from fb_ingest.transform.edge_builder import (
    EdgeRecord,
    build_direct_edge,
    build_flattened_cvt_edge,
)
from fb_ingest.transform.fallback import make_fallback
from fb_ingest.transform.node_builder import NodeRecord
from fb_ingest.transform.canonicalize import PredicateCanonicalizer
from fb_ingest.validation.counters import write_stats
from fb_ingest.validation.samples import SampleCollector, cvt_record_snapshot


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


def run_phase2_reduce(settings: Settings) -> dict:
    logger = get_logger("fb_ingest.phase2_reduce")
    paths = phase2_paths(settings.work_dir)
    stats = Phase2ReduceStats()
    samples = SampleCollector(settings.sample_count)

    phase1_schema_dir = settings.work_dir / "phase1" / "schema"
    canonicalizer = PredicateCanonicalizer.from_schema_dir(phase1_schema_dir)

    nodes: dict[str, NodeRecord] = {}
    edges_by_bucket: dict[str, list[dict]] = {}
    retained_cvts_by_bucket: dict[str, list[dict]] = {}
    fallback_by_bucket: dict[str, list[dict]] = {}
    audit_by_bucket: dict[str, list[dict]] = {}

    for _, row in iter_jsonl_dir(paths["stage_type_facts"]):
        mid = row["node"]
        fb_type = row["type"]
        node = nodes.setdefault(mid, NodeRecord(mid=mid))
        node.add_type(fb_type)

    for _, row in iter_jsonl_dir(paths["stage_special_facts"]):
        mid = row["subject_mid"]
        node = nodes.setdefault(mid, NodeRecord(mid=mid))
        node.add_special_fact(row["predicate"], row["payload"])

    for _, row in iter_jsonl_dir(paths["stage_direct_literals"]):
        mid = row["subject_mid"]
        node = nodes.setdefault(mid, NodeRecord(mid=mid))
        node.add_literal_property(row["predicate"], row["parsed_value"])

    for _, row in iter_jsonl_dir(paths["stage_direct_edges"]):
        canonical = canonicalizer.canonicalize(row["predicate"])
        edge = build_direct_edge(
            source_mid=row["source_mid"],
            target_mid=row["target_mid"],
            canonical=canonical,
        )
        bucket = _bucket_for_mid(edge.source_mid, settings.partition_count)
        edge_dict = _edge_to_dict(edge)
        edges_by_bucket.setdefault(bucket, []).append(edge_dict)
        stats.edge_records_written += 1
        samples.add("direct_edge", edge_dict)

        nodes.setdefault(edge.source_mid, NodeRecord(mid=edge.source_mid))
        nodes.setdefault(edge.target_mid, NodeRecord(mid=edge.target_mid))

    cvt_stager = CVTStager()

    for _, row in iter_jsonl_dir(paths["stage_cvt_facts"]):
        kind = row["kind"]
        cvt_mid = row["cvt_mid"]

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
            nodes.setdefault(row["target_mid"], NodeRecord(mid=row["target_mid"]))
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
            bucket = _bucket_for_mid(edge.source_mid, settings.partition_count)
            edge_dict = _edge_to_dict(edge)
            edges_by_bucket.setdefault(bucket, []).append(edge_dict)
            stats.edge_records_written += 1
            stats.flattened_cvts += 1
            samples.add(
                "flattened_cvt_edge",
                cvt_record_snapshot(record, decision, flattened_edge=edge_dict),
            )
            continue

        retained_row = {
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
        samples.add(
            "retained_cvt",
            cvt_record_snapshot(record, decision),
        )

        retained_bucket = _bucket_for_mid(record.cvt_mid, settings.partition_count)
        retained_cvts_by_bucket.setdefault(retained_bucket, []).append(retained_row)
        stats.retained_cvts += 1

        if not record.incoming:
            stats.orphan_cvts += 1
            orphan_row = {
                "kind": "orphan_cvt",
                "cvt_mid": record.cvt_mid,
                "evidence_count": len(record.outgoing_entities)
                + len(record.outgoing_literals)
                + len(record.chained_cvts),
            }
            audit_by_bucket.setdefault(retained_bucket, []).append(orphan_row)
            samples.add("orphan_cvt", orphan_row)

        if record.chained_cvts:
            stats.cvt_chains += 1
            fallback = make_fallback(
                category="cvt_chain",
                subject=record.cvt_mid,
                predicate=record.chained_cvts[0][1],
                object_value=record.chained_cvts[0][2],
                chain_count=len(record.chained_cvts),
            )
            fallback_by_bucket.setdefault(retained_bucket, []).append(
                {
                    "category": fallback.category,
                    "subject": fallback.subject,
                    "predicate": fallback.predicate,
                    "object_value": fallback.object_value,
                    "context": fallback.context,
                }
            )
            stats.fallback_records_written += 1

    for mid, node in nodes.items():
        bucket = _bucket_for_mid(mid, settings.partition_count)
        path = paths["reduce_nodes"] / f"nodes_{bucket}.jsonl"
        existing = []
        if path.exists():
            from fb_ingest.batch.reducers import iter_jsonl
            existing = list(iter_jsonl(path))
        node_dict = _node_to_dict(node)
        existing.append(node_dict)
        write_jsonl(path, existing)
        stats.node_records_written += 1
        category = "entity_node_with_name" if node.properties.get("name") else "entity_node"
        samples.add(category, node_dict)

    for bucket, rows in edges_by_bucket.items():
        write_jsonl(paths["reduce_edges"] / f"edges_{bucket}.jsonl", rows)

    for bucket, rows in retained_cvts_by_bucket.items():
        write_jsonl(paths["reduce_retained_cvts"] / f"retained_cvts_{bucket}.jsonl", rows)

    for bucket, rows in fallback_by_bucket.items():
        write_jsonl(paths["reduce_fallback"] / f"fallback_{bucket}.jsonl", rows)

    for bucket, rows in audit_by_bucket.items():
        write_jsonl(paths["reduce_audit"] / f"audit_{bucket}.jsonl", rows)

    stats.partitions_processed = len(
        {p.stem.split("_")[-1] for p in paths["reduce_nodes"].glob("nodes_*.jsonl")}
    )

    stats_path = paths["stats"] / "phase2_reduce_stats.json"
    write_stats(stats, stats_path)
    sample_path = samples.write(paths["base"], "phase2_reduce", filename="reduce_samples.json")

    manifest = {
        "phase": "phase2_reduce",
        "phase1_schema_dir": str(phase1_schema_dir),
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
        "Phase 2 reduce complete: %s nodes, %s edges, %s flattened CVTs",
        stats.node_records_written,
        stats.edge_records_written,
        stats.flattened_cvts,
    )
    return manifest
