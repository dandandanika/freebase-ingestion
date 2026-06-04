from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from config.settings import Settings
from fb_ingest.batch.manifest import write_manifest
from fb_ingest.batch.partitioning import stable_partition
from fb_ingest.batch.reducers import iter_jsonl, iter_jsonl_dir, write_jsonl
from fb_ingest.cvt.flatten import decide_cvt_flatten
from fb_ingest.cvt.stage import CVTStager
from fb_ingest.logging_utils import get_logger
from fb_ingest.paths import ensure_dir
from fb_ingest.pipeline.phase2_stage import phase2_paths
from fb_ingest.transform.edge_builder import EdgeRecord, build_flattened_cvt_edge
from fb_ingest.validation.counters import write_stats
from fb_ingest.validation.samples import SampleCollector, cvt_record_snapshot


@dataclass
class RefactorCvtsStats:
    partitions_processed: int = 0
    cvt_records_seen: int = 0
    flattened_cvts: int = 0
    retained_cvts: int = 0
    new_edges_written: int = 0
    cvt_nodes_removed: int = 0
    existing_edges_carried: int = 0
    existing_nodes_carried: int = 0


def refactor_cvts_paths(work_dir: Path) -> dict[str, Path]:
    base = ensure_dir(work_dir / "phase3" / "cvt_refactor")
    return {
        "base": base,
        "nodes": ensure_dir(base / "nodes"),
        "edges": ensure_dir(base / "edges"),
        "retained_cvts": ensure_dir(base / "retained_cvts"),
        "stats": ensure_dir(base / "stats"),
        "manifests": ensure_dir(base / "manifests"),
    }


def _bucket_for_mid(mid: str, partition_count: int) -> str:
    return f"{stable_partition(mid, partition_count):03d}"


def _edge_to_dict(edge: EdgeRecord) -> dict:
    return {
        "source_mid": edge.source_mid,
        "target_mid": edge.target_mid,
        "rel_type": edge.rel_type,
        "predicate": edge.predicate,
        "properties": edge.properties,
    }


def _load_nodes_by_mid(path: Path) -> dict[str, dict]:
    nodes: dict[str, dict] = {}
    if not path.exists():
        return nodes
    for row in iter_jsonl(path):
        nodes[row["mid"]] = row
    return nodes


def _process_cvt_bucket(
    bucket: str,
    cvt_facts_dir: Path,
    partition_count: int,
    samples: SampleCollector | None = None,
) -> tuple[list[dict], list[dict], set[str], RefactorCvtsStats]:
    bucket_stats = RefactorCvtsStats()
    cvt_stager = CVTStager()
    flattened_mids: set[str] = set()
    new_edges: list[dict] = []
    retained_rows: list[dict] = []

    for file_path in sorted(cvt_facts_dir.glob(f"cvt_facts_{bucket}_*.jsonl")):
        for row in iter_jsonl(file_path):
            kind = row["kind"]
            cvt_mid = row["cvt_mid"]

            if kind == "incoming":
                cvt_stager.add_incoming(
                    source_mid=row["source_mid"],
                    predicate=row["predicate"],
                    cvt_mid=cvt_mid,
                    line_no=row.get("line_no", 0),
                )
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
        bucket_stats.cvt_records_seen += 1
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
            new_edges.append(edge_dict)
            flattened_mids.add(record.cvt_mid)
            bucket_stats.flattened_cvts += 1
            bucket_stats.new_edges_written += 1
            if samples:
                samples.add(
                    "flattened_cvt",
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
        retained_rows.append(retained_row)
        if samples:
            samples.add("retained_cvt", cvt_record_snapshot(record, decision))
        bucket_stats.retained_cvts += 1

    return new_edges, retained_rows, flattened_mids, bucket_stats


def run_refactor_cvts(settings: Settings) -> dict:
    """
    Re-flatten binary CVT nodes into direct edges using partition-scoped processing.

    Reads Phase 2 stage CVT facts and reduce artifacts, writes refactored node/edge
    partitions with flattened CVT mids removed from the node set.
    """
    logger = get_logger("fb_ingest.refactor_cvts")
    phase2 = phase2_paths(settings.work_dir)
    paths = refactor_cvts_paths(settings.work_dir)
    stats = RefactorCvtsStats()
    samples = SampleCollector(settings.sample_count)

    cvt_buckets = {
        path.name.split("_")[2]
        for path in phase2["stage_cvt_facts"].glob("cvt_facts_*_*.jsonl")
    }
    node_buckets = {
        path.stem.split("_")[-1]
        for path in phase2["reduce_nodes"].glob("nodes_*.jsonl")
    }
    edge_buckets = {
        path.stem.split("_")[-1]
        for path in phase2["reduce_edges"].glob("edges_*.jsonl")
    }
    buckets = sorted(cvt_buckets | node_buckets | edge_buckets)

    if not buckets:
        buckets = [f"{idx:03d}" for idx in range(settings.partition_count)]

    for bucket in buckets:
        new_edges, retained_rows, flattened_mids, bucket_stats = _process_cvt_bucket(
            bucket=bucket,
            cvt_facts_dir=phase2["stage_cvt_facts"],
            partition_count=settings.partition_count,
            samples=samples,
        )
        stats.cvt_records_seen += bucket_stats.cvt_records_seen
        stats.flattened_cvts += bucket_stats.flattened_cvts
        stats.retained_cvts += bucket_stats.retained_cvts
        stats.new_edges_written += bucket_stats.new_edges_written

        existing_edges_path = phase2["reduce_edges"] / f"edges_{bucket}.jsonl"
        existing_edges: list[dict] = []
        if existing_edges_path.exists():
            existing_edges = list(iter_jsonl(existing_edges_path))
            stats.existing_edges_carried += len(existing_edges)
            if existing_edges:
                samples.add("carried_direct_edge", existing_edges[0])

        write_jsonl(
            paths["edges"] / f"edges_{bucket}.jsonl",
            existing_edges + new_edges,
        )

        if retained_rows:
            write_jsonl(
                paths["retained_cvts"] / f"retained_cvts_{bucket}.jsonl",
                retained_rows,
            )

        nodes_path = phase2["reduce_nodes"] / f"nodes_{bucket}.jsonl"
        nodes_by_mid = _load_nodes_by_mid(nodes_path)
        output_nodes: list[dict] = []

        for mid, node in nodes_by_mid.items():
            if mid in flattened_mids:
                stats.cvt_nodes_removed += 1
                samples.add("cvt_node_removed", node)
                continue
            output_nodes.append(node)
            stats.existing_nodes_carried += 1

        if output_nodes or nodes_by_mid:
            write_jsonl(paths["nodes"] / f"nodes_{bucket}.jsonl", output_nodes)

        stats.partitions_processed += 1

    stats_path = paths["stats"] / "refactor_cvts_stats.json"
    write_stats(stats, stats_path)
    sample_path = samples.write(paths["base"], "refactor_cvts")

    manifest = {
        "phase": "refactor_cvts",
        "work_dir": str(settings.work_dir),
        "partition_count": settings.partition_count,
        "stats": asdict(stats),
        "artifacts": {
            "nodes_dir": str(paths["nodes"]),
            "edges_dir": str(paths["edges"]),
            "retained_cvts_dir": str(paths["retained_cvts"]),
            "stats_file": str(stats_path),
            "samples_file": str(sample_path) if sample_path else None,
        },
    }
    write_manifest(paths["manifests"] / "refactor_cvts_manifest.json", manifest)
    logger.info(
        "CVT refactor complete: %s flattened, %s retained, %s CVT nodes removed",
        stats.flattened_cvts,
        stats.retained_cvts,
        stats.cvt_nodes_removed,
    )
    return manifest
