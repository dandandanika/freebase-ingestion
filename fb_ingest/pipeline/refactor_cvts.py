from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from config.settings import Settings
from fb_ingest.batch.buckets import discover_buckets
from fb_ingest.batch.manifest import write_manifest
from fb_ingest.batch.parallel import map_parallel, merge_sample_dicts
from fb_ingest.batch.reducers import iter_jsonl, write_jsonl
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


@dataclass
class RefactorBucketTask:
    bucket: str
    work_dir: str
    partition_count: int
    sample_count: int


@dataclass
class RefactorBucketResult:
    stats: dict
    samples: dict[str, list]


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


def _refactor_bucket(task: RefactorBucketTask) -> RefactorBucketResult:
    phase2 = phase2_paths(Path(task.work_dir))
    paths = refactor_cvts_paths(Path(task.work_dir))
    bucket = task.bucket

    collector = SampleCollector(task.sample_count) if task.sample_count > 0 else None

    new_edges, retained_rows, flattened_mids, bucket_stats = _process_cvt_bucket(
        bucket=bucket,
        cvt_facts_dir=phase2["stage_cvt_facts"],
        partition_count=task.partition_count,
        samples=collector,
    )

    existing_edges_path = phase2["reduce_edges"] / f"edges_{bucket}.jsonl"
    existing_edges: list[dict] = []
    if existing_edges_path.exists():
        existing_edges = list(iter_jsonl(existing_edges_path))
        if existing_edges and collector:
            collector.add("carried_direct_edge", existing_edges[0])

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
    cvt_nodes_removed = 0
    existing_nodes_carried = 0

    for mid, node in nodes_by_mid.items():
        if mid in flattened_mids:
            cvt_nodes_removed += 1
            if collector:
                collector.add("cvt_node_removed", node)
            continue
        output_nodes.append(node)
        existing_nodes_carried += 1

    if output_nodes or nodes_by_mid:
        write_jsonl(paths["nodes"] / f"nodes_{bucket}.jsonl", output_nodes)

    stats = RefactorCvtsStats(
        partitions_processed=1,
        cvt_records_seen=bucket_stats.cvt_records_seen,
        flattened_cvts=bucket_stats.flattened_cvts,
        retained_cvts=bucket_stats.retained_cvts,
        new_edges_written=bucket_stats.new_edges_written,
        cvt_nodes_removed=cvt_nodes_removed,
        existing_edges_carried=len(existing_edges),
        existing_nodes_carried=existing_nodes_carried,
    )

    return RefactorBucketResult(
        stats=asdict(stats),
        samples=collector._buckets if collector else {},
    )


def _merge_refactor_stats(results: list[RefactorBucketResult]) -> RefactorCvtsStats:
    merged = RefactorCvtsStats()
    for result in results:
        stats = result.stats
        merged.partitions_processed += stats.get("partitions_processed", 0)
        merged.cvt_records_seen += stats.get("cvt_records_seen", 0)
        merged.flattened_cvts += stats.get("flattened_cvts", 0)
        merged.retained_cvts += stats.get("retained_cvts", 0)
        merged.new_edges_written += stats.get("new_edges_written", 0)
        merged.cvt_nodes_removed += stats.get("cvt_nodes_removed", 0)
        merged.existing_edges_carried += stats.get("existing_edges_carried", 0)
        merged.existing_nodes_carried += stats.get("existing_nodes_carried", 0)
    return merged


def run_refactor_cvts(settings: Settings) -> dict:
    """
    Re-flatten binary CVT nodes into direct edges using partition-scoped processing.

    Reads Phase 2 stage CVT facts and reduce artifacts, writes refactored node/edge
    partitions with flattened CVT mids removed from the node set.
    """
    logger = get_logger("fb_ingest.refactor_cvts")
    phase2 = phase2_paths(settings.work_dir)
    paths = refactor_cvts_paths(settings.work_dir)
    samples = SampleCollector(settings.sample_count)

    buckets = discover_buckets(
        [
            phase2["stage_cvt_facts"],
            phase2["reduce_nodes"],
            phase2["reduce_edges"],
        ],
        settings.partition_count,
    )

    tasks = [
        RefactorBucketTask(
            bucket=bucket,
            work_dir=str(settings.work_dir),
            partition_count=settings.partition_count,
            sample_count=settings.sample_count,
        )
        for bucket in buckets
    ]

    logger.info(
        "CVT refactor: processing %s buckets (workers=%s)",
        len(tasks),
        settings.workers,
    )

    results = map_parallel(
        tasks,
        _refactor_bucket,
        workers=settings.workers,
        label="refactor buckets",
    )

    stats = _merge_refactor_stats(results)
    merged_samples: dict[str, list] = {}
    for result in results:
        merge_sample_dicts(merged_samples, result.samples)

    for category, records in merged_samples.items():
        for record in records[: settings.sample_count]:
            samples.add(category, record)

    stats_path = paths["stats"] / "refactor_cvts_stats.json"
    write_stats(stats, stats_path)
    sample_path = samples.write(paths["base"], "refactor_cvts")

    manifest = {
        "phase": "refactor_cvts",
        "work_dir": str(settings.work_dir),
        "partition_count": settings.partition_count,
        "workers": settings.workers,
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
