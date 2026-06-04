from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from config.settings import Settings
from fb_ingest.batch.manifest import write_manifest
from fb_ingest.batch.parallel import map_parallel, merge_sample_dicts, resolve_workers
from fb_ingest.batch.reducers import iter_jsonl, write_jsonl
from fb_ingest.enrich.descriptions import build_edge_description, build_node_description
from fb_ingest.enrich.embedder import load_embedder
from fb_ingest.logging_utils import get_logger
from fb_ingest.paths import ensure_dir
from fb_ingest.pipeline.phase2_stage import phase2_paths
from fb_ingest.pipeline.refactor_cvts import refactor_cvts_paths
from fb_ingest.validation.counters import write_stats
from fb_ingest.validation.samples import SampleCollector


@dataclass
class EnrichEmbedStats:
    partitions_processed: int = 0
    nodes_enriched: int = 0
    edges_enriched: int = 0
    embedding_batches: int = 0
    used_fallback_embedder: bool = False


@dataclass(frozen=True)
class EnrichEmbedSettings:
    work_dir: Path
    partition_count: int = 256
    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_batch_size: int = 256
    allow_fallback_embedder: bool = True
    input_phase: str = "auto"
    skip_embeddings: bool = False
    max_records: int | None = None
    sample_count: int = 0
    workers: int = 1


@dataclass
class EnrichPartitionTask:
    kind: str
    input_path: str
    output_path: str
    embedding_batch_size: int
    skip_embeddings: bool
    sample_count: int
    max_rows: int | None = None


@dataclass
class EnrichPartitionResult:
    stats: dict
    samples: dict[str, list]


_WORKER_EMBEDDER = None
_WORKER_NAME_INDEX: dict[str, str] = {}


def _init_enrich_worker(
    model_name: str,
    allow_fallback: bool,
    skip_embeddings: bool,
    name_index: dict[str, str],
) -> None:
    global _WORKER_EMBEDDER, _WORKER_NAME_INDEX
    _WORKER_NAME_INDEX = name_index
    if skip_embeddings:
        _WORKER_EMBEDDER = None
    else:
        _WORKER_EMBEDDER = load_embedder(
            model_name=model_name,
            allow_fallback=allow_fallback,
        )


def enrich_embed_paths(work_dir: Path) -> dict[str, Path]:
    base = ensure_dir(work_dir / "phase3" / "enriched")
    return {
        "base": base,
        "nodes": ensure_dir(base / "nodes"),
        "edges": ensure_dir(base / "edges"),
        "stats": ensure_dir(base / "stats"),
        "manifests": ensure_dir(base / "manifests"),
    }


def _resolve_input_dirs(settings: EnrichEmbedSettings) -> tuple[Path, Path]:
    refactor = refactor_cvts_paths(settings.work_dir)
    phase2 = phase2_paths(settings.work_dir)

    if settings.input_phase == "refactor_cvts":
        return refactor["nodes"], refactor["edges"]
    if settings.input_phase == "phase2":
        return phase2["reduce_nodes"], phase2["reduce_edges"]

    if any(refactor["nodes"].glob("nodes_*.jsonl")):
        return refactor["nodes"], refactor["edges"]
    return phase2["reduce_nodes"], phase2["reduce_edges"]


def _load_name_index(nodes_dir: Path) -> dict[str, str]:
    names: dict[str, str] = {}
    for path in sorted(nodes_dir.glob("nodes_*.jsonl")):
        for row in iter_jsonl(path):
            props = row.get("properties") or {}
            name = props.get("name")
            if name:
                names[row["mid"]] = name
    return names


def _embed_batch(
    embedder,
    texts: list[str],
    batch_size: int,
) -> tuple[list[list[float]], int]:
    vectors: list[list[float]] = []
    batches = 0
    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        vectors.extend(embedder.encode(batch))
        batches += 1
    return vectors, batches


def _enrich_partition(task: EnrichPartitionTask) -> EnrichPartitionResult:
    rows = list(iter_jsonl(Path(task.input_path)))
    if task.max_rows is not None:
        rows = rows[: task.max_rows]
    stats = EnrichEmbedStats(partitions_processed=1)
    samples: dict[str, list] = {}
    sample_cap = task.sample_count

    def add_sample(category: str, record: dict) -> None:
        if sample_cap <= 0:
            return
        bucket = samples.setdefault(category, [])
        if len(bucket) >= sample_cap:
            return
        bucket.append(record)

    if task.kind == "node":
        descriptions = [build_node_description(row) for row in rows]
        embeddings: list[list[float]] = []
        if not task.skip_embeddings and _WORKER_EMBEDDER is not None:
            embeddings, batch_count = _embed_batch(
                _WORKER_EMBEDDER,
                descriptions,
                task.embedding_batch_size,
            )
            stats.embedding_batches = batch_count
            stats.used_fallback_embedder = (
                _WORKER_EMBEDDER.__class__.__name__ == "HashFallbackEmbedder"
            )

        enriched_rows = []
        for idx, row in enumerate(rows):
            props = dict(row.get("properties") or {})
            props["description"] = descriptions[idx]
            if embeddings:
                props["embedding"] = embeddings[idx]
            enriched = dict(row)
            enriched["properties"] = props
            enriched_rows.append(enriched)
            stats.nodes_enriched += 1
            add_sample("enriched_node", enriched)
    else:
        descriptions = [
            build_edge_description(
                row,
                source_name=_WORKER_NAME_INDEX.get(row.get("source_mid", "")),
                target_name=_WORKER_NAME_INDEX.get(row.get("target_mid", "")),
            )
            for row in rows
        ]
        embeddings = []
        if not task.skip_embeddings and _WORKER_EMBEDDER is not None:
            embeddings, batch_count = _embed_batch(
                _WORKER_EMBEDDER,
                descriptions,
                task.embedding_batch_size,
            )
            stats.embedding_batches = batch_count
            stats.used_fallback_embedder = (
                _WORKER_EMBEDDER.__class__.__name__ == "HashFallbackEmbedder"
            )

        enriched_rows = []
        for idx, row in enumerate(rows):
            props = dict(row.get("properties") or {})
            props["description"] = descriptions[idx]
            if embeddings:
                props["embedding"] = embeddings[idx]
            enriched = dict(row)
            enriched["properties"] = props
            enriched_rows.append(enriched)
            stats.edges_enriched += 1
            if (row.get("properties") or {}).get("cvt_mid"):
                add_sample("enriched_cvt_edge", enriched)
            else:
                add_sample("enriched_direct_edge", enriched)

    write_jsonl(Path(task.output_path), enriched_rows)
    return EnrichPartitionResult(stats=asdict(stats), samples=samples)


def _merge_enrich_stats(results: list[EnrichPartitionResult]) -> EnrichEmbedStats:
    merged = EnrichEmbedStats()
    for result in results:
        stats = result.stats
        merged.partitions_processed += stats.get("partitions_processed", 0)
        merged.nodes_enriched += stats.get("nodes_enriched", 0)
        merged.edges_enriched += stats.get("edges_enriched", 0)
        merged.embedding_batches += stats.get("embedding_batches", 0)
        if stats.get("used_fallback_embedder"):
            merged.used_fallback_embedder = True
    return merged


def run_enrich_and_embed(settings: EnrichEmbedSettings) -> dict:
    """
    Add semantic description and embedding properties to node/edge records.

    Processes one partition file at a time to stay memory-safe on large Freebase runs.
    """
    logger = get_logger("fb_ingest.enrich_and_embed")
    input_nodes_dir, input_edges_dir = _resolve_input_dirs(settings)
    output = enrich_embed_paths(settings.work_dir)
    samples = SampleCollector(settings.sample_count)

    name_index = _load_name_index(input_nodes_dir)

    tasks: list[EnrichPartitionTask] = []
    nodes_remaining = settings.max_records
    for node_path in sorted(input_nodes_dir.glob("nodes_*.jsonl")):
        if nodes_remaining is not None and nodes_remaining <= 0:
            break
        max_rows = None
        if nodes_remaining is not None:
            row_count = sum(1 for _ in iter_jsonl(node_path))
            max_rows = min(row_count, nodes_remaining)
            nodes_remaining -= max_rows
        tasks.append(
            EnrichPartitionTask(
                kind="node",
                input_path=str(node_path),
                output_path=str(output["nodes"] / node_path.name),
                embedding_batch_size=settings.embedding_batch_size,
                skip_embeddings=settings.skip_embeddings,
                sample_count=settings.sample_count,
                max_rows=max_rows,
            )
        )

    edges_remaining = settings.max_records
    for edge_path in sorted(input_edges_dir.glob("edges_*.jsonl")):
        if edges_remaining is not None and edges_remaining <= 0:
            break
        max_rows = None
        if edges_remaining is not None:
            row_count = sum(1 for _ in iter_jsonl(edge_path))
            max_rows = min(row_count, edges_remaining)
            edges_remaining -= max_rows
        tasks.append(
            EnrichPartitionTask(
                kind="edge",
                input_path=str(edge_path),
                output_path=str(output["edges"] / edge_path.name),
                embedding_batch_size=settings.embedding_batch_size,
                skip_embeddings=settings.skip_embeddings,
                sample_count=settings.sample_count,
                max_rows=max_rows,
            )
        )

    worker_count = resolve_workers(settings.workers)
    logger.info(
        "Enrichment: processing %s partitions (workers=%s)",
        len(tasks),
        worker_count,
    )

    initargs = (
        settings.embedding_model,
        settings.allow_fallback_embedder,
        settings.skip_embeddings,
        name_index,
    )

    if worker_count == 1:
        _init_enrich_worker(*initargs)
        results = [_enrich_partition(task) for task in tasks]
    else:
        results = map_parallel(
            tasks,
            _enrich_partition,
            workers=settings.workers,
            label="enrich partitions",
            initializer=_init_enrich_worker,
            initargs=initargs,
        )

    stats = _merge_enrich_stats(results)
    if not settings.skip_embeddings and results:
        stats.used_fallback_embedder = any(
            r.stats.get("used_fallback_embedder") for r in results
        )

    merged_samples: dict[str, list] = {}
    for result in results:
        merge_sample_dicts(merged_samples, result.samples)

    for category, records in merged_samples.items():
        for record in records[: settings.sample_count]:
            samples.add(category, record)

    stats_path = output["stats"] / "enrich_embed_stats.json"
    write_stats(stats, stats_path)
    sample_path = samples.write(output["base"], "enrich_and_embed")

    manifest = {
        "phase": "enrich_and_embed",
        "work_dir": str(settings.work_dir),
        "input_nodes_dir": str(input_nodes_dir),
        "input_edges_dir": str(input_edges_dir),
        "embedding_model": settings.embedding_model,
        "embedding_batch_size": settings.embedding_batch_size,
        "skip_embeddings": settings.skip_embeddings,
        "max_records": settings.max_records,
        "workers": settings.workers,
        "stats": asdict(stats),
        "artifacts": {
            "nodes_dir": str(output["nodes"]),
            "edges_dir": str(output["edges"]),
            "stats_file": str(stats_path),
            "samples_file": str(sample_path) if sample_path else None,
        },
    }
    write_manifest(output["manifests"] / "enrich_embed_manifest.json", manifest)
    logger.info(
        "Enrichment complete: %s nodes, %s edges",
        stats.nodes_enriched,
        stats.edges_enriched,
    )
    return manifest


def run_enrich_and_embed_from_settings(settings: Settings, **kwargs) -> dict:
    enrich_settings = EnrichEmbedSettings(
        work_dir=settings.work_dir,
        partition_count=settings.partition_count,
        sample_count=settings.sample_count,
        workers=settings.workers,
        **kwargs,
    )
    return run_enrich_and_embed(enrich_settings)
