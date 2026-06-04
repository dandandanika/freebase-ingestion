from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from config.settings import Settings
from fb_ingest.batch.manifest import write_manifest
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
    stats: EnrichEmbedStats,
) -> list[list[float]]:
    vectors: list[list[float]] = []
    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        vectors.extend(embedder.encode(batch))
        stats.embedding_batches += 1
    return vectors


def run_enrich_and_embed(settings: EnrichEmbedSettings) -> dict:
    """
    Add semantic description and embedding properties to node/edge records.

    Processes one partition file at a time to stay memory-safe on large Freebase runs.
    """
    logger = get_logger("fb_ingest.enrich_and_embed")
    input_nodes_dir, input_edges_dir = _resolve_input_dirs(settings)
    output = enrich_embed_paths(settings.work_dir)
    stats = EnrichEmbedStats()
    samples = SampleCollector(settings.sample_count)

    embedder = None
    if not settings.skip_embeddings:
        embedder = load_embedder(
            model_name=settings.embedding_model,
            allow_fallback=settings.allow_fallback_embedder,
        )
        stats.used_fallback_embedder = embedder.__class__.__name__ == "HashFallbackEmbedder"

    name_index = _load_name_index(input_nodes_dir)

    node_files = sorted(input_nodes_dir.glob("nodes_*.jsonl"))
    for node_path in node_files:
        rows = list(iter_jsonl(node_path))
        if settings.max_records is not None:
            remaining = settings.max_records - stats.nodes_enriched
            if remaining <= 0:
                break
            rows = rows[:remaining]

        descriptions = [build_node_description(row) for row in rows]
        embeddings: list[list[float]] = []
        if not settings.skip_embeddings:
            embeddings = _embed_batch(
                embedder,
                descriptions,
                settings.embedding_batch_size,
                stats,
            )

        enriched_rows = []
        for idx, row in enumerate(rows):
            description = descriptions[idx]
            props = dict(row.get("properties") or {})
            props["description"] = description
            if not settings.skip_embeddings:
                props["embedding"] = embeddings[idx]
            enriched = dict(row)
            enriched["properties"] = props
            enriched_rows.append(enriched)
            stats.nodes_enriched += 1
            samples.add("enriched_node", enriched)

        write_jsonl(output["nodes"] / node_path.name, enriched_rows)
        stats.partitions_processed += 1

    edge_files = sorted(input_edges_dir.glob("edges_*.jsonl"))
    for edge_path in edge_files:
        rows = list(iter_jsonl(edge_path))
        if settings.max_records is not None:
            remaining = settings.max_records - stats.edges_enriched
            if remaining <= 0:
                break
            rows = rows[:remaining]

        descriptions = [
            build_edge_description(
                row,
                source_name=name_index.get(row.get("source_mid", "")),
                target_name=name_index.get(row.get("target_mid", "")),
            )
            for row in rows
        ]
        embeddings = []
        if not settings.skip_embeddings:
            embeddings = _embed_batch(
                embedder,
                descriptions,
                settings.embedding_batch_size,
                stats,
            )

        enriched_rows = []
        for idx, row in enumerate(rows):
            description = descriptions[idx]
            props = dict(row.get("properties") or {})
            props["description"] = description
            if not settings.skip_embeddings:
                props["embedding"] = embeddings[idx]
            enriched = dict(row)
            enriched["properties"] = props
            enriched_rows.append(enriched)
            stats.edges_enriched += 1
            if (row.get("properties") or {}).get("cvt_mid"):
                samples.add("enriched_cvt_edge", enriched)
            else:
                samples.add("enriched_direct_edge", enriched)

        write_jsonl(output["edges"] / edge_path.name, enriched_rows)

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
        **kwargs,
    )
    return run_enrich_and_embed(enrich_settings)
