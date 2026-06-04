from pathlib import Path

from config.settings import build_settings
from fb_ingest.batch.buckets import bucket_from_spool_filename, discover_buckets
from fb_ingest.batch.parallel import resolve_workers
from fb_ingest.pipeline.phase1 import run_phase1
from fb_ingest.pipeline.phase2 import run_phase2
from fb_ingest.pipeline.refactor_cvts import run_refactor_cvts
from fb_ingest.pipeline.enrich_and_embed import run_enrich_and_embed_from_settings
from tests.test_cvt_and_enrich import _write_sample_freebase


def test_bucket_from_spool_filename():
    assert bucket_from_spool_filename(Path("direct_literals_005_00000.jsonl")) == "005"
    assert bucket_from_spool_filename(Path("cvt_facts_042_00001.jsonl")) == "042"


def test_discover_buckets_empty(tmp_path: Path):
    buckets = discover_buckets([tmp_path / "missing"], partition_count=8)
    assert buckets == [f"{idx:03d}" for idx in range(8)]


def test_resolve_workers():
    assert resolve_workers(1) == 1
    assert resolve_workers(4) == 4
    assert resolve_workers(0) >= 1


def test_parallel_pipeline_matches_sequential(tmp_path: Path):
    sample = _write_sample_freebase(tmp_path)
    work_dir = tmp_path / "work"

    sequential_settings = build_settings(
        input_path=str(sample),
        work_dir=str(work_dir / "sequential"),
        partition_count=4,
        spool_max_records=100,
        log_every=100,
        workers=1,
    )
    parallel_settings = build_settings(
        input_path=str(sample),
        work_dir=str(work_dir / "parallel"),
        partition_count=4,
        spool_max_records=100,
        log_every=100,
        workers=2,
    )

    for settings in (sequential_settings, parallel_settings):
        run_phase1(settings)

    seq_result = run_phase2(sequential_settings)
    par_result = run_phase2(parallel_settings)

    seq_reduce = seq_result.reduce_manifest["stats"]
    par_reduce = par_result.reduce_manifest["stats"]
    assert par_reduce["edge_records_written"] == seq_reduce["edge_records_written"]
    assert par_reduce["flattened_cvts"] == seq_reduce["flattened_cvts"]
    assert par_reduce["node_records_written"] == seq_reduce["node_records_written"]

    seq_refactor = run_refactor_cvts(sequential_settings)
    par_refactor = run_refactor_cvts(parallel_settings)
    assert (
        par_refactor["stats"]["flattened_cvts"]
        == seq_refactor["stats"]["flattened_cvts"]
    )

    seq_enrich = run_enrich_and_embed_from_settings(
        sequential_settings,
        allow_fallback_embedder=True,
        input_phase="refactor_cvts",
        skip_embeddings=True,
    )
    par_enrich = run_enrich_and_embed_from_settings(
        parallel_settings,
        allow_fallback_embedder=True,
        input_phase="refactor_cvts",
        skip_embeddings=True,
    )
    assert par_enrich["stats"]["nodes_enriched"] == seq_enrich["stats"]["nodes_enriched"]
    assert par_enrich["stats"]["edges_enriched"] == seq_enrich["stats"]["edges_enriched"]
