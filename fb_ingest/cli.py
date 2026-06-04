from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.settings import build_settings
from fb_ingest.pipeline.phase1 import run_phase1
from fb_ingest.pipeline.phase2 import run_phase2
from fb_ingest.pipeline.refactor_cvts import run_refactor_cvts
from fb_ingest.pipeline.enrich_and_embed import run_enrich_and_embed_from_settings


def _add_common_pipeline_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input", required=True)
    parser.add_argument("--work-dir", required=True)
    parser.add_argument("--partition-count", type=int, default=256)
    parser.add_argument("--spool-max-records", type=int, default=250000)
    parser.add_argument("--log-every", type=int, default=1000000)
    parser.add_argument(
        "--sample-count",
        type=int,
        default=0,
        help="Write up to N example records per category to samples/samples.json (0=off)",
    )


def main() -> None:
    parser = argparse.ArgumentParser(prog="fb_ingest")
    subparsers = parser.add_subparsers(dest="command", required=True)

    phase1_parser = subparsers.add_parser(
        "phase1",
        help="Run Phase 1 schema/type discovery",
    )
    _add_common_pipeline_args(phase1_parser)

    phase2_parser = subparsers.add_parser(
        "phase2",
        help="Run Phase 2 staging and reduction",
    )
    _add_common_pipeline_args(phase2_parser)

    refactor_parser = subparsers.add_parser(
        "refactor-cvts",
        help="Flatten binary CVT nodes into direct edges (partition-scoped)",
    )
    _add_common_pipeline_args(refactor_parser)

    enrich_parser = subparsers.add_parser(
        "enrich-and-embed",
        help="Add description and embedding properties to nodes and edges",
    )
    _add_common_pipeline_args(enrich_parser)
    enrich_parser.add_argument("--embedding-model", default="all-MiniLM-L6-v2")
    enrich_parser.add_argument("--embedding-batch-size", type=int, default=256)
    enrich_parser.add_argument(
        "--input-phase",
        choices=["auto", "phase2", "refactor_cvts"],
        default="auto",
    )
    enrich_parser.add_argument(
        "--require-real-embeddings",
        action="store_true",
        help="Fail if sentence-transformers is not installed",
    )
    enrich_parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Only write description properties (fast smoke tests)",
    )
    enrich_parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Cap total nodes/edges enriched (useful for quick embedding samples)",
    )

    direct_parser = subparsers.add_parser(
        "direct-ingest",
        help="Stream raw/preprocessed Freebase triples directly into Neo4j",
    )
    direct_parser.add_argument("--input", required=True)
    direct_parser.add_argument("--neo4j-uri", required=True)
    direct_parser.add_argument("--neo4j-user", required=True)
    direct_parser.add_argument("--neo4j-password", required=True)
    direct_parser.add_argument("--neo4j-database", default="neo4j")
    direct_parser.add_argument("--batch-size", type=int, default=10000)
    direct_parser.add_argument("--log-every", type=int, default=1000000)

    args = parser.parse_args()

    if args.command == "direct-ingest":
        from fb_ingest.pipeline.direct_ingest import DirectIngestSettings, run_direct_ingest

        result = run_direct_ingest(
            DirectIngestSettings(
                input_path=Path(args.input),
                neo4j_uri=args.neo4j_uri,
                neo4j_user=args.neo4j_user,
                neo4j_password=args.neo4j_password,
                neo4j_database=args.neo4j_database,
                batch_size=args.batch_size,
                log_every=args.log_every,
            )
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    settings = build_settings(
        input_path=args.input,
        work_dir=args.work_dir,
        partition_count=args.partition_count,
        spool_max_records=args.spool_max_records,
        log_every=args.log_every,
        sample_count=args.sample_count,
    )

    if args.command == "phase1":
        result = run_phase1(settings)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "phase2":
        result = run_phase2(settings)
        print(
            json.dumps(
                {
                    "stage_manifest": result.stage_manifest,
                    "reduce_manifest": result.reduce_manifest,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "refactor-cvts":
        result = run_refactor_cvts(settings)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "enrich-and-embed":
        result = run_enrich_and_embed_from_settings(
            settings,
            embedding_model=args.embedding_model,
            embedding_batch_size=args.embedding_batch_size,
            input_phase=args.input_phase,
            allow_fallback_embedder=not args.require_real_embeddings,
            skip_embeddings=args.skip_embeddings,
            max_records=args.max_records,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return


if __name__ == "__main__":
    main()
