from pathlib import Path

from config.settings import build_settings
from fb_ingest.cvt.detector import Phase1Artifacts
from fb_ingest.parse.ntriples import parse_preprocessed_line
from fb_ingest.pipeline.phase1 import run_phase1
from fb_ingest.pipeline.phase2 import run_phase2
from fb_ingest.pipeline.refactor_cvts import run_refactor_cvts
from fb_ingest.pipeline.enrich_and_embed import run_enrich_and_embed_from_settings
from fb_ingest.transform.classifier import classify_triple


def _write_sample_freebase(tmp_path: Path) -> Path:
    sample = tmp_path / "sample.nt"
    sample.write_text(
        "\n".join(
            [
                "/people/marriage\t/freebase/type_hints/mediator\t/type/boolean/true",
                "/people/person/marriage\t/type/property/expected_type\t/people/marriage",
                "/m/person1\t/type/object/type\t/people/person",
                "/m/person2\t/type/object/type\t/people/person",
                "/m/person1\t/type/object/name\t\"Alice\"@en",
                "/m/person2\t/type/object/name\t\"Bob\"@en",
                "/m/person1\t/people/person/marriage\t/m/cvt1",
                "/m/cvt1\t/type/object/type\t/people/marriage",
                "/m/cvt1\t/people/marriage/spouse\t/m/person2",
                "/m/cvt1\t/people/marriage/from\t\"2001-06-01\"^^<http://www.w3.org/2001/XMLSchema#date>",
                "/m/person1\t/people/person/nationality\t/m/country1",
                "/m/country1\t/type/object/name\t\"France\"@en",
            ]
        ),
        encoding="utf-8",
    )
    return sample


def test_cvt_detection_via_expected_type_without_cvt_type_assertion(tmp_path: Path):
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()
    (schema_dir / "mediator_types.json").write_text(
        '["/people/marriage"]',
        encoding="utf-8",
    )
    (schema_dir / "expected_type.json").write_text(
        '{"/people/person/marriage": "/people/marriage"}',
        encoding="utf-8",
    )
    (schema_dir / "node_types.json").write_text("{}", encoding="utf-8")

    artifacts = Phase1Artifacts.from_schema_dir(schema_dir)
    triple = parse_preprocessed_line(
        "/m/person1\t/people/person/marriage\t/m/cvt1",
        line_no=1,
    )

    assert artifacts.is_cvt_instance("/m/cvt1", via_predicate="/people/person/marriage")
    assert classify_triple(triple, artifacts).kind == "cvt_incoming_fact"


def test_phase2_and_refactor_cvts_pipeline(tmp_path: Path):
    sample = _write_sample_freebase(tmp_path)
    work_dir = tmp_path / "work"

    phase1_settings = build_settings(
        input_path=str(sample),
        work_dir=str(work_dir),
        partition_count=4,
        spool_max_records=100,
        log_every=100,
    )
    run_phase1(phase1_settings)

    phase2_settings = build_settings(
        input_path=str(sample),
        work_dir=str(work_dir),
        partition_count=4,
        spool_max_records=100,
        log_every=100,
    )
    phase2_result = run_phase2(phase2_settings)

    stage_stats = phase2_result.stage_manifest["stats"]
    assert stage_stats["cvt_incoming_facts"] >= 1
    assert stage_stats["cvt_entity_out_facts"] >= 1

    refactor_manifest = run_refactor_cvts(phase2_settings)
    assert refactor_manifest["stats"]["flattened_cvts"] >= 1
    assert refactor_manifest["stats"]["cvt_nodes_removed"] >= 1


def test_enrich_and_embed_adds_description_and_embedding(tmp_path: Path):
    sample = _write_sample_freebase(tmp_path)
    work_dir = tmp_path / "work"

    settings = build_settings(
        input_path=str(sample),
        work_dir=str(work_dir),
        partition_count=4,
        spool_max_records=100,
        log_every=100,
    )
    run_phase1(settings)
    run_phase2(settings)
    run_refactor_cvts(settings)

    manifest = run_enrich_and_embed_from_settings(
        settings,
        allow_fallback_embedder=True,
        input_phase="refactor_cvts",
    )
    assert manifest["stats"]["nodes_enriched"] >= 1
    assert manifest["stats"]["edges_enriched"] >= 1

    enriched_nodes_dir = work_dir / "phase3" / "enriched" / "nodes"
    first_node_file = next(enriched_nodes_dir.glob("nodes_*.jsonl"))
    row = next(open(first_node_file, encoding="utf-8"))
    import json

    node = json.loads(row)
    assert "description" in node["properties"]
    assert isinstance(node["properties"]["embedding"], list)
    assert len(node["properties"]["embedding"]) > 0
