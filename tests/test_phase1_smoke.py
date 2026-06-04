from pathlib import Path

from config.settings import build_settings
from fb_ingest.pipeline.phase1 import run_phase1


def test_phase1_smoke(tmp_path: Path):
    sample = tmp_path / "sample.nt"
    sample.write_text(
        "\n".join(
            [
                "/people/marriage\t/freebase/type_hints/mediator\t/type/boolean/true",
                "/m/abc\t/type/object/type\t/people/person",
                "/m/abc\t/type/object/name\t\"Alice\"@en",
                "/people/person/date_of_birth\t/type/property/expected_type\t/type/datetime",
                "/film/film/release_date\t/type/property/expected_type\thttp://www.w3.org/2001/XMLSchema#date",
            ]
        ),
        encoding="utf-8",
    )

    settings = build_settings(
        input_path=str(sample),
        work_dir=str(tmp_path / "work"),
        partition_count=4,
        spool_max_records=2,
        log_every=2,
    )
    manifest = run_phase1(settings)

    assert manifest["stats"]["triples_read"] == 5
    assert manifest["stats"]["mediator_types_found"] == 1
    assert manifest["stats"]["type_assertions"] == 1
