from pathlib import Path

from config.settings import build_settings
from fb_ingest.parse.ntriples import parse_line_auto
from fb_ingest.pipeline.phase1 import run_phase1

NS = "http://rdf.freebase.com/ns/"


def test_parse_line_auto_tabbed_uri_triple():
    line = (
        f"<{NS}people.marriage>\t"
        f"<{NS}freebase.type_hints.mediator>\t"
        f"<{NS}type.boolean.true>\t."
    )
    triple = parse_line_auto(line, line_no=1)
    assert triple.s == "/people/marriage"
    assert triple.p == "/freebase/type_hints/mediator"
    assert triple.o == "/type/boolean/true"


def test_phase1_discovers_mediator_types_from_uri_dump(tmp_path: Path):
    sample = tmp_path / "sample_uri.nt"
    lines = [
        f"<{NS}people.marriage>\t<{NS}freebase.type_hints.mediator>\t<{NS}type.boolean.true>\t.",
        f"<{NS}people.person.marriage>\t<{NS}type.property.expected_type>\t<{NS}people.marriage>\t.",
        f"<{NS}m.abc>\t<{NS}type.object.type>\t<{NS}people.person>\t.",
    ]
    sample.write_text("\n".join(lines), encoding="utf-8")

    settings = build_settings(
        input_path=str(sample),
        work_dir=str(tmp_path / "work"),
        partition_count=4,
    )
    manifest = run_phase1(settings)

    assert manifest["stats"]["mediator_types_found"] == 1
    assert manifest["stats"]["expected_type_triples"] == 1
