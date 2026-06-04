from fb_ingest.enrich.descriptions import build_edge_description, build_node_description


def test_build_node_description():
    node = {
        "mid": "/m/abc",
        "labels": ["Entity", "Person"],
        "fb_types": ["/people/person"],
        "properties": {
            "name": "Alice",
            "people.person.date_of_birth": "1980-01-01",
        },
        "multi_properties": {
            "aliases": ["A. Example"],
        },
    }
    description = build_node_description(node)
    assert "Person: Alice" in description
    assert "Freebase types: person" in description
    assert "Also known as: A. Example" in description


def test_build_edge_description():
    edge = {
        "source_mid": "/m/a",
        "target_mid": "/m/b",
        "rel_type": "NATIONALITY",
        "predicate": "/people/person/nationality",
        "properties": {},
    }
    description = build_edge_description(
        edge,
        source_name="Alice",
        target_name="France",
    )
    assert "NATIONALITY relationship from Alice to France" in description
    assert "Predicate: /people/person/nationality" in description
