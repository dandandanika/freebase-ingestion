from __future__ import annotations


CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT entity_mid IF NOT EXISTS FOR (n:Entity) REQUIRE n.mid IS UNIQUE",
    "CREATE INDEX entity_name IF NOT EXISTS FOR (n:Entity) ON (n.name)",
]


def get_constraint_queries() -> list[str]:
    return CONSTRAINT_QUERIES[:]
