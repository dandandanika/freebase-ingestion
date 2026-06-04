from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from pathlib import Path

from neo4j import GraphDatabase

from fb_ingest.logging_utils import get_logger
from fb_ingest.parse.ntriples import TripleParseError, parse_line_auto
from fb_ingest.parse.reader import iter_lines


@dataclass(frozen=True)
class DirectIngestSettings:
    input_path: Path
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    neo4j_database: str = "neo4j"
    batch_size: int = 10_000
    log_every: int = 1_000_000


@dataclass
class DirectIngestStats:
    triples_read: int = 0
    parse_errors: int = 0
    entity_triples_written: int = 0
    literal_triples_written: int = 0
    batches_committed: int = 0


ENTITY_QUERY = """
UNWIND $rows AS row
MERGE (s:Entity {mid: row.s})
MERGE (o:Entity {mid: row.o})
MERGE (s)-[:FB_REL {predicate: row.p, object_mid: row.o}]->(o)
"""

LITERAL_QUERY = """
UNWIND $rows AS row
MERGE (s:Entity {mid: row.s})
MERGE (l:Literal {literal_id: row.literal_id})
ON CREATE SET
  l.lexical = row.lexical,
  l.datatype = row.datatype,
  l.lang = row.lang
MERGE (s)-[:FB_LITERAL {predicate: row.p, literal_id: row.literal_id}]->(l)
"""


def ensure_constraints(driver, database: str) -> None:
    with driver.session(database=database) as session:
        session.run(
            "CREATE CONSTRAINT entity_mid IF NOT EXISTS FOR (n:Entity) REQUIRE n.mid IS UNIQUE"
        ).consume()
        session.run(
            "CREATE CONSTRAINT literal_id IF NOT EXISTS FOR (n:Literal) REQUIRE n.literal_id IS UNIQUE"
        ).consume()


def run_direct_ingest(settings: DirectIngestSettings) -> dict:
    """
    Stream triples directly into Neo4j using bounded in-memory batches.
    """
    logger = get_logger("fb_ingest.direct_ingest")
    stats = DirectIngestStats()
    entity_rows: list[dict] = []
    literal_rows: list[dict] = []
    started_at = time.monotonic()

    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    logger.info(
        "Starting direct ingest: input=%s database=%s batch_size=%s log_every=%s",
        settings.input_path,
        settings.neo4j_database,
        settings.batch_size,
        settings.log_every,
    )
    ensure_constraints(driver, settings.neo4j_database)

    try:
        with driver.session(database=settings.neo4j_database) as session:
            for line_no, line in iter_lines(settings.input_path):
                if not line.strip():
                    continue

                try:
                    triple = parse_line_auto(line, line_no)
                except TripleParseError:
                    stats.parse_errors += 1
                    continue

                stats.triples_read += 1

                if triple.is_literal:
                    literal_rows.append(
                        {
                            "s": triple.s,
                            "p": triple.p,
                            "literal_id": _literal_id(
                                triple.p,
                                triple.lexical or "",
                                triple.datatype,
                                triple.lang,
                            ),
                            "lexical": triple.lexical or "",
                            "datatype": triple.datatype,
                            "lang": triple.lang,
                        }
                    )
                    stats.literal_triples_written += 1
                else:
                    entity_rows.append({"s": triple.s, "p": triple.p, "o": triple.o})
                    stats.entity_triples_written += 1

                if len(entity_rows) >= settings.batch_size:
                    _flush_entity_batch(session, entity_rows)
                    stats.batches_committed += 1
                    entity_rows = []

                if len(literal_rows) >= settings.batch_size:
                    _flush_literal_batch(session, literal_rows)
                    stats.batches_committed += 1
                    literal_rows = []

                if stats.triples_read and stats.triples_read % settings.log_every == 0:
                    elapsed = max(time.monotonic() - started_at, 1e-9)
                    rate = stats.triples_read / elapsed
                    logger.info(
                        "Progress: triples=%s parse_errors=%s batches=%s rate=%.0f triples/s",
                        stats.triples_read,
                        stats.parse_errors,
                        stats.batches_committed,
                        rate,
                    )

            if entity_rows:
                _flush_entity_batch(session, entity_rows)
                stats.batches_committed += 1
            if literal_rows:
                _flush_literal_batch(session, literal_rows)
                stats.batches_committed += 1
    finally:
        driver.close()

    elapsed = max(time.monotonic() - started_at, 1e-9)
    rate = stats.triples_read / elapsed
    logger.info(
        "Direct ingest complete: triples=%s parse_errors=%s entity=%s literal=%s batches=%s elapsed=%.1fs rate=%.0f triples/s",
        stats.triples_read,
        stats.parse_errors,
        stats.entity_triples_written,
        stats.literal_triples_written,
        stats.batches_committed,
        elapsed,
        rate,
    )
    return {
        "triples_read": stats.triples_read,
        "parse_errors": stats.parse_errors,
        "entity_triples_written": stats.entity_triples_written,
        "literal_triples_written": stats.literal_triples_written,
        "batches_committed": stats.batches_committed,
    }


def _literal_id(predicate: str, lexical: str, datatype: str | None, lang: str | None) -> str:
    token = f"{predicate}\u241f{lexical}\u241f{datatype or ''}\u241f{lang or ''}"
    return hashlib.blake2b(token.encode("utf-8"), digest_size=16).hexdigest()


def _flush_entity_batch(session, rows: list[dict]) -> None:
    session.run(ENTITY_QUERY, rows=rows).consume()


def _flush_literal_batch(session, rows: list[dict]) -> None:
    session.run(LITERAL_QUERY, rows=rows).consume()
