from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class CVTIncomingFact:
    source_mid: str
    predicate: str
    cvt_mid: str
    line_no: int = 0


@dataclass(frozen=True)
class CVTEntityFact:
    cvt_mid: str
    predicate: str
    target_mid: str
    line_no: int = 0


@dataclass(frozen=True)
class CVTLiteralFact:
    cvt_mid: str
    predicate: str
    lexical: str
    parsed_value: Any
    value_kind: str
    datatype: Optional[str] = None
    lang: Optional[str] = None
    line_no: int = 0


@dataclass
class CVTRecord:
    cvt_mid: str
    types: list[str] = field(default_factory=list)
    incoming: list[CVTIncomingFact] = field(default_factory=list)
    outgoing_entities: list[CVTEntityFact] = field(default_factory=list)
    outgoing_literals: list[CVTLiteralFact] = field(default_factory=list)
    chained_cvts: list[tuple[str, str, str]] = field(default_factory=list)
