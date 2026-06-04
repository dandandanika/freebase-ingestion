from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FallbackRecord:
    category: str
    subject: str
    predicate: str
    object_value: Any
    context: dict[str, Any] = field(default_factory=dict)


def make_fallback(
    category: str,
    subject: str,
    predicate: str,
    object_value: Any,
    **context,
) -> FallbackRecord:
    return FallbackRecord(
        category=category,
        subject=subject,
        predicate=predicate,
        object_value=object_value,
        context=context,
    )
