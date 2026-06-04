from __future__ import annotations

import gzip
from collections.abc import Iterator
from pathlib import Path
from typing import TextIO


def open_text_stream(path: Path) -> TextIO:
    """
    Open a plain-text or .gz text file as a UTF-8 stream.

    Uses errors='replace' so a few malformed bytes do not kill a multi-hour run.
    If you prefer strict failure, change to errors='strict'.
    """
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "rt", encoding="utf-8", errors="replace")


def iter_lines(path: Path) -> Iterator[tuple[int, str]]:
    """
    Yield (line_number, line_without_trailing_newline).
    """
    with open_text_stream(path) as handle:
        for line_no, line in enumerate(handle, start=1):
            yield line_no, line.rstrip("\n")
