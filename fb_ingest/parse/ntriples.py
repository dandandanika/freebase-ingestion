from __future__ import annotations

import re

from fb_ingest.models import Triple
import codecs

class TripleParseError(ValueError):
    pass


def parse_preprocessed_line(line: str, line_no: int) -> Triple:
    """
    Parse one preprocessed triple line.

    Expected input format:
        subject<TAB>predicate<TAB>object_or_literal
    """
    parts = line.split("\t", 2)
    if len(parts) != 3:
        raise TripleParseError(
            f"Line {line_no}: expected 3 tab-separated fields, got {len(parts)}"
        )

    s, p, o = parts
    if not s or not p or not o:
        raise TripleParseError(
            f"Line {line_no}: empty subject, predicate, or object field"
        )

    if o.startswith('"'):
        lexical, lang, datatype = split_literal(o, line_no)
        return Triple(
            s=s,
            p=p,
            o=o,
            is_literal=True,
            lexical=lexical,
            lang=lang,
            datatype=datatype,
            line_no=line_no,
        )

    return Triple(
        s=s,
        p=p,
        o=o,
        is_literal=False,
        line_no=line_no,
    )


NTRIPLE_PATTERN = re.compile(r"^\s*<([^>]+)>\s+<([^>]+)>\s+(.+?)\s*\.\s*$")
FREEBASE_NS = "http://rdf.freebase.com/ns/"


def parse_line_auto(line: str, line_no: int) -> Triple:
    """
    Parse either preprocessed TSV or raw N-Triples-like Freebase lines.
    """
    if "\t" in line and not line.lstrip().startswith("<"):
        return parse_preprocessed_line(line, line_no)
    return parse_raw_ntriple_line(line, line_no)


def parse_raw_ntriple_line(line: str, line_no: int) -> Triple:
    match = NTRIPLE_PATTERN.match(line)
    if not match:
        raise TripleParseError(f"Line {line_no}: invalid ntriple line")

    s_uri, p_uri, obj_token = match.groups()
    subject = _normalize_fb_uri(s_uri)
    predicate = _normalize_fb_uri(p_uri)
    obj_token = obj_token.strip()

    if obj_token.startswith("<") and obj_token.endswith(">"):
        object_uri = obj_token[1:-1]
        return Triple(
            s=subject,
            p=predicate,
            o=_normalize_fb_uri(object_uri),
            is_literal=False,
            line_no=line_no,
        )

    if obj_token.startswith('"'):
        lexical, lang, datatype = split_literal_ntriple(obj_token, line_no)
        return Triple(
            s=subject,
            p=predicate,
            o=obj_token,
            is_literal=True,
            lexical=lexical,
            lang=lang,
            datatype=datatype,
            line_no=line_no,
        )

    raise TripleParseError(f"Line {line_no}: unsupported object token")


def split_literal(token: str, line_no: int) -> tuple[str, str | None, str | None]:
    end = find_closing_quote(token)
    if end < 1:
        raise TripleParseError(f"Line {line_no}: unterminated literal")

    lexical = token[1:end]
    suffix = token[end + 1:]

    if not suffix:
        return lexical, None, None

    if suffix.startswith("@"):
        return lexical, suffix[1:], None

    if suffix.startswith("^^"):
        return lexical, None, suffix[2:]

    raise TripleParseError(f"Line {line_no}: invalid literal suffix {suffix!r}")


def split_literal_ntriple(token: str, line_no: int) -> tuple[str, str | None, str | None]:
    end = find_closing_quote(token)
    if end < 1:
        raise TripleParseError(f"Line {line_no}: unterminated literal")

    lexical = _unescape_lexical(token[1:end])
    suffix = token[end + 1:]
    if not suffix:
        return lexical, None, None
    if suffix.startswith("@"):
        return lexical, suffix[1:], None
    if suffix.startswith("^^<") and suffix.endswith(">"):
        return lexical, None, suffix[3:-1]
    raise TripleParseError(f"Line {line_no}: invalid literal suffix {suffix!r}")


def find_closing_quote(token: str) -> int:
    escaped = False
    for i in range(1, len(token)):
        ch = token[i]
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == '"':
            return i
    return -1


def _normalize_fb_uri(uri: str) -> str:
    if uri.startswith(FREEBASE_NS):
        local = uri[len(FREEBASE_NS):]
        return "/" + local.replace(".", "/")
    if uri.startswith("/"):
        return uri
    return uri


# def _unescape_lexical(value: str) -> str:
#     return (
#         value.replace(r"\\", "\\")
#         .replace(r"\"", '"')
#         .replace(r"\t", "\t")
#         .replace(r"\n", "\n")
#         .replace(r"\r", "\r")
#     )

# CHANGE

def _unescape_lexical(value: str) -> str:
    try:
        return codecs.decode(value, "unicode_escape")
    except UnicodeDecodeError:
        # rare malformed escape; fall back to raw
        return value