from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TypedLiteral:
    lexical: str
    value_kind: str
    value: object
    datatype: Optional[str] = None
    lang: Optional[str] = None

_XSD = "http://www.w3.org/2001/xmlschema#"
_FB  = "/type/"

_INT_DTS   = {f"{_XSD}int", f"{_XSD}integer", f"{_XSD}long",
              f"{_XSD}short", f"{_XSD}byte",
              f"{_XSD}nonnegativeinteger", f"{_XSD}positiveinteger",
              f"{_FB}int"}
_FLOAT_DTS = {f"{_XSD}float", f"{_XSD}double", f"{_XSD}decimal",
              f"{_FB}float"}
_BOOL_DTS  = {f"{_XSD}boolean", f"{_FB}boolean"}
_DATE_DTS  = {f"{_XSD}date", f"{_XSD}datetime",
              f"{_FB}datetime"}
_PARTIAL   = {f"{_XSD}gyear", f"{_XSD}gyearmonth"}

def parse_typed_literal(lexical, datatype, lang):
    if lang is not None:
        return TypedLiteral(lexical, "lang_text", lexical, datatype, lang)
    if datatype is None:
        return TypedLiteral(lexical, "text", lexical, None, None)
    dt = datatype.lower()
    if dt in _INT_DTS:
        try: return TypedLiteral(lexical, "int", int(lexical), datatype, None)
        except ValueError: pass
    elif dt in _FLOAT_DTS:
        try: return TypedLiteral(lexical, "float", float(lexical), datatype, None)
        except ValueError: pass
    elif dt in _BOOL_DTS:
        return TypedLiteral(lexical, "boolean", lexical.lower() == "true", datatype, None)
    elif dt in _DATE_DTS:
        return TypedLiteral(lexical, "date_like", lexical, datatype, None)
    elif dt in _PARTIAL:
        return TypedLiteral(lexical, "partial_date", lexical, datatype, None)
    return TypedLiteral(lexical, "unknown_typed_literal", lexical, datatype, None)



# CHANGE
 
# def parse_typed_literal(
#     lexical: str,
#     datatype: str | None,
#     lang: str | None,
# ) -> TypedLiteral:
#     """
#     Parse a preprocessed literal into a lightweight typed representation.
#     """
#     if lang is not None:
#         return TypedLiteral(
#             lexical=lexical,
#             value_kind="lang_text",
#             value=lexical,
#             datatype=datatype,
#             lang=lang,
#         )

#     if datatype is None:
#         return TypedLiteral(
#             lexical=lexical,
#             value_kind="text",
#             value=lexical,
#             datatype=None,
#             lang=None,
#         )

#     dt = datatype.lower()

#     if dt.endswith("type.int"):
#         try:
#             parsed = int(lexical)
#         except (TypeError, ValueError):
#             parsed = lexical
#             kind = "unknown_typed_literal"
#         else:
#             kind = "int"
#         return TypedLiteral(
#             lexical=lexical,
#             value_kind=kind,
#             value=parsed,
#             datatype=datatype,
#             lang=None,
#         )

#     if dt.endswith("type.float") or dt.endswith("xmlschema#double"):
#         try:
#             parsed = float(lexical)
#         except (TypeError, ValueError):
#             parsed = lexical
#             kind = "unknown_typed_literal"
#         else:
#             kind = "float"
#         return TypedLiteral(
#             lexical=lexical,
#             value_kind=kind,
#             value=parsed,
#             datatype=datatype,
#             lang=None,
#         )

#     if dt.endswith("type.boolean"):
#         return TypedLiteral(
#             lexical=lexical,
#             value_kind="boolean",
#             value=(lexical.lower() == "true"),
#             datatype=datatype,
#             lang=None,
#         )

#     if dt.endswith("xmlschema#date") or dt.endswith("xmlschema#datetime"):
#         return TypedLiteral(
#             lexical=lexical,
#             value_kind="date_like",
#             value=lexical,
#             datatype=datatype,
#             lang=None,
#         )

#     if dt.endswith("xmlschema#gyear") or dt.endswith("xmlschema#gyearmonth"):
#         return TypedLiteral(
#             lexical=lexical,
#             value_kind="partial_date",
#             value=lexical,
#             datatype=datatype,
#             lang=None,
#         )

#     return TypedLiteral(
#         lexical=lexical,
#         value_kind="unknown_typed_literal",
#         value=lexical,
#         datatype=datatype,
#         lang=None,
#     )
