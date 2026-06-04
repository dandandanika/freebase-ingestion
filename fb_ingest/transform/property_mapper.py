from __future__ import annotations


SPECIAL_LITERAL_PROPERTY_MAP = {
    "/type/object/name": "name",
    "/common/topic/alias": "aliases",
    "/common/topic/description": "description",
}

MULTI_VALUED_DEFAULTS = {
    "/common/topic/alias",
}


def predicate_to_property_key(predicate: str) -> str:
    if predicate in SPECIAL_LITERAL_PROPERTY_MAP:
        return SPECIAL_LITERAL_PROPERTY_MAP[predicate]
    return predicate.strip("/").replace("/", ".")


def is_multi_valued_property(predicate: str) -> bool:
    if predicate in MULTI_VALUED_DEFAULTS:
        return True
    return False
