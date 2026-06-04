from __future__ import annotations

import json
from typing import Any


TEXTUAL_PROPERTY_KEYS = {
    "name",
    "description",
    "aliases",
}


def _humanize_token(value: str) -> str:
    return value.replace("_", " ").replace(".", " ").strip()


def _format_fb_type(fb_type: str) -> str:
    return _humanize_token(fb_type.strip("/").split("/")[-1])


def _stringify_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        parts = [_stringify_value(item) for item in value]
        rendered = [part for part in parts if part]
        if not rendered:
            return None
        return ", ".join(rendered)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _collect_text_properties(
    properties: dict[str, Any],
    multi_properties: dict[str, list[Any]],
) -> list[str]:
    lines: list[str] = []

    for key in sorted(properties):
        if key in TEXTUAL_PROPERTY_KEYS:
            continue
        rendered = _stringify_value(properties[key])
        if rendered:
            lines.append(f"{_humanize_token(key)}: {rendered}")

    for key in sorted(multi_properties):
        if key in TEXTUAL_PROPERTY_KEYS:
            continue
        rendered = _stringify_value(multi_properties[key])
        if rendered:
            lines.append(f"{_humanize_token(key)}: {rendered}")

    return lines


def build_node_description(node: dict[str, Any]) -> str:
    labels = node.get("labels") or ["Entity"]
    props = node.get("properties") or {}
    multi_props = node.get("multi_properties") or {}
    fb_types = node.get("fb_types") or []

    name = props.get("name") or node.get("mid", "unknown")
    label_text = ", ".join(labels)
    parts = [f"{label_text}: {name}"]

    if fb_types:
        rendered_types = ", ".join(_format_fb_type(fb_type) for fb_type in fb_types)
        parts.append(f"Freebase types: {rendered_types}")

    if props.get("description"):
        parts.append(f"Description: {props['description']}")

    aliases = multi_props.get("aliases") or props.get("aliases")
    alias_text = _stringify_value(aliases)
    if alias_text:
        parts.append(f"Also known as: {alias_text}")

    parts.extend(_collect_text_properties(props, multi_props))
    return ". ".join(parts)


def build_edge_description(
    edge: dict[str, Any],
    *,
    source_name: str | None = None,
    target_name: str | None = None,
) -> str:
    rel_type = edge.get("rel_type") or "RELATION"
    predicate = edge.get("predicate") or ""
    props = edge.get("properties") or {}

    source_label = source_name or edge.get("source_mid", "unknown source")
    target_label = target_name or edge.get("target_mid", "unknown target")

    parts = [f"{rel_type} relationship from {source_label} to {target_label}"]
    if predicate:
        parts.append(f"Predicate: {predicate}")

    for key in sorted(props):
        if key == "cvt_mid":
            continue
        rendered = _stringify_value(props[key])
        if rendered:
            parts.append(f"{_humanize_token(key)}: {rendered}")

    return ". ".join(parts)
