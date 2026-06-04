from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from config.special_predicates import TOPIC_ALIAS, TOPIC_DESCRIPTION, TYPE_OBJECT_NAME
from fb_ingest.transform.property_mapper import (
    is_multi_valued_property,
    predicate_to_property_key,
)


PROMOTED_TYPE_LABELS = {
    "/people/person": "Person",
    "/location/location": "Location",
    "/organization/organization": "Organization",
    "/film/film": "Film",
    "/music/artist": "Artist",
    "/music/album": "Album",
    "/music/recording": "Recording",
    "/book/book": "Book",
    "/tv/tv_program": "TvProgram",
    "/sports/sports_team": "SportsTeam",
}


@dataclass
class NodeRecord:
    mid: str
    labels: list[str] = field(default_factory=lambda: ["Entity"])
    fb_types: list[str] = field(default_factory=list)
    properties: dict[str, Any] = field(default_factory=dict)
    multi_properties: dict[str, list[Any]] = field(default_factory=dict)

    def add_type(self, fb_type: str) -> None:
        if fb_type not in self.fb_types:
            self.fb_types.append(fb_type)

        promoted = PROMOTED_TYPE_LABELS.get(fb_type)
        if promoted and promoted not in self.labels:
            self.labels.append(promoted)

    def add_literal_property(self, predicate: str, value: Any) -> None:
        key = predicate_to_property_key(predicate)

        if predicate in {TOPIC_ALIAS} or is_multi_valued_property(predicate):
            arr = self.multi_properties.setdefault(key, [])
            if value not in arr:
                arr.append(value)
            return

        if key in self.properties:
            existing = self.properties[key]
            if existing != value:
                arr = self.multi_properties.setdefault(key, [])
                if existing not in arr:
                    arr.append(existing)
                if value not in arr:
                    arr.append(value)
                self.properties.pop(key, None)
            return

        self.properties[key] = value

    def add_special_fact(self, predicate: str, payload: dict) -> None:
        if predicate == TYPE_OBJECT_NAME:
            text = payload.get("text")
            lang = payload.get("lang")
            if lang in (None, "en") and text:
                self.properties["name"] = text
            return

        if predicate == TOPIC_ALIAS:
            text = payload.get("text")
            if text:
                arr = self.multi_properties.setdefault("aliases", [])
                if text not in arr:
                    arr.append(text)
            return

        if predicate == TOPIC_DESCRIPTION:
            text = payload.get("text")
            lang = payload.get("lang")
            if lang in (None, "en") and text and "description" not in self.properties:
                self.properties["description"] = text
            return

        key = predicate_to_property_key(predicate)
        arr = self.multi_properties.setdefault(key, [])
        if payload not in arr:
            arr.append(payload)
