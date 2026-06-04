#!/usr/bin/env python3
"""
Generate a small tab-separated Freebase sample for smoke testing.

Default target is ~1000 triples with schema, binary CVTs (marriage + employment),
direct edges, and literal properties — enough to exercise CVT detection/flattening
without running a full dump.
"""
from __future__ import annotations

import argparse
from pathlib import Path


SCHEMA_TRIPLES = [
    "/people/marriage\t/freebase/type_hints/mediator\t/type/boolean/true",
    "/people/employment_tenure\t/freebase/type_hints/mediator\t/type/boolean/true",
    "/people/person/marriage\t/type/property/expected_type\t/people/marriage",
    "/people/person/employment_history\t/type/property/expected_type\t/people/employment_tenure",
    "/people/person/nationality\t/type/property/expected_type\t/location/country",
    "/people/person/place_of_birth\t/type/property/expected_type\t/location/location",
    "/people/marriage/spouse\t/type/property/expected_type\t/people/person",
    "/people/marriage/from\t/type/property/expected_type\t/type/datetime",
    "/people/employment_tenure/person\t/type/property/expected_type\t/people/person",
    "/people/employment_tenure/company\t/type/property/expected_type\t/organization/organization",
    "/people/employment_tenure/from\t/type/property/expected_type\t/type/datetime",
    "/people/person\t/type/object/type\t/type/type",
    "/location/country\t/type/object/type\t/type/type",
    "/organization/organization\t/type/object/type\t/type/type",
]


def _triples_for_person(person_id: int) -> list[str]:
    mid = f"/m/person{person_id:04d}"
    name = f"Person {person_id}"
    return [
        f"{mid}\t/type/object/type\t/people/person",
        f'{mid}\t/type/object/name\t"{name}"@en',
    ]


def _triples_for_country(country_id: int) -> list[str]:
    mid = f"/m/country{country_id:03d}"
    name = f"Country {country_id}"
    return [
        f"{mid}\t/type/object/type\t/location/country",
        f'{mid}\t/type/object/name\t"{name}"@en',
    ]


def _triples_for_company(company_id: int) -> list[str]:
    mid = f"/m/org{company_id:03d}"
    name = f"Company {company_id}"
    return [
        f"{mid}\t/type/object/type\t/organization/organization",
        f'{mid}\t/type/object/name\t"{name}"@en',
    ]


def _marriage_cvt(person_a: int, person_b: int, cvt_id: int, year: int) -> list[str]:
    cvt = f"/m/marriage{cvt_id:04d}"
    return [
        f"/m/person{person_a:04d}\t/people/person/marriage\t{cvt}",
        f"{cvt}\t/type/object/type\t/people/marriage",
        f"{cvt}\t/people/marriage/spouse\t/m/person{person_b:04d}",
        f'{cvt}\t/people/marriage/from\t"{year}-06-01"^^<http://www.w3.org/2001/XMLSchema#date>',
    ]


def _employment_cvt(person_id: int, company_id: int, cvt_id: int, year: int) -> list[str]:
    cvt = f"/m/job{cvt_id:04d}"
    return [
        f"/m/person{person_id:04d}\t/people/person/employment_history\t{cvt}",
        f"{cvt}\t/type/object/type\t/people/employment_tenure",
        f"{cvt}\t/people/employment_tenure/company\t/m/org{company_id:03d}",
        f'{cvt}\t/people/employment_tenure/from\t"{year}-01-01"^^<http://www.w3.org/2001/XMLSchema#date>',
    ]


def generate_triples(target: int) -> list[str]:
    lines = list(SCHEMA_TRIPLES)

    person_count = max(40, target // 10)
    country_count = max(5, target // 200)
    company_count = max(5, target // 200)

    for idx in range(1, person_count + 1):
        lines.extend(_triples_for_person(idx))
    for idx in range(1, country_count + 1):
        lines.extend(_triples_for_country(idx))
    for idx in range(1, company_count + 1):
        lines.extend(_triples_for_company(idx))

    marriage_id = 1
    employment_id = 1
    person_idx = 1

    while len(lines) < target:
        spouse = (person_idx % person_count) + 1
        partner = ((person_idx + 17) % person_count) + 1
        if spouse == partner:
            partner = (partner % person_count) + 1 or 1
        year = 1990 + (marriage_id % 25)
        lines.extend(_marriage_cvt(spouse, partner, marriage_id, year))
        marriage_id += 1

        if marriage_id % 2 == 0:
            company = (employment_id % company_count) + 1
            year = 2000 + (employment_id % 20)
            lines.extend(_employment_cvt(spouse, company, employment_id, year))
            employment_id += 1

        country = (person_idx % country_count) + 1
        lines.append(
            f"/m/person{spouse:04d}\t/people/person/nationality\t/m/country{country:03d}"
        )
        person_idx += 1

    return lines[:target]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate small Freebase smoke-test data")
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output .nt path (tab-separated triples)",
    )
    parser.add_argument(
        "--target-triples",
        type=int,
        default=1000,
        help="Approximate number of triples to emit (default: 1000)",
    )
    args = parser.parse_args()

    lines = generate_triples(args.target_triples)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {len(lines)} triples to {args.output}")


if __name__ == "__main__":
    main()
