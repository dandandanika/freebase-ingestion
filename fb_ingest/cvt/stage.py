from __future__ import annotations

from fb_ingest.cvt.models import (
    CVTEntityFact,
    CVTIncomingFact,
    CVTLiteralFact,
    CVTRecord,
)


class CVTStager:
    """
    Accumulates CVT neighborhood facts keyed by cvt_mid.
    """

    def __init__(self):
        self.records: dict[str, CVTRecord] = {}

    def _get(self, cvt_mid: str) -> CVTRecord:
        if cvt_mid not in self.records:
            self.records[cvt_mid] = CVTRecord(cvt_mid=cvt_mid)
        return self.records[cvt_mid]

    def add_type(self, cvt_mid: str, fb_type: str) -> None:
        record = self._get(cvt_mid)
        if fb_type not in record.types:
            record.types.append(fb_type)

    def add_incoming(
        self,
        source_mid: str,
        predicate: str,
        cvt_mid: str,
        line_no: int = 0,
    ) -> None:
        record = self._get(cvt_mid)
        record.incoming.append(
            CVTIncomingFact(
                source_mid=source_mid,
                predicate=predicate,
                cvt_mid=cvt_mid,
                line_no=line_no,
            )
        )

    def add_entity_out(
        self,
        cvt_mid: str,
        predicate: str,
        target_mid: str,
        line_no: int = 0,
    ) -> None:
        record = self._get(cvt_mid)
        record.outgoing_entities.append(
            CVTEntityFact(
                cvt_mid=cvt_mid,
                predicate=predicate,
                target_mid=target_mid,
                line_no=line_no,
            )
        )

    def add_literal_out(
        self,
        cvt_mid: str,
        predicate: str,
        lexical: str,
        parsed_value,
        value_kind: str,
        datatype: str | None = None,
        lang: str | None = None,
        line_no: int = 0,
    ) -> None:
        record = self._get(cvt_mid)
        record.outgoing_literals.append(
            CVTLiteralFact(
                cvt_mid=cvt_mid,
                predicate=predicate,
                lexical=lexical,
                parsed_value=parsed_value,
                value_kind=value_kind,
                datatype=datatype,
                lang=lang,
                line_no=line_no,
            )
        )

    def add_chained_cvt(
        self,
        cvt_mid: str,
        predicate: str,
        target_cvt_mid: str,
    ) -> None:
        record = self._get(cvt_mid)
        record.chained_cvts.append((cvt_mid, predicate, target_cvt_mid))

    def get_record(self, cvt_mid: str) -> CVTRecord | None:
        return self.records.get(cvt_mid)

    def iter_records(self):
        yield from self.records.values()
