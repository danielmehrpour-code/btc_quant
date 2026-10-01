"""Ingest-lineage (PHASE 4, steg 4): posttyper, identitet och strikt tolkning. Ingen I/O.

  * IngestRecord = en oföränderlig post per ingest, även vid upprepad ingest av
    identiskt innehåll (seq + prev_ingest_id gör varje händelse unik).
  * ingest_id = sha256(b"btc-quant/ingest-id/v1\\x00" + kompakt kanonisk JSON av posten).
    Filen (to_file_bytes) är kanoniskt kodad: sorterade nycklar, indent 2, radslut.
  * Phase 2:s `extra` delas upp: CONTENT_DERIVED_KEYS går till objektmanifestet,
    SOURCE_KEYS (sources, duplicates_dropped) går ENDAST till ingest-posten.
    Okänd nyckel -> IngestExtraError (en ValueError) innan något skrivs.
  * Fel: en egen klass per felsort, som ärver både DataError och ValueError (steg 2-mönstret,
    jfr InvalidSourcesError): IngestExtraError (Phase 2:s extra) och IngestRecordError
    (ingest-post/källa). `except ValueError` och `except DataError` fångar båda.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from btc_trading_system.data_validation.errors import DataError
from btc_trading_system.database import catalog
from btc_trading_system.database.refs import DatasetVersionRef, compact_json_bytes

INGEST_FORMAT = "btc-quant/ingest/v1"
INGEST_ID_DOMAIN = b"btc-quant/ingest-id/v1\x00"
CONTENT_DERIVED_KEYS = frozenset({"gaps", "zero_volume_bars"})
SOURCE_KEYS = frozenset({"sources", "duplicates_dropped"})
PUBLICATIONS = ("published", "reused")
REF_OUTCOMES = ("changed", "unchanged")
_RECORD_KEYS = frozenset({"format", "seq", "prev_ingest_id", "dataset", "dataset_id", "data_version",
                          "publication", "ref_outcome", "previous_data_version", "ref_log_seq",
                          "ref_log_entry_sha256", "sources", "duplicates_dropped", "params", "code"})
_PARAM_KEYS = frozenset({"now", "fail_on_gaps", "allow_regression"})
_CODE_KEYS = frozenset({"version", "worktree_clean"})
_SOURCE_FIELDS = frozenset({"name", "rows", "sha256"})


class IngestExtraError(DataError, ValueError):
    """Phase 2:s extra kan inte delas upp: saknad eller okänd nyckel, fel typ eller ogiltig källa."""


class IngestRecordError(DataError, ValueError):
    """Ogiltig ingest-post eller källa (konstruktion eller strikt tolkning)."""


def _file_json_bytes(obj: Any) -> bytes:
    # Samma kodning som versioned_store.canonical_json_bytes.
    return (json.dumps(obj, sort_keys=True, indent=2) + "\n").encode()


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def check_content_extra(extra: dict[str, Any]) -> None:
    """Det som går till objektmanifestet får bara innehålla innehållshärledda nycklar."""
    unknown = sorted(set(extra) - CONTENT_DERIVED_KEYS)
    if unknown:
        raise IngestExtraError(f"extra till objektet får bara innehålla {sorted(CONTENT_DERIVED_KEYS)}; "
                         f"fick även {unknown}")


@dataclass(frozen=True)
class IngestSource:
    name: str
    rows: int
    sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise IngestRecordError(f"ogiltigt källnamn: {self.name!r}")
        if not _is_int(self.rows) or self.rows < 0:
            raise IngestRecordError(f"ogiltigt radantal för {self.name}: {self.rows!r}")
        if not catalog.is_sha256_hex(self.sha256):
            raise IngestRecordError(f"ogiltig sha256 för {self.name}: {self.sha256!r}")

    def to_json_obj(self) -> dict[str, Any]:
        return {"name": self.name, "rows": self.rows, "sha256": self.sha256}

    @classmethod
    def from_json_obj(cls, obj: Any) -> IngestSource:
        if not isinstance(obj, dict) or set(obj) != _SOURCE_FIELDS:
            raise IngestRecordError(f"ogiltig källa: {obj!r}")
        return cls(obj["name"], obj["rows"], obj["sha256"])


def split_ingest_extra(extra: dict[str, Any]) -> tuple[dict[str, Any], tuple[IngestSource, ...], int]:
    """Phase 2:s extra -> (innehållsnycklar till objektet, källor, duplicates_dropped).
    Innehållsdelen kontrolleras separat med check_content_extra innan något skrivs."""
    missing = sorted((CONTENT_DERIVED_KEYS | SOURCE_KEYS) - set(extra))
    if missing:
        raise IngestExtraError(f"Phase 2 extra saknar {missing}")
    if not isinstance(extra["sources"], list):
        raise IngestExtraError("Phase 2 extra: sources måste vara en lista")
    try:
        sources = tuple(IngestSource.from_json_obj(s) for s in extra["sources"])
    except IngestRecordError as e:
        raise IngestExtraError(f"Phase 2 extra: {e}") from e
    dropped = extra["duplicates_dropped"]
    if not _is_int(dropped) or dropped < 0:
        raise IngestExtraError(f"ogiltigt duplicates_dropped: {dropped!r}")
    content = {k: v for k, v in extra.items() if k not in SOURCE_KEYS}
    return content, sources, dropped


def _check_now(now: Any) -> None:
    if not isinstance(now, str):
        raise IngestRecordError(f"params.now måste vara en ISO-sträng: {now!r}")
    try:
        parsed = datetime.fromisoformat(now)
    except ValueError as e:
        raise IngestRecordError(f"params.now är inte ISO 8601: {now!r}") from e
    if parsed.utcoffset() != timedelta(0) or parsed.isoformat() != now:
        raise IngestRecordError(f"params.now måste vara kanonisk UTC (isoformat, +00:00): {now!r}")


@dataclass(frozen=True)
class IngestRecord:
    seq: int
    prev_ingest_id: str | None
    ref: DatasetVersionRef
    publication: str
    ref_outcome: str
    previous_data_version: str | None
    ref_log_seq: int | None
    ref_log_entry_sha256: str | None
    sources: tuple[IngestSource, ...]
    duplicates_dropped: int
    now: str
    fail_on_gaps: bool
    allow_regression: bool
    code_version: str
    worktree_clean: bool | None

    def __post_init__(self) -> None:
        if not _is_int(self.seq) or self.seq < 1:
            raise IngestRecordError(f"ogiltigt seq: {self.seq!r}")
        if (self.seq == 1) != (self.prev_ingest_id is None):
            raise IngestRecordError("prev_ingest_id saknas exakt när seq == 1")
        if self.prev_ingest_id is not None and not catalog.is_sha256_hex(self.prev_ingest_id):
            raise IngestRecordError(f"ogiltigt prev_ingest_id: {self.prev_ingest_id!r}")
        if not isinstance(self.ref, DatasetVersionRef):
            raise IngestRecordError("ref måste vara DatasetVersionRef")
        if self.publication not in PUBLICATIONS or self.ref_outcome not in REF_OUTCOMES:
            raise IngestRecordError(f"ogiltigt utfall: {self.publication!r}/{self.ref_outcome!r}")
        if self.previous_data_version is not None and not catalog.is_sha256_hex(self.previous_data_version):
            raise IngestRecordError(f"ogiltig previous_data_version: {self.previous_data_version!r}")
        if self.ref_outcome == "changed":
            if not isinstance(self.ref_log_seq, int) or isinstance(self.ref_log_seq, bool) \
                    or self.ref_log_seq < 1 \
                    or not catalog.is_sha256_hex(self.ref_log_entry_sha256):
                raise IngestRecordError("changed kräver ref_log_seq och ref_log_entry_sha256")
            if self.previous_data_version == self.ref.data_version:
                raise IngestRecordError("changed kräver previous_data_version != data_version")
        else:
            if self.ref_log_seq is not None or self.ref_log_entry_sha256 is not None:
                raise IngestRecordError("unchanged får inte ha ref_log-länk")
            if self.previous_data_version != self.ref.data_version:
                raise IngestRecordError("unchanged kräver previous_data_version == data_version")
        if not isinstance(self.sources, tuple) or not self.sources \
                or not all(isinstance(s, IngestSource) for s in self.sources):
            raise IngestRecordError("sources måste vara en icke-tom tuple av IngestSource")
        if [s.name for s in self.sources] != sorted(s.name for s in self.sources):
            raise IngestRecordError("sources måste vara sorterade på namn (Phase 2-ordning)")
        if not _is_int(self.duplicates_dropped) or self.duplicates_dropped < 0:
            raise IngestRecordError(f"ogiltigt duplicates_dropped: {self.duplicates_dropped!r}")
        _check_now(self.now)
        if not isinstance(self.fail_on_gaps, bool) or not isinstance(self.allow_regression, bool):
            raise IngestRecordError("fail_on_gaps och allow_regression måste vara bool")
        if not isinstance(self.code_version, str) or not self.code_version:
            raise IngestRecordError("code_version måste vara en icke-tom sträng")
        if self.worktree_clean is not None and not isinstance(self.worktree_clean, bool):
            raise IngestRecordError("worktree_clean måste vara bool eller None")

    @property
    def params(self) -> dict[str, Any]:
        return {"now": self.now, "fail_on_gaps": self.fail_on_gaps,
                "allow_regression": self.allow_regression}

    def to_json_obj(self) -> dict[str, Any]:
        return {"format": INGEST_FORMAT, "seq": self.seq, "prev_ingest_id": self.prev_ingest_id,
                **self.ref.to_json_obj(),
                "publication": self.publication, "ref_outcome": self.ref_outcome,
                "previous_data_version": self.previous_data_version,
                "ref_log_seq": self.ref_log_seq, "ref_log_entry_sha256": self.ref_log_entry_sha256,
                "sources": [s.to_json_obj() for s in self.sources],
                "duplicates_dropped": self.duplicates_dropped, "params": self.params,
                "code": {"version": self.code_version, "worktree_clean": self.worktree_clean}}

    @property
    def ingest_id(self) -> str:
        return hashlib.sha256(INGEST_ID_DOMAIN + compact_json_bytes(self.to_json_obj())).hexdigest()

    def to_file_bytes(self) -> bytes:
        return _file_json_bytes(self.to_json_obj())

    @classmethod
    def from_file_bytes(cls, data: bytes, expected_id: str | None = None) -> IngestRecord:
        """Strikt: exakta nycklar, format, kanonisk kodning och (valfritt) förväntat id."""
        try:
            obj = json.loads(data)
        except (ValueError, UnicodeDecodeError) as e:
            raise IngestRecordError("ingest-posten är inte JSON") from e
        if not isinstance(obj, dict) or set(obj) != _RECORD_KEYS or obj["format"] != INGEST_FORMAT \
                or not isinstance(obj["params"], dict) or set(obj["params"]) != _PARAM_KEYS \
                or not isinstance(obj["code"], dict) or set(obj["code"]) != _CODE_KEYS \
                or not isinstance(obj["sources"], list):
            raise IngestRecordError("ogiltigt format på ingest-posten")
        try:
            ref = DatasetVersionRef.from_json_obj({k: obj[k] for k in ("dataset", "dataset_id", "data_version")})
        except (ValueError, TypeError) as e:
            raise IngestRecordError(f"ogiltig datasetreferens i ingest-posten: {e}") from e
        rec = cls(
            seq=obj["seq"], prev_ingest_id=obj["prev_ingest_id"], ref=ref,
            publication=obj["publication"], ref_outcome=obj["ref_outcome"],
            previous_data_version=obj["previous_data_version"],
            ref_log_seq=obj["ref_log_seq"], ref_log_entry_sha256=obj["ref_log_entry_sha256"],
            sources=tuple(IngestSource.from_json_obj(s) for s in obj["sources"]),
            duplicates_dropped=obj["duplicates_dropped"],
            now=obj["params"]["now"], fail_on_gaps=obj["params"]["fail_on_gaps"],
            allow_regression=obj["params"]["allow_regression"],
            code_version=obj["code"]["version"], worktree_clean=obj["code"]["worktree_clean"])
        if rec.to_file_bytes() != data:
            raise IngestRecordError("ingest-posten är inte kanoniskt kodad")
        if expected_id is not None and rec.ingest_id != expected_id:
            raise IngestRecordError(f"ingest_id {rec.ingest_id} != förväntat {expected_id}")
        return rec
