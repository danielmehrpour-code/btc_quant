"""Referenstyper (PHASE 4, steg 3): DatasetVersionRef och DataSnapshot. Ingen I/O.

  * DatasetVersionRef = (dataset, data_version). dataset_id härleds alltid ur
    nyckeln (catalog.dataset_id) och kan aldrig anges fritt.
  * DataSnapshot = oföränderlig, icke-tom mängd DatasetVersionRef, högst en
    version per dataset, SORTERAD på dataset_id. Osorterade medlemmar avvisas;
    DataSnapshot.of() sorterar kanoniskt.

SNAPSHOT-IDENTITET
  snapshot_id = sha256(b"btc-quant/snapshot-id/v1\\x00" + kompakt kanonisk JSON av
  de sorterade medlemmarna). Inga namn/etiketter ingår i identiteten.
  Filformatet (to_file_bytes) är kanoniskt: sorterade nycklar, indent 2, radslut,
  och from_file_bytes accepterar bara exakt den kodningen.

SNAPSHOT V2 (etapp B steg 4, SB-S4-a A2)
  En snapshot kan dessutom pinna funding-versioner (FundingVersionRef, högst en per
  funding-dataset, sorterade på dataset_id). Utan funding-medlemmar är den exakt v1:
  samma format, samma bytes, samma snapshot_id (befintliga snapshots oförändrade).
  Med funding-medlemmar: format btc-quant/snapshot/v2 och
  snapshot_id = sha256(b"btc-quant/snapshot-id/v2\\x00" + kompakt kanonisk JSON av
  {"funding": [...], "members": [...]}). En v2-fil utan funding avvisas (icke-kanonisk).
  OHLCV-medlemmarna krävs som förut (minst en).
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from btc_trading_system.data.funding import FundingKey
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.database import catalog

SNAPSHOT_FORMAT = "btc-quant/snapshot/v1"
SNAPSHOT_ID_DOMAIN = b"btc-quant/snapshot-id/v1\x00"
SNAPSHOT_FORMAT_V2 = "btc-quant/snapshot/v2"
SNAPSHOT_ID_DOMAIN_V2 = b"btc-quant/snapshot-id/v2\x00"
_REF_KEYS = frozenset({"dataset", "dataset_id", "data_version"})
_SNAPSHOT_KEYS = frozenset({"format", "snapshot_id", "members"})
_SNAPSHOT_KEYS_V2 = frozenset({"format", "snapshot_id", "members", "funding"})


def compact_json_bytes(obj: Any) -> bytes:
    """Kompakt kanonisk JSON (identitetshashar, ref_log-rader)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _file_json_bytes(obj: Any) -> bytes:
    # Samma kodning som versioned_store.canonical_json_bytes (Phase 2-manifestets stil).
    return (json.dumps(obj, sort_keys=True, indent=2) + "\n").encode()


@dataclass(frozen=True)
class DatasetVersionRef:
    key: DatasetKey
    data_version: str

    def __post_init__(self) -> None:
        if not isinstance(self.key, DatasetKey):
            raise TypeError(f"key måste vara DatasetKey, fick {type(self.key).__name__}")
        catalog.dataset_key_json(self.key)                   # validerar exchange
        if not catalog.is_sha256_hex(self.data_version):
            raise ValueError(f"ogiltig data_version: {self.data_version!r}")

    @property
    def dataset_id(self) -> str:
        return catalog.dataset_id(self.key)

    def to_json_obj(self) -> dict[str, Any]:
        return {"dataset": json.loads(catalog.dataset_key_json(self.key)),
                "dataset_id": self.dataset_id, "data_version": self.data_version}

    @classmethod
    def from_json_obj(cls, obj: Any) -> DatasetVersionRef:
        """Strikt: exakt nycklarna, kanonisk datasetnyckel, dataset_id måste stämma."""
        if not isinstance(obj, dict) or set(obj) != _REF_KEYS or not isinstance(obj["dataset"], dict):
            raise ValueError(f"ogiltig datasetversionsreferens: {obj!r}")
        try:
            key = catalog.dataset_key(**obj["dataset"])
        except TypeError as e:
            raise ValueError(f"ogiltig datasetnyckel: {obj['dataset']!r}") from e
        ref = cls(key, obj["data_version"])
        if ref.dataset_id != obj["dataset_id"]:
            raise ValueError(f"dataset_id {obj['dataset_id']!r} matchar inte {key.relpath}")
        return ref


@dataclass(frozen=True)
class FundingVersionRef:
    """(funding-dataset, data_version); dataset_id härleds ur nyckeln (catalog.funding_dataset_id)."""
    key: FundingKey
    data_version: str

    def __post_init__(self) -> None:
        if not isinstance(self.key, FundingKey):
            raise TypeError(f"key måste vara FundingKey, fick {type(self.key).__name__}")
        catalog.funding_dataset_key_json(self.key)           # validerar exchange
        if not catalog.is_sha256_hex(self.data_version):
            raise ValueError(f"ogiltig data_version: {self.data_version!r}")

    @property
    def dataset_id(self) -> str:
        return catalog.funding_dataset_id(self.key)

    def to_json_obj(self) -> dict[str, Any]:
        return {"dataset": json.loads(catalog.funding_dataset_key_json(self.key)),
                "dataset_id": self.dataset_id, "data_version": self.data_version}

    @classmethod
    def from_json_obj(cls, obj: Any) -> FundingVersionRef:
        """Strikt: exakt nycklarna, kanonisk funding-nyckel (kind = funding), dataset_id måste stämma."""
        if not isinstance(obj, dict) or set(obj) != _REF_KEYS or not isinstance(obj["dataset"], dict) \
                or set(obj["dataset"]) != {"exchange", "instrument", "kind", "symbol"} \
                or obj["dataset"]["kind"] != "funding":
            raise ValueError(f"ogiltig funding-versionsreferens: {obj!r}")
        fields = {k: obj["dataset"][k] for k in ("exchange", "instrument", "symbol")}
        if not all(isinstance(v, str) for v in fields.values()):
            raise ValueError(f"ogiltig funding-nyckel: {obj['dataset']!r}")
        ref = cls(catalog.funding_dataset_key(**fields), obj["data_version"])
        if ref.dataset_id != obj["dataset_id"]:
            raise ValueError(f"dataset_id {obj['dataset_id']!r} matchar inte {ref.key.relpath}")
        return ref


@dataclass(frozen=True)
class DataSnapshot:
    members: tuple[DatasetVersionRef, ...]
    funding: tuple[FundingVersionRef, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.members, tuple):
            raise TypeError("members måste vara en tuple (använd DataSnapshot.of)")
        if not self.members:
            raise ValueError("en snapshot måste ha minst en medlem")
        if not all(isinstance(m, DatasetVersionRef) for m in self.members):
            raise TypeError("members måste vara DatasetVersionRef")
        ids = [m.dataset_id for m in self.members]
        if len(set(ids)) != len(ids):
            raise ValueError("högst en version per dataset i en snapshot")
        if ids != sorted(ids):
            raise ValueError("snapshotmedlemmar måste vara sorterade på dataset_id (använd DataSnapshot.of)")
        if not isinstance(self.funding, tuple) or not all(isinstance(f, FundingVersionRef) for f in self.funding):
            raise TypeError("funding måste vara en tuple av FundingVersionRef (använd DataSnapshot.of)")
        fids = [f.dataset_id for f in self.funding]
        if len(set(fids)) != len(fids):
            raise ValueError("högst en version per funding-dataset i en snapshot")
        if fids != sorted(fids):
            raise ValueError("funding-medlemmar måste vara sorterade på dataset_id (använd DataSnapshot.of)")

    @classmethod
    def of(cls, members: Iterable[DatasetVersionRef], funding: Iterable[FundingVersionRef] = ()) -> DataSnapshot:
        """Kanonisk konstruktion: sorterar på dataset_id (OHLCV och funding var för sig)."""
        pins = tuple(sorted(funding, key=lambda f: f.dataset_id))
        if pins:
            return cls(tuple(sorted(members, key=lambda m: m.dataset_id)), pins)
        return cls(tuple(sorted(members, key=lambda m: m.dataset_id)))

    def canonical_members_bytes(self) -> bytes:
        if self.funding:                                  # v2 (SB-S4-a A2)
            return compact_json_bytes({"funding": [f.to_json_obj() for f in self.funding],
                                       "members": [m.to_json_obj() for m in self.members]})
        return compact_json_bytes([m.to_json_obj() for m in self.members])

    @property
    def snapshot_id(self) -> str:
        domain = SNAPSHOT_ID_DOMAIN_V2 if self.funding else SNAPSHOT_ID_DOMAIN
        return hashlib.sha256(domain + self.canonical_members_bytes()).hexdigest()

    def funding_version_of(self, key: FundingKey) -> FundingVersionRef | None:
        did = catalog.funding_dataset_id(key)
        return next((f for f in self.funding if f.dataset_id == did), None)

    def version_of(self, key: DatasetKey) -> DatasetVersionRef | None:
        did = catalog.dataset_id(key)
        return next((m for m in self.members if m.dataset_id == did), None)

    def to_file_bytes(self) -> bytes:
        if self.funding:
            return _file_json_bytes({"format": SNAPSHOT_FORMAT_V2, "snapshot_id": self.snapshot_id,
                                     "members": [m.to_json_obj() for m in self.members],
                                     "funding": [f.to_json_obj() for f in self.funding]})
        return _file_json_bytes({"format": SNAPSHOT_FORMAT, "snapshot_id": self.snapshot_id,
                                 "members": [m.to_json_obj() for m in self.members]})

    @classmethod
    def from_file_bytes(cls, data: bytes, expected_id: str | None = None) -> DataSnapshot:
        """Strikt tolkning: format, nycklar, sortering, id och exakt kanonisk kodning."""
        try:
            obj = json.loads(data)
        except (ValueError, UnicodeDecodeError) as e:
            raise ValueError("snapshotfilen är inte JSON") from e
        v1 = isinstance(obj, dict) and set(obj) == _SNAPSHOT_KEYS and obj["format"] == SNAPSHOT_FORMAT
        v2 = isinstance(obj, dict) and set(obj) == _SNAPSHOT_KEYS_V2 and obj["format"] == SNAPSHOT_FORMAT_V2 \
            and isinstance(obj["funding"], list) and len(obj["funding"]) > 0
        if not (v1 or v2) or not isinstance(obj["members"], list):
            raise ValueError("ogiltigt snapshotformat")
        snap = cls(tuple(DatasetVersionRef.from_json_obj(m) for m in obj["members"]),
                   tuple(FundingVersionRef.from_json_obj(f) for f in obj["funding"]) if v2 else ())
        if snap.snapshot_id != obj["snapshot_id"]:
            raise ValueError("snapshot_id matchar inte medlemmarna")
        if expected_id is not None and snap.snapshot_id != expected_id:
            raise ValueError(f"snapshot_id {snap.snapshot_id} != förväntat {expected_id}")
        if snap.to_file_bytes() != data:
            raise ValueError("snapshotfilen är inte kanoniskt kodad")
        return snap
