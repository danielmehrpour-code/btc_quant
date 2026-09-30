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
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.database import catalog

SNAPSHOT_FORMAT = "btc-quant/snapshot/v1"
SNAPSHOT_ID_DOMAIN = b"btc-quant/snapshot-id/v1\x00"
_REF_KEYS = frozenset({"dataset", "dataset_id", "data_version"})
_SNAPSHOT_KEYS = frozenset({"format", "snapshot_id", "members"})


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
class DataSnapshot:
    members: tuple[DatasetVersionRef, ...]

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

    @classmethod
    def of(cls, members: Iterable[DatasetVersionRef]) -> DataSnapshot:
        """Kanonisk konstruktion: sorterar på dataset_id."""
        return cls(tuple(sorted(members, key=lambda m: m.dataset_id)))

    def canonical_members_bytes(self) -> bytes:
        return compact_json_bytes([m.to_json_obj() for m in self.members])

    @property
    def snapshot_id(self) -> str:
        return hashlib.sha256(SNAPSHOT_ID_DOMAIN + self.canonical_members_bytes()).hexdigest()

    def version_of(self, key: DatasetKey) -> DatasetVersionRef | None:
        did = catalog.dataset_id(key)
        return next((m for m in self.members if m.dataset_id == did), None)

    def to_file_bytes(self) -> bytes:
        return _file_json_bytes({"format": SNAPSHOT_FORMAT, "snapshot_id": self.snapshot_id,
                                 "members": [m.to_json_obj() for m in self.members]})

    @classmethod
    def from_file_bytes(cls, data: bytes, expected_id: str | None = None) -> DataSnapshot:
        """Strikt tolkning: format, nycklar, sortering, id och exakt kanonisk kodning."""
        try:
            obj = json.loads(data)
        except (ValueError, UnicodeDecodeError) as e:
            raise ValueError("snapshotfilen är inte JSON") from e
        if not isinstance(obj, dict) or set(obj) != _SNAPSHOT_KEYS or obj["format"] != SNAPSHOT_FORMAT \
                or not isinstance(obj["members"], list):
            raise ValueError("ogiltigt snapshotformat")
        snap = cls(tuple(DatasetVersionRef.from_json_obj(m) for m in obj["members"]))
        if snap.snapshot_id != obj["snapshot_id"]:
            raise ValueError("snapshot_id matchar inte medlemmarna")
        if expected_id is not None and snap.snapshot_id != expected_id:
            raise ValueError(f"snapshot_id {snap.snapshot_id} != förväntat {expected_id}")
        if snap.to_file_bytes() != data:
            raise ValueError("snapshotfilen är inte kanoniskt kodad")
        return snap
