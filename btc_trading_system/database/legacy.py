"""Import av ett Phase 2-lager (legacy) till versionslagret (PHASE 4, steg 6).

KÄLLAN (t.ex. ~/btc_data/store_quality_v1) är SKRIVSKYDDAD: ingenting i den skrivs, skapas,
byter namn, chmod:as eller tas bort. Filer öppnas O_RDONLY|O_NOFOLLOW, symlänkar följs aldrig.
Källans trädhash (samma metod som ~/phase4_work/treehash.sh) beräknas före och efter; ändrades
den under importen misslyckas importen (failure="SOURCE_CHANGED", ok=False).
Importen skriver ENDAST i ett separat, redan initierat VersionedStore-rot (aldrig i källan;
källa och lager får inte överlappa).

FLÖDE (under skrivarlåset, även i dry-run)
  1. källroten kontrolleras; väntande ref-uppdatering/ingest -> fel innan något skrivs
  2. upptäckt: exakt binance/<instrument>/<SYMBOL>/<tf>.parquet + <tf>.manifest.json;
     allt annat -> `unrecognized`, importeras inte. Ordning: dataset_id. Inga dataset -> SOURCE_EMPTY.
  Per dataset (ett fel i A-C -> datasetet "failed", ingenting skrivs för det, importen fortsätter;
  fel i mållagret avbryter):
  A. manifest- och databytes läses EN gång; manifestet strikt (nycklar ⊆ bas + innehåll + källa,
     bas- och innehållsnycklar krävs, dataset == sökvägen)
  B. Phase 2:s egen läsväg DatasetStore.read på källan (content_hash, rows, check_ohlcv)
  C. gap_count, missing_bars_total, gaps, zero_volume_bars, rows, första/sista open_time
     räknas om med Phase 2:s egna funktioner och jämförs med manifestet
  D. objekt: finns data_version -> FULL verifiering, bas- och innehållsnycklar jämförs, källfält
     (sources, duplicates_dropped) jämförs ALDRIG (P4-S4-01) -> "reused"; annars publiceras de
     inlästa bytena BYTE FÖR BYTE (Phase 2-läsvägen körs först på exakt de bytena i tmp/),
     verifieras igen och registreras -> "published"
  E. importpost imports/<import_id>.json (0444, link, skrivs aldrig över) -> "recorded"/"existing"
  F. ref via steg 3 (action="import_legacy", logg före ref): ingen ref -> "set"; samma version ->
     "unchanged"; annan version -> rörs inte: "kept: ref points to another version"
  apply=False (standard): A-C plus en läsande förutsägelse av D-F; skriver ingenting.

IMPORTPOST
  Innehållsadresserad, ingen seq/kedja, ingen väggklocka, ingen kodversion, ingen sökväg:
  dataset, data_version, sha256 + storlek för båda källfilerna och källmanifestet som exakt text.
  import_id = sha256(b"btc-quant/legacy-import-id/v1\\x00" + kompakt kanonisk JSON av posten).
  Samma källbytes -> samma post; en omimport skriver ingenting.

KRASCH (per dataset): staging i tmp/ -> TMP_LEFTOVER; objekt utan katalograd ->
  OBJECT_UNREGISTERED (omimport/repair); post utan ref -> giltigt (som "kept"); ref_log-rad utan
  ref-fil -> väntande D1 (complete_pending_ref_updates() explicit); ref-fil utan katalog ->
  CATALOG_BEHIND. Varje ref_log-post med action=import_legacy har en importpost som skrevs före den.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from btc_trading_system.data.schema import (
    EXCHANGE,
    SCHEMA_VERSION,
    DatasetKey,
    to_epoch_ns,
)
from btc_trading_system.data.storage import DatasetStore, ParquetBackend
from btc_trading_system.data_validation.errors import DataError, DataIntegrityError
from btc_trading_system.data_validation.ohlcv import check_ohlcv, compute_gap_flags
from btc_trading_system.database import catalog
from btc_trading_system.database import versioned_store as vs
from btc_trading_system.database.lineage import CONTENT_DERIVED_KEYS, SOURCE_KEYS
from btc_trading_system.database.refs import DatasetVersionRef, compact_json_bytes

LEGACY_IMPORT_FORMAT = "btc-quant/legacy-import/v1"
IMPORT_REPORT_FORMAT = "btc-quant/legacy-import-report/v1"
IMPORT_ID_DOMAIN = b"btc-quant/legacy-import-id/v1\x00"
IMPORTS_DIR = "imports"
DATA_SUFFIX = ".parquet"
MANIFEST_SUFFIX = ".manifest.json"
# Manifestnycklar: bas + innehåll krävs och jämförs vid återbruk; källfält bevaras, jämförs aldrig.
COMPARED_MANIFEST_KEYS = vs.MANIFEST_BASE_KEYS | CONTENT_DERIVED_KEYS
ALLOWED_MANIFEST_KEYS = COMPARED_MANIFEST_KEYS | SOURCE_KEYS
_INT_MANIFEST_KEYS = ("schema_version", "rows", "gap_count", "missing_bars_total", "zero_volume_bars")
REF_SET = "set"
REF_UNCHANGED = "unchanged"
REF_KEPT = "kept: ref points to another version"
FAILURE_CODES = ("SOURCE_UNREADABLE", "SOURCE_MANIFEST_INVALID", "SOURCE_VERIFY_FAILED",
                 "SOURCE_MANIFEST_MISMATCH", "LEGACY_OBJECT_CONFLICT")
_RECORD_KEYS = frozenset({"format", "dataset", "dataset_id", "data_version", "source", "manifest_text"})
_SOURCE_PARTS = frozenset({"data_file", "manifest"})
_FILE_KEYS = frozenset({"name", "sha256", "bytes"})


class LegacyImportError(vs.StoreError):
    """Importen som helhet kan inte köras (ogiltig källrot, överlapp med lagret, källan är ett versionslager)."""


class ImportRecordError(DataError, ValueError):
    """Ogiltig importpost (konstruktion eller strikt tolkning)."""


class ImportCorruptError(DataIntegrityError):
    """En lagrad importpost är skadad."""


class _DatasetFailure(Exception):
    """Internt: ett källdataset kan inte importeras (rapporteras, avbryter inte importen)."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def source_names(key: DatasetKey) -> tuple[str, str]:
    """Phase 2-filnamnen (data, manifest) för ett dataset."""
    return f"{key.timeframe}{DATA_SUFFIX}", f"{key.timeframe}{MANIFEST_SUFFIX}"


# ------------------------------------------------------------------ importpost (ingen I/O)
@dataclass(frozen=True)
class ImportRecord:
    ref: DatasetVersionRef
    data_file_sha256: str
    data_file_bytes: int
    manifest_sha256: str
    manifest_bytes: int
    manifest_text: str

    def __post_init__(self) -> None:
        if not isinstance(self.ref, DatasetVersionRef):
            raise ImportRecordError("ref måste vara DatasetVersionRef")
        for name in ("data_file_sha256", "manifest_sha256"):
            if not catalog.is_sha256_hex(getattr(self, name)):
                raise ImportRecordError(f"ogiltig {name}: {getattr(self, name)!r}")
        for name in ("data_file_bytes", "manifest_bytes"):
            if not _is_int(getattr(self, name)) or getattr(self, name) <= 0:
                raise ImportRecordError(f"ogiltig {name}: {getattr(self, name)!r}")
        if not isinstance(self.manifest_text, str):
            raise ImportRecordError("manifest_text måste vara en sträng")
        try:
            raw = self.manifest_text.encode("utf-8")
        except UnicodeEncodeError as e:
            raise ImportRecordError("manifest_text är inte giltig UTF-8") from e
        if hashlib.sha256(raw).hexdigest() != self.manifest_sha256 or len(raw) != self.manifest_bytes:
            raise ImportRecordError("manifest_text matchar inte manifest_sha256/manifest_bytes")
        m = self.manifest
        key = self.ref.key
        expected = {"exchange": key.exchange, "instrument": key.instrument.value, "symbol": key.symbol,
                    "timeframe": key.timeframe, "data_version": self.ref.data_version}
        if any(m.get(k) != v for k, v in expected.items()):
            raise ImportRecordError("manifestet tillhör inte postens dataset/data_version")

    @property
    def manifest(self) -> dict[str, Any]:
        try:
            m = json.loads(self.manifest_text)
        except ValueError as e:
            raise ImportRecordError("manifest_text är inte JSON") from e
        if not isinstance(m, dict):
            raise ImportRecordError("manifest_text är inte ett JSON-objekt")
        return m

    def to_json_obj(self) -> dict[str, Any]:
        data_name, manifest_name = source_names(self.ref.key)
        return {"format": LEGACY_IMPORT_FORMAT, **self.ref.to_json_obj(),
                "source": {"data_file": {"name": data_name, "sha256": self.data_file_sha256,
                                         "bytes": self.data_file_bytes},
                           "manifest": {"name": manifest_name, "sha256": self.manifest_sha256,
                                        "bytes": self.manifest_bytes}},
                "manifest_text": self.manifest_text}

    @property
    def import_id(self) -> str:
        return hashlib.sha256(IMPORT_ID_DOMAIN + compact_json_bytes(self.to_json_obj())).hexdigest()

    def to_file_bytes(self) -> bytes:
        return vs.canonical_json_bytes(self.to_json_obj())

    @classmethod
    def from_file_bytes(cls, data: bytes, *, expected_id: str) -> ImportRecord:
        """Strikt: exakt nycklarna, kanonisk kodning, id == expected_id."""
        try:
            obj = json.loads(data)
        except ValueError as e:
            raise ImportRecordError("importposten är inte JSON") from e
        if not isinstance(obj, dict) or set(obj) != _RECORD_KEYS or obj["format"] != LEGACY_IMPORT_FORMAT:
            raise ImportRecordError("importposten har fel nycklar eller format")
        try:
            ref = DatasetVersionRef.from_json_obj({k: obj[k] for k in ("dataset", "dataset_id", "data_version")})
        except (ValueError, TypeError) as e:
            raise ImportRecordError(f"ogiltig datasetversion: {e}") from e
        src = obj["source"]
        if not isinstance(src, dict) or set(src) != _SOURCE_PARTS \
                or any(not isinstance(src[p], dict) or set(src[p]) != _FILE_KEYS for p in _SOURCE_PARTS):
            raise ImportRecordError("importpostens source har fel nycklar")
        if (src["data_file"]["name"], src["manifest"]["name"]) != source_names(ref.key):
            raise ImportRecordError("importpostens källfilnamn matchar inte datasetet")
        rec = cls(ref, src["data_file"]["sha256"], src["data_file"]["bytes"],
                  src["manifest"]["sha256"], src["manifest"]["bytes"], obj["manifest_text"])
        if rec.to_file_bytes() != data:
            raise ImportRecordError("importposten är inte kanoniskt kodad")
        if rec.import_id != expected_id:
            raise ImportRecordError(f"import_id {rec.import_id} != {expected_id}")
        return rec


def manifest_conflicts(legacy: dict[str, Any], stored: dict[str, Any]) -> list[str]:
    """Bas- och innehållsnycklar som skiljer; källfält jämförs aldrig (P4-S4-01)."""
    return sorted(k for k in COMPARED_MANIFEST_KEYS if legacy.get(k) != stored.get(k))


# ------------------------------------------------------------------ importposter: läsning (inget lås)
def _read_import_file(path: Path) -> tuple[ImportRecord, str]:
    name = path.name
    import_id = name[:-5] if name.endswith(".json") else ""
    if not catalog.is_sha256_hex(import_id):
        raise ImportCorruptError(f"okänd fil i {IMPORTS_DIR}/: {name}")
    st = os.lstat(path)
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode) or st.st_mode & 0o222:
        raise ImportCorruptError(f"{name}: inte en skrivskyddad vanlig fil")
    data = path.read_bytes()
    try:
        rec = ImportRecord.from_file_bytes(data, expected_id=import_id)
    except ImportRecordError as e:
        raise ImportCorruptError(f"{name}: {e}") from e
    return rec, hashlib.sha256(data).hexdigest()


def read_import_records(store: vs.VersionedStore) -> tuple[tuple[ImportRecord, str], ...]:
    """Alla importposter (post, filens sha256), strikt verifierade, sorterade på import_id."""
    root = store.root / IMPORTS_DIR
    if not os.path.lexists(root):
        return ()
    if not stat.S_ISDIR(os.lstat(root).st_mode):
        raise ImportCorruptError(f"{IMPORTS_DIR}/ är inte en katalog")
    return tuple(_read_import_file(p) for p in sorted(root.iterdir()))


# ------------------------------------------------------------------ källans trädhash
def source_tree_sha256(root: str | Path) -> str:
    """Samma metod som ~/phase4_work/treehash.sh: en rad per post (även roten "."), sorterad
    bytevis; fil: "<väg> <oktalt läge> <sha256>", annars "<väg> <oktalt läge> dir". Följer inga
    katalogsymlänkar; en fil-symlänk hashas som sitt mål (som `[ -f ]` + `sha256sum <`)."""
    base = Path(root)
    rels: list[bytes] = [b"."]

    def walk(d: Path, rel: bytes) -> None:
        with os.scandir(d) as it:
            for e in it:
                r = rel + b"/" + os.fsencode(e.name)
                rels.append(r)
                if e.is_dir(follow_symlinks=False):
                    walk(Path(e.path), r)

    walk(base, b".")
    h = hashlib.sha256()
    for r in sorted(rels):
        p = base / os.fsdecode(r)
        mode = f"{stat.S_IMODE(os.lstat(p).st_mode):o}".encode()
        tail = vs.sha256_file(p).encode() if os.path.isfile(p) else b"dir"
        h.update(r + b" " + mode + b" " + tail + b"\n")
    return h.hexdigest()


# ------------------------------------------------------------------ rapport
DatasetStatus = Literal["imported", "planned", "failed"]


@dataclass(frozen=True)
class DatasetImportResult:
    relpath: str
    status: DatasetStatus
    data_version: str | None = None
    publication: Literal["published", "reused"] | None = None
    record: Literal["recorded", "existing"] | None = None
    import_id: str | None = None
    record_sha256: str | None = None
    ref_outcome: str | None = None                    # REF_SET | REF_UNCHANGED | REF_KEPT
    current_data_version: str | None = None           # refens version när ref_outcome == REF_KEPT
    failure: str | None = None                        # en av FAILURE_CODES
    detail: str = ""

    def to_json_obj(self) -> dict[str, Any]:
        return {"relpath": self.relpath, "status": self.status, "data_version": self.data_version,
                "publication": self.publication, "record": self.record, "import_id": self.import_id,
                "record_sha256": self.record_sha256, "ref_outcome": self.ref_outcome,
                "current_data_version": self.current_data_version, "failure": self.failure,
                "detail": self.detail}


@dataclass(frozen=True)
class LegacyImportReport:
    applied: bool
    datasets: tuple[DatasetImportResult, ...]
    unrecognized: tuple[str, ...]
    source_tree_sha256_before: str
    source_tree_sha256_after: str
    failure: str | None                               # "SOURCE_CHANGED", "SOURCE_EMPTY" eller None

    @property
    def ok(self) -> bool:
        return (self.failure is None and not self.unrecognized
                and all(d.status != "failed" for d in self.datasets))

    def to_json_obj(self) -> dict[str, Any]:
        return {"format": IMPORT_REPORT_FORMAT, "applied": self.applied, "ok": self.ok,
                "failure": self.failure,
                "source_tree_sha256_before": self.source_tree_sha256_before,
                "source_tree_sha256_after": self.source_tree_sha256_after,
                "unrecognized": list(self.unrecognized),
                "datasets": [d.to_json_obj() for d in self.datasets]}

    def to_json_bytes(self) -> bytes:
        return vs.canonical_json_bytes(self.to_json_obj())


# ------------------------------------------------------------------ källa: rot och upptäckt
def _check_roots(source_root: Path, store_root: Path) -> None:
    try:
        st = os.lstat(source_root)
    except OSError as e:
        raise LegacyImportError(f"källroten finns inte: {e.strerror}") from e
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise LegacyImportError("källroten måste vara en katalog (inte en symlänk)")
    if os.path.lexists(source_root / vs.STORE_FILE):
        raise LegacyImportError("källan är ett versionslager, inte ett Phase 2-lager")
    src = Path(os.path.realpath(source_root))
    dst = Path(os.path.realpath(store_root))
    if src == dst or src in dst.parents or dst in src.parents:
        raise LegacyImportError("källa och lager får inte vara samma katalog eller ligga i varandra")


@dataclass(frozen=True)
class _SourceDataset:
    key: DatasetKey
    problem: str | None                               # filpar ofullständigt / inte vanliga filer


def _classify_file(name: str) -> tuple[str, str] | None:
    for tf in catalog.SCHEMA_TIMEFRAMES:
        if name == tf + DATA_SUFFIX:
            return "data", tf
        if name == tf + MANIFEST_SUFFIX:
            return "manifest", tf
    return None


def _scan(d: Path) -> list[os.DirEntry[str]]:
    with os.scandir(d) as it:
        return sorted(it, key=lambda e: e.name)


def _plain_dir(e: os.DirEntry[str]) -> bool:
    return e.is_dir(follow_symlinks=False)


def _discover(src: Path) -> tuple[list[_SourceDataset], list[str]]:
    """Exakt Phase 2-layouten; allt annat -> unrecognized (relativa sökvägar). Följer inga länkar."""
    found: list[_SourceDataset] = []
    unrecognized: list[str] = []
    for e0 in _scan(src):
        if e0.name != EXCHANGE or not _plain_dir(e0):
            unrecognized.append(e0.name)
            continue
        for e1 in _scan(Path(e0.path)):
            rel1 = f"{e0.name}/{e1.name}"
            if e1.name not in catalog.SCHEMA_INSTRUMENTS or not _plain_dir(e1):
                unrecognized.append(rel1)
                continue
            for e2 in _scan(Path(e1.path)):
                rel2 = f"{rel1}/{e2.name}"
                try:
                    catalog.dataset_key(exchange=EXCHANGE, instrument=e1.name, symbol=e2.name,
                                        timeframe=catalog.SCHEMA_TIMEFRAMES[0])
                except ValueError:
                    unrecognized.append(rel2)
                    continue
                if not _plain_dir(e2):
                    unrecognized.append(rel2)
                    continue
                files: dict[str, dict[str, bool]] = {}
                for e3 in _scan(Path(e2.path)):
                    kind = _classify_file(e3.name)
                    if kind is None:
                        unrecognized.append(f"{rel2}/{e3.name}")
                        continue
                    files.setdefault(kind[1], {})[kind[0]] = e3.is_file(follow_symlinks=False)
                for tf, kinds in sorted(files.items()):
                    key = catalog.dataset_key(exchange=EXCHANGE, instrument=e1.name, symbol=e2.name,
                                              timeframe=tf)
                    problem = None
                    if set(kinds) != {"data", "manifest"}:
                        problem = "data- eller manifestfilen saknas"
                    elif not all(kinds.values()):
                        problem = "data- eller manifestfilen är inte en vanlig fil (symlänk eller annan typ)"
                    found.append(_SourceDataset(key, problem))
    found.sort(key=lambda d: catalog.dataset_id(d.key))
    return found, sorted(unrecognized)


# ------------------------------------------------------------------ källa: läsning och kontroll
@dataclass(frozen=True)
class _Captured:
    key: DatasetKey
    manifest_bytes: bytes
    data_bytes: bytes
    manifest: dict[str, Any]

    @property
    def data_version(self) -> str:
        return str(self.manifest["data_version"])

    def record(self) -> ImportRecord:
        return ImportRecord(DatasetVersionRef(self.key, self.data_version),
                            hashlib.sha256(self.data_bytes).hexdigest(), len(self.data_bytes),
                            hashlib.sha256(self.manifest_bytes).hexdigest(), len(self.manifest_bytes),
                            self.manifest_bytes.decode("utf-8"))


def _read_source_file(path: Path, rel: str) -> bytes:
    """En läsning, O_RDONLY|O_NOFOLLOW; måste vara en vanlig fil."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as e:
        raise _DatasetFailure("SOURCE_UNREADABLE", f"{rel}: {e.strerror}") from e
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise _DatasetFailure("SOURCE_UNREADABLE", f"{rel}: inte en vanlig fil")
        chunks = []
        while chunk := os.read(fd, 1 << 20):
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def _check_manifest(manifest: Any, key: DatasetKey) -> None:
    def bad(msg: str) -> _DatasetFailure:
        return _DatasetFailure("SOURCE_MANIFEST_INVALID", msg)
    if not isinstance(manifest, dict):
        raise bad("manifestet är inte ett JSON-objekt")
    unknown = sorted(set(manifest) - ALLOWED_MANIFEST_KEYS)
    missing = sorted(COMPARED_MANIFEST_KEYS - set(manifest))
    if unknown or missing:
        raise bad(f"manifestnycklar: okända {unknown}, saknade {missing}")
    if any(not _is_int(manifest[k]) or manifest[k] < 0 for k in _INT_MANIFEST_KEYS):
        raise bad(f"heltalsfälten {list(_INT_MANIFEST_KEYS)} måste vara heltal >= 0")
    if manifest["schema_version"] != SCHEMA_VERSION:
        raise bad(f"schema_version {manifest['schema_version']} != {SCHEMA_VERSION}")
    expected = {"exchange": key.exchange, "instrument": key.instrument.value, "symbol": key.symbol,
                "timeframe": key.timeframe}
    if any(manifest[k] != v for k, v in expected.items()):
        raise bad("manifestets dataset matchar inte sökvägen")
    if not catalog.is_sha256_hex(manifest["data_version"]):
        raise bad("ogiltig data_version")
    try:
        vs._parse_sources(manifest.get("sources"))
    except vs.InvalidSourcesError as e:
        raise bad(str(e)) from e
    if "duplicates_dropped" in manifest and not (
            _is_int(manifest["duplicates_dropped"]) and manifest["duplicates_dropped"] >= 0):
        raise bad("duplicates_dropped måste vara ett heltal >= 0")


def _capture(src: Path, ds: _SourceDataset) -> _Captured:
    rel = ds.key.relpath
    if ds.problem is not None:
        raise _DatasetFailure("SOURCE_UNREADABLE", f"{rel}: {ds.problem}")
    mbytes = _read_source_file(src / (rel + MANIFEST_SUFFIX), rel + MANIFEST_SUFFIX)
    dbytes = _read_source_file(src / (rel + DATA_SUFFIX), rel + DATA_SUFFIX)
    try:
        manifest = json.loads(mbytes.decode("utf-8"))
    except ValueError as e:
        raise _DatasetFailure("SOURCE_MANIFEST_INVALID", f"manifestet är inte UTF-8-JSON: {e}") from e
    _check_manifest(manifest, ds.key)
    return _Captured(ds.key, mbytes, dbytes, manifest)


def _phase2_read(root: Path, cap: _Captured) -> pd.DataFrame:
    """Phase 2:s egen läsväg (oförändrad): content_hash == data_version, rows, check_ohlcv."""
    try:
        frame, manifest = DatasetStore(root, ParquetBackend()).read(cap.key)
    except Exception as e:                               # Phase 2- och pyarrow-fel av alla slag
        raise _DatasetFailure("SOURCE_VERIFY_FAILED", str(e)) from e
    if manifest != cap.manifest:
        raise _DatasetFailure("SOURCE_MANIFEST_MISMATCH", "manifestet ändrades mellan läsningarna")
    return frame


def _check_content(frame: pd.DataFrame, cap: _Captured) -> None:
    """Innehållshärledda manifestfält räknas om med Phase 2:s egna funktioner."""
    if len(frame) == 0:
        raise _DatasetFailure("SOURCE_MANIFEST_MISMATCH", "datasetet är tomt")
    tf = cap.key.timeframe
    rep = check_ohlcv(frame, tf)
    _, gaps = compute_gap_flags(to_epoch_ns(frame["open_time"]), tf)
    derived = {
        "rows": len(frame),
        "first_open_time": frame["open_time"].iloc[0].isoformat(),
        "last_open_time": frame["open_time"].iloc[-1].isoformat(),
        "gap_count": len(rep.gaps),
        "missing_bars_total": int(rep.gaps["missing_bars"].sum()),
        "gaps": [{"gap_start": g.gap_start.isoformat(), "gap_end": g.gap_end.isoformat(),
                  "missing_bars": int(g.missing_bars)} for g in gaps.itertuples()],
        "zero_volume_bars": int((frame["volume"] == 0).sum()),
    }
    wrong = sorted(k for k, v in derived.items() if json.loads(json.dumps(v)) != cap.manifest[k])
    if wrong:
        raise _DatasetFailure("SOURCE_MANIFEST_MISMATCH", f"manifestfält stämmer inte med innehållet: {wrong}")


# ------------------------------------------------------------------ mållagret
def _version_bytes(cap: _Captured) -> bytes:
    """version.json för de inlästa bytena (samma format som StoreWriter._stage)."""
    key, m = cap.key, cap.manifest
    return vs.canonical_json_bytes({
        "format": vs.VERSION_FORMAT,
        "dataset_id": catalog.dataset_id(key),
        "dataset": json.loads(catalog.dataset_key_json(key)),
        "data_version": cap.data_version,
        "data_file": {"path": vs.DATA_FILE, "sha256": hashlib.sha256(cap.data_bytes).hexdigest(),
                      "bytes": len(cap.data_bytes)},
        "manifest": {"path": vs.MANIFEST_FILE, "sha256": hashlib.sha256(cap.manifest_bytes).hexdigest(),
                     "bytes": len(cap.manifest_bytes)},
        "rows": m["rows"], "first_open_time": m["first_open_time"], "last_open_time": m["last_open_time"],
    })


def _write_new(path: Path, data: bytes) -> None:
    with path.open("xb") as f:
        f.write(data)


def _publish_object(w: vs.StoreWriter, store: vs.VersionedStore, cap: _Captured, target: Path) -> None:
    """De inlästa bytena byte för byte: Phase 2-läsvägen på exakt dessa bytes i tmp/, sedan
    steg 2-publicering (0444, fsync, skyddad rename, 0555)."""
    staging = w._new_staging()
    try:
        p2 = staging / "phase2"
        data_path = p2 / (cap.key.relpath + DATA_SUFFIX)
        manifest_path = p2 / (cap.key.relpath + MANIFEST_SUFFIX)
        data_path.parent.mkdir(parents=True)
        _write_new(data_path, cap.data_bytes)
        _write_new(manifest_path, cap.manifest_bytes)
        _phase2_read(p2, cap)
        obj = staging / "object"
        obj.mkdir()
        os.replace(data_path, obj / vs.DATA_FILE)
        os.replace(manifest_path, obj / vs.MANIFEST_FILE)
        _write_new(obj / vs.VERSION_FILE, _version_bytes(cap))
        for name in sorted(vs.OBJECT_FILES):
            os.chmod(obj / name, vs.FILE_MODE)
            vs._fsync_file(obj / name)
        vs._fsync_dir(obj)
        w._publish_dir(obj, target)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _publish_record(store: vs.VersionedStore, rec: ImportRecord) -> None:
    """tmp (0444) -> fsync -> link (aldrig överskrivning) -> fsync av katalogen."""
    path = store.root / IMPORTS_DIR / f"{rec.import_id}.json"
    vs._mkdirs_durable(path.parent, store.root)
    staged = store.tmp / f"{uuid.uuid4().hex}.import.json"
    try:
        _write_new(staged, rec.to_file_bytes())
        os.chmod(staged, vs.FILE_MODE)
        vs._fsync_file(staged)
        try:
            vs._link(staged, path)                          # importpost: aldrig överskrivning
        except FileExistsError as e:
            raise vs.ImmutableObjectError(f"{path.name}: importposten finns redan; skrivs aldrig över") from e
    finally:
        staged.unlink(missing_ok=True)
    vs._fsync_dir(path.parent)


def _import_one(w: vs.StoreWriter, store: vs.VersionedStore, src: Path, ds: _SourceDataset,
                apply: bool) -> DatasetImportResult:
    cap = _capture(src, ds)                                              # A
    _check_content(_phase2_read(src, cap), cap)                          # B, C
    key, dv = cap.key, cap.data_version
    target = store.object_dir(key, dv)
    exists = os.path.lexists(target)
    if exists:                                                           # D: aldrig blint återbruk
        info, stored, _ = store._verify_object(key, dv)
        conflicts = manifest_conflicts(cap.manifest, stored)
        if conflicts:
            raise _DatasetFailure("LEGACY_OBJECT_CONFLICT",
                                  f"befintligt objekt {dv} har andra manifestvärden: {conflicts}")
    rec = cap.record()
    rec_path = store.root / IMPORTS_DIR / f"{rec.import_id}.json"
    has_record = os.path.lexists(rec_path)
    if has_record:
        _read_import_file(rec_path)                                      # strikt; id == innehållets hash
    cur = store._load_ref_state().refs.get(catalog.dataset_id(key))
    keep = cur is not None and cur.ref.data_version != dv
    if apply:
        if exists:
            w._finish_dir(target)
        else:
            _publish_object(w, store, cap, target)
            info, stored, _ = store._verify_object(key, dv)          # durabelt objekt verifieras igen
        w._register(info, stored)
        if not has_record:                                               # E: post före ref
            _publish_record(store, rec)
        if keep:                                                         # F
            ref_outcome = REF_KEPT
        else:
            ref_outcome = REF_SET if w._move_ref(key, dv, "import_legacy").changed else REF_UNCHANGED
    else:
        ref_outcome = REF_KEPT if keep else REF_UNCHANGED if cur is not None else REF_SET
    return DatasetImportResult(
        relpath=key.relpath, status="imported" if apply else "planned", data_version=dv,
        publication="reused" if exists else "published", record="existing" if has_record else "recorded",
        import_id=rec.import_id, record_sha256=hashlib.sha256(rec.to_file_bytes()).hexdigest(),
        ref_outcome=ref_outcome, current_data_version=cur.ref.data_version if keep and cur else None)


def import_legacy(store: vs.VersionedStore, source_root: str | Path, *, apply: bool = False
                  ) -> LegacyImportReport:
    """Importerar ett Phase 2-lager. Dry-run som standard (apply=False skriver ingenting).
    Se modulens docstring."""
    src = Path(source_root)
    _check_roots(src, store.root)
    roots = sorted({str(src), os.path.realpath(src), str(store.root), os.path.realpath(store.root)},
                   key=len, reverse=True)

    def clean(text: str) -> str:
        for r in roots:
            text = text.replace(r, "<källa>" if r in (str(src), os.path.realpath(src)) else "<lager>")
        return text

    with store.writer() as w:
        state = store._load_ref_state()                  # väntande ingest -> PendingIngestError
        if state.pending is not None:
            raise vs.PendingRefUpdateError(
                f"väntande ref-uppdatering (ref_log seq {state.pending.seq}); "
                "anropa complete_pending_ref_updates() först")
        before = source_tree_sha256(src)
        found, unrecognized = _discover(src)
        results = []
        for ds in found:
            try:
                results.append(_import_one(w, store, src, ds, apply))
            except _DatasetFailure as f:
                results.append(DatasetImportResult(relpath=ds.key.relpath, status="failed",
                                                   failure=f.code, detail=clean(f.detail)))
        after = source_tree_sha256(src)
    failure: str | None = None
    if after != before:                                  # källan ändrades: väger tyngst
        failure = "SOURCE_CHANGED"
    elif not found:                                      # inga igenkända dataset: aldrig ok
        failure = "SOURCE_EMPTY"
    return LegacyImportReport(applied=apply, datasets=tuple(results), unrecognized=tuple(unrecognized),
                              source_tree_sha256_before=before, source_tree_sha256_after=after,
                              failure=failure)
