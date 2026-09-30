"""Oföränderliga, innehållsadresserade datasetversioner (PHASE 4, steg 2).

Layout (steg 2 skapar bara det som behövs här):
  <root>/STORE.json
  <root>/.lock                  skrivarlås (flock); läsare tar det aldrig
  <root>/objects/binance/<instrument>/<SYMBOL>/<timeframe>/<data_version>/
      bars.parquet              Phase 2 ParquetBackend, oförändrat format
      manifest.json             Phase 2 DatasetStore-manifest, oförändrat format
      version.json              objektmetadata: dataset_id + hashar
  <root>/tmp/                   unika stagingkataloger
  <root>/catalog.sqlite         härledd (catalog.py)

IDENTITETER
  * data_version     = Phase 2 content_hash (logiskt innehåll), OFÖRÄNDRAD definition.
                       Objektkatalogens namn.
  * data_file_sha256 = sha256 av bars.parquet (fysisk representation).
  * manifest_sha256  = sha256 av manifest.json-bytes.
  * version_sha256   = sha256 av version.json-bytes.

OFÖRÄNDERLIGHET
  * Ett publicerat objekt skrivs aldrig över. Finns data_version redan verifieras
    objektet FULLT (alla hashar + innehåll) och återanvänds; annars FAIL.
  * Filer 0444, objektkatalog 0555. API:t saknar muterande operationer på objekt.

PUBLICERING (crash-säker, under skrivarlåset)
  1. data + manifest skrivs av Phase 2 DatasetStore i en unik tmp/<uuid>/
  2. version.json skrivs; alla filer chmod 0444 + fsync; stagingkatalogen fsync
  3. föräldrakataloger skapas durabelt; atomisk rename till objects/.../<data_version>
  4. objektkatalog chmod 0555 + fsync; föräldern fsync
  5. objektet verifieras på nytt från disk
  6. först därefter: katalograd i EN transaktion
  Krasch före 3: bara skräp i tmp/. Krasch mellan 3 och 6: durabelt objekt utan
  katalograd; nästa publicering av samma data_version verifierar och registrerar.

ÅTERBRUK OCH PROVENIENS
  Samma data_version + samma `extra` (alla manifestfält utöver Phase 2-basfälten,
  inkl. sources) -> "reused". Samma data_version men annan `extra` ->
  ProvenanceConflictError; det lagrade manifestet skrivs aldrig om och ny
  proveniens tappas aldrig tyst.

  KRAV FÖR STEG 4 (ingest; EJ implementerat i steg 2, där gäller strikt jämförelse ovan):
    * På ingest-vägen får `extra` bara innehålla
      CONTENT_DERIVED_KEYS = {"gaps", "zero_volume_bars"}; annan nyckel ->
      ValueError innan något skrivs.
    * Vid återbruk jämförs exakt dessa nycklar mot det lagrade manifestet;
      avvikelse -> ObjectCorruptError (samma innehåll kan inte ge andra härledda värden).
    * Källberoende fält i legacy-manifest (steg 6: sources, duplicates_dropped)
      bevaras som lagrade men jämförs inte. Ny källproveniens registreras i
      ingest_sources och ingest_events.duplicates_dropped, aldrig i objektmanifestet.

INIT
  VersionedStore.init() initierar en katalog som saknas eller är tom, eller
  återupptar en avbruten init. Återupptagning kräver ALLT av:
    * poster ⊆ {.lock, objects, tmp, catalog.sqlite}, inga symlänkar
    * objects/ saknas eller är tom
    * tmp/ saknas eller innehåller bara vanliga filer ".store-*.json",
      ".catalog-*.sqlite.tmp" och dess "-journal"/"-wal"/"-shm"
    * catalog.sqlite saknas, eller öppnas av catalog.open_catalog och har
      0 dataset och 0 versioner
  Annars (t.ex. ett Phase 2-lager eller en orelaterad katalog) StoreFormatError
  och ingenting skapas. Kontrollen görs först utan lås (så att inte ens .lock
  skapas i en främmande katalog) och sedan igen under skrivarlåset.

KAUSALITET
  read_version(as_of=t) exponerar ENDAST barer med available_at <= t, där
  available_at = open_time + timeframe (samma kontrakt som LocalOHLCVProvider).
  gap_before läses som lagrad (beror bara på t-1 och t).

KÄNDA BEGRÄNSNINGAR
  * Linux rename() ersätter tyst en TOM målkatalog och stdlib saknar
    RENAME_NOREPLACE. Skydd: kontroll under skrivarlåset direkt före rename;
    ett icke-tomt mål ger ImmutableObjectError. En tom katalog som någon annan
    skapar exakt mellan kontroll och rename skulle ersättas (den innehåller ingen data).
  * Övergivna stagingkataloger i tmp/ efter en krasch städas inte här
    (ingen GC i steg 2); hanteras av ett senare steg (repair). Samma gäller
    kvarlämnade tmp/.store-*.json och tmp/.catalog-*.sqlite.tmp* från en avbruten init.
  * All init-staging (STORE.json och katalogbygget) sker i tmp/, så lagrets rot
    innehåller bara slutliga artefakter. init() raderar inga kvarlämnade
    stagingfiler; städning av tmp/ är ett senare GC-steg.
"""
from __future__ import annotations

import errno
import fcntl
import fnmatch
import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from btc_trading_system.core.types import TIMEFRAMES, require_utc
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data.storage import (
    DatasetStore,
    ParquetBackend,
    canonicalize,
    content_hash,
)
from btc_trading_system.data_validation.errors import (
    DataError,
    DataIntegrityError,
    DatasetNotFoundError,
    DataValidationError,
)
from btc_trading_system.data_validation.ohlcv import check_ohlcv
from btc_trading_system.database import catalog

STORE_FORMAT = "btc-quant/versioned-store"
STORE_LAYOUT_VERSION = 1
VERSION_FORMAT = "btc-quant/dataset-version/v1"
STORE_FILE = "STORE.json"
LOCK_FILE = ".lock"
CATALOG_FILE = "catalog.sqlite"
DATA_FILE = "bars.parquet"
MANIFEST_FILE = "manifest.json"
VERSION_FILE = "version.json"
OBJECT_FILES = frozenset({DATA_FILE, MANIFEST_FILE, VERSION_FILE})
FILE_MODE = 0o444
DIR_MODE = 0o555

# Fält som DatasetStore.write själv sätter; `extra` får aldrig skugga dem.
MANIFEST_BASE_KEYS = frozenset({
    "schema_version", "exchange", "instrument", "symbol", "timeframe", "rows",
    "first_open_time", "last_open_time", "data_version", "gap_count", "missing_bars_total"})
_DV_COLUMNS = ("dataset_id", "data_version", "data_file_sha256", "data_file_bytes",
               "manifest_sha256", "version_sha256", "manifest_schema_version", "rows",
               "first_open_time_ms", "last_open_time_ms", "gap_count", "missing_bars_total")
_VS_COLUMNS = ("dataset_id", "data_version", "source_index", "name", "sha256", "rows")
_VERSION_KEYS = frozenset({"format", "dataset_id", "dataset", "data_version", "data_file",
                           "manifest", "rows", "first_open_time", "last_open_time"})


class StoreError(DataError):
    """Versionslagret kan inte användas."""


class StoreFormatError(StoreError):
    """STORE.json saknas, är ogiltig eller har en layout som inte stöds."""


class StoreLockedError(StoreError):
    """En annan skrivare håller skrivarlåset."""


class ImmutableObjectError(StoreError):
    """Försök att skriva över ett publicerat, oföränderligt objekt."""


class ProvenanceConflictError(StoreError):
    """data_version finns redan men anroparens proveniens (`extra`) skiljer sig."""


class InvalidSourcesError(StoreError, ValueError):
    """Ogiltig `sources` (inte en lista, eller ogiltigt element)."""


class ObjectCorruptError(DataIntegrityError):
    """Ett publicerat objekt matchar inte sina hashar/sitt innehåll."""


@dataclass(frozen=True)
class VersionInfo:
    key: DatasetKey
    dataset_id: str
    data_version: str
    data_file_sha256: str
    data_file_bytes: int
    manifest_sha256: str
    manifest_bytes: int
    version_sha256: str
    rows: int
    first_open_time: str
    last_open_time: str


@dataclass(frozen=True)
class PublishResult:
    info: VersionInfo
    outcome: Literal["published", "reused"]

    @property
    def published(self) -> bool:
        return self.outcome == "published"


# ------------------------------------------------------------------ hjälp (spionerbara i test)
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_json_bytes(obj: Any) -> bytes:
    """Samma kodning som Phase 2-manifestet: sorterade nycklar, indent 2, radslut."""
    return (json.dumps(obj, sort_keys=True, indent=2) + "\n").encode()


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _rename(src: Path, dst: Path) -> None:
    os.rename(src, dst)


def _mkdirs_durable(path: Path, stop: Path) -> None:
    """Skapar saknade kataloger under `stop` och fsync:ar varje ny katalogs förälder."""
    missing = []
    p = path
    while not p.exists():
        if p == stop or stop not in p.parents:
            raise StoreError(f"{path} ligger utanför {stop}")
        missing.append(p)
        p = p.parent
    for d in reversed(missing):
        d.mkdir()
        _fsync_dir(d.parent)


# ------------------------------------------------------------------ kausalitet
def causal_cutoff(as_of: datetime, timeframe: str) -> pd.Timestamp:
    """Senaste open_time som får exponeras: available_at = open_time + tf <= as_of."""
    return pd.Timestamp(require_utc(as_of, "as_of")) - TIMEFRAMES[timeframe]


def apply_as_of(frame: pd.DataFrame, timeframe: str, as_of: datetime) -> pd.DataFrame:
    mask = frame["open_time"] <= causal_cutoff(as_of, timeframe)
    return frame.loc[mask].reset_index(drop=True)


# ------------------------------------------------------------------ lås
class _WriterLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    @property
    def held(self) -> bool:
        return self._fd is not None

    def acquire(self) -> None:
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            os.close(fd)
            if e.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                raise StoreLockedError(f"{self.path}: en annan skrivare håller låset") from e
            raise
        self._fd = fd

    def release(self) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


# ------------------------------------------------------------------ lager
class VersionedStore:
    """Läsare: VersionedStore(root). Skrivare: `with store.writer() as w: ...`."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self._read_store_file()
        self.objects = self.root / "objects"
        self.tmp = self.root / "tmp"
        self.catalog_path = self.root / CATALOG_FILE
        self._backend = ParquetBackend()

    @classmethod
    def init(cls, root: str | Path) -> VersionedStore:
        """Skapar ett nytt lager (idempotent). STORE.json skrivs sist = lagret är komplett."""
        root = Path(root)
        if (root / STORE_FILE).exists():
            return cls(root)
        if os.path.lexists(root):
            _check_resumable_init(root)                  # inget skapas i främmande katalog
        root.mkdir(parents=True, exist_ok=True)
        lock = _WriterLock(root / LOCK_FILE)
        lock.acquire()
        try:
            if (root / STORE_FILE).exists():             # annan init blev klar under tiden
                return cls(root)
            _check_resumable_init(root)                  # om igen, under låset
            for d in ("objects", "tmp"):
                (root / d).mkdir(exist_ok=True)
            conn = catalog.open_catalog(root / CATALOG_FILE, create=True, staging_dir=root / "tmp")
            conn.close()
            staged = root / "tmp" / f".store-{uuid.uuid4().hex}.json"
            staged.write_bytes(canonical_json_bytes(
                {"format": STORE_FORMAT, "layout_version": STORE_LAYOUT_VERSION}))
            _fsync_file(staged)
            try:
                os.link(staged, root / STORE_FILE)          # aldrig överskrivning
            finally:
                staged.unlink()
            _fsync_dir(root)
        finally:
            lock.release()
        return cls(root)

    def _read_store_file(self) -> None:
        path = self.root / STORE_FILE
        try:
            meta = json.loads(path.read_bytes())
        except FileNotFoundError as e:
            raise StoreFormatError(f"{self.root}: inget {STORE_FILE} (VersionedStore.init först)") from e
        except (ValueError, UnicodeDecodeError) as e:
            raise StoreFormatError(f"{path}: ogiltig JSON") from e
        if not isinstance(meta, dict) or set(meta) != {"format", "layout_version"} \
                or meta["format"] != STORE_FORMAT \
                or not isinstance(meta["layout_version"], int) \
                or isinstance(meta["layout_version"], bool):
            raise StoreFormatError(f"{path}: okänt lagerformat")
        if meta["layout_version"] > STORE_LAYOUT_VERSION:
            raise StoreFormatError(f"{path}: layout {meta['layout_version']} är nyare än "
                                   f"stödd {STORE_LAYOUT_VERSION}")
        if meta["layout_version"] < 1:
            raise StoreFormatError(f"{path}: ogiltig layout_version")

    # -------------------------------------------------------------- läsning (inget lås)
    def object_dir(self, key: DatasetKey, data_version: str) -> Path:
        if not catalog.is_sha256_hex(data_version):
            raise ValueError(f"ogiltig data_version: {data_version!r}")
        catalog.dataset_key_json(key)                        # validerar exchange
        return self.objects / key.relpath / data_version

    def has_version(self, key: DatasetKey, data_version: str) -> bool:
        return os.path.lexists(self.object_dir(key, data_version))

    def verify_version(self, key: DatasetKey, data_version: str) -> VersionInfo:
        """Full verifiering: filuppsättning, rättigheter, alla hashar, innehåll, validering."""
        return self._verify_object(key, data_version)[0]

    def read_version(self, key: DatasetKey, data_version: str, *,
                     as_of: datetime | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Exakt denna version (aldrig "senaste"). Verifieras vid varje läsning.
        as_of=t -> endast barer med available_at <= t."""
        _, manifest, frame = self._verify_object(key, data_version)
        if as_of is not None:
            frame = apply_as_of(frame, key.timeframe, as_of)
        return frame, manifest

    def _verify_object(self, key: DatasetKey, dv: str
                       ) -> tuple[VersionInfo, dict[str, Any], pd.DataFrame]:
        d = self.object_dir(key, dv)
        where = f"{key.relpath}/{dv}"
        if not os.path.lexists(d):
            raise DatasetNotFoundError(f"version saknas: {where}")
        if d.is_symlink() or not d.is_dir():
            raise ObjectCorruptError(f"{where}: inte en katalog")
        names = {p.name for p in d.iterdir()}
        if names != OBJECT_FILES:
            raise ObjectCorruptError(f"{where}: filer {sorted(names)} != {sorted(OBJECT_FILES)}")
        for name in sorted(OBJECT_FILES):
            path = d / name
            if path.is_symlink() or not path.is_file():
                raise ObjectCorruptError(f"{where}/{name}: inte en vanlig fil")
            if os.lstat(path).st_mode & 0o222:
                raise ObjectCorruptError(
                    f"{where}/{name}: skrivbar (oföränderliga filer ska vara 0444)")

        vbytes = (d / VERSION_FILE).read_bytes()
        version = self._parse_version(vbytes, key, dv, where)

        mbytes = (d / MANIFEST_FILE).read_bytes()
        if hashlib.sha256(mbytes).hexdigest() != version["manifest"]["sha256"] \
                or len(mbytes) != version["manifest"]["bytes"]:
            raise ObjectCorruptError(f"{where}: manifest_sha256 matchar inte version.json")
        try:
            manifest = json.loads(mbytes)
        except ValueError as e:
            raise ObjectCorruptError(f"{where}: manifest är inte JSON") from e
        expected = {"exchange": key.exchange, "instrument": key.instrument.value,
                    "symbol": key.symbol, "timeframe": key.timeframe, "data_version": dv,
                    "rows": version["rows"], "first_open_time": version["first_open_time"],
                    "last_open_time": version["last_open_time"]}
        if not isinstance(manifest, dict) or any(manifest.get(k) != v for k, v in expected.items()):
            raise ObjectCorruptError(f"{where}: manifestet matchar inte objektet")

        data_path = d / DATA_FILE
        if sha256_file(data_path) != version["data_file"]["sha256"] \
                or data_path.stat().st_size != version["data_file"]["bytes"]:
            raise ObjectCorruptError(f"{where}: data_file_sha256 matchar inte version.json")
        try:
            frame = canonicalize(self._backend.read(data_path))
        except Exception as e:                           # trasig Parquet trots rätt hash
            raise ObjectCorruptError(f"{where}: bars.parquet kan inte läsas: {e}") from e
        if content_hash(frame) != dv:
            raise ObjectCorruptError(f"{where}: innehållet matchar inte data_version")
        if len(frame) != version["rows"] \
                or frame["open_time"].iloc[0].isoformat() != version["first_open_time"] \
                or frame["open_time"].iloc[-1].isoformat() != version["last_open_time"]:
            raise ObjectCorruptError(f"{where}: rader/täckning matchar inte version.json")
        try:
            check_ohlcv(frame, key.timeframe).raise_if_errors(context=f"verify {where}")
        except DataValidationError as e:
            raise ObjectCorruptError(f"{where}: validering misslyckades: {e}") from e

        info = VersionInfo(
            key=key, dataset_id=version["dataset_id"], data_version=dv,
            data_file_sha256=version["data_file"]["sha256"],
            data_file_bytes=version["data_file"]["bytes"],
            manifest_sha256=version["manifest"]["sha256"],
            manifest_bytes=version["manifest"]["bytes"],
            version_sha256=hashlib.sha256(vbytes).hexdigest(), rows=version["rows"],
            first_open_time=version["first_open_time"], last_open_time=version["last_open_time"])
        return info, manifest, frame

    @staticmethod
    def _parse_version(vbytes: bytes, key: DatasetKey, dv: str, where: str) -> dict[str, Any]:
        try:
            v = json.loads(vbytes)
        except ValueError as e:
            raise ObjectCorruptError(f"{where}: version.json är inte JSON") from e
        ok = (isinstance(v, dict) and set(v) == _VERSION_KEYS
              and canonical_json_bytes(v) == vbytes
              and v["format"] == VERSION_FORMAT
              and v["dataset_id"] == catalog.dataset_id(key)
              and v["dataset"] == json.loads(catalog.dataset_key_json(key))
              and v["data_version"] == dv
              and isinstance(v["rows"], int) and v["rows"] >= 1
              and all(isinstance(v[k], dict) and set(v[k]) == {"path", "sha256", "bytes"}
                      and catalog.is_sha256_hex(v[k]["sha256"])
                      and isinstance(v[k]["bytes"], int) and v[k]["bytes"] > 0
                      for k in ("data_file", "manifest"))
              and v["data_file"]["path"] == DATA_FILE and v["manifest"]["path"] == MANIFEST_FILE)
        if not ok:
            raise ObjectCorruptError(f"{where}: version.json är ogiltig eller tillhör annat objekt")
        return v

    # -------------------------------------------------------------- skrivning
    @contextmanager
    def writer(self) -> Iterator[StoreWriter]:
        """Exklusivt skrivarlås. En andra skrivare får StoreLockedError direkt."""
        lock = _WriterLock(self.root / LOCK_FILE)
        lock.acquire()
        try:
            conn = catalog.open_catalog(self.catalog_path)
            try:
                yield StoreWriter(self, conn, lock)
            finally:
                conn.close()
        finally:
            lock.release()


class StoreWriter:
    """Skapas bara av VersionedStore.writer(); ogiltig när låset släppts."""

    def __init__(self, store: VersionedStore, conn: sqlite3.Connection, lock: _WriterLock) -> None:
        self._store = store
        self._conn = conn
        self._lock = lock

    def _require_lock(self) -> None:
        if not self._lock.held:
            raise StoreLockedError("skrivaren är stängd (skrivarlåset är släppt)")

    def publish_version(self, key: DatasetKey, df: pd.DataFrame,
                        extra: dict[str, Any] | None = None) -> PublishResult:
        """Publicerar innehållet som en oföränderlig version, eller verifierar och
        återanvänder ett befintligt objekt med samma data_version."""
        self._require_lock()
        extra = dict(extra or {})
        clash = sorted(set(extra) & MANIFEST_BASE_KEYS)
        if clash:
            raise ValueError(f"extra får inte skugga manifestfält: {clash}")
        _parse_sources(extra.get("sources"))                 # validera innan något skrivs
        canon = canonicalize(df)
        dv = content_hash(canon)
        store = self._store
        target = store.object_dir(key, dv)

        if os.path.lexists(target):
            info, manifest, _ = store._verify_object(key, dv)     # aldrig blint återbruk
            stored = {k: v for k, v in manifest.items() if k not in MANIFEST_BASE_KEYS}
            if json.loads(json.dumps(extra)) != stored:
                raise ProvenanceConflictError(
                    f"{key.relpath}/{dv}: finns redan med annan proveniens (extra/sources); "
                    "det lagrade manifestet skrivs aldrig om")
            self._finish_dir(target)
            self._register(info, manifest)
            return PublishResult(info, "reused")

        staging = self._new_staging()
        try:
            staged = self._stage(staging, key, canon, dv, extra)
            self._publish_dir(staged, target)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
        info, manifest, _ = store._verify_object(key, dv)
        self._register(info, manifest)
        return PublishResult(info, "published")

    # -------------------------------------------------------------- steg
    def _new_staging(self) -> Path:
        d = self._store.tmp / uuid.uuid4().hex
        d.mkdir()                                            # FileExistsError: aldrig delad
        return d

    def _stage(self, staging: Path, key: DatasetKey, canon: pd.DataFrame, dv: str,
               extra: dict[str, Any]) -> Path:
        phase2 = DatasetStore(staging / "phase2", ParquetBackend())
        manifest = phase2.write(key, canon, extra)           # Phase 2: validering + manifest
        if manifest["data_version"] != dv:
            raise DataIntegrityError(f"{key.relpath}: data_version ändrades under staging")
        obj = staging / "object"
        obj.mkdir()
        os.replace(phase2.data_path(key), obj / DATA_FILE)
        os.replace(phase2.manifest_path(key), obj / MANIFEST_FILE)
        mbytes = (obj / MANIFEST_FILE).read_bytes()
        version = {
            "format": VERSION_FORMAT,
            "dataset_id": catalog.dataset_id(key),
            "dataset": json.loads(catalog.dataset_key_json(key)),
            "data_version": dv,
            "data_file": {"path": DATA_FILE, "sha256": sha256_file(obj / DATA_FILE),
                          "bytes": (obj / DATA_FILE).stat().st_size},
            "manifest": {"path": MANIFEST_FILE, "sha256": hashlib.sha256(mbytes).hexdigest(),
                         "bytes": len(mbytes)},
            "rows": manifest["rows"],
            "first_open_time": manifest["first_open_time"],
            "last_open_time": manifest["last_open_time"],
        }
        with (obj / VERSION_FILE).open("xb") as f:
            f.write(canonical_json_bytes(version))
        for name in sorted(OBJECT_FILES):
            os.chmod(obj / name, FILE_MODE)
            _fsync_file(obj / name)
        _fsync_dir(obj)
        return obj

    def _publish_dir(self, staged: Path, target: Path) -> None:
        _mkdirs_durable(target.parent, self._store.objects)
        if os.path.lexists(target):
            raise ImmutableObjectError(f"{target}: objektet finns redan; skrivs aldrig över")
        try:
            _rename(staged, target)
        except OSError as e:
            if e.errno in (errno.EEXIST, errno.ENOTEMPTY):
                raise ImmutableObjectError(
                    f"{target}: objektet finns redan; skrivs aldrig över") from e
            raise
        self._finish_dir(target)

    @staticmethod
    def _finish_dir(target: Path) -> None:
        """0555 + fsync. Idempotent; slutför en publicering som avbröts efter rename."""
        if os.stat(target).st_mode & 0o777 != DIR_MODE:
            os.chmod(target, DIR_MODE)
        _fsync_dir(target)
        _fsync_dir(target.parent)

    def _register(self, info: VersionInfo, manifest: dict[str, Any]) -> None:
        """Katalograd för ett verifierat, durabelt objekt (idempotent, i EN transaktion)."""
        row = {"dataset_id": info.dataset_id, "data_version": info.data_version,
               "data_file_sha256": info.data_file_sha256, "data_file_bytes": info.data_file_bytes,
               "manifest_sha256": info.manifest_sha256, "version_sha256": info.version_sha256,
               "manifest_schema_version": int(manifest["schema_version"]), "rows": info.rows,
               "first_open_time_ms": _iso_to_ms(info.first_open_time),
               "last_open_time_ms": _iso_to_ms(info.last_open_time),
               "gap_count": int(manifest["gap_count"]),
               "missing_bars_total": int(manifest["missing_bars_total"])}
        sources = [{"dataset_id": info.dataset_id, "data_version": info.data_version,
                    "source_index": i, "name": s["name"], "sha256": s["sha256"], "rows": s["rows"]}
                   for i, s in enumerate(_parse_sources(manifest.get("sources")))]
        ident = {"dataset_id": info.dataset_id, "data_version": info.data_version}
        dv_cols, vs_cols = ", ".join(_DV_COLUMNS), ", ".join(_VS_COLUMNS)
        conn = self._conn
        with catalog.transaction(conn):
            if catalog.ensure_dataset(conn, info.key) != info.dataset_id:
                raise catalog.CatalogCorruptError(f"{info.key.relpath}: dataset_id avviker")
            existing = conn.execute(
                f"SELECT {dv_cols} FROM dataset_versions "
                "WHERE dataset_id = :dataset_id AND data_version = :data_version", ident).fetchone()
            if existing is None:
                conn.execute(f"INSERT INTO dataset_versions ({dv_cols}) "
                             f"VALUES ({', '.join(':' + c for c in _DV_COLUMNS)})", row)
                conn.executemany(f"INSERT INTO version_sources ({vs_cols}) "
                                 f"VALUES ({', '.join(':' + c for c in _VS_COLUMNS)})", sources)
                return
            have = [dict(zip(_VS_COLUMNS, r, strict=True)) for r in conn.execute(
                f"SELECT {vs_cols} FROM version_sources "
                "WHERE dataset_id = :dataset_id AND data_version = :data_version "
                "ORDER BY source_index", ident)]
            if dict(zip(_DV_COLUMNS, existing, strict=True)) != row or have != sources:
                raise catalog.CatalogCorruptError(
                    f"{info.key.relpath}/{info.data_version}: katalograden matchar inte objektet")


_INIT_ENTRIES = frozenset({LOCK_FILE, "objects", "tmp", CATALOG_FILE})
_INIT_TMP_PATTERNS = (".store-*.json", ".catalog-*.sqlite.tmp", ".catalog-*.sqlite.tmp-journal",
                      ".catalog-*.sqlite.tmp-wal", ".catalog-*.sqlite.tmp-shm")


def _check_resumable_init(root: Path) -> None:
    """Tillåter bara en tom katalog eller spåren av en avbruten init (se INIT)."""
    if root.is_symlink() or not root.is_dir():
        raise StoreFormatError(f"{root}: inte en katalog")
    refuse = f"{root}: icke-tom katalog utan {STORE_FILE} som inte är en avbruten init"
    entries = {p.name: p for p in root.iterdir()}
    if not set(entries) <= _INIT_ENTRIES or any(p.is_symlink() for p in entries.values()):
        raise StoreFormatError(refuse)
    for name in (LOCK_FILE, CATALOG_FILE):
        if name in entries and not entries[name].is_file():
            raise StoreFormatError(refuse)
    for name in ("objects", "tmp"):
        if name in entries and not entries[name].is_dir():
            raise StoreFormatError(refuse)
    if "objects" in entries and any(entries["objects"].iterdir()):
        raise StoreFormatError(refuse)
    if "tmp" in entries:
        for p in entries["tmp"].iterdir():
            if p.is_symlink() or not p.is_file() \
                    or not any(fnmatch.fnmatchcase(p.name, pat) for pat in _INIT_TMP_PATTERNS):
                raise StoreFormatError(refuse)
    if CATALOG_FILE in entries:
        try:
            conn = catalog.open_catalog(entries[CATALOG_FILE], readonly=True)
        except catalog.CatalogError as e:
            raise StoreFormatError(f"{refuse}: {e}") from e
        try:
            n_ds = conn.execute("SELECT count(*) FROM datasets").fetchone()[0]
            n_dv = conn.execute("SELECT count(*) FROM dataset_versions").fetchone()[0]
        finally:
            conn.close()
        if n_ds or n_dv:
            raise StoreFormatError(f"{refuse}: katalogen innehåller dataset/versioner")


def _parse_sources(sources: Any) -> list[dict[str, Any]]:
    if sources is None:
        return []
    if not isinstance(sources, list):
        raise InvalidSourcesError("manifest 'sources' måste vara en lista")
    for s in sources:
        if not (isinstance(s, dict) and isinstance(s.get("name"), str) and s["name"]
                and catalog.is_sha256_hex(s.get("sha256"))
                and isinstance(s.get("rows"), int) and not isinstance(s["rows"], bool)
                and s["rows"] >= 0):
            raise InvalidSourcesError(f"ogiltig källa i 'sources': {s!r}")
    return sources


def _iso_to_ms(iso: str) -> int:
    return int(pd.Timestamp(iso).value // 1_000_000)
