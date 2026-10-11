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

REFS (steg 3)
  <root>/ref_log.jsonl   en kompakt kanonisk JSON-rad per post; entry_sha256 = sha256
                         av radens bytes (utan radslut); hashkedja via prev_entry_sha256.
  <root>/refs/binance/<instrument>/<SYMBOL>/<timeframe>.json   aktuell version (0444).
  Ordning i set_current (under skrivarlåset):
    1. målobjektet verifieras FULLT (och registreras i katalogen)
    2. ref_log = gamla bytes + ny rad -> tmp, fsync, rename, fsync   (historik först)
    3. ref-filen -> tmp, fsync, rename, fsync                         (sedan "aktuell")
    4. katalogen synkas från filerna i EN transaktion
  Krasch mellan 2 och 3 lämnar EN väntande post. set_current, create_snapshot och
  snapshot_current vägrar då (PendingRefUpdateError) tills complete_pending_ref_updates()
  anropas explicit. Den slutför bara om posten är datasetets sista, dess
  previous_data_version == ref-filens version (eller båda saknas) och målobjektet
  klarar full verifiering igen; annars fel och ingen ändring. Fler än en väntande
  post = korruption. Läsare läser ref-filen FÖRE ref_log (loggen växer bara) och ser
  en pending-flagga utan att det ändrar vad de läser.

SNAPSHOTS (steg 3)
  <root>/snapshots/<snapshot_id>.json, 0444, publiceras med link (aldrig överskrivning).
  Identitet: se refs.py. snapshot_current() pinnar versionerna vid skapandet och
  följer aldrig senare ref-ändringar. PinnedStore läser endast pinnade versioner,
  öppnar aldrig refs/ref_log och avvisar alla skrivningar (ReadOnlyStoreError).

KAUSALITET GENOM REFS OCH SNAPSHOTS
  as_of styr bara vilka barer som syns (available_at <= as_of) INOM den valda
  versionen; det väljer aldrig "versionen som var aktuell vid t" (inga tidsstämplar).
  KRAV: reproducerbar forskning och backtester MÅSTE läsa via en snapshot
  (PinnedStore). read_current(as_of=t) är "senast kända data kapad vid t", inte
  point-in-time, och får aldrig användas för en backtest.

INGEST MED LINEAGE (steg 4)
  <root>/ingests/<ingest_id>.json   en oföränderlig post per ingest (0444, link), även för
                                    upprepad ingest av identiskt innehåll; kedja via
                                    seq + prev_ingest_id (typer och id: lineage.py).
  Phase 2 ingest_dataset() körs OFÖRÄNDRAD mot en fail-closed fångstlagring (exakt ett
  write; allt annat -> fel). Dess extra delas upp: CONTENT_DERIVED_KEYS går till
  objektmanifestet, sources/duplicates_dropped ENDAST till ingest-posten. Vid återbruk
  jämförs exakt innehållsnycklarna (avvikelse -> ObjectCorruptError). Den publika
  publish_version är oförändrad (strikt M2); ingest använder en egen intern väg.
  Ordning i ingest (under skrivarlåset):
    1. Phase 2 (parsning, dedup, validering) - skriver ingenting
    2. regressionsskydd: ny version som slutar tidigare än aktuell -> fel, om inte
       allow_regression=True (sparas i postens params)
    3. objektet publiceras eller verifieras fullt och återanvänds; katalograd
    4. planerad ref_log-rad beräknas (skrivs inte)
    5. ingest-posten -> tmp, fsync, link, fsync          (commit-punkt för lineage)
    6. ref_log-raden och ref-filen via steg 3 (action="ingest", logg före ref)
    7. katalogen synkas från filerna (refs, ref_log, ingest_events, ingest_sources)
  Krasch mellan 5 och 6 lämnar EN väntande ingest: alla ref-/ingest-operationer vägrar
  (PendingIngestError) tills complete_pending_ingest() anropas explicit. Den slutför bara
  om posten är den sista, dess planerade ref_log-rad kan reproduceras exakt,
  previous_data_version == ref-filens version (eller båda saknas) och objektet klarar
  full verifiering igen; annars fel och ingen ändring. Kontrollen sker via en inlagd rad
  i _load_ref_state och är en no-op när det inte finns några ingest-poster.

INTERVALLÄSNING (steg 7)
  read_range(key, data_version, start, end, as_of=None) och PinnedStore.read_range(key, start,
  end, as_of=None) har EXAKT LocalOHLCVProvider.get_bars-semantik (Phase 2):
    * [start, end) på open_time: bar exakt vid start ingår, bar exakt vid end ingår inte;
      start == end -> tomt; end < start -> ValueError
    * as_of -> endast barer med available_at = open_time + tf <= as_of (causal_cutoff);
      as_of=None -> ingen kausalitetsfiltrering
    * start/end/as_of måste vara datetime (annars TypeError), tz-aware med UTC-offset 0
      (require_utc; naiv eller annan offset -> ValueError); kontrolleras innan någon fil öppnas
    * gap_before som lagrad (räknas aldrig om på utsnittet); inga luckor fylls
  Varje anrop verifierar hela objektet (read_version/_verify_object) innan utsnittet tas: ingen
  cache, inga delläsningar. read_current har ingen intervallvariant (P4-S3-D4). Intervall-
  läsningarna kopplas inte in i LocalOHLCVProvider.

FUNDING (etapp B steg 4, SB-S4-a A2)
  <root>/objects/binance/perp/<SYMBOL>/funding/<data_version>/
      funding.parquet           kanonisk funding-ram (data/funding.py)
      manifest.json             kanonisk JSON: nyckel, rader, första/sista calc_time, data_version,
                                källfilerna (namn, sha256, rader) och borttagna dubbletter
      version.json              objektmetadata: dataset_id (funding-domänen) + hashar
  Samma oföränderlighet och publiceringsordning som OHLCV-objekten (staging i tmp/, 0444/0555,
  fsync, atomisk rename, verifiering från disk, katalograd sist i EN transaktion:
  funding_datasets, funding_versions, funding_version_sources). Samma data_version med andra
  källor -> ProvenanceConflictError (strikt, som publish_version). Inga refs, ingen ref_log och
  inga ingest-poster för funding: en funding-version pinnas bara i en snapshot (format v2,
  refs.py) och läses via PinnedStore. Kausalitet: as_of=t exponerar ENDAST rader med
  available_at = calc_time + 15 s <= t (data/funding.py, SB-S4-c); intervall [start, end) på
  calc_time med samma indatakontroller som read_range.
"""
from __future__ import annotations

import copy
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
from types import MappingProxyType
from typing import Any, Literal

import pandas as pd

from btc_trading_system.core.types import TIMEFRAMES, require_utc
from btc_trading_system.data import funding as fd
from btc_trading_system.data import schema as p2_schema
from btc_trading_system.data.ingest import ingest_dataset
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
from btc_trading_system.database.lineage import (
    CONTENT_DERIVED_KEYS,
    IngestRecord,
    check_content_extra,
    split_ingest_extra,
)
from btc_trading_system.database.refs import (
    DatasetVersionRef,
    DataSnapshot,
    FundingVersionRef,
    compact_json_bytes,
)

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

REF_FORMAT = "btc-quant/ref/v1"
REF_LOG_FILE = "ref_log.jsonl"
REFS_DIR = "refs"
SNAPSHOTS_DIR = "snapshots"
REF_ACTIONS = ("ingest", "set_current", "import_legacy")          # = catalog CHECK
_REF_LOG_KEYS = frozenset({"seq", "prev_entry_sha256", "dataset", "dataset_id", "data_version",
                           "previous_data_version", "action"})
_REF_FILE_KEYS = frozenset({"format", "dataset", "dataset_id", "data_version", "ref_log_seq",
                            "ref_log_entry_sha256"})
INGESTS_DIR = "ingests"
FUNDING_DATA_FILE = "funding.parquet"
FUNDING_OBJECT_FILES = frozenset({FUNDING_DATA_FILE, MANIFEST_FILE, VERSION_FILE})
FUNDING_VERSION_FORMAT = "btc-quant/funding-version/v1"
_FUNDING_VERSION_KEYS = frozenset({"format", "dataset_id", "dataset", "data_version", "data_file",
                                   "manifest", "rows", "first_calc_time", "last_calc_time"})
_FUNDING_MANIFEST_KEYS = frozenset({"schema_version", "kind", "exchange", "instrument", "symbol", "rows",
                                    "first_calc_time", "last_calc_time", "data_version", "sources",
                                    "duplicates_dropped"})
_FV_COLUMNS = ("dataset_id", "data_version", "data_file_sha256", "data_file_bytes", "manifest_sha256",
               "version_sha256", "manifest_schema_version", "rows", "first_calc_time_ms",
               "last_calc_time_ms", "duplicates_dropped")
_FVS_COLUMNS = ("dataset_id", "data_version", "source_index", "name", "sha256", "rows")


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


class ReadOnlyStoreError(StoreError):
    """Skrivning via en PinnedStore (read-only)."""


class PendingRefUpdateError(StoreError):
    """En ref_log-post väntar på sin ref-fil; anropa complete_pending_ref_updates()."""


class RefCorruptError(DataIntegrityError):
    """Ref-filer och ref_log är inkonsistenta eller manipulerade."""


class SnapshotCorruptError(DataIntegrityError):
    """En snapshotfil matchar inte sin identitet/sitt format."""


class SnapshotNotFoundError(DatasetNotFoundError):
    """Snapshoten finns inte."""


class NoCurrentRefError(DatasetNotFoundError):
    """Datasetet har ingen aktuell ref."""


class DatasetNotInSnapshotError(DatasetNotFoundError):
    """Datasetet ingår inte i snapshoten; en PinnedStore läser aldrig något annat."""


class PendingIngestError(StoreError):
    """En ingest-post väntar på sin ref_log-rad; anropa complete_pending_ingest()."""


class IngestCorruptError(DataIntegrityError):
    """Ingest-poster och ref_log är inkonsistenta eller manipulerade."""


class VersionRegressionError(StoreError):
    """Ingesten skulle göra en version som slutar tidigare till aktuell (allow_regression=False)."""


@dataclass(frozen=True)
class CurrentRef:
    ref: DatasetVersionRef | None                    # None = ingen ref ännu
    ref_log_seq: int | None
    ref_log_entry_sha256: str | None
    ref_file_sha256: str | None
    pending: bool                                    # loggen har en post som ref-filen inte speglar
    pending_data_version: str | None


@dataclass(frozen=True)
class RefUpdate:
    ref: DatasetVersionRef
    changed: bool
    previous_data_version: str | None
    ref_log_seq: int


@dataclass(frozen=True)
class SnapshotResult:
    snapshot: DataSnapshot
    outcome: Literal["created", "reused"]


@dataclass(frozen=True)
class IngestOutcome:
    record: IngestRecord
    ingest_id: str
    record_sha256: str


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


@dataclass(frozen=True)
class FundingVersionInfo:
    key: fd.FundingKey
    dataset_id: str
    data_version: str
    data_file_sha256: str
    data_file_bytes: int
    manifest_sha256: str
    manifest_bytes: int
    version_sha256: str
    rows: int
    first_calc_time: str
    last_calc_time: str


@dataclass(frozen=True)
class FundingPublishResult:
    info: FundingVersionInfo
    outcome: Literal["published", "reused"]
    duplicates_dropped: int


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


# ------------------------------------------------------------------ intervalläsning (steg 7)
def _check_range(start: datetime, end: datetime, as_of: datetime | None) -> tuple[datetime, datetime]:
    """Indata innan någon fil öppnas: datetime, tz-aware UTC (require_utc), end >= start."""
    named = [("start", start), ("end", end)] + ([("as_of", as_of)] if as_of is not None else [])
    for name, ts in named:
        if not isinstance(ts, datetime):
            raise TypeError(f"{name} måste vara datetime, fick {type(ts).__name__}")
        require_utc(ts, name)
    lo, hi = require_utc(start, "start"), require_utc(end, "end")
    if hi < lo:
        raise ValueError("end < start")
    return lo, hi


def range_slice(frame: pd.DataFrame, timeframe: str, start: datetime, end: datetime,
                as_of: datetime | None = None) -> pd.DataFrame:
    """[start, end) på open_time; as_of -> available_at <= as_of. Exakt samma indexaritmetik
    som LocalOHLCVProvider.get_bars, på en verifierad frame i lagrets format."""
    p2_schema.timeframe_ms(timeframe)                        # okänd timeframe -> ValueError
    lo_ts, hi_ts = _check_range(start, end, as_of)
    times = frame["open_time"]
    lo = int(times.searchsorted(pd.Timestamp(lo_ts), side="left"))
    hi = int(times.searchsorted(pd.Timestamp(hi_ts), side="left"))
    if as_of is not None:
        hi = min(hi, int(times.searchsorted(causal_cutoff(as_of, timeframe), side="right")))
    return frame.iloc[lo:max(lo, hi)].reset_index(drop=True)


def funding_range_slice(frame: pd.DataFrame, start: datetime, end: datetime,
                        as_of: datetime | None = None) -> pd.DataFrame:
    """[start, end) på calc_time; as_of -> available_at = calc_time + 15 s <= as_of (SB-S4-c)."""
    start_ts, end_ts = _check_range(start, end, as_of)
    calc = frame["calc_time"]
    first = int(calc.searchsorted(pd.Timestamp(start_ts), side="left"))
    stop = int(calc.searchsorted(pd.Timestamp(end_ts), side="left"))
    if as_of is not None:
        stop = min(stop, int(calc.searchsorted(fd.funding_cutoff(as_of), side="right")))
    return frame.iloc[first:max(first, stop)].reset_index(drop=True)


def apply_funding_as_of(frame: pd.DataFrame, as_of: datetime) -> pd.DataFrame:
    return frame.loc[frame["calc_time"] <= fd.funding_cutoff(as_of)].reset_index(drop=True)


# ------------------------------------------------------------------ refs / ref_log (steg 3)
@dataclass(frozen=True)
class _LogEntry:
    seq: int
    entry_sha256: str
    prev_entry_sha256: str | None
    ref: DatasetVersionRef
    previous_data_version: str | None
    action: str


@dataclass(frozen=True)
class _RefFile:
    ref: DatasetVersionRef
    ref_log_seq: int
    ref_log_entry_sha256: str
    file_sha256: str


@dataclass(frozen=True)
class _RefState:
    log_bytes: bytes
    entries: tuple[_LogEntry, ...]
    refs: dict[str, _RefFile]
    pending: _LogEntry | None


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _link(src: Path, dst: Path) -> None:
    os.link(src, dst)                                   # FileExistsError: aldrig överskrivning


def parse_ref_log(data: bytes) -> tuple[_LogEntry, ...]:
    """Strikt: kompletta kanoniska rader, seq 1..n, hashkedja, datasetvis kontinuitet."""
    if not data:
        return ()
    if not data.endswith(b"\n"):
        raise RefCorruptError("ref_log: ofullständig sista rad")
    out: list[_LogEntry] = []
    prev: str | None = None
    last_by_dataset: dict[str, str] = {}
    for i, line in enumerate(data[:-1].split(b"\n"), start=1):
        try:
            obj = json.loads(line)
        except ValueError as e:
            raise RefCorruptError(f"ref_log rad {i}: inte JSON") from e
        if not isinstance(obj, dict) or set(obj) != _REF_LOG_KEYS or compact_json_bytes(obj) != line:
            raise RefCorruptError(f"ref_log rad {i}: ogiltig eller icke-kanonisk")
        if not _is_int(obj["seq"]) or obj["seq"] != i:
            raise RefCorruptError(f"ref_log rad {i}: seq {obj['seq']!r}")
        if obj["prev_entry_sha256"] != prev:
            raise RefCorruptError(f"ref_log rad {i}: hashkedjan är bruten")
        if obj["action"] not in REF_ACTIONS:
            raise RefCorruptError(f"ref_log rad {i}: okänd action {obj['action']!r}")
        try:
            ref = DatasetVersionRef.from_json_obj(
                {k: obj[k] for k in ("dataset", "dataset_id", "data_version")})
        except (ValueError, TypeError) as e:
            raise RefCorruptError(f"ref_log rad {i}: {e}") from e
        previous = obj["previous_data_version"]
        if previous != last_by_dataset.get(ref.dataset_id) or previous == ref.data_version:
            raise RefCorruptError(f"ref_log rad {i}: previous_data_version bryter datasetets historik")
        sha = hashlib.sha256(line).hexdigest()
        out.append(_LogEntry(i, sha, prev, ref, previous, obj["action"]))
        prev = sha
        last_by_dataset[ref.dataset_id] = ref.data_version
    return tuple(out)


def parse_ref_file(data: bytes, key: DatasetKey) -> _RefFile:
    try:
        obj = json.loads(data)
    except (ValueError, UnicodeDecodeError) as e:
        raise RefCorruptError(f"{key.relpath}: ref-filen är inte JSON") from e
    if not isinstance(obj, dict) or set(obj) != _REF_FILE_KEYS or canonical_json_bytes(obj) != data \
            or obj["format"] != REF_FORMAT or not _is_int(obj["ref_log_seq"]) or obj["ref_log_seq"] < 1 \
            or not catalog.is_sha256_hex(obj["ref_log_entry_sha256"]):
        raise RefCorruptError(f"{key.relpath}: ogiltig ref-fil")
    try:
        ref = DatasetVersionRef.from_json_obj({k: obj[k] for k in ("dataset", "dataset_id", "data_version")})
    except (ValueError, TypeError) as e:
        raise RefCorruptError(f"{key.relpath}: {e}") from e
    if ref.key != key:
        raise RefCorruptError(f"{key.relpath}: ref-filen tillhör {ref.key.relpath}")
    return _RefFile(ref, obj["ref_log_seq"], obj["ref_log_entry_sha256"], hashlib.sha256(data).hexdigest())


def _ref_file_bytes(ref: DatasetVersionRef, entry: _LogEntry) -> bytes:
    return canonical_json_bytes({"format": REF_FORMAT, **ref.to_json_obj(),
                                 "ref_log_seq": entry.seq, "ref_log_entry_sha256": entry.entry_sha256})


def _current_from(ref_file: _RefFile | None, entries: tuple[_LogEntry, ...], did: str) -> CurrentRef:
    """Läsarvy (tolerant mot en samtidig skrivare, strikt mot inkonsistens)."""
    ds_entries = [e for e in entries if e.ref.dataset_id == did]
    if ref_file is None:
        return CurrentRef(None, None, None, None, pending=bool(ds_entries),
                          pending_data_version=ds_entries[-1].ref.data_version if ds_entries else None)
    seq = ref_file.ref_log_seq
    if seq > len(entries) or entries[seq - 1].entry_sha256 != ref_file.ref_log_entry_sha256 \
            or entries[seq - 1].ref != ref_file.ref:
        raise RefCorruptError(f"{ref_file.ref.key.relpath}: ref-filen matchar inte ref_log (seq {seq})")
    later = [e for e in ds_entries if e.seq > seq]
    return CurrentRef(ref_file.ref, seq, ref_file.ref_log_entry_sha256, ref_file.file_sha256,
                      pending=bool(later), pending_data_version=later[-1].ref.data_version if later else None)


# ------------------------------------------------------------------ ingest (steg 4)
class _CaptureStore(DatasetStore):
    """Fail-closed ersättning för DatasetStore i Phase 2 ingest_dataset(): tar emot EXAKT ett
    write(key, frame, extra) och skriver ingenting. Andra anrop eller ett andra write -> fel."""

    def __init__(self, key: DatasetKey) -> None:           # ingen super().__init__: ingen rot/backend
        self._expected_key = key
        self._writes = 0
        self.captured: tuple[pd.DataFrame, dict[str, Any]] | None = None

    def write(self, key: DatasetKey, df: pd.DataFrame, extra: dict[str, Any]) -> dict[str, Any]:
        self._writes += 1
        if self._writes != 1:
            raise StoreError("fångstlagring: mer än ett write-anrop")
        if key != self._expected_key:
            raise StoreError(f"fångstlagring: {key.relpath} != {self._expected_key.relpath}")
        self.captured = (df.copy(), copy.deepcopy(extra))
        return {}

    @staticmethod
    def _refuse(name: str) -> StoreError:
        return StoreError(f"fångstlagring: otillåtet anrop {name}()")

    def data_path(self, key: DatasetKey) -> Path:
        raise self._refuse("data_path")

    def manifest_path(self, key: DatasetKey) -> Path:
        raise self._refuse("manifest_path")

    def exists(self, key: DatasetKey) -> bool:
        raise self._refuse("exists")

    def read_manifest(self, key: DatasetKey) -> dict[str, Any]:
        raise self._refuse("read_manifest")

    def read(self, key: DatasetKey) -> tuple[pd.DataFrame, dict[str, Any]]:
        raise self._refuse("read")


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

    def read_range(self, key: DatasetKey, data_version: str, start: datetime, end: datetime, *,
                   as_of: datetime | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Exakt denna version, [start, end) med Phase 2-semantik (se INTERVALLÄSNING).
        Indata kontrolleras först; hela objektet verifieras vid varje anrop."""
        _check_range(start, end, as_of)
        _, manifest, frame = self._verify_object(key, data_version)
        return range_slice(frame, key.timeframe, start, end, as_of), manifest

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

    # -------------------------------------------------------------- funding: läsning (inget lås)
    def funding_object_dir(self, key: fd.FundingKey, data_version: str) -> Path:
        if not catalog.is_sha256_hex(data_version):
            raise ValueError(f"ogiltig data_version: {data_version!r}")
        catalog.funding_dataset_key_json(key)                # validerar exchange
        return self.objects / key.relpath / data_version

    def has_funding_version(self, key: fd.FundingKey, data_version: str) -> bool:
        return os.path.lexists(self.funding_object_dir(key, data_version))

    def verify_funding_version(self, key: fd.FundingKey, data_version: str) -> FundingVersionInfo:
        return self._verify_funding_object(key, data_version)[0]

    def read_funding_version(self, key: fd.FundingKey, data_version: str, *,
                             as_of: datetime | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Exakt denna version, verifierad vid varje läsning; as_of -> available_at <= as_of."""
        if as_of is not None:
            require_utc(as_of, "as_of")
        _, manifest, frame = self._verify_funding_object(key, data_version)
        if as_of is not None:
            frame = apply_funding_as_of(frame, as_of)
        return frame, manifest

    def read_funding_range(self, key: fd.FundingKey, data_version: str, start: datetime, end: datetime, *,
                           as_of: datetime | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Exakt denna version, [start, end) på calc_time; indata kontrolleras före all fil-I/O."""
        _check_range(start, end, as_of)
        _, manifest, frame = self._verify_funding_object(key, data_version)
        return funding_range_slice(frame, start, end, as_of), manifest

    def _verify_funding_object(self, key: fd.FundingKey, dv: str
                               ) -> tuple[FundingVersionInfo, dict[str, Any], pd.DataFrame]:
        d = self.funding_object_dir(key, dv)
        where = f"{key.relpath}/{dv}"
        if not os.path.lexists(d):
            raise DatasetNotFoundError(f"funding-version saknas: {where}")
        if d.is_symlink() or not d.is_dir():
            raise ObjectCorruptError(f"{where}: inte en katalog")
        names = {p.name for p in d.iterdir()}
        if names != FUNDING_OBJECT_FILES:
            raise ObjectCorruptError(f"{where}: filer {sorted(names)} != {sorted(FUNDING_OBJECT_FILES)}")
        for name in sorted(FUNDING_OBJECT_FILES):
            path = d / name
            if path.is_symlink() or not path.is_file():
                raise ObjectCorruptError(f"{where}/{name}: inte en vanlig fil")
            if os.lstat(path).st_mode & 0o222:
                raise ObjectCorruptError(f"{where}/{name}: skrivbar (oföränderliga filer ska vara 0444)")
        vbytes = (d / VERSION_FILE).read_bytes()
        version = self._parse_funding_version(vbytes, key, dv, where)
        mbytes = (d / MANIFEST_FILE).read_bytes()
        if hashlib.sha256(mbytes).hexdigest() != version["manifest"]["sha256"] \
                or len(mbytes) != version["manifest"]["bytes"]:
            raise ObjectCorruptError(f"{where}: manifest_sha256 matchar inte version.json")
        try:
            manifest = json.loads(mbytes)
        except ValueError as e:
            raise ObjectCorruptError(f"{where}: manifest är inte JSON") from e
        expected = {"schema_version": fd.FUNDING_SCHEMA_VERSION, "kind": fd.FUNDING_KIND,
                    "exchange": key.exchange, "instrument": key.instrument.value, "symbol": key.symbol,
                    "data_version": dv, "rows": version["rows"], "first_calc_time": version["first_calc_time"],
                    "last_calc_time": version["last_calc_time"]}
        if not isinstance(manifest, dict) or set(manifest) != _FUNDING_MANIFEST_KEYS \
                or canonical_json_bytes(manifest) != mbytes \
                or any(manifest[k] != v for k, v in expected.items()) \
                or not _is_int(manifest["duplicates_dropped"]) or manifest["duplicates_dropped"] < 0:
            raise ObjectCorruptError(f"{where}: manifestet matchar inte objektet")
        try:
            sources = _parse_funding_sources(manifest["sources"])
        except InvalidSourcesError as e:
            raise ObjectCorruptError(f"{where}: {e}") from e
        fpath = d / FUNDING_DATA_FILE
        if sha256_file(fpath) != version["data_file"]["sha256"] \
                or fpath.stat().st_size != version["data_file"]["bytes"]:
            raise ObjectCorruptError(f"{where}: data_file_sha256 matchar inte version.json")
        try:
            frame = fd.canonicalize_funding(fd.FundingParquet.read(fpath))
            fd.check_funding_frame(frame)
        except Exception as e:                           # trasig Parquet eller ogiltig ram trots rätt hash
            raise ObjectCorruptError(f"{where}: funding.parquet kan inte läsas eller valideras: {e}") from e
        if fd.funding_content_hash(frame) != dv:
            raise ObjectCorruptError(f"{where}: innehållet matchar inte data_version")
        if len(frame) != version["rows"] \
                or frame["calc_time"].iloc[0].isoformat() != version["first_calc_time"] \
                or frame["calc_time"].iloc[-1].isoformat() != version["last_calc_time"] \
                or sum(s["rows"] for s in sources) != len(frame) + manifest["duplicates_dropped"]:
            raise ObjectCorruptError(f"{where}: rader/täckning matchar inte version.json och källorna")
        info = FundingVersionInfo(
            key=key, dataset_id=version["dataset_id"], data_version=dv,
            data_file_sha256=version["data_file"]["sha256"], data_file_bytes=version["data_file"]["bytes"],
            manifest_sha256=version["manifest"]["sha256"], manifest_bytes=version["manifest"]["bytes"],
            version_sha256=hashlib.sha256(vbytes).hexdigest(), rows=version["rows"],
            first_calc_time=version["first_calc_time"], last_calc_time=version["last_calc_time"])
        return info, manifest, frame

    @staticmethod
    def _parse_funding_version(vbytes: bytes, key: fd.FundingKey, dv: str, where: str) -> dict[str, Any]:
        try:
            v = json.loads(vbytes)
        except ValueError as e:
            raise ObjectCorruptError(f"{where}: version.json är inte JSON") from e
        ok = (isinstance(v, dict) and set(v) == _FUNDING_VERSION_KEYS
              and canonical_json_bytes(v) == vbytes
              and v["format"] == FUNDING_VERSION_FORMAT
              and v["dataset_id"] == catalog.funding_dataset_id(key)
              and v["dataset"] == json.loads(catalog.funding_dataset_key_json(key))
              and v["data_version"] == dv
              and _is_int(v["rows"]) and v["rows"] >= 1
              and all(isinstance(v[k], dict) and set(v[k]) == {"path", "sha256", "bytes"}
                      and catalog.is_sha256_hex(v[k]["sha256"])
                      and _is_int(v[k]["bytes"]) and v[k]["bytes"] > 0
                      for k in ("data_file", "manifest"))
              and v["data_file"]["path"] == FUNDING_DATA_FILE and v["manifest"]["path"] == MANIFEST_FILE)
        if not ok:
            raise ObjectCorruptError(f"{where}: version.json är ogiltig eller tillhör annat objekt")
        return v

    # -------------------------------------------------------------- refs / snapshots: läsning (inget lås)
    def _ref_path(self, key: DatasetKey) -> Path:
        catalog.dataset_key_json(key)                        # validerar exchange
        return self.root / REFS_DIR / f"{key.relpath}.json"

    def _read_ref_log(self) -> bytes:
        path = self.root / REF_LOG_FILE
        if not os.path.lexists(path):
            return b""
        if path.is_symlink() or not path.is_file() or os.lstat(path).st_mode & 0o222:
            raise RefCorruptError(f"{REF_LOG_FILE}: inte en skrivskyddad vanlig fil")
        return path.read_bytes()

    def _read_ref_file(self, path: Path, key: DatasetKey) -> _RefFile:
        if path.is_symlink() or not path.is_file() or os.lstat(path).st_mode & 0o222:
            raise RefCorruptError(f"{path}: inte en skrivskyddad vanlig fil")
        return parse_ref_file(path.read_bytes(), key)

    def current_ref(self, key: DatasetKey) -> CurrentRef:
        """Aktuell ref (filerna är facit, inte katalogen). Ref-filen läses FÖRE ref_log:
        loggen växer bara, så refens post finns alltid i den senare läsningen.

        The pending flag seen by a reader is advisory: during a concurrent set_current
        it can be a false positive. Only a writer's classification is authoritative.
        What the reader reads is always consistent."""
        path = self._ref_path(key)
        ref_file = self._read_ref_file(path, key) if os.path.lexists(path) else None
        return _current_from(ref_file, parse_ref_log(self._read_ref_log()), catalog.dataset_id(key))

    def read_current(self, key: DatasetKey, *, as_of: datetime | None = None
                     ) -> tuple[pd.DataFrame, dict[str, Any], DatasetVersionRef]:
        """Senast kända data, kapad vid as_of. INTE point-in-time: backtester och
        reproducerbar forskning MÅSTE läsa via en snapshot (PinnedStore)."""
        cur = self.current_ref(key)
        if cur.ref is None:
            raise NoCurrentRefError(f"ingen aktuell ref för {key.relpath}")
        frame, manifest = self.read_version(key, cur.ref.data_version, as_of=as_of)
        return frame, manifest, cur.ref

    def read_snapshot(self, snapshot_id: str) -> DataSnapshot:
        if not catalog.is_sha256_hex(snapshot_id):
            raise ValueError(f"ogiltigt snapshot_id: {snapshot_id!r}")
        path = self.root / SNAPSHOTS_DIR / f"{snapshot_id}.json"
        if not os.path.lexists(path):
            raise SnapshotNotFoundError(f"snapshot saknas: {snapshot_id}")
        if path.is_symlink() or not path.is_file() or os.lstat(path).st_mode & 0o222:
            raise SnapshotCorruptError(f"{path.name}: inte en skrivskyddad vanlig fil")
        try:
            return DataSnapshot.from_file_bytes(path.read_bytes(), expected_id=snapshot_id)
        except (ValueError, TypeError) as e:
            raise SnapshotCorruptError(f"{path.name}: {e}") from e

    def pinned(self, snapshot_id: str) -> PinnedStore:
        """Read-only vy över exakt snapshotens versioner."""
        return PinnedStore(self, self.read_snapshot(snapshot_id))

    def _load_ref_state(self) -> _RefState:
        """Strikt tillstånd för hela lagret. Endast under skrivarlåset."""
        log_bytes = self._read_ref_log()
        entries = parse_ref_log(log_bytes)
        refs: dict[str, _RefFile] = {}
        refs_root = self.root / REFS_DIR
        if os.path.lexists(refs_root):
            if refs_root.is_symlink() or not refs_root.is_dir():
                raise RefCorruptError(f"{REFS_DIR}/ är inte en katalog")
            for p in sorted(refs_root.rglob("*")):
                if p.is_symlink():
                    raise RefCorruptError(f"symlänk i {REFS_DIR}/: {p}")
                if p.is_dir():
                    continue
                parts = p.relative_to(refs_root).parts
                if len(parts) != 4 or not parts[3].endswith(".json"):
                    raise RefCorruptError(f"okänd fil i {REFS_DIR}/: {p}")
                try:
                    key = catalog.dataset_key(exchange=parts[0], instrument=parts[1],
                                              symbol=parts[2], timeframe=parts[3][:-5])
                except ValueError as e:
                    raise RefCorruptError(f"okänd ref-sökväg: {p}") from e
                rf = self._read_ref_file(p, key)
                refs[rf.ref.dataset_id] = rf
        for rf in refs.values():
            _current_from(rf, entries, rf.ref.dataset_id)          # refens post måste finnas och stämma
        last: dict[str, _LogEntry] = {}
        for entry in entries:
            last[entry.ref.dataset_id] = entry
        unmirrored = [(did, latest) for did, latest in last.items()
                      if did not in refs or refs[did].ref_log_seq != latest.seq]
        if len(unmirrored) > 1:
            raise RefCorruptError(f"mer än en väntande ref_log-post ({len(unmirrored)} dataset)")
        pending: _LogEntry | None = None
        for did, latest in unmirrored:
            current = refs.get(did)
            behind = [x for x in entries if x.ref.dataset_id == did
                      and x.seq > (current.ref_log_seq if current is not None else 0)]
            if len(behind) != 1 or latest.seq != len(entries) \
                    or latest.previous_data_version != (current.ref.data_version if current is not None else None):
                raise RefCorruptError(f"{latest.ref.key.relpath}: ref-filen och ref_log går inte ihop")
            pending = latest
        self._check_ingests(entries)                     # steg 4: väntande/inkonsistent ingest
        return _RefState(log_bytes, entries, refs, pending)

    # -------------------------------------------------------------- ingest-lineage: läsning (inget lås)
    def _read_ingest_file(self, path: Path, ingest_id: str) -> tuple[IngestRecord, str]:
        if path.is_symlink() or not path.is_file() or os.lstat(path).st_mode & 0o222:
            raise IngestCorruptError(f"{path.name}: inte en skrivskyddad vanlig fil")
        data = path.read_bytes()
        try:
            rec = IngestRecord.from_file_bytes(data, expected_id=ingest_id)
        except (ValueError, TypeError) as e:
            raise IngestCorruptError(f"{path.name}: {e}") from e
        return rec, hashlib.sha256(data).hexdigest()

    def _load_ingests(self) -> tuple[tuple[IngestRecord, str], ...]:
        """Alla ingest-poster, strikt: filnamn = ingest_id, seq 1..n, prev_ingest_id-kedja."""
        root = self.root / INGESTS_DIR
        if not os.path.lexists(root):
            return ()
        if root.is_symlink() or not root.is_dir():
            raise IngestCorruptError(f"{INGESTS_DIR}/ är inte en katalog")
        out: list[tuple[IngestRecord, str]] = []
        for p in sorted(root.iterdir()):
            stem = p.name[:-5] if p.name.endswith(".json") else ""
            if not catalog.is_sha256_hex(stem):
                raise IngestCorruptError(f"okänd fil i {INGESTS_DIR}/: {p.name}")
            out.append(self._read_ingest_file(p, stem))
        out.sort(key=lambda item: item[0].seq)
        prev: str | None = None
        for i, (rec, _) in enumerate(out, start=1):
            if rec.seq != i:
                raise IngestCorruptError(f"ingest-poster: seq {rec.seq} på plats {i}")
            if rec.prev_ingest_id != prev:
                raise IngestCorruptError(f"ingest-post seq {i}: kedjan (prev_ingest_id) är bruten")
            prev = rec.ingest_id
        return tuple(out)

    def read_ingest(self, ingest_id: str) -> IngestRecord:
        """En ingest-post (strikt verifierad mot sitt id)."""
        if not catalog.is_sha256_hex(ingest_id):
            raise ValueError(f"ogiltigt ingest_id: {ingest_id!r}")
        path = self.root / INGESTS_DIR / f"{ingest_id}.json"
        if not os.path.lexists(path):
            raise DatasetNotFoundError(f"ingest-post saknas: {ingest_id}")
        return self._read_ingest_file(path, ingest_id)[0]

    def ingest_history(self, key: DatasetKey | None = None) -> tuple[IngestRecord, ...]:
        """Hela den verifierade ingest-kedjan, ev. filtrerad på dataset."""
        records = tuple(rec for rec, _ in self._load_ingests())
        if key is None:
            return records
        did = catalog.dataset_id(key)
        return tuple(rec for rec in records if rec.ref.dataset_id == did)

    @contextmanager
    def _permit_pending_ingest(self) -> Iterator[None]:
        """Endast för complete_pending_ingest(): läs tillståndet trots en väntande ingest."""
        self._pending_ingest_permitted = True
        try:
            yield
        finally:
            self._pending_ingest_permitted = False

    def _check_ingests(self, entries: tuple[_LogEntry, ...]) -> None:
        """Anropas från _load_ref_state. No-op utan ingest-poster. Varje ref_log-post med
        action=ingest hör till exakt en post med ref_outcome=changed och tvärtom; en
        väntande ingest blockerar (PendingIngestError)."""
        records = self._load_ingests()
        if not records:
            return
        linked: set[int] = set()
        for i, (rec, _) in enumerate(records):
            if rec.ref_outcome != "changed":
                continue
            s = rec.ref_log_seq or 0
            if s <= len(entries):
                e = entries[s - 1]
                if e.entry_sha256 != rec.ref_log_entry_sha256 or e.action != "ingest" or e.ref != rec.ref \
                        or e.previous_data_version != rec.previous_data_version:
                    raise IngestCorruptError(f"ingest-post seq {rec.seq}: ref_log seq {s} matchar inte")
                linked.add(s)
            elif i == len(records) - 1 and s == len(entries) + 1:
                if not getattr(self, "_pending_ingest_permitted", False):
                    raise PendingIngestError(f"väntande ingest (seq {rec.seq}); "
                                             "anropa complete_pending_ingest() först")
            else:
                raise IngestCorruptError(f"ingest-post seq {rec.seq}: ref_log seq {s} saknas")
        orphans = [e.seq for e in entries if e.action == "ingest" and e.seq not in linked]
        if orphans:
            raise IngestCorruptError(f"ref_log-poster med action=ingest utan ingest-post: {orphans}")

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

    def migrate_catalog(self, *, apply: bool = False) -> catalog.MigrationResult:
        """Explicit katalogmigrering (SB-S4-MIG), dry-run som standard. apply=True under skrivarlåset;
        objekt, refs och alla andra filer rörs aldrig, bara catalog.sqlite (och dess journal)."""
        if not apply:
            return catalog.migrate_catalog(self.catalog_path)
        lock = _WriterLock(self.root / LOCK_FILE)
        lock.acquire()
        try:
            return catalog.migrate_catalog(self.catalog_path, apply=True)
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

    # -------------------------------------------------------------- refs (steg 3)
    def set_current(self, key: DatasetKey, data_version: str) -> RefUpdate:
        """Flyttar datasetets ref till en fullt verifierad version (även en äldre).
        Samma version som nu -> ingen ändring och ingen ref_log-post."""
        return self._move_ref(key, data_version, "set_current")

    def _move_ref(self, key: DatasetKey, data_version: str, action: str) -> RefUpdate:
        self._require_lock()
        if action not in REF_ACTIONS:
            raise ValueError(f"okänd ref-action: {action!r}")
        store = self._store
        state = store._load_ref_state()
        if state.pending is not None:
            raise PendingRefUpdateError(
                f"väntande ref-uppdatering (ref_log seq {state.pending.seq}); "
                "anropa complete_pending_ref_updates() först")
        info, manifest, _ = store._verify_object(key, data_version)    # 1) aldrig overifierat mål
        self._register(info, manifest)
        target = DatasetVersionRef(key, data_version)
        cur = state.refs.get(target.dataset_id)
        if cur is not None and cur.ref.data_version == data_version:
            self._sync_ref_catalog(state)
            return RefUpdate(target, False, data_version, cur.ref_log_seq)
        previous = cur.ref.data_version if cur is not None else None
        line, entry = self._next_entry(state, target, previous, action)
        self._append_ref_log(state, line, entry)                         # 2) historik först
        self._write_ref_file(target, entry)                              # 3) sedan ref
        self._sync_ref_catalog(store._load_ref_state())                  # 4) katalog sist
        return RefUpdate(target, True, previous, entry.seq)

    def complete_pending_ref_updates(self) -> list[RefUpdate]:
        """Slutför EN väntande ref_log-post (krasch mellan logg och ref), explicit.
        Kräver: datasetets sista post, previous_data_version == ref-filens version
        (eller båda saknas) och att målobjektet klarar full verifiering nu."""
        self._require_lock()
        store = self._store
        state = store._load_ref_state()          # strikt: >1 väntande eller inkonsistens -> fel
        e = state.pending
        if e is None:
            return []
        cur = state.refs.get(e.ref.dataset_id)
        last_for_dataset = [x for x in state.entries if x.ref.dataset_id == e.ref.dataset_id][-1]
        if last_for_dataset != e:
            raise RefCorruptError(f"ref_log seq {e.seq} är inte datasetets sista post")
        if e.previous_data_version != (cur.ref.data_version if cur is not None else None):
            raise RefCorruptError(f"ref_log seq {e.seq}: previous_data_version matchar inte ref-filen")
        info, manifest, _ = store._verify_object(e.ref.key, e.ref.data_version)
        self._register(info, manifest)
        self._write_ref_file(e.ref, e)
        self._sync_ref_catalog(store._load_ref_state())
        if e.action == "ingest":                         # steg 4-fix: ingest-postens katalograd utan fördröjning
            self._sync_ingest_catalog(store._load_ingests())
        return [RefUpdate(e.ref, True, e.previous_data_version, e.seq)]

    @staticmethod
    def _next_entry(state: _RefState, target: DatasetVersionRef, previous: str | None,
                    action: str) -> tuple[bytes, _LogEntry]:
        seq = len(state.entries) + 1
        prev_sha = state.entries[-1].entry_sha256 if state.entries else None
        line = compact_json_bytes({"seq": seq, "prev_entry_sha256": prev_sha, **target.to_json_obj(),
                                   "previous_data_version": previous, "action": action})
        return line, _LogEntry(seq, hashlib.sha256(line).hexdigest(), prev_sha, target, previous, action)

    def _append_ref_log(self, state: _RefState, line: bytes, entry: _LogEntry) -> None:
        """Append-only: ny fil = exakt de gamla bytena + en rad, atomiskt ersatt."""
        store = self._store
        current = store._read_ref_log()
        if current != state.log_bytes:
            raise RefCorruptError(f"{REF_LOG_FILE} ändrades under skrivningen")
        new = current + line + b"\n"
        if parse_ref_log(new)[-1] != entry:
            raise RefCorruptError(f"{REF_LOG_FILE}: ny post går inte att verifiera")
        self._replace_file(store.root / REF_LOG_FILE, new)

    def _write_ref_file(self, ref: DatasetVersionRef, entry: _LogEntry) -> None:
        path = self._store._ref_path(ref.key)
        _mkdirs_durable(path.parent, self._store.root)
        self._replace_file(path, _ref_file_bytes(ref, entry))

    def _replace_file(self, path: Path, data: bytes) -> None:
        """tmp (0444) -> fsync -> atomisk rename -> fsync av katalogen."""
        staged = self._store.tmp / f"{uuid.uuid4().hex}.{path.name}"
        try:
            with staged.open("xb") as f:
                f.write(data)
            os.chmod(staged, FILE_MODE)
            _fsync_file(staged)
            _rename(staged, path)
        finally:
            staged.unlink(missing_ok=True)
        _fsync_dir(path.parent)

    def _sync_ref_catalog(self, state: _RefState) -> None:
        """Katalogen (härledd) synkas från ref-filerna och ref_log i EN transaktion."""
        conn = self._conn
        log_cols = ("seq", "entry_sha256", "prev_entry_sha256", "dataset_id", "data_version",
                    "previous_data_version", "action")
        ref_cols = ("dataset_id", "data_version", "ref_log_seq", "ref_file_sha256")
        with catalog.transaction(conn):
            have = {r[0]: tuple(r) for r in conn.execute(f"SELECT {', '.join(log_cols)} FROM ref_log")}
            for e in state.entries:
                row = (e.seq, e.entry_sha256, e.prev_entry_sha256, e.ref.dataset_id,
                       e.ref.data_version, e.previous_data_version, e.action)
                if e.seq in have:
                    if have[e.seq] != row:
                        raise catalog.CatalogCorruptError(f"ref_log seq {e.seq}: katalogen avviker")
                    continue
                conn.execute(f"INSERT INTO ref_log ({', '.join(log_cols)}) "
                             f"VALUES ({', '.join(':' + c for c in log_cols)})",
                             dict(zip(log_cols, row, strict=True)))
            if set(have) - {e.seq for e in state.entries}:
                raise catalog.CatalogCorruptError("katalogen har ref_log-poster som saknas i filen")
            cat_refs = {r[0]: tuple(r) for r in conn.execute(f"SELECT {', '.join(ref_cols)} FROM refs")}
            for did, rf in state.refs.items():
                ref_row = (did, rf.ref.data_version, rf.ref_log_seq, rf.file_sha256)
                old = cat_refs.get(did)
                if old is None:
                    conn.execute(f"INSERT INTO refs ({', '.join(ref_cols)}) "
                                 f"VALUES ({', '.join(':' + c for c in ref_cols)})",
                                 dict(zip(ref_cols, ref_row, strict=True)))
                elif old != ref_row:
                    if old[2] >= rf.ref_log_seq:
                        raise catalog.CatalogCorruptError(f"refs {did}: katalogen avviker")
                    conn.execute("UPDATE refs SET data_version = :data_version, ref_log_seq = :ref_log_seq, "
                                 "ref_file_sha256 = :ref_file_sha256 WHERE dataset_id = :dataset_id",
                                 dict(zip(ref_cols, ref_row, strict=True)))
            if set(cat_refs) - set(state.refs):
                raise catalog.CatalogCorruptError("katalogen har refs som saknas i filerna")

    # -------------------------------------------------------------- snapshots (steg 3)
    def create_snapshot(self, members: DataSnapshot | list[DatasetVersionRef] | tuple[DatasetVersionRef, ...],
                        funding: tuple[FundingVersionRef, ...] = ()) -> SnapshotResult:
        """Pinnar exakt dessa versioner (alla verifieras fullt). Samma medlemmar -> samma id.
        funding (eller en DataSnapshot med funding) ger en snapshot v2 (refs.py)."""
        self._require_lock()
        if isinstance(members, DataSnapshot) and funding:
            raise ValueError("funding anges i DataSnapshot, inte separat")
        store = self._store
        if store._load_ref_state().pending is not None:
            raise PendingRefUpdateError("väntande ref-uppdatering; anropa complete_pending_ref_updates() först")
        snap = members if isinstance(members, DataSnapshot) else DataSnapshot.of(members, funding)
        for m in snap.members:
            info, manifest, _ = store._verify_object(m.key, m.data_version)
            self._register(info, manifest)
        for f in snap.funding:                           # v2: varje funding-version verifieras fullt
            finfo, fmanifest, _ = store._verify_funding_object(f.key, f.data_version)
            self._register_funding(finfo, fmanifest)
        return self._publish_snapshot(snap)

    def snapshot_current(self, keys: list[DatasetKey] | tuple[DatasetKey, ...],
                         funding: tuple[FundingVersionRef, ...] = ()) -> SnapshotResult:
        """Pinnar datasetens AKTUELLA versioner nu; följer aldrig senare ref-ändringar.
        Funding har inga refs: funding-versioner anges explicit (snapshot v2)."""
        self._require_lock()
        state = self._store._load_ref_state()
        if state.pending is not None:
            raise PendingRefUpdateError("väntande ref-uppdatering; anropa complete_pending_ref_updates() först")
        members = []
        for key in keys:
            rf = state.refs.get(catalog.dataset_id(key))
            if rf is None:
                raise NoCurrentRefError(f"ingen aktuell ref för {key.relpath}")
            members.append(rf.ref)
        return self.create_snapshot(members, funding)

    def _publish_snapshot(self, snap: DataSnapshot) -> SnapshotResult:
        store = self._store
        path = store.root / SNAPSHOTS_DIR / f"{snap.snapshot_id}.json"
        data = snap.to_file_bytes()
        sha = hashlib.sha256(data).hexdigest()
        if os.path.lexists(path):
            if store.read_snapshot(snap.snapshot_id) != snap or path.read_bytes() != data:
                raise SnapshotCorruptError(f"{path.name}: befintlig fil avviker")
            self._register_snapshot(snap, sha)
            return SnapshotResult(snap, "reused")
        _mkdirs_durable(path.parent, store.root)
        staged = store.tmp / f"{uuid.uuid4().hex}.snapshot.json"
        try:
            with staged.open("xb") as f:
                f.write(data)
            os.chmod(staged, FILE_MODE)
            _fsync_file(staged)
            try:
                _link(staged, path)
            except FileExistsError as e:
                raise ImmutableObjectError(f"{path.name}: snapshoten finns redan; skrivs aldrig över") from e
        finally:
            staged.unlink(missing_ok=True)
        _fsync_dir(path.parent)
        self._register_snapshot(snap, sha)
        return SnapshotResult(snap, "created")

    def _register_snapshot(self, snap: DataSnapshot, file_sha256: str) -> None:
        conn = self._conn
        sid = snap.snapshot_id
        members = [(sid, m.dataset_id, m.data_version) for m in snap.members]
        funding = [(sid, f.dataset_id, f.data_version) for f in snap.funding]     # v2 (SB-S4-a A2)
        with catalog.transaction(conn):
            row = conn.execute("SELECT snapshot_file_sha256, member_count FROM snapshots "
                               "WHERE snapshot_id = :sid", {"sid": sid}).fetchone()
            if row is None:
                conn.execute("INSERT INTO snapshots (snapshot_id, snapshot_file_sha256, member_count) "
                             "VALUES (:sid, :sha, :n)", {"sid": sid, "sha": file_sha256, "n": len(members)})
                conn.executemany("INSERT INTO snapshot_members (snapshot_id, dataset_id, data_version) "
                                 "VALUES (?, ?, ?)", members)
                conn.executemany("INSERT INTO snapshot_funding_members (snapshot_id, dataset_id, data_version) "
                                 "VALUES (?, ?, ?)", funding)
                return
            have = [tuple(r) for r in conn.execute(
                "SELECT snapshot_id, dataset_id, data_version FROM snapshot_members "
                "WHERE snapshot_id = :sid ORDER BY dataset_id", {"sid": sid})]
            have_funding = [tuple(r) for r in conn.execute(
                "SELECT snapshot_id, dataset_id, data_version FROM snapshot_funding_members "
                "WHERE snapshot_id = :sid ORDER BY dataset_id", {"sid": sid})]
            if tuple(row) != (file_sha256, len(members)) or have != members or have_funding != funding:
                raise catalog.CatalogCorruptError(f"snapshot {sid}: katalogen avviker")

    # -------------------------------------------------------------- ingest (steg 4)
    def ingest(self, key: DatasetKey, files: list[str | Path] | tuple[str | Path, ...], *,
               now: datetime, code_version: str, worktree_clean: bool | None,
               fail_on_gaps: bool = False, allow_regression: bool = False) -> IngestOutcome:
        """Phase 2-ingest (oförändrad) -> oföränderlig version + lineage-post -> ref
        (action="ingest"). Varje anrop ger en ny post, även för identiskt innehåll."""
        self._require_lock()
        store = self._store
        now = require_utc(now, "now")
        if not isinstance(code_version, str) or not code_version:
            raise ValueError("code_version måste anges (icke-tom sträng)")
        if worktree_clean is not None and not isinstance(worktree_clean, bool):
            raise ValueError("worktree_clean måste vara bool eller None")
        state = store._load_ref_state()                  # väntande ingest -> PendingIngestError
        if state.pending is not None:
            raise PendingRefUpdateError("väntande ref-uppdatering; anropa complete_pending_ref_updates() först")
        ingests = store._load_ingests()
        # 1) Phase 2 oförändrad: parsning, filnamn, dedup, validering, gap-flaggor (skriver inget)
        capture = _CaptureStore(key)
        ingest_dataset(list(files), key.instrument, key.timeframe, capture, symbol=key.symbol,
                       now=now, fail_on_gaps=fail_on_gaps)
        if capture.captured is None:
            raise StoreError("Phase 2 ingest lämnade inget dataset till fångstlagringen")
        frame, extra = capture.captured
        # 2) innehållsnycklar -> objektet, källor -> lineage (okänd nyckel -> ValueError)
        content, sources, dropped = split_ingest_extra(extra)
        check_content_extra(content)
        canon = canonicalize(frame)
        dv = content_hash(canon)
        target = DatasetVersionRef(key, dv)
        cur = state.refs.get(target.dataset_id)
        previous = cur.ref.data_version if cur is not None else None
        # regressionsskydd, före all skrivning
        if previous is not None and previous != dv and not allow_regression:
            current_info = store._verify_object(key, previous)[0]
            new_last = canon["open_time"].iloc[-1].isoformat()
            if _iso_to_ms(new_last) < _iso_to_ms(current_info.last_open_time):
                raise VersionRegressionError(
                    f"{key.relpath}: ny version slutar {new_last}, aktuell slutar "
                    f"{current_info.last_open_time}; ange allow_regression=True om det är avsikten")
        # 3) objektet: publicera, eller verifiera fullt och återanvänd
        publication = self._publish_ingest_version(key, canon, dv, content)
        # 4) planerad ref_log-rad (skrivs inte än)
        line: bytes | None = None
        entry: _LogEntry | None = None
        if previous != dv:
            line, entry = self._next_entry(state, target, previous, "ingest")
        rec = IngestRecord(
            seq=len(ingests) + 1, prev_ingest_id=ingests[-1][0].ingest_id if ingests else None,
            ref=target, publication=publication,
            ref_outcome="changed" if entry is not None else "unchanged",
            previous_data_version=previous if entry is not None else dv,
            ref_log_seq=entry.seq if entry is not None else None,
            ref_log_entry_sha256=entry.entry_sha256 if entry is not None else None,
            sources=sources, duplicates_dropped=dropped, now=now.isoformat(),
            fail_on_gaps=fail_on_gaps, allow_regression=allow_regression,
            code_version=code_version, worktree_clean=worktree_clean)
        # 5) lineage-posten (commit-punkt för ingesten)
        record_sha = self._publish_ingest_record(rec)
        # 6) ref via steg 3 (logg före ref)
        if line is not None and entry is not None:
            self._append_ref_log(state, line, entry)
            self._write_ref_file(target, entry)
        # 7) katalog sist
        self._sync_ref_catalog(store._load_ref_state())
        self._sync_ingest_catalog(store._load_ingests())
        return IngestOutcome(rec, rec.ingest_id, record_sha)

    def complete_pending_ingest(self) -> IngestOutcome | None:
        """Slutför EN väntande ingest (krasch efter ingest-posten, före ref_log-raden), explicit.
        Kräver: sista posten, planerad ref_log-rad reproduceras exakt, previous_data_version
        == ref-filens version (eller båda saknas) och objektet klarar full verifiering nu."""
        self._require_lock()
        store = self._store
        with store._permit_pending_ingest():
            state = store._load_ref_state()
        if state.pending is not None:
            raise PendingRefUpdateError("väntande ref-uppdatering; anropa complete_pending_ref_updates() först")
        records = store._load_ingests()
        if not records:
            return None
        rec, record_sha = records[-1]
        if rec.ref_outcome != "changed" or (rec.ref_log_seq or 0) <= len(state.entries):
            return None
        cur = state.refs.get(rec.ref.dataset_id)
        if rec.previous_data_version != (cur.ref.data_version if cur is not None else None):
            raise IngestCorruptError(f"ingest seq {rec.seq}: previous_data_version matchar inte ref-filen")
        line, entry = self._next_entry(state, rec.ref, rec.previous_data_version, "ingest")
        if entry.seq != rec.ref_log_seq or entry.entry_sha256 != rec.ref_log_entry_sha256:
            raise IngestCorruptError(f"ingest seq {rec.seq}: planerad ref_log-rad kan inte reproduceras")
        info, manifest, _ = store._verify_object(rec.ref.key, rec.ref.data_version)
        self._register(info, manifest)
        self._append_ref_log(state, line, entry)
        self._write_ref_file(rec.ref, entry)
        self._sync_ref_catalog(store._load_ref_state())
        self._sync_ingest_catalog(store._load_ingests())
        return IngestOutcome(rec, rec.ingest_id, record_sha)

    def _publish_ingest_version(self, key: DatasetKey, canon: pd.DataFrame, dv: str,
                                content: dict[str, Any]) -> Literal["published", "reused"]:
        """Ingest-vägen: som publish_version men vid återbruk jämförs ENDAST innehållsnycklarna
        (P4-S4-01). Den publika publish_version är oförändrad (strikt M2)."""
        check_content_extra(content)
        store = self._store
        target = store.object_dir(key, dv)
        if os.path.lexists(target):
            info, manifest, _ = store._verify_object(key, dv)   # ingest: aldrig blint återbruk
            mine = json.loads(json.dumps({k: content[k] for k in sorted(CONTENT_DERIVED_KEYS)}))
            stored = {k: manifest.get(k) for k in sorted(CONTENT_DERIVED_KEYS)}
            if stored != mine:
                raise ObjectCorruptError(f"{key.relpath}/{dv}: lagrade innehållsnycklar avviker från innehållet")
            self._finish_dir(target)
            self._register(info, manifest)
            return "reused"
        staging = self._new_staging()
        try:
            staged = self._stage(staging, key, canon, dv, content)
            self._publish_dir(staged, target)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
        info, manifest, _ = store._verify_object(key, dv)
        self._register(info, manifest)
        return "published"

    def _publish_ingest_record(self, rec: IngestRecord) -> str:
        """tmp (0444) -> fsync -> link (aldrig överskrivning) -> fsync av katalogen."""
        store = self._store
        path = store.root / INGESTS_DIR / f"{rec.ingest_id}.json"
        data = rec.to_file_bytes()
        if os.path.lexists(path):
            raise ImmutableObjectError(f"{path.name}: ingest-posten finns redan; skrivs aldrig över")
        _mkdirs_durable(path.parent, store.root)
        staged = store.tmp / f"{uuid.uuid4().hex}.ingest.json"
        try:
            with staged.open("xb") as f:
                f.write(data)
            os.chmod(staged, FILE_MODE)
            _fsync_file(staged)
            try:
                _link(staged, path)                               # ingest-post: aldrig överskrivning
            except FileExistsError as e:
                raise ImmutableObjectError(f"{path.name}: ingest-posten finns redan; skrivs aldrig över") from e
        finally:
            staged.unlink(missing_ok=True)
        _fsync_dir(path.parent)
        return hashlib.sha256(data).hexdigest()

    def _sync_ingest_catalog(self, records: tuple[tuple[IngestRecord, str], ...]) -> None:
        """ingest_events/ingest_sources synkas från posterna i EN transaktion. En väntande
        ingest (ref_log-raden saknas ännu) synkas inte."""
        conn = self._conn
        n_entries = len(parse_ref_log(self._store._read_ref_log()))
        ev_cols = ("seq", "ingest_id", "record_sha256", "dataset_id", "data_version",
                   "previous_data_version", "params_json", "code_version", "worktree_clean",
                   "publication", "ref_outcome", "ref_log_seq", "duplicates_dropped")
        src_cols = ("ingest_seq", "source_index", "name", "sha256", "rows")
        with catalog.transaction(conn):
            have = {r[0]: tuple(r) for r in conn.execute(f"SELECT {', '.join(ev_cols)} FROM ingest_events")}
            have_src: dict[int, list[tuple[Any, ...]]] = {}
            for r in conn.execute(f"SELECT {', '.join(src_cols)} FROM ingest_sources "
                                  "ORDER BY ingest_seq, source_index"):
                have_src.setdefault(r[0], []).append(tuple(r))
            synced: set[int] = set()
            for rec, sha in records:
                if rec.ref_log_seq is not None and rec.ref_log_seq > n_entries:
                    continue                                      # väntande ingest
                row = (rec.seq, rec.ingest_id, sha, rec.ref.dataset_id, rec.ref.data_version,
                       rec.previous_data_version, compact_json_bytes(rec.params).decode("ascii"),
                       rec.code_version, None if rec.worktree_clean is None else int(rec.worktree_clean),
                       rec.publication, rec.ref_outcome, rec.ref_log_seq, rec.duplicates_dropped)
                srcs = [(rec.seq, i, s.name, s.sha256, s.rows) for i, s in enumerate(rec.sources)]
                synced.add(rec.seq)
                if rec.seq in have:
                    if have[rec.seq] != row or have_src.get(rec.seq, []) != srcs:
                        raise catalog.CatalogCorruptError(f"ingest seq {rec.seq}: katalogen avviker")
                    continue
                conn.execute(f"INSERT INTO ingest_events ({', '.join(ev_cols)}) "
                             f"VALUES ({', '.join(':' + c for c in ev_cols)})",
                             dict(zip(ev_cols, row, strict=True)))
                conn.executemany(f"INSERT INTO ingest_sources ({', '.join(src_cols)}) "
                                 f"VALUES ({', '.join(':' + c for c in src_cols)})",
                                 [dict(zip(src_cols, r, strict=True)) for r in srcs])
            if set(have) - synced:
                raise catalog.CatalogCorruptError("katalogen har ingest_events som saknas i filerna")

    # -------------------------------------------------------------- funding (etapp B steg 4)
    def publish_funding(self, key: fd.FundingKey, files: list[str | Path] | tuple[str | Path, ...]
                        ) -> FundingPublishResult:
        """Funding-filerna -> oföränderlig version (eller full verifiering och återbruk) + katalograd.
        Inga refs, ingen ref_log, ingen ingest-post (SB-S4-a A2); pinnas via create_snapshot."""
        self._require_lock()
        frame, sources, dropped = fd.build_funding_frame(list(files), key)      # läser bara indata
        dv = fd.funding_content_hash(frame)
        manifest = fd.funding_manifest(key, frame, sources, dropped)
        store = self._store
        target = store.funding_object_dir(key, dv)
        if os.path.lexists(target):
            info, stored, _ = store._verify_funding_object(key, dv)          # aldrig blint återbruk
            if stored != json.loads(json.dumps(manifest)):
                raise ProvenanceConflictError(
                    f"{key.relpath}/{dv}: finns redan med andra källor; det lagrade manifestet skrivs aldrig om")
            self._finish_dir(target)
            self._register_funding(info, stored)
            return FundingPublishResult(info, "reused", dropped)
        staging = self._new_staging()
        try:
            staged = self._stage_funding(staging, key, frame, dv, manifest)
            self._publish_dir(staged, target)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
        info, stored, _ = store._verify_funding_object(key, dv)
        self._register_funding(info, stored)
        return FundingPublishResult(info, "published", dropped)

    def _stage_funding(self, staging: Path, key: fd.FundingKey, frame: pd.DataFrame, dv: str,
                       manifest: dict[str, Any]) -> Path:
        obj = staging / "object"
        obj.mkdir()
        fd.FundingParquet.write(obj / FUNDING_DATA_FILE, frame)
        mbytes = canonical_json_bytes(manifest)
        with (obj / MANIFEST_FILE).open("xb") as f:
            f.write(mbytes)
        version = {
            "format": FUNDING_VERSION_FORMAT,
            "dataset_id": catalog.funding_dataset_id(key),
            "dataset": json.loads(catalog.funding_dataset_key_json(key)),
            "data_version": dv,
            "data_file": {"path": FUNDING_DATA_FILE, "sha256": sha256_file(obj / FUNDING_DATA_FILE),
                          "bytes": (obj / FUNDING_DATA_FILE).stat().st_size},
            "manifest": {"path": MANIFEST_FILE, "sha256": hashlib.sha256(mbytes).hexdigest(),
                         "bytes": len(mbytes)},
            "rows": manifest["rows"],
            "first_calc_time": manifest["first_calc_time"],
            "last_calc_time": manifest["last_calc_time"],
        }
        with (obj / VERSION_FILE).open("xb") as f:
            f.write(canonical_json_bytes(version))
        for fname in sorted(FUNDING_OBJECT_FILES):
            os.chmod(obj / fname, FILE_MODE)
            _fsync_file(obj / fname)
        _fsync_dir(obj)
        return obj

    def _register_funding(self, info: FundingVersionInfo, manifest: dict[str, Any]) -> None:
        """Katalograder för ett verifierat, durabelt funding-objekt (idempotent, EN transaktion)."""
        row = funding_version_row(info, manifest)
        sources = funding_source_rows(info, manifest)
        ident = {"dataset_id": info.dataset_id, "data_version": info.data_version}
        fv_cols, fvs_cols = ", ".join(_FV_COLUMNS), ", ".join(_FVS_COLUMNS)
        conn = self._conn
        with catalog.transaction(conn):
            if catalog.ensure_funding_dataset(conn, info.key) != info.dataset_id:
                raise catalog.CatalogCorruptError(f"{info.key.relpath}: dataset_id avviker")
            existing = conn.execute(
                f"SELECT {fv_cols} FROM funding_versions "
                "WHERE dataset_id = :dataset_id AND data_version = :data_version", ident).fetchone()
            if existing is None:
                conn.execute(f"INSERT INTO funding_versions ({fv_cols}) "
                             f"VALUES ({', '.join(':' + c for c in _FV_COLUMNS)})",
                             dict(zip(_FV_COLUMNS, row, strict=True)))
                conn.executemany(f"INSERT INTO funding_version_sources ({fvs_cols}) "
                                 f"VALUES ({', '.join(':' + c for c in _FVS_COLUMNS)})",
                                 [dict(zip(_FVS_COLUMNS, r, strict=True)) for r in sources])
                return
            have = [tuple(r) for r in conn.execute(
                f"SELECT {fvs_cols} FROM funding_version_sources "
                "WHERE dataset_id = :dataset_id AND data_version = :data_version ORDER BY source_index", ident)]
            if tuple(existing) != row or have != sources:
                raise catalog.CatalogCorruptError(
                    f"{info.key.relpath}/{info.data_version}: katalograden matchar inte objektet")


class PinnedStore:
    """Read-only vy över EXAKT en snapshots versioner. Följer aldrig refs, öppnar
    aldrig ref-filer/ref_log, och alla skrivningar ger ReadOnlyStoreError."""

    def __init__(self, store: VersionedStore, snapshot: DataSnapshot) -> None:
        # Bara en publicerad (durabel, reproducerbar) snapshot får användas.
        published = store.read_snapshot(snapshot.snapshot_id)   # SnapshotNotFoundError om opublicerad
        if published != snapshot:
            raise SnapshotCorruptError(f"snapshot {snapshot.snapshot_id}: publicerad fil avviker")
        self._store = store
        self._snapshot = snapshot
        self._pins = MappingProxyType({m.dataset_id: m for m in snapshot.members})
        self._funding_pins = MappingProxyType({f.dataset_id: f for f in snapshot.funding})

    @property
    def snapshot(self) -> DataSnapshot:
        return self._snapshot

    @property
    def snapshot_id(self) -> str:
        return self._snapshot.snapshot_id

    @property
    def members(self) -> tuple[DatasetVersionRef, ...]:
        return self._snapshot.members

    def version_of(self, key: DatasetKey) -> DatasetVersionRef:
        pin = self._pins.get(catalog.dataset_id(key))
        if pin is None:
            raise DatasetNotInSnapshotError(f"{key.relpath} ingår inte i snapshot {self.snapshot_id}")
        return pin

    def read(self, key: DatasetKey, *, as_of: datetime | None = None
             ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Den pinnade versionen (full verifiering); as_of -> available_at <= as_of."""
        pin = self.version_of(key)
        return self._store.read_version(pin.key, pin.data_version, as_of=as_of)

    def read_range(self, key: DatasetKey, start: datetime, end: datetime, *,
                   as_of: datetime | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Den pinnade versionen, [start, end) med Phase 2-semantik (full verifiering)."""
        pin = self.version_of(key)
        return self._store.read_range(pin.key, pin.data_version, start, end, as_of=as_of)

    @property
    def funding(self) -> tuple[FundingVersionRef, ...]:
        return self._snapshot.funding

    def funding_version_of(self, key: fd.FundingKey) -> FundingVersionRef:
        pin = self._funding_pins.get(catalog.funding_dataset_id(key))
        if pin is None:
            raise DatasetNotInSnapshotError(f"funding-dataset {key.relpath} ingår inte i snapshot {self.snapshot_id}")
        return pin

    def read_funding(self, key: fd.FundingKey, *, as_of: datetime | None = None
                     ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Den pinnade funding-versionen (full verifiering); as_of -> available_at <= as_of."""
        pin = self.funding_version_of(key)
        return self._store.read_funding_version(pin.key, pin.data_version, as_of=as_of)

    def read_funding_range(self, key: fd.FundingKey, start: datetime, end: datetime, *,
                           as_of: datetime | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Den pinnade funding-versionen, [start, end) på calc_time (full verifiering)."""
        pin = self.funding_version_of(key)
        return self._store.read_funding_range(pin.key, pin.data_version, start, end, as_of=as_of)

    def _read_only(self, *args: Any, **kwargs: Any) -> None:
        raise ReadOnlyStoreError(f"PinnedStore {self.snapshot_id} är read-only")

    publish_version = _read_only
    set_current = _read_only
    create_snapshot = _read_only
    snapshot_current = _read_only
    complete_pending_ref_updates = _read_only
    writer = _read_only
    publish_funding = _read_only


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


def _parse_funding_sources(sources: Any) -> list[dict[str, Any]]:
    """Funding-manifestets källor: icke-tom lista, exakt nycklarna, sorterade på (namn, sha256)."""
    if not isinstance(sources, list) or not sources:
        raise InvalidSourcesError("funding-manifestets 'sources' måste vara en icke-tom lista")
    for s in sources:
        if not (isinstance(s, dict) and set(s) == {"name", "sha256", "rows"}
                and isinstance(s["name"], str) and s["name"] and catalog.is_sha256_hex(s["sha256"])
                and _is_int(s["rows"]) and s["rows"] >= 1):
            raise InvalidSourcesError(f"ogiltig källa i funding-manifestet: {s!r}")
    keys = [(s["name"], s["sha256"]) for s in sources]
    if keys != sorted(keys) or len({s["name"] for s in sources}) != len(sources):
        raise InvalidSourcesError("funding-manifestets källor måste vara unika och sorterade")
    return sources


def funding_version_row(info: FundingVersionInfo, manifest: dict[str, Any]) -> tuple[Any, ...]:
    """funding_versions-raden (samma i _register_funding och i integrity)."""
    return (info.dataset_id, info.data_version, info.data_file_sha256, info.data_file_bytes,
            info.manifest_sha256, info.version_sha256, int(manifest["schema_version"]), info.rows,
            _iso_to_ms(info.first_calc_time), _iso_to_ms(info.last_calc_time),
            int(manifest["duplicates_dropped"]))


def funding_source_rows(info: FundingVersionInfo, manifest: dict[str, Any]) -> list[tuple[Any, ...]]:
    return [(info.dataset_id, info.data_version, i, s["name"], s["sha256"], s["rows"])
            for i, s in enumerate(_parse_funding_sources(manifest["sources"]))]
