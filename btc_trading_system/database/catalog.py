"""SQLite-katalog (PHASE 4, steg 1): schema, migreringar, deterministiska dataset-ID.

Katalogen är HÄRLEDD och återuppbyggbar. De oföränderliga filobjekten (Parquet +
manifest + metadata) är lagringens facit; katalogen får aldrig vara enda källan
till någon uppgift och kan alltid byggas om (`rebuild_catalog`).

IDENTITETER (hålls isär, blandas aldrig ihop)
  * dataset_id       = sha256(domäntagg + kanonisk JSON {exchange, instrument, symbol, timeframe})
  * datasetversion   = (dataset_id, data_version)
  * data_version     = Phase 2:s logiska innehållshash (storage.content_hash), OFÖRÄNDRAD
  * data_file_sha256 = sha256 av den fysiska Parquet-filen
  * manifest_sha256  = sha256 av manifestets bytes

SÄKERHET
  * STRICT-tabeller. Kräver SQLite >= 3.37; äldre avvisas hellre än att
    typkontrollen tyst saknas (schemat ska vara identiskt överallt).
  * foreign_keys=ON på varje anslutning (verifieras, sätts inte bara).
  * journal_mode=DELETE (rollback-journal, aldrig WAL), synchronous=FULL.
  * user_version + schema_migrations (sha256 av varje migrerings-SQL): nyare
    schema avvisas, manipulerad/partiell migrering avvisas.
  * Migrering sker ALDRIG automatiskt när katalogen öppnas (inte heller skrivbart):
    en katalog med äldre schema vägras (CatalogMigrationRequiredError) och migreras
    bara explicit med migrate_catalog() (SB-S4-MIG, etapp B steg 4).
  * Faktiskt schema jämförs mot förväntat (sqlite_schema) vid varje öppning.
  * Alla tabeller utom refs är append-only (triggers blockerar UPDATE/DELETE);
    refs får bara uppdateras, aldrig raderas.
  * Ny katalog och rebuild byggs i en temporär fil, kontrolleras fullt och
    publiceras atomiskt (link/os.replace + fsync av fil och katalog).
  * Ingen väggklocka i katalogen: dump är deterministisk givet innehållet.
"""
from __future__ import annotations

import functools
import hashlib
import json
import os
import sqlite3
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from btc_trading_system.data.funding import FUNDING_KIND, FundingKey
from btc_trading_system.data.schema import EXCHANGE, DatasetKey
from btc_trading_system.data_validation.errors import DataError

CATALOG_SCHEMA_VERSION = 2
MIN_SQLITE_VERSION = (3, 37, 0)                      # STRICT-tabeller
DATASET_ID_DOMAIN = b"btc-quant/dataset-id/v1\x00"
FUNDING_DATASET_ID_DOMAIN = b"btc-quant/funding-dataset-id/v1\x00"
_SQLITE_HEADER = b"SQLite format 3\x00"

# Låsta i schemat (CHECK). Test säkerställer att de matchar Phase 1-3-konstanterna.
SCHEMA_TIMEFRAMES = ("1m", "5m", "15m", "30m", "1h", "4h", "1d")
SCHEMA_INSTRUMENTS = ("spot", "perp")
REPORT_TYPES = ("dataset", "tf_aggregation", "cross_instrument")
OVERALL_STATUSES = ("PASS", "WARN", "FAIL", "INCOMPLETE")


class CatalogError(DataError):
    """Katalogen kan inte användas."""


class CatalogVersionError(CatalogError):
    """Katalogens schemaversion stöds inte (nyare än koden, eller kräver migrering)."""


class CatalogCorruptError(CatalogError):
    """Katalogen är skadad, partiell eller manipulerad. Åtgärd: rebuild."""


class CatalogMigrationError(CatalogError):
    """En migrering misslyckades; transaktionen rullades tillbaka, katalogen är orörd."""


class CatalogMigrationRequiredError(CatalogVersionError):
    """Katalogen har ett äldre, oskadat schema; den migreras bara med migrate_catalog()."""


class UnsupportedSQLiteError(CatalogError):
    """SQLite-biblioteket saknar funktioner katalogen kräver (STRICT)."""


# ------------------------------------------------------------------ dataset-ID
def dataset_key(*, exchange: str, instrument: str, symbol: str, timeframe: str) -> DatasetKey:
    """Strikt DatasetKey från lagrade fält (manifest, ref, katalog).

    Phase 2-normaliseringen (DatasetKey) används, men indata måste redan vara
    kanonisk: t.ex. 'btc/usdt' avvisas här i stället för att tyst bli 'BTCUSDT'.
    """
    key = DatasetKey(instrument, symbol, timeframe, exchange)  # type: ignore[arg-type]
    canon = (key.exchange, key.instrument.value, key.symbol, key.timeframe)
    if canon != (exchange, instrument, symbol, timeframe) or exchange != EXCHANGE:
        raise ValueError(f"icke-kanonisk datasetnyckel: {(exchange, instrument, symbol, timeframe)!r}")
    return key


def dataset_key_json(key: DatasetKey) -> str:
    """Kanonisk strukturerad representation (ingen tvetydig strängsammanfogning)."""
    if key.exchange != EXCHANGE:
        raise ValueError(f"exchange {key.exchange!r} stöds inte (endast {EXCHANGE!r})")
    return json.dumps({"exchange": key.exchange, "instrument": key.instrument.value,
                       "symbol": key.symbol, "timeframe": key.timeframe},
                      sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def dataset_id(key: DatasetKey) -> str:
    return hashlib.sha256(DATASET_ID_DOMAIN + dataset_key_json(key).encode("ascii")).hexdigest()


def funding_dataset_key(*, exchange: str, instrument: str, symbol: str) -> FundingKey:
    """Strikt FundingKey från lagrade fält; indata måste redan vara kanonisk (som dataset_key)."""
    key = FundingKey(instrument, symbol, exchange)  # type: ignore[arg-type]
    if (key.exchange, key.instrument.value, key.symbol) != (exchange, instrument, symbol) or exchange != EXCHANGE:
        raise ValueError(f"icke-kanonisk funding-nyckel: {(exchange, instrument, symbol)!r}")
    return key


def funding_dataset_key_json(key: FundingKey) -> str:
    """Kanonisk representation; "kind" skiljer den från varje OHLCV-nyckel."""
    if key.exchange != EXCHANGE:
        raise ValueError(f"exchange {key.exchange!r} stöds inte (endast {EXCHANGE!r})")
    return json.dumps({"exchange": key.exchange, "instrument": key.instrument.value, "kind": FUNDING_KIND,
                       "symbol": key.symbol}, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def funding_dataset_id(key: FundingKey) -> str:
    """Egen domäntagg: ett funding-id kan aldrig sammanfalla med ett OHLCV-dataset_id."""
    return hashlib.sha256(FUNDING_DATASET_ID_DOMAIN + funding_dataset_key_json(key).encode("ascii")).hexdigest()


def is_sha256_hex(value: object) -> bool:
    return (isinstance(value, str) and len(value) == 64
            and all(c in "0123456789abcdef" for c in value))


# ------------------------------------------------------------------ schema v1
def _sha(col: str) -> str:
    # GLOB är skiftlägeskänslig: versaler avvisas. NULL passerar (styrs av NOT NULL).
    return f"CHECK (length({col}) = 64 AND {col} NOT GLOB '*[^0-9a-f]*')"


def _in(col: str, values: tuple[str, ...]) -> str:
    return f"CHECK ({col} IN ({', '.join(repr(v) for v in values)}))"


def _append_only(table: str, *, allow_update: bool = False) -> str:
    out = []
    if not allow_update:
        out.append(f"CREATE TRIGGER {table}_no_update BEFORE UPDATE ON {table}\n"
                   f"BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END;")
    out.append(f"CREATE TRIGGER {table}_no_delete BEFORE DELETE ON {table}\n"
               f"BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END;")
    return "\n".join(out)


_V1_TABLES = f"""
CREATE TABLE schema_migrations (
    version       INTEGER PRIMARY KEY CHECK (version >= 1),
    name          TEXT NOT NULL CHECK (length(name) > 0),
    sql_sha256    TEXT NOT NULL {_sha("sql_sha256")}
) STRICT;

CREATE TABLE datasets (
    dataset_id    TEXT PRIMARY KEY {_sha("dataset_id")},
    exchange      TEXT NOT NULL {_in("exchange", (EXCHANGE,))},
    instrument    TEXT NOT NULL {_in("instrument", SCHEMA_INSTRUMENTS)},
    symbol        TEXT NOT NULL CHECK (length(symbol) BETWEEN 5 AND 20
                                       AND symbol NOT GLOB '*[^A-Z0-9]*'),
    timeframe     TEXT NOT NULL {_in("timeframe", SCHEMA_TIMEFRAMES)},
    UNIQUE (exchange, instrument, symbol, timeframe)
) STRICT, WITHOUT ROWID;

CREATE TABLE dataset_versions (
    dataset_id          TEXT NOT NULL REFERENCES datasets (dataset_id),
    data_version        TEXT NOT NULL {_sha("data_version")},
    data_file_sha256    TEXT NOT NULL {_sha("data_file_sha256")},
    data_file_bytes     INTEGER NOT NULL CHECK (data_file_bytes > 0),
    manifest_sha256     TEXT NOT NULL {_sha("manifest_sha256")},
    version_sha256      TEXT NOT NULL {_sha("version_sha256")},
    manifest_schema_version INTEGER NOT NULL CHECK (manifest_schema_version >= 1),
    rows                INTEGER NOT NULL CHECK (rows >= 1),
    first_open_time_ms  INTEGER NOT NULL,
    last_open_time_ms   INTEGER NOT NULL,
    gap_count           INTEGER NOT NULL CHECK (gap_count >= 0),
    missing_bars_total  INTEGER NOT NULL CHECK (missing_bars_total >= 0),
    PRIMARY KEY (dataset_id, data_version),
    CHECK (last_open_time_ms >= first_open_time_ms)
) STRICT, WITHOUT ROWID;

CREATE TABLE version_sources (
    dataset_id    TEXT NOT NULL,
    data_version  TEXT NOT NULL,
    source_index  INTEGER NOT NULL CHECK (source_index >= 0),
    name          TEXT NOT NULL CHECK (length(name) > 0),
    sha256        TEXT NOT NULL {_sha("sha256")},
    rows          INTEGER NOT NULL CHECK (rows >= 0),
    PRIMARY KEY (dataset_id, data_version, source_index),
    FOREIGN KEY (dataset_id, data_version) REFERENCES dataset_versions (dataset_id, data_version)
) STRICT, WITHOUT ROWID;

CREATE TABLE ref_log (
    seq                   INTEGER PRIMARY KEY CHECK (seq >= 1),
    entry_sha256          TEXT NOT NULL UNIQUE {_sha("entry_sha256")},
    prev_entry_sha256     TEXT UNIQUE {_sha("prev_entry_sha256")}
                              REFERENCES ref_log (entry_sha256),
    dataset_id            TEXT NOT NULL,
    data_version          TEXT NOT NULL,
    previous_data_version TEXT {_sha("previous_data_version")},
    action                TEXT NOT NULL {_in("action", ("ingest", "set_current", "import_legacy"))},
    UNIQUE (dataset_id, data_version, seq),
    CHECK ((seq = 1) = (prev_entry_sha256 IS NULL)),
    CHECK (previous_data_version IS NULL OR previous_data_version <> data_version),
    FOREIGN KEY (dataset_id, data_version) REFERENCES dataset_versions (dataset_id, data_version),
    FOREIGN KEY (dataset_id, previous_data_version)
        REFERENCES dataset_versions (dataset_id, data_version)
) STRICT;

CREATE TABLE refs (
    dataset_id       TEXT PRIMARY KEY REFERENCES datasets (dataset_id),
    data_version     TEXT NOT NULL,
    ref_log_seq      INTEGER NOT NULL,
    ref_file_sha256  TEXT NOT NULL {_sha("ref_file_sha256")},
    FOREIGN KEY (dataset_id, data_version) REFERENCES dataset_versions (dataset_id, data_version),
    FOREIGN KEY (dataset_id, data_version, ref_log_seq)
        REFERENCES ref_log (dataset_id, data_version, seq)
) STRICT, WITHOUT ROWID;

CREATE TABLE ingest_events (
    seq                   INTEGER PRIMARY KEY CHECK (seq >= 1),
    ingest_id             TEXT NOT NULL UNIQUE {_sha("ingest_id")},
    record_sha256         TEXT NOT NULL {_sha("record_sha256")},
    dataset_id            TEXT NOT NULL,
    data_version          TEXT NOT NULL,
    previous_data_version TEXT {_sha("previous_data_version")},
    params_json           TEXT NOT NULL CHECK (json_valid(params_json)),
    code_version          TEXT NOT NULL CHECK (length(code_version) > 0),
    worktree_clean        INTEGER CHECK (worktree_clean IN (0, 1)),
    publication           TEXT NOT NULL {_in("publication", ("published", "reused"))},
    ref_outcome           TEXT NOT NULL {_in("ref_outcome", ("changed", "unchanged"))},
    ref_log_seq           INTEGER UNIQUE,
    duplicates_dropped    INTEGER NOT NULL CHECK (duplicates_dropped >= 0),
    CHECK ((ref_outcome = 'changed') = (ref_log_seq IS NOT NULL)),
    CHECK (ref_outcome = 'changed' OR previous_data_version IS data_version),
    FOREIGN KEY (dataset_id, data_version) REFERENCES dataset_versions (dataset_id, data_version),
    FOREIGN KEY (dataset_id, data_version, ref_log_seq)
        REFERENCES ref_log (dataset_id, data_version, seq)
) STRICT;

CREATE TABLE ingest_sources (
    ingest_seq    INTEGER NOT NULL REFERENCES ingest_events (seq),
    source_index  INTEGER NOT NULL CHECK (source_index >= 0),
    name          TEXT NOT NULL CHECK (length(name) > 0),
    sha256        TEXT NOT NULL {_sha("sha256")},
    rows          INTEGER CHECK (rows >= 0),
    PRIMARY KEY (ingest_seq, source_index)
) STRICT, WITHOUT ROWID;

CREATE TABLE snapshots (
    snapshot_id           TEXT PRIMARY KEY {_sha("snapshot_id")},
    snapshot_file_sha256  TEXT NOT NULL {_sha("snapshot_file_sha256")},
    member_count          INTEGER NOT NULL CHECK (member_count >= 1)
) STRICT, WITHOUT ROWID;

CREATE TABLE snapshot_members (
    snapshot_id   TEXT NOT NULL REFERENCES snapshots (snapshot_id),
    dataset_id    TEXT NOT NULL,
    data_version  TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, dataset_id),
    FOREIGN KEY (dataset_id, data_version) REFERENCES dataset_versions (dataset_id, data_version)
) STRICT, WITHOUT ROWID;

CREATE TABLE quality_reports (
    report_sha256          TEXT PRIMARY KEY {_sha("report_sha256")},
    report_type            TEXT NOT NULL {_in("report_type", REPORT_TYPES)},
    overall                TEXT NOT NULL {_in("overall", OVERALL_STATUSES)},
    report_schema_version  INTEGER NOT NULL CHECK (report_schema_version >= 1),
    config_version         INTEGER NOT NULL CHECK (config_version >= 1),
    config_sha256          TEXT NOT NULL {_sha("config_sha256")},
    code_version           TEXT NOT NULL CHECK (length(code_version) > 0),
    worktree_clean         INTEGER CHECK (worktree_clean IN (0, 1)),
    dataset_count          INTEGER NOT NULL CHECK (dataset_count >= 1)
) STRICT, WITHOUT ROWID;

CREATE TABLE quality_report_datasets (
    report_sha256  TEXT NOT NULL REFERENCES quality_reports (report_sha256),
    position       INTEGER NOT NULL CHECK (position >= 0),
    dataset_id     TEXT NOT NULL,
    data_version   TEXT NOT NULL,
    PRIMARY KEY (report_sha256, position),
    UNIQUE (report_sha256, dataset_id, data_version),
    FOREIGN KEY (dataset_id, data_version) REFERENCES dataset_versions (dataset_id, data_version)
) STRICT, WITHOUT ROWID;
"""

_V1_APPEND_ONLY = ("schema_migrations", "datasets", "dataset_versions", "version_sources",
                   "ref_log", "ingest_events", "ingest_sources", "snapshots",
                   "snapshot_members", "quality_reports", "quality_report_datasets")

_V1_SQL = (_V1_TABLES.strip() + "\n\n"
           + "\n".join(_append_only(t) for t in _V1_APPEND_ONLY) + "\n"
           + _append_only("refs", allow_update=True) + "\n")


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


# Migration 2 (etapp B steg 4, SB-S4-a A2): funding-dataset. Bara nya tabeller; migration 1 är
# fryst (P4-S1-08) och ingen av dess tabeller ändras. Inga refs, ingen ref_log och inga
# ingest-poster för funding; funding-versioner pinnas via snapshots (format v2).
_V2_TABLES = f"""
CREATE TABLE funding_datasets (
    dataset_id    TEXT PRIMARY KEY {_sha("dataset_id")},
    exchange      TEXT NOT NULL {_in("exchange", (EXCHANGE,))},
    instrument    TEXT NOT NULL {_in("instrument", ("perp",))},
    symbol        TEXT NOT NULL CHECK (length(symbol) BETWEEN 5 AND 20
                                       AND symbol NOT GLOB '*[^A-Z0-9]*'),
    UNIQUE (exchange, instrument, symbol)
) STRICT, WITHOUT ROWID;

CREATE TABLE funding_versions (
    dataset_id          TEXT NOT NULL REFERENCES funding_datasets (dataset_id),
    data_version        TEXT NOT NULL {_sha("data_version")},
    data_file_sha256    TEXT NOT NULL {_sha("data_file_sha256")},
    data_file_bytes     INTEGER NOT NULL CHECK (data_file_bytes > 0),
    manifest_sha256     TEXT NOT NULL {_sha("manifest_sha256")},
    version_sha256      TEXT NOT NULL {_sha("version_sha256")},
    manifest_schema_version INTEGER NOT NULL CHECK (manifest_schema_version >= 1),
    rows                INTEGER NOT NULL CHECK (rows >= 1),
    first_calc_time_ms  INTEGER NOT NULL,
    last_calc_time_ms   INTEGER NOT NULL,
    duplicates_dropped  INTEGER NOT NULL CHECK (duplicates_dropped >= 0),
    PRIMARY KEY (dataset_id, data_version),
    CHECK (last_calc_time_ms >= first_calc_time_ms)
) STRICT, WITHOUT ROWID;

CREATE TABLE funding_version_sources (
    dataset_id    TEXT NOT NULL,
    data_version  TEXT NOT NULL,
    source_index  INTEGER NOT NULL CHECK (source_index >= 0),
    name          TEXT NOT NULL CHECK (length(name) > 0),
    sha256        TEXT NOT NULL {_sha("sha256")},
    rows          INTEGER NOT NULL CHECK (rows >= 1),
    PRIMARY KEY (dataset_id, data_version, source_index),
    FOREIGN KEY (dataset_id, data_version) REFERENCES funding_versions (dataset_id, data_version)
) STRICT, WITHOUT ROWID;

CREATE TABLE snapshot_funding_members (
    snapshot_id   TEXT NOT NULL REFERENCES snapshots (snapshot_id),
    dataset_id    TEXT NOT NULL,
    data_version  TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, dataset_id),
    FOREIGN KEY (dataset_id, data_version) REFERENCES funding_versions (dataset_id, data_version)
) STRICT, WITHOUT ROWID;
"""

_V2_APPEND_ONLY = ("funding_datasets", "funding_versions", "funding_version_sources",
                   "snapshot_funding_members")

_V2_SQL = (_V2_TABLES.strip() + "\n\n"
           + "\n".join(_append_only(t) for t in _V2_APPEND_ONLY) + "\n")

MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "initial_catalog", _V1_SQL),
    Migration(2, "funding_datasets", _V2_SQL),
)


# ------------------------------------------------------------------ anslutning
def _require_sqlite() -> None:
    if sqlite3.sqlite_version_info < MIN_SQLITE_VERSION:
        raise UnsupportedSQLiteError(
            f"SQLite {sqlite3.sqlite_version} saknar STRICT-tabeller; kräver "
            f">= {'.'.join(map(str, MIN_SQLITE_VERSION))}")


def _configure(conn: sqlite3.Connection, readonly: bool) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise CatalogError("foreign_keys kunde inte aktiveras")
    conn.execute("PRAGMA trusted_schema = OFF")
    conn.execute("PRAGMA cell_size_check = ON")
    if readonly:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    else:
        mode = conn.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
        conn.execute("PRAGMA synchronous = FULL")
    if str(mode).lower() != "delete":
        raise CatalogError(f"journal_mode={mode}; katalogen kräver rollback-journal (DELETE)")


def _connect(path: Path, mode: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode={mode}", uri=True,
                           isolation_level=None, timeout=5.0)
    try:
        _configure(conn, readonly=(mode == "ro"))
    except BaseException:
        conn.close()
        raise
    return conn


def _check_header(path: Path) -> None:
    with path.open("rb") as f:
        head = f.read(100)
    if len(head) < 100 or head[:16] != _SQLITE_HEADER:
        raise CatalogCorruptError(f"{path.name}: inte en komplett SQLite-databas")
    if head[18] != 1 or head[19] != 1:              # 2 = WAL
        raise CatalogCorruptError(f"{path.name}: WAL-format; katalogen kräver rollback-journal")


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """BEGIN IMMEDIATE ... COMMIT; ROLLBACK vid alla fel."""
    if conn.in_transaction:
        raise CatalogError("nästlade transaktioner stöds inte")
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")                        # kan fallera (t.ex. uppskjutna FK)
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def _apply(conn: sqlite3.Connection, m: Migration) -> None:
    """En migrering = en transaktion: DDL + schema_migrations + user_version."""
    script = (f"BEGIN IMMEDIATE;\n{m.sql}\n"
              f"INSERT INTO schema_migrations (version, name, sql_sha256) "
              f"VALUES ({int(m.version)}, '{m.name}', '{m.sha256}');\n"
              f"PRAGMA user_version = {int(m.version)};\nCOMMIT;")
    try:
        conn.executescript(script)
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def _user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _schema_objects(conn: sqlite3.Connection) -> list[tuple[Any, ...]]:
    return [tuple(r) for r in conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name")]


@functools.lru_cache(maxsize=16)
def _expected_schema(migrations: tuple[Migration, ...]) -> tuple[tuple[Any, ...], ...]:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    try:
        for m in migrations:
            _apply(conn, m)
        return tuple(_schema_objects(conn))
    finally:
        conn.close()


def _schema_problems(conn: sqlite3.Connection, version: int) -> list[str]:
    applied = MIGRATIONS[:version]
    if tuple(_schema_objects(conn)) != _expected_schema(applied):
        return [f"schemat avviker från migreringar 1..{version}"]
    if version == 0:
        return []
    rows = [tuple(r) for r in conn.execute(
        "SELECT version, name, sql_sha256 FROM schema_migrations ORDER BY version")]
    if rows != [(m.version, m.name, m.sha256) for m in applied]:
        return ["schema_migrations matchar inte kodens migreringar"]
    return []


def check_catalog(conn: sqlite3.Connection, *, full: bool = False) -> list[str]:
    """Strukturell kontroll av katalogen. Tom lista = OK. `full` = integrity_check."""
    problems: list[str] = []
    if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        problems.append("foreign_keys är avstängt på anslutningen")
    check = "integrity_check" if full else "quick_check"
    res = [r[0] for r in conn.execute(f"PRAGMA {check}")]
    if res != ["ok"]:
        problems.append(f"{check}: {res[:5]}")
        return problems
    version = _user_version(conn)
    if version != len(MIGRATIONS):
        problems.append(f"user_version={version}, förväntat {len(MIGRATIONS)}")
        return problems
    problems += _schema_problems(conn, version)
    if problems:
        return problems
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk:
        problems.append(f"foreign_key_check: {len(fk)} brott, t.ex. {tuple(fk[0])}")
    for did, exch, inst, sym, tf in conn.execute(
            "SELECT dataset_id, exchange, instrument, symbol, timeframe FROM datasets"):
        try:
            ok = dataset_id(dataset_key(exchange=exch, instrument=inst, symbol=sym,
                                        timeframe=tf)) == did
        except ValueError:
            ok = False
        if not ok:
            problems.append(f"dataset_id {did} matchar inte {exch}/{inst}/{sym}/{tf}")
    for did, exch, inst, sym in conn.execute(
            "SELECT dataset_id, exchange, instrument, symbol FROM funding_datasets"):
        try:
            ok = funding_dataset_id(funding_dataset_key(exchange=exch, instrument=inst, symbol=sym)) == did
        except ValueError:
            ok = False
        if not ok:
            problems.append(f"funding dataset_id {did} matchar inte {exch}/{inst}/{sym}/{FUNDING_KIND}")
    return problems


def _open_existing(path: Path, readonly: bool) -> sqlite3.Connection:
    journal = path.with_name(path.name + "-journal")
    hot = journal.exists() and journal.stat().st_size > 0
    if not hot:
        _check_header(path)                           # innan SQLite rör filen
    elif readonly:
        raise CatalogError(f"{path.name}: avbruten skrivning (hot journal); "
                           "öppna skrivbart för att rulla tillbaka")
    conn = _connect(path, "ro" if readonly else "rw")
    try:
        version = _user_version(conn)                 # ev. rollback av hot journal sker här
        _check_header(path)
        if version > len(MIGRATIONS):
            raise CatalogVersionError(
                f"{path.name}: schemaversion {version} är nyare än stödd {len(MIGRATIONS)}")
        if version < len(MIGRATIONS):
            # Aldrig automatisk migrering, inte heller skrivbart (SB-S4-MIG): bara migrate_catalog().
            problems = _schema_problems(conn, version)
            if problems:
                raise CatalogCorruptError(f"{path.name}: {problems}")
            raise CatalogMigrationRequiredError(
                f"{path.name}: schemaversion {version} kräver migrering till {len(MIGRATIONS)} "
                "(migrate_catalog; CLI: migrate-catalog --apply)")
        problems = check_catalog(conn)
        if problems:
            raise CatalogCorruptError(f"{path.name}: {problems}")
        return conn
    except BaseException:
        conn.close()
        raise


def open_catalog(path: str | Path, *, readonly: bool = False, create: bool = False,
                 staging_dir: str | Path | None = None) -> sqlite3.Connection:
    """Öppnar (och vid behov migrerar) katalogen. Läsare: readonly=True.

    create=True bygger en saknad katalog i `staging_dir` (default: katalogens egen
    mapp; samma filsystem krävs) och länkar in den utan överskrivning.
    Nyare schema -> CatalogVersionError. Skadad/partiell/manipulerad -> CatalogCorruptError.
    """
    _require_sqlite()
    path = Path(path)
    if not path.exists():
        if readonly or not create:
            raise CatalogError(f"katalog saknas: {path}")
        _build(path, populate=None, replace=False,
               staging_dir=Path(staging_dir) if staging_dir is not None else None)
    try:
        return _open_existing(path, readonly)
    except sqlite3.DatabaseError as e:
        if getattr(e, "sqlite_errorname", "") in ("SQLITE_BUSY", "SQLITE_LOCKED"):
            raise CatalogError(f"{path.name}: upptagen av annan anslutning") from e
        raise CatalogCorruptError(f"{path.name}: {e}") from e


# ------------------------------------------------------------------ bygg/rebuild
def _fsync_path(path: Path, directory: bool = False) -> None:
    fd = os.open(path, os.O_RDONLY | (os.O_DIRECTORY if directory else 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _build(path: Path, populate: Callable[[sqlite3.Connection], None] | None, *,
           replace: bool, staging_dir: Path | None = None) -> None:
    """Bygger en komplett katalog i en temporär fil och publicerar den atomiskt."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = staging_dir if staging_dir is not None else path.parent
    tmp = staging / f".catalog-{uuid.uuid4().hex}.sqlite.tmp"
    try:
        conn = _connect(tmp, "rwc")
        try:
            for m in MIGRATIONS:
                _apply(conn, m)
            if populate is not None:
                with transaction(conn):
                    populate(conn)
            problems = check_catalog(conn, full=True)
            if problems:
                raise CatalogCorruptError(f"ny katalog underkänd: {problems}")
        finally:
            conn.close()
        _fsync_path(tmp)
        if replace:
            # En kvarlämnad hot journal hör till den GAMLA filen och får aldrig
            # spelas upp mot den nya.
            path.with_name(path.name + "-journal").unlink(missing_ok=True)
            os.replace(tmp, path)
        else:
            os.link(tmp, path)                        # misslyckas om path finns
        _fsync_path(path.parent, directory=True)
    finally:
        tmp.unlink(missing_ok=True)
        for suffix in ("-journal", "-wal", "-shm"):
            tmp.with_name(tmp.name + suffix).unlink(missing_ok=True)


def rebuild_catalog(path: str | Path, populate: Callable[[sqlite3.Connection], None], *,
                    staging_dir: str | Path | None = None) -> str:
    """Ersätter katalogen med en nybyggd. `populate` fyller den i EN transaktion
    från de oföränderliga filobjekten. Vid fel lämnas befintlig katalog orörd.
    Returnerar dump_sha256 för den nya katalogen. Anroparen håller skrivlåset."""
    _require_sqlite()
    path = Path(path)
    _build(path, populate, replace=True,
           staging_dir=Path(staging_dir) if staging_dir is not None else None)
    conn = open_catalog(path, readonly=True)
    try:
        return dump_sha256(conn)
    finally:
        conn.close()


# ------------------------------------------------------------------ dump/query
def _tables(conn: sqlite3.Connection) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name")]


def dump_catalog(conn: sqlite3.Connection) -> str:
    """Deterministisk textdump (JSON-rader): schema-hash + alla rader sorterade
    på samtliga kolumner. Oberoende av insättningsordning och sidlayout."""
    schema = json.dumps(_schema_objects(conn), separators=(",", ":"), ensure_ascii=True)
    lines = [json.dumps({"catalog_schema_version": _user_version(conn),
                         "schema_sha256": hashlib.sha256(schema.encode()).hexdigest()},
                        sort_keys=True, separators=(",", ":"))]
    for t in _tables(conn):
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")')]
        order = ", ".join(str(i) for i in range(1, len(cols) + 1))
        lines.append(json.dumps({"table": t, "columns": cols}, separators=(",", ":")))
        for row in conn.execute(f'SELECT * FROM "{t}" ORDER BY {order}'):
            lines.append(json.dumps([t, list(row)], separators=(",", ":"), ensure_ascii=True))
    return "\n".join(lines) + "\n"


def dump_sha256(conn: sqlite3.Connection) -> str:
    return hashlib.sha256(dump_catalog(conn).encode("ascii")).hexdigest()


def ensure_dataset(conn: sqlite3.Connection, key: DatasetKey) -> str:
    """Registrerar datasetet (idempotent) och returnerar dess dataset_id."""
    did = dataset_id(key)
    fields = (key.exchange, key.instrument.value, key.symbol, key.timeframe)
    conn.execute("INSERT INTO datasets (dataset_id, exchange, instrument, symbol, timeframe) "
                 "VALUES (?, ?, ?, ?, ?) ON CONFLICT (dataset_id) DO NOTHING", (did, *fields))
    row = conn.execute("SELECT exchange, instrument, symbol, timeframe FROM datasets "
                       "WHERE dataset_id = ?", (did,)).fetchone()
    if tuple(row) != fields:
        raise CatalogCorruptError(f"dataset_id {did} är registrerat för {tuple(row)}")
    return did


def list_datasets(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cur = conn.execute("SELECT dataset_id, exchange, instrument, symbol, timeframe FROM datasets "
                       "ORDER BY exchange, instrument, symbol, timeframe")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur]


def list_dataset_versions(conn: sqlite3.Connection, did: str) -> list[dict[str, Any]]:
    cur = conn.execute("SELECT * FROM dataset_versions WHERE dataset_id = ? "
                       "ORDER BY first_open_time_ms, last_open_time_ms, data_version", (did,))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur]


# ------------------------------------------------------------------ explicit migrering (SB-S4-MIG)
@dataclass(frozen=True)
class MigrationPlan:
    version: int                                     # katalogens user_version nu
    target: int                                      # len(MIGRATIONS)
    pending: tuple[tuple[int, str, str], ...]        # (version, namn, sql_sha256) som återstår


@dataclass(frozen=True)
class MigrationResult:
    plan: MigrationPlan
    applied: bool
    version_after: int


def plan_migration(path: str | Path) -> MigrationPlan:
    """Läsande: katalogens schemaversion och återstående migreringar. Skrivs aldrig.
    Saknad katalog, hot journal, nyare schema eller ett schema som inte är exakt migreringar
    1..version -> fel (en skadad katalog migreras aldrig)."""
    _require_sqlite()
    path = Path(path)
    if not path.exists():
        raise CatalogError(f"katalog saknas: {path}")
    journal = path.with_name(path.name + "-journal")
    if journal.exists() and journal.stat().st_size > 0:
        raise CatalogError(f"{path.name}: avbruten skrivning (hot journal); migrate_catalog(apply=True) "
                           "rullar tillbaka den först")
    _check_header(path)
    conn = _connect(path, "ro")
    try:
        return _plan(conn, path)
    finally:
        conn.close()


def _plan(conn: sqlite3.Connection, path: Path) -> MigrationPlan:
    version = _user_version(conn)
    if version > len(MIGRATIONS):
        raise CatalogVersionError(f"{path.name}: schemaversion {version} är nyare än stödd {len(MIGRATIONS)}")
    problems = _schema_problems(conn, version)
    if problems:
        raise CatalogCorruptError(f"{path.name}: {problems}")
    return MigrationPlan(version, len(MIGRATIONS),
                         tuple((m.version, m.name, m.sha256) for m in MIGRATIONS[version:]))


def migrate_catalog(path: str | Path, *, apply: bool = False) -> MigrationResult:
    """Explicit migrering till len(MIGRATIONS), dry-run som standard (SB-S4-MIG). Varje migrering
    är en transaktion (_apply); efteråt full kontroll. Anroparen håller lagrets skrivarlås.
    apply=True rullar först tillbaka en avbruten skrivning (SQLite, skrivbar öppning)."""
    _require_sqlite()
    path = Path(path)
    if not apply:
        plan = plan_migration(path)
        return MigrationResult(plan, applied=False, version_after=plan.version)
    if not path.exists():
        raise CatalogError(f"katalog saknas: {path}")
    conn = _connect(path, "rw")
    try:
        plan = _plan(conn, path)                     # ev. rollback av hot journal sker här
        _check_header(path)
        for m in MIGRATIONS[plan.version:]:
            try:
                _apply(conn, m)
            except sqlite3.Error as e:
                raise CatalogMigrationError(
                    f"{path.name}: migrering {m.version} ({m.name}) misslyckades: {e}") from e
        problems = check_catalog(conn, full=True)
        if problems:
            raise CatalogCorruptError(f"{path.name}: efter migrering: {problems}")
        return MigrationResult(plan, applied=bool(plan.pending), version_after=_user_version(conn))
    finally:
        conn.close()


def ensure_funding_dataset(conn: sqlite3.Connection, key: FundingKey) -> str:
    """Registrerar funding-datasetet (idempotent) och returnerar dess dataset_id."""
    did = funding_dataset_id(key)
    fields = (key.exchange, key.instrument.value, key.symbol)
    conn.execute("INSERT INTO funding_datasets (dataset_id, exchange, instrument, symbol) "
                 "VALUES (?, ?, ?, ?) ON CONFLICT (dataset_id) DO NOTHING", (did, *fields))
    row = conn.execute("SELECT exchange, instrument, symbol FROM funding_datasets "
                       "WHERE dataset_id = ?", (did,)).fetchone()
    if tuple(row) != fields:
        raise CatalogCorruptError(f"funding dataset_id {did} är registrerat för {tuple(row)}")
    return did


def list_funding_versions(conn: sqlite3.Connection, did: str) -> list[dict[str, Any]]:
    cur = conn.execute("SELECT * FROM funding_versions WHERE dataset_id = ? "
                       "ORDER BY first_calc_time_ms, last_calc_time_ms, data_version", (did,))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur]
