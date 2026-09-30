"""PHASE 4 steg 1: SQLite-katalog - schema, migreringar, constraints, FK, ID, säkerhet."""
import hashlib
import json
import re
import sqlite3
import subprocess
import sys

import pytest

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data.schema import EXCHANGE, DatasetKey, Instrument
from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.database import catalog as cat

SPOT15 = DatasetKey("spot", "BTCUSDT", "15m")
PERP15 = DatasetKey("perp", "BTCUSDT", "15m")


def h(tag: str) -> str:
    return hashlib.sha256(tag.encode()).hexdigest()


def _new(tmp_path, name="catalog.sqlite"):
    return cat.open_catalog(tmp_path / name, create=True)


def _raw(path):
    """Rå anslutning förbi katalogens skydd (simulerar fel/manipulation)."""
    return sqlite3.connect(path, isolation_level=None)


# ------------------------------------------------------------------ testdata
def _version_row(did, dv, first=0, last=900_000):
    return (did, dv, h("file" + dv), 1234, h("man" + dv), h("ver" + dv), 1, 2, first, last, 0, 0)


def _populate_steps(conn):
    """Komplett, konsistent katalog som (tabell, rader)-lista i beroendeordning."""
    s, p = cat.dataset_id(SPOT15), cat.dataset_id(PERP15)
    v1, v2, pv = h("v1"), h("v2"), h("pv")
    e1, e2, e3 = h("e1"), h("e2"), h("e3")
    return [
        ("datasets", [(s, "binance", "spot", "BTCUSDT", "15m"),
                      (p, "binance", "perp", "BTCUSDT", "15m")]),
        ("dataset_versions", [_version_row(s, v1), _version_row(s, v2, 0, 1_800_000),
                              _version_row(p, pv)]),
        ("version_sources", [(s, v1, 0, "a.zip", h("za"), 2), (s, v2, 0, "a.zip", h("za"), 2),
                             (s, v2, 1, "b.zip", h("zb"), 1), (p, pv, 0, "p.zip", h("zp"), 2)]),
        ("ref_log", [(1, e1, None, s, v1, None, "ingest"),
                     (2, e2, e1, p, pv, None, "ingest"),
                     (3, e3, e2, s, v2, v1, "ingest")]),
        ("refs", [(s, v2, 3, h("refs")), (p, pv, 2, h("refp"))]),
        ("ingest_events", [(1, h("i1"), h("r1"), s, v1, None, "{}", "abc", 1, "published", "changed", 1, 0),
                           (2, h("i2"), h("r2"), p, pv, None, "{}", "abc", 1, "published", "changed", 2, 0),
                           (3, h("i3"), h("r3"), s, v2, v1, '{"x":1}', "abc", 0, "published", "changed", 3, 1),
                           (4, h("i4"), h("r4"), s, v2, v2, '{"x":1}', "abc", 0, "reused", "unchanged", None, 2)]),
        ("ingest_sources", [(1, 0, "a.zip", h("za"), 2), (4, 0, "b.zip", h("zb"), None)]),
        ("snapshots", [(h("snap"), h("snapfile"), 2)]),
        ("snapshot_members", [(h("snap"), s, v1), (h("snap"), p, pv)]),
        ("quality_reports", [(h("rep"), "cross_instrument", "WARN", 1, 1, h("cfg"), "abc", 1, 2)]),
        ("quality_report_datasets", [(h("rep"), 0, s, v1), (h("rep"), 1, p, pv)]),
    ]


def _insert(conn, table, row):
    conn.execute(f"INSERT INTO {table} VALUES ({', '.join('?' * len(row))})", row)


def populate(conn, reverse_rows=False):
    if reverse_rows:        # ref_log-kedjan refererar bakåt; FK kontrolleras då vid COMMIT
        conn.execute("PRAGMA defer_foreign_keys = ON")
    for table, rows in _populate_steps(conn):
        for row in (reversed(rows) if reverse_rows else rows):
            _insert(conn, table, row)


@pytest.fixture
def full(tmp_path):
    conn = _new(tmp_path)
    with cat.transaction(conn):
        populate(conn)
    yield conn
    conn.close()


# ------------------------------------------------------------------ schema/migrering
REQUIRED_TABLES = {"datasets", "dataset_versions", "version_sources", "refs", "ref_log",
                   "ingest_events", "ingest_sources", "snapshots", "snapshot_members",
                   "quality_reports", "quality_report_datasets"}


def test_new_catalog_has_required_strict_tables_and_version(tmp_path):
    conn = _new(tmp_path)
    tables = {r[1]: r[5] for r in conn.execute("PRAGMA table_list")
              if r[2] == "table" and not r[1].startswith("sqlite_")}
    assert set(tables) == REQUIRED_TABLES | {"schema_migrations"}
    assert all(strict == 1 for strict in tables.values())
    assert conn.execute("PRAGMA user_version").fetchone()[0] == cat.CATALOG_SCHEMA_VERSION == 1
    assert conn.execute("SELECT version, name, sql_sha256 FROM schema_migrations").fetchall() == \
        [(1, "initial_catalog", cat.MIGRATIONS[0].sha256)]
    assert cat.check_catalog(conn, full=True) == []


def test_migration_sql_is_frozen():
    """Migrering 1 får aldrig ändras i efterhand: ny ändring = ny migrering."""
    assert cat.MIGRATIONS[0].sha256 == \
        "a277d5b0e51813a0faca66cdf388d611bee19e1c8b5f01694685ef0aa9d9e1a6"
    assert [m.version for m in cat.MIGRATIONS] == list(range(1, len(cat.MIGRATIONS) + 1))
    assert cat.CATALOG_SCHEMA_VERSION == len(cat.MIGRATIONS)


def test_schema_enums_match_phase1_3_constants():
    assert set(cat.SCHEMA_TIMEFRAMES) == set(TIMEFRAMES)
    assert set(cat.SCHEMA_INSTRUMENTS) == {i.value for i in Instrument}
    assert cat.OVERALL_STATUSES == ("PASS", "WARN", "FAIL", "INCOMPLETE")
    assert {qc.overall_status([s]) for s in qc.Status} <= set(cat.OVERALL_STATUSES)


def test_every_hash_column_has_lowercase_sha256_check(tmp_path):
    conn = _new(tmp_path)
    hash_col = re.compile(r"(sha256|^data_version$|^previous_data_version$|^dataset_id$|"
                          r"^snapshot_id$|^ingest_id$)")
    found = 0
    for (table, sql) in conn.execute("SELECT name, sql FROM sqlite_schema WHERE type='table'"):
        for col in (r[1] for r in conn.execute(f"PRAGMA table_info({table})")):
            if not hash_col.search(col):
                continue
            direct = f"length({col}) = 64 AND {col} NOT GLOB '*[^0-9a-f]*'" in sql
            via_fk = f"FOREIGN KEY ({col}" in sql or f"({col}) REFERENCES" in sql or \
                re.search(rf"\b{col}\b[^,]*REFERENCES", sql) or \
                re.search(rf"FOREIGN KEY \([^)]*\b{col}\b[^)]*\)", sql)
            assert direct or via_fk, f"{table}.{col} saknar hash-CHECK/FK"
            found += 1
    assert found >= 25


def test_v0_empty_catalog_is_migrated_and_readonly_refuses(tmp_path):
    path = tmp_path / "catalog.sqlite"
    raw = _raw(path)
    raw.execute("VACUUM")                                # giltig tom databas, user_version 0
    raw.close()
    with pytest.raises(cat.CatalogVersionError, match="kräver migrering"):
        cat.open_catalog(path, readonly=True)
    conn = cat.open_catalog(path)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
    assert cat.check_catalog(conn, full=True) == []


def test_newer_schema_version_is_rejected(tmp_path):
    path = tmp_path / "catalog.sqlite"
    _new(tmp_path).close()
    raw = _raw(path)
    raw.execute("PRAGMA user_version = 2")
    raw.close()
    for ro in (False, True):
        with pytest.raises(cat.CatalogVersionError, match="nyare"):
            cat.open_catalog(path, readonly=ro)
    raw = _raw(path)
    assert raw.execute("PRAGMA user_version").fetchone()[0] == 2    # orörd
    raw.close()


def test_failing_migration_rolls_back_completely(tmp_path, monkeypatch):
    path = tmp_path / "catalog.sqlite"
    _new(tmp_path).close()
    before = cat.dump_catalog(cat.open_catalog(path, readonly=True))
    bad = cat.Migration(2, "broken", "CREATE TABLE x (a INTEGER) STRICT;\nINSERT INTO nope VALUES (1);")
    monkeypatch.setattr(cat, "MIGRATIONS", cat.MIGRATIONS + (bad,))
    with pytest.raises(cat.CatalogMigrationError, match="migrering 2"):
        cat.open_catalog(path)
    monkeypatch.undo()
    conn = cat.open_catalog(path, readonly=True)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
    assert cat.dump_catalog(conn) == before


def test_successful_second_migration_is_applied_and_recorded(tmp_path, monkeypatch):
    path = tmp_path / "catalog.sqlite"
    _new(tmp_path).close()
    m2 = cat.Migration(2, "add_x", "CREATE TABLE x (a INTEGER) STRICT;")
    monkeypatch.setattr(cat, "MIGRATIONS", cat.MIGRATIONS + (m2,))
    conn = cat.open_catalog(path)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
    assert conn.execute("SELECT version, sql_sha256 FROM schema_migrations WHERE version=2"
                        ).fetchone() == (2, m2.sha256)


# ------------------------------------------------------------------ säkerhet/korruption
def test_tampered_migration_record_is_rejected(tmp_path):
    path = tmp_path / "catalog.sqlite"
    _new(tmp_path).close()
    raw = _raw(path)
    raw.execute("DROP TRIGGER schema_migrations_no_update")
    raw.execute("UPDATE schema_migrations SET sql_sha256 = ?", (h("x"),))
    raw.close()
    with pytest.raises(cat.CatalogCorruptError, match="schema"):
        cat.open_catalog(path)


@pytest.mark.parametrize("sql", [
    "DROP TRIGGER dataset_versions_no_update",
    "CREATE INDEX extra ON datasets (symbol)",
    "CREATE TABLE extra (a INTEGER)",
])
def test_schema_drift_is_rejected(tmp_path, sql):
    path = tmp_path / "catalog.sqlite"
    _new(tmp_path).close()
    raw = _raw(path)
    raw.execute(sql)
    raw.close()
    with pytest.raises(cat.CatalogCorruptError, match="schemat avviker"):
        cat.open_catalog(path, readonly=True)


def test_partial_v0_catalog_with_foreign_objects_is_rejected(tmp_path):
    path = tmp_path / "catalog.sqlite"
    raw = _raw(path)
    raw.execute("CREATE TABLE datasets (x)")
    raw.close()
    with pytest.raises(cat.CatalogCorruptError):
        cat.open_catalog(path)


@pytest.mark.parametrize("content", [b"", b"not a database at all" * 10,
                                     b"SQLite format 3\x00" + b"\x00" * 50])
def test_garbage_or_truncated_file_is_corrupt(tmp_path, content):
    path = tmp_path / "catalog.sqlite"
    path.write_bytes(content)
    for ro in (True, False):
        with pytest.raises(cat.CatalogCorruptError):
            cat.open_catalog(path, readonly=ro)
    assert path.read_bytes() == content                 # aldrig "reparerad" i smyg


def test_truncated_real_catalog_is_corrupt(tmp_path, full):
    path = tmp_path / "catalog.sqlite"
    full.close()
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])
    with pytest.raises(cat.CatalogCorruptError):
        conn = cat.open_catalog(path, readonly=True)
        cat.dump_catalog(conn)


def test_wal_catalog_is_rejected(tmp_path):
    path = tmp_path / "catalog.sqlite"
    _new(tmp_path).close()
    raw = _raw(path)
    assert raw.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"
    raw.close()
    with pytest.raises(cat.CatalogCorruptError, match="WAL"):
        cat.open_catalog(path)


def test_rollback_journal_mode_and_no_wal_files(tmp_path, full):
    assert full.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert full.execute("PRAGMA synchronous").fetchone()[0] == 2          # FULL
    assert not list(tmp_path.glob("*-wal")) and not list(tmp_path.glob("*-shm"))


def test_hot_journal_is_rolled_back_by_writer_and_refused_by_reader(tmp_path, full):
    """Simulerad krasch mitt i en skrivning: ofullständig transaktion får aldrig synas."""
    path = tmp_path / "catalog.sqlite"
    before = cat.dump_catalog(full)
    full.close()
    code = (
        "import sqlite3, os\n"
        f"c = sqlite3.connect({str(path)!r}, isolation_level=None)\n"
        "c.execute('PRAGMA cache_size=1'); c.execute('BEGIN IMMEDIATE')\n"
        "c.execute('DROP TRIGGER datasets_no_delete')\n"
        "c.execute('PRAGMA foreign_keys=OFF'); c.execute('DELETE FROM datasets')\n"
        "c.execute(\"CREATE TABLE junk (x)\")\n"
        "c.executemany('INSERT INTO junk VALUES (?)', [(os.urandom(512),) for _ in range(400)])\n"
        "os._exit(0)\n")
    subprocess.run([sys.executable, "-c", code], check=True)
    journal = tmp_path / "catalog.sqlite-journal"
    assert journal.exists() and journal.stat().st_size > 0
    with pytest.raises(cat.CatalogError, match="hot journal"):
        cat.open_catalog(path, readonly=True)
    conn = cat.open_catalog(path)
    assert cat.dump_catalog(conn) == before
    assert cat.check_catalog(conn, full=True) == []


def test_readonly_connection_cannot_write(full, tmp_path):
    ro = cat.open_catalog(tmp_path / "catalog.sqlite", readonly=True)
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        ro.execute("INSERT INTO snapshots VALUES (?, ?, 1)", (h("a"), h("b")))


def test_missing_catalog_is_not_created_implicitly(tmp_path):
    for kw in ({}, {"readonly": True}, {"readonly": True, "create": True}):
        with pytest.raises(cat.CatalogError, match="saknas"):
            cat.open_catalog(tmp_path / "catalog.sqlite", **kw)
    assert not (tmp_path / "catalog.sqlite").exists()


def test_create_never_overwrites_existing_catalog(tmp_path, full):
    before = cat.dump_catalog(full)
    again = cat.open_catalog(tmp_path / "catalog.sqlite", create=True)
    assert cat.dump_catalog(again) == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["catalog.sqlite"]


def test_old_sqlite_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(cat, "MIN_SQLITE_VERSION", (99, 0, 0))
    with pytest.raises(cat.UnsupportedSQLiteError):
        _new(tmp_path)


# ------------------------------------------------------------------ foreign keys
def test_foreign_keys_enabled_on_every_connection(tmp_path):
    _new(tmp_path).close()
    for ro in (False, True, False):
        conn = cat.open_catalog(tmp_path / "catalog.sqlite", readonly=ro)
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        conn.close()


@pytest.mark.parametrize("table,row", [
    ("dataset_versions", _version_row(h("nods"), h("v"))),
    ("version_sources", (cat.dataset_id(SPOT15), h("nov"), 0, "a.zip", h("z"), 1)),
    ("refs", (cat.dataset_id(SPOT15), h("v1"), 99, h("r"))),               # ref_log saknas
    ("refs", (cat.dataset_id(SPOT15), h("v1"), 2, h("r"))),                # ref_log-rad för annat dataset
    ("snapshot_members", (h("nosnap"), cat.dataset_id(SPOT15), h("v1"))),
    ("snapshot_members", (h("snap2"), cat.dataset_id(SPOT15), h("nov"))),   # version saknas
    ("quality_report_datasets", (h("rep"), 5, cat.dataset_id(SPOT15), h("nov"))),
    ("ingest_sources", (99, 0, "a.zip", h("z"), 1)),
    ("ref_log", (4, h("e4"), h("unknown"), cat.dataset_id(SPOT15), h("v1"), h("v2"), "set_current")),
])
def test_foreign_key_violations_are_rejected(full, table, row):
    if table == "refs":
        full.execute("DROP TRIGGER refs_no_delete")
        full.execute("DELETE FROM refs WHERE dataset_id = ?", (row[0],))
    if row[0] == h("snap2"):
        _insert(full, "snapshots", (h("snap2"), h("f2"), 1))
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
        _insert(full, table, row)


def test_mutation_foreign_keys_disabled_is_detected(tmp_path):
    """Mutation: en skrivare med foreign_keys=OFF lämnar föräldralösa rader -> katalogen underkänns."""
    path = tmp_path / "catalog.sqlite"
    _new(tmp_path).close()
    raw = _raw(path)                                     # foreign_keys är OFF som default
    _insert(raw, "dataset_versions", _version_row(h("orphan-ds"), h("v")))
    raw.close()
    with pytest.raises(cat.CatalogCorruptError, match="foreign_key_check"):
        cat.open_catalog(path, readonly=True)


def test_check_catalog_reports_disabled_foreign_keys(full):
    full.execute("PRAGMA foreign_keys = OFF")
    assert any("foreign_keys" in p for p in cat.check_catalog(full))


# ------------------------------------------------------------------ constraints
def test_full_population_is_valid(full):
    assert cat.check_catalog(full, full=True) == []


BAD_HASHES = ["A" * 64, "a" * 63, "a" * 65, "g" * 64, "", " " + "a" * 63, "0x" + "a" * 62]


@pytest.mark.parametrize("bad", BAD_HASHES)
def test_invalid_hash_values_are_rejected(full, bad):
    s = cat.dataset_id(SPOT15)
    cases = [
        ("dataset_versions", (s, bad) + _version_row(s, h("x"))[2:]),
        ("dataset_versions", _version_row(s, h("y"))[:2] + (bad,) + _version_row(s, h("y"))[3:]),
        ("datasets", (bad, "binance", "spot", "BTCUSDT", "1h")),
        ("snapshots", (bad, h("f"), 1)),
        ("quality_reports", (h("r2"), "dataset", "PASS", 1, 1, bad, "abc", None, 1)),
    ]
    for table, row in cases:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            _insert(full, table, row)


@pytest.mark.parametrize("table,row", [
    ("datasets", (h("d"), "binance", "futures", "BTCUSDT", "15m")),
    ("datasets", (h("d"), "binance", "spot", "btcusdt", "15m")),
    ("datasets", (h("d"), "binance", "spot", "BTC/USDT", "15m")),
    ("datasets", (h("d"), "binance", "spot", "BTCUSDT", "2m")),
    ("datasets", (h("d"), "bybit", "spot", "BTCUSDT", "15m")),
    ("dataset_versions", _version_row(cat.dataset_id(SPOT15), h("z"), 10, 5)),        # last < first
    ("dataset_versions", _version_row(cat.dataset_id(SPOT15), h("z"))[:7] + (0,)
     + _version_row(cat.dataset_id(SPOT15), h("z"))[8:]),                               # rows 0
    ("ref_log", (4, h("e4"), h("e3"), cat.dataset_id(SPOT15), h("v2"), h("v2"), "set_current")),
    ("ref_log", (4, h("e4"), None, cat.dataset_id(SPOT15), h("v1"), h("v2"), "set_current")),
    ("ref_log", (4, h("e4"), h("e3"), cat.dataset_id(SPOT15), h("v1"), h("v2"), "delete")),
    ("ingest_events", (5, h("i5"), h("r5"), cat.dataset_id(SPOT15), h("v2"), h("v2"), "{}", "abc",
                       1, "reused", "changed", None, 0)),                                   # changed utan ref_log
    ("ingest_events", (5, h("i5"), h("r5"), cat.dataset_id(SPOT15), h("v2"), h("v1"), "{}", "abc",
                       1, "reused", "unchanged", None, 0)),                                 # unchanged men annan prev
    ("ingest_events", (5, h("i5"), h("r5"), cat.dataset_id(SPOT15), h("v2"), h("v2"), "{bad", "abc",
                       1, "reused", "unchanged", None, 0)),
    ("ingest_events", (5, h("i5"), h("r5"), cat.dataset_id(SPOT15), h("v2"), h("v2"), "{}", "abc",
                       2, "reused", "unchanged", None, 0)),
    ("ingest_events", (5, h("i5"), h("r5"), cat.dataset_id(SPOT15), h("v2"), h("v2"), "{}", "abc",
                       1, "skipped", "unchanged", None, 0)),
    ("ingest_events", (5, h("i5"), h("r5"), cat.dataset_id(SPOT15), h("v2"), h("v2"), "{}", "abc",
                       1, "reused", "unchanged", None, -1)),                             # duplicates_dropped < 0
    ("quality_reports", (h("r2"), "dataset", "OK", 1, 1, h("c"), "abc", None, 1)),
    ("quality_reports", (h("r2"), "xinst", "PASS", 1, 1, h("c"), "abc", None, 1)),
    ("quality_reports", (h("r2"), "dataset", "PASS", 1, 1, h("c"), "", None, 1)),
    ("snapshots", (h("s2"), h("f"), 0)),
])
def test_check_constraints(full, table, row):
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        _insert(full, table, row)


@pytest.mark.parametrize("table,row", [
    ("datasets", (cat.dataset_id(SPOT15), "binance", "spot", "BTCUSDT", "15m")),
    ("datasets", (h("other-id"), "binance", "spot", "BTCUSDT", "15m")),      # samma nyckel, annat ID
    ("dataset_versions", _version_row(cat.dataset_id(SPOT15), h("v1"))),
    ("ref_log", (4, h("e1"), h("e3"), cat.dataset_id(SPOT15), h("v1"), h("v2"), "set_current")),
    ("ref_log", (4, h("e4"), h("e2"), cat.dataset_id(SPOT15), h("v1"), h("v2"), "set_current")),  # gren
    ("ingest_events", (5, h("i1"), h("r5"), cat.dataset_id(SPOT15), h("v2"), h("v2"), "{}", "abc",
                       1, "reused", "unchanged", None, 0)),
    ("snapshot_members", (h("snap"), cat.dataset_id(SPOT15), h("v2"))),     # två versioner av samma dataset
    ("quality_report_datasets", (h("rep"), 2, cat.dataset_id(SPOT15), h("v1"))),
])
def test_unique_constraints(full, table, row):
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE|PRIMARY"):
        _insert(full, table, row)


def test_duplicates_dropped_is_required(full):
    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
        _insert(full, "ingest_events", (5, h("i5"), h("r5"), cat.dataset_id(SPOT15), h("v2"), h("v2"),
                                        "{}", "abc", 1, "reused", "unchanged", None, None))


def test_strict_tables_reject_wrong_types(full):
    s = cat.dataset_id(SPOT15)
    row = list(_version_row(s, h("t")))
    row[7] = "two"                                      # rows som TEXT
    with pytest.raises(sqlite3.IntegrityError, match="cannot store TEXT"):
        _insert(full, "dataset_versions", tuple(row))
    row[7] = 2.5
    with pytest.raises(sqlite3.IntegrityError, match="cannot store REAL"):
        _insert(full, "dataset_versions", tuple(row))


@pytest.mark.parametrize("sql", [
    "UPDATE dataset_versions SET rows = 3",
    "DELETE FROM dataset_versions",
    "UPDATE datasets SET symbol = 'ETHUSDT'",
    "DELETE FROM ref_log WHERE seq = 3",
    "UPDATE ref_log SET action = 'set_current'",
    "UPDATE ingest_events SET publication = 'reused'",
    "DELETE FROM ingest_events",
    "DELETE FROM snapshot_members",
    "UPDATE quality_reports SET overall = 'PASS'",
    "DELETE FROM refs",
    "DELETE FROM schema_migrations",
])
def test_append_only_tables(full, sql):
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        full.execute(sql)


def test_ref_update_must_follow_ref_log(full):
    s = cat.dataset_id(SPOT15)
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):   # ingen logg för v1@seq 3
        full.execute("UPDATE refs SET data_version = ? WHERE dataset_id = ?", (h("v1"), s))
    _insert(full, "ref_log", (4, h("e4"), h("e3"), s, h("v1"), h("v2"), "set_current"))
    full.execute("UPDATE refs SET data_version = ?, ref_log_seq = 4 WHERE dataset_id = ?", (h("v1"), s))
    assert cat.check_catalog(full) == []


def test_transaction_rolls_back_when_deferred_fk_fails_at_commit(full):
    before = cat.dump_catalog(full)
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"), cat.transaction(full):
        full.execute("PRAGMA defer_foreign_keys = ON")
        _insert(full, "snapshots", (h("s9"), h("f"), 1))
        _insert(full, "snapshot_members", (h("s9"), cat.dataset_id(SPOT15), h("missing")))
    assert cat.dump_catalog(full) == before and not full.in_transaction


def test_transaction_rolls_back_on_error(full):
    before = cat.dump_catalog(full)
    with pytest.raises(sqlite3.IntegrityError), cat.transaction(full):
        _insert(full, "snapshots", (h("ok"), h("f"), 1))
        _insert(full, "snapshots", (h("ok"), h("f"), 1))
    assert cat.dump_catalog(full) == before and not full.in_transaction


# ------------------------------------------------------------------ dataset-ID
def test_dataset_id_golden_values():
    """Frysta värden: ett ändrat ID bryter alla refs/snapshots. Ändras aldrig."""
    assert cat.dataset_key_json(SPOT15) == \
        '{"exchange":"binance","instrument":"spot","symbol":"BTCUSDT","timeframe":"15m"}'
    assert cat.dataset_id(SPOT15) == \
        "f607d43c4d9de93d95427145ed5da4396e73b08cf6267ef471de6d700eb8e052"
    assert cat.dataset_id(SPOT15) == hashlib.sha256(
        b"btc-quant/dataset-id/v1\x00" + cat.dataset_key_json(SPOT15).encode()).hexdigest()


def test_dataset_id_is_stable_across_processes():
    code = ("from btc_trading_system.data.schema import DatasetKey\n"
            "from btc_trading_system.database.catalog import dataset_id\n"
            "print(dataset_id(DatasetKey('perp', 'BTCUSDT', '1m')))")
    out = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True,
                         env={"PYTHONHASHSEED": "123", "PATH": ""}).stdout.strip()
    assert out == cat.dataset_id(DatasetKey("perp", "BTCUSDT", "1m"))


def test_dataset_ids_are_unique_per_field():
    keys = [DatasetKey(i, s, tf) for i in ("spot", "perp") for s in ("BTCUSDT", "ETHUSDT", "BTCUSDC")
            for tf in TIMEFRAMES]
    ids = {cat.dataset_id(k) for k in keys}
    assert len(ids) == len(keys) and all(cat.is_sha256_hex(i) for i in ids)


def test_dataset_id_follows_phase2_normalization():
    assert cat.dataset_id(DatasetKey("spot", "btc/usdt", "15m")) == cat.dataset_id(SPOT15)
    assert cat.dataset_id(DatasetKey(Instrument.SPOT, "BTC-USDT", "15m")) == cat.dataset_id(SPOT15)


def test_strict_dataset_key_rejects_non_canonical_fields():
    assert cat.dataset_key(exchange="binance", instrument="perp", symbol="BTCUSDT",
                           timeframe="15m") == PERP15
    for bad in ({"symbol": "btcusdt"}, {"symbol": "BTC/USDT"}, {"instrument": "PERP"},
                {"exchange": "Binance"}, {"exchange": "bybit"}, {"timeframe": "15M"}):
        fields = {"exchange": "binance", "instrument": "perp", "symbol": "BTCUSDT",
                  "timeframe": "15m", **bad}
        with pytest.raises(ValueError):
            cat.dataset_key(**fields)
    with pytest.raises(ValueError):
        cat.dataset_id(DatasetKey("spot", "BTCUSDT", "15m", exchange="bybit"))
    assert EXCHANGE == "binance"


def test_ensure_dataset_is_idempotent_and_listed(tmp_path):
    conn = _new(tmp_path)
    with cat.transaction(conn):
        a = cat.ensure_dataset(conn, PERP15)
        b = cat.ensure_dataset(conn, DatasetKey("perp", "btc-usdt", "15m"))
        cat.ensure_dataset(conn, SPOT15)
    assert a == b == cat.dataset_id(PERP15)
    assert [d["instrument"] for d in cat.list_datasets(conn)] == ["perp", "spot"]
    assert cat.list_dataset_versions(conn, a) == []


def test_dataset_id_mismatch_in_catalog_is_detected(tmp_path):
    path = tmp_path / "catalog.sqlite"
    _new(tmp_path).close()
    raw = _raw(path)
    _insert(raw, "datasets", (cat.dataset_id(PERP15), "binance", "spot", "BTCUSDT", "15m"))
    raw.close()
    with pytest.raises(cat.CatalogCorruptError, match="dataset_id"):
        cat.open_catalog(path)


def test_hash_helper():
    assert cat.is_sha256_hex(h("x"))
    for bad in BAD_HASHES + [None, 1, b"a" * 64]:
        assert not cat.is_sha256_hex(bad)


# ------------------------------------------------------------------ dump/rebuild
def test_dump_is_independent_of_insertion_order(tmp_path):
    a = cat.open_catalog(tmp_path / "a.sqlite", create=True)
    b = cat.open_catalog(tmp_path / "b.sqlite", create=True)
    with cat.transaction(a):
        populate(a)
    with cat.transaction(b):
        populate(b, reverse_rows=True)
    assert cat.dump_catalog(a) == cat.dump_catalog(b)
    lines = cat.dump_catalog(a).splitlines()
    assert json.loads(lines[0])["catalog_schema_version"] == 1
    assert sum(1 for ln in lines if ln.startswith('["datasets"')) == 2


def test_rebuild_is_deterministic_and_replaces_atomically(tmp_path, full):
    path = tmp_path / "catalog.sqlite"
    expected = cat.dump_catalog(full)
    full.close()
    d1 = cat.rebuild_catalog(path, populate)
    d2 = cat.rebuild_catalog(path, lambda c: populate(c, reverse_rows=True))
    assert d1 == d2 == hashlib.sha256(expected.encode()).hexdigest()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["catalog.sqlite"]


def test_rebuild_from_empty_state(tmp_path):
    path = tmp_path / "catalog.sqlite"
    digest = cat.rebuild_catalog(path, lambda c: None)
    fresh = cat.dump_catalog(_new(tmp_path, "fresh.sqlite"))
    assert digest == hashlib.sha256(fresh.encode()).hexdigest()


@pytest.mark.parametrize("failure", ["exception", "fk"])
def test_failed_rebuild_leaves_existing_catalog_untouched(tmp_path, full, failure):
    path = tmp_path / "catalog.sqlite"
    before = path.read_bytes()
    full.close()

    def bad(conn):
        populate(conn)
        if failure == "exception":
            raise RuntimeError("avbruten rebuild")
        conn.execute("PRAGMA defer_foreign_keys = ON")
        _insert(conn, "snapshot_members", (h("snap"), cat.dataset_id(SPOT15), h("missing")))

    with pytest.raises((RuntimeError, sqlite3.IntegrityError)):
        cat.rebuild_catalog(path, bad)
    assert path.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["catalog.sqlite"]


def test_rebuild_replaces_corrupt_catalog_and_discards_stale_journal(tmp_path):
    path = tmp_path / "catalog.sqlite"
    path.write_bytes(b"garbage" * 100)
    (tmp_path / "catalog.sqlite-journal").write_bytes(b"stale" * 100)
    cat.rebuild_catalog(path, populate)
    assert not (tmp_path / "catalog.sqlite-journal").exists()
    conn = cat.open_catalog(path, readonly=True)
    assert cat.check_catalog(conn, full=True) == [] and len(cat.list_datasets(conn)) == 2


def test_rebuild_uses_separate_staging_dir(tmp_path):
    staging = tmp_path / "tmp"
    staging.mkdir()
    cat.rebuild_catalog(tmp_path / "catalog.sqlite", populate, staging_dir=staging)
    assert list(staging.iterdir()) == []
