"""Etapp B steg 4: explicit katalogmigrering (SB-S4-MIG). En katalog med äldre schema migreras
aldrig automatiskt; bara migrate_catalog / CLI migrate-catalog --apply migrerar. Hermetiskt."""
import hashlib
import json
import os
import sqlite3
import stat
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from data_helpers import canon_frame
from funding_helpers import write_month

from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.database import catalog, cli
from btc_trading_system.database import integrity as ig
from btc_trading_system.database import versioned_store as vs
from btc_trading_system.database.refs import DatasetVersionRef

KEY = DatasetKey("spot", "BTCUSDT", "15m")
V1_TABLES = ("schema_migrations", "datasets", "dataset_versions", "version_sources", "ref_log", "refs",
             "snapshots", "snapshot_members", "ingest_events", "ingest_sources", "quality_reports",
             "quality_report_datasets")


def tree_hash(root: Path) -> str:
    hh = hashlib.sha256()
    for p in [root, *sorted(root.rglob("*"))]:
        st = os.lstat(p)
        hh.update(f"{p.relative_to(root)}|{st.st_mode:o}|".encode())
        if stat.S_ISREG(st.st_mode):
            hh.update(p.read_bytes())
    return hh.hexdigest()


def v1_rows(path: Path) -> dict[str, list]:
    conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        return {t: sorted(conn.execute(f"SELECT * FROM {t}").fetchall(), key=repr) for t in V1_TABLES}
    finally:
        conn.close()


@pytest.fixture
def v1_store(tmp_path):
    """Ett lager som Phase 4-koden lämnade det: data, ref och snapshot; katalogen har bara migration 1."""
    store = vs.VersionedStore.init(tmp_path / "store")
    with store.writer() as w:
        dv = w.publish_version(KEY, canon_frame("15m", 30)).info.data_version
        w.set_current(KEY, dv)
        w.create_snapshot([DatasetVersionRef(KEY, dv)])
    v2 = sqlite3.connect(store.catalog_path)
    rows = {t: v2.execute(f"SELECT * FROM {t}").fetchall() for t in V1_TABLES if t != "schema_migrations"}
    v2.close()
    staged = store.tmp / "v1.sqlite"
    conn = sqlite3.connect(staged, isolation_level=None)
    conn.execute("PRAGMA journal_mode = DELETE")
    catalog._apply(conn, catalog.MIGRATIONS[0])
    conn.execute("BEGIN")
    conn.execute("PRAGMA defer_foreign_keys = ON")
    for t, rs in rows.items():
        if rs:
            conn.executemany(f"INSERT INTO {t} VALUES ({', '.join('?' * len(rs[0]))})", rs)
    conn.execute("COMMIT")
    conn.close()
    os.replace(staged, store.catalog_path)
    return store


def _version(path: Path) -> int:
    conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()


# ================================================================== aldrig automatiskt
def test_no_open_ever_migrates(v1_store):
    path = v1_store.catalog_path
    before = path.read_bytes()
    for ro in (True, False):
        with pytest.raises(catalog.CatalogMigrationRequiredError, match="kräver migrering"):
            catalog.open_catalog(path, readonly=ro)
    assert path.read_bytes() == before and _version(path) == 1
    assert not path.with_name(path.name + "-journal").exists()


def test_every_writer_refuses_and_nothing_changes(v1_store, tmp_path):
    before = tree_hash(v1_store.root)
    with pytest.raises(catalog.CatalogMigrationRequiredError), v1_store.writer():
        pass
    f = write_month(tmp_path / "src", 2024, 1)
    with pytest.raises(catalog.CatalogMigrationRequiredError), v1_store.writer() as w:
        w.publish_funding(vs.fd.FundingKey("perp", "BTCUSDT"), [f])
    assert tree_hash(v1_store.root) == before


def test_verify_reports_migration_required_as_pending(v1_store):
    before = tree_hash(v1_store.root)
    r = ig.verify_store(v1_store.root)
    assert r.codes == {"CATALOG_MIGRATION_REQUIRED"} and r.exit_status == 2
    assert r.findings[0].severity == "PENDING" and "kräver migrering till 2" in r.findings[0].detail
    scope = {s.check: s for s in r.scope}
    assert scope["catalog.structure_full"].status == "skipped"
    assert scope["catalog.structure_full"].reason == "katalogen kräver migrering"
    assert all(scope[f"catalog.rows.{t}"].reason == "katalogen kräver migrering" for t, _, _ in ig.CATALOG_TABLES)
    assert scope["objects.verify_full"].status == "performed"
    assert tree_hash(v1_store.root) == before


def test_repair_and_rebuild_never_migrate(v1_store):
    before = tree_hash(v1_store.root)
    assert ig.repair_store(v1_store.root).plan.rebuild_catalog is False
    for fn in (ig.repair_store, ig.rebuild_catalog_store):
        with pytest.raises(ig.RepairRefusedError, match="kräver migrering"):
            fn(v1_store.root, apply=True)
    assert tree_hash(v1_store.root) == before and _version(v1_store.catalog_path) == 1


def _hot_journal(path: Path) -> None:
    code = (
        "import sqlite3, os\n"
        f"c = sqlite3.connect({str(path)!r}, isolation_level=None)\n"
        "c.execute('PRAGMA cache_size=1'); c.execute('BEGIN IMMEDIATE')\n"
        "c.execute(\"CREATE TABLE junk (x)\")\n"
        "c.executemany('INSERT INTO junk VALUES (?)', [(os.urandom(512),) for _ in range(400)])\n"
        "os._exit(0)\n")
    subprocess.run([sys.executable, "-c", code], check=True)
    journal = path.with_name(path.name + "-journal")
    assert journal.exists() and journal.stat().st_size > 0


def test_rebuild_after_a_crash_rolls_back_but_still_refuses_to_migrate(v1_store):
    rows = v1_rows(v1_store.catalog_path)
    _hot_journal(v1_store.catalog_path)
    r = ig.verify_store(v1_store.root)
    assert r.codes == {"CATALOG_UNREADABLE"}
    with pytest.raises(vs.StoreError, match="migrate-catalog"):
        ig.repair_store(v1_store.root, apply=True)
    assert _version(v1_store.catalog_path) == 1 and v1_rows(v1_store.catalog_path) == rows
    assert ig.verify_store(v1_store.root).codes == {"CATALOG_MIGRATION_REQUIRED"}


# ================================================================== explicit migrering
def test_dry_run_plans_and_writes_nothing(v1_store):
    before = tree_hash(v1_store.root)
    res = v1_store.migrate_catalog()
    assert res.applied is False and res.version_after == 1
    assert res.plan == catalog.MigrationPlan(1, 2, ((2, "funding_datasets", catalog.MIGRATIONS[1].sha256),))
    assert tree_hash(v1_store.root) == before


def test_apply_migrates_keeps_every_v1_row_and_verifies_clean(v1_store):
    rows = v1_rows(v1_store.catalog_path)
    files_before = tree_hash(v1_store.root / "objects"), tree_hash(v1_store.root / "snapshots")
    res = v1_store.migrate_catalog(apply=True)
    assert res.applied is True and res.version_after == 2
    after = v1_rows(v1_store.catalog_path)
    assert after["schema_migrations"] == rows["schema_migrations"] + [
        (2, "funding_datasets", catalog.MIGRATIONS[1].sha256)]
    assert {t: r for t, r in after.items() if t != "schema_migrations"} == \
        {t: r for t, r in rows.items() if t != "schema_migrations"}
    assert (tree_hash(v1_store.root / "objects"), tree_hash(v1_store.root / "snapshots")) == files_before
    r = ig.verify_store(v1_store.root)
    assert r.exit_status == 0 and r.findings == ()
    again = v1_store.migrate_catalog(apply=True)
    assert again.applied is False and again.plan.pending == () and again.version_after == 2
    with v1_store.writer():
        pass


def test_migration_runs_under_the_writer_lock(v1_store):
    before = tree_hash(v1_store.root)
    lock = vs._WriterLock(v1_store.root / vs.LOCK_FILE)
    lock.acquire()
    try:
        with pytest.raises(vs.StoreLockedError):
            v1_store.migrate_catalog(apply=True)
    finally:
        lock.release()
    assert tree_hash(v1_store.root) == before


def test_corrupt_or_newer_or_missing_catalog_is_never_migrated(v1_store, tmp_path):
    path = v1_store.catalog_path
    raw = sqlite3.connect(path, isolation_level=None)
    raw.execute("DROP TRIGGER datasets_no_delete")
    raw.close()
    before = path.read_bytes()
    for apply in (False, True):
        with pytest.raises(catalog.CatalogCorruptError, match="schemat avviker"):
            catalog.migrate_catalog(path, apply=apply)
    assert path.read_bytes() == before
    newer = tmp_path / "newer.sqlite"
    catalog.open_catalog(newer, create=True).close()
    raw = sqlite3.connect(newer, isolation_level=None)
    raw.execute("PRAGMA user_version = 3")
    raw.close()
    for apply in (False, True):
        with pytest.raises(catalog.CatalogVersionError, match="nyare"):
            catalog.migrate_catalog(newer, apply=apply)
    for apply in (False, True):
        with pytest.raises(catalog.CatalogError, match="saknas"):
            catalog.migrate_catalog(tmp_path / "none.sqlite", apply=apply)
    assert not (tmp_path / "none.sqlite").exists()


def test_dry_run_refuses_a_hot_journal_and_apply_rolls_it_back(v1_store):
    rows = v1_rows(v1_store.catalog_path)
    _hot_journal(v1_store.catalog_path)
    with pytest.raises(catalog.CatalogError, match="hot journal"):
        v1_store.migrate_catalog()
    res = v1_store.migrate_catalog(apply=True)
    assert res.applied and res.version_after == 2
    assert {t: r for t, r in v1_rows(v1_store.catalog_path).items() if t != "schema_migrations"} == \
        {t: r for t, r in rows.items() if t != "schema_migrations"}


# ================================================================== CLI
def run(capsys, *argv):
    code = cli.main([str(a) for a in argv], clock=lambda: datetime(2030, 1, 1, tzinfo=UTC),
                    git=lambda: ("abc", True))
    return code, json.loads(capsys.readouterr().out)


def test_cli_refuses_until_migrate_catalog_apply(v1_store, capsys, tmp_path):
    root = v1_store.root
    before = tree_hash(root)
    code, env = run(capsys, "verify", "--store", root)
    assert code == 2
    dv = json.loads((root / "refs/binance/spot/BTCUSDT/15m.json").read_bytes())["data_version"]
    f = write_month(tmp_path / "src", 2024, 1)
    for argv in (("set-current", "--store", root, "--dataset", KEY.relpath, "--data-version", dv, "--apply"),
                 ("snapshot", "create", "--store", root, "--member", f"{KEY.relpath}@{dv}", "--apply"),
                 ("ingest-funding", "--store", root, "--dataset", "binance/perp/BTCUSDT/funding", f, "--apply"),
                 ("rebuild", "--store", root, "--apply"), ("repair", "--store", root, "--apply")):
        code, env = run(capsys, *argv)
        assert code == cli.EXIT_REFUSED == 5, argv
        assert env["error"]["type"] in ("CatalogMigrationRequiredError", "RepairRefusedError"), argv
    code, env = run(capsys, "migrate-catalog", "--store", root)
    assert code == 0 and env["applied"] is False and env["result"]["migrated"] is False
    assert env["result"]["pending"] == [{"version": 2, "name": "funding_datasets",
                                         "sql_sha256": catalog.MIGRATIONS[1].sha256}]
    assert tree_hash(root) == before
    code, env = run(capsys, "migrate-catalog", "--store", root, "--apply")
    assert code == 0 and env["result"]["migrated"] is True and env["result"]["version_after"] == 2
    code, env = run(capsys, "verify", "--store", root)
    assert code == 0


def test_reads_on_a_store_that_requires_migration(v1_store, capsys):
    """Före migrate-catalog: kommandon som öppnar katalogen vägrar (exit 5), verify ger exit 2;
    snapshotfiler och objekt läses som förut (inga katalogöppningar). Ingenting skrivs."""
    root = v1_store.root
    before = tree_hash(root)
    for argv in (("list", "--store", root), ("versions", "--store", root, "--dataset", KEY.relpath),
                 ("funding-versions", "--store", root, "--dataset", "binance/perp/BTCUSDT/funding")):
        code, env = run(capsys, *argv)
        assert code == cli.EXIT_REFUSED == 5, argv
        assert env["error"]["type"] == "CatalogMigrationRequiredError", argv
    code, env = run(capsys, "verify", "--store", root)
    assert code == 2 and "CATALOG_MIGRATION_REQUIRED" in {f["code"] for f in env["result"]["findings"]}
    sid = next((root / "snapshots").iterdir()).name.removesuffix(".json")
    code, env = run(capsys, "snapshot", "show", "--store", root, "--id", sid)
    assert code == 0 and env["result"]["snapshot_id"] == sid
    pinned = v1_store.pinned(sid)
    frame, _ = pinned.read(KEY)
    assert len(frame) == 30
    frame, _, _ = v1_store.read_current(KEY)
    assert len(frame) == 30
    assert tree_hash(root) == before
    assert _version(v1_store.catalog_path) == 1
