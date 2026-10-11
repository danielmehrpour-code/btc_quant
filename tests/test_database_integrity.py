"""PHASE 4 steg 5: djup verifiering (verify_store) och begränsad reparation (repair_store)."""
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from data_helpers import NOW, canon_frame, make_klines, write_csv

from btc_trading_system.data.ingest import ingest_dataset
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data.storage import DatasetStore, ParquetBackend
from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.data_validation import quality_report as qr
from btc_trading_system.database import catalog
from btc_trading_system.database import integrity as ig
from btc_trading_system.database import legacy as lg
from btc_trading_system.database import quality_registry as qreg
from btc_trading_system.database import versioned_store as vs

ROOT = Path(__file__).resolve().parents[1]
SPOT = DatasetKey("spot", "BTCUSDT", "15m")
PERP = DatasetKey("perp", "BTCUSDT", "15m")
H = "0123456789abcdef0123456789abcdef"


def h(tag: str) -> str:
    return hashlib.sha256(tag.encode()).hexdigest()


# ================================================================== hjälp
@pytest.fixture
def store(tmp_path):
    return vs.VersionedStore.init(tmp_path / "store")


@pytest.fixture
def src(tmp_path):
    d = tmp_path / "src"
    d.mkdir()
    return d


def csv(d: Path, name: str, n: int = 60, seed: int = 0, rows=None) -> Path:
    df = make_klines("15m", n, seed)
    if rows is not None:
        df = df.iloc[rows].reset_index(drop=True)
    return write_csv(d / name, df)


def ingest(store, files, key=SPOT, **kw):
    kw = {"now": NOW, "code_version": "test", "worktree_clean": True, **kw}
    with store.writer() as w:
        return w.ingest(key, files, **kw)


def publish(store, seed: int, n: int = 30, key=SPOT, extra=None) -> str:
    with store.writer() as w:
        return w.publish_version(key, canon_frame("15m", n, seed=seed), extra).info.data_version


def set_current(store, key, dv):
    with store.writer() as w:
        return w.set_current(key, dv)


def history(store, src) -> dict:
    """Liten historik med alla entitetstyper (ingen skada)."""
    a = ingest(store, [csv(src, "a.csv", n=60)])
    b = ingest(store, [csv(src, "b.csv", n=90)])
    p = ingest(store, [csv(src, "p.csv", n=40, seed=3)], key=PERP)
    with store.writer() as w:
        snap = w.snapshot_current([SPOT, PERP]).snapshot
    loose = publish(store, seed=9)
    return {"a": a, "b": b, "p": p, "snap": snap, "loose": loose}


def legacy_source(root: Path, specs) -> Path:
    """Hermetiskt Phase 2-lager byggt med Phase 2:s egen ingest (ParquetBackend), steg 6."""
    p2 = DatasetStore(root, ParquetBackend())
    for instrument, tf, n, seed in specs:
        f = write_csv(root.parent / f"{root.name}-{instrument}-{tf}-{seed}.csv", make_klines(tf, n, seed))
        ingest_dataset([f], instrument, tf, p2, now=NOW)
    return root


def legacy_import(store, root: Path, specs) -> lg.LegacyImportReport:
    report = lg.import_legacy(store, legacy_source(root, specs), apply=True)
    assert report.ok, report.to_json_obj()
    return report


def dataset_reports(root: Path, datasets) -> Path:
    """Phase 3:s datasetrapporter (byggare + write_report) för ett Phase 2-lager, steg 6b."""
    p2, cfg, out = DatasetStore(root, ParquetBackend()), qc.load_quality_config(), root.parent / f"{root.name}-q"
    for instrument, tf in datasets:
        key = DatasetKey(instrument, "BTCUSDT", tf)
        frame, manifest = p2.read(key)
        qr.write_report(qr.build_dataset_report(frame, key, manifest, cfg, "test", True), out)
    return out


def quality_register(store, root: Path, specs) -> qreg.QualityRegistrationReport:
    """Legacy-import och registrering av datasetrapporterna för samma dataset (steg 6b)."""
    legacy_import(store, root, specs)
    report = qreg.register_quality_reports(store, dataset_reports(root, [(i, tf) for i, tf, _, _ in specs]),
                                           apply=True)
    assert report.ok, report.to_json_obj()
    return report


def tree_hash(root: Path, *, mtime: bool = False) -> str:
    hh = hashlib.sha256()
    for p in [root, *sorted(root.rglob("*"))]:
        st = os.lstat(p)
        hh.update(f"{p.relative_to(root)}|{st.st_mode:o}|{st.st_mtime_ns if mtime else ''}|".encode())
        if stat.S_ISREG(st.st_mode):
            hh.update(p.read_bytes())
        elif stat.S_ISLNK(st.st_mode):
            hh.update(os.readlink(p).encode())
    return hh.hexdigest()


IMMUTABLE = ("objects", "refs", "ref_log.jsonl", "snapshots", "ingests", "STORE.json")


def immutable_hash(store) -> str:
    hh = hashlib.sha256()
    for name in IMMUTABLE:
        p = store.root / name
        hh.update(name.encode() + (tree_hash(p).encode() if p.is_dir() else
                                   p.read_bytes() if p.exists() else b"-"))
    return hh.hexdigest()


def rewrite(path: Path, data: bytes) -> None:
    os.chmod(path, 0o644)
    path.write_bytes(data)
    os.chmod(path, 0o444)


def corrupt_object(store, key, dv) -> None:
    """Samma storlek, samma logiska innehåll, andra bytes: bara filhashen avslöjar det."""
    d = store.object_dir(key, dv)
    os.chmod(d, 0o755)
    p = d / "bars.parquet"
    data = p.read_bytes()
    i = data.find(b"parquet-cpp-arrow")
    rewrite(p, data[:i] + b"Parquet" + data[i + 7:])
    os.chmod(d, 0o555)


def remove_object(store, key, dv) -> None:
    d = store.object_dir(key, dv)
    os.chmod(d, 0o755)
    for p in d.iterdir():
        p.unlink()
    d.rmdir()


def catalog_rows(store) -> dict[str, set]:
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        return {t: {tuple(r) for r in conn.execute(f"SELECT {', '.join(cols)} FROM {t}")}
                for t, cols, _ in ig.CATALOG_TABLES}
    finally:
        conn.close()


def dump(store) -> str:
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        return catalog.dump_catalog(conn)
    finally:
        conn.close()


def raw_sql(store, sql: str, params=()) -> None:
    conn = sqlite3.connect(store.catalog_path)
    try:
        conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def assert_derived_matches(store) -> None:
    """D-S5-g: härledda rader == skrivarens katalog, tabell för tabell."""
    report, derived = ig._verify(store, best_effort=False)
    assert derived.unknown_tables == {}
    have = catalog_rows(store)
    for t, _, _ in ig.CATALOG_TABLES:
        assert derived.rows[t] == have[t], t
    assert not report.codes & {"CATALOG_BEHIND", "CATALOG_CONFLICT", "OBJECT_UNREGISTERED"}, report.findings


def crash_child(store, body: str) -> None:
    """Kör `body` i en underprocess som dör hårt (os._exit) vid injicerad krasch."""
    code = ("import os, sys\n"
            "from btc_trading_system.database import versioned_store as vs, integrity as ig, catalog\n"
            "from btc_trading_system.data.schema import DatasetKey\n"
            "from data_helpers import NOW, canon_frame\n"
            f"st = vs.VersionedStore({str(store.root)!r})\n"
            "K = DatasetKey('spot', 'BTCUSDT', '15m')\n"
            "def die(*a, **k):\n    os._exit(77)\n"
            + body + "\nos._exit(0)\n")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "tests")]),
           "PYTHONDONTWRITEBYTECODE": "1"}
    r = subprocess.run([sys.executable, "-c", code], env=env, cwd=ROOT, capture_output=True, text=True,
                       timeout=180, check=False)
    assert r.returncode == 77, r.stderr


def non_info(report) -> set[tuple[str, str]]:
    return {(f.code, f.subject) for f in report.findings if f.severity != "INFO"}


# ================================================================== allvarlighet / rapport
def test_codes_table_is_the_approved_scale():
    assert ig.CODES == {
        "ROOT_LAYOUT": "ERROR", "STORE_FILE_INVALID": "ERROR", "ROOT_FOREIGN_ENTRY": "WARN",
        "LOCK_MISSING": "WARN", "SYMLINK": "ERROR",
        "OBJECT_LAYOUT": "ERROR", "OBJECT_CORRUPT": "ERROR", "OBJECT_DIR_MODE": "WARN",
        "OBJECT_UNREGISTERED": "REPAIRABLE", "OBJECT_UNREFERENCED": "INFO",
        "REF_LOG_CORRUPT": "ERROR", "REF_FILE_CORRUPT": "ERROR", "REF_FOREIGN_ENTRY": "ERROR",
        "REF_TARGET_MISSING": "ERROR", "PENDING_REF_UPDATE": "PENDING",
        "SNAPSHOT_CORRUPT": "ERROR", "SNAPSHOT_MEMBER_MISSING": "ERROR",
        "INGEST_CORRUPT": "ERROR", "INGEST_LOG_MISMATCH": "ERROR", "INGEST_TARGET_MISSING": "ERROR",
        "IMPORT_CORRUPT": "ERROR", "IMPORT_TARGET_MISSING": "ERROR", "IMPORT_LOG_MISMATCH": "ERROR",
        "QUALITY_CORRUPT": "ERROR", "QUALITY_TARGET_MISSING": "ERROR", "QUALITY_BINDING_MISMATCH": "ERROR",
        "PENDING_INGEST": "PENDING", "CATALOG_MISSING": "REPAIRABLE", "CATALOG_UNREADABLE": "REPAIRABLE",
        "CATALOG_NEWER_SCHEMA": "ERROR", "CATALOG_STRUCTURE": "REPAIRABLE", "CATALOG_BEHIND": "REPAIRABLE",
        "CATALOG_CONFLICT": "ERROR", "TMP_LEFTOVER": "REPAIRABLE", "TMP_UNKNOWN": "WARN",
        "CATALOG_MIGRATION_REQUIRED": "PENDING"}                          # SB-S4-MIG


@pytest.mark.parametrize("codes,expected", [
    ((), 0), (("OBJECT_UNREFERENCED",), 0), (("TMP_UNKNOWN",), 1), (("TMP_LEFTOVER",), 1),
    (("PENDING_INGEST", "TMP_LEFTOVER"), 2), (("CATALOG_CONFLICT", "PENDING_INGEST", "TMP_UNKNOWN"), 3),
    (("OBJECT_CORRUPT",), 3),
])
def test_exit_status_is_the_worst_severity(codes, expected):
    report = ig.VerifyReport(tuple(ig.Finding(c, "s", "") for c in codes), (), (), None, False)
    assert report.exit_status == expected


def test_unknown_finding_code_is_rejected():
    with pytest.raises(ValueError):
        ig.Finding("NOPE", "s", "")


def test_clean_store_after_full_history(store, src):
    ctx = history(store, src)
    r = ig.verify_store(store.root)
    assert r.exit_status == 0 and r.best_effort is False
    assert {f.code for f in r.findings} == {"OBJECT_UNREFERENCED"}
    assert r.by_code("OBJECT_UNREFERENCED")[0].subject == f"objects/{SPOT.relpath}/{ctx['loose']}"
    assert [s.check for s in r.scope] == list(ig.SCOPE_CHECKS)
    assert all(s.status == "performed" for s in r.scope)
    assert dict(r.counts) == {"datasets": 2, "ingests": 3, "objects_corrupt": 0, "objects_valid": 4,
                              "imports": 0, "funding_datasets": 0, "funding_objects_valid": 0,
                              "quality_reports": 0,
                              "ref_log_entries": 3, "refs": 2, "snapshots": 1, "tmp_entries": 0}
    assert r.catalog_dump_sha256 == hashlib.sha256(dump(store).encode("ascii")).hexdigest()


def test_report_json_is_canonical_and_complete(store, src):
    history(store, src)
    r = ig.verify_store(store.root)
    obj = json.loads(r.to_json_bytes())
    assert r.to_json_bytes() == (json.dumps(obj, sort_keys=True, indent=2) + "\n").encode()
    assert obj["format"] == "btc-quant/verify-report/v1" and obj["best_effort"] is False
    assert [s["check"] for s in obj["scope"]] == list(ig.SCOPE_CHECKS)
    assert r.report_sha256 == hashlib.sha256(r.to_json_bytes()).hexdigest()
    assert str(store.root) not in r.to_json_bytes().decode()


def _damaged_twin(root: Path, src: Path) -> bytes:
    st = vs.VersionedStore.init(root)
    ctx = history(st, src)
    corrupt_object(st, SPOT, ctx["loose"])
    (st.root / "notes.txt").write_text("x")
    (st.tmp / "junk").write_text("x")
    (st.tmp / f"{H}.ingest.json").write_text("x")
    os.unlink(st.catalog_path)
    return ig.verify_store(st.root).to_json_bytes()


def test_report_is_deterministic_and_sorted(tmp_path, src):
    a = _damaged_twin(tmp_path / "one", src)
    b = _damaged_twin(tmp_path / "two" / "deeper", src)
    assert a == b
    r = ig.verify_store(tmp_path / "one")
    assert list(r.findings) == sorted(r.findings, key=ig.Finding.sort_key)
    assert [f.severity for f in r.findings][:1] == ["ERROR"] and r.findings[-1].severity in ("WARN", "INFO")


def test_scope_states_what_was_skipped(store, src):
    history(store, src)
    log = store.root / "ref_log.jsonl"
    rewrite(log, log.read_bytes().replace(b'"seq":1', b'"seq":7', 1))
    r = ig.verify_store(store.root)
    scope = {s.check: s for s in r.scope}
    assert scope["ref_log.parse"].status == "performed"
    assert scope["refs.consistency"].status == "skipped" and scope["refs.consistency"].reason
    for t in ("ref_log", "refs", "ingest_events", "ingest_sources"):
        assert scope[f"catalog.rows.{t}"].status == "skipped"
    assert scope["catalog.rows.dataset_versions"].status == "performed"


# ================================================================== läsande / lås
def _chmod_tree(root: Path, writable: bool) -> None:
    for p in [root, *root.rglob("*")]:
        if p.is_symlink():
            continue
        mode = stat.S_IMODE(os.lstat(p).st_mode)
        os.chmod(p, (mode | 0o200) if writable else (mode & ~0o222))


def test_verify_is_strictly_read_only(store, src, monkeypatch):
    ctx = history(store, src)
    (store.tmp / f"{H}.ingest.json").write_text("x")
    with monkeypatch.context() as m:                                        # katalogen ligger efter
        m.setattr(vs.StoreWriter, "_sync_ingest_catalog", lambda self, r: None)
        ingest(store, [csv(src, "c.csv", n=120)])
    corrupt_object(store, SPOT, ctx["loose"])
    _chmod_tree(store.root, writable=False)
    try:
        frozen = tree_hash(store.root.parent, mtime=True)
        r = ig.verify_store(store.root)
        assert tree_hash(store.root.parent, mtime=True) == frozen
    finally:
        _chmod_tree(store.root, writable=True)
    assert {"OBJECT_CORRUPT", "CATALOG_BEHIND", "TMP_LEFTOVER"} <= r.codes and r.best_effort is False


def test_verify_never_creates_the_lock_file(store, src):
    history(store, src)
    os.unlink(store.root / ".lock")
    r = ig.verify_store(store.root)
    assert not os.path.lexists(store.root / ".lock")
    assert "LOCK_MISSING" in r.codes and r.best_effort is True


def test_lock_false_is_best_effort(store, src):
    history(store, src)
    r = ig.verify_store(store.root, lock=False)
    assert r.best_effort is True and json.loads(r.to_json_bytes())["best_effort"] is True


def test_verify_refuses_at_once_while_a_writer_holds_the_lock(store, src):
    history(store, src)
    with store.writer(), pytest.raises(vs.StoreLockedError):
        ig.verify_store(store.root)
    with store.writer():
        assert ig.verify_store(store.root, lock=False).best_effort is True


def test_writers_are_blocked_while_verify_runs(store, src, monkeypatch):
    history(store, src)
    seen = []
    real = ig._Verifier.check_tmp

    def probe(self):
        with pytest.raises(vs.StoreLockedError), store.writer():
            pass
        seen.append(True)
        real(self)
    monkeypatch.setattr(ig._Verifier, "check_tmp", probe)
    ig.verify_store(store.root)
    assert seen == [True]


def test_verify_detects_same_size_corruption_sha_not_bypassed(store, src):
    ctx = history(store, src)
    corrupt_object(store, SPOT, ctx["b"].record.ref.data_version)
    r = ig.verify_store(store.root)
    [f] = r.by_code("OBJECT_CORRUPT")
    assert f.subject == f"objects/{SPOT.relpath}/{ctx['b'].record.ref.data_version}"
    assert "data_file_sha256" in f.detail and r.exit_status == 3


# ================================================================== ett fall per fyndkod
def _symlink_in_objects(st, ctx):
    (st.objects / "binance" / "spot" / "LINK").symlink_to(st.objects / "binance" / "spot" / "BTCUSDT")


def _foreign_object_file(st, ctx):
    (st.objects / "binance" / "spot" / "BTCUSDT" / "notes.txt").write_text("x")


def _bad_object_dir_name(st, ctx):
    (st.objects / "binance" / "spot" / "BTCUSDT" / "15m" / "not-a-version").mkdir()


def _object_dir_writable(st, ctx):
    os.chmod(st.object_dir(SPOT, ctx["loose"]), 0o755)


def _unregistered_object(st, ctx):
    with pytest.MonkeyPatch.context() as m:
        m.setattr(vs.StoreWriter, "_register", lambda self, info, manifest: None)
        publish(st, seed=11)


def _ref_log_edited(st, ctx):
    log = st.root / "ref_log.jsonl"
    rewrite(log, log.read_bytes().replace(b'"action":"ingest"', b'"action":"set_current"', 1))


def _ref_log_writable(st, ctx):
    os.chmod(st.root / "ref_log.jsonl", 0o644)


def _ref_file_writable(st, ctx):
    os.chmod(st.root / "refs" / f"{SPOT.relpath}.json", 0o644)


def _ref_file_points_elsewhere(st, ctx):
    p = st.root / "refs" / f"{SPOT.relpath}.json"
    obj = json.loads(p.read_bytes())
    obj["ref_log_seq"] = 1
    rewrite(p, vs.canonical_json_bytes(obj))


def _foreign_ref_file(st, ctx):
    (st.root / "refs" / "binance" / "spot" / "BTCUSDT" / "notes.txt").write_text("x")


def _ref_target_missing(st, ctx):
    remove_object(st, SPOT, ctx["b"].record.ref.data_version)


def _pending_ref(st, ctx):
    def boom(self, ref, entry):
        raise RuntimeError("krasch")
    with pytest.MonkeyPatch.context() as m:
        m.setattr(vs.StoreWriter, "_write_ref_file", boom)
        with pytest.raises(RuntimeError):
            set_current(st, SPOT, ctx["loose"])


def _snapshot_writable(st, ctx):
    os.chmod(st.root / "snapshots" / f"{ctx['snap'].snapshot_id}.json", 0o644)


def _snapshot_foreign(st, ctx):
    (st.root / "snapshots" / "notes.txt").write_text("x")


def _snapshot_member_missing(st, ctx):
    remove_object(st, PERP, ctx["p"].record.ref.data_version)


def _ingest_writable(st, ctx):
    os.chmod(st.root / "ingests" / f"{ctx['a'].ingest_id}.json", 0o644)


def _ingest_record_deleted(st, ctx):
    p = st.root / "ingests" / f"{ctx['p'].ingest_id}.json"
    os.chmod(p, 0o644)
    p.unlink()


def _pending_ingest(st, ctx):
    def boom(*a, **k):
        raise RuntimeError("krasch")
    with pytest.MonkeyPatch.context() as m:
        m.setattr(vs.StoreWriter, "_append_ref_log", boom)
        with pytest.raises(RuntimeError):
            ingest(st, [csv(ctx["src"], "z.csv", n=150)])


def _catalog_missing(st, ctx):
    os.unlink(st.catalog_path)


def _catalog_garbage(st, ctx):
    data = bytearray(st.catalog_path.read_bytes())
    data[100:4000] = b"\x00" * 3900
    st.catalog_path.write_bytes(bytes(data))


def _catalog_fake_hot_journal(st, ctx):
    (st.root / "catalog.sqlite-journal").write_bytes(b"x" * 512)


def _catalog_newer(st, ctx):
    raw_sql(st, "PRAGMA user_version = 99")


def _catalog_behind(st, ctx):
    with pytest.MonkeyPatch.context() as m:
        m.setattr(vs.StoreWriter, "_sync_ingest_catalog", lambda self, r: None)
        ingest(st, [csv(ctx["src"], "c.csv", n=120)])


def _catalog_ref_behind(st, ctx):
    with pytest.MonkeyPatch.context() as m:
        m.setattr(vs.StoreWriter, "_sync_ref_catalog", lambda self, s: None)
        set_current(st, SPOT, ctx["loose"])


def _catalog_extra_row(st, ctx):
    raw_sql(st, "INSERT INTO version_sources (dataset_id, data_version, source_index, name, sha256, rows) "
                "VALUES (?, ?, 0, 'x.csv', ?, 1)", (catalog.dataset_id(SPOT), ctx["loose"], h("x")))


def _snapshot_deleted(st, ctx):
    p = st.root / "snapshots" / f"{ctx['snap'].snapshot_id}.json"
    os.chmod(p, 0o644)
    p.unlink()


def _unreferenced_object_deleted(st, ctx):
    remove_object(st, SPOT, ctx["loose"])


def _quality_dir(st, ctx):
    (st.root / "quality").mkdir()


def _quality_row(st, ctx):
    raw_sql(st, "INSERT INTO quality_reports VALUES (?, 'dataset', 'PASS', 1, 1, ?, 'c', 1, 1)",
            (h("r"), h("c")))


def _tmp_missing(st, ctx):
    shutil.rmtree(st.tmp)


def _store_file_symlink(st, ctx):
    real = st.root.parent / "STORE.real.json"
    os.replace(st.root / "STORE.json", real)
    (st.root / "STORE.json").symlink_to(real)


def _root_foreign(st, ctx):
    (st.root / "notes.txt").write_text("x")


def _lock_missing(st, ctx):
    os.unlink(st.root / ".lock")


def _tmp_unknown(st, ctx):
    (st.tmp / "junk.bin").write_text("x")


def _tmp_leftover(st, ctx):
    (st.tmp / f"{H}.snapshot.json").write_text("x")


def _import_writable(st, ctx):
    rep = legacy_import(st, ctx["src"].parent / "p2w", [("spot", "1m", 40, 5)])
    os.chmod(st.root / "imports" / f"{rep.datasets[0].import_id}.json", 0o644)


def _import_target_missing(st, ctx):
    rep = legacy_import(st, ctx["src"].parent / "p2t", [("spot", "15m", 70, 6)])    # SPOT har ref: kept
    assert rep.datasets[0].ref_outcome == lg.REF_KEPT
    remove_object(st, SPOT, rep.datasets[0].data_version)


def _import_record_deleted(st, ctx):
    rep = legacy_import(st, ctx["src"].parent / "p2d", [("perp", "1m", 40, 7)])
    p = st.root / "imports" / f"{rep.datasets[0].import_id}.json"
    os.chmod(p, 0o644)
    p.unlink()


def _quality_file(st, rep) -> Path:
    return st.root / "quality" / f"{rep.reports[0].report_sha256}.quality.json"


def _quality_writable(st, ctx):
    rep = quality_register(st, ctx["src"].parent / "q2w", [("perp", "1m", 40, 5)])
    os.chmod(_quality_file(st, rep), 0o644)


def _quality_foreign(st, ctx):
    quality_register(st, ctx["src"].parent / "q2f", [("perp", "1m", 40, 5)])
    (st.root / "quality" / "notes.txt").write_text("x")


def _quality_target_missing(st, ctx):
    rep = quality_register(st, ctx["src"].parent / "q2t", [("spot", "15m", 70, 6)])     # SPOT har ref: kept
    remove_object(st, SPOT, rep.reports[0].datasets[0][1])


def _quality_binding(st, ctx):
    """En giltig, låst, sha-namngiven rapportfil vars rows inte stämmer med objektet."""
    rep = quality_register(st, ctx["src"].parent / "q2b", [("perp", "1m", 40, 7)])
    obj = json.loads(_quality_file(st, rep).read_bytes())
    obj["datasets"][0]["rows"] += 1
    data = qr.report_to_json(obj)
    p = st.root / "quality" / f"{hashlib.sha256(data).hexdigest()}.quality.json"
    p.write_bytes(data)
    os.chmod(p, 0o444)


DAMAGE = {
    "ROOT_LAYOUT": _tmp_missing, "STORE_FILE_INVALID": _store_file_symlink, "ROOT_FOREIGN_ENTRY": _root_foreign,
    "CATALOG_CONFLICT/quality_row": _quality_row,
    "LOCK_MISSING": _lock_missing, "SYMLINK": _symlink_in_objects,
    "OBJECT_LAYOUT/file": _foreign_object_file, "OBJECT_LAYOUT/name": _bad_object_dir_name,
    "OBJECT_DIR_MODE": _object_dir_writable, "OBJECT_UNREGISTERED": _unregistered_object,
    "REF_LOG_CORRUPT/edited": _ref_log_edited, "REF_LOG_CORRUPT/writable": _ref_log_writable,
    "REF_FILE_CORRUPT/writable": _ref_file_writable, "REF_FILE_CORRUPT/seq": _ref_file_points_elsewhere,
    "REF_FOREIGN_ENTRY": _foreign_ref_file, "REF_TARGET_MISSING": _ref_target_missing,
    "PENDING_REF_UPDATE": _pending_ref,
    "SNAPSHOT_CORRUPT/writable": _snapshot_writable, "SNAPSHOT_CORRUPT/foreign": _snapshot_foreign,
    "SNAPSHOT_MEMBER_MISSING": _snapshot_member_missing,
    "INGEST_CORRUPT": _ingest_writable, "INGEST_LOG_MISMATCH": _ingest_record_deleted,
    "INGEST_TARGET_MISSING": _ref_target_missing, "PENDING_INGEST": _pending_ingest,
    "CATALOG_MISSING": _catalog_missing, "CATALOG_UNREADABLE/garbage": _catalog_garbage,
    "CATALOG_UNREADABLE/hot_journal": _catalog_fake_hot_journal, "CATALOG_NEWER_SCHEMA": _catalog_newer,
    "CATALOG_BEHIND/ingest": _catalog_behind, "CATALOG_BEHIND/refs": _catalog_ref_behind,
    "CATALOG_CONFLICT/extra_row": _catalog_extra_row, "CATALOG_CONFLICT/snapshot_deleted": _snapshot_deleted,
    "CATALOG_CONFLICT/object_deleted": _unreferenced_object_deleted,
    "TMP_UNKNOWN": _tmp_unknown, "TMP_LEFTOVER": _tmp_leftover,
    "IMPORT_CORRUPT": _import_writable, "IMPORT_TARGET_MISSING": _import_target_missing,
    "IMPORT_LOG_MISMATCH": _import_record_deleted,
    "QUALITY_CORRUPT/writable": _quality_writable, "QUALITY_CORRUPT/foreign": _quality_foreign,
    "QUALITY_TARGET_MISSING": _quality_target_missing, "QUALITY_BINDING_MISMATCH": _quality_binding,
}


@pytest.mark.parametrize("name", sorted(DAMAGE))
def test_each_finding_code_is_detected(store, src, name):
    ctx = {**history(store, src), "src": src}
    DAMAGE[name](store, ctx)
    code = name.split("/")[0]
    r = ig.verify_store(store.root)
    assert code in r.codes, r.findings
    assert r.exit_status == max(ig.SEVERITY_EXIT[ig.CODES[c]] for c in r.codes)
    assert r.exit_status >= ig.SEVERITY_EXIT[ig.CODES[code]]


def test_catalog_structure_problem_is_repairable(store, src, monkeypatch):
    """integrity_check-fel som quick_check missar kan inte framkallas pålitligt; simuleras."""
    history(store, src)
    real = catalog.check_catalog
    monkeypatch.setattr(catalog, "check_catalog",
                        lambda conn, full=False: real(conn, full=full) + (["simulerat"] if full else []))
    r = ig.verify_store(store.root)
    assert "CATALOG_STRUCTURE" in r.codes and r.exit_status == 1
    assert ig.plan_repair(r).rebuild_catalog


def test_empty_quality_dir_is_valid(store, src):
    ctx = {**history(store, src), "src": src}
    _quality_dir(store, ctx)
    r = ig.verify_store(store.root)
    assert r.exit_status == 0 and dict(r.counts)["quality_reports"] == 0, r.findings


def test_repair_rebuilds_quality_rows_and_never_touches_quality(store, src):
    history(store, src)
    rep = quality_register(store, src.parent / "q2r", [("perp", "1m", 40, 5), ("spot", "1m", 40, 6)])
    assert len(rep.reports) == 2
    final, files = dump(store), tree_hash(store.root / "quality", mtime=True)
    os.unlink(store.catalog_path)
    r = ig.verify_store(store.root)
    assert "CATALOG_MISSING" in r.codes and dict(r.counts)["quality_reports"] == 2
    ig.repair_store(store.root, apply=True)
    assert dump(store) == final
    assert tree_hash(store.root / "quality", mtime=True) == files
    assert ig.verify_store(store.root).exit_status == 0


# ================================================================== tmp/: verkliga kraschrester
TMP_CRASHES = {
    "staging_dir": (("vs.StoreWriter._publish_dir = die\n"
                     "with st.writer() as w:\n    w.publish_version(K, canon_frame('15m', 30, seed=21))\n"),
                    rf"{'[0-9a-f]' * 32}"),
    "ref_log": ("vs._rename = die\nwith st.writer() as w:\n    w.set_current(K, LOOSE)\n",
                rf"{'[0-9a-f]' * 32}\.ref_log\.jsonl"),
    "ref_file": (("real = vs._rename\nn = [0]\n"
                  "def r(a, b):\n    n[0] += 1\n    if n[0] == 2:\n        os._exit(77)\n    real(a, b)\n"
                  "vs._rename = r\nwith st.writer() as w:\n    w.set_current(K, LOOSE)\n"),
                 rf"{'[0-9a-f]' * 32}\.15m\.json"),
    "snapshot": ("vs._link = die\nwith st.writer() as w:\n    w.snapshot_current([K])\n",
                 rf"{'[0-9a-f]' * 32}\.snapshot\.json"),
    "ingest": (("vs._link = die\nwith st.writer() as w:\n"
                "    w.ingest(K, [SRC], now=NOW, code_version='t', worktree_clean=True)\n"),
               rf"{'[0-9a-f]' * 32}\.ingest\.json"),
    "import": (("from btc_trading_system.data.ingest import ingest_dataset\n"
                "from btc_trading_system.data.storage import DatasetStore, ParquetBackend\n"
                "from btc_trading_system.database import legacy\n"
                "P2 = os.path.join(os.path.dirname(SRC), 'p2')\n"
                "ingest_dataset([SRC], 'perp', '15m', DatasetStore(P2, ParquetBackend()), now=NOW)\n"
                "vs._link = die\nlegacy.import_legacy(st, P2, apply=True)\n"),
               rf"{'[0-9a-f]' * 32}\.import\.json"),
    "quality": (("from btc_trading_system.data.ingest import ingest_dataset\n"
                 "from btc_trading_system.data.storage import DatasetStore, ParquetBackend\n"
                 "from btc_trading_system.data_validation import quality_config as qc, quality_report as qr\n"
                 "from btc_trading_system.database import legacy, quality_registry as qreg\n"
                 "P2 = os.path.join(os.path.dirname(SRC), 'p2q')\n"
                 "OUT = os.path.join(os.path.dirname(SRC), 'reports')\n"
                 "ingest_dataset([SRC], 'perp', '15m', DatasetStore(P2, ParquetBackend()), now=NOW)\n"
                 "legacy.import_legacy(st, P2, apply=True)\n"
                 "P = DatasetKey('perp', 'BTCUSDT', '15m')\n"
                 "f, m = DatasetStore(P2, ParquetBackend()).read(P)\n"
                 "qr.write_report(qr.build_dataset_report(f, P, m, qc.load_quality_config(), 't', True), OUT)\n"
                 "vs._link = die\nqreg.register_quality_reports(st, OUT, apply=True)\n"),
                rf"{'[0-9a-f]' * 32}\.quality\.json"),
}


@pytest.mark.parametrize("name", sorted(TMP_CRASHES))
def test_real_crash_leftovers_are_known_patterns_and_removed(store, src, name):
    ctx = history(store, src)
    body, pattern = TMP_CRASHES[name]
    f = csv(src, "crash.csv", n=200)
    crash_child(store, f"LOOSE = {ctx['loose']!r}\nSRC = {str(f)!r}\n" + body)
    left = [p.name for p in store.tmp.iterdir()]
    assert len(left) == 1 and re.fullmatch(pattern, left[0]), left
    r = ig.verify_store(store.root)
    assert [x.subject for x in r.by_code("TMP_LEFTOVER")] == [f"tmp/{left[0]}"]
    before = immutable_hash(store)
    res = ig.repair_store(store.root, apply=True)
    assert res.applied and list(store.tmp.iterdir()) == []
    assert immutable_hash(store) == before
    assert not {"TMP_LEFTOVER", "CATALOG_BEHIND", "OBJECT_UNREGISTERED"} & res.after.codes


def test_hard_link_leftover_never_changes_the_published_record(store, src):
    history(store, src)
    f = csv(src, "crash.csv", n=200)
    crash_child(store, f"SRC = {str(f)!r}\nreal = vs._link\n"
                       "def l(a, b):\n    real(a, b)\n    os._exit(77)\n"
                       "vs._link = l\nwith st.writer() as w:\n"
                       "    w.ingest(K, [SRC], now=NOW, code_version='t', worktree_clean=True)\n")
    [left] = list(store.tmp.iterdir())
    [rec] = [p for p in (store.root / "ingests").iterdir() if os.path.samefile(p, left)]
    data = rec.read_bytes()
    r = ig.verify_store(store.root)
    assert {"TMP_LEFTOVER", "PENDING_INGEST"} <= r.codes
    res = ig.repair_store(store.root, apply=True)
    assert list(store.tmp.iterdir()) == []
    assert rec.read_bytes() == data and stat.S_IMODE(os.lstat(rec).st_mode) == 0o444
    assert os.lstat(rec).st_nlink == 1 and "PENDING_INGEST" in res.after.codes


def test_read_only_staging_tree_is_removed_without_touching_file_modes(store, src):
    history(store, src)
    d = store.tmp / H
    (d / "object").mkdir(parents=True)
    (d / "object" / "bars.parquet").write_bytes(b"x")
    os.chmod(d / "object" / "bars.parquet", 0o444)
    os.chmod(d / "object", 0o555)
    other = store.root.parent / "outside.bin"
    other.write_bytes(b"keep")
    os.link(other, d / "linked.bin")                                       # hårdlänk ut ur tmp/
    os.chmod(other, 0o444)
    ig.repair_store(store.root, apply=True)
    assert not d.exists()
    assert other.read_bytes() == b"keep" and stat.S_IMODE(os.lstat(other).st_mode) == 0o444


def test_tmp_cleanup_never_follows_symlinks_or_leaves_tmp(store, src):
    history(store, src)
    outside = store.root.parent / "outside"
    outside.mkdir()
    (outside / "precious.txt").write_text("keep")
    (store.tmp / f"{H}.ingest.json").symlink_to(outside / "precious.txt")
    (store.tmp / ("f" * 32)).symlink_to(outside)
    trap = store.tmp / ("e" * 32)
    trap.mkdir()
    (trap / "escape").symlink_to(outside)
    before = tree_hash(outside)
    r = ig.verify_store(store.root)
    assert {x.subject for x in r.by_code("TMP_UNKNOWN")} == {
        f"tmp/{H}.ingest.json", f"tmp/{'f' * 32}", f"tmp/{'e' * 32}"}
    assert not r.by_code("TMP_LEFTOVER")
    ig.repair_store(store.root, apply=True)
    assert tree_hash(outside) == before
    assert {p.name for p in store.tmp.iterdir()} == {f"{H}.ingest.json", "f" * 32, "e" * 32}
    assert (trap / "escape").is_symlink()


def test_unknown_tmp_entries_are_never_removed(store, src):
    history(store, src)
    for name in ("junk.bin", f"{H}.other.json", f"{H}.ingest.json.bak", "X" * 32, f".store-{H}.txt"):
        (store.tmp / name).write_text("x")
    (store.tmp / f"{H}.ingest.json").write_text("x")
    (store.tmp / f".store-{H}.json").write_text("x")
    for suffix in ("", "-journal", "-wal", "-shm"):
        (store.tmp / f".catalog-{H}.sqlite.tmp{suffix}").write_text("x")
    ig.repair_store(store.root, apply=True)
    assert {p.name for p in store.tmp.iterdir()} == {
        "junk.bin", f"{H}.other.json", f"{H}.ingest.json.bak", "X" * 32, f".store-{H}.txt"}


# ================================================================== repair: dry-run, vägran, låsning
def test_repair_is_a_dry_run_by_default(store, src):
    ctx = {**history(store, src), "src": src}
    _catalog_behind(store, ctx)
    (store.tmp / f"{H}.snapshot.json").write_text("x")
    before = tree_hash(store.root, mtime=True)
    res = ig.repair_store(store.root)
    assert res.applied is False and res.after is None
    assert res.plan.rebuild_catalog and res.plan.tmp_removals == (f"tmp/{H}.snapshot.json",)
    assert tree_hash(store.root, mtime=True) == before


ERROR_DAMAGE = sorted(n for n in DAMAGE if ig.CODES[n.split("/")[0]] == "ERROR")


@pytest.mark.parametrize("name", ERROR_DAMAGE)
def test_repair_refuses_every_error_and_changes_nothing(store, src, name):
    ctx = {**history(store, src), "src": src}
    (store.tmp / f"{H}.snapshot.json").write_text("x")                     # även tmp/ lämnas
    DAMAGE[name](store, ctx)
    before = tree_hash(store.root.parent, mtime=True)
    with pytest.raises(ig.RepairRefusedError) as e:
        ig.repair_store(store.root, apply=True)
    assert e.value.report.exit_status == 3
    assert tree_hash(store.root.parent, mtime=True) == before


@pytest.mark.parametrize("damage", [_catalog_extra_row, _snapshot_deleted, _unreferenced_object_deleted])
def test_catalog_evidence_is_never_rebuilt_over(store, src, damage):
    """Oberoende av kodtabellen: en CONFLICT är ERROR, repair vägrar och katalogens bevis finns kvar."""
    ctx = {**history(store, src), "src": src}
    damage(store, ctx)
    evidence = dump(store)
    r = ig.verify_store(store.root)
    conflict = r.by_code("CATALOG_CONFLICT")
    assert conflict and all(f.severity == "ERROR" for f in conflict) and r.exit_status == 3
    with pytest.raises(ig.RepairRefusedError):
        ig.repair_store(store.root, apply=True)
    assert dump(store) == evidence


def test_repair_needs_the_writer_lock(store, src):
    history(store, src)
    with store.writer(), pytest.raises(vs.StoreLockedError):
        ig.repair_store(store.root, apply=True)


def test_writers_are_blocked_while_repair_runs(store, src, monkeypatch):
    ctx = {**history(store, src), "src": src}
    _catalog_missing(store, ctx)
    seen = []
    real = ig._rebuild_catalog

    def probe(st, derived):
        with pytest.raises(vs.StoreLockedError), store.writer():
            pass
        seen.append(True)
        real(st, derived)
    monkeypatch.setattr(ig, "_rebuild_catalog", probe)
    ig.repair_store(store.root, apply=True)
    assert seen == [True]


def test_repair_and_writer_in_another_process_exclude_each_other(store, src):
    history(store, src)
    code = ("import sys, time\nfrom btc_trading_system.database import versioned_store as vs\n"
            f"st = vs.VersionedStore({str(store.root)!r})\n"
            "with st.writer():\n    print('held', flush=True)\n    sys.stdin.readline()\n")
    env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONDONTWRITEBYTECODE": "1"}
    child = subprocess.Popen([sys.executable, "-c", code], env=env, cwd=ROOT, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "held"
        with pytest.raises(vs.StoreLockedError):
            ig.repair_store(store.root, apply=True)
        with pytest.raises(vs.StoreLockedError):
            ig.verify_store(store.root)
    finally:
        child.communicate("\n", timeout=60)
    assert ig.repair_store(store.root, apply=True).applied


def test_repair_refuses_a_symlinked_lock_file(store, src):
    history(store, src)
    target = store.root.parent / "elsewhere.lock"
    target.write_text("")
    os.unlink(store.root / ".lock")
    (store.root / ".lock").symlink_to(target)
    with pytest.raises(vs.StoreError):
        ig.repair_store(store.root, apply=True)


# ================================================================== repair: katalogen
TWIN_OPS = {                       # skada -> samma operation utförd felfritt i tvillingen
    "CATALOG_BEHIND/ingest": lambda st, ctx: ingest(st, [csv(ctx["src"], "c.csv", n=120)]),
    "CATALOG_BEHIND/refs": lambda st, ctx: set_current(st, SPOT, ctx["loose"]),
    "OBJECT_UNREGISTERED": lambda st, ctx: publish(st, seed=11),
}


@pytest.mark.parametrize("damage", ["CATALOG_MISSING", "CATALOG_UNREADABLE/garbage",
                                    "CATALOG_UNREADABLE/hot_journal", "CATALOG_BEHIND/ingest",
                                    "CATALOG_BEHIND/refs", "OBJECT_UNREGISTERED"])
def test_catalog_is_rebuilt_equal_to_an_undamaged_twin(tmp_path, damage):
    def build(name: str):
        st = vs.VersionedStore.init(tmp_path / name / "store")
        d = tmp_path / name / "src"
        d.mkdir()
        return st, {**history(st, d), "src": d}
    twin, tctx = build("twin")
    if damage in TWIN_OPS:
        TWIN_OPS[damage](twin, tctx)
    st, ctx = build("damaged")
    DAMAGE[damage](st, ctx)
    before = immutable_hash(st)
    res = ig.repair_store(st.root, apply=True)
    assert res.applied and res.plan.rebuild_catalog
    assert res.after.exit_status == 0, res.after.findings
    assert immutable_hash(st) == before
    assert dump(st) == dump(twin)
    assert_derived_matches(st)
    assert not os.path.exists(st.root / "catalog.sqlite-journal")


def test_real_hot_journal_is_rolled_back_and_catalog_rebuilt(store, src):
    history(store, src)
    pristine = dump(store)
    code = ("import os, sqlite3, hashlib\n"
            f"c = sqlite3.connect({str(store.catalog_path)!r}, isolation_level=None)\n"
            "c.execute('PRAGMA journal_mode=DELETE'); c.execute('PRAGMA cache_size=1')\n"
            "c.execute('BEGIN IMMEDIATE')\n"
            "for i in range(3000):\n"
            "    s = hashlib.sha256(str(i).encode()).hexdigest()\n"
            "    c.execute(\"INSERT INTO quality_reports VALUES (?, 'dataset', 'PASS', 1, 1, ?, 'c', 1, 1)\", (s, s))\n"
            "os._exit(77)\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120,
                       check=False)
    assert r.returncode == 77, r.stderr
    journal = store.root / "catalog.sqlite-journal"
    assert journal.exists() and journal.stat().st_size > 0
    rep = ig.verify_store(store.root)
    assert "CATALOG_UNREADABLE" in rep.codes and rep.exit_status == 1
    res = ig.repair_store(store.root, apply=True)
    assert res.after.exit_status == 0 and dump(store) == pristine
    assert not journal.exists()


def test_writers_work_after_a_rebuild(store, src):
    ctx = {**history(store, src), "src": src}
    _catalog_missing(store, ctx)
    ig.repair_store(store.root, apply=True)
    ingest(store, [csv(src, "n.csv", n=130)])
    set_current(store, SPOT, ctx["loose"])
    with store.writer() as w:
        w.snapshot_current([SPOT, PERP])
    assert ig.verify_store(store.root).exit_status == 0
    assert_derived_matches(store)


# ================================================================== väntande operationer
def test_repair_never_completes_pending_operations(tmp_path, src):
    for kind in ("ref", "ingest"):
        st = vs.VersionedStore.init(tmp_path / kind)
        ctx = {**history(st, src), "src": src}
        (_pending_ref if kind == "ref" else _pending_ingest)(st, ctx)
        _catalog_missing(st, ctx)
        (st.tmp / f"{H}.snapshot.json").write_text("x")
        before = immutable_hash(st)
        res = ig.repair_store(st.root, apply=True)
        code = "PENDING_REF_UPDATE" if kind == "ref" else "PENDING_INGEST"
        assert res.applied and code in res.before.codes and code in res.after.codes
        assert res.after.exit_status == 2 and immutable_hash(st) == before
        assert_derived_matches(st)
        with st.writer() as w:
            if kind == "ref":
                assert len(w.complete_pending_ref_updates()) == 1
            else:
                assert w.complete_pending_ingest() is not None
        after = ig.verify_store(st.root)
        assert after.exit_status == 0, after.findings
        assert_derived_matches(st)


# ================================================================== D-S5-g: härledning == skrivarens katalog
def test_derived_rows_equal_the_writer_catalog_in_every_state(store, src):
    legacy = {"sources": [{"name": "legacy.csv", "rows": 30, "sha256": h("legacy")}],
              "gaps": [], "zero_volume_bars": 0}
    v1 = publish(store, seed=1, extra=legacy)                               # publicerad (med källor)
    assert_derived_matches(store)
    assert publish(store, seed=1, extra=legacy) == v1                       # återanvänd
    assert_derived_matches(store)
    ingest(store, [csv(src, "m.csv", n=60)])                                # publicerad, changed
    assert_derived_matches(store)
    d1 = csv(src, "BTCUSDT-15m-2024-01-01.csv", n=60, rows=slice(0, 40))
    d2 = csv(src, "BTCUSDT-15m-2024-01-02.csv", n=60, rows=slice(30, 60))
    same = ingest(store, [d2, d1])                                          # samma innehåll, andra filer
    assert (same.record.publication, same.record.ref_outcome) == ("reused", "unchanged")
    assert_derived_matches(store)
    ingest(store, [csv(src, "p.csv", n=40, seed=3)], key=PERP)
    long = ingest(store, [csv(src, "long.csv", n=120)])
    assert_derived_matches(store)
    with store.writer() as w:
        w.snapshot_current([SPOT, PERP])
        w.create_snapshot([long.record.ref])
    assert_derived_matches(store)
    back = ingest(store, [csv(src, "short.csv", n=50)], allow_regression=True)   # ref bakåt
    assert back.record.ref_outcome == "changed" and back.record.allow_regression
    assert_derived_matches(store)
    set_current(store, SPOT, v1)                                            # ref bakåt via set_current
    assert_derived_matches(store)
    imp = legacy_import(store, src.parent / "p2", [("spot", "1m", 50, 4), ("perp", "15m", 45, 8)])
    assert [d.ref_outcome for d in imp.datasets] == [lg.REF_SET, lg.REF_KEPT]   # legacy-import (steg 6)
    assert_derived_matches(store)
    reg = qreg.register_quality_reports(store, dataset_reports(src.parent / "p2", [("spot", "1m"), ("perp", "15m")]),
                                        apply=True)
    assert reg.ok and len(reg.reports) == 2                                 # kvalitetsrapporter (steg 6b)
    assert_derived_matches(store)
    ctx = {"loose": long.record.ref.data_version, "src": src}
    _pending_ref(store, ctx)                                                # väntande D1
    assert "PENDING_REF_UPDATE" in ig.verify_store(store.root).codes
    assert_derived_matches(store)
    with store.writer() as w:
        w.complete_pending_ref_updates()
    assert_derived_matches(store)
    _pending_ingest(store, ctx)                                             # väntande ingest
    assert "PENDING_INGEST" in ig.verify_store(store.root).codes
    assert_derived_matches(store)
    with store.writer() as w:
        w.complete_pending_ingest()
    assert_derived_matches(store)
    final = dump(store)
    os.unlink(store.catalog_path)
    ig.repair_store(store.root, apply=True)
    assert dump(store) == final


def test_ingest_crash_d1_then_completion_has_no_catalog_lag(store, src):
    """Med steg 4-fixen (P4-S4-FIX1) synkar complete_pending_ref_updates även ingest-katalogen:
    direkt efter slutförandet är katalogen lika med de härledda raderna och verify ger exit 0."""
    history(store, src)

    def boom(self, ref, entry):
        raise RuntimeError("krasch")
    with pytest.MonkeyPatch.context() as m:
        m.setattr(vs.StoreWriter, "_write_ref_file", boom)
        with pytest.raises(RuntimeError):
            ingest(store, [csv(src, "q.csv", n=150)])
    assert_derived_matches(store)                                           # väntande: lika
    assert ig.verify_store(store.root).exit_status == 2
    with store.writer() as w:
        w.complete_pending_ref_updates()
    assert_derived_matches(store)
    r = ig.verify_store(store.root)
    assert r.exit_status == 0, r.findings


# ================================================================== krasch under repair
REPAIR_CRASHES = {
    "rebuild_before_replace": "catalog.os.replace = die\n",
    "after_rebuild": "ig._remove_tmp_entry = die\n",
    "during_tmp_cleanup": ("real = ig._remove_tmp_entry\nn = [0]\n"
                           "def r(st, rel):\n    n[0] += 1\n    if n[0] == 2:\n        os._exit(77)\n"
                           "    real(st, rel)\n"
                           "ig._remove_tmp_entry = r\n"),
    "before_final_verify": ("real = ig._verify\nn = [0]\n"
                            "def v(st, best_effort):\n    n[0] += 1\n    if n[0] == 2:\n        os._exit(77)\n"
                            "    return real(st, best_effort)\n"
                            "ig._verify = v\n"),
}


@pytest.mark.parametrize("point", sorted(REPAIR_CRASHES))
def test_crash_during_repair_never_makes_the_store_worse(store, src, point):
    ctx = {**history(store, src), "src": src}
    _catalog_behind(store, ctx)
    for name in (f"{H}.snapshot.json", f"{'a' * 32}.ingest.json", f"{'b' * 32}.15m.json"):
        (store.tmp / name).write_text("x")
    before_report = ig.verify_store(store.root)
    before = immutable_hash(store)
    crash_child(store, REPAIR_CRASHES[point] + "ig.repair_store(st.root, apply=True)\n")
    assert immutable_hash(store) == before
    mid = ig.verify_store(store.root)
    new = non_info(mid) - non_info(before_report)
    assert all(code == "TMP_LEFTOVER" and subject.startswith("tmp/.catalog-") for code, subject in new), new
    assert mid.exit_status <= before_report.exit_status
    res = ig.repair_store(store.root, apply=True)
    assert res.after.exit_status == 0, res.after.findings
    assert immutable_hash(store) == before
    assert_derived_matches(store)


# ================================================================== steg 10: rebuild (P4-S8-d, P4-S9-h)
@pytest.mark.parametrize("name", ERROR_DAMAGE)
def test_rebuild_refuses_every_error_and_changes_nothing(store, src, name):
    ctx = {**history(store, src), "src": src}
    (store.tmp / f"{H}.snapshot.json").write_text("x")
    DAMAGE[name](store, ctx)
    before = tree_hash(store.root.parent, mtime=True)
    dry = ig.rebuild_catalog_store(store.root)
    assert dry.applied is False and dry.after is None and dry.blocking and dry.before.exit_status == 3
    with pytest.raises(ig.RepairRefusedError) as e:
        ig.rebuild_catalog_store(store.root, apply=True)
    assert e.value.report.exit_status == 3
    assert tree_hash(store.root.parent, mtime=True) == before


@pytest.mark.parametrize("damage", [_catalog_extra_row, _snapshot_deleted, _unreferenced_object_deleted])
def test_rebuild_never_rebuilds_over_catalog_evidence(store, src, damage):
    ctx = {**history(store, src), "src": src}
    damage(store, ctx)
    evidence = dump(store)
    assert ig.verify_store(store.root).by_code("CATALOG_CONFLICT")
    with pytest.raises(ig.RepairRefusedError):
        ig.rebuild_catalog_store(store.root, apply=True)
    assert dump(store) == evidence


def test_rebuild_refuses_a_symlinked_lock_file(store, src):
    history(store, src)
    target = store.root.parent / "elsewhere.lock"
    target.write_text("")
    os.unlink(store.root / ".lock")
    (store.root / ".lock").symlink_to(target)
    before = dump(store)
    with pytest.raises(vs.StoreError, match="symlänk"):
        ig.rebuild_catalog_store(store.root, apply=True)
    assert dump(store) == before


def test_rebuild_never_completes_pending_operations(tmp_path, src):
    for kind in ("ref", "ingest"):
        st = vs.VersionedStore.init(tmp_path / kind)
        ctx = {**history(st, src), "src": src}
        (_pending_ref if kind == "ref" else _pending_ingest)(st, ctx)
        _catalog_missing(st, ctx)
        before = immutable_hash(st)
        res = ig.rebuild_catalog_store(st.root, apply=True)
        code = "PENDING_REF_UPDATE" if kind == "ref" else "PENDING_INGEST"
        assert res.applied and code in res.before.codes and code in res.after.codes
        assert res.after.exit_status == 2 and res.identical is False and immutable_hash(st) == before
        assert_derived_matches(st)
        with st.writer() as w:
            if kind == "ref":
                assert len(w.complete_pending_ref_updates()) == 1
            else:
                assert w.complete_pending_ingest() is not None
        assert ig.verify_store(st.root).exit_status == 0
        assert_derived_matches(st)


def test_rebuild_touches_only_the_catalog(store, src):
    history(store, src)
    (store.tmp / f"{H}.snapshot.json").write_text("x")
    before, tmp_before, dump_before = immutable_hash(store), tree_hash(store.tmp), dump(store)
    res = ig.rebuild_catalog_store(store.root, apply=True)
    assert res.applied and res.identical is True and res.catalog_dump_sha256_after == res.catalog_dump_sha256_before
    assert immutable_hash(store) == before and tree_hash(store.tmp) == tmp_before and dump(store) == dump_before
    assert res.after.codes == {"TMP_LEFTOVER", "OBJECT_UNREFERENCED"}


REBUILD_CRASHES = {k: REPAIR_CRASHES[k] for k in ("rebuild_before_replace", "before_final_verify")}


@pytest.mark.parametrize("point", sorted(REBUILD_CRASHES))
def test_crash_during_rebuild_never_makes_the_store_worse(store, src, point):
    ctx = {**history(store, src), "src": src}
    _catalog_behind(store, ctx)
    before_report = ig.verify_store(store.root)
    before = immutable_hash(store)
    crash_child(store, REBUILD_CRASHES[point] + "ig.rebuild_catalog_store(st.root, apply=True)\n")
    assert immutable_hash(store) == before
    mid = ig.verify_store(store.root)
    new = non_info(mid) - non_info(before_report)
    assert all(code == "TMP_LEFTOVER" and subject.startswith("tmp/.catalog-") for code, subject in new), new
    assert mid.exit_status <= before_report.exit_status
    assert ig.repair_store(store.root, apply=True).after.exit_status == 0
    again = ig.rebuild_catalog_store(store.root, apply=True)
    assert again.identical is True and again.after.exit_status == 0
    assert immutable_hash(store) == before
    assert_derived_matches(store)


# ================================================================== steg 10: kraschtabellernas verify-fynd
def test_snapshot_crash_before_catalog_is_catalog_behind_and_repaired(store, src):
    history(store, src)
    crash_child(store, "vs.StoreWriter._register_snapshot = die\n"
                       "with st.writer() as w:\n    w.snapshot_current([K])\n")
    r = ig.verify_store(store.root)
    assert {code for code, _ in non_info(r)} == {"CATALOG_BEHIND"}, r.findings
    assert ig.repair_store(store.root, apply=True).after.exit_status == 0
    assert_derived_matches(store)


def test_ingest_crash_before_the_record_leaves_nothing_above_info(store, src):
    history(store, src)
    f = csv(src, "n.csv", n=200)
    crash_child(store, "vs.StoreWriter._publish_ingest_record = die\n"
                       f"with st.writer() as w:\n    w.ingest(K, [{str(f)!r}], now=NOW, code_version='t', "
                       "worktree_clean=True)\n")
    r = ig.verify_store(store.root)
    assert non_info(r) == set(), r.findings
    out = ingest(store, [f])
    assert (out.record.publication, out.record.ref_outcome) == ("reused", "changed")
    assert f"objects/{SPOT.relpath}/{out.record.ref.data_version}" in {x.subject for x in r.by_code("OBJECT_UNREFERENCED")}
    assert ig.verify_store(store.root).exit_status == 0


PUBLISH_CRASHES = {               # punkt -> (injektion, förväntade icke-INFO-fynd efter kraschen)
    "rename_before_chmod": ("vs.StoreWriter._finish_dir = staticmethod(die)\n",
                            {"OBJECT_DIR_MODE", "OBJECT_UNREGISTERED", "TMP_LEFTOVER"}),
    "chmod_before_catalog": ("vs.StoreWriter._register = die\n", {"OBJECT_UNREGISTERED"}),
}


@pytest.mark.parametrize("point", sorted(PUBLISH_CRASHES))
def test_publish_crash_states_have_the_stated_findings(store, src, point):
    history(store, src)
    body, expected = PUBLISH_CRASHES[point]
    crash_child(store, body + "with st.writer() as w:\n    w.publish_version(K, canon_frame('15m', 30, seed=21))\n")
    r = ig.verify_store(store.root)
    assert {code for code, _ in non_info(r)} == expected, r.findings
    publish(store, seed=21)                                  # nästa publicering verifierar, 0555, registrerar
    ig.repair_store(store.root, apply=True)                  # tmp/-rester
    assert ig.verify_store(store.root).exit_status == 0
    assert_derived_matches(store)


# ================================================================== steg 10: katalogbevis och symlänkar
def test_a_changed_immutable_catalog_row_is_a_conflict_never_rebuilt_over(store, src):
    """P4-S5-c: en ändrad (inte extra, inte saknad) rad för en oföränderlig entitet är CATALOG_CONFLICT."""
    history(store, src)
    _, derived = ig._verify(store, best_effort=False)
    rows = sorted(derived.rows["dataset_versions"], key=repr)
    i = vs._DV_COLUMNS.index("data_file_bytes")
    changed = rows[0][:i] + (rows[0][i] + 1,) + rows[0][i + 1:]
    derived.rows["dataset_versions"] = (set(rows) - {rows[0]}) | {changed}
    catalog.rebuild_catalog(store.catalog_path, ig._populate(derived), staging_dir=store.tmp)
    evidence = dump(store)
    r = ig.verify_store(store.root)
    conflict = r.by_code("CATALOG_CONFLICT")
    assert [f.subject for f in conflict] == [f"catalog:dataset_versions/{rows[0][0]}/{rows[0][1]}"], r.findings
    assert conflict[0].severity == "ERROR" and r.exit_status == 3
    for fn in (ig.repair_store, ig.rebuild_catalog_store):
        with pytest.raises(ig.RepairRefusedError):
            fn(store.root, apply=True)
    assert dump(store) == evidence


def _symlink_under(sub: str):
    def damage(st, ctx):
        d = st.root / sub
        d.mkdir(exist_ok=True)
        target = st.root.parent / f"outside-{sub}.json"
        target.write_text("{}")
        (d / "link.json").symlink_to(target)
    return damage


SYMLINK_DAMAGE = {f"SYMLINK/{sub}": _symlink_under(sub) for sub in ("refs", "snapshots", "imports", "quality")}


@pytest.mark.parametrize("name", sorted(SYMLINK_DAMAGE))
def test_symlinks_under_every_record_directory_are_errors(store, src, name):
    ctx = {**history(store, src), "src": src}
    SYMLINK_DAMAGE[name](store, ctx)
    r = ig.verify_store(store.root)
    assert [(f.subject, f.severity) for f in r.by_code("SYMLINK")] == [(f"{name.split('/')[1]}/link.json", "ERROR")]
    assert r.exit_status == 3
    before = tree_hash(store.root.parent, mtime=True)
    for fn in (ig.repair_store, ig.rebuild_catalog_store):
        with pytest.raises(ig.RepairRefusedError):
            fn(store.root, apply=True)
    assert tree_hash(store.root.parent, mtime=True) == before
