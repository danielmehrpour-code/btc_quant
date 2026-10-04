"""PHASE 4 steg 6: legacy-import (Phase 2-lager -> versionslager); källan är skrivskyddad."""
import dataclasses
import hashlib
import inspect
import json
import os
import shutil
import stat
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
from data_helpers import NOW, make_klines, write_csv

from btc_trading_system.data.ingest import ingest_dataset
from btc_trading_system.data.provider import LocalOHLCVProvider
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data.storage import DatasetStore, ParquetBackend
from btc_trading_system.data_validation.errors import DataError, DataIntegrityError
from btc_trading_system.database import catalog
from btc_trading_system.database import integrity as ig
from btc_trading_system.database import legacy as lg
from btc_trading_system.database import versioned_store as vs
from btc_trading_system.database.refs import DatasetVersionRef, compact_json_bytes

ROOT = Path(__file__).resolve().parents[1]
SPOT1 = DatasetKey("spot", "BTCUSDT", "1m")
SPOT15 = DatasetKey("spot", "BTCUSDT", "15m")
PERP1 = DatasetKey("perp", "BTCUSDT", "1m")
PERP15 = DatasetKey("perp", "BTCUSDT", "15m")
SPECS = ((SPOT1, 120, 1), (SPOT15, 90, 2), (PERP1, 110, 3), (PERP15, 80, 4))
ORDER = sorted((k for k, _, _ in SPECS), key=catalog.dataset_id)
ZERO = ["volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]


def h(tag: str) -> str:
    return hashlib.sha256(tag.encode()).hexdigest()


# ================================================================== hjälp
def klines(key: DatasetKey, n: int, seed: int) -> pd.DataFrame:
    """Luckor (rader 7, 8, 30 saknas) och en nollvolymsbar."""
    df = make_klines(key.timeframe, n, seed).drop(index=[7, 8, 30]).reset_index(drop=True)
    df.loc[12, ZERO] = 0.0
    return df


def source_files(raw: Path, key: DatasetKey, n: int, seed: int) -> list[Path]:
    """Två överlappande källfiler (identiska dubbletter tas bort av Phase 2)."""
    raw.mkdir(parents=True, exist_ok=True)
    df = klines(key, n, seed)
    tag = f"{key.instrument.value}-{key.timeframe}-{seed}"
    return [write_csv(raw / f"{tag}-a.csv", df.iloc[: n // 2]),
            write_csv(raw / f"{tag}-b.csv", df.iloc[n // 2 - 5:])]


def build_phase2(root: Path, specs=SPECS) -> Path:
    """Hermetiskt Phase 2-lager byggt med Phase 2:s egen ingest + DatasetStore (ParquetBackend)."""
    raw = root.parent / f"{root.name}-raw"
    p2 = DatasetStore(root, ParquetBackend())
    for key, n, seed in specs:
        ingest_dataset(source_files(raw, key, n, seed), key.instrument, key.timeframe, p2, now=NOW)
    return root


@pytest.fixture
def src(tmp_path):
    return build_phase2(tmp_path / "p2")


@pytest.fixture
def store(tmp_path):
    return vs.VersionedStore.init(tmp_path / "store")


def boom(*a, **k):
    raise RuntimeError("injicerad krasch")


def run(store, source, **kw) -> lg.LegacyImportReport:
    return lg.import_legacy(store, source, **kw)


def apply(store, source) -> lg.LegacyImportReport:
    return lg.import_legacy(store, source, apply=True)


def by_rel(report) -> dict[str, lg.DatasetImportResult]:
    return {d.relpath: d for d in report.datasets}


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


IMMUTABLE = ("objects", "refs", "ref_log.jsonl", "snapshots", "ingests", "imports", "STORE.json")


def immutable_hash(store) -> str:
    hh = hashlib.sha256()
    for name in IMMUTABLE:
        p = store.root / name
        hh.update(name.encode() + (tree_hash(p).encode() if p.is_dir() else
                                   p.read_bytes() if p.exists() else b"-"))
    return hh.hexdigest()


def dump(store) -> str:
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        return catalog.dump_catalog(conn)
    finally:
        conn.close()


def catalog_rows(store, sql, params=()):
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def src_file(source: Path, key: DatasetKey, suffix: str) -> Path:
    return source / f"{key.relpath}{suffix}"


def edit_manifest(source: Path, key: DatasetKey, change) -> None:
    p = src_file(source, key, lg.MANIFEST_SUFFIX)
    m = json.loads(p.read_bytes())
    change(m)
    p.write_bytes((json.dumps(m, sort_keys=True, indent=2) + "\n").encode())


def ingest(store, key, files, **kw):
    kw = {"now": NOW, "code_version": "test", "worktree_clean": True, **kw}
    with store.writer() as w:
        return w.ingest(key, files, **kw)


def records(store) -> dict[str, lg.ImportRecord]:
    return {rec.ref.key.relpath: rec for rec, _ in lg.read_import_records(store)}


def nothing_written_for(store, key: DatasetKey) -> None:
    assert key.relpath not in records(store)
    assert not (store.root / "refs" / f"{key.relpath}.json").exists()
    assert not (store.objects / key.relpath).exists()


def non_info(report) -> set[str]:
    return {f.code for f in report.findings if f.severity != "INFO"}


def crash_child(store, source: Path, body: str) -> None:
    """Kör importen i en underprocess som dör hårt (os._exit) vid den injicerade punkten."""
    code = ("import os, sys\n"
            "from btc_trading_system.database import versioned_store as vs, legacy as lg\n"
            f"st = vs.VersionedStore({str(store.root)!r})\n"
            "def die(*a, **k):\n    os._exit(77)\n"
            + body + f"\nlg.import_legacy(st, {str(source)!r}, apply=True)\nos._exit(0)\n")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "tests")]),
           "PYTHONDONTWRITEBYTECODE": "1"}
    r = subprocess.run([sys.executable, "-c", code], env=env, cwd=ROOT, capture_output=True, text=True,
                       timeout=300, check=False)
    assert r.returncode == 77, r.stderr


# ================================================================== API
def test_public_api_is_pinned():
    assert str(inspect.signature(lg.import_legacy)) == (
        "(store: 'vs.VersionedStore', source_root: 'str | Path', *, apply: 'bool' = False) "
        "-> 'LegacyImportReport'")
    assert str(inspect.signature(lg.read_import_records)) == (
        "(store: 'vs.VersionedStore') -> 'tuple[tuple[ImportRecord, str], ...]'")
    assert [f.name for f in dataclasses.fields(lg.ImportRecord)] == [
        "ref", "data_file_sha256", "data_file_bytes", "manifest_sha256", "manifest_bytes", "manifest_text"]
    assert (lg.LEGACY_IMPORT_FORMAT, lg.IMPORT_ID_DOMAIN, lg.IMPORTS_DIR) == (
        "btc-quant/legacy-import/v1", b"btc-quant/legacy-import-id/v1\x00", "imports")
    assert (lg.REF_SET, lg.REF_UNCHANGED, lg.REF_KEPT) == (
        "set", "unchanged", "kept: ref points to another version")
    assert lg.ALLOWED_MANIFEST_KEYS == vs.MANIFEST_BASE_KEYS | {"gaps", "zero_volume_bars", "sources",
                                                               "duplicates_dropped"}
    assert lg.COMPARED_MANIFEST_KEYS == vs.MANIFEST_BASE_KEYS | {"gaps", "zero_volume_bars"}


def test_error_classes():
    assert issubclass(lg.LegacyImportError, vs.StoreError)
    assert issubclass(lg.ImportRecordError, DataError) and issubclass(lg.ImportRecordError, ValueError)
    assert issubclass(lg.ImportCorruptError, DataIntegrityError)


# ================================================================== dry-run och import
def test_dry_run_is_the_default_and_writes_nothing(store, src):
    before, src_before = tree_hash(store.root, mtime=True), tree_hash(src, mtime=True)
    r = run(store, src)
    assert r.applied is False and r.ok
    assert [d.relpath for d in r.datasets] == [k.relpath for k in ORDER]
    assert {(d.status, d.publication, d.record, d.ref_outcome) for d in r.datasets} == {
        ("planned", "published", "recorded", lg.REF_SET)}
    assert tree_hash(store.root, mtime=True) == before
    assert tree_hash(src, mtime=True) == src_before
    assert r.source_tree_sha256_before == r.source_tree_sha256_after == lg.source_tree_sha256(src)


def test_dry_run_predicts_exactly_what_apply_does(store, src):
    plan = run(store, src)
    done = apply(store, src)
    strip = [dataclasses.replace(d, status="x") for d in plan.datasets]
    assert strip == [dataclasses.replace(d, status="x") for d in done.datasets]
    again = run(store, src)
    assert [(d.publication, d.record, d.ref_outcome) for d in again.datasets] == [
        ("reused", "existing", lg.REF_UNCHANGED)] * 4


def test_apply_imports_every_dataset_byte_for_byte(store, src):
    src_before = lg.source_tree_sha256(src)
    r = apply(store, src)
    assert r.applied and r.ok and r.failure is None and r.unrecognized == ()
    recs = records(store)
    for key in ORDER:
        d = by_rel(r)[key.relpath]
        assert (d.status, d.publication, d.record, d.ref_outcome) == (
            "imported", "published", "recorded", lg.REF_SET)
        data = src_file(src, key, lg.DATA_SUFFIX).read_bytes()
        mbytes = src_file(src, key, lg.MANIFEST_SUFFIX).read_bytes()
        obj = store.object_dir(key, d.data_version)
        assert (obj / "bars.parquet").read_bytes() == data                       # I5
        assert (obj / "manifest.json").read_bytes() == mbytes
        assert d.data_version == json.loads(mbytes)["data_version"]              # I4
        info = store.verify_version(key, d.data_version)
        assert (info.data_file_sha256, info.manifest_sha256) == (
            hashlib.sha256(data).hexdigest(), hashlib.sha256(mbytes).hexdigest())
        rec = recs[key.relpath]
        assert (rec.import_id, rec.manifest_text.encode()) == (d.import_id, mbytes)
        assert (rec.data_file_sha256, rec.data_file_bytes) == (hashlib.sha256(data).hexdigest(), len(data))
        path = store.root / "imports" / f"{d.import_id}.json"
        assert stat.S_IMODE(os.lstat(path).st_mode) == 0o444
        assert hashlib.sha256(path.read_bytes()).hexdigest() == d.record_sha256
        assert store.current_ref(key).ref.data_version == d.data_version
        did = catalog.dataset_id(key)
        legacy_sources = json.loads(mbytes)["sources"]
        assert catalog_rows(store, "SELECT name, sha256, rows FROM version_sources WHERE dataset_id = ? "
                                   "ORDER BY source_index", (did,)) == [
            (s["name"], s["sha256"], s["rows"]) for s in legacy_sources]
    entries = vs.parse_ref_log(store._read_ref_log())
    assert [(e.seq, e.action, e.ref.key) for e in entries] == [
        (i, "import_legacy", k) for i, k in enumerate(ORDER, start=1)]
    v = ig.verify_store(store.root)
    assert v.exit_status == 0 and v.findings == () and dict(v.counts)["imports"] == 4
    assert lg.source_tree_sha256(src) == src_before
    final = dump(store)
    os.unlink(store.catalog_path)
    ig.repair_store(store.root, apply=True)
    assert dump(store) == final


def test_reads_equal_phase2_provider_and_as_of_boundaries(store, src):
    r = apply(store, src)
    p2 = DatasetStore(src, ParquetBackend())
    with store.writer() as w:
        snap = w.snapshot_current(list(ORDER)).snapshot
    pinned = store.pinned(snap.snapshot_id)
    for key in ORDER:
        dv = by_rel(r)[key.relpath].data_version
        frame2, manifest2 = p2.read(key)
        frame4, manifest4 = store.read_version(key, dv)
        pd.testing.assert_frame_equal(frame4, frame2)
        assert manifest4 == manifest2
        pd.testing.assert_frame_equal(pinned.read(key)[0], frame2)
        provider = LocalOHLCVProvider(p2, key.instrument)
        tf = timedelta(minutes=int(key.timeframe[:-1]))
        start, end = frame2["open_time"].iloc[0].to_pydatetime(), frame2["open_time"].iloc[-1].to_pydatetime()
        for k in (0, 20, len(frame2) - 1):
            avail = (frame2["open_time"].iloc[k] + tf).to_pydatetime()
            for as_of, included in ((avail, True), (avail - timedelta(milliseconds=1), False)):
                prov = provider.get_bars(key.symbol, key.timeframe, start, end + tf, as_of=as_of)
                mine = pinned.read(key, as_of=as_of)[0]
                assert list(mine["open_time"]) == list(prov.index)
                assert (frame2["open_time"].iloc[k] in set(mine["open_time"])) is included
        gaps = provider.get_gaps(key.symbol, key.timeframe)
        assert [(g.gap_start.isoformat(), g.gap_end.isoformat(), int(g.missing_bars))
                for g in gaps.itertuples()] == [
            (g["gap_start"], g["gap_end"], g["missing_bars"]) for g in manifest4["gaps"]]
        assert manifest4["gap_count"] == len(gaps) == 2 and manifest4["zero_volume_bars"] == 1


def test_reimport_is_idempotent_byte_for_byte(store, src):
    first = apply(store, src)
    before = tree_hash(store.root)
    again = apply(store, src)
    assert again.ok and [(d.publication, d.record, d.ref_outcome) for d in again.datasets] == [
        ("reused", "existing", lg.REF_UNCHANGED)] * 4
    assert [d.import_id for d in again.datasets] == [d.import_id for d in first.datasets]
    assert tree_hash(store.root) == before                                        # I9


def test_import_is_deterministic(tmp_path, src):
    a = vs.VersionedStore.init(tmp_path / "a")
    b = vs.VersionedStore.init(tmp_path / "b")
    ra, rb = apply(a, src), apply(b, src)
    assert ra.to_json_bytes() == rb.to_json_bytes()
    assert immutable_hash(a) == immutable_hash(b) and dump(a) == dump(b)
    assert str(tmp_path) not in ra.to_json_bytes().decode()


GOLDEN_MANIFEST = json.dumps({"data_version": h("dv"), "exchange": "binance", "instrument": "spot",
                              "symbol": "BTCUSDT", "timeframe": "15m"}, sort_keys=True, indent=2) + "\n"
GOLDEN_IMPORT_ID = "572e3fd420b74428ea9dc2ec65f1b28c5d71fef0f817e6286edb9441dc2483cd"


def golden_record() -> lg.ImportRecord:
    return lg.ImportRecord(DatasetVersionRef(SPOT15, h("dv")), h("data"), 1234,
                           hashlib.sha256(GOLDEN_MANIFEST.encode()).hexdigest(), len(GOLDEN_MANIFEST),
                           GOLDEN_MANIFEST)


def test_golden_import_id():
    rec = golden_record()
    assert rec.import_id == GOLDEN_IMPORT_ID
    assert rec.import_id == hashlib.sha256(
        b"btc-quant/legacy-import-id/v1\x00" + json.dumps(rec.to_json_obj(), sort_keys=True,
                                                          separators=(",", ":")).encode()).hexdigest()
    assert lg.ImportRecord.from_file_bytes(rec.to_file_bytes(), expected_id=rec.import_id) == rec
    assert set(rec.to_json_obj()) == {"format", "dataset", "dataset_id", "data_version", "source",
                                      "manifest_text"}


def test_bytes_are_preserved_for_any_valid_layout(store, src):
    """Ett giltigt men icke-kanoniskt manifest och en Parquet-fil med annan radgruppslayout
    (båda klarar Phase 2-läsning) publiceras byte för byte."""
    mp = src_file(src, SPOT15, lg.MANIFEST_SUFFIX)
    mp.write_bytes(json.dumps(json.loads(mp.read_bytes()), indent=4).encode())
    dp = src_file(src, PERP1, lg.DATA_SUFFIX)
    pq.write_table(pq.read_table(dp), dp, row_group_size=7, compression="gzip")
    DatasetStore(src, ParquetBackend()).read(SPOT15)
    DatasetStore(src, ParquetBackend()).read(PERP1)
    r = apply(store, src)
    assert r.ok
    for key in (SPOT15, PERP1):
        obj = store.object_dir(key, by_rel(r)[key.relpath].data_version)
        assert (obj / "manifest.json").read_bytes() == src_file(src, key, lg.MANIFEST_SUFFIX).read_bytes()
        assert (obj / "bars.parquet").read_bytes() == src_file(src, key, lg.DATA_SUFFIX).read_bytes()
    assert ig.verify_store(store.root).exit_status == 0


def test_read_only_source_is_imported_and_untouched(store, src):
    for p in sorted(src.rglob("*"), reverse=True):
        os.chmod(p, 0o555 if p.is_dir() else 0o444)
    os.chmod(src, 0o555)
    before = tree_hash(src, mtime=True)
    assert apply(store, src).ok
    assert tree_hash(src, mtime=True) == before


def test_source_tree_hash_is_the_treehash_sh_method(tmp_path, src):
    (src / "binance" / "spot" / "LINK").symlink_to(src / "binance" / "spot" / "BTCUSDT")
    (src / "file-link").symlink_to(src_file(src, SPOT15, lg.MANIFEST_SUFFIX))
    os.chmod(src_file(src, PERP1, lg.DATA_SUFFIX), 0o600)
    script = ('cd "$1" && find . -print0 | LC_ALL=C sort -z | while IFS= read -r -d \'\' p; do\n'
              '  if [ -f "$p" ]; then printf \'%s %s %s\\n\' "$p" "$(stat -c %a "$p")" '
              '"$(sha256sum < "$p" | cut -c1-64)"\n'
              '  else printf \'%s %s dir\\n\' "$p" "$(stat -c %a "$p")"; fi\n'
              'done | sha256sum | cut -c1-64\n')
    out = subprocess.run(["bash", "-c", script, "treehash", str(src)], capture_output=True, text=True,
                         check=True).stdout.strip()
    assert lg.source_tree_sha256(src) == out


def test_source_changed_during_import_fails_the_import(store, src, monkeypatch):
    real = lg._check_content

    def touching(frame, cap):
        real(frame, cap)
        (src / "binance" / "touched.txt").write_text("x")
    monkeypatch.setattr(lg, "_check_content", touching)
    r = apply(store, src)
    assert r.failure == "SOURCE_CHANGED" and not r.ok
    assert r.source_tree_sha256_before != r.source_tree_sha256_after
    assert json.loads(r.to_json_bytes())["failure"] == "SOURCE_CHANGED"


def test_source_without_datasets_fails_the_import_and_writes_nothing(store, tmp_path):
    """Noll igenkända dataset -> SOURCE_EMPTY, ok=False, ingenting skrivs (även i dry-run)."""
    empty, only_other = tmp_path / "empty", tmp_path / "only-other"
    empty.mkdir()
    (only_other / "binance" / "spot" / "BTCUSDT").mkdir(parents=True)
    (only_other / "README.txt").write_text("x")
    before = tree_hash(store.root, mtime=True)
    for source, unrecognized in ((empty, ()), (only_other, ("README.txt",))):
        src_before = tree_hash(source, mtime=True)
        for mode in (False, True):
            r = lg.import_legacy(store, source, apply=mode)
            assert (r.applied, r.datasets, r.unrecognized) == (mode, (), unrecognized)
            assert r.failure == "SOURCE_EMPTY" and not r.ok
            assert json.loads(r.to_json_bytes())["failure"] == "SOURCE_EMPTY"
            assert r.source_tree_sha256_before == r.source_tree_sha256_after
        assert tree_hash(source, mtime=True) == src_before
    assert tree_hash(store.root, mtime=True) == before
    assert ig.verify_store(store.root).exit_status == 0


def test_a_recognised_but_failed_dataset_is_not_source_empty(store, tmp_path):
    source = tmp_path / "half"
    (source / "binance" / "spot" / "BTCUSDT").mkdir(parents=True)
    (source / "binance" / "spot" / "BTCUSDT" / "15m.manifest.json").write_text("{}")
    r = apply(store, source)
    assert [(d.relpath, d.status, d.failure) for d in r.datasets] == [
        (SPOT15.relpath, "failed", "SOURCE_UNREADABLE")]
    assert r.failure is None and not r.ok


def test_source_changed_wins_over_source_empty(store, tmp_path, monkeypatch):
    empty = tmp_path / "empty"
    empty.mkdir()
    real = lg._discover

    def touching(s):
        found = real(s)
        (empty / "touched.txt").write_text("x")
        return found
    monkeypatch.setattr(lg, "_discover", touching)
    r = apply(store, empty)
    assert r.failure == "SOURCE_CHANGED" and not r.ok and r.datasets == ()


# ================================================================== fel per dataset
def _flip_content(src):
    p = src_file(src, SPOT15, lg.DATA_SUFFIX)
    df = ParquetBackend().read(p)
    df.loc[3, "close"] = df.loc[3, "close"] + 0.01
    df.loc[3, "high"] = max(df.loc[3, "high"], df.loc[3, "close"])
    ParquetBackend().write(p, df)


def _symlinked_data(src):
    p = src_file(src, SPOT15, lg.DATA_SUFFIX)
    real = src.parent / "elsewhere.parquet"
    shutil.move(p, real)
    p.symlink_to(real)


def _set(**kv):
    return lambda src: edit_manifest(src, SPOT15, lambda m: m.update(kv))


def _drop(name):
    return lambda src: edit_manifest(src, SPOT15, lambda m: m.pop(name))


def _shift(name):
    return lambda src: edit_manifest(src, SPOT15, lambda m: m.update(
        {name: (pd.Timestamp(m[name]) + timedelta(minutes=15)).isoformat()}))


def _raw_manifest(data):
    return lambda src: src_file(src, SPOT15, lg.MANIFEST_SUFFIX).write_bytes(data)


SOURCE_DAMAGE = {
    "parquet_content": (_flip_content, "SOURCE_VERIFY_FAILED"),
    "data_version": (_set(data_version=h("x")), "SOURCE_VERIFY_FAILED"),
    "rows": (lambda s: edit_manifest(s, SPOT15, lambda m: m.update(rows=m["rows"] + 1)), "SOURCE_VERIFY_FAILED"),
    "symbol": (_set(symbol="ETHUSDT"), "SOURCE_MANIFEST_INVALID"),
    "instrument": (_set(instrument="perp"), "SOURCE_MANIFEST_INVALID"),
    "first_open_time": (_shift("first_open_time"), "SOURCE_MANIFEST_MISMATCH"),
    "last_open_time": (_shift("last_open_time"), "SOURCE_MANIFEST_MISMATCH"),
    "gap_count": (_set(gap_count=5), "SOURCE_MANIFEST_MISMATCH"),
    "missing_bars_total": (_set(missing_bars_total=9), "SOURCE_MANIFEST_MISMATCH"),
    "gaps": (_set(gaps=[]), "SOURCE_MANIFEST_MISMATCH"),
    "zero_volume_bars": (_set(zero_volume_bars=0), "SOURCE_MANIFEST_MISMATCH"),
    "unknown_key": (_set(note="x"), "SOURCE_MANIFEST_INVALID"),
    "missing_key": (_drop("zero_volume_bars"), "SOURCE_MANIFEST_INVALID"),
    "schema_version": (_set(schema_version=2), "SOURCE_MANIFEST_INVALID"),
    "int_field_bool": (_set(gap_count=True), "SOURCE_MANIFEST_INVALID"),
    "sources": (_set(sources=[{"name": "", "rows": 1, "sha256": h("s")}]), "SOURCE_MANIFEST_INVALID"),
    "duplicates_dropped": (_set(duplicates_dropped=-1), "SOURCE_MANIFEST_INVALID"),
    "not_json": (_raw_manifest(b"{"), "SOURCE_MANIFEST_INVALID"),
    "not_utf8": (_raw_manifest(b"\xff\xfe{}"), "SOURCE_MANIFEST_INVALID"),
    "not_object": (_raw_manifest(b"[]\n"), "SOURCE_MANIFEST_INVALID"),
    "missing_data_file": (lambda s: src_file(s, SPOT15, lg.DATA_SUFFIX).unlink(), "SOURCE_UNREADABLE"),
    "missing_manifest": (lambda s: src_file(s, SPOT15, lg.MANIFEST_SUFFIX).unlink(), "SOURCE_UNREADABLE"),
    "symlinked_data_file": (_symlinked_data, "SOURCE_UNREADABLE"),
}


@pytest.mark.parametrize("name", sorted(SOURCE_DAMAGE))
def test_a_bad_source_dataset_fails_alone_and_writes_nothing(store, src, name):
    damage, code = SOURCE_DAMAGE[name]
    damage(src)
    before = lg.source_tree_sha256(src)
    for mode in (False, True):
        r = lg.import_legacy(store, src, apply=mode)
        bad = by_rel(r)[SPOT15.relpath]
        assert (bad.status, bad.failure) == ("failed", code), bad
        assert bad.data_version is None and bad.import_id is None and not r.ok
        assert str(src) not in bad.detail and str(store.root) not in bad.detail
        assert {d.relpath for d in r.datasets if d.status != "failed"} == {
            k.relpath for k in ORDER if k != SPOT15}
        assert r.failure is None and r.source_tree_sha256_after == before
    nothing_written_for(store, SPOT15)
    assert list(store.tmp.iterdir()) == []
    assert ig.verify_store(store.root).exit_status == 0


def test_unrecognized_entries_are_reported_and_never_followed(store, src, tmp_path):
    spot = src / "binance" / "spot"
    (src / "README.txt").write_text("x")
    (src / "other").mkdir()
    (spot / "BTCUSDT" / "15m.parquet.tmp").write_text("x")
    (spot / "BTCUSDT" / "notes").mkdir()
    (spot / "btcusdt").mkdir()
    (spot / "btcusdt" / "15m.parquet").write_text("x")
    (src / "binance" / "futures").mkdir()
    (spot / "LINK").symlink_to(spot / "BTCUSDT")
    shutil.copy(src_file(src, SPOT15, lg.DATA_SUFFIX), spot / "BTCUSDT" / "1h.parquet")
    r = apply(store, src)
    assert r.unrecognized == ("README.txt", "binance/futures", "binance/spot/BTCUSDT/15m.parquet.tmp",
                              "binance/spot/BTCUSDT/notes", "binance/spot/LINK", "binance/spot/btcusdt",
                              "other")
    assert not r.ok
    assert (by_rel(r)["binance/spot/BTCUSDT/1h"].failure) == "SOURCE_UNREADABLE"
    assert {d.relpath for d in r.datasets if d.status == "imported"} == {k.relpath for k in ORDER}
    assert not (store.objects / "binance" / "spot" / "LINK").exists()


# ================================================================== rötter, lås, väntande
def _store_inside_source(tmp_path, store, src):
    return vs.VersionedStore.init(src / "nested-store"), src


def _source_inside_store(tmp_path, store, src):
    inner = store.root / "legacy"
    shutil.copytree(src, inner)
    return store, inner


ROOTS = {
    "same": lambda t, st, s: (st, st.root),
    "store_inside_source": _store_inside_source,
    "source_inside_store": _source_inside_store,
    "source_is_a_store": lambda t, st, s: (st, vs.VersionedStore.init(t / "other").root),
    "source_is_a_symlink": lambda t, st, s: (st, (t / "link").symlink_to(s) or t / "link"),
    "source_missing": lambda t, st, s: (st, t / "missing"),
    "source_is_a_file": lambda t, st, s: (st, src_file(s, SPOT15, lg.MANIFEST_SUFFIX)),
}


@pytest.mark.parametrize("name", sorted(ROOTS))
def test_invalid_roots_are_refused_before_any_write(tmp_path, store, src, name):
    target, source = ROOTS[name](tmp_path, store, src)
    before = tree_hash(target.root, mtime=True)
    for mode in (False, True):
        with pytest.raises(lg.LegacyImportError):
            lg.import_legacy(target, source, apply=mode)
    assert tree_hash(target.root, mtime=True) == before


def test_second_writer_is_refused_at_once(store, src):
    before = tree_hash(store.root)
    with store.writer():
        for mode in (False, True):
            with pytest.raises(vs.StoreLockedError):
                lg.import_legacy(store, src, apply=mode)
    assert tree_hash(store.root) == before


def _ingested(store, tmp_path, key=SPOT15, n=60, seed=9):
    return ingest(store, key, source_files(tmp_path / "raw4", key, n, seed))


def test_pending_ref_update_refuses_the_import(store, src, tmp_path, monkeypatch):
    a = _ingested(store, tmp_path)
    _ingested(store, tmp_path, n=70, seed=10)
    with monkeypatch.context() as m:
        m.setattr(vs.StoreWriter, "_write_ref_file", boom)
        with pytest.raises(RuntimeError), store.writer() as w:
            w.set_current(SPOT15, a.record.ref.data_version)
    before = tree_hash(store.root)
    for mode in (False, True):
        with pytest.raises(vs.PendingRefUpdateError):
            lg.import_legacy(store, src, apply=mode)
    assert tree_hash(store.root) == before


def test_pending_ingest_refuses_the_import(store, src, tmp_path, monkeypatch):
    _ingested(store, tmp_path)
    with monkeypatch.context() as m:
        m.setattr(vs.StoreWriter, "_append_ref_log", boom)
        with pytest.raises(RuntimeError):
            _ingested(store, tmp_path, n=70, seed=10)
    before = tree_hash(store.root)
    for mode in (False, True):
        with pytest.raises(vs.PendingIngestError):
            lg.import_legacy(store, src, apply=mode)
    assert tree_hash(store.root) == before


# ================================================================== befintliga refs och objekt
def test_existing_ref_to_another_version_is_kept(store, src, tmp_path):
    other = _ingested(store, tmp_path).record.ref.data_version
    ref_file = store.root / "refs" / f"{SPOT15.relpath}.json"
    ref_before = ref_file.read_bytes()
    r = apply(store, src)
    d = by_rel(r)[SPOT15.relpath]
    assert r.ok and (d.publication, d.record, d.ref_outcome, d.current_data_version) == (
        "published", "recorded", lg.REF_KEPT, other)
    assert ref_file.read_bytes() == ref_before
    assert store.has_version(SPOT15, d.data_version) and SPOT15.relpath in records(store)
    v = ig.verify_store(store.root)
    assert v.exit_status == 0 and v.findings == ()                     # objektet refereras av importposten
    assert "kept: ref points to another version" in r.to_json_bytes().decode()


def test_reuse_never_compares_source_fields(store, src, tmp_path):
    """Samma innehåll finns redan (Phase 4-ingest, manifest utan sources): reused, posten bevarar
    legacy-manifestet exakt, inget fel (P4-S4-01)."""
    out = ingest(store, SPOT15, source_files(tmp_path / "raw-same", SPOT15, 90, 2))
    dv = out.record.ref.data_version
    assert "sources" not in store.read_version(SPOT15, dv)[1]
    r = apply(store, src)
    d = by_rel(r)[SPOT15.relpath]
    assert r.ok and (d.data_version, d.publication, d.record, d.ref_outcome) == (
        dv, "reused", "recorded", lg.REF_UNCHANGED)
    assert records(store)[SPOT15.relpath].manifest_text.encode() == \
        src_file(src, SPOT15, lg.MANIFEST_SUFFIX).read_bytes()
    assert "sources" not in store.read_version(SPOT15, dv)[1]           # objektet skrivs aldrig om
    assert ig.verify_store(store.root).exit_status == 0


def test_reuse_conflict_on_content_derived_values_fails_the_dataset(store, src):
    frame, manifest = DatasetStore(src, ParquetBackend()).read(SPOT15)
    with store.writer() as w:
        w.publish_version(SPOT15, frame, {"gaps": manifest["gaps"], "zero_volume_bars": 7})
    r = apply(store, src)
    d = by_rel(r)[SPOT15.relpath]
    assert (d.status, d.failure) == ("failed", "LEGACY_OBJECT_CONFLICT") and "zero_volume_bars" in d.detail
    assert SPOT15.relpath not in records(store)
    assert not (store.root / "refs" / f"{SPOT15.relpath}.json").exists()


def test_corrupt_existing_object_is_never_reused(store, src, tmp_path):
    dv = ingest(store, SPOT15, source_files(tmp_path / "raw-same", SPOT15, 90, 2)).record.ref.data_version
    d = store.object_dir(SPOT15, dv)
    os.chmod(d, 0o755)
    p = d / "bars.parquet"
    data = p.read_bytes()
    i = data.find(b"parquet-cpp-arrow")
    os.chmod(p, 0o644)
    p.write_bytes(data[:i] + b"Parquet" + data[i + 7:])
    os.chmod(p, 0o444)
    os.chmod(d, 0o555)
    with pytest.raises(vs.ObjectCorruptError):
        apply(store, src)
    assert SPOT15.relpath not in records(store)
    assert [e.action for e in vs.parse_ref_log(store._read_ref_log()) if e.ref.key == SPOT15] == ["ingest"]


# ================================================================== importposter
def test_record_is_written_before_the_ref_moves(store, src, monkeypatch):
    real = vs.StoreWriter._append_ref_log
    seen = []

    def checking(self, state, line, entry):
        assert entry.ref.key.relpath in records(store)                  # I8
        seen.append(entry.action)
        real(self, state, line, entry)
    monkeypatch.setattr(vs.StoreWriter, "_append_ref_log", checking)
    apply(store, src)
    assert seen == ["import_legacy"] * 4


def test_tampered_existing_record_is_refused_and_never_overwritten(store, src):
    plan = run(store, src)
    d = by_rel(plan)[PERP1.relpath]
    path = store.root / "imports" / f"{d.import_id}.json"
    path.parent.mkdir()
    path.write_bytes(b"intruder")
    os.chmod(path, 0o444)
    with pytest.raises(lg.ImportCorruptError):
        apply(store, src)
    assert path.read_bytes() == b"intruder"


def test_record_is_never_overwritten_even_in_a_race(store, src, monkeypatch):
    real = vs._fsync_file
    seen = {}

    def racing(p):
        real(p)
        if Path(p).name.endswith(".import.json") and not seen:
            obj = json.loads(Path(p).read_bytes())
            iid = hashlib.sha256(lg.IMPORT_ID_DOMAIN + compact_json_bytes(obj)).hexdigest()
            target = store.root / "imports" / f"{iid}.json"
            target.write_bytes(b"intruder")
            seen["target"] = target
    monkeypatch.setattr(vs, "_fsync_file", racing)
    with pytest.raises(vs.ImmutableObjectError):
        apply(store, src)
    assert seen["target"].read_bytes() == b"intruder"


def _golden_obj():
    return json.loads(golden_record().to_file_bytes())


def _with(**changes):
    obj = _golden_obj()
    obj.update(changes)
    return vs.canonical_json_bytes(obj)


def _source_with(part, **changes):
    obj = _golden_obj()
    obj["source"][part].update(changes)
    return vs.canonical_json_bytes(obj)


OTHER_MANIFEST = GOLDEN_MANIFEST.replace("spot", "perp")
BAD_RECORDS = {
    "not_json": b"{",
    "extra_key": _with(note="x"),
    "missing_key": vs.canonical_json_bytes({k: v for k, v in _golden_obj().items() if k != "source"}),
    "format": _with(format="btc-quant/legacy-import/v2"),
    "dataset_id": _with(dataset_id=h("other")),
    "data_version": _with(data_version=h("other")),
    "non_canonical": json.dumps(_golden_obj(), sort_keys=True).encode(),
    "manifest_sha": _source_with("manifest", sha256=h("m")),
    "manifest_bytes": _source_with("manifest", bytes=1),
    "data_sha": _source_with("data_file", sha256="X" * 64),
    "data_bytes_zero": _source_with("data_file", bytes=0),
    "data_bytes_bool": _source_with("data_file", bytes=True),
    "name": _source_with("data_file", name="1m.parquet"),
    "source_keys": _source_with("manifest", extra=1),
    "manifest_text_other_dataset": vs.canonical_json_bytes({
        **_golden_obj(), "manifest_text": OTHER_MANIFEST,
        "source": {**_golden_obj()["source"], "manifest": {
            "name": "15m.manifest.json", "sha256": hashlib.sha256(OTHER_MANIFEST.encode()).hexdigest(),
            "bytes": len(OTHER_MANIFEST)}}}),
}


@pytest.mark.parametrize("name", sorted(BAD_RECORDS))
def test_strict_record_parsing(name):
    with pytest.raises(lg.ImportRecordError):
        lg.ImportRecord.from_file_bytes(BAD_RECORDS[name], expected_id=golden_record().import_id)


def test_record_parsing_checks_the_expected_id():
    rec = golden_record()
    with pytest.raises(lg.ImportRecordError, match="import_id"):
        lg.ImportRecord.from_file_bytes(rec.to_file_bytes(), expected_id=h("other"))


def test_stored_record_must_be_a_read_only_regular_file(store, src):
    apply(store, src)
    d = store.root / "imports"
    first = min(d.iterdir())
    os.chmod(first, 0o644)
    with pytest.raises(lg.ImportCorruptError, match="skrivskyddad"):
        lg.read_import_records(store)
    os.chmod(first, 0o444)
    (d / "notes.txt").write_text("x")
    with pytest.raises(lg.ImportCorruptError, match="okänd fil"):
        lg.read_import_records(store)


# ================================================================== krasch vid varje skrivning
def _rename_then_die():
    return "real = vs._rename\ndef r(a, b):\n    real(a, b)\n    os._exit(77)\nvs._rename = r\n"


def _link_then_die():
    return "real = vs._link\ndef l(a, b):\n    real(a, b)\n    os._exit(77)\nvs._link = l\n"


CRASHES = {                    # punkt -> (injektion, förväntade icke-INFO-fynd efter kraschen)
    "1_staging": ("vs.StoreWriter._publish_dir = die\n", {"TMP_LEFTOVER"}),
    "3_rename": (_rename_then_die(), {"OBJECT_DIR_MODE", "OBJECT_UNREGISTERED", "CATALOG_BEHIND",
                                      "TMP_LEFTOVER"}),                   # nytt dataset: datasets-raden saknas
    "4_before_register": ("vs.StoreWriter._register = die\n", {"OBJECT_UNREGISTERED", "CATALOG_BEHIND"}),
    "6_record_staged": ("vs._link = die\n", {"TMP_LEFTOVER"}),
    "7_record_linked": (_link_then_die(), {"TMP_LEFTOVER"}),
    "8_ref_log": ("vs.StoreWriter._write_ref_file = die\n", {"PENDING_REF_UPDATE"}),
    "9_ref_file": ("vs.StoreWriter._sync_ref_catalog = die\n", {"CATALOG_BEHIND"}),
}


@pytest.mark.parametrize("point", sorted(CRASHES))
def test_crash_at_every_write_then_recovery_equals_an_uninterrupted_run(tmp_path, store, src, point):
    body, expected = CRASHES[point]
    src_before = lg.source_tree_sha256(src)
    crash_child(store, src, body)
    r = ig.verify_store(store.root)
    assert non_info(r) == expected, r.findings
    if "PENDING_REF_UPDATE" in expected:
        with pytest.raises(vs.PendingRefUpdateError):
            apply(store, src)
        with store.writer() as w:
            assert len(w.complete_pending_ref_updates()) == 1
    assert apply(store, src).ok
    ig.repair_store(store.root, apply=True)
    twin = vs.VersionedStore.init(tmp_path / "twin")
    apply(twin, src)
    assert immutable_hash(store) == immutable_hash(twin)
    assert dump(store) == dump(twin)
    after = ig.verify_store(store.root)
    assert after.exit_status == 0 and after.findings == ()
    assert list(store.tmp.iterdir()) == []
    assert lg.source_tree_sha256(src) == src_before


# ================================================================== samspel med steg 4
def test_ingest_after_import_follows_step4_rules(store, src, tmp_path):
    imported = by_rel(apply(store, src))[SPOT15.relpath].data_version
    with pytest.raises(vs.VersionRegressionError):
        _ingested(store, tmp_path, n=40, seed=2)
    same = ingest(store, SPOT15, source_files(tmp_path / "raw-same", SPOT15, 90, 2))
    assert (same.record.publication, same.record.ref_outcome, same.record.ref.data_version) == (
        "reused", "unchanged", imported)
    assert "sources" in store.read_version(SPOT15, imported)[1]          # legacy-manifestet bevarat
    newer = ingest(store, SPOT15, source_files(tmp_path / "raw-new", SPOT15, 200, 2))
    assert newer.record.ref_outcome == "changed"
    assert [e.action for e in vs.parse_ref_log(store._read_ref_log())][-1] == "ingest"
    again = apply(store, src)
    assert by_rel(again)[SPOT15.relpath].ref_outcome == lg.REF_KEPT and again.ok
    assert ig.verify_store(store.root).exit_status == 0
