"""PHASE 4 steg 2: oföränderliga, innehållsadresserade datasetversioner."""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
from data_helpers import START, canon_frame, make_klines

from btc_trading_system.data.provider import LocalOHLCVProvider
from btc_trading_system.data.schema import DatasetKey, to_epoch_ns
from btc_trading_system.data.storage import (
    DatasetStore,
    ParquetBackend,
    canonicalize,
    content_hash,
)
from btc_trading_system.data_validation.errors import DataError, DatasetNotFoundError
from btc_trading_system.data_validation.ohlcv import compute_gap_flags
from btc_trading_system.database import catalog
from btc_trading_system.database import versioned_store as vs

ROOT = Path(__file__).resolve().parents[1]
KEY = DatasetKey("spot", "BTCUSDT", "15m")
TF = timedelta(minutes=15)
US = timedelta(microseconds=1)
IS_ROOT = hasattr(os, "geteuid") and os.geteuid() == 0


def h(tag: str) -> str:
    return hashlib.sha256(tag.encode()).hexdigest()


def src(name: str, rows: int = 10) -> dict:
    return {"name": name, "rows": rows, "sha256": h(name)}


@pytest.fixture
def store(tmp_path):
    return vs.VersionedStore.init(tmp_path / "store")


def publish(store, df, extra=None, key=KEY):
    with store.writer() as w:
        return w.publish_version(key, df, extra)


def dump(store) -> str:
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        return catalog.dump_catalog(conn)
    finally:
        conn.close()


def tree_hash(root: Path) -> str:
    """Namn + rättigheter + innehåll för hela trädet (inkl. roten)."""
    hh = hashlib.sha256()
    for p in [root, *sorted(root.rglob("*"))]:
        st = os.lstat(p)
        hh.update(f"{p.relative_to(root)}|{st.st_mode:o}|".encode())
        if p.is_file() and not p.is_symlink():
            hh.update(p.read_bytes())
    return hh.hexdigest()


def gapped_frame(n=40, drop=(10, 11, 25), tf="15m"):
    """Kanonisk frame med luckor; gap_before beräknad kausalt på hela serien."""
    df = make_klines(tf, n).drop(columns=["close_time"]).drop(index=list(drop)).reset_index(drop=True)
    df["gap_before"], _ = compute_gap_flags(to_epoch_ns(df["open_time"]), tf)
    return canonicalize(df)


def obj_dir(store, res) -> Path:
    return store.object_dir(res.info.key, res.info.data_version)


def patch_object(d: Path, name: str, data: bytes) -> None:
    """Skriver om en fil i ett publicerat objekt och återställer rättigheterna
    (så att bara den avsedda manipulationen finns kvar)."""
    os.chmod(d, 0o755)
    os.chmod(d / name, 0o644)
    (d / name).write_bytes(data)
    os.chmod(d / name, 0o444)
    os.chmod(d, 0o555)


def rewrite_version_json(d: Path, **changes) -> None:
    v = json.loads((d / vs.VERSION_FILE).read_bytes())
    for k, val in changes.items():
        v[k] = val
    patch_object(d, vs.VERSION_FILE, vs.canonical_json_bytes(v))


def assert_causal(frame, timeframe, as_of):
    """Invarianten: ingen exponerad bar får ha available_at > as_of."""
    avail = frame["open_time"] + pd.Timedelta(timeframe)
    assert (avail <= pd.Timestamp(as_of)).all(), "framtida bar exponerad"


# ================================================================== init / STORE.json
def test_init_creates_exact_layout(tmp_path):
    st = vs.VersionedStore.init(tmp_path / "s")
    assert sorted(p.name for p in st.root.iterdir()) == \
        [".lock", "STORE.json", "catalog.sqlite", "objects", "tmp"]
    assert json.loads((st.root / "STORE.json").read_bytes()) == \
        {"format": "btc-quant/versioned-store", "layout_version": 1}
    assert list(st.tmp.iterdir()) == [] and list(st.objects.iterdir()) == []
    assert dump(st) == catalog.dump_catalog(catalog.open_catalog(st.catalog_path, readonly=True))


def test_init_is_idempotent(tmp_path, store):
    publish(store, canon_frame("15m", 30))
    before = tree_hash(store.root)
    vs.VersionedStore.init(store.root)
    assert tree_hash(store.root) == before


def _phase2_layout(r: Path) -> None:
    for inst in ("perp", "spot"):
        d = r / "binance" / inst / "BTCUSDT"
        d.mkdir(parents=True)
        for tf in ("1m", "15m"):
            (d / f"{tf}.parquet").write_bytes(b"PAR1")
            (d / f"{tf}.manifest.json").write_text("{}")


def _catalog_with_dataset(r: Path) -> None:
    (r / "objects").mkdir()
    conn = catalog.open_catalog(r / "catalog.sqlite", create=True)
    with catalog.transaction(conn):
        catalog.ensure_dataset(conn, KEY)
    conn.close()


def _symlink_entry(r: Path) -> None:
    (r / "objects").symlink_to(r.parent)


REFUSED = {
    "phase2_store": _phase2_layout,
    "unrelated_file": lambda r: (r / "notes.txt").write_text("x"),
    "foreign_tmp_file": lambda r: ((r / "tmp").mkdir(), (r / "tmp" / "data.bin").write_bytes(b"x")),
    "nonempty_objects": lambda r: ((r / "objects" / "binance").mkdir(parents=True)),
    "catalog_with_dataset": _catalog_with_dataset,
    "corrupt_catalog": lambda r: (r / "catalog.sqlite").write_bytes(b"junk" * 50),
    "symlink_entry": _symlink_entry,
    "lock_is_dir": lambda r: (r / ".lock").mkdir(),
}


@pytest.mark.parametrize("name", sorted(REFUSED))
def test_init_refuses_foreign_directory_and_creates_nothing(tmp_path, name):
    r = tmp_path / "target"
    r.mkdir()
    REFUSED[name](r)
    before = tree_hash(r)
    with pytest.raises(vs.StoreFormatError):
        vs.VersionedStore.init(r)
    assert tree_hash(r) == before
    assert not (r / ".lock").exists() or name == "lock_is_dir"


def test_init_refuses_a_file_path(tmp_path):
    f = tmp_path / "file"
    f.write_text("x")
    with pytest.raises(vs.StoreFormatError):
        vs.VersionedStore.init(f)
    assert f.read_text() == "x"


def test_init_refuses_real_phase2_store(tmp_path):
    """Ett äkta Phase 2-lager (skrivet av Phase 2:s egen DatasetStore) får aldrig initieras."""
    r = tmp_path / "p2"
    p2 = DatasetStore(r, ParquetBackend())
    for key, seed in ((DatasetKey("spot", "BTCUSDT", "15m"), 1), (DatasetKey("perp", "BTCUSDT", "15m"), 2)):
        p2.write(key, canon_frame("15m", 40, seed=seed), {"sources": [src(f"{key.instrument.value}.zip", 40)]})
        assert p2.exists(key)
    before = tree_hash(r)
    with pytest.raises(vs.StoreFormatError):
        vs.VersionedStore.init(r)
    assert tree_hash(r) == before
    assert sorted(str(p.relative_to(r)) for p in r.rglob("*.parquet")) == \
        ["binance/perp/BTCUSDT/15m.parquet", "binance/spot/BTCUSDT/15m.parquet"]


INIT_CRASHES = {
    "after_lock": "o = vs._WriterLock.acquire\n"
                  "def f(self):\n    o(self)\n    os._exit(3)\n"
                  "vs._WriterLock.acquire = f",
    "after_dirs": "catalog.open_catalog = lambda *a, **k: os._exit(3)",
    "mid_catalog_creation": "catalog.check_catalog = lambda *a, **k: os._exit(3)",
    "after_catalog": "vs.canonical_json_bytes = lambda *a, **k: os._exit(3)",
    "after_staged_store_file": "vs._fsync_file = lambda *a, **k: os._exit(3)",
}


def _run_py(code: str, **kw) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "tests")]),
           "PYTHONDONTWRITEBYTECODE": "1"}
    return subprocess.run([sys.executable, "-c", code], env=env, cwd=ROOT, check=False, **kw)


@pytest.mark.parametrize("stage", sorted(INIT_CRASHES))
def test_init_resumes_after_crash_at_each_stage(tmp_path, stage):
    r = tmp_path / "s"
    code = ("import os\n"
            "from btc_trading_system.database import versioned_store as vs, catalog\n"
            f"{INIT_CRASHES[stage]}\n"
            f"vs.VersionedStore.init({str(r)!r})\n")
    assert _run_py(code).returncode == 3
    assert not (r / "STORE.json").exists()
    leftovers = {p.name for p in r.iterdir()}
    assert leftovers <= {".lock", "objects", "tmp", "catalog.sqlite"}     # roten: bara slutliga artefakter
    if stage == "mid_catalog_creation":
        assert any(p.name.startswith(".catalog-") for p in (r / "tmp").iterdir())
        assert "catalog.sqlite" not in leftovers
    st = vs.VersionedStore.init(r)
    assert sorted(p.name for p in r.iterdir()) == [".lock", "STORE.json", "catalog.sqlite",
                                                   "objects", "tmp"]
    conn = catalog.open_catalog(st.catalog_path, readonly=True)
    assert catalog.check_catalog(conn, full=True) == []
    conn.close()
    assert publish(st, canon_frame("15m", 20)).published


def test_concurrent_init_is_serialized_by_the_lock(tmp_path):
    r = tmp_path / "s"
    r.mkdir()
    (r / "objects").mkdir()                                  # avbruten init
    lock = vs._WriterLock(r / ".lock")
    lock.acquire()
    try:
        with pytest.raises(vs.StoreLockedError):
            vs.VersionedStore.init(r)
    finally:
        lock.release()
    assert not (r / "STORE.json").exists()
    vs.VersionedStore.init(r)


@pytest.mark.parametrize("content", [
    None, b"not json", b"[]", b'{"format": "other", "layout_version": 1}',
    b'{"format": "btc-quant/versioned-store", "layout_version": 2}',
    b'{"format": "btc-quant/versioned-store", "layout_version": true}',
    b'{"format": "btc-quant/versioned-store", "layout_version": 0}',
    b'{"format": "btc-quant/versioned-store", "layout_version": 1, "x": 1}',
])
def test_invalid_or_newer_store_file_is_rejected(tmp_path, store, content):
    p = store.root / "STORE.json"
    if content is None:
        p.unlink()
    else:
        p.write_bytes(content)
    with pytest.raises(vs.StoreFormatError):
        vs.VersionedStore(store.root)


# ================================================================== skapande / identitet
def test_publish_creates_immutable_object(store):
    df = canon_frame("15m", 50)
    res = publish(store, df, {"sources": [src("a.zip", 50)]})
    d = obj_dir(store, res)
    assert res.published and res.outcome == "published"
    assert d == store.root / "objects/binance/spot/BTCUSDT/15m" / content_hash(canonicalize(df))
    assert sorted(p.name for p in d.iterdir()) == ["bars.parquet", "manifest.json", "version.json"]
    assert os.stat(d).st_mode & 0o777 == 0o555
    assert all(os.stat(d / n).st_mode & 0o777 == 0o444 for n in vs.OBJECT_FILES)
    assert list(store.tmp.iterdir()) == []                    # staging städad


def test_version_json_content(store):
    df = canon_frame("15m", 50)
    res = publish(store, df)
    d = obj_dir(store, res)
    v = json.loads((d / "version.json").read_bytes())
    assert (d / "version.json").read_bytes() == vs.canonical_json_bytes(v)
    assert v == {
        "format": "btc-quant/dataset-version/v1",
        "dataset_id": catalog.dataset_id(KEY),
        "dataset": {"exchange": "binance", "instrument": "spot", "symbol": "BTCUSDT", "timeframe": "15m"},
        "data_version": res.info.data_version,
        "data_file": {"path": "bars.parquet", "sha256": vs.sha256_file(d / "bars.parquet"),
                      "bytes": (d / "bars.parquet").stat().st_size},
        "manifest": {"path": "manifest.json",
                     "sha256": hashlib.sha256((d / "manifest.json").read_bytes()).hexdigest(),
                     "bytes": (d / "manifest.json").stat().st_size},
        "rows": 50,
        "first_open_time": df["open_time"].iloc[0].isoformat(),
        "last_open_time": df["open_time"].iloc[-1].isoformat(),
    }
    assert res.info.version_sha256 == hashlib.sha256((d / "version.json").read_bytes()).hexdigest()


def test_data_version_is_phase2_content_hash_and_manifest_is_phase2_format(tmp_path, store):
    df = canon_frame("15m", 50)
    extra = {"sources": [src("a.zip", 50)], "zero_volume_bars": 0}
    res = publish(store, df, extra)
    p2 = DatasetStore(tmp_path / "p2", ParquetBackend())
    m2 = p2.write(KEY, df, extra)
    assert res.info.data_version == content_hash(canonicalize(df)) == m2["data_version"]
    assert (obj_dir(store, res) / "manifest.json").read_bytes() == p2.manifest_path(KEY).read_bytes()
    assert (obj_dir(store, res) / "bars.parquet").read_bytes() == p2.data_path(KEY).read_bytes()


def test_physical_logical_and_manifest_hashes_are_distinct(store):
    res = publish(store, canon_frame("15m", 50))
    i = res.info
    assert len({i.data_version, i.data_file_sha256, i.manifest_sha256, i.version_sha256}) == 4
    assert all(catalog.is_sha256_hex(x) for x in (i.data_version, i.data_file_sha256,
                                                   i.manifest_sha256, i.version_sha256))


def test_same_content_different_physical_file_fails_verification(store):
    """Logiskt identiskt innehåll men annan Parquet-kodning: data_version är lika,
    data_file_sha256 skiljer -> objektet underkänns (fysisk hash kontrolleras separat)."""
    res = publish(store, canon_frame("15m", 50))
    d = obj_dir(store, res)
    staged = d.parent / "regz.parquet"
    pq.write_table(pq.read_table(d / "bars.parquet"), staged, compression="gzip")
    data = staged.read_bytes()
    staged.unlink()
    assert data != (d / "bars.parquet").read_bytes()
    patch_object(d, "bars.parquet", data)
    assert content_hash(canonicalize(ParquetBackend().read(d / "bars.parquet"))) == res.info.data_version
    with pytest.raises(vs.ObjectCorruptError, match="data_file_sha256"):
        store.verify_version(KEY, res.info.data_version)


def test_same_size_same_content_different_bytes_is_caught_only_by_file_hash(store):
    """Byte-ändring i Parquet-metadata (created_by): samma storlek, samma logiska
    innehåll. Bara data_file_sha256 kan upptäcka den."""
    res = publish(store, canon_frame("15m", 50))
    d = obj_dir(store, res)
    data = (d / "bars.parquet").read_bytes()
    i = data.find(b"parquet-cpp-arrow")
    assert i > 0
    patch_object(d, "bars.parquet", data[:i] + b"Parquet" + data[i + 7:])
    assert (d / "bars.parquet").stat().st_size == res.info.data_file_bytes
    assert content_hash(canonicalize(ParquetBackend().read(d / "bars.parquet"))) == res.info.data_version
    with pytest.raises(vs.ObjectCorruptError, match="data_file_sha256"):
        store.verify_version(KEY, res.info.data_version)


def test_publication_is_deterministic(tmp_path):
    df = canon_frame("15m", 60, seed=3)
    extra = {"sources": [src("a.zip", 60)]}
    a, b = vs.VersionedStore.init(tmp_path / "a"), vs.VersionedStore.init(tmp_path / "b")
    ra, rb = publish(a, df, extra), publish(b, df.copy(), json.loads(json.dumps(extra)))
    assert ra.info == rb.info
    for n in sorted(vs.OBJECT_FILES):
        assert (obj_dir(a, ra) / n).read_bytes() == (obj_dir(b, rb) / n).read_bytes()
    assert dump(a) == dump(b)


def test_different_content_gives_different_versions(store):
    r1 = publish(store, canon_frame("15m", 50, seed=1))
    r2 = publish(store, canon_frame("15m", 50, seed=2))
    assert r1.info.data_version != r2.info.data_version
    assert r1.published and r2.published
    store.verify_version(KEY, r1.info.data_version)
    store.verify_version(KEY, r2.info.data_version)


# ================================================================== återbruk / proveniens
def test_identical_content_is_verified_and_reused(store, monkeypatch):
    df = canon_frame("15m", 50)
    extra = {"sources": [src("a.zip", 50)]}
    r1 = publish(store, df, extra)
    d = obj_dir(store, r1)
    before, cat_before = tree_hash(d), dump(store)
    calls = []
    orig = vs.VersionedStore._verify_object
    monkeypatch.setattr(vs.VersionedStore, "_verify_object",
                        lambda self, k, dv: calls.append(dv) or orig(self, k, dv))
    monkeypatch.setattr(vs, "_rename", lambda *a: pytest.fail("rename vid återbruk"))
    r2 = publish(store, df.copy(), extra)
    assert r2.outcome == "reused" and not r2.published and r2.info == r1.info
    assert calls == [r1.info.data_version]                      # aldrig blint återbruk
    assert tree_hash(d) == before and dump(store) == cat_before
    assert list(store.tmp.iterdir()) == []


def test_reuse_with_different_sources_raises_provenance_conflict(store):
    df = canon_frame("15m", 50)
    r1 = publish(store, df, {"sources": [src("a.zip", 50)]})
    d = obj_dir(store, r1)
    before, cat_before = tree_hash(d), dump(store)
    for extra in ({"sources": [src("b.zip", 50)]}, {}, {"sources": [src("a.zip", 50)], "x": 1},
                  {"sources": [src("a.zip", 49)]}):
        with pytest.raises(vs.ProvenanceConflictError):
            publish(store, df, extra)
    assert tree_hash(d) == before and dump(store) == cat_before


def test_provenance_conflict_is_checked_after_full_verification(store):
    df = canon_frame("15m", 50)
    r1 = publish(store, df, {"sources": [src("a.zip", 50)]})
    d = obj_dir(store, r1)
    patch_object(d, "bars.parquet", (d / "bars.parquet").read_bytes()[:-1] + b"\x00")
    with pytest.raises(vs.ObjectCorruptError):
        publish(store, df, {"sources": [src("b.zip", 50)]})


@pytest.mark.parametrize("sources", ["a.zip", {"name": "a"}, [{"name": "a"}],
                                     [{"name": "", "rows": 1, "sha256": h("a")}],
                                     [{"name": "a", "rows": True, "sha256": h("a")}],
                                     [{"name": "a", "rows": -1, "sha256": h("a")}],
                                     [{"name": "a", "rows": 1, "sha256": h("a").upper()}]])
def test_invalid_sources_raise_invalid_sources_error_before_any_write(store, sources):
    before, cat_before = tree_hash(store.root), dump(store)
    with pytest.raises(vs.InvalidSourcesError) as ei:
        publish(store, canon_frame("15m", 30), {"sources": sources})
    assert isinstance(ei.value, ValueError) and isinstance(ei.value, DataError)
    assert tree_hash(store.root) == before and dump(store) == cat_before


@pytest.mark.parametrize("key", sorted(vs.MANIFEST_BASE_KEYS))
def test_extra_may_not_shadow_manifest_base_fields(store, key):
    before = tree_hash(store.root)
    with pytest.raises(ValueError, match="skugga"):
        publish(store, canon_frame("15m", 30), {key: "x"})
    assert tree_hash(store.root) == before


# ================================================================== korruption
def _flip_last_byte(b: bytes) -> bytes:
    return b[:-1] + bytes([b[-1] ^ 0xFF])


CORRUPTIONS = {
    "bars_byte_flip": lambda d: patch_object(d, "bars.parquet", _flip_last_byte((d / "bars.parquet").read_bytes())),
    "bars_truncated": lambda d: patch_object(d, "bars.parquet", (d / "bars.parquet").read_bytes()[:100]),
    "manifest_edited": lambda d: patch_object(d, "manifest.json",
                                              (d / "manifest.json").read_bytes().replace(b'"rows": 50', b'"rows": 51')),
    "version_whitespace": lambda d: patch_object(d, "version.json", (d / "version.json").read_bytes() + b"\n"),
    "version_wrong_rows": lambda d: rewrite_version_json(d, rows=49),
    "version_wrong_dataset_id": lambda d: rewrite_version_json(d, dataset_id=h("x")),
    "extra_file": lambda d: (os.chmod(d, 0o755), (d / "extra.txt").write_text("x"), os.chmod(d, 0o555)),
    "missing_manifest": lambda d: (os.chmod(d, 0o755), (d / "manifest.json").unlink(), os.chmod(d, 0o555)),
    "writable_file": lambda d: os.chmod(d / "bars.parquet", 0o644),
    "file_replaced_by_symlink": lambda d: (os.chmod(d, 0o755), (d / "manifest.json").unlink(),
                                           (d / "manifest.json").symlink_to(d / "version.json"),
                                           os.chmod(d, 0o555)),
}


@pytest.mark.parametrize("name", sorted(CORRUPTIONS))
def test_corrupted_existing_object_fails_and_is_never_overwritten(store, name):
    df = canon_frame("15m", 50)
    res = publish(store, df)
    d = obj_dir(store, res)
    CORRUPTIONS[name](d)
    before, cat_before = tree_hash(d), dump(store)
    with pytest.raises(vs.ObjectCorruptError):
        store.read_version(KEY, res.info.data_version)
    with pytest.raises(vs.ObjectCorruptError):
        publish(store, df)                                   # samma data_version: FAIL, ingen överskrivning
    assert tree_hash(d) == before and dump(store) == cat_before


def _rewrite_consistently(d: Path, frame) -> None:
    """Angripare som byter innehåll OCH uppdaterar version.json:s filhash."""
    os.chmod(d, 0o755)
    os.chmod(d / "bars.parquet", 0o644)
    ParquetBackend().write(d / "bars.parquet", frame)
    os.chmod(d / "bars.parquet", 0o444)
    os.chmod(d, 0o555)
    rewrite_version_json(d, data_file={"path": "bars.parquet", "sha256": vs.sha256_file(d / "bars.parquet"),
                                       "bytes": (d / "bars.parquet").stat().st_size})


def test_consistently_tampered_content_is_caught_by_data_version(store):
    res = publish(store, canon_frame("15m", 50))
    d = obj_dir(store, res)
    frame = canonicalize(ParquetBackend().read(d / "bars.parquet"))
    frame.loc[3, "trade_count"] += 1                         # giltig OHLCV, annat innehåll
    _rewrite_consistently(d, frame)
    manifest = json.loads((d / "manifest.json").read_bytes())
    rewrite_version_json(d, manifest={"path": "manifest.json", "sha256": hashlib.sha256(
        (d / "manifest.json").read_bytes()).hexdigest(), "bytes": (d / "manifest.json").stat().st_size})
    assert manifest["data_version"] == res.info.data_version
    with pytest.raises(vs.ObjectCorruptError, match="data_version"):
        store.verify_version(KEY, res.info.data_version)


def test_object_directory_replaced_by_symlink_is_corrupt(store, tmp_path):
    res = publish(store, canon_frame("15m", 30))
    d = obj_dir(store, res)
    elsewhere = tmp_path / "moved"
    os.chmod(d, 0o755)                                           # rename av katalog kräver skriv på '..'
    os.rename(d, elsewhere)
    os.chmod(elsewhere, 0o555)
    d.symlink_to(elsewhere)
    with pytest.raises(vs.ObjectCorruptError, match="katalog"):
        store.verify_version(KEY, res.info.data_version)


# ================================================================== överskrivning
def test_non_empty_existing_target_gives_immutable_object_error(store, tmp_path):
    res = publish(store, canon_frame("15m", 50))
    target = obj_dir(store, res)
    before = tree_hash(target)
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "bars.parquet").write_bytes(b"other")
    with store.writer() as w, pytest.raises(vs.ImmutableObjectError):
        w._publish_dir(staged, target)
    assert tree_hash(target) == before and (staged / "bars.parquet").exists()


def test_empty_existing_target_gives_immutable_object_error(store, tmp_path):
    """rename() skulle tyst ersätta en TOM katalog; skyddet före rename måste slå till."""
    target = store.objects / KEY.relpath / h("empty-target")
    target.mkdir(parents=True)
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "bars.parquet").write_bytes(b"x")
    with store.writer() as w, pytest.raises(vs.ImmutableObjectError):
        w._publish_dir(staged, target)
    assert list(target.iterdir()) == [] and (staged / "bars.parquet").exists()


def test_rename_race_onto_non_empty_target_is_immutable_error(store, monkeypatch):
    """Målet dyker upp (icke-tomt) efter kontrollen: rename vägrar, ImmutableObjectError."""
    df = canon_frame("15m", 30)
    dv = content_hash(canonicalize(df))
    target = store.object_dir(KEY, dv)
    real = vs._rename

    def racing(src_, dst):
        dst.mkdir()
        (dst / "intruder").write_text("x")
        real(src_, dst)

    monkeypatch.setattr(vs, "_rename", racing)
    cat_before = dump(store)
    with pytest.raises(vs.ImmutableObjectError):
        publish(store, df)
    assert [p.name for p in target.iterdir()] == ["intruder"] and dump(store) == cat_before


@pytest.mark.skipif(IS_ROOT, reason="root ignorerar filrättigheter")
def test_published_files_and_directory_are_read_only(store):
    res = publish(store, canon_frame("15m", 30))
    d = obj_dir(store, res)
    for n in vs.OBJECT_FILES:
        with pytest.raises(PermissionError):
            (d / n).open("r+b")
    with pytest.raises(PermissionError):
        (d / "new.txt").write_text("x")
    with pytest.raises(PermissionError):
        (d / "bars.parquet").unlink()


def test_api_has_no_mutating_operations_and_reads_are_copies(store):
    res = publish(store, canon_frame("15m", 30))
    frame, manifest = store.read_version(KEY, res.info.data_version)
    frame.loc[0, "close"] = -1.0
    manifest["rows"] = 0
    again, man2 = store.read_version(KEY, res.info.data_version)
    assert again.loc[0, "close"] > 0 and man2["rows"] == 30
    assert {n for n in dir(vs.StoreWriter) if not n.startswith("_")} == \
        {"publish_version", "set_current", "complete_pending_ref_updates", "create_snapshot",
         "snapshot_current", "ingest", "complete_pending_ingest", "publish_funding"}
    assert {n for n in dir(vs.VersionedStore) if not n.startswith("_")} == \
        {"init", "object_dir", "has_version", "verify_version", "read_version", "read_range", "writer",
         "current_ref", "read_current", "read_snapshot", "pinned", "read_ingest", "ingest_history",
         "funding_object_dir", "has_funding_version", "verify_funding_version", "read_funding_version",
         "read_funding_range", "migrate_catalog"}                                   # etapp B steg 4


# ================================================================== publiceringsordning / crash
def test_publication_order_is_crash_safe(store, monkeypatch):
    events = []
    for name in ("_fsync_file", "_fsync_dir", "_rename"):
        orig = getattr(vs, name)

        def spy(*a, _n=name, _o=orig):
            events.append((_n, Path(a[-1]).name if _n == "_rename" else Path(a[0])))
            return _o(*a)
        monkeypatch.setattr(vs, name, spy)
    ov, orr = vs.VersionedStore._verify_object, vs.StoreWriter._register
    monkeypatch.setattr(vs.VersionedStore, "_verify_object",
                        lambda self, k, dv: events.append(("verify", None)) or ov(self, k, dv))
    monkeypatch.setattr(vs.StoreWriter, "_register",
                        lambda self, i, m: events.append(("register", None)) or orr(self, i, m))
    res = publish(store, canon_frame("15m", 40))
    target = obj_dir(store, res)
    kinds = [e[0] for e in events]
    i_rename = kinds.index("_rename")
    files = [e[1].name for e in events[:i_rename] if e[0] == "_fsync_file"]
    assert sorted(files) == sorted(vs.OBJECT_FILES)                         # 1) filer durabla
    staged_dir_sync = [e for e in events[:i_rename] if e[0] == "_fsync_dir" and e[1].name == "object"]
    assert staged_dir_sync                                                   # 2) stagingkatalog durabel
    parents = [e[1] for e in events[:i_rename] if e[0] == "_fsync_dir" and e[1] != staged_dir_sync[0][1]]
    assert store.objects in parents                                          # 3) nya föräldrar durabla
    after = events[i_rename + 1:]
    assert after[0] == ("_fsync_dir", target) and after[1] == ("_fsync_dir", target.parent)  # 4)
    assert [e[0] for e in after[2:]] == ["verify", "register"]               # 5) verifiera, 6) katalog
    assert "register" not in kinds[:i_rename] and "verify" not in kinds[:i_rename]


@pytest.mark.parametrize("stage", ["phase2_write", "fsync_file", "mkdirs", "rename"])
def test_failed_publication_leaves_catalog_and_objects_unchanged(store, monkeypatch, stage):
    publish(store, canon_frame("15m", 30, seed=9))            # befintligt innehåll
    before_cat, before_objs = dump(store), tree_hash(store.objects)

    def boom(*a, **k):
        raise OSError(5, "injicerat fel")

    target = {"phase2_write": (DatasetStore, "write"), "fsync_file": (vs, "_fsync_file"),
              "mkdirs": (vs, "_mkdirs_durable"), "rename": (vs, "_rename")}[stage]
    monkeypatch.setattr(*target, boom)
    with pytest.raises(OSError, match="injicerat"):
        publish(store, canon_frame("15m", 30, seed=10))
    assert dump(store) == before_cat
    assert tree_hash(store.objects) == before_objs or stage == "mkdirs"
    assert not store.has_version(KEY, content_hash(canonicalize(canon_frame("15m", 30, seed=10))))
    assert list(store.tmp.iterdir()) == []


def test_crash_after_rename_before_catalog_is_completed_by_next_publish(store, monkeypatch):
    df = canon_frame("15m", 40)
    extra = {"sources": [src("a.zip", 40)]}
    orig = vs.StoreWriter._register

    def crash(self, info, manifest):
        raise RuntimeError("krasch före katalog")

    monkeypatch.setattr(vs.StoreWriter, "_register", crash)
    with pytest.raises(RuntimeError):
        publish(store, df, extra)
    dv = content_hash(canonicalize(df))
    store.verify_version(KEY, dv)                                # durabelt, komplett objekt
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert catalog.list_dataset_versions(conn, catalog.dataset_id(KEY)) == []
    conn.close()
    monkeypatch.setattr(vs.StoreWriter, "_register", orig)
    res = publish(store, df, extra)
    assert res.outcome == "reused"
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert [r["data_version"] for r in catalog.list_dataset_versions(conn, res.info.dataset_id)] == [dv]
    conn.close()


def test_crash_after_rename_before_chmod_is_completed_after_full_verification(store, monkeypatch):
    df = canon_frame("15m", 40)
    real_finish = vs.StoreWriter.__dict__["_finish_dir"]         # staticmethod-objektet
    monkeypatch.setattr(vs.StoreWriter, "_finish_dir",
                        staticmethod(lambda t: (_ for _ in ()).throw(RuntimeError("krasch före chmod"))))
    with pytest.raises(RuntimeError):
        publish(store, df)
    dv = content_hash(canonicalize(df))
    d = store.object_dir(KEY, dv)
    assert os.stat(d).st_mode & 0o777 == 0o755                  # rename skedde, chmod inte
    files_before = {n: (os.stat(d / n).st_ino, os.stat(d / n).st_mtime_ns, (d / n).read_bytes())
                    for n in vs.OBJECT_FILES}
    monkeypatch.setattr(vs.StoreWriter, "_finish_dir", real_finish)
    calls = []
    ov = vs.VersionedStore._verify_object
    monkeypatch.setattr(vs.VersionedStore, "_verify_object",
                        lambda self, k, v: calls.append(os.stat(d).st_mode & 0o777) or ov(self, k, v))
    res = publish(store, df)
    assert res.outcome == "reused"
    assert calls == [0o755]                                      # verifierat FÖRE chmod
    assert os.stat(d).st_mode & 0o777 == 0o555
    assert {n: (os.stat(d / n).st_ino, os.stat(d / n).st_mtime_ns, (d / n).read_bytes())
            for n in vs.OBJECT_FILES} == files_before           # filer orörda


def test_writable_dir_with_corrupt_content_is_not_chmodded(store, monkeypatch):
    df = canon_frame("15m", 40)
    monkeypatch.setattr(vs.StoreWriter, "_finish_dir",
                        staticmethod(lambda t: (_ for _ in ()).throw(RuntimeError("krasch"))))
    with pytest.raises(RuntimeError):
        publish(store, df)
    monkeypatch.undo()
    d = store.object_dir(KEY, content_hash(canonicalize(df)))
    os.chmod(d / "bars.parquet", 0o644)
    (d / "bars.parquet").write_bytes(_flip_last_byte((d / "bars.parquet").read_bytes()))
    os.chmod(d / "bars.parquet", 0o444)
    with pytest.raises(vs.ObjectCorruptError):
        publish(store, df)
    assert os.stat(d).st_mode & 0o777 == 0o755


def test_catalog_row_mismatch_is_detected(store):
    df = canon_frame("15m", 30)
    dv = content_hash(canonicalize(df))
    conn = catalog.open_catalog(store.catalog_path)
    with catalog.transaction(conn):
        did = catalog.ensure_dataset(conn, KEY)
        conn.execute("INSERT INTO dataset_versions (dataset_id, data_version, data_file_sha256, "
                     "data_file_bytes, manifest_sha256, version_sha256, manifest_schema_version, rows, "
                     "first_open_time_ms, last_open_time_ms, gap_count, missing_bars_total) "
                     "VALUES (?, ?, ?, 1, ?, ?, 1, 30, 0, 1, 0, 0)", (did, dv, h("f"), h("m"), h("v")))
    conn.close()
    with pytest.raises(catalog.CatalogCorruptError, match="katalograden"):
        publish(store, df)


# ================================================================== katalog
def test_catalog_is_consistent_after_publication(store):
    df = gapped_frame()
    sources = [src("a.zip", 20), src("b.zip", 17)]
    res = publish(store, df, {"sources": sources})
    i = res.info
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert catalog.check_catalog(conn, full=True) == []
    assert catalog.list_datasets(conn) == [{"dataset_id": i.dataset_id, "exchange": "binance",
                                            "instrument": "spot", "symbol": "BTCUSDT", "timeframe": "15m"}]
    manifest = json.loads((obj_dir(store, res) / "manifest.json").read_bytes())
    assert catalog.list_dataset_versions(conn, i.dataset_id) == [{
        "dataset_id": i.dataset_id, "data_version": i.data_version,
        "data_file_sha256": i.data_file_sha256, "data_file_bytes": i.data_file_bytes,
        "manifest_sha256": i.manifest_sha256, "version_sha256": i.version_sha256,
        "manifest_schema_version": manifest["schema_version"], "rows": len(df),
        "first_open_time_ms": int(df["open_time"].iloc[0].value // 1_000_000),
        "last_open_time_ms": int(df["open_time"].iloc[-1].value // 1_000_000),
        "gap_count": manifest["gap_count"], "missing_bars_total": manifest["missing_bars_total"]}]
    assert manifest["gap_count"] == 2 and manifest["missing_bars_total"] == 3
    rows = conn.execute("SELECT source_index, name, sha256, rows FROM version_sources "
                        "ORDER BY source_index").fetchall()
    assert rows == [(0, "a.zip", h("a.zip"), 20), (1, "b.zip", h("b.zip"), 17)]
    conn.close()


def test_catalog_column_lists_match_step1_schema():
    conn = sqlite3.connect(":memory:", isolation_level=None)
    catalog._apply(conn, catalog.MIGRATIONS[0])
    assert vs._DV_COLUMNS == tuple(r[1] for r in conn.execute("PRAGMA table_info(dataset_versions)"))
    assert vs._VS_COLUMNS == tuple(r[1] for r in conn.execute("PRAGMA table_info(version_sources)"))


# ================================================================== lås
def test_second_writer_fails_immediately(store):
    with store.writer():
        t0 = time.monotonic()
        with pytest.raises(vs.StoreLockedError), store.writer():
            pass
        assert time.monotonic() - t0 < 1.0
        with pytest.raises(vs.StoreLockedError), vs.VersionedStore(store.root).writer():
            pass
    with store.writer() as w:                                     # släppt igen
        w.publish_version(KEY, canon_frame("15m", 20))


def test_closed_writer_cannot_publish(store):
    with store.writer() as w:
        pass
    with pytest.raises(vs.StoreLockedError, match="stängd"):
        w.publish_version(KEY, canon_frame("15m", 20))


def test_writer_lock_is_exclusive_across_processes_and_readers_need_no_lock(store, tmp_path):
    res = publish(store, canon_frame("15m", 30))
    ready, stop = tmp_path / "ready", tmp_path / "stop"
    code = ("import time, pathlib\n"
            "from btc_trading_system.database import versioned_store as vs\n"
            f"st = vs.VersionedStore({str(store.root)!r})\n"
            "with st.writer():\n"
            f"    pathlib.Path({str(ready)!r}).touch()\n"
            f"    while not pathlib.Path({str(stop)!r}).exists():\n"
            "        time.sleep(0.01)\n")
    env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONDONTWRITEBYTECODE": "1"}
    child = subprocess.Popen([sys.executable, "-c", code], env=env, cwd=ROOT)
    try:
        deadline = time.monotonic() + 30
        while not ready.exists():
            assert child.poll() is None and time.monotonic() < deadline
            time.sleep(0.01)
        with pytest.raises(vs.StoreLockedError):
            publish(store, canon_frame("15m", 20, seed=5))
        frame, _ = vs.VersionedStore(store.root).read_version(KEY, res.info.data_version)   # läsare: inget lås
        assert len(frame) == 30
    finally:
        stop.touch()
        assert child.wait(timeout=30) == 0
    assert publish(store, canon_frame("15m", 20, seed=5)).published


def test_reader_sees_no_object_or_a_complete_verified_one(store, monkeypatch):
    df = canon_frame("15m", 40)
    dv = content_hash(canonicalize(df))
    reader = vs.VersionedStore(store.root)
    seen = []
    real = vs._rename

    def observed(src_, dst):
        with pytest.raises(DatasetNotFoundError):
            reader.read_version(KEY, dv)
        seen.append("absent")
        real(src_, dst)
        reader.verify_version(KEY, dv)                            # direkt efter rename: komplett
        seen.append("complete")

    monkeypatch.setattr(vs, "_rename", observed)
    publish(store, df)
    assert seen == ["absent", "complete"]


def test_concurrent_reader_never_sees_a_partial_object(store):
    frames = [canon_frame("15m", 300, seed=s) for s in range(4)]
    dvs = [content_hash(canonicalize(f)) for f in frames]
    code = ("from btc_trading_system.database import versioned_store as vs\n"
            "from btc_trading_system.data.schema import DatasetKey\n"
            "from data_helpers import canon_frame\n"
            f"st = vs.VersionedStore({str(store.root)!r})\n"
            "with st.writer() as w:\n"
            "    for s in range(4):\n"
            "        w.publish_version(DatasetKey('spot', 'BTCUSDT', '15m'), canon_frame('15m', 300, seed=s))\n")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "tests")]),
           "PYTHONDONTWRITEBYTECODE": "1"}
    child = subprocess.Popen([sys.executable, "-c", code], env=env, cwd=ROOT)
    outcomes = {"absent": 0, "complete": 0}
    reader = vs.VersionedStore(store.root)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        done = child.poll() is not None
        for dv in dvs:
            try:
                reader.verify_version(KEY, dv)
                outcomes["complete"] += 1
            except DatasetNotFoundError:
                outcomes["absent"] += 1
        if done:
            break
    assert child.wait(timeout=60) == 0
    for dv in dvs:
        reader.verify_version(KEY, dv)
    assert outcomes["complete"] >= len(dvs)


# ================================================================== staging
def test_each_publication_uses_a_unique_staging_directory(store, monkeypatch):
    made = []
    orig = vs.StoreWriter._new_staging
    monkeypatch.setattr(vs.StoreWriter, "_new_staging", lambda self: made.append(orig(self)) or made[-1])
    with store.writer() as w:
        w.publish_version(KEY, canon_frame("15m", 20, seed=1))
        w.publish_version(KEY, canon_frame("15m", 20, seed=2))
    assert len(made) == 2 and made[0] != made[1]
    assert all(p.parent == store.tmp for p in made) and list(store.tmp.iterdir()) == []


def test_staging_directory_is_never_shared(store, monkeypatch):
    fixed = uuid.UUID(int=7)
    occupied = store.tmp / fixed.hex
    occupied.mkdir()
    (occupied / "marker").write_text("annan skrivares data")
    monkeypatch.setattr(vs.uuid, "uuid4", lambda: fixed)
    cat_before, objs_before = dump(store), tree_hash(store.objects)
    with pytest.raises(FileExistsError):
        publish(store, canon_frame("15m", 20))
    assert (occupied / "marker").read_text() == "annan skrivares data"
    assert dump(store) == cat_before and tree_hash(store.objects) == objs_before


# ================================================================== kausalitet
def test_as_of_exact_boundary_includes_the_bar(store):
    df = canon_frame("15m", 20)
    res = publish(store, df)
    t5 = df["open_time"].iloc[5].to_pydatetime()
    frame, _ = store.read_version(KEY, res.info.data_version, as_of=t5 + TF)   # available_at(5) == as_of
    assert len(frame) == 6 and frame["open_time"].iloc[-1] == df["open_time"].iloc[5]
    assert_causal(frame, "15m", t5 + TF)


def test_as_of_just_before_boundary_excludes_the_bar(store):
    df = canon_frame("15m", 20)
    res = publish(store, df)
    t5 = df["open_time"].iloc[5].to_pydatetime()
    frame, _ = store.read_version(KEY, res.info.data_version, as_of=t5 + TF - US)
    assert len(frame) == 5 and frame["open_time"].iloc[-1] == df["open_time"].iloc[4]
    assert_causal(frame, "15m", t5 + TF - US)


def test_in_progress_bar_is_never_exposed(store):
    df = canon_frame("15m", 20)
    res = publish(store, df)
    t5 = df["open_time"].iloc[5].to_pydatetime()
    for as_of in (t5, t5 + TF / 2, t5 + TF - US):                  # bar 5 pågår
        frame, _ = store.read_version(KEY, res.info.data_version, as_of=as_of)
        assert df["open_time"].iloc[5] not in set(frame["open_time"])
        assert_causal(frame, "15m", as_of)


def test_as_of_before_first_availability_is_empty_with_schema(store):
    df = canon_frame("15m", 20)
    res = publish(store, df)
    frame, _ = store.read_version(KEY, res.info.data_version,
                                  as_of=df["open_time"].iloc[0].to_pydatetime() + TF - US)
    assert len(frame) == 0 and list(frame.columns) == list(df.columns)


def test_as_of_none_and_far_future_return_everything(store):
    df = canon_frame("15m", 20)
    res = publish(store, df)
    full, _ = store.read_version(KEY, res.info.data_version)
    fut, _ = store.read_version(KEY, res.info.data_version, as_of=datetime(2100, 1, 1, tzinfo=UTC))
    pd.testing.assert_frame_equal(full, df)
    pd.testing.assert_frame_equal(fut, df)


@pytest.mark.parametrize("bad", [datetime(2024, 1, 1, 5),  # noqa: DTZ001 - avsiktligt naiv
                                 datetime(2024, 1, 1, 5, tzinfo=timezone(timedelta(hours=1)))])
def test_as_of_must_be_utc(store, bad):
    res = publish(store, canon_frame("15m", 20))
    with pytest.raises(ValueError, match="UTC"):
        store.read_version(KEY, res.info.data_version, as_of=bad)


def test_future_rows_do_not_affect_historical_view(store):
    """as_of-vyn av full data == hela prefix-versionen (inkl. gap_before)."""
    full = gapped_frame(n=40, drop=(10, 11, 25))
    k = 20
    raw = make_klines("15m", 40).drop(columns=["close_time"]).drop(index=[10, 11, 25]).reset_index(drop=True)
    prefix = raw.iloc[:k].reset_index(drop=True)
    prefix["gap_before"], _ = compute_gap_flags(to_epoch_ns(prefix["open_time"]), "15m")
    prefix = canonicalize(prefix)
    rf, rp = publish(store, full), publish(store, prefix)
    assert rf.info.data_version != rp.info.data_version
    as_of = prefix["open_time"].iloc[-1].to_pydatetime() + TF
    view, _ = store.read_version(KEY, rf.info.data_version, as_of=as_of)
    pinned, _ = store.read_version(KEY, rp.info.data_version)
    pd.testing.assert_frame_equal(view, pinned)
    assert view["gap_before"].sum() == 1                         # luckan vid index 10-11 syns, 25 inte


def test_pinned_version_read_ignores_newer_versions(store):
    df = canon_frame("15m", 20)
    old = publish(store, df.iloc[:10].reset_index(drop=True))
    publish(store, df)                                           # nyare version publiceras
    frame, manifest = store.read_version(KEY, old.info.data_version,
                                         as_of=datetime(2100, 1, 1, tzinfo=UTC))
    assert len(frame) == 10 and manifest["data_version"] == old.info.data_version


def test_as_of_matches_phase2_provider_semantics(store, tmp_path):
    df = gapped_frame(n=60, drop=(5, 30, 31))
    res = publish(store, df)
    p2 = DatasetStore(tmp_path / "p2", ParquetBackend())
    p2.write(KEY, df, {})
    prov = LocalOHLCVProvider(p2, "spot")
    end = datetime(2030, 1, 1, tzinfo=UTC)
    t = df["open_time"]
    for as_of in [t.iloc[0] + TF - US, t.iloc[0] + TF, t.iloc[29] + TF, t.iloc[29] + TF - US,
                  t.iloc[-1] + TF, t.iloc[-1] + 3 * TF]:
        as_of = as_of.to_pydatetime()
        ours, _ = store.read_version(KEY, res.info.data_version, as_of=as_of)
        theirs = prov.get_bars("BTCUSDT", "15m", START, end, as_of=as_of)
        assert list(ours["open_time"]) == list(theirs.index)
        assert list(ours["gap_before"]) == list(theirs["gap_before"])


def test_mutation_future_data_leak_is_caught(store, monkeypatch):
    """Mutation: cutoff utan '- timeframe' (open_time <= as_of) läcker pågående bar."""
    df = canon_frame("15m", 20)
    res = publish(store, df)
    as_of = df["open_time"].iloc[5].to_pydatetime() + TF - US
    monkeypatch.setattr(vs, "causal_cutoff", lambda a, tf: pd.Timestamp(a))
    leaked, _ = store.read_version(KEY, res.info.data_version, as_of=as_of)
    with pytest.raises(AssertionError, match="framtida"):
        assert_causal(leaked, "15m", as_of)


# ================================================================== läs-API
def test_read_missing_version_and_invalid_ids(store):
    with pytest.raises(DatasetNotFoundError):
        store.read_version(KEY, h("missing"))
    for bad in ("x", h("a").upper(), h("a")[:63]):
        with pytest.raises(ValueError):
            store.object_dir(KEY, bad)
    with pytest.raises(ValueError):
        store.object_dir(DatasetKey("spot", "BTCUSDT", "15m", exchange="bybit"), h("a"))
    assert not store.has_version(KEY, h("missing"))
