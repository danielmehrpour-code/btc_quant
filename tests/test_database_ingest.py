"""PHASE 4 steg 4: ingest med lineage (Phase 2-ingest oförändrad, lineage-poster, ref via steg 3)."""
import dataclasses
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest
from data_helpers import NOW, START, make_klines, write_csv

from btc_trading_system.data.ingest import ingest_dataset
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data.storage import DatasetStore, ParquetBackend, canonicalize
from btc_trading_system.data_validation.errors import (
    DataError,
    DataValidationError,
    MalformedDataError,
)
from btc_trading_system.database import catalog
from btc_trading_system.database import versioned_store as vs
from btc_trading_system.database.lineage import (
    CONTENT_DERIVED_KEYS,
    IngestExtraError,
    IngestRecord,
    IngestRecordError,
    IngestSource,
    split_ingest_extra,
)
from btc_trading_system.database.refs import DatasetVersionRef

ROOT = Path(__file__).resolve().parents[1]
KEY = DatasetKey("spot", "BTCUSDT", "15m")
PERP = DatasetKey("perp", "BTCUSDT", "15m")
TF = timedelta(minutes=15)


def h(tag: str) -> str:
    return hashlib.sha256(tag.encode()).hexdigest()


@pytest.fixture
def store(tmp_path):
    return vs.VersionedStore.init(tmp_path / "store")


@pytest.fixture
def src(tmp_path):
    d = tmp_path / "src"
    d.mkdir()
    return d


def csv(d: Path, name: str, n: int = 60, seed: int = 0, drop=None, rows=None) -> Path:
    df = make_klines("15m", n, seed)
    if rows is not None:
        df = df.iloc[rows].reset_index(drop=True)
    if drop:
        df = df.drop(index=drop).reset_index(drop=True)
    return write_csv(d / name, df)


def ingest(store, files, key=KEY, **kw):
    kw = {"now": NOW, "code_version": "test", "worktree_clean": True, **kw}
    with store.writer() as w:
        return w.ingest(key, files, **kw)


def dump(store) -> str:
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        return catalog.dump_catalog(conn)
    finally:
        conn.close()


def tree_hash(root: Path) -> str:
    hh = hashlib.sha256()
    for p in [root, *sorted(root.rglob("*"))]:
        st = os.lstat(p)
        hh.update(f"{p.relative_to(root)}|{st.st_mode:o}|".encode())
        if p.is_file() and not p.is_symlink():
            hh.update(p.read_bytes())
    return hh.hexdigest()


def state_hash(store) -> str:
    """Allt utom tmp/ och låsfilen."""
    hh = hashlib.sha256()
    for name in ("objects", "refs", "snapshots", "ingests", "ref_log.jsonl", "catalog.sqlite"):
        p = store.root / name
        hh.update(name.encode() + (tree_hash(p).encode() if p.is_dir() else
                                   p.read_bytes() if p.exists() else b"-"))
    return hh.hexdigest()


def rewrite(path: Path, data: bytes) -> None:
    os.chmod(path, 0o644)
    path.write_bytes(data)
    os.chmod(path, 0o444)


def patch_object_file(d: Path, name: str, data: bytes) -> None:
    os.chmod(d, 0o755)
    rewrite(d / name, data)
    os.chmod(d, 0o555)


def records_dir(store) -> Path:
    return store.root / "ingests"


def catalog_rows(store, sql):
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def corrupt_object(store, key, dv) -> None:
    """Samma storlek, samma logiska innehåll, andra bytes: bara filhashen avslöjar det."""
    d = store.object_dir(key, dv)
    data = (d / "bars.parquet").read_bytes()
    i = data.find(b"parquet-cpp-arrow")
    patch_object_file(d, "bars.parquet", data[:i] + b"Parquet" + data[i + 7:])


def crash(monkeypatch, cls_or_mod, name, msg="injicerad krasch"):
    def boom(*a, **k):
        raise RuntimeError(msg)
    monkeypatch.setattr(cls_or_mod, name, boom)


# ================================================================== paritet med Phase 2
def test_parity_with_phase2_ingest(store, src, tmp_path):
    files = [csv(src, "BTCUSDT-15m-2024-01.csv", n=80, drop=[10, 11, 40])]
    out = ingest(store, files)
    p2 = DatasetStore(tmp_path / "p2", ParquetBackend())
    ref = ingest_dataset(files, "spot", "15m", p2, now=NOW)
    assert out.record.ref.data_version == ref.manifest["data_version"]
    ours, manifest = store.read_version(KEY, out.record.ref.data_version)
    theirs, m2 = p2.read(KEY)
    pd.testing.assert_frame_equal(ours, theirs)
    for k in CONTENT_DERIVED_KEYS:
        assert manifest[k] == m2[k]
    assert manifest["gap_count"] == 2 and manifest["missing_bars_total"] == 3
    assert [s.to_json_obj() for s in out.record.sources] == m2["sources"]
    assert out.record.duplicates_dropped == m2["duplicates_dropped"]


def test_manifest_contains_only_base_and_content_keys(store, src):
    out = ingest(store, [csv(src, "BTCUSDT-15m-2024-01.csv")])
    _, manifest = store.read_version(KEY, out.record.ref.data_version)
    assert set(manifest) == vs.MANIFEST_BASE_KEYS | CONTENT_DERIVED_KEYS
    assert "sources" not in manifest and "duplicates_dropped" not in manifest
    assert catalog_rows(store, "SELECT count(*) FROM version_sources") == [(0,)]


@pytest.mark.parametrize("name,files,kw,exc", [
    ("conflicting_duplicates", lambda d: [csv(d, "a.csv", n=10, seed=0), csv(d, "b.csv", n=10, seed=1)], {},
     DataValidationError),
    ("wrong_filename", lambda d: [csv(d, "ETHUSDT-15m-2024-01.csv")], {}, MalformedDataError),
    ("unfinished_bar_at_now", lambda d: [csv(d, "a.csv", n=10)], {"now": START + 5 * TF}, DataValidationError),
    ("fail_on_gaps", lambda d: [csv(d, "a.csv", n=30, drop=[5])], {"fail_on_gaps": True}, DataValidationError),
])
def test_phase2_errors_write_nothing(store, src, name, files, kw, exc):
    before = state_hash(store)
    with pytest.raises(exc):
        ingest(store, files(src), **kw)
    assert state_hash(store) == before and list(store.tmp.iterdir()) == []


def test_gaps_are_allowed_without_fail_on_gaps(store, src):
    out = ingest(store, [csv(src, "a.csv", n=30, drop=[5])])
    assert out.record.params["fail_on_gaps"] is False
    assert store.read_version(KEY, out.record.ref.data_version)[1]["gap_count"] == 1


# ================================================================== lineage
def test_first_ingest_is_published_and_changes_the_ref(store, src):
    f = csv(src, "BTCUSDT-15m-2024-01.csv")
    out = ingest(store, [f])
    r = out.record
    assert (r.seq, r.prev_ingest_id, r.publication, r.ref_outcome) == (1, None, "published", "changed")
    assert (r.previous_data_version, r.ref_log_seq) == (None, 1)
    assert r.sources == (IngestSource(f.name, 60, hashlib.sha256(f.read_bytes()).hexdigest()),)
    assert r.params == {"now": NOW.isoformat(), "fail_on_gaps": False, "allow_regression": False}
    assert (r.code_version, r.worktree_clean, r.duplicates_dropped) == ("test", True, 0)
    p = records_dir(store) / f"{out.ingest_id}.json"
    assert os.stat(p).st_mode & 0o777 == 0o444
    assert out.record_sha256 == hashlib.sha256(p.read_bytes()).hexdigest()
    assert store.current_ref(KEY).ref.data_version == r.ref.data_version
    line = json.loads((store.root / "ref_log.jsonl").read_bytes().splitlines()[0])
    assert line["action"] == "ingest" and line["seq"] == 1
    assert hashlib.sha256((store.root / "ref_log.jsonl").read_bytes().splitlines()[0]).hexdigest() == \
        r.ref_log_entry_sha256
    assert list(store.tmp.iterdir()) == []


def test_repeated_identical_ingest_is_never_collapsed(store, src):
    f = csv(src, "BTCUSDT-15m-2024-01.csv")
    a, b = ingest(store, [f]), ingest(store, [f])
    assert a.ingest_id != b.ingest_id
    assert (b.record.seq, b.record.prev_ingest_id) == (2, a.ingest_id)
    assert (b.record.publication, b.record.ref_outcome) == ("reused", "unchanged")
    assert b.record.previous_data_version == b.record.ref.data_version == a.record.ref.data_version
    assert b.record.ref_log_seq is None
    assert len((store.root / "ref_log.jsonl").read_bytes().splitlines()) == 1
    assert catalog_rows(store, "SELECT seq, publication, ref_outcome FROM ingest_events ORDER BY seq") == \
        [(1, "published", "changed"), (2, "reused", "unchanged")]


def test_identical_content_from_different_files_is_reused_with_new_lineage(store, src):
    monthly = csv(src, "BTCUSDT-15m-2024-01.csv", n=60)
    day1 = csv(src, "BTCUSDT-15m-2024-01-01.csv", n=60, rows=slice(0, 40))
    day2 = csv(src, "BTCUSDT-15m-2024-01-02.csv", n=60, rows=slice(30, 60))   # 10 rader överlapp
    a = ingest(store, [monthly])
    b = ingest(store, [day2, day1])                                             # annan ordning, andra filer
    assert b.record.ref.data_version == a.record.ref.data_version
    assert (b.record.publication, b.record.ref_outcome) == ("reused", "unchanged")
    assert [s.name for s in b.record.sources] == [day1.name, day2.name]
    assert b.record.duplicates_dropped == 10
    assert catalog_rows(store, "SELECT ingest_seq, name, rows FROM ingest_sources ORDER BY 1, 2") == \
        [(1, monthly.name, 60), (2, day1.name, 40), (2, day2.name, 30)]
    assert catalog_rows(store, "SELECT seq, duplicates_dropped FROM ingest_events ORDER BY seq") == \
        [(1, 0), (2, 10)]


def test_new_content_is_published_and_moves_the_ref(store, src):
    a = ingest(store, [csv(src, "a.csv", n=60)])
    b = ingest(store, [csv(src, "b.csv", n=90)])
    assert (b.record.publication, b.record.ref_outcome) == ("published", "changed")
    assert (b.record.previous_data_version, b.record.ref_log_seq) == (a.record.ref.data_version, 2)
    assert store.current_ref(KEY).ref.data_version == b.record.ref.data_version


def test_ingest_history_and_read_ingest(store, src):
    a = ingest(store, [csv(src, "a.csv")])
    b = ingest(store, [csv(src, "p.csv", seed=3)], key=PERP)
    assert store.ingest_history() == (a.record, b.record)
    assert store.ingest_history(PERP) == (b.record,)
    assert store.read_ingest(a.ingest_id) == a.record
    with pytest.raises(DataError):
        store.read_ingest(h("missing"))
    with pytest.raises(ValueError):
        store.read_ingest("x")


def test_worktree_clean_none_is_recorded_as_null(store, src):
    out = ingest(store, [csv(src, "a.csv")], worktree_clean=None, code_version="unknown")
    assert out.record.worktree_clean is None
    assert catalog_rows(store, "SELECT code_version, worktree_clean FROM ingest_events") == [("unknown", None)]


@pytest.mark.parametrize("kw", [{"code_version": ""}, {"code_version": None}, {"worktree_clean": "yes"}])
def test_invalid_code_state_is_rejected_before_any_write(store, src, kw):
    before = state_hash(store)
    with pytest.raises(ValueError):
        ingest(store, [csv(src, "a.csv")], **kw)
    assert state_hash(store) == before


def test_now_must_be_explicit_utc(store, src):
    with store.writer() as w, pytest.raises(TypeError):
        w.ingest(KEY, [csv(src, "a.csv")], code_version="t", worktree_clean=True)     # now saknas
    with pytest.raises(ValueError, match="UTC"):
        ingest(store, [csv(src, "a.csv")], now=NOW.replace(tzinfo=None))


def test_golden_first_record(tmp_path):
    d = tmp_path / "g"
    d.mkdir()
    st = vs.VersionedStore.init(tmp_path / "gs")
    out = ingest(st, [csv(d, "BTCUSDT-15m-2024-01.csv", n=40, seed=1)])
    data = (st.root / "ingests" / f"{out.ingest_id}.json").read_bytes()
    assert out.ingest_id == GOLDEN_INGEST_ID
    assert hashlib.sha256(data).hexdigest() == GOLDEN_RECORD_SHA
    assert data == GOLDEN_RECORD


GOLDEN_INGEST_ID = "802b1d6db6befd6d00af9943efd97448cfd5dbd39c5fa6f99eeb9eb8ab5add9c"
GOLDEN_RECORD_SHA = "cfdbd27a9874d30f61b223a611eee56f28c0abc57d75f0f7f4d6543c2b8f4c53"
GOLDEN_RECORD = (
    b"{\n  \"code\": {\n    \"version\": \"test\",\n    \"worktree_clean\": true\n  },\n  \"data_ver"
    b"sion\": \"3a2a655a4d9007586cd1afcc9f4e9995dbc79017628807ad9b99071b6e171026\",\n  \"da"
    b"taset\": {\n    \"exchange\": \"binance\",\n    \"instrument\": \"spot\",\n    \"symbol\": \"BT"
    b"CUSDT\",\n    \"timeframe\": \"15m\"\n  },\n  \"dataset_id\": \"f607d43c4d9de93d95427145ed5"
    b"da4396e73b08cf6267ef471de6d700eb8e052\",\n  \"duplicates_dropped\": 0,\n  \"format\": \""
    b"btc-quant/ingest/v1\",\n  \"params\": {\n    \"allow_regression\": false,\n    \"fail_on_"
    b"gaps\": false,\n    \"now\": \"2030-01-01T00:00:00+00:00\"\n  },\n  \"prev_ingest_id\": nu"
    b"ll,\n  \"previous_data_version\": null,\n  \"publication\": \"published\",\n  \"ref_log_en"
    b"try_sha256\": \"a806e3762e50b66642e0c18b4e15318164412118ead1f63f2a7ac7bc899e9f0c\","
    b"\n  \"ref_log_seq\": 1,\n  \"ref_outcome\": \"changed\",\n  \"seq\": 1,\n  \"sources\": [\n    "
    b"{\n      \"name\": \"BTCUSDT-15m-2024-01.csv\",\n      \"rows\": 40,\n      \"sha256\": \"e6"
    b"4b7ca5d254afeb6527ebc27c692778884a0f190076092a7f742f8366a1edee\"\n    }\n  ]\n}\n"
)


# ================================================================== innehållsnycklar / återbruk
def test_unknown_extra_key_raises_before_any_write(store, src, monkeypatch):
    real = vs._CaptureStore.write
    monkeypatch.setattr(vs._CaptureStore, "write",
                        lambda self, k, df, extra: real(self, k, df, {**extra, "surprise": 1}))
    before = state_hash(store)
    with pytest.raises(IngestExtraError, match="surprise"):
        ingest(store, [csv(src, "a.csv")])
    assert state_hash(store) == before


def test_missing_phase2_key_raises_before_any_write(store, src, monkeypatch):
    real = vs._CaptureStore.write
    monkeypatch.setattr(vs._CaptureStore, "write",
                        lambda self, k, df, extra: real(self, k, df, {x: v for x, v in extra.items()
                                                                        if x != "duplicates_dropped"}))
    before = state_hash(store)
    with pytest.raises(IngestExtraError, match="saknar"):
        ingest(store, [csv(src, "a.csv")])
    assert state_hash(store) == before


def test_split_ingest_extra():
    s = {"name": "a.csv", "rows": 3, "sha256": h("a")}
    content, sources, dropped = split_ingest_extra(
        {"gaps": [], "zero_volume_bars": 0, "sources": [s], "duplicates_dropped": 2})
    assert content == {"gaps": [], "zero_volume_bars": 0} and dropped == 2
    assert sources == (IngestSource("a.csv", 3, h("a")),)
    with pytest.raises(IngestExtraError, match="lista"):
        split_ingest_extra({"gaps": [], "zero_volume_bars": 0, "sources": "x", "duplicates_dropped": 0})
    with pytest.raises(IngestExtraError, match="duplicates_dropped"):
        split_ingest_extra({"gaps": [], "zero_volume_bars": 0, "sources": [s], "duplicates_dropped": -1})
    with pytest.raises(IngestExtraError, match="källa"):
        split_ingest_extra({"gaps": [], "zero_volume_bars": 0, "sources": [{"name": "a.csv"}],
                            "duplicates_dropped": 0})


def test_lineage_errors_are_value_errors_and_data_errors():
    for cls in (IngestExtraError, IngestRecordError):
        assert issubclass(cls, ValueError) and issubclass(cls, DataError)
    assert not issubclass(IngestExtraError, IngestRecordError) and not issubclass(IngestRecordError, IngestExtraError)


def _golden_obj():
    return json.loads(GOLDEN_RECORD)


def _file(obj) -> bytes:
    return (json.dumps(obj, sort_keys=True, indent=2) + "\n").encode()


def _with(**changes):
    obj = _golden_obj()
    for k, v in changes.items():
        if "." in k:
            a, b = k.split(".")
            obj[a][b] = v
        else:
            obj[k] = v
    return _file(obj)


@pytest.mark.parametrize("name,data,match", [
    ("not_json", b"{not json", "inte JSON"),
    ("unknown_key", _file({**_golden_obj(), "x": 1}), "format"),
    ("wrong_format", _with(format="btc-quant/ingest/v0"), "format"),
    ("non_canonical", json.dumps(_golden_obj(), sort_keys=True).encode(), "kanoniskt"),
    ("bad_dataset_id", _with(dataset_id="0" * 64), "datasetreferens"),
    ("bad_dataset", _with(dataset=[1]), "datasetreferens"),
    ("seq_as_string", _with(seq="1"), "seq"),
    ("naive_now", _with(**{"params.now": "2030-01-01T00:00:00"}), "UTC"),
    ("now_not_string", _with(**{"params.now": 1}), "ISO"),
    ("flag_not_bool", _with(**{"params.allow_regression": 0}), "bool"),
    ("source_missing_field", _with(sources=[{"name": "a.csv", "rows": 1}]), "källa"),
])
def test_strict_parsing_raises_ingest_record_error(name, data, match):
    with pytest.raises(IngestRecordError, match=match):
        IngestRecord.from_file_bytes(data)


def test_parsing_checks_the_expected_id_and_construction_checks_types():
    assert IngestRecord.from_file_bytes(GOLDEN_RECORD, expected_id=GOLDEN_INGEST_ID).ingest_id == GOLDEN_INGEST_ID
    with pytest.raises(IngestRecordError, match="förväntat"):
        IngestRecord.from_file_bytes(GOLDEN_RECORD, expected_id=h("other"))
    rec = IngestRecord.from_file_bytes(GOLDEN_RECORD)
    with pytest.raises(IngestRecordError, match="DatasetVersionRef"):
        dataclasses.replace(rec, ref="x")
    with pytest.raises(IngestRecordError, match="ref_log"):
        dataclasses.replace(rec, ref_log_seq=None)


def test_reuse_with_tampered_content_keys_is_object_corrupt(store, src):
    f = csv(src, "a.csv")
    out = ingest(store, [f])
    d = store.object_dir(KEY, out.record.ref.data_version)
    m = json.loads((d / "manifest.json").read_bytes())
    m["zero_volume_bars"] += 1
    mbytes = vs.canonical_json_bytes(m)
    patch_object_file(d, "manifest.json", mbytes)
    v = json.loads((d / "version.json").read_bytes())
    v["manifest"] = {"path": "manifest.json", "sha256": hashlib.sha256(mbytes).hexdigest(), "bytes": len(mbytes)}
    patch_object_file(d, "version.json", vs.canonical_json_bytes(v))
    store.verify_version(KEY, out.record.ref.data_version)            # konsistent manipulerat objekt
    before = state_hash(store)
    with pytest.raises(vs.ObjectCorruptError, match="innehållsnycklar"):
        ingest(store, [f])
    assert state_hash(store) == before


def test_reuse_of_corrupt_existing_object_fails_and_moves_nothing(store, src):
    f = csv(src, "a.csv")
    out = ingest(store, [f])
    corrupt_object(store, KEY, out.record.ref.data_version)
    before = state_hash(store)
    with pytest.raises(vs.ObjectCorruptError):
        ingest(store, [f])
    assert state_hash(store) == before


def test_reuse_ignores_source_fields_of_legacy_style_manifest(store, src, tmp_path):
    f = csv(src, "a.csv")
    p2 = DatasetStore(tmp_path / "p2", ParquetBackend())
    m2 = ingest_dataset([f], "spot", "15m", p2, now=NOW).manifest
    legacy_extra = {k: m2[k] for k in ("sources", "duplicates_dropped", "gaps", "zero_volume_bars")}
    with store.writer() as w:
        w.publish_version(KEY, p2.read(KEY)[0], legacy_extra)           # manifest bär sources (som legacy)
    out = ingest(store, [csv(src, "other-name.csv")])
    assert (out.record.publication, out.record.ref_outcome) == ("reused", "changed")
    assert "sources" in store.read_version(KEY, out.record.ref.data_version)[1]


def test_public_publish_version_keeps_strict_m2(store, src, tmp_path):
    f = csv(src, "a.csv")
    ingest(store, [f])
    p2 = DatasetStore(tmp_path / "p2", ParquetBackend())
    m2 = ingest_dataset([f], "spot", "15m", p2, now=NOW).manifest
    with store.writer() as w, pytest.raises(vs.ProvenanceConflictError):
        w.publish_version(KEY, p2.read(KEY)[0], {"sources": m2["sources"], "gaps": m2["gaps"],
                                                 "zero_volume_bars": m2["zero_volume_bars"]})


# ================================================================== regressionsskydd
def test_regression_guard_refuses_an_earlier_ending_version(store, src):
    ingest(store, [csv(src, "full.csv", n=90)])
    before = state_hash(store)
    with pytest.raises(vs.VersionRegressionError):
        ingest(store, [csv(src, "short.csv", n=60)])
    assert state_hash(store) == before


def test_regression_is_allowed_explicitly_and_recorded(store, src):
    ingest(store, [csv(src, "full.csv", n=90)])
    out = ingest(store, [csv(src, "short.csv", n=60)], allow_regression=True)
    assert out.record.ref_outcome == "changed" and out.record.allow_regression is True
    assert json.loads(catalog_rows(store, "SELECT params_json FROM ingest_events WHERE seq = 2")[0][0]) == \
        {"now": NOW.isoformat(), "fail_on_gaps": False, "allow_regression": True}


def test_revision_with_same_end_is_not_a_regression(store, src):
    ingest(store, [csv(src, "a.csv", n=60, seed=0)])
    out = ingest(store, [csv(src, "b.csv", n=60, seed=5)])                   # samma slut, annat innehåll
    assert out.record.ref_outcome == "changed"


# ================================================================== ordning / crash / väntande
def test_ingest_write_order(store, src, monkeypatch):
    events = []
    ov, olink, oren = vs.VersionedStore._verify_object, vs._link, vs._rename
    osr, osi = vs.StoreWriter._sync_ref_catalog, vs.StoreWriter._sync_ingest_catalog
    monkeypatch.setattr(vs.VersionedStore, "_verify_object",
                        lambda self, k, dv: (events.append("verify"), ov(self, k, dv))[1])
    monkeypatch.setattr(vs, "_link", lambda a, b: (events.append(f"link:{Path(b).parent.name}"), olink(a, b)))
    monkeypatch.setattr(vs, "_rename", lambda a, b: (events.append(
        "rename:object" if catalog.is_sha256_hex(Path(b).name) else f"rename:{Path(b).name}"), oren(a, b)))
    monkeypatch.setattr(vs.StoreWriter, "_sync_ref_catalog",
                        lambda self, st: (events.append("sync_ref"), osr(self, st)))
    monkeypatch.setattr(vs.StoreWriter, "_sync_ingest_catalog",
                        lambda self, r: (events.append("sync_ingest"), osi(self, r)))
    ingest(store, [csv(src, "a.csv")])
    i_obj = events.index("rename:object")
    order = [i_obj, events.index("verify", i_obj), events.index("link:ingests"),
             events.index("rename:ref_log.jsonl"), events.index("rename:15m.json"),
             events.index("sync_ref"), events.index("sync_ingest")]
    assert order == sorted(order) and len(set(order)) == len(order), events


def test_crash_before_record_leaves_no_lineage(store, src, monkeypatch):
    f = csv(src, "a.csv")
    with monkeypatch.context() as m:
        crash(m, vs.StoreWriter, "_publish_ingest_record")
        with pytest.raises(RuntimeError):
            ingest(store, [f])
    assert store.ingest_history() == () and store.current_ref(KEY).ref is None
    out = ingest(store, [f])
    assert (out.record.seq, out.record.publication, out.record.ref_outcome) == (1, "reused", "changed")


def _pending_ingest(store, src, monkeypatch):
    first = ingest(store, [csv(src, "a.csv", n=60)])
    with monkeypatch.context() as m:
        crash(m, vs.StoreWriter, "_append_ref_log")
        with pytest.raises(RuntimeError):
            ingest(store, [csv(src, "b.csv", n=90)])
    return first, store.ingest_history()[-1]


def test_crash_after_record_blocks_everything_until_explicit_completion(store, src, monkeypatch):
    first, pending = _pending_ingest(store, src, monkeypatch)
    assert pending.seq == 2 and pending.ref_log_seq == 2
    assert store.current_ref(KEY).ref.data_version == first.record.ref.data_version     # läsare: gammal ref
    before = state_hash(store)
    snap_member = DatasetVersionRef(KEY, first.record.ref.data_version)
    with store.writer() as w:
        for call in (lambda: w.ingest(KEY, [csv(src, "c.csv")], now=NOW, code_version="t", worktree_clean=True),
                     lambda: w.set_current(KEY, first.record.ref.data_version),
                     lambda: w.create_snapshot([snap_member]), lambda: w.snapshot_current([KEY]),
                     lambda: w.complete_pending_ref_updates()):
            with pytest.raises(vs.PendingIngestError):
                call()
    assert state_hash(store) == before
    with store.writer() as w:
        done = w.complete_pending_ingest()
    assert done.record == pending and done.ingest_id == pending.ingest_id
    assert store.current_ref(KEY).ref.data_version == pending.ref.data_version
    assert catalog_rows(store, "SELECT seq, ref_log_seq FROM ingest_events ORDER BY seq") == [(1, 1), (2, 2)]
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert catalog.check_catalog(conn, full=True) == []
    conn.close()
    with store.writer() as w:
        w.set_current(KEY, first.record.ref.data_version)                           # blockeringen hävd


def test_crash_after_log_before_ref_is_a_d1_pending(store, src, monkeypatch):
    ingest(store, [csv(src, "a.csv", n=60)])
    with monkeypatch.context() as m:
        crash(m, vs.StoreWriter, "_write_ref_file")
        with pytest.raises(RuntimeError):
            ingest(store, [csv(src, "b.csv", n=90)])
    assert store.current_ref(KEY).pending
    with pytest.raises(vs.PendingRefUpdateError):
        ingest(store, [csv(src, "c.csv", n=90, seed=2)])
    with store.writer() as w:
        assert len(w.complete_pending_ref_updates()) == 1
    ingest(store, [csv(src, "b.csv", n=90)])                                          # synkar ingest-rader
    assert catalog_rows(store, "SELECT seq, ref_outcome FROM ingest_events ORDER BY seq") == \
        [(1, "changed"), (2, "changed"), (3, "unchanged")]


def test_crash_before_ingest_catalog_is_synced_later(store, src, monkeypatch):
    f = csv(src, "a.csv")
    with monkeypatch.context() as m:
        crash(m, vs.StoreWriter, "_sync_ingest_catalog")
        with pytest.raises(RuntimeError):
            ingest(store, [f])
    assert len(store.ingest_history()) == 1
    assert catalog_rows(store, "SELECT count(*) FROM ingest_events") == [(0,)]
    ingest(store, [f])
    assert catalog_rows(store, "SELECT seq FROM ingest_events ORDER BY seq") == [(1,), (2,)]


def test_complete_refuses_when_object_fails_verification_now(store, src, monkeypatch):
    _, pending = _pending_ingest(store, src, monkeypatch)
    corrupt_object(store, KEY, pending.ref.data_version)
    before = state_hash(store)
    with store.writer() as w, pytest.raises(vs.ObjectCorruptError):
        w.complete_pending_ingest()
    assert state_hash(store) == before


def test_complete_refuses_when_previous_version_does_not_match_ref(store, src):
    first = ingest(store, [csv(src, "a.csv", n=60)])
    with store.writer() as w:
        other = w.publish_version(KEY, canonicalize(make_klines("15m", 70, 4).drop(columns=["close_time"])
                                                   .assign(gap_before=False))).info.data_version
        target = w.publish_version(KEY, canonicalize(make_klines("15m", 90, 5).drop(columns=["close_time"])
                                                    .assign(gap_before=False))).info.data_version
    state = store._load_ref_state()
    _, entry = vs.StoreWriter._next_entry(state, DatasetVersionRef(KEY, target), other, "ingest")
    forged = IngestRecord(seq=2, prev_ingest_id=first.ingest_id, ref=DatasetVersionRef(KEY, target),
                          publication="published", ref_outcome="changed", previous_data_version=other,
                          ref_log_seq=entry.seq, ref_log_entry_sha256=entry.entry_sha256,
                          sources=first.record.sources, duplicates_dropped=0, now=NOW.isoformat(),
                          fail_on_gaps=False, allow_regression=False, code_version="t", worktree_clean=True)
    p = records_dir(store) / f"{forged.ingest_id}.json"
    p.write_bytes(forged.to_file_bytes())
    os.chmod(p, 0o444)
    before = state_hash(store)
    with store.writer() as w, pytest.raises(vs.IngestCorruptError, match="previous_data_version"):
        w.complete_pending_ingest()
    assert state_hash(store) == before


def test_complete_without_pending_is_a_noop(store, src):
    ingest(store, [csv(src, "a.csv")])
    before = state_hash(store)
    with store.writer() as w:
        assert w.complete_pending_ingest() is None
    assert state_hash(store) == before


def test_check_is_a_noop_without_ingests(store, src, monkeypatch):
    with store.writer() as w:
        v = w.publish_version(KEY, canonicalize(make_klines("15m", 30).drop(columns=["close_time"])
                                                .assign(gap_before=False))).info.data_version
    monkeypatch.setattr(vs.VersionedStore, "_read_ingest_file", lambda *a: pytest.fail("läste ingest-post"))
    with store.writer() as w:
        w.set_current(KEY, v)
        w.snapshot_current([KEY])
    assert not records_dir(store).exists()


# ================================================================== manipulation av poster
def _two_changed(store, src):
    a = ingest(store, [csv(src, "a.csv", n=60)])
    b = ingest(store, [csv(src, "b.csv", n=90)])
    return a, b


def _chain_broken(store, a, b):
    """seq 2 med fel prev_ingest_id men giltigt filnamn och korrekt ref_log-länk."""
    bad = dataclasses.replace(b.record, prev_ingest_id=h("not-the-previous"))
    (records_dir(store) / f"{b.ingest_id}.json").chmod(0o644)
    (records_dir(store) / f"{b.ingest_id}.json").unlink()
    p = records_dir(store) / f"{bad.ingest_id}.json"
    p.write_bytes(bad.to_file_bytes())
    os.chmod(p, 0o444)


TAMPER = {
    "record_edited": lambda st, a, b: rewrite(records_dir(st) / f"{a.ingest_id}.json",
                                              (records_dir(st) / f"{a.ingest_id}.json").read_bytes()
                                              .replace(b'"duplicates_dropped": 0', b'"duplicates_dropped": 1')),
    "first_record_deleted": lambda st, a, b: ((records_dir(st) / f"{a.ingest_id}.json").chmod(0o644),
                                              (records_dir(st) / f"{a.ingest_id}.json").unlink()),
    "orphan_ingest_log_entry": lambda st, a, b: ((records_dir(st) / f"{b.ingest_id}.json").chmod(0o644),
                                                 (records_dir(st) / f"{b.ingest_id}.json").unlink()),
    "chain_broken": _chain_broken,
    "record_writable": lambda st, a, b: os.chmod(records_dir(st) / f"{a.ingest_id}.json", 0o644),
    "foreign_file": lambda st, a, b: (records_dir(st) / "notes.txt").write_text("x"),
    "record_symlink": lambda st, a, b: ((records_dir(st) / f"{a.ingest_id}.json").chmod(0o644),
                                        (records_dir(st) / f"{a.ingest_id}.json").unlink(),
                                        (records_dir(st) / f"{a.ingest_id}.json").symlink_to(
                                            records_dir(st) / f"{b.ingest_id}.json")),
}


@pytest.mark.parametrize("name", sorted(TAMPER))
def test_tampered_lineage_is_detected_by_readers_and_writers(store, src, name):
    a, b = _two_changed(store, src)
    TAMPER[name](store, a, b)
    before = state_hash(store)
    if name != "orphan_ingest_log_entry":
        with pytest.raises(vs.IngestCorruptError):
            store.ingest_history()
    with pytest.raises(vs.IngestCorruptError):
        ingest(store, [csv(src, "a.csv", n=60)])
    with store.writer() as w, pytest.raises(vs.IngestCorruptError):
        w.set_current(KEY, a.record.ref.data_version)
    assert state_hash(store) == before


def test_record_is_never_overwritten_even_in_a_race(store, src, monkeypatch):
    real = vs._fsync_file
    seen = {}

    def racing(p):
        real(p)
        if Path(p).name.endswith(".ingest.json"):
            rec = IngestRecord.from_file_bytes(Path(p).read_bytes())
            target = records_dir(store) / f"{rec.ingest_id}.json"
            target.write_bytes(b"intruder")
            seen["target"] = target
    monkeypatch.setattr(vs, "_fsync_file", racing)
    with pytest.raises(vs.ImmutableObjectError):
        ingest(store, [csv(src, "a.csv")])
    assert seen["target"].read_bytes() == b"intruder"


# ================================================================== fail-closed fångstlagring
def test_capture_store_accepts_exactly_one_write():
    cap = vs._CaptureStore(KEY)
    df = canonicalize(make_klines("15m", 10).drop(columns=["close_time"]).assign(gap_before=False))
    assert cap.write(KEY, df, {"a": [1]}) == {}
    assert cap.captured is not None
    with pytest.raises(vs.StoreError, match="mer än ett"):
        cap.write(KEY, df, {})


@pytest.mark.parametrize("method", ["data_path", "manifest_path", "exists", "read_manifest", "read"])
def test_capture_store_refuses_every_other_method(method):
    cap = vs._CaptureStore(KEY)
    with pytest.raises(vs.StoreError, match="otillåtet"):
        getattr(cap, method)(KEY)
    with pytest.raises(AttributeError):
        cap.root                                                           # noqa: B018 - ingen rot


def test_capture_store_refuses_wrong_key():
    cap = vs._CaptureStore(KEY)
    with pytest.raises(vs.StoreError):
        cap.write(PERP, pd.DataFrame(), {})


def test_ingest_fails_closed_when_phase2_writes_twice(store, src, monkeypatch):
    real = vs.ingest_dataset

    def twice(files, instrument, timeframe, st, **kw):
        res = real(files, instrument, timeframe, st, **kw)
        st.write(DatasetKey(instrument, "BTCUSDT", timeframe), pd.DataFrame(), {})
        return res
    monkeypatch.setattr(vs, "ingest_dataset", twice)
    before = state_hash(store)
    with pytest.raises(vs.StoreError, match="mer än ett"):
        ingest(store, [csv(src, "a.csv")])
    assert state_hash(store) == before


# ================================================================== katalog / läsare / lås
def test_catalog_matches_records(store, src):
    ingest(store, [csv(src, "a.csv", n=60)])
    ingest(store, [csv(src, "a.csv", n=60)])
    ingest(store, [csv(src, "b.csv", n=90)], worktree_clean=False)
    rows = catalog_rows(store, "SELECT seq, ingest_id, record_sha256, dataset_id, data_version, "
                               "previous_data_version, params_json, code_version, worktree_clean, publication, "
                               "ref_outcome, ref_log_seq, duplicates_dropped FROM ingest_events ORDER BY seq")
    for row, rec in zip(rows, store.ingest_history(), strict=True):
        sha = hashlib.sha256((records_dir(store) / f"{rec.ingest_id}.json").read_bytes()).hexdigest()
        assert row == (rec.seq, rec.ingest_id, sha, rec.ref.dataset_id, rec.ref.data_version,
                       rec.previous_data_version, json.dumps(rec.params, sort_keys=True, separators=(",", ":")),
                       rec.code_version, int(rec.worktree_clean), rec.publication, rec.ref_outcome,
                       rec.ref_log_seq, rec.duplicates_dropped)
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert catalog.check_catalog(conn, full=True) == []
    conn.close()


def test_readers_need_no_lock(store, src):
    out = ingest(store, [csv(src, "a.csv")])
    with store.writer():
        assert vs.VersionedStore(store.root).ingest_history() == (out.record,)


def test_closed_writer_cannot_ingest(store, src):
    with store.writer() as w:
        pass
    with pytest.raises(vs.StoreLockedError):
        w.ingest(KEY, [csv(src, "a.csv")], now=NOW, code_version="t", worktree_clean=True)
    with pytest.raises(vs.StoreLockedError):
        w.complete_pending_ingest()


def test_concurrent_reader_sees_only_complete_lineage(store, src):
    files = [csv(src, f"f{i}.csv", n=60 + 10 * i) for i in range(4)]
    code = ("from btc_trading_system.database import versioned_store as vs\n"
            "from btc_trading_system.data.schema import DatasetKey\n"
            "from data_helpers import NOW\n"
            f"st = vs.VersionedStore({str(store.root)!r})\n"
            "with st.writer() as w:\n"
            f"    for f in {[str(f) for f in files]!r}:\n"
            "        w.ingest(DatasetKey('spot', 'BTCUSDT', '15m'), [f], now=NOW, code_version='t',"
            " worktree_clean=True)\n")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "tests")]),
           "PYTHONDONTWRITEBYTECODE": "1"}
    child = subprocess.Popen([sys.executable, "-c", code], env=env, cwd=ROOT)
    reader = vs.VersionedStore(store.root)
    seen = set()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        done = child.poll() is not None
        hist = reader.ingest_history()
        assert [r.seq for r in hist] == list(range(1, len(hist) + 1))
        seen.add(len(hist))
        if done:
            break
    assert child.wait(timeout=60) == 0
    assert len(reader.ingest_history()) == 4 and max(seen) <= 4
