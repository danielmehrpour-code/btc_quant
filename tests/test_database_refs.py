"""PHASE 4 steg 3: refs, ref_log, snapshots, PinnedStore."""
import hashlib
import itertools
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from data_helpers import canon_frame

from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data.storage import canonicalize, content_hash
from btc_trading_system.data_validation.errors import DatasetNotFoundError
from btc_trading_system.database import catalog
from btc_trading_system.database import versioned_store as vs
from btc_trading_system.database.refs import (
    DatasetVersionRef,
    DataSnapshot,
    compact_json_bytes,
)

ROOT = Path(__file__).resolve().parents[1]
SPOT = DatasetKey("spot", "BTCUSDT", "15m")
PERP = DatasetKey("perp", "BTCUSDT", "15m")
SPOT1M = DatasetKey("spot", "BTCUSDT", "1m")
TF = timedelta(minutes=15)
US = timedelta(microseconds=1)
FAR = datetime(2100, 1, 1, tzinfo=UTC)


def h(tag: str) -> str:
    return hashlib.sha256(tag.encode()).hexdigest()


def frame(seed: int, n: int = 30, tf: str = "15m"):
    return canon_frame(tf, n, seed=seed)


def dv_of(df) -> str:
    return content_hash(canonicalize(df))


@pytest.fixture
def store(tmp_path):
    return vs.VersionedStore.init(tmp_path / "store")


def publish(store, df, key=SPOT) -> str:
    with store.writer() as w:
        return w.publish_version(key, df).info.data_version


def set_current(store, key, dv):
    with store.writer() as w:
        return w.set_current(key, dv)


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
    """Allt utom tmp/ (staging) och låsfilen."""
    hh = hashlib.sha256()
    for name in ("objects", "refs", "snapshots", "ref_log.jsonl", "catalog.sqlite"):
        p = store.root / name
        hh.update(name.encode() + (tree_hash(p).encode() if p.is_dir() else
                                   p.read_bytes() if p.exists() else b"-"))
    return hh.hexdigest()


def rewrite(path: Path, data: bytes) -> None:
    """Skriver om en skrivskyddad fil (manipulation i test) och återställer 0444."""
    os.chmod(path, 0o644)
    path.write_bytes(data)
    os.chmod(path, 0o444)


def log_lines(store) -> list[bytes]:
    p = store.root / "ref_log.jsonl"
    return p.read_bytes().splitlines() if p.exists() else []


def ref_path(store, key) -> Path:
    return store.root / "refs" / f"{key.relpath}.json"


def corrupt_object(store, key, dv) -> None:
    d = store.object_dir(key, dv)
    os.chmod(d, 0o755)
    p = d / "bars.parquet"
    data = p.read_bytes()
    i = data.find(b"parquet-cpp-arrow")
    rewrite(p, data[:i] + b"Parquet" + data[i + 7:])
    os.chmod(d, 0o555)


def crash_between_log_and_ref(store, key, dv, monkeypatch):
    """Injicerar krasch efter att ref_log skrivits men före ref-filen."""
    def boom(self, ref, entry):
        raise RuntimeError("krasch mellan logg och ref")
    with monkeypatch.context() as m:
        m.setattr(vs.StoreWriter, "_write_ref_file", boom)
        with pytest.raises(RuntimeError, match="krasch"):
            set_current(store, key, dv)


@pytest.fixture
def two_versions(store):
    """spot: v1, v2 publicerade; perp: p1 publicerad. Inga refs än."""
    return publish(store, frame(1)), publish(store, frame(2, n=40)), publish(store, frame(3), PERP)


# ================================================================== refs.py
def test_dataset_version_ref_validation():
    r = DatasetVersionRef(SPOT, h("a"))
    assert r.dataset_id == catalog.dataset_id(SPOT)
    for bad in ("x", h("a").upper(), h("a")[:63], None):
        with pytest.raises(ValueError):
            DatasetVersionRef(SPOT, bad)
    with pytest.raises(TypeError):
        DatasetVersionRef("spot/BTCUSDT/15m", h("a"))
    with pytest.raises(ValueError):
        DatasetVersionRef(DatasetKey("spot", "BTCUSDT", "15m", exchange="bybit"), h("a"))


def test_dataset_version_ref_json_is_strict():
    obj = DatasetVersionRef(SPOT, h("a")).to_json_obj()
    assert DatasetVersionRef.from_json_obj(obj) == DatasetVersionRef(SPOT, h("a"))
    bad = [{**obj, "x": 1}, {**obj, "dataset_id": h("other")},
           {**obj, "dataset": {**obj["dataset"], "symbol": "btcusdt"}},
           {**obj, "dataset": {**obj["dataset"], "extra": 1}}, [obj]]
    for b in bad:
        with pytest.raises(ValueError):
            DatasetVersionRef.from_json_obj(b)


def test_snapshot_rejects_unsorted_duplicate_and_empty():
    a, b = DatasetVersionRef(SPOT, h("a")), DatasetVersionRef(PERP, h("b"))
    ordered = sorted([a, b], key=lambda m: m.dataset_id)
    DataSnapshot(tuple(ordered))
    with pytest.raises(ValueError, match="sorterade"):
        DataSnapshot(tuple(reversed(ordered)))
    with pytest.raises(ValueError, match="en version per dataset"):
        DataSnapshot.of([a, DatasetVersionRef(SPOT, h("c"))])
    with pytest.raises(ValueError):
        DataSnapshot(())
    with pytest.raises(TypeError):
        DataSnapshot(ordered)


def test_snapshot_id_is_independent_of_member_order():
    members = [DatasetVersionRef(SPOT, h("a")), DatasetVersionRef(PERP, h("b")),
               DatasetVersionRef(SPOT1M, h("c"))]
    ids = {DataSnapshot.of(p).snapshot_id for p in itertools.permutations(members)}
    assert len(ids) == 1
    assert DataSnapshot.of(members[:2]).snapshot_id not in ids
    assert DataSnapshot.of([DatasetVersionRef(SPOT, h("x")), *members[1:]]).snapshot_id not in ids


def test_snapshot_id_golden():
    snap = DataSnapshot.of([DatasetVersionRef(SPOT, h("a")), DatasetVersionRef(PERP, h("b"))])
    assert snap.snapshot_id == hashlib.sha256(
        b"btc-quant/snapshot-id/v1\x00" + snap.canonical_members_bytes()).hexdigest()
    assert snap.snapshot_id == "11bd41b88d06f1ff10be4274cbace15e95f3aeafea27d2c4876cb1ec7f9deede"


def test_snapshot_file_round_trip_is_strict():
    snap = DataSnapshot.of([DatasetVersionRef(SPOT, h("a")), DatasetVersionRef(PERP, h("b"))])
    data = snap.to_file_bytes()
    assert DataSnapshot.from_file_bytes(data, snap.snapshot_id) == snap
    for bad in (data + b"\n", data.replace(b'"format"', b'"format" '),
                data.replace(h("a").encode(), h("z").encode())):
        with pytest.raises(ValueError):
            DataSnapshot.from_file_bytes(bad)
    with pytest.raises(ValueError, match="förväntat"):
        DataSnapshot.from_file_bytes(data, h("other"))


def test_ref_actions_match_catalog_schema():
    assert "CHECK (action IN ('ingest', 'set_current', 'import_legacy'))" in catalog.MIGRATIONS[0].sql
    assert vs.REF_ACTIONS == ("ingest", "set_current", "import_legacy")


# ================================================================== set_current
def test_first_set_current_creates_log_ref_and_catalog(store, two_versions):
    v1, _, _ = two_versions
    upd = set_current(store, SPOT, v1)
    assert upd.changed and upd.previous_data_version is None and upd.ref_log_seq == 1
    cur = store.current_ref(SPOT)
    assert cur.ref == DatasetVersionRef(SPOT, v1) and cur.ref_log_seq == 1 and not cur.pending
    assert len(log_lines(store)) == 1
    for p in (store.root / "ref_log.jsonl", ref_path(store, SPOT)):
        assert os.stat(p).st_mode & 0o777 == 0o444
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert catalog.check_catalog(conn, full=True) == []
    assert conn.execute("SELECT dataset_id, data_version, ref_log_seq, ref_file_sha256 FROM refs").fetchall() == \
        [(catalog.dataset_id(SPOT), v1, 1, cur.ref_file_sha256)]
    assert conn.execute("SELECT seq, data_version, previous_data_version, action FROM ref_log").fetchall() == \
        [(1, v1, None, "set_current")]
    conn.close()
    assert list(store.tmp.iterdir()) == []


def test_set_current_change_and_rollback(store, two_versions):
    v1, v2, _ = two_versions
    set_current(store, SPOT, v1)
    u2 = set_current(store, SPOT, v2)
    u3 = set_current(store, SPOT, v1)                              # tillbaka till äldre version
    assert (u2.previous_data_version, u2.ref_log_seq) == (v1, 2)
    assert (u3.previous_data_version, u3.ref_log_seq) == (v2, 3)
    assert store.current_ref(SPOT).ref.data_version == v1
    frame_, _, ref = store.read_current(SPOT)
    assert ref.data_version == v1 and len(frame_) == 30


def test_set_current_same_version_is_a_noop(store, two_versions):
    v1, _, _ = two_versions
    set_current(store, SPOT, v1)
    before = state_hash(store)
    upd = set_current(store, SPOT, v1)
    assert not upd.changed and upd.ref_log_seq == 1
    assert state_hash(store) == before


def test_set_current_refuses_corrupt_object_and_changes_nothing(store, two_versions):
    v1, v2, _ = two_versions
    set_current(store, SPOT, v1)
    corrupt_object(store, SPOT, v2)
    before = state_hash(store)
    with pytest.raises(vs.ObjectCorruptError):
        set_current(store, SPOT, v2)
    assert state_hash(store) == before and store.current_ref(SPOT).ref.data_version == v1


def test_set_current_refuses_missing_or_foreign_version(store, two_versions):
    _, _, p1 = two_versions
    before = state_hash(store)
    for key, dv in ((SPOT, h("missing")), (SPOT, p1)):            # p1 tillhör perp
        with pytest.raises(DatasetNotFoundError):
            set_current(store, key, dv)
    assert state_hash(store) == before


def test_set_current_never_touches_objects(store, two_versions):
    v1, v2, p1 = two_versions
    before = tree_hash(store.objects)
    set_current(store, SPOT, v1)
    set_current(store, SPOT, v2)
    set_current(store, PERP, p1)
    assert tree_hash(store.objects) == before


def test_ref_log_and_ref_file_golden(tmp_path):
    """Frysta bytes och sha256 för ref_log-rad 1 och 2 samt en ref-fil."""
    st = vs.VersionedStore.init(tmp_path / "g")
    v1, v2 = publish(st, frame(1)), publish(st, frame(2, n=40))
    set_current(st, SPOT, v1)
    set_current(st, SPOT, v2)
    l1, l2 = log_lines(st)
    assert l1 == GOLDEN_LINE_1 and hashlib.sha256(l1).hexdigest() == GOLDEN_LINE_1_SHA
    assert l2 == GOLDEN_LINE_2 and hashlib.sha256(l2).hexdigest() == GOLDEN_LINE_2_SHA
    ref = ref_path(st, SPOT).read_bytes()
    assert ref == GOLDEN_REF_FILE and hashlib.sha256(ref).hexdigest() == GOLDEN_REF_FILE_SHA


GOLDEN_LINE_1 = (
    b"{\"action\":\"set_current\",\"data_version\":\"dee36f37554c22e3ccbaf937569de7589f7c260e"
    b"ab248e885f0be31a56c2feda\",\"dataset\":{\"exchange\":\"binance\",\"instrument\":\"spot\",\"s"
    b"ymbol\":\"BTCUSDT\",\"timeframe\":\"15m\"},\"dataset_id\":\"f607d43c4d9de93d95427145ed5da4"
    b"396e73b08cf6267ef471de6d700eb8e052\",\"prev_entry_sha256\":null,\"previous_data_vers"
    b"ion\":null,\"seq\":1}"
)
GOLDEN_LINE_1_SHA = "c2b1964a5fc518e12606f6ed512d986fabb5fd3edbc4c707f90e31224a8286ec"
GOLDEN_LINE_2 = (
    b"{\"action\":\"set_current\",\"data_version\":\"0f10711873994bf49c0c5dd5b611ad88894ebda6"
    b"e4c9b9b9bf8444c363ed727c\",\"dataset\":{\"exchange\":\"binance\",\"instrument\":\"spot\",\"s"
    b"ymbol\":\"BTCUSDT\",\"timeframe\":\"15m\"},\"dataset_id\":\"f607d43c4d9de93d95427145ed5da4"
    b"396e73b08cf6267ef471de6d700eb8e052\",\"prev_entry_sha256\":\"c2b1964a5fc518e12606f6e"
    b"d512d986fabb5fd3edbc4c707f90e31224a8286ec\",\"previous_data_version\":\"dee36f37554c"
    b"22e3ccbaf937569de7589f7c260eab248e885f0be31a56c2feda\",\"seq\":2}"
)
GOLDEN_LINE_2_SHA = "302f1747051d7b7755d9784638a6591295882df408bdc4442f2dcbe4a5597bab"
GOLDEN_REF_FILE = (
    b"{\n  \"data_version\": \"0f10711873994bf49c0c5dd5b611ad88894ebda6e4c9b9b9bf8444c363e"
    b"d727c\",\n  \"dataset\": {\n    \"exchange\": \"binance\",\n    \"instrument\": \"spot\",\n    "
    b"\"symbol\": \"BTCUSDT\",\n    \"timeframe\": \"15m\"\n  },\n  \"dataset_id\": \"f607d43c4d9de9"
    b"3d95427145ed5da4396e73b08cf6267ef471de6d700eb8e052\",\n  \"format\": \"btc-quant/ref/"
    b"v1\",\n  \"ref_log_entry_sha256\": \"302f1747051d7b7755d9784638a6591295882df408bdc444"
    b"2f2dcbe4a5597bab\",\n  \"ref_log_seq\": 2\n}\n"
)
GOLDEN_REF_FILE_SHA = "7ad9d8a4331abc92964a6fc6fb157fb0e29be3867e81763500668ccdef4ed7bf"


def test_refs_and_ref_log_are_deterministic(tmp_path):
    out = []
    for name in ("a", "b"):
        st = vs.VersionedStore.init(tmp_path / name)
        v1, v2, p1 = publish(st, frame(1)), publish(st, frame(2, n=40)), publish(st, frame(3), PERP)
        set_current(st, SPOT, v1)
        set_current(st, PERP, p1)
        set_current(st, SPOT, v2)
        out.append((tree_hash(st.root / "refs"), (st.root / "ref_log.jsonl").read_bytes(), dump(st)))
    assert out[0] == out[1]


def test_ref_log_is_append_only_prefix(store, two_versions):
    v1, v2, p1 = two_versions
    snapshots = [b""]
    for key, dv in ((SPOT, v1), (PERP, p1), (SPOT, v2), (SPOT, v1)):
        set_current(store, key, dv)
        snapshots.append((store.root / "ref_log.jsonl").read_bytes())
    for old, new in itertools.pairwise(snapshots):
        assert new.startswith(old) and new.count(b"\n") == old.count(b"\n") + 1


# ================================================================== manipulation
def _three_entries(store, two_versions):
    v1, v2, p1 = two_versions
    set_current(store, SPOT, v1)          # seq 1 (ingen ref pekar hit efter seq 2)
    set_current(store, SPOT, v2)          # seq 2 <- spot-ref
    set_current(store, PERP, p1)          # seq 3 <- perp-ref


def _edit_line(i, fn):
    def apply(store):
        p = store.root / "ref_log.jsonl"
        lines = p.read_bytes().splitlines()
        lines[i] = fn(lines[i])
        rewrite(p, b"\n".join(lines) + b"\n")
    return apply


def _drop_line(i):
    def apply(store):
        p = store.root / "ref_log.jsonl"
        lines = p.read_bytes().splitlines()
        del lines[i]
        rewrite(p, b"\n".join(lines) + b"\n")
    return apply


TAMPER = {
    # Rad 1 refereras inte av någon ref: bara hashkedjan kan avslöja ändringen.
    "unreferenced_line_edited": _edit_line(0, lambda b: b.replace(b'"set_current"', b'"import_legacy"')),
    "line_not_canonical": _edit_line(1, lambda b: b.replace(b'","', b'", "', 1)),
    "line_deleted": _drop_line(0),
    "lines_reordered": lambda st: rewrite(st.root / "ref_log.jsonl", b"\n".join(
        [log_lines(st)[1], log_lines(st)[0], log_lines(st)[2]]) + b"\n"),
    "truncated_last_line": lambda st: rewrite(st.root / "ref_log.jsonl",
                                              (st.root / "ref_log.jsonl").read_bytes()[:-5]),
    "log_writable": lambda st: os.chmod(st.root / "ref_log.jsonl", 0o644),
    "ref_file_points_elsewhere": lambda st: rewrite(ref_path(st, SPOT), ref_path(st, SPOT).read_bytes()
                                                    .replace(b'"ref_log_seq": 2', b'"ref_log_seq": 1')),
    "ref_file_writable": lambda st: os.chmod(ref_path(st, SPOT), 0o644),
    "ref_file_for_wrong_dataset": lambda st: rewrite(ref_path(st, SPOT), ref_path(st, PERP).read_bytes()),
}


@pytest.mark.parametrize("name", sorted(TAMPER))
def test_tampering_is_detected_by_readers_and_writers(store, two_versions, name):
    _three_entries(store, two_versions)
    TAMPER[name](store)
    before = state_hash(store)
    with pytest.raises(vs.RefCorruptError):
        store.current_ref(SPOT)
    with pytest.raises(vs.RefCorruptError):
        set_current(store, PERP, two_versions[2])
    assert state_hash(store) == before


# ================================================================== ordning / crash / pending
def test_set_current_write_order(store, two_versions, monkeypatch):
    v1, _, _ = two_versions
    events = []
    real_rename, real_sync = vs._rename, vs.StoreWriter._sync_ref_catalog
    monkeypatch.setattr(vs, "_rename", lambda s, d: (events.append(("rename", Path(d).name)), real_rename(s, d)))
    monkeypatch.setattr(vs, "_fsync_file", lambda p, _o=vs._fsync_file: (events.append(("fsync", Path(p).name)),
                                                                        _o(p)))
    monkeypatch.setattr(vs.StoreWriter, "_sync_ref_catalog",
                        lambda self, st: (events.append(("catalog", None)), real_sync(self, st)))
    set_current(store, SPOT, v1)
    names = [e for e in events if e[0] in ("rename", "catalog")]
    assert names == [("rename", "ref_log.jsonl"), ("rename", "15m.json"), ("catalog", None)]
    i_log = events.index(("rename", "ref_log.jsonl"))
    assert events[i_log - 1][0] == "fsync" and events[i_log - 1][1].endswith("ref_log.jsonl")


def test_crash_before_log_changes_nothing(store, two_versions, monkeypatch):
    v1, v2, _ = two_versions
    set_current(store, SPOT, v1)
    before = state_hash(store)
    monkeypatch.setattr(vs.StoreWriter, "_append_ref_log",
                        lambda *a: (_ for _ in ()).throw(RuntimeError("krasch före logg")))
    with pytest.raises(RuntimeError):
        set_current(store, SPOT, v2)
    assert state_hash(store) == before and not store.current_ref(SPOT).pending


def test_crash_between_log_and_ref_blocks_until_explicit_completion(store, two_versions, monkeypatch):
    v1, v2, p1 = two_versions
    set_current(store, SPOT, v1)
    crash_between_log_and_ref(store, SPOT, v2, monkeypatch)
    cur = store.current_ref(SPOT)
    assert cur.ref.data_version == v1 and cur.pending and cur.pending_data_version == v2
    assert store.read_current(SPOT)[2].data_version == v1          # läsare ser gamla versionen
    before = state_hash(store)
    with store.writer() as w:
        for call in (lambda: w.set_current(PERP, p1), lambda: w.set_current(SPOT, v1),
                     lambda: w.create_snapshot([DatasetVersionRef(SPOT, v1)]),
                     lambda: w.snapshot_current([SPOT])):
            with pytest.raises(vs.PendingRefUpdateError):
                call()
    assert state_hash(store) == before
    with store.writer() as w:
        done = w.complete_pending_ref_updates()
    assert [(u.ref.data_version, u.previous_data_version, u.ref_log_seq) for u in done] == [(v2, v1, 2)]
    cur = store.current_ref(SPOT)
    assert cur.ref.data_version == v2 and not cur.pending
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert catalog.check_catalog(conn, full=True) == []
    assert conn.execute("SELECT data_version, ref_log_seq FROM refs").fetchall() == [(v2, 2)]
    conn.close()
    set_current(store, PERP, p1)                                   # blockeringen hävd


def test_snapshot_current_raises_while_pending(store, two_versions, monkeypatch):
    v1, v2, _ = two_versions
    set_current(store, SPOT, v1)
    crash_between_log_and_ref(store, SPOT, v2, monkeypatch)
    with store.writer() as w, pytest.raises(vs.PendingRefUpdateError):
        w.snapshot_current([SPOT])


def test_first_ever_set_current_crash_is_pending_without_ref(store, two_versions, monkeypatch):
    v1, _, _ = two_versions
    crash_between_log_and_ref(store, SPOT, v1, monkeypatch)
    cur = store.current_ref(SPOT)
    assert cur.ref is None and cur.pending and cur.pending_data_version == v1
    with pytest.raises(vs.NoCurrentRefError):
        store.read_current(SPOT)
    with store.writer() as w:
        assert len(w.complete_pending_ref_updates()) == 1
    assert store.current_ref(SPOT).ref.data_version == v1


def test_complete_refuses_when_previous_version_does_not_match_ref_file(store, two_versions, monkeypatch):
    v1, v2, _ = two_versions
    set_current(store, SPOT, v1)
    ref_after_v1 = ref_path(store, SPOT).read_bytes()
    set_current(store, SPOT, v2)
    crash_between_log_and_ref(store, SPOT, v1, monkeypatch)       # väntande: v2 -> v1
    rewrite(ref_path(store, SPOT), ref_after_v1)                  # ref-filen pekar på v1 (seq 1)
    before = state_hash(store)
    with store.writer() as w, pytest.raises(vs.RefCorruptError):
        w.complete_pending_ref_updates()
    assert state_hash(store) == before


def test_complete_refuses_when_target_fails_verification_now(store, two_versions, monkeypatch):
    v1, v2, _ = two_versions
    set_current(store, SPOT, v1)
    crash_between_log_and_ref(store, SPOT, v2, monkeypatch)
    corrupt_object(store, SPOT, v2)
    before = state_hash(store)
    with store.writer() as w, pytest.raises(vs.ObjectCorruptError):
        w.complete_pending_ref_updates()
    assert state_hash(store) == before and store.current_ref(SPOT).ref.data_version == v1


def test_complete_without_pending_is_a_noop(store, two_versions):
    set_current(store, SPOT, two_versions[0])
    before = state_hash(store)
    with store.writer() as w:
        assert w.complete_pending_ref_updates() == []
    assert state_hash(store) == before


def test_more_than_one_pending_entry_is_corruption(store, two_versions, monkeypatch):
    v1, v2, p1 = two_versions
    set_current(store, SPOT, v1)
    crash_between_log_and_ref(store, SPOT, v2, monkeypatch)
    last = log_lines(store)[-1]
    forged = compact_json_bytes({"seq": 3, "prev_entry_sha256": hashlib.sha256(last).hexdigest(),
                                 **DatasetVersionRef(PERP, p1).to_json_obj(),
                                 "previous_data_version": None, "action": "set_current"})
    p = store.root / "ref_log.jsonl"
    rewrite(p, p.read_bytes() + forged + b"\n")
    before = state_hash(store)
    with store.writer() as w, pytest.raises(vs.RefCorruptError, match="mer än en"):
        w.complete_pending_ref_updates()
    assert state_hash(store) == before


def test_crash_between_ref_and_catalog_is_synced_later(store, two_versions, monkeypatch):
    v1, v2, _ = two_versions
    set_current(store, SPOT, v1)
    real = vs.StoreWriter._sync_ref_catalog
    monkeypatch.setattr(vs.StoreWriter, "_sync_ref_catalog",
                        lambda *a: (_ for _ in ()).throw(RuntimeError("krasch före katalog")))
    with pytest.raises(RuntimeError):
        set_current(store, SPOT, v2)
    assert store.current_ref(SPOT).ref.data_version == v2          # filerna är facit
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert conn.execute("SELECT data_version FROM refs").fetchall() == [(v1,)]
    conn.close()
    monkeypatch.setattr(vs.StoreWriter, "_sync_ref_catalog", real)
    assert not set_current(store, SPOT, v2).changed                # no-op synkar katalogen
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert conn.execute("SELECT data_version, ref_log_seq FROM refs").fetchall() == [(v2, 2)]
    assert conn.execute("SELECT count(*) FROM ref_log").fetchone() == (2,)
    assert catalog.check_catalog(conn, full=True) == []
    conn.close()


# ================================================================== snapshots
def test_create_snapshot_and_identical_again_is_reused(store, two_versions):
    v1, _, p1 = two_versions
    members = [DatasetVersionRef(SPOT, v1), DatasetVersionRef(PERP, p1)]
    with store.writer() as w:
        r1 = w.create_snapshot(members)
        r2 = w.create_snapshot(list(reversed(members)))
    assert (r1.outcome, r2.outcome) == ("created", "reused")
    assert r1.snapshot == r2.snapshot and r1.snapshot.snapshot_id == DataSnapshot.of(members).snapshot_id
    p = store.root / "snapshots" / f"{r1.snapshot.snapshot_id}.json"
    assert os.stat(p).st_mode & 0o777 == 0o444 and p.read_bytes() == r1.snapshot.to_file_bytes()
    assert store.read_snapshot(r1.snapshot.snapshot_id) == r1.snapshot
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert conn.execute("SELECT count(*) FROM snapshot_members").fetchone() == (2,)
    assert catalog.check_catalog(conn, full=True) == []
    conn.close()
    assert list(store.tmp.iterdir()) == []


def test_snapshot_is_never_overwritten_even_in_a_race(store, two_versions, monkeypatch):
    v1, _, _ = two_versions
    snap = DataSnapshot.of([DatasetVersionRef(SPOT, v1)])
    target = store.root / "snapshots" / f"{snap.snapshot_id}.json"
    real = vs._fsync_file

    def racing(p):
        real(p)
        if Path(p).name.endswith(".snapshot.json"):
            target.write_bytes(b"intruder")                        # dyker upp efter kontrollen
    monkeypatch.setattr(vs, "_fsync_file", racing)
    with store.writer() as w, pytest.raises(vs.ImmutableObjectError):
        w.create_snapshot(snap)
    assert target.read_bytes() == b"intruder"


def test_tampered_existing_snapshot_is_detected(store, two_versions):
    v1, _, _ = two_versions
    with store.writer() as w:
        snap = w.create_snapshot([DatasetVersionRef(SPOT, v1)]).snapshot
    p = store.root / "snapshots" / f"{snap.snapshot_id}.json"
    rewrite(p, p.read_bytes().replace(b"\n}", b"}"))
    with pytest.raises(vs.SnapshotCorruptError):
        store.read_snapshot(snap.snapshot_id)
    with pytest.raises(vs.SnapshotCorruptError):
        store.pinned(snap.snapshot_id)
    with store.writer() as w, pytest.raises(vs.SnapshotCorruptError):
        w.create_snapshot(snap)


def test_create_snapshot_refuses_corrupt_member(store, two_versions):
    _, v2, _ = two_versions
    corrupt_object(store, SPOT, v2)
    before = state_hash(store)
    with store.writer() as w, pytest.raises(vs.ObjectCorruptError):
        w.create_snapshot([DatasetVersionRef(SPOT, v2)])
    assert state_hash(store) == before


def test_snapshot_current_pins_and_never_follows_refs(store, two_versions):
    v1, v2, p1 = two_versions
    set_current(store, SPOT, v1)
    set_current(store, PERP, p1)
    with store.writer() as w:
        snap = w.snapshot_current([SPOT, PERP]).snapshot
    set_current(store, SPOT, v2)                                   # ref flyttas efteråt
    pinned = store.pinned(snap.snapshot_id)
    assert pinned.version_of(SPOT).data_version == v1
    assert store.current_ref(SPOT).ref.data_version == v2


def test_snapshot_current_raises_for_key_without_ref(store, two_versions):
    set_current(store, SPOT, two_versions[0])
    before = state_hash(store)
    with store.writer() as w, pytest.raises(vs.NoCurrentRefError):
        w.snapshot_current([SPOT, PERP])
    assert state_hash(store) == before


def test_crash_after_link_before_catalog_is_completed_by_same_snapshot(store, two_versions, monkeypatch):
    v1, _, _ = two_versions
    real = vs.StoreWriter._register_snapshot
    monkeypatch.setattr(vs.StoreWriter, "_register_snapshot",
                        lambda *a: (_ for _ in ()).throw(RuntimeError("krasch före katalog")))
    with store.writer() as w, pytest.raises(RuntimeError):
        w.create_snapshot([DatasetVersionRef(SPOT, v1)])
    monkeypatch.setattr(vs.StoreWriter, "_register_snapshot", real)
    with store.writer() as w:
        res = w.create_snapshot([DatasetVersionRef(SPOT, v1)])
    assert res.outcome == "reused"
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert conn.execute("SELECT snapshot_id FROM snapshots").fetchall() == [(res.snapshot.snapshot_id,)]
    conn.close()


# ================================================================== PinnedStore
@pytest.fixture
def pinned_setup(store, two_versions):
    v1, v2, p1 = two_versions
    with store.writer() as w:
        snap = w.create_snapshot([DatasetVersionRef(SPOT, v1)]).snapshot
        w.set_current(SPOT, v2)                                    # aktuell ref != pinnad
        w.set_current(PERP, p1)
    return store.pinned(snap.snapshot_id), v1, v2


def test_pinned_reads_the_pinned_version_not_the_current_ref(pinned_setup):
    pinned, v1, _ = pinned_setup
    df, manifest = pinned.read(SPOT)
    assert manifest["data_version"] == v1 and len(df) == 30
    pd.testing.assert_frame_equal(df, frame(1))


def test_pinned_never_reads_outside_the_snapshot(pinned_setup):
    pinned, _, _ = pinned_setup
    for key in (PERP, SPOT1M):                                     # PERP har en aktuell ref
        with pytest.raises(vs.DatasetNotInSnapshotError):
            pinned.read(key)
        with pytest.raises(vs.DatasetNotInSnapshotError):
            pinned.version_of(key)


@pytest.mark.parametrize("method", ["publish_version", "set_current", "create_snapshot",
                                    "snapshot_current", "complete_pending_ref_updates", "writer"])
def test_pinned_store_rejects_all_writes(pinned_setup, method):
    pinned, v1, _ = pinned_setup
    before = state_hash(pinned._store)
    with pytest.raises(vs.ReadOnlyStoreError):
        getattr(pinned, method)(SPOT, v1)
    with pytest.raises(vs.ReadOnlyStoreError):
        getattr(pinned, method)()
    assert state_hash(pinned._store) == before


def test_pinned_never_opens_refs_or_ref_log(pinned_setup, monkeypatch):
    pinned, v1, _ = pinned_setup
    for name in ("current_ref", "_read_ref_log", "_read_ref_file", "_load_ref_state", "read_current"):
        monkeypatch.setattr(vs.VersionedStore, name, lambda *a, _n=name, **k: pytest.fail(f"{_n} anropad"))
    assert pinned.read(SPOT)[1]["data_version"] == v1


def test_pinned_as_of_boundaries(pinned_setup):
    pinned, _, _ = pinned_setup
    df = frame(1)
    t5 = df["open_time"].iloc[5].to_pydatetime()
    at, _ = pinned.read(SPOT, as_of=t5 + TF)
    before, _ = pinned.read(SPOT, as_of=t5 + TF - US)
    assert len(at) == 6 and len(before) == 5
    assert (before["open_time"] + TF <= pd.Timestamp(t5 + TF - US)).all()


def test_pinned_read_ignores_newer_versions(pinned_setup):
    pinned, v1, v2 = pinned_setup
    df, manifest = pinned.read(SPOT, as_of=FAR)
    assert manifest["data_version"] == v1 != v2 and len(df) == 30   # v2 har 40 rader


def test_pinned_store_requires_a_published_snapshot(store, two_versions):
    v1, _, p1 = two_versions
    unpublished = DataSnapshot.of([DatasetVersionRef(SPOT, v1), DatasetVersionRef(PERP, p1)])
    before = state_hash(store)
    with pytest.raises(vs.SnapshotNotFoundError):
        vs.PinnedStore(store, unpublished)
    assert state_hash(store) == before


def test_pinned_store_rejects_snapshot_differing_from_published_file(store, two_versions, monkeypatch):
    v1, v2, _ = two_versions
    with store.writer() as w:
        snap = w.create_snapshot([DatasetVersionRef(SPOT, v1)]).snapshot
    other = DataSnapshot.of([DatasetVersionRef(SPOT, v2)])
    monkeypatch.setattr(vs.VersionedStore, "read_snapshot", lambda self, sid: other)
    with pytest.raises(vs.SnapshotCorruptError, match="avviker"):
        vs.PinnedStore(store, snap)


def test_pinned_store_with_published_snapshot_works(store, two_versions):
    v1, _, _ = two_versions
    with store.writer() as w:
        snap = w.create_snapshot([DatasetVersionRef(SPOT, v1)]).snapshot
    direct = vs.PinnedStore(store, snap)
    assert direct.snapshot_id == snap.snapshot_id == store.pinned(snap.snapshot_id).snapshot_id
    assert direct.read(SPOT)[1]["data_version"] == v1


def test_pinned_missing_snapshot(store):
    with pytest.raises(vs.SnapshotNotFoundError):
        store.pinned(h("missing"))
    with pytest.raises(ValueError):
        store.pinned("x")


# ================================================================== läsare / lås
def test_current_ref_and_read_current_without_ref(store, two_versions):
    cur = store.current_ref(SPOT)
    assert cur.ref is None and not cur.pending
    with pytest.raises(vs.NoCurrentRefError):
        store.read_current(SPOT)


def test_read_current_applies_as_of_and_returns_version(store, two_versions):
    _, v2, _ = two_versions
    set_current(store, SPOT, v2)
    t0 = frame(2, n=40)["open_time"].iloc[0].to_pydatetime()
    df, manifest, ref = store.read_current(SPOT, as_of=t0 + 3 * TF)
    assert ref.data_version == v2 == manifest["data_version"] and len(df) == 3


def test_readers_need_no_lock(store, two_versions):
    set_current(store, SPOT, two_versions[0])
    with store.writer():
        assert store.current_ref(SPOT).ref.data_version == two_versions[0]
        assert store.read_current(SPOT)[2].data_version == two_versions[0]


def test_closed_writer_cannot_change_refs_or_snapshots(store, two_versions):
    v1, _, _ = two_versions
    with store.writer() as w:
        pass
    for call in (lambda: w.set_current(SPOT, v1), lambda: w.create_snapshot([DatasetVersionRef(SPOT, v1)]),
                 lambda: w.snapshot_current([SPOT]), lambda: w.complete_pending_ref_updates()):
        with pytest.raises(vs.StoreLockedError):
            call()


def test_concurrent_reader_sees_old_or_new_version_never_a_mix(store, two_versions):
    v1, v2, _ = two_versions
    set_current(store, SPOT, v1)
    code = ("from btc_trading_system.database import versioned_store as vs\n"
            "from btc_trading_system.data.schema import DatasetKey\n"
            f"st = vs.VersionedStore({str(store.root)!r})\n"
            "k = DatasetKey('spot', 'BTCUSDT', '15m')\n"
            "with st.writer() as w:\n"
            "    for i in range(12):\n"
            f"        w.set_current(k, {v2!r} if i % 2 == 0 else {v1!r})\n")
    env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONDONTWRITEBYTECODE": "1"}
    child = subprocess.Popen([sys.executable, "-c", code], env=env, cwd=ROOT)
    seen = set()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        done = child.poll() is not None
        df, manifest, ref = store.read_current(SPOT)
        assert ref.data_version == manifest["data_version"] and len(df) == (30 if ref.data_version == v1 else 40)
        seen.add(ref.data_version)
        if done:
            break
    assert child.wait(timeout=60) == 0
    assert seen <= {v1, v2} and store.current_ref(SPOT).ref.data_version == v1
    assert len(log_lines(store)) == 13


# ================================================================== steg 10: strikt tolkning av ref_log och ref-filer (P4-S1-05)
def _edit_last_line_obj(change):
    def edit(line: bytes) -> bytes:
        import json  # lokal import: filens importblock är oförändrat
        return compact_json_bytes(change(json.loads(line)))
    return _edit_line(2, edit)


def _ref_file_extra_key(st):
    import json
    p = ref_path(st, SPOT)
    rewrite(p, vs.canonical_json_bytes({**json.loads(p.read_bytes()), "extra": 1}))


TAMPER_STRICT = {
    "line_not_json": _edit_line(2, lambda b: b"not json"),
    "unknown_action": _edit_last_line_obj(lambda o: {**o, "action": "rename"}),
    "previous_breaks_history": _edit_last_line_obj(lambda o: {**o, "previous_data_version": h("x")}),
    "ref_file_not_json": lambda st: rewrite(ref_path(st, SPOT), b"not json"),
    "ref_file_wrong_keys": _ref_file_extra_key,
}


@pytest.mark.parametrize("name", sorted(TAMPER_STRICT))
def test_strict_ref_parsing_rejects_every_malformed_form(store, two_versions, name, monkeypatch):
    monkeypatch.setitem(TAMPER, name, TAMPER_STRICT[name])
    test_tampering_is_detected_by_readers_and_writers(store, two_versions, name)


# ================================================================== steg 10: DataSnapshot.version_of (P4-S10-h)
def test_snapshot_version_of():
    v1, p1 = h("v1"), h("p1")
    snap = DataSnapshot.of([DatasetVersionRef(PERP, p1), DatasetVersionRef(SPOT, v1)])
    assert snap.version_of(SPOT) == DatasetVersionRef(SPOT, v1)
    assert snap.version_of(PERP) == DatasetVersionRef(PERP, p1)
    assert snap.version_of(SPOT1M) is None
