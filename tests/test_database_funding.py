"""Etapp B steg 4: funding i versionslagret (SB-S4-a A2) - objekt, katalog, läsning, snapshot v2,
verify/rebuild, krasch och CLI. Hermetiskt: allt i tmp_path."""
import hashlib
import json
import os
import sqlite3
import stat
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from data_helpers import canon_frame
from funding_helpers import FKEY, month_rows, write_month

from btc_trading_system.data import funding as fd
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data_validation.errors import DatasetNotFoundError
from btc_trading_system.database import catalog, cli
from btc_trading_system.database import integrity as ig
from btc_trading_system.database import versioned_store as vs
from btc_trading_system.database.refs import (
    DatasetVersionRef,
    DataSnapshot,
    FundingVersionRef,
)

KEY = DatasetKey("perp", "BTCUSDT", "15m")
NS = pd.Timedelta(1, "ns")                      # datetime har bara µs; gränsfallen körs på pd.Timestamp
SEC15 = timedelta(seconds=15)
IS_ROOT = hasattr(os, "geteuid") and os.geteuid() == 0


def tree_hash(root: Path) -> str:
    hh = hashlib.sha256()
    for p in [root, *sorted(root.rglob("*"))]:
        st = os.lstat(p)
        hh.update(f"{p.relative_to(root)}|{st.st_mode:o}|".encode())
        if stat.S_ISREG(st.st_mode):
            hh.update(p.read_bytes())
    return hh.hexdigest()


def dump(store) -> str:
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        return catalog.dump_catalog(conn)
    finally:
        conn.close()


@pytest.fixture
def files(tmp_path):
    src = tmp_path / "src"
    return [write_month(src, 2024, 1, month_rows(2024, 1, jitter_ms=2)),
            write_month(src, 2024, 2, month_rows(2024, 2, days=3))]


@pytest.fixture
def store(tmp_path):
    return vs.VersionedStore.init(tmp_path / "store")


def publish_funding(store, files, key=FKEY):
    with store.writer() as w:
        return w.publish_funding(key, files)


def publish_ohlcv(store):
    with store.writer() as w:
        return w.publish_version(KEY, canon_frame("15m", 30))


# ================================================================== identitet
def test_funding_dataset_id_has_its_own_domain_and_is_pinned():
    did = catalog.funding_dataset_id(FKEY)
    assert did == hashlib.sha256(b"btc-quant/funding-dataset-id/v1\x00"
                                 + b'{"exchange":"binance","instrument":"perp","kind":"funding","symbol":"BTCUSDT"}'
                                 ).hexdigest()
    assert did == "7cdffd1deb4b929419f33cd51370ccf22109442900fd5d45ff9e7e008cfd3f05"
    assert did not in {catalog.dataset_id(DatasetKey(i, "BTCUSDT", tf)) for i in ("spot", "perp")
                       for tf in catalog.SCHEMA_TIMEFRAMES}
    with pytest.raises(ValueError, match="icke-kanonisk"):
        catalog.funding_dataset_key(exchange="binance", instrument="perp", symbol="btcusdt")
    with pytest.raises(ValueError):
        catalog.funding_dataset_key(exchange="binance", instrument="spot", symbol="BTCUSDT")


# ================================================================== publicering
def test_publish_creates_an_immutable_verified_object_and_catalog_rows(store, files):
    res = publish_funding(store, files)
    assert res.outcome == "published" and res.duplicates_dropped == 0
    d = store.funding_object_dir(FKEY, res.info.data_version)
    assert d == store.root / "objects/binance/perp/BTCUSDT/funding" / res.info.data_version
    assert sorted(p.name for p in d.iterdir()) == ["funding.parquet", "manifest.json", "version.json"]
    assert stat.S_IMODE(os.lstat(d).st_mode) == 0o555
    assert all(stat.S_IMODE(os.lstat(p).st_mode) == 0o444 for p in d.iterdir())
    manifest = json.loads((d / "manifest.json").read_bytes())
    assert manifest["sources"] == [{"name": f.name, "sha256": hashlib.sha256(f.read_bytes()).hexdigest(),
                                    "rows": r} for f, r in zip(files, (93, 9), strict=True)]
    assert manifest["rows"] == res.info.rows == 102 and manifest["data_version"] == res.info.data_version
    version = json.loads((d / "version.json").read_bytes())
    assert version["format"] == "btc-quant/funding-version/v1"
    assert version["dataset"] == {"exchange": "binance", "instrument": "perp", "kind": "funding",
                                  "symbol": "BTCUSDT"}
    assert store.verify_funding_version(FKEY, res.info.data_version) == res.info
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert conn.execute("SELECT * FROM funding_datasets").fetchall() == [
        (res.info.dataset_id, "binance", "perp", "BTCUSDT")]
    assert conn.execute("SELECT rows, first_calc_time_ms, last_calc_time_ms, duplicates_dropped "
                        "FROM funding_versions").fetchall() == [(102, 1704067200000, 1706976000000, 0)]
    assert [r[3:] for r in conn.execute("SELECT * FROM funding_version_sources ORDER BY source_index")] == [
        (f.name, hashlib.sha256(f.read_bytes()).hexdigest(), r) for f, r in zip(files, (93, 9), strict=True)]
    assert conn.execute("SELECT count(*) FROM datasets").fetchone()[0] == 0          # ingen OHLCV-rad
    conn.close()
    assert sorted(p.name for p in store.tmp.iterdir()) == []


def test_republish_is_reused_and_changes_nothing(store, files):
    first = publish_funding(store, files)
    before = tree_hash(store.root / "objects"), dump(store)
    again = publish_funding(store, list(reversed(files)))
    assert again.outcome == "reused" and again.info == first.info
    assert (tree_hash(store.root / "objects"), dump(store)) == before


def test_same_content_from_other_files_is_a_provenance_conflict(store, files, tmp_path):
    publish_funding(store, files)
    rows = month_rows(2024, 1, jitter_ms=2)
    other = write_month(tmp_path / "other", 2024, 1, [(t, iv, "1E-5" if r == "0.00001000" else r)
                                                       for t, iv, r in rows])
    assert other.read_bytes() != files[0].read_bytes()           # samma innehåll, andra bytes
    before = tree_hash(store.root)
    with pytest.raises(vs.ProvenanceConflictError, match="andra källor"):
        publish_funding(store, [other, files[1]])
    assert tree_hash(store.root) == before


def test_invalid_input_writes_nothing(store, tmp_path):
    bad = write_month(tmp_path / "bad", 2024, 1, [("1704067200000", "8", "1E-13")])
    before = tree_hash(store.root)
    with pytest.raises(fd.FundingFormatError):
        publish_funding(store, [bad])
    assert tree_hash(store.root) == before


def test_publish_requires_the_writer_lock(store, files):
    with store.writer() as w:
        pass
    with pytest.raises(vs.StoreLockedError):
        w.publish_funding(FKEY, files)


# ================================================================== verifiering av objektet
def _object(store, files):
    res = publish_funding(store, files)
    return res.info.data_version, store.funding_object_dir(FKEY, res.info.data_version)


@pytest.mark.skipif(IS_ROOT, reason="root ignorerar 0444")
@pytest.mark.parametrize("damage,match", [
    (lambda d: (d / "funding.parquet").write_bytes((d / "funding.parquet").read_bytes()[:-1] + b"\x00"),
     "data_file_sha256"),
    (lambda d: (d / "manifest.json").write_bytes((d / "manifest.json").read_bytes().replace(b'"rows": 102',
                                                                                             b'"rows": 101')),
     "manifest_sha256"),
    (lambda d: (d / "version.json").write_bytes((d / "version.json").read_bytes() + b" "), "version.json"),
])
def test_tampered_object_is_corrupt(store, files, damage, match):
    dv, d = _object(store, files)
    os.chmod(d, 0o755)
    for p in d.iterdir():
        os.chmod(p, 0o644)
    damage(d)
    for p in d.iterdir():
        os.chmod(p, 0o444)
    with pytest.raises(vs.ObjectCorruptError, match=match):
        store.verify_funding_version(FKEY, dv)


def test_writable_file_or_extra_file_is_corrupt(store, files):
    dv, d = _object(store, files)
    os.chmod(d / "manifest.json", 0o644)
    with pytest.raises(vs.ObjectCorruptError, match="skrivbar"):
        store.verify_funding_version(FKEY, dv)
    os.chmod(d / "manifest.json", 0o444)
    os.chmod(d, 0o755)
    (d / "extra").write_bytes(b"x")
    with pytest.raises(vs.ObjectCorruptError, match="filer"):
        store.verify_funding_version(FKEY, dv)


def test_missing_version_is_not_found(store):
    with pytest.raises(DatasetNotFoundError):
        store.read_funding_version(FKEY, "a" * 64)
    with pytest.raises(ValueError):
        store.funding_object_dir(FKEY, "A" * 64)


# ================================================================== läsning och kausalitet (SB-S4-c)
def test_as_of_exposes_a_row_exactly_15_seconds_after_calc_time(store, files):
    dv = publish_funding(store, files).info.data_version
    full, _ = store.read_funding_version(FKEY, dv)
    t = full["calc_time"].iloc[1]                            # 2024-01-01T08:00:00.002Z
    assert t == pd.Timestamp("2024-01-01T08:00:00.002Z")
    at = t + SEC15
    n = [len(store.read_funding_version(FKEY, dv, as_of=d)[0]) for d in (t, at - NS, at, at + NS)]
    assert n == [1, 1, 2, 2]
    frame, _ = store.read_funding_version(FKEY, dv, as_of=at)
    pd.testing.assert_frame_equal(frame, full.iloc[:2].reset_index(drop=True))
    assert len(store.read_funding_version(FKEY, dv, as_of=full["calc_time"].iloc[0])[0]) == 0
    assert len(store.read_funding_version(FKEY, dv, as_of=at.to_pydatetime())[0]) == 2      # datetime går också


def test_range_is_half_open_on_calc_time_with_as_of(store, files):
    dv = publish_funding(store, files).info.data_version
    full, _ = store.read_funding_version(FKEY, dv)
    ts = list(full["calc_time"])
    got, _ = store.read_funding_range(FKEY, dv, ts[3], ts[6])
    pd.testing.assert_frame_equal(got, full.iloc[3:6].reset_index(drop=True))
    assert len(store.read_funding_range(FKEY, dv, ts[3], ts[3])[0]) == 0
    assert len(store.read_funding_range(FKEY, dv, ts[3] + NS, ts[6] + NS)[0]) == 3
    got, _ = store.read_funding_range(FKEY, dv, ts[3], ts[9], as_of=ts[5] + SEC15)
    pd.testing.assert_frame_equal(got, full.iloc[3:6].reset_index(drop=True))
    got, _ = store.read_funding_range(FKEY, dv, ts[3], ts[9], as_of=ts[5] + SEC15 - NS)
    assert len(got) == 2
    with pytest.raises(ValueError, match="end < start"):
        store.read_funding_range(FKEY, dv, ts[6], ts[3])
    with pytest.raises(TypeError):
        store.read_funding_range(FKEY, dv, "2024-01-01", ts[3])
    for bad in (datetime(2024, 1, 2), datetime(2024, 1, 2, tzinfo=timezone(timedelta(hours=2)))):  # noqa: DTZ001 - avsiktligt naiv
        with pytest.raises(ValueError, match="UTC"):
            store.read_funding_range(FKEY, dv, ts[0], ts[3], as_of=bad)
        with pytest.raises(ValueError, match="UTC"):
            store.read_funding_version(FKEY, dv, as_of=bad)


def test_reads_are_copies(store, files):
    dv = publish_funding(store, files).info.data_version
    frame, manifest = store.read_funding_version(FKEY, dv)
    frame.loc[0, "funding_rate_e12"] = 123
    manifest["rows"] = 0
    again, man2 = store.read_funding_version(FKEY, dv)
    assert again.loc[0, "funding_rate_e12"] != 123 and man2["rows"] == 102


# ================================================================== snapshot v2
def test_snapshot_v1_is_unchanged_by_the_v2_format(store):
    dv = publish_ohlcv(store).info.data_version
    snap = DataSnapshot.of([DatasetVersionRef(KEY, dv)])
    members = json.dumps([DatasetVersionRef(KEY, dv).to_json_obj()], sort_keys=True, separators=(",", ":"))
    assert snap.snapshot_id == hashlib.sha256(b"btc-quant/snapshot-id/v1\x00" + members.encode()).hexdigest()
    assert json.loads(snap.to_file_bytes())["format"] == "btc-quant/snapshot/v1"
    assert set(json.loads(snap.to_file_bytes())) == {"format", "snapshot_id", "members"}


def test_snapshot_v2_pins_funding_and_is_read_through_pinned_store(store, files):
    odv = publish_ohlcv(store).info.data_version
    fdv = publish_funding(store, files).info.data_version
    ref, fref = DatasetVersionRef(KEY, odv), FundingVersionRef(FKEY, fdv)
    with store.writer() as w:
        res = w.create_snapshot([ref], (fref,))
    snap = res.snapshot
    assert res.outcome == "created" and snap.funding == (fref,)
    v1 = DataSnapshot.of([ref])
    assert snap.snapshot_id != v1.snapshot_id
    body = json.dumps({"funding": [fref.to_json_obj()], "members": [ref.to_json_obj()]}, sort_keys=True,
                      separators=(",", ":"))
    assert snap.snapshot_id == hashlib.sha256(b"btc-quant/snapshot-id/v2\x00" + body.encode()).hexdigest()
    data = (store.root / "snapshots" / f"{snap.snapshot_id}.json").read_bytes()
    obj = json.loads(data)
    assert obj["format"] == "btc-quant/snapshot/v2" and obj["funding"] == [fref.to_json_obj()]
    assert store.read_snapshot(snap.snapshot_id) == snap
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    assert conn.execute("SELECT * FROM snapshot_funding_members").fetchall() == [
        (snap.snapshot_id, fref.dataset_id, fdv)]
    assert conn.execute("SELECT member_count FROM snapshots").fetchone()[0] == 1
    conn.close()
    pinned = store.pinned(snap.snapshot_id)
    assert pinned.funding == (fref,) and pinned.funding_version_of(FKEY) == fref
    frame, _ = pinned.read_funding(FKEY)
    assert len(frame) == 102
    t = frame["calc_time"].iloc[0].to_pydatetime()
    assert len(pinned.read_funding(FKEY, as_of=t + SEC15)[0]) == 1
    assert len(pinned.read_funding_range(FKEY, t, t + timedelta(hours=17))[0]) == 3
    with pytest.raises(vs.ReadOnlyStoreError):
        pinned.publish_funding(FKEY, files)
    with store.writer() as w:
        assert w.create_snapshot(snap).outcome == "reused"


def test_funding_outside_the_snapshot_is_never_read(store, files):
    odv = publish_ohlcv(store).info.data_version
    publish_funding(store, files)
    with store.writer() as w:
        snap = w.create_snapshot([DatasetVersionRef(KEY, odv)]).snapshot
    with pytest.raises(vs.DatasetNotInSnapshotError):
        store.pinned(snap.snapshot_id).read_funding(FKEY)


def test_snapshot_with_a_missing_funding_version_writes_nothing(store):
    odv = publish_ohlcv(store).info.data_version
    before = tree_hash(store.root)
    with store.writer() as w, pytest.raises(DatasetNotFoundError):
        w.create_snapshot([DatasetVersionRef(KEY, odv)], (FundingVersionRef(FKEY, "b" * 64),))
    assert tree_hash(store.root) == before


def test_snapshot_current_takes_explicit_funding(store, files, tmp_path):
    from data_helpers import make_klines, write_csv
    (tmp_path / "k").mkdir()
    p = write_csv(tmp_path / "k" / "BTCUSDT-15m-2024-01.csv", make_klines("15m", 20))
    with store.writer() as w:
        w.ingest(KEY, [p], now=datetime(2030, 1, 1, tzinfo=UTC), code_version="x", worktree_clean=True)
    fdv = publish_funding(store, files).info.data_version
    with store.writer() as w:
        snap = w.snapshot_current([KEY], (FundingVersionRef(FKEY, fdv),)).snapshot
    assert snap.funding == (FundingVersionRef(FKEY, fdv),) and len(snap.members) == 1


@pytest.mark.parametrize("edit,match", [
    (lambda o: o.update(funding=[]), "snapshotformat"),
    (lambda o: o.update(format="btc-quant/snapshot/v1"), "snapshotformat"),
    (lambda o: o.pop("funding"), "snapshotformat"),
    (lambda o: o["funding"][0]["dataset"].update(kind="15m"), "funding-versionsreferens"),
    (lambda o: o["funding"][0].update(dataset_id="c" * 64), "matchar inte"),
    (lambda o: o["funding"].append(dict(o["funding"][0])), "högst en version"),
])
def test_snapshot_v2_file_is_parsed_strictly(store, files, edit, match):
    odv = publish_ohlcv(store).info.data_version
    fdv = publish_funding(store, files).info.data_version
    snap = DataSnapshot.of([DatasetVersionRef(KEY, odv)], [FundingVersionRef(FKEY, fdv)])
    obj = json.loads(snap.to_file_bytes())
    edit(obj)
    data = (json.dumps(obj, sort_keys=True, indent=2) + "\n").encode()
    with pytest.raises(ValueError, match=match):
        DataSnapshot.from_file_bytes(data)


def test_snapshot_v2_members_are_still_required():
    with pytest.raises(ValueError, match="minst en medlem"):
        DataSnapshot((), (FundingVersionRef(FKEY, "a" * 64),))
    with pytest.raises(TypeError):
        DataSnapshot((DatasetVersionRef(KEY, "b" * 64),), [FundingVersionRef(FKEY, "a" * 64)])  # type: ignore[arg-type]


# ================================================================== verify / rebuild / krasch
def _v2_store(store, files):
    odv = publish_ohlcv(store).info.data_version
    fdv = publish_funding(store, files).info.data_version
    with store.writer() as w:
        snap = w.create_snapshot([DatasetVersionRef(KEY, odv)], (FundingVersionRef(FKEY, fdv),)).snapshot
    return odv, fdv, snap


def test_verify_covers_funding_objects_snapshots_and_tables(store, files):
    _v2_store(store, files)
    r = ig.verify_store(store.root)
    assert r.exit_status == 0 and r.findings == ()
    assert dict(r.counts)["funding_objects_valid"] == 1 and dict(r.counts)["funding_datasets"] == 1
    scope = {s.check: s.status for s in r.scope}
    for t in ("funding_datasets", "funding_versions", "funding_version_sources", "snapshot_funding_members"):
        assert scope[f"catalog.rows.{t}"] == "performed"


def test_unpinned_funding_version_is_info(store, files):
    publish_funding(store, files)
    r = ig.verify_store(store.root)
    assert r.codes == {"OBJECT_UNREFERENCED"} and r.exit_status == 0
    assert r.findings[0].subject.startswith("objects/binance/perp/BTCUSDT/funding/")


def test_verify_reports_corrupt_funding_object_and_missing_snapshot_member(store, files):
    _, fdv, _ = _v2_store(store, files)
    d = store.funding_object_dir(FKEY, fdv)
    os.chmod(d, 0o755)
    os.chmod(d / "manifest.json", 0o644)
    r = ig.verify_store(store.root)
    assert "OBJECT_CORRUPT" in r.codes and r.exit_status == 3
    os.chmod(d / "manifest.json", 0o444)
    for p in d.iterdir():
        os.chmod(p, 0o644)
        p.unlink()
    d.rmdir()
    r = ig.verify_store(store.root)
    assert "SNAPSHOT_MEMBER_MISSING" in r.codes
    assert any(f.detail.startswith("binance/perp/BTCUSDT/funding/") for f in r.by_code("SNAPSHOT_MEMBER_MISSING"))


def _raw_change(store, trigger: str, sql: str) -> None:
    """Ändring förbi katalogens skydd; triggern återställs så att schemat är oförändrat."""
    raw = sqlite3.connect(store.catalog_path, isolation_level=None)
    (ddl,) = raw.execute("SELECT sql FROM sqlite_schema WHERE name = ?", (trigger,)).fetchone()
    raw.execute("PRAGMA foreign_keys=OFF")
    raw.execute(f"DROP TRIGGER {trigger}")
    raw.execute(sql)
    raw.execute(ddl)
    raw.close()


def test_catalog_rows_for_funding_are_a_second_witness(store, files):
    _v2_store(store, files)
    _raw_change(store, "snapshot_funding_members_no_delete", "DELETE FROM snapshot_funding_members")
    r = ig.verify_store(store.root)
    assert r.codes == {"CATALOG_BEHIND"} and r.exit_status == 1
    res = ig.repair_store(store.root, apply=True)
    assert res.after is not None and res.after.exit_status == 0
    _raw_change(store, "funding_versions_no_update", "UPDATE funding_versions SET duplicates_dropped = 7")
    r = ig.verify_store(store.root)
    assert "CATALOG_CONFLICT" in r.codes and r.exit_status == 3


def test_rebuild_is_deterministic_with_funding(store, files):
    _v2_store(store, files)
    before = dump(store)
    res = ig.rebuild_catalog_store(store.root, apply=True)
    assert res.identical is True and dump(store) == before
    assert "funding_versions" in before and "snapshot_funding_members" in before


def test_crash_after_rename_leaves_an_unregistered_object_that_republish_registers(store, files, monkeypatch):
    def boom(self, info, manifest):
        raise RuntimeError("krasch före katalograden")
    monkeypatch.setattr(vs.StoreWriter, "_register_funding", boom)
    with pytest.raises(RuntimeError):
        publish_funding(store, files)
    monkeypatch.undo()
    r = ig.verify_store(store.root)
    assert "OBJECT_UNREGISTERED" in r.codes and r.exit_status == 1
    res = publish_funding(store, files)
    assert res.outcome == "reused"
    assert ig.verify_store(store.root).codes == {"OBJECT_UNREFERENCED"}


def test_crash_before_rename_leaves_only_a_tmp_leftover(store, files, monkeypatch):
    def boom(src, dst):
        raise RuntimeError("krasch före rename")
    monkeypatch.setattr(vs, "_rename", boom)
    monkeypatch.setattr(vs.shutil, "rmtree", lambda *a, **k: None)      # stagingen blir kvar
    with pytest.raises(RuntimeError):
        publish_funding(store, files)
    monkeypatch.undo()
    assert [p for p in (store.root / "objects").rglob("*") if p.is_file()] == []    # inget objekt
    r = ig.verify_store(store.root)
    assert r.codes == {"TMP_LEFTOVER"}


def test_publication_order_fsyncs_before_rename_and_registers_last(store, files, monkeypatch):
    events = []
    for name in ("_fsync_file", "_fsync_dir", "_rename"):
        orig = getattr(vs, name)

        def spy(*a, _n=name, _o=orig):
            events.append(_n)
            return _o(*a)
        monkeypatch.setattr(vs, name, spy)
    orig_reg = vs.StoreWriter._register_funding

    def reg(self, info, manifest):
        events.append("register")
        return orig_reg(self, info, manifest)
    monkeypatch.setattr(vs.StoreWriter, "_register_funding", reg)
    publish_funding(store, files)
    assert events.index("_rename") > events.index("_fsync_file")
    assert events[:4] == ["_fsync_file"] * 3 + ["_fsync_dir"]
    assert events[-1] == "register"


# ================================================================== CLI
def run(capsys, *argv):
    code = cli.main([str(a) for a in argv], clock=lambda: datetime(2030, 1, 1, tzinfo=UTC),
                    git=lambda: ("abc", True))
    out = capsys.readouterr()
    return code, json.loads(out.out)


def test_cli_ingest_funding_dry_run_and_apply(store, files, capsys):
    before = tree_hash(store.root)
    code, env = run(capsys, "ingest-funding", "--store", store.root, "--dataset", "binance/perp/BTCUSDT/funding",
                    *files)
    assert code == 0 and env["applied"] is False and env["result"]["exists"] is False
    assert tree_hash(store.root) == before
    dv = env["result"]["data_version"]
    code, env = run(capsys, "ingest-funding", "--store", store.root, "--dataset", "binance/perp/BTCUSDT/funding",
                    *files, "--apply")
    assert code == 0 and env["result"]["outcome"] == "published" and env["result"]["data_version"] == dv
    code, env = run(capsys, "funding-versions", "--store", store.root, "--dataset", "binance/perp/BTCUSDT/funding")
    assert code == 0 and [v["data_version"] for v in env["result"]["versions"]] == [dv]
    odv = publish_ohlcv(store).info.data_version
    code, env = run(capsys, "snapshot", "create", "--store", store.root, "--member", f"{KEY.relpath}@{odv}",
                    "--funding-member", f"binance/perp/BTCUSDT/funding@{dv}", "--apply")
    assert code == 0 and env["result"]["funding"][0]["data_version"] == dv
    sid = env["result"]["snapshot_id"]
    code, env = run(capsys, "snapshot", "show", "--store", store.root, "--id", sid)
    assert code == 0 and env["result"]["funding"][0]["data_version"] == dv


@pytest.mark.parametrize("dataset", ["binance/spot/BTCUSDT/funding", "binance/perp/BTCUSDT/15m",
                                     "binance/perp/btcusdt/funding", "binance/perp/BTCUSDT"])
def test_cli_refuses_non_funding_datasets(store, files, capsys, dataset):
    code, env = run(capsys, "ingest-funding", "--store", store.root, "--dataset", dataset, *files)
    assert code == 64 and env["error"]["type"] == "UsageError"
