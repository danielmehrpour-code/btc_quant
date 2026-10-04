"""PHASE 4 steg 6b: registrering av Phase 3-kvalitetsrapporter; källan är skrivskyddad."""
import dataclasses
import hashlib
import inspect
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml
from data_helpers import NOW, canon_frame, make_klines, write_csv

from btc_trading_system.data.ingest import ingest_dataset
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data.storage import DatasetStore, ParquetBackend
from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.data_validation import quality_report as qr
from btc_trading_system.data_validation.errors import DataError, DataIntegrityError
from btc_trading_system.database import catalog
from btc_trading_system.database import integrity as ig
from btc_trading_system.database import legacy as lg
from btc_trading_system.database import quality_registry as qreg
from btc_trading_system.database import versioned_store as vs

ROOT = Path(__file__).resolve().parents[1]
SPOT1 = DatasetKey("spot", "BTCUSDT", "1m")
SPOT15 = DatasetKey("spot", "BTCUSDT", "15m")
PERP1 = DatasetKey("perp", "BTCUSDT", "1m")
PERP15 = DatasetKey("perp", "BTCUSDT", "15m")
SPECS = ((SPOT1, 120, 1), (SPOT15, 90, 2), (PERP1, 110, 3), (PERP15, 80, 4))
ZERO = ["volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]
CODE = ("test", True)


def h(tag: str) -> str:
    return hashlib.sha256(tag.encode()).hexdigest()


# ================================================================== hjälp
def source_files(raw: Path, key: DatasetKey, n: int, seed: int) -> list[Path]:
    """Två överlappande källfiler (dubbletter tas bort: duplicates_dropped > 0); luckor och en
    nollvolymsbar."""
    raw.mkdir(parents=True, exist_ok=True)
    df = make_klines(key.timeframe, n, seed).drop(index=[7, 8, 30]).reset_index(drop=True)
    df.loc[12, ZERO] = 0.0
    tag = f"{key.instrument.value}-{key.timeframe}-{seed}"
    return [write_csv(raw / f"{tag}-a.csv", df.iloc[: n // 2]),
            write_csv(raw / f"{tag}-b.csv", df.iloc[n // 2 - 5:])]


def build_phase2(root: Path, specs=SPECS) -> Path:
    """Hermetiskt Phase 2-lager byggt med Phase 2:s egen ingest + DatasetStore (ParquetBackend)."""
    p2 = DatasetStore(root, ParquetBackend())
    for key, n, seed in specs:
        ingest_dataset(source_files(root.parent / f"{root.name}-raw", key, n, seed), key.instrument,
                       key.timeframe, p2, now=NOW)
    return root


def p2_read(p2root: Path, key: DatasetKey):
    frame, manifest = DatasetStore(p2root, ParquetBackend()).read(key)
    return frame, key, manifest


def write_reports(p2root: Path, out: Path, cfg=None, code=CODE, kinds=("dataset", "tf", "xinst"),
                  keys=(SPOT1, SPOT15, PERP1, PERP15)) -> list[Path]:
    """Rapporter skrivna exakt som quality_cli gör (Phase 3:s byggare + write_report)."""
    cfg = cfg or qc.load_quality_config()
    paths = []
    if "dataset" in kinds:
        for k in keys:
            paths.append(qr.write_report(qr.build_dataset_report(*p2_read(p2root, k), cfg, *code), out)[0])
    if "tf" in kinds:
        for lo, hi in ((SPOT1, SPOT15), (PERP1, PERP15)):
            paths.append(qr.write_report(qr.build_tf_report(
                *p2_read(p2root, lo), *p2_read(p2root, hi), cfg, code_version=code[0],
                worktree_clean=code[1]), out)[0])
    if "xinst" in kinds:
        for s, p in ((SPOT1, PERP1), (SPOT15, PERP15)):
            paths.append(qr.write_report(qr.build_xinst_report(
                *p2_read(p2root, s), *p2_read(p2root, p), cfg, code_version=code[0],
                worktree_clean=code[1]), out)[0])
    return paths


@dataclasses.dataclass
class Env:
    tmp: Path
    p2: Path
    store: vs.VersionedStore
    out: Path


@pytest.fixture
def env(tmp_path):
    p2 = build_phase2(tmp_path / "p2")
    store = vs.VersionedStore.init(tmp_path / "store")
    assert lg.import_legacy(store, p2, apply=True).ok
    out = tmp_path / "reports"
    write_reports(p2, out)
    return Env(tmp_path, p2, store, out)


def run(store, source, **kw) -> qreg.QualityRegistrationReport:
    return qreg.register_quality_reports(store, source, **kw)


def apply(store, source) -> qreg.QualityRegistrationReport:
    return qreg.register_quality_reports(store, source, apply=True)


def by_name(report) -> dict[str, qreg.ReportRegistrationResult]:
    return {r.name: r for r in report.reports}


def names(out: Path, prefix: str = "") -> list[str]:
    return sorted(p.name for p in out.iterdir() if p.name.endswith(".quality.json") and p.name.startswith(prefix))


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


def quality_rows(store) -> tuple[set, set]:
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        return ({tuple(r) for r in conn.execute(f"SELECT {', '.join(qreg.QR_COLUMNS)} FROM quality_reports")},
                {tuple(r) for r in conn.execute(
                    f"SELECT {', '.join(qreg.QRD_COLUMNS)} FROM quality_report_datasets")})
    finally:
        conn.close()


def dump(store) -> str:
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        return catalog.dump_catalog(conn)
    finally:
        conn.close()


def stored(store) -> list[str]:
    d = store.root / qreg.QUALITY_DIR
    return sorted(p.name for p in d.iterdir()) if d.exists() else []


def rewrite(out: Path, name: str, change, *, rename: bool = True) -> str:
    """Ändrar en rapport konsekvent: kanonisk JSON, Markdown återskapad, filnamn = report_stem."""
    p = out / name
    obj = json.loads(p.read_bytes())
    change(obj)
    new = qr.report_stem(obj) + qreg.QUALITY_SUFFIX if rename else name
    old_md = out / md_of(name)
    try:
        md = qr.report_to_markdown(obj).encode()
    except (KeyError, TypeError):                       # avsiktligt trasig rapport: .md behålls
        md = old_md.read_bytes()
    p.unlink()
    old_md.unlink()
    (out / new).write_bytes(qr.report_to_json(obj))
    (out / md_of(new)).write_bytes(md)
    return new


def md_of(name: str) -> str:
    return name[:-len(qreg.QUALITY_SUFFIX)] + qreg.MARKDOWN_SUFFIX


def crash_child(store, out: Path, body: str) -> None:
    """Registreringen i en underprocess som dör hårt (os._exit) vid den injicerade punkten."""
    code = ("import os, sys\n"
            "from btc_trading_system.database import versioned_store as vs, quality_registry as qreg\n"
            f"st = vs.VersionedStore({str(store.root)!r})\n"
            "def die(*a, **k):\n    os._exit(77)\n"
            + body + f"\nqreg.register_quality_reports(st, {str(out)!r}, apply=True)\nos._exit(0)\n")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "tests")]),
           "PYTHONDONTWRITEBYTECODE": "1"}
    r = subprocess.run([sys.executable, "-c", code], env=env, cwd=ROOT, capture_output=True, text=True,
                       timeout=300, check=False)
    assert r.returncode == 77, r.stderr


# ================================================================== API
def test_public_api_is_pinned():
    assert str(inspect.signature(qreg.register_quality_reports)) == (
        "(store: 'vs.VersionedStore', source_root: 'str | Path', *, apply: 'bool' = False) "
        "-> 'QualityRegistrationReport'")
    assert str(inspect.signature(qreg.parse_report_bytes)) == (
        "(data: 'bytes', *, name: 'str | None' = None) -> 'RegisteredReport'")
    assert str(inspect.signature(qreg.read_registered_reports)) == (
        "(store: 'vs.VersionedStore') -> 'tuple[RegisteredReport, ...]'")
    assert (qreg.QUALITY_DIR, qreg.QUALITY_SUFFIX, qreg.MARKDOWN_SUFFIX, qreg.REGISTER_REPORT_FORMAT) == (
        "quality", ".quality.json", ".quality.md", "btc-quant/quality-register-report/v1")
    assert qreg.FAILURE_CODES == ("SOURCE_UNREADABLE", "REPORT_INVALID", "MARKDOWN_MISMATCH",
                                  "CONFIG_MISMATCH", "TARGET_MISSING", "BINDING_MISMATCH",
                                  "NOT_REPRODUCIBLE")
    assert qreg.LOCKED_CONFIG_SHA256 == "d1350cde29f02294bb327176fd9ba2c4df4ac5187f72ff54f6c674588319b9a0"
    assert qc.load_quality_config().canonical_sha256 == qreg.LOCKED_CONFIG_SHA256       # P3-01
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(catalog.MIGRATIONS[0].sql)
        for table, cols in (("quality_reports", qreg.QR_COLUMNS), ("quality_report_datasets", qreg.QRD_COLUMNS)):
            assert tuple(r[1] for r in conn.execute(f"PRAGMA table_info({table})")) == cols
    finally:
        conn.close()
    assert [f.name for f in dataclasses.fields(qreg.RegisteredReport)] == [
        "report_sha256", "report_type", "overall", "schema_version", "config_version", "config_sha256",
        "code_version", "worktree_clean", "datasets", "bindings"]


def test_error_classes():
    assert issubclass(qreg.QualityRegistrationError, vs.StoreError)
    assert issubclass(qreg.QualityReportError, DataError) and issubclass(qreg.QualityReportError, ValueError)
    assert issubclass(qreg.QualityCorruptError, DataIntegrityError)


# ================================================================== dry-run och registrering
def test_dry_run_is_the_default_and_writes_nothing(env):
    before, src_before = tree_hash(env.store.root, mtime=True), tree_hash(env.out, mtime=True)
    r = run(env.store, env.out)
    assert r.applied is False and r.ok and r.failure is None
    assert [x.name for x in r.reports] == names(env.out)
    assert {(x.status, x.file, x.catalog) for x in r.reports} == {("planned", "stored", "inserted")}
    assert tree_hash(env.store.root, mtime=True) == before
    assert tree_hash(env.out, mtime=True) == src_before
    assert r.source_tree_sha256_before == r.source_tree_sha256_after == lg.source_tree_sha256(env.out)


def test_dry_run_predicts_exactly_what_apply_does(env):
    plan = run(env.store, env.out)
    done = apply(env.store, env.out)
    assert [dataclasses.replace(x, status="x") for x in plan.reports] == [
        dataclasses.replace(x, status="x") for x in done.reports]
    again = run(env.store, env.out)
    assert {(x.status, x.file, x.catalog) for x in again.reports} == {("planned", "existing", "existing")}


def test_apply_registers_every_report_byte_for_byte(env):
    keep = {n: tree_hash(env.store.root / n) for n in ("objects", "refs", "imports")}
    log = (env.store.root / "ref_log.jsonl").read_bytes()
    r = apply(env.store, env.out)
    assert r.applied and r.ok and len(r.reports) == 8
    want_reports, want_datasets = set(), set()
    for x in r.reports:
        data = (env.out / x.name).read_bytes()
        assert x.status == "registered" and x.report_sha256 == hashlib.sha256(data).hexdigest()
        p = env.store.root / "quality" / f"{x.report_sha256}.quality.json"
        assert p.read_bytes() == data and stat.S_IMODE(os.lstat(p).st_mode) == 0o444
        rep = qreg.parse_report_bytes(data, name=x.name)
        row, rows = rep.catalog_rows()
        want_reports.add(row)
        want_datasets |= set(rows)
        obj = json.loads(data)
        assert row == (x.report_sha256, obj["report_type"], obj["overall"], 1, 1,
                       qreg.LOCKED_CONFIG_SHA256, "test", 1, len(obj["datasets"]))
        assert x.datasets == tuple((DatasetKey(d["instrument"], d["symbol"], d["timeframe"]).relpath,
                                    d["data_version"]) for d in obj["datasets"])
    assert quality_rows(env.store) == (want_reports, want_datasets)
    assert {rep.report_sha256 for rep in qreg.read_registered_reports(env.store)} == {
        x.report_sha256 for x in r.reports}
    assert {n: tree_hash(env.store.root / n) for n in ("objects", "refs", "imports")} == keep
    assert (env.store.root / "ref_log.jsonl").read_bytes() == log
    v = ig.verify_store(env.store.root)
    assert v.exit_status == 0 and dict(v.counts)["quality_reports"] == 8, v.findings


def test_reregistration_is_idempotent_byte_for_byte(env):
    apply(env.store, env.out)
    before = tree_hash(env.store.root, mtime=True)
    r = apply(env.store, env.out)
    assert r.ok and {(x.file, x.catalog) for x in r.reports} == {("existing", "existing")}
    assert tree_hash(env.store.root, mtime=True) == before


def test_registration_is_deterministic(tmp_path, env):
    first = apply(env.store, env.out)
    twin = vs.VersionedStore.init(tmp_path / "deeper" / "twin")
    assert lg.import_legacy(twin, env.p2, apply=True).ok
    assert apply(twin, env.out).to_json_bytes() == first.to_json_bytes()
    assert tree_hash(twin.root / "quality") == tree_hash(env.store.root / "quality")
    assert dump(twin) == dump(env.store)


def test_read_only_source_is_registered_and_untouched(env):
    for p in env.out.iterdir():
        os.chmod(p, 0o444)
    os.chmod(env.out, 0o555)
    try:
        before = tree_hash(env.out, mtime=True)
        assert apply(env.store, env.out).ok
        assert tree_hash(env.out, mtime=True) == before
    finally:
        os.chmod(env.out, 0o755)


def test_source_changed_during_registration_fails(env, monkeypatch):
    real = qreg._Planner.plan

    def touching(self, sr):
        p = real(self, sr)
        (env.out / "touched.txt").write_text("x")
        return p
    monkeypatch.setattr(qreg._Planner, "plan", touching)
    r = apply(env.store, env.out)
    assert r.failure == "SOURCE_CHANGED" and not r.ok
    assert r.source_tree_sha256_before != r.source_tree_sha256_after


def test_source_without_reports_fails_and_writes_nothing(env):
    empty, only_other, only_md = env.tmp / "empty", env.tmp / "only-other", env.tmp / "only-md"
    empty.mkdir()
    (only_other / "run1").mkdir(parents=True)
    (only_other / "README.txt").write_text("x")
    only_md.mkdir()
    shutil.copy(env.out / md_of(names(env.out)[0]), only_md)
    before = tree_hash(env.store.root, mtime=True)
    for source, unrecognized in ((empty, ()), (only_other, ("README.txt", "run1")),
                                 (only_md, (md_of(names(env.out)[0]),))):
        for mode in (False, True):
            r = qreg.register_quality_reports(env.store, source, apply=mode)
            assert (r.applied, r.reports, r.unrecognized) == (mode, (), unrecognized)
            assert r.failure == "SOURCE_EMPTY" and not r.ok
            assert json.loads(r.to_json_bytes())["failure"] == "SOURCE_EMPTY"
    assert tree_hash(env.store.root, mtime=True) == before


def test_source_changed_wins_over_source_empty(env, monkeypatch):
    empty = env.tmp / "empty"
    empty.mkdir()
    real = qreg._discover

    def touching(s):
        found = real(s)
        (empty / "touched.txt").write_text("x")
        return found
    monkeypatch.setattr(qreg, "_discover", touching)
    r = apply(env.store, empty)
    assert r.failure == "SOURCE_CHANGED" and not r.ok and r.reports == ()


# ================================================================== fel per rapport
def _first(env, prefix="dataset_spot_BTCUSDT_15m") -> str:
    return names(env.out, prefix)[0]


def _raw(data: bytes):
    def damage(env):
        name = _first(env)
        (env.out / name).write_bytes(data)
        return name
    return damage


def _edit(change, prefix="dataset_spot_BTCUSDT_15m", rename=True):
    def damage(env):
        return rewrite(env.out, _first(env, prefix), change, rename=rename)
    return damage


def _set_overall(obj):
    obj["overall"] = "PASS" if obj["overall"] != "PASS" else "FAIL"


def _non_canonical(env):
    name = _first(env)
    p = env.out / name
    p.write_bytes(json.dumps(json.loads(p.read_bytes()), sort_keys=True, indent=4).encode() + b"\n")
    return name


def _renamed(env):
    name = _first(env)
    os.replace(env.out / name, env.out / "renamed.quality.json")
    os.replace(env.out / md_of(name), env.out / "renamed.quality.md")
    return "renamed.quality.json"


def _symlinked_json(env):
    name = _first(env)
    real = env.tmp / "outside.json"
    os.replace(env.out / name, real)
    (env.out / name).symlink_to(real)
    return name


def _symlinked_md(env):
    name = _first(env)
    real = env.tmp / "outside.md"
    os.replace(env.out / md_of(name), real)
    (env.out / md_of(name)).symlink_to(real)
    return name


def _md_missing(env):
    name = _first(env)
    (env.out / md_of(name)).unlink()
    return name


def _md_edited(env):
    name = _first(env)
    with (env.out / md_of(name)).open("ab") as f:
        f.write(b"\nredigerad\n")
    return name


def _other_config(env):
    """Rapport byggd med en annan giltig konfiguration (annan canonical_sha256)."""
    data = yaml.safe_load(qc.DEFAULT_CONFIG_PATH.read_bytes())
    data["anomalies"]["return_z_threshold"] = 11.0
    p = env.tmp / "other.yaml"
    p.write_text(yaml.safe_dump(data))
    cfg = qc.load_quality_config(p)
    assert cfg.canonical_sha256 != qreg.LOCKED_CONFIG_SHA256
    name = _first(env)
    path = write_reports(env.p2, env.out, cfg=cfg, kinds=("dataset",), keys=(SPOT15,))[0]
    assert path.name == name
    return name


def _target_missing(env):
    """Rapport för en data_version som inte finns i lagret."""
    other = build_phase2(env.tmp / "other-p2", specs=((SPOT15, 70, 9),))
    path = write_reports(other, env.out, kinds=("dataset",), keys=(SPOT15,))[0]
    return path.name


def _rows(obj):
    obj["datasets"][0]["rows"] += 1


def _finding_edited(obj):
    c = next(c for c in obj["checks"] if c["findings"])
    c["findings"][0]["message"] += " (redigerad)"


def _coverage(obj):
    obj["coverage"]["zero_volume_bars"] += 1


def _duplicates(obj):
    obj["coverage"]["duplicates_dropped"] = 999


def _config_version(obj):
    obj["config"]["version"] = 2


REPORT_DAMAGE = {
    "not_utf8": (_raw(b"\xff\xfe{}"), "REPORT_INVALID", "UTF-8"),
    "not_json": (_raw(b"{"), "REPORT_INVALID", "JSON"),
    "not_object": (_raw(b"[]\n"), "REPORT_INVALID", "JSON-objekt"),
    "unknown_key": (_edit(lambda o: o.update(note="x")), "REPORT_INVALID", "okända ['note']"),
    "missing_key": (_edit(lambda o: o.pop("notes")), "REPORT_INVALID", "saknade ['notes']"),
    "schema_version": (_edit(lambda o: o.update(schema_version=2), rename=False), "REPORT_INVALID",
                       "schema_version"),
    "report_type": (_edit(lambda o: o.update(report_type="xinst"), rename=False), "REPORT_INVALID",
                    "report_type"),
    "dataset_count": (_edit(lambda o: o["datasets"].pop(), prefix="tf_spot", rename=False),
                      "REPORT_INVALID", "kräver 2 dataset"),
    "xinst_order": (_edit(lambda o: o["datasets"].reverse(), prefix="xinst_BTCUSDT_15m", rename=False),
                    "REPORT_INVALID", "(spot, perp)"),
    "data_version": (_edit(lambda o: o["datasets"][0].update(data_version="zz"), rename=False),
                     "REPORT_INVALID", "datasetpost"),
    "dataset_key": (_edit(lambda o: o["datasets"][0].update(symbol="btcusdt")), "REPORT_INVALID",
                    "datasetpost"),
    "overall": (_edit(_set_overall), "REPORT_INVALID", "enligt checks"),
    "worktree_clean": (_edit(lambda o: o["code"].update(worktree_clean="yes")), "REPORT_INVALID",
                       "code-fält"),
    "code_version": (_edit(lambda o: o["code"].update(version="")), "REPORT_INVALID", "code-fält"),
    "non_canonical": (_non_canonical, "REPORT_INVALID", "kanoniskt"),
    "name_not_stem": (_renamed, "REPORT_INVALID", "report_stem"),
    "symlinked_json": (_symlinked_json, "SOURCE_UNREADABLE", "vanlig fil"),
    "symlinked_md": (_symlinked_md, "SOURCE_UNREADABLE", "vanlig fil"),
    "md_missing": (_md_missing, "SOURCE_UNREADABLE", "saknas"),
    "md_edited": (_md_edited, "MARKDOWN_MISMATCH", "report_to_markdown"),
    "other_config": (_other_config, "CONFIG_MISMATCH", "låst"),
    "config_version": (_edit(_config_version), "CONFIG_MISMATCH", "låst"),
    "target_missing": (_target_missing, "TARGET_MISSING", "saknas i lagret"),
    "rows": (_edit(_rows), "BINDING_MISMATCH", "objektet"),
    "finding_edited": (_edit(_finding_edited), "NOT_REPRODUCIBLE", "andra bytes"),
    "coverage": (_edit(_coverage), "NOT_REPRODUCIBLE", "andra bytes"),
    "duplicates_dropped": (_edit(_duplicates), "NOT_REPRODUCIBLE", "proveniens"),
}


@pytest.mark.parametrize("name", sorted(REPORT_DAMAGE))
def test_a_bad_report_fails_alone_and_writes_nothing(env, name):
    damage, code, needle = REPORT_DAMAGE[name]
    bad_name = damage(env)
    good = [n for n in names(env.out) if n != bad_name]
    before = lg.source_tree_sha256(env.out)
    for mode in (False, True):
        r = qreg.register_quality_reports(env.store, env.out, apply=mode)
        bad = by_name(r)[bad_name]
        assert (bad.status, bad.failure) == ("failed", code), bad
        assert needle in bad.detail, bad.detail
        assert bad.report_sha256 is None and bad.file is None and bad.catalog is None and not r.ok
        assert str(env.out) not in bad.detail and str(env.store.root) not in bad.detail
        assert sorted(x.name for x in r.reports if x.status != "failed") == good
        assert r.failure is None and r.source_tree_sha256_after == before
    assert len(stored(env.store)) == len(good) == len(quality_rows(env.store)[0])
    assert list(env.store.tmp.iterdir()) == []
    assert ig.verify_store(env.store.root).exit_status == 0


# ================================================================== reproduktion och proveniens
def test_reproduction_reads_the_same_frames_as_phase2(env):
    for key, _, _ in SPECS:
        frame, _, manifest = p2_read(env.p2, key)
        got, got_manifest = env.store.read_version(key, manifest["data_version"])
        pd.testing.assert_frame_equal(got, frame)
        assert got_manifest == manifest


def test_provenance_from_an_ingest_record(tmp_path):
    """Objektet publicerat av steg 4-ingest (manifest utan duplicates_dropped): ingest-posten bär värdet."""
    p2 = build_phase2(tmp_path / "p2", specs=((SPOT15, 90, 2),))
    store = vs.VersionedStore.init(tmp_path / "store")
    with store.writer() as w:
        out4 = w.ingest(SPOT15, source_files(tmp_path / "p2-raw", SPOT15, 90, 2), now=NOW,
                        code_version="t", worktree_clean=True)
    _, _, manifest = p2_read(p2, SPOT15)
    assert out4.record.ref.data_version == manifest["data_version"]
    assert out4.record.duplicates_dropped == manifest["duplicates_dropped"] > 0
    assert "duplicates_dropped" not in store.read_version(SPOT15, manifest["data_version"])[1]
    assert lg.read_import_records(store) == ()
    write_reports(p2, tmp_path / "out", kinds=("dataset",), keys=(SPOT15,))
    r = apply(store, tmp_path / "out")
    assert r.ok and [x.status for x in r.reports] == ["registered"]
    assert ig.verify_store(store.root).exit_status == 0


def test_provenance_from_an_import_record_only(env):
    assert env.store.ingest_history() == ()
    dvs = {rec.ref.data_version: rec.manifest["duplicates_dropped"] for rec, _ in lg.read_import_records(env.store)}
    assert all(v > 0 for v in dvs.values())
    r = apply(env.store, env.out)
    assert r.ok and len([x for x in r.reports if x.report_type == "dataset"]) == 4


def test_no_provenance_record_is_not_reproducible(tmp_path):
    """Objektet finns och verifierar, men ingen importpost eller ingest-post bär duplicates_dropped."""
    p2 = build_phase2(tmp_path / "p2", specs=((SPOT15, 90, 2),))
    store = vs.VersionedStore.init(tmp_path / "store")
    frame, _, manifest = p2_read(p2, SPOT15)
    with store.writer() as w:
        res = w.publish_version(SPOT15, frame, {"gaps": manifest["gaps"],
                                                "zero_volume_bars": manifest["zero_volume_bars"]})
    assert res.info.data_version == manifest["data_version"]
    write_reports(p2, tmp_path / "out", kinds=("dataset",), keys=(SPOT15,))
    r = apply(store, tmp_path / "out")
    [x] = r.reports
    assert (x.status, x.failure) == ("failed", "NOT_REPRODUCIBLE") and "proveniens" in x.detail
    assert stored(store) == []


def test_unregistered_target_object_is_registered_first(tmp_path):
    """tf-rapport (ingen proveniens krävs) mot två objekt varav ett saknar katalograd."""
    p2 = build_phase2(tmp_path / "p2", specs=((SPOT1, 120, 1), (SPOT15, 90, 2)))
    store = vs.VersionedStore.init(tmp_path / "store")
    with store.writer() as w:
        w.publish_version(SPOT15, p2_read(p2, SPOT15)[0])
    with pytest.MonkeyPatch.context() as m:
        m.setattr(vs.StoreWriter, "_register", lambda self, info, manifest: None)
        with store.writer() as w:
            w.publish_version(SPOT1, p2_read(p2, SPOT1)[0])
    assert "OBJECT_UNREGISTERED" in ig.verify_store(store.root).codes
    qr.write_report(qr.build_tf_report(*p2_read(p2, SPOT1), *p2_read(p2, SPOT15), qc.load_quality_config(),
                                       code_version="test", worktree_clean=True), tmp_path / "out")
    r = apply(store, tmp_path / "out")
    assert r.ok and [x.status for x in r.reports] == ["registered"]
    v = ig.verify_store(store.root)
    assert v.exit_status == 0 and "OBJECT_UNREFERENCED" not in v.codes, v.findings


# ================================================================== upptäckt och rötter
def test_unrecognized_entries_are_reported_and_never_followed(env):
    (env.out / "README.txt").write_text("x")
    (env.out / "run1").mkdir()
    (env.out / "run1" / names(env.out)[0]).write_text("x")
    (env.out / "x.quality.json.tmp").write_text("x")
    (env.out / "lone.quality.md").write_text("x")
    outside = env.tmp / "outside"
    outside.mkdir()
    (env.out / "linkdir").symlink_to(outside)
    r = apply(env.store, env.out)
    assert r.unrecognized == ("README.txt", "linkdir", "lone.quality.md", "run1", "x.quality.json.tmp")
    assert not r.ok and len([x for x in r.reports if x.status == "registered"]) == 8


def _store_inside_source(env):
    return vs.VersionedStore.init(env.out / "nested-store"), env.out


def _source_inside_store(env):
    inner = env.store.root / "reports"
    shutil.copytree(env.out, inner)
    return env.store, inner


ROOTS = {
    "same": lambda e: (e.store, e.store.root),
    "store_inside_source": _store_inside_source,
    "source_inside_store": _source_inside_store,
    "source_is_a_store": lambda e: (e.store, vs.VersionedStore.init(e.tmp / "other").root),
    "source_is_a_symlink": lambda e: (e.store, (e.tmp / "link").symlink_to(e.out) or e.tmp / "link"),
    "source_missing": lambda e: (e.store, e.tmp / "missing"),
    "source_is_a_file": lambda e: (e.store, e.out / names(e.out)[0]),
}


@pytest.mark.parametrize("name", sorted(ROOTS))
def test_invalid_roots_are_refused_before_any_write(env, name):
    target, source = ROOTS[name](env)
    before = tree_hash(target.root, mtime=True)
    for mode in (False, True):
        with pytest.raises(qreg.QualityRegistrationError):
            qreg.register_quality_reports(target, source, apply=mode)
    assert tree_hash(target.root, mtime=True) == before


def test_unlocked_repo_config_refuses_the_run(env, monkeypatch):
    real = qc.load_quality_config()
    monkeypatch.setattr(qc, "load_quality_config",
                        lambda path=None: dataclasses.replace(real, canonical_sha256=h("other")))
    before = tree_hash(env.store.root, mtime=True)
    for mode in (False, True):
        with pytest.raises(qreg.QualityRegistrationError):
            qreg.register_quality_reports(env.store, env.out, apply=mode)
    assert tree_hash(env.store.root, mtime=True) == before


# ================================================================== lås och väntande tillstånd
def test_second_writer_is_refused_at_once(env):
    before = tree_hash(env.store.root)
    with env.store.writer():
        for mode in (False, True):
            with pytest.raises(vs.StoreLockedError):
                qreg.register_quality_reports(env.store, env.out, apply=mode)
    assert tree_hash(env.store.root) == before


def _boom(*a, **k):
    raise RuntimeError("injicerad krasch")


def test_pending_ref_update_refuses_the_run(env, monkeypatch):
    with env.store.writer() as w:
        dv = w.publish_version(SPOT15, canon_frame("15m", 40, seed=7)).info.data_version
    with monkeypatch.context() as m:
        m.setattr(vs.StoreWriter, "_write_ref_file", _boom)
        with pytest.raises(RuntimeError), env.store.writer() as w:
            w.set_current(SPOT15, dv)
    before = tree_hash(env.store.root)
    for mode in (False, True):
        with pytest.raises(vs.PendingRefUpdateError):
            qreg.register_quality_reports(env.store, env.out, apply=mode)
    assert tree_hash(env.store.root) == before


def test_pending_ingest_refuses_the_run(env, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(vs.StoreWriter, "_append_ref_log", _boom)
        with pytest.raises(RuntimeError), env.store.writer() as w:
            w.ingest(SPOT15, source_files(env.tmp / "raw4", SPOT15, 200, 9), now=NOW,
                     code_version="t", worktree_clean=True)
    before = tree_hash(env.store.root)
    for mode in (False, True):
        with pytest.raises(vs.PendingIngestError):
            qreg.register_quality_reports(env.store, env.out, apply=mode)
    assert tree_hash(env.store.root) == before


# ================================================================== mållagret
def _corrupt(store, key, dv) -> None:
    """Samma storlek, samma logiska innehåll, andra bytes: bara filhashen avslöjar det."""
    d = store.object_dir(key, dv)
    os.chmod(d, 0o755)
    p = d / "bars.parquet"
    os.chmod(p, 0o644)
    data = p.read_bytes()
    i = data.find(b"parquet-cpp-arrow")
    p.write_bytes(data[:i] + b"Parquet" + data[i + 7:])
    os.chmod(p, 0o444)
    os.chmod(d, 0o555)


def test_unverifiable_target_object_raises_before_anything_is_written(env):
    """Rapporterna före (i namnordning) är giltiga; ingenting skrivs ändå (D-S6b-g)."""
    dv = p2_read(env.p2, SPOT1)[2]["data_version"]
    first = sorted(names(env.out))
    assert first.index(names(env.out, "dataset_spot_BTCUSDT_1m")[0]) == 3
    _corrupt(env.store, SPOT1, dv)
    before = tree_hash(env.store.root, mtime=True)
    for mode in (False, True):
        with pytest.raises(vs.ObjectCorruptError):
            qreg.register_quality_reports(env.store, env.out, apply=mode)
    assert tree_hash(env.store.root, mtime=True) == before
    assert stored(env.store) == []


def test_tampered_stored_report_is_refused_and_never_overwritten(env):
    r = apply(env.store, env.out)
    p = env.store.root / "quality" / f"{r.reports[0].report_sha256}.quality.json"
    os.chmod(p, 0o644)
    p.write_bytes(p.read_bytes() + b" ")
    os.chmod(p, 0o444)
    tampered = p.read_bytes()
    for mode in (False, True):
        with pytest.raises(qreg.QualityCorruptError):
            qreg.register_quality_reports(env.store, env.out, apply=mode)
    assert p.read_bytes() == tampered


def test_stored_report_is_never_overwritten_even_in_a_race(env, monkeypatch):
    real = qreg._load_report
    hit = []

    def racing(path, rep, obj):
        real(path, rep, obj)
        if path.parent == env.store.tmp and not hit:
            target = env.store.root / "quality" / f"{rep.report_sha256}.quality.json"
            target.write_bytes(b"race")
            hit.append(target)
    monkeypatch.setattr(qreg, "_load_report", racing)
    with pytest.raises(vs.ImmutableObjectError):
        apply(env.store, env.out)
    assert hit[0].read_bytes() == b"race"
    assert list(env.store.tmp.iterdir()) == []


def test_catalog_rows_that_differ_from_the_file_are_refused(env):
    """En befintlig kvalitetsrad som inte är exakt den härledda: fel i mållagret, ingenting skrivs."""
    name = names(env.out)[0]
    rep = qreg.parse_report_bytes((env.out / name).read_bytes(), name=name)
    row = list(rep.catalog_rows()[0])
    row[2] = "PASS" if row[2] != "PASS" else "WARN"
    conn = sqlite3.connect(env.store.catalog_path)
    try:
        conn.execute(f"INSERT INTO quality_reports ({', '.join(qreg.QR_COLUMNS)}) VALUES "
                     f"({', '.join('?' * len(row))})", row)
        conn.commit()
    finally:
        conn.close()
    before = tree_hash(env.store.root, mtime=True)
    for mode in (False, True):
        with pytest.raises(catalog.CatalogCorruptError):
            qreg.register_quality_reports(env.store, env.out, apply=mode)
    assert tree_hash(env.store.root, mtime=True) == before


def test_corrupt_provenance_records_raise_before_anything_is_written(env):
    rec = lg.read_import_records(env.store)[0][0]
    os.chmod(env.store.root / "imports" / f"{rec.import_id}.json", 0o644)
    before = tree_hash(env.store.root, mtime=True)
    for mode in (False, True):
        with pytest.raises(lg.ImportCorruptError):
            qreg.register_quality_reports(env.store, env.out, apply=mode)
    assert tree_hash(env.store.root, mtime=True) == before


def _copy_read_only(p: Path, dst: Path) -> None:
    dst.write_bytes(p.read_bytes())
    os.chmod(dst, 0o444)


STORED_DAMAGE = {
    "writable": (lambda d, p: os.chmod(p, 0o644), "skrivskyddad"),
    "foreign_file": (lambda d, p: (d / "notes.txt").write_text("x"), "okänd fil"),
    "name_not_sha": (lambda d, p: _copy_read_only(p, d / f"{h('x')}.quality.json"), "filnamnet"),
    "symlink": (lambda d, p: (d / f"{h('y')}.quality.json").symlink_to(p), "skrivskyddad"),
}


@pytest.mark.parametrize("name", sorted(STORED_DAMAGE))
def test_stored_reports_are_read_strictly(env, name):
    r = apply(env.store, env.out)
    d = env.store.root / "quality"
    damage, needle = STORED_DAMAGE[name]
    damage(d, d / f"{r.reports[0].report_sha256}.quality.json")
    with pytest.raises(qreg.QualityCorruptError, match=needle):
        qreg.read_registered_reports(env.store)


# ================================================================== krasch
CRASHES = {                   # punkt -> (injektion, förväntade icke-INFO-fynd efter kraschen)
    "2_staged": ("vs._link = die\n", {"TMP_LEFTOVER"}),
    "3_linked": (("real = vs._link\n"
                  "def l(a, b):\n    real(a, b)\n    os._exit(77)\n"
                  "vs._link = l\n"), {"TMP_LEFTOVER", "CATALOG_BEHIND"}),
    "4_unlinked": ("qreg._insert_rows = die\n", {"CATALOG_BEHIND"}),
}


@pytest.mark.parametrize("point", sorted(CRASHES))
def test_crash_at_every_write_then_recovery_equals_an_uninterrupted_run(tmp_path, env, point):
    body, expected = CRASHES[point]
    crash_child(env.store, env.out, body)
    r = ig.verify_store(env.store.root)
    assert {f.code for f in r.findings if f.severity != "INFO"} == expected, r.findings
    if "TMP_LEFTOVER" in expected:
        ig.repair_store(env.store.root, apply=True)
    again = apply(env.store, env.out)
    assert again.ok
    twin = vs.VersionedStore.init(tmp_path / "twin")
    assert lg.import_legacy(twin, env.p2, apply=True).ok
    assert apply(twin, env.out).ok
    assert tree_hash(env.store.root / "quality") == tree_hash(twin.root / "quality")
    assert quality_rows(env.store) == quality_rows(twin)
    assert list(env.store.tmp.iterdir()) == []
    assert ig.verify_store(env.store.root).exit_status == 0


def test_report_for_a_non_current_version_registers_and_refs_are_untouched(env):
    with env.store.writer() as w:
        dv = w.publish_version(SPOT15, canon_frame("15m", 40, seed=7)).info.data_version
        w.set_current(SPOT15, dv)
    refs = tree_hash(env.store.root / "refs")
    log = (env.store.root / "ref_log.jsonl").read_bytes()
    assert apply(env.store, env.out).ok
    assert tree_hash(env.store.root / "refs") == refs
    assert (env.store.root / "ref_log.jsonl").read_bytes() == log
