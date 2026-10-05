"""PHASE 4 steg 8: datastore-CLI, ett tunt lager över biblioteket (python -m btc_trading_system.database.cli)."""
import argparse
import ast
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from data_helpers import NOW, make_klines, write_csv

from btc_trading_system.data import quality_cli
from btc_trading_system.data.ingest import ingest_dataset
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data.storage import DatasetStore, ParquetBackend
from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.data_validation import quality_report as qr
from btc_trading_system.database import catalog, cli
from btc_trading_system.database import integrity as ig
from btc_trading_system.database import legacy as lg
from btc_trading_system.database import quality_registry as qreg
from btc_trading_system.database import versioned_store as vs

ROOT = Path(__file__).resolve().parents[1]
SPOT = DatasetKey("spot", "BTCUSDT", "15m")
PERP = DatasetKey("perp", "BTCUSDT", "15m")
SPOT_D = "binance/spot/BTCUSDT/15m"
PERP_D = "binance/perp/BTCUSDT/15m"
T = datetime(2031, 5, 6, 7, 8, 9, 123456, tzinfo=UTC)
GIT = ("c0de" * 10, True)
FAKE_HEAD = "1" * 40
MUTATING = ("init", "import-legacy", "ingest", "set-current", "snapshot create", "register-report",
            "repair", "rebuild", "complete-pending")


# ================================================================== hjälp
def _build_template(base: Path) -> None:
    """Phase 2-lager (spot + perp 15m) -> importerat versionslager (refs satta), Phase 3-rapporter,
    indatafiler för ingest. Allt hermetiskt under base."""
    raw = base / "p2raw"
    raw.mkdir()
    p2 = DatasetStore(base / "p2", ParquetBackend())
    for key, n, seed in ((SPOT, 80, 1), (PERP, 70, 2)):
        f = write_csv(raw / f"{key.instrument.value}.csv", make_klines("15m", n, seed))
        ingest_dataset([f], key.instrument, key.timeframe, p2, now=NOW)
    store = vs.VersionedStore.init(base / "store")
    assert lg.import_legacy(store, base / "p2", apply=True).ok
    cfg = qc.load_quality_config()
    for key in (SPOT, PERP):
        frame, manifest = p2.read(key)
        qr.write_report(qr.build_dataset_report(frame, key, manifest, cfg, "test", True), base / "reports")
    inp = base / "in"
    inp.mkdir()
    write_csv(inp / "new.csv", make_klines("15m", 100, 7))                      # slutar senare
    write_csv(inp / "old.csv", make_klines("15m", 40, 8))                       # slutar tidigare
    write_csv(inp / "gappy.csv", make_klines("15m", 60, 9).drop(index=[10, 11]).reset_index(drop=True))
    (base / "empty").mkdir()
    assert ig.verify_store(base / "store").exit_status == 0


@dataclass
class W:
    base: Path

    @property
    def store(self) -> Path:
        return self.base / "store"

    @property
    def p2(self) -> Path:
        return self.base / "p2"

    @property
    def reports(self) -> Path:
        return self.base / "reports"

    @property
    def inp(self) -> Path:
        return self.base / "in"

    def vs(self) -> vs.VersionedStore:
        return vs.VersionedStore(self.store)

    def dv(self, key: DatasetKey = SPOT) -> str:
        return self.vs().current_ref(key).ref.data_version


@pytest.fixture(scope="module")
def template(tmp_path_factory):
    base = tmp_path_factory.mktemp("tpl")
    _build_template(base)
    return base


def _copy(template: Path, dst: Path) -> W:
    shutil.copytree(template, dst, symlinks=True)
    return W(dst)


@pytest.fixture
def w(template, tmp_path):
    return _copy(template, tmp_path / "w")


@pytest.fixture
def twin(template, tmp_path):
    return _copy(template, tmp_path / "twin")


@dataclass
class R:
    code: int
    out: str
    err: str

    @property
    def env(self) -> dict:
        return json.loads(self.out)


def _never(*_a):
    raise AssertionError("klocka/git får bara anropas av ingest")


@pytest.fixture
def run(capsys):
    def go(*argv, clock=lambda: T, git=lambda: GIT) -> R:
        code = cli.main([str(a) for a in argv], clock=clock, git=git)
        o = capsys.readouterr()
        return R(code, o.out, o.err)
    return go


def tree_hash(root: Path, *, mtime: bool = True) -> str:
    hh = hashlib.sha256()
    for p in [root, *sorted(root.rglob("*"))]:
        st = os.lstat(p)
        hh.update(f"{p.relative_to(root)}|{st.st_mode:o}|{st.st_mtime_ns if mtime else ''}|".encode())
        if stat.S_ISREG(st.st_mode):
            hh.update(p.read_bytes())
        elif stat.S_ISLNK(st.st_mode):
            hh.update(os.readlink(p).encode())
    return hh.hexdigest()


def lib_ingest(store: vs.VersionedStore, files, key=SPOT, **kw) -> vs.IngestOutcome:
    kw = {"now": T, "code_version": GIT[0], "worktree_clean": True, **kw}
    with store.writer() as wr:
        return wr.ingest(key, [Path(f) for f in files], **kw)


def _boom(*_a, **_k):
    raise RuntimeError("injicerad krasch")


def make_pending_ingest(w: W, monkeypatch) -> None:
    with monkeypatch.context() as m:
        m.setattr(vs.StoreWriter, "_append_ref_log", _boom)
        with pytest.raises(RuntimeError):
            lib_ingest(w.vs(), [w.inp / "new.csv"])


def make_pending_ref(w: W, monkeypatch) -> None:
    legacy_dv = w.dv()
    lib_ingest(w.vs(), [w.inp / "new.csv"])
    with monkeypatch.context() as m:
        m.setattr(vs.StoreWriter, "_write_ref_file", _boom)
        with pytest.raises(RuntimeError), w.vs().writer() as wr:
            wr.set_current(SPOT, legacy_dv)


def corrupt_object(w: W, key: DatasetKey = SPOT) -> None:
    """Samma storlek, samma logiska innehåll, andra bytes: bara filhashen avslöjar det."""
    d = w.vs().object_dir(key, w.dv(key))
    data = (d / "bars.parquet").read_bytes()
    i = data.find(b"parquet-cpp-arrow")
    os.chmod(d, 0o755)
    os.chmod(d / "bars.parquet", 0o644)
    (d / "bars.parquet").write_bytes(data[:i] + b"Parquet" + data[i + 7:])
    os.chmod(d / "bars.parquet", 0o444)
    os.chmod(d, 0o555)


def dump_sha(store: Path) -> str:
    conn = catalog.open_catalog(store / vs.CATALOG_FILE, readonly=True)
    try:
        return catalog.dump_sha256(conn)
    finally:
        conn.close()


def fresh_store(base: Path, name: str = "fresh") -> Path:
    vs.VersionedStore.init(base / name)
    return base / name


# ================================================================== parser
REQ = "required"
PIN = {
    "": {"--format": "json"},
    "init": {"--store": REQ, "--apply": False},
    "import-legacy": {"--store": REQ, "--source": REQ, "--apply": False},
    "ingest": {"--store": REQ, "--dataset": REQ, "files": REQ, "--fail-on-gaps": False,
               "--allow-regression": False, "--apply": False},
    "list": {"--store": REQ},
    "versions": {"--store": REQ, "--dataset": REQ},
    "set-current": {"--store": REQ, "--dataset": REQ, "--data-version": REQ, "--apply": False},
    "snapshot": {},
    "snapshot create": {"--store": REQ, "--current": None, "--member": None, "--apply": False},
    "snapshot show": {"--store": REQ, "--id": REQ},
    "register-report": {"--store": REQ, "--source": REQ, "--apply": False},
    "verify": {"--store": REQ},
    "repair": {"--store": REQ, "--apply": False},
    "rebuild": {"--store": REQ, "--apply": False},
    "complete-pending": {"--store": REQ, "--kind": REQ, "--apply": False},
}


def _pin(parser: argparse.ArgumentParser, name: str, out: dict) -> None:
    opts = {}
    for a in parser._actions:
        if isinstance(a, argparse._SubParsersAction):
            for sub, sp in a.choices.items():
                _pin(sp, f"{name} {sub}".strip(), out)
        elif not isinstance(a, argparse._HelpAction):
            opts[a.option_strings[0] if a.option_strings else a.dest] = REQ if a.required else a.default
    out[name] = opts


def test_command_set_and_arguments_are_pinned():
    got: dict = {}
    _pin(cli._parser(), "", got)
    assert got == PIN
    assert set(cli.HANDLERS) | {"", "snapshot"} == set(PIN)
    assert cli.MUTATING == frozenset(MUTATING)
    assert [k for k, v in PIN.items() if "--apply" in v] == list(MUTATING)


ARGV_NO_STORE = {
    "init": ["init"], "import-legacy": ["import-legacy", "--source", "x"],
    "ingest": ["ingest", "--dataset", SPOT_D, "f.csv"], "list": ["list"],
    "versions": ["versions", "--dataset", SPOT_D],
    "set-current": ["set-current", "--dataset", SPOT_D, "--data-version", "a" * 64],
    "snapshot create": ["snapshot", "create", "--current", SPOT_D], "snapshot show": ["snapshot", "show", "--id", "a" * 64],
    "register-report": ["register-report", "--source", "x"], "verify": ["verify"], "repair": ["repair"],
    "rebuild": ["rebuild"], "complete-pending": ["complete-pending", "--kind", "ingest"],
}


@pytest.mark.parametrize("command", sorted(ARGV_NO_STORE))
def test_every_command_requires_an_explicit_store(run, command):
    r = run(*ARGV_NO_STORE[command], clock=_never, git=_never)
    assert r.code == cli.EXIT_USAGE == 64
    assert r.env["error"]["type"] == "UsageError" and "--store" in r.env["error"]["message"]
    assert r.env["command"] is None and r.env["result"] is None


def test_apply_must_be_spelled_out(run, w):
    before = tree_hash(w.base)
    for flag in ("--app", "--a", "--appl"):
        r = run("repair", "--store", w.store, flag)
        assert r.code == 64 and r.env["error"]["type"] == "UsageError"
    assert tree_hash(w.base) == before


# ================================================================== dry-run och --apply
def _dry_cases(w: W) -> dict[str, list]:
    dv = w.dv()
    return {
        "init-new": ["init", "--store", w.base / "new"],
        "init-existing": ["init", "--store", w.store],
        "import-legacy": ["import-legacy", "--store", w.store, "--source", w.p2],
        "ingest": ["ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv"],
        "set-current-same": ["set-current", "--store", w.store, "--dataset", SPOT_D, "--data-version", dv],
        "set-current-other": ["set-current", "--store", w.store, "--dataset", SPOT_D, "--data-version", "a" * 64],
        "snapshot-current": ["snapshot", "create", "--store", w.store, "--current", SPOT_D, "--current", PERP_D],
        "snapshot-member": ["snapshot", "create", "--store", w.store, "--member", f"{SPOT_D}@{dv}"],
        "register-report": ["register-report", "--store", w.store, "--source", w.reports],
        "repair": ["repair", "--store", w.store],
        "rebuild": ["rebuild", "--store", w.store],
        "complete-pending-ingest": ["complete-pending", "--store", w.store, "--kind", "ingest"],
        "complete-pending-ref": ["complete-pending", "--store", w.store, "--kind", "ref"],
    }


DRY_CASES = ("init-new", "init-existing", "import-legacy", "ingest", "set-current-same", "set-current-other",
             "snapshot-current", "snapshot-member", "register-report", "repair", "rebuild",
             "complete-pending-ingest", "complete-pending-ref")


@pytest.mark.parametrize("case", DRY_CASES)
def test_dry_run_is_the_default_and_writes_nothing(run, w, case):
    (w.store / "tmp" / ("0" * 32 + ".snapshot.json")).write_bytes(b"x")    # repair/rebuild har något att göra
    before = tree_hash(w.base)
    cases = _dry_cases(w)
    assert sorted(cases) == sorted(DRY_CASES)
    r = run(*cases[case])
    assert r.code == (1 if case in ("repair", "rebuild") else 0)                # 1 = TMP_LEFTOVER kvar
    assert r.env["applied"] is False and r.env["error"] is None
    assert tree_hash(w.base) == before
    assert not (w.base / "new").exists()


def test_ingest_apply_equals_the_library(run, w, twin):
    r = run("ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv", "--apply")
    lib = lib_ingest(twin.vs(), [twin.inp / "new.csv"])
    assert r.code == 0 and r.env["applied"] is True
    assert r.env["result"] == {"ingest_id": lib.ingest_id, "record_sha256": lib.record_sha256,
                               "record": lib.record.to_json_obj()}
    assert r.env["result_sha256"] == lib.record_sha256
    name = f"ingests/{lib.ingest_id}.json"
    assert (w.store / name).read_bytes() == (twin.store / name).read_bytes()
    assert dump_sha(w.store) == dump_sha(twin.store)


def test_set_current_apply_equals_the_library_and_the_dry_run(run, w, twin):
    for x in (w, twin):
        legacy_dv = x.dv()
        lib_ingest(x.vs(), [x.inp / "new.csv"])
    argv = ["set-current", "--store", w.store, "--dataset", SPOT_D, "--data-version", legacy_dv]
    dry = run(*argv)
    r = run(*argv, "--apply")
    with twin.vs().writer() as wr:
        u = wr.set_current(SPOT, legacy_dv)
    expected = {"dataset": SPOT_D, "dataset_id": catalog.dataset_id(SPOT), "data_version": legacy_dv,
                "changed": True, "previous_data_version": u.previous_data_version, "ref_log_seq": u.ref_log_seq}
    assert r.code == 0 and r.env["result"] == expected and u.changed
    assert {k: dry.env["result"][k] for k in expected if k != "ref_log_seq"} == \
        {k: v for k, v in expected.items() if k != "ref_log_seq"}
    assert dry.env["result"]["target_exists"] is True
    assert dump_sha(w.store) == dump_sha(twin.store)
    again = run(*argv, "--apply")                                    # P4-S3-D5: ingen ändring
    assert again.code == 0 and again.env["result"]["changed"] is False
    assert run(*argv).env["result"]["changed"] is False


@pytest.mark.parametrize("form", ["current", "member"])
def test_snapshot_create_equals_the_library_and_the_dry_run(run, w, twin, form):
    spot, perp = w.dv(SPOT), w.dv(PERP)
    args = (["--current", SPOT_D, "--current", PERP_D] if form == "current"
            else ["--member", f"{PERP_D}@{perp}", "--member", f"{SPOT_D}@{spot}"])
    dry = run("snapshot", "create", "--store", w.store, *args)
    r = run("snapshot", "create", "--store", w.store, *args, "--apply")
    with twin.vs().writer() as wr:
        lib = wr.snapshot_current([SPOT, PERP])
    assert r.code == 0 and r.env["result"]["snapshot_id"] == lib.snapshot.snapshot_id
    assert dry.env["result"] == r.env["result"] and r.env["result"]["outcome"] == "created"
    assert r.env["result_sha256"] == hashlib.sha256(lib.snapshot.to_file_bytes()).hexdigest()
    name = f"snapshots/{lib.snapshot.snapshot_id}.json"
    assert (w.store / name).read_bytes() == (twin.store / name).read_bytes()
    assert run("snapshot", "create", "--store", w.store, *args).env["result"]["outcome"] == "reused"
    assert run("snapshot", "create", "--store", w.store, *args, "--apply").env["result"]["outcome"] == "reused"
    show = run("snapshot", "show", "--store", w.store, "--id", lib.snapshot.snapshot_id)
    assert show.code == 0 and show.env["result"]["members"] == r.env["result"]["members"]
    assert show.env["result_sha256"] == r.env["result_sha256"]


def test_import_legacy_equals_the_library_dry_run_and_apply(run, w, twin):
    a, b = fresh_store(w.base), fresh_store(twin.base)
    dry = run("import-legacy", "--store", a, "--source", w.p2)
    lib_dry = lg.import_legacy(vs.VersionedStore(b), twin.p2)
    assert dry.code == 0 and dry.env["result"] == lib_dry.to_json_obj()
    assert dry.env["result_sha256"] == hashlib.sha256(lib_dry.to_json_bytes()).hexdigest()
    r = run("import-legacy", "--store", a, "--source", w.p2, "--apply")
    lib = lg.import_legacy(vs.VersionedStore(b), twin.p2, apply=True)
    assert r.code == 0 and r.env["applied"] is True and r.env["result"] == lib.to_json_obj()
    assert dump_sha(a) == dump_sha(b) and ig.verify_store(a).exit_status == 0


def test_register_report_equals_the_library_dry_run_and_apply(run, w, twin):
    dry = run("register-report", "--store", w.store, "--source", w.reports)
    lib_dry = qreg.register_quality_reports(twin.vs(), twin.reports)
    assert dry.code == 0 and dry.env["result"] == lib_dry.to_json_obj()
    r = run("register-report", "--store", w.store, "--source", w.reports, "--apply")
    lib = qreg.register_quality_reports(twin.vs(), twin.reports, apply=True)
    assert r.code == 0 and r.env["result"] == lib.to_json_obj()
    assert r.env["result_sha256"] == hashlib.sha256(lib.to_json_bytes()).hexdigest()
    assert {x["status"] for x in r.env["result"]["reports"]} == {"registered"}
    assert dump_sha(w.store) == dump_sha(twin.store) and ig.verify_store(w.store).exit_status == 0


def test_repair_equals_the_library(run, w, twin):
    for x in (w, twin):
        (x.store / "tmp" / ("0" * 32 + ".snapshot.json")).write_bytes(b"x")
        (x.store / vs.CATALOG_FILE).unlink()
    dry = run("repair", "--store", w.store)
    lib_dry = ig.repair_store(twin.store)
    assert dry.code == 1 and dry.env["result"]["before"] == lib_dry.before.to_json_obj()
    assert dry.env["result"]["plan"] == {"tmp_removals": ["tmp/" + "0" * 32 + ".snapshot.json"],
                                         "rebuild_catalog": True, "blocking": []}
    r = run("repair", "--store", w.store, "--apply")
    lib = ig.repair_store(twin.store, apply=True)
    assert r.code == 0 and r.env["result"]["after"] == lib.after.to_json_obj()
    assert r.env["result"]["applied"] is True and dump_sha(w.store) == dump_sha(twin.store)


def test_init_equals_the_library(run, w, twin):
    dry = run("init", "--store", w.base / "new")
    assert dry.code == 0 and dry.env["result"] == {"already_initialised": False, "predicted": False}
    assert not (w.base / "new").exists()
    r = run("init", "--store", w.base / "new", "--apply")
    vs.VersionedStore.init(twin.base / "new")
    assert r.code == 0 and r.env["result"] == {"already_initialised": False, "initialised": True}
    assert (w.base / "new" / vs.STORE_FILE).read_bytes() == (twin.base / "new" / vs.STORE_FILE).read_bytes()
    assert dump_sha(w.base / "new") == dump_sha(twin.base / "new")
    assert run("verify", "--store", w.base / "new").code == 0
    again = run("init", "--store", w.base / "new", "--apply")
    assert again.code == 0 and again.env["result"]["already_initialised"] is True


def test_init_refuses_a_foreign_directory(run, w):
    before = tree_hash(w.p2)
    r = run("init", "--store", w.p2, "--apply")
    assert r.code == 6 and r.env["error"]["type"] == "StoreFormatError"
    assert tree_hash(w.p2) == before


# ================================================================== P4-S4-c och arbetskatalogen
def test_ingest_context_is_resolved_once_passed_explicitly_and_shown(run, w):
    calls = {"clock": 0, "git": 0}

    def clock():
        calls["clock"] += 1
        return T

    def git():
        calls["git"] += 1
        return GIT

    ctx = {"now": T.isoformat(), "code_version": GIT[0], "worktree_clean": True}
    dry = run("ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv", clock=clock, git=git)
    assert calls == {"clock": 1, "git": 1} and dry.env["context"] == ctx
    r = run("ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv", "--apply", clock=clock, git=git)
    assert calls == {"clock": 2, "git": 2} and r.code == 0 and r.env["context"] == ctx
    rec = w.vs().read_ingest(r.env["result"]["ingest_id"]).to_json_obj()
    assert rec["params"] == {"now": T.isoformat(), "fail_on_gaps": False, "allow_regression": False}
    assert rec["code"] == {"version": GIT[0], "worktree_clean": True}


def test_ingest_dry_run_preview(run, w):
    r = run("ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv", w.inp / "gappy.csv")
    data = {p: (w.inp / p).read_bytes() for p in ("gappy.csv", "new.csv")}
    assert r.code == 0 and r.env["result"] == {
        "predicted": False, "dataset": SPOT_D, "dataset_id": catalog.dataset_id(SPOT),
        "files": [{"name": n, "bytes": len(b), "sha256": hashlib.sha256(b).hexdigest()} for n, b in data.items()],
        "current": {"data_version": w.dv(), "ref_log_seq": w.vs().current_ref(SPOT).ref_log_seq, "pending": False,
                    "pending_data_version": None},
        "fail_on_gaps": False, "allow_regression": False}
    applied = run("ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv", "--apply")
    assert [s["sha256"] for s in applied.env["result"]["record"]["sources"]] == \
        [hashlib.sha256(data["new.csv"]).hexdigest()]


@pytest.mark.parametrize("command", ["list", "versions", "verify", "repair", "rebuild", "snapshot create",
                                     "set-current", "import-legacy", "register-report", "complete-pending", "init"])
def test_only_ingest_resolves_clock_and_git(run, w, command):
    argv = {"list": ["list", "--store", w.store], "versions": ["versions", "--store", w.store, "--dataset", SPOT_D],
            "verify": ["verify", "--store", w.store], "repair": ["repair", "--store", w.store],
            "rebuild": ["rebuild", "--store", w.store],
            "snapshot create": ["snapshot", "create", "--store", w.store, "--current", SPOT_D, "--apply"],
            "set-current": ["set-current", "--store", w.store, "--dataset", SPOT_D, "--data-version", w.dv(), "--apply"],
            "import-legacy": ["import-legacy", "--store", w.store, "--source", w.p2, "--apply"],
            "register-report": ["register-report", "--store", w.store, "--source", w.reports, "--apply"],
            "complete-pending": ["complete-pending", "--store", w.store, "--kind", "ref", "--apply"],
            "init": ["init", "--store", w.base / "new", "--apply"]}[command]
    r = run(*argv, clock=_never, git=_never)
    assert r.code == 0 and r.env["context"] is None


@pytest.mark.parametrize("now", [datetime(2031, 5, 6, 7, 8, 9),                     # noqa: DTZ001 - avsiktligt naiv
                                 datetime(2031, 5, 6, 7, 8, 9, tzinfo=timezone(timedelta(hours=1)))])
@pytest.mark.parametrize("apply", [False, True])
def test_ingest_refuses_a_clock_that_is_not_utc(run, w, now, apply):
    before = tree_hash(w.base)
    r = run("ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv", *(["--apply"] if apply else []),
            clock=lambda: now)
    assert r.code == 6 and r.env["error"]["type"] == "ValueError"
    assert r.env["context"]["now"] == now.isoformat()
    assert tree_hash(w.base) == before


@pytest.mark.parametrize("state", [("c0de" * 10, False), ("unknown", None), ("c0de" * 10, None)])
def test_ingest_apply_requires_a_clean_worktree(run, w, state):
    before = tree_hash(w.base)
    argv = ["ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv"]
    r = run(*argv, "--apply", git=lambda: state)
    assert r.code == 5 and r.env["error"]["type"] == "WorktreeNotCleanError" and r.env["result"] is None
    assert r.env["context"] == {"now": T.isoformat(), "code_version": state[0], "worktree_clean": state[1]}
    assert tree_hash(w.base) == before
    dry = run(*argv, git=lambda: state)                                   # dry-run visar värdena
    assert dry.code == 0 and dry.env["context"]["worktree_clean"] == state[1]
    text = run("--format", "text", *argv, git=lambda: state)
    assert "VARNING: arbetskatalogen är inte ren eller okänd; ingest --apply vägras" in text.out
    assert tree_hash(w.base) == before
    assert "VARNING" not in run("--format", "text", *argv).out


# ================================================================== P4-S4-d
def test_regression_guard_is_only_lifted_by_the_flag(run, w):
    before = tree_hash(w.base)
    argv = ["ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "old.csv", "--apply"]
    r = run(*argv)
    assert r.code == 5 and r.env["error"]["type"] == "VersionRegressionError"
    assert "--allow-regression" in r.env["error"]["message"] and "--allow-regression" in r.err
    assert tree_hash(w.base) == before
    ok = run(*argv, "--allow-regression")
    assert ok.code == 0 and ok.env["result"]["record"]["params"]["allow_regression"] is True
    assert ok.env["result"]["record"]["ref_outcome"] == "changed"
    newer = run("ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv", "--apply")
    assert newer.code == 0 and newer.env["result"]["record"]["params"]["allow_regression"] is False


# ================================================================== verify-exitkoder
def _warn(w, mp):
    (w.store / "tmp" / "junk.txt").write_bytes(b"x")


def _repairable(w, mp):
    (w.store / vs.CATALOG_FILE).unlink()


def _pending(w, mp):
    make_pending_ingest(w, mp)


def _error(w, mp):
    corrupt_object(w)


@pytest.mark.parametrize(("damage", "code", "finding"), [
    (None, 0, None), (_warn, 1, "TMP_UNKNOWN"), (_repairable, 1, "CATALOG_MISSING"),
    (_pending, 2, "PENDING_INGEST"), (_error, 3, "OBJECT_CORRUPT")])
def test_verify_exit_status_is_the_report_severity(run, w, monkeypatch, damage, code, finding):
    if damage is not None:
        damage(w, monkeypatch)
    r = run("verify", "--store", w.store)
    rep = ig.verify_store(w.store)
    assert r.code == code == rep.exit_status == r.env["exit_code"]
    assert r.env["result"] == rep.to_json_obj() and r.env["result_sha256"] == rep.report_sha256
    assert finding is None or finding in rep.codes
    assert r.env["error"] is None and r.env["applied"] is False


# ================================================================== felkoder
def _lock_argv(w: W) -> dict[str, list]:
    return {
        "import-legacy": ["import-legacy", "--store", w.store, "--source", w.p2, "--apply"],
        "ingest": ["ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv", "--apply"],
        "set-current": ["set-current", "--store", w.store, "--dataset", SPOT_D, "--data-version", w.dv(), "--apply"],
        "snapshot create": ["snapshot", "create", "--store", w.store, "--current", SPOT_D, "--apply"],
        "register-report": ["register-report", "--store", w.store, "--source", w.reports, "--apply"],
        "repair": ["repair", "--store", w.store, "--apply"],
        "rebuild": ["rebuild", "--store", w.store, "--apply"],
        "complete-pending": ["complete-pending", "--store", w.store, "--kind", "ingest", "--apply"],
        "verify": ["verify", "--store", w.store],
    }


LOCK_COMMANDS = ("complete-pending", "import-legacy", "ingest", "rebuild", "register-report", "repair",
                 "set-current", "snapshot create", "verify")


@pytest.mark.parametrize("command", LOCK_COMMANDS)
def test_a_held_writer_lock_refuses_every_writer_and_verify(run, w, command):
    cases = _lock_argv(w)
    assert sorted(cases) == list(LOCK_COMMANDS)
    argv = cases[command]
    before = tree_hash(w.base)
    with w.vs().writer():
        r = run(*argv)
    assert r.code == 5 and r.env["error"]["type"] == "StoreLockedError" and r.env["result"] is None
    assert tree_hash(w.base) == before


def test_readers_work_while_a_writer_holds_the_lock(run, w):
    with w.vs().writer() as wr:
        sid = wr.snapshot_current([SPOT]).snapshot.snapshot_id
    before = tree_hash(w.base)
    with w.vs().writer():
        for argv in (["list", "--store", w.store], ["versions", "--store", w.store, "--dataset", SPOT_D],
                     ["snapshot", "show", "--store", w.store, "--id", sid]):
            assert run(*argv).code == 0
    assert tree_hash(w.base) == before


@pytest.mark.parametrize("argv", [
    ["ingest", "--dataset", SPOT_D, "IN/new.csv", "--apply"],
    ["set-current", "--dataset", SPOT_D, "--data-version", "DV", "--apply"],
    ["snapshot", "create", "--current", SPOT_D, "--apply"],
    ["complete-pending", "--kind", "ref", "--apply"]])
def test_a_pending_ingest_refuses_writers(run, w, monkeypatch, argv):
    make_pending_ingest(w, monkeypatch)
    sub = {"IN/new.csv": str(w.inp / "new.csv"), "DV": w.dv()}
    before = tree_hash(w.base)
    r = run(*[sub.get(a, a) for a in argv], "--store", w.store)
    assert r.code == 5 and r.env["error"]["type"] == "PendingIngestError"
    assert tree_hash(w.base) == before


def test_a_pending_ref_update_refuses_set_current(run, w, monkeypatch):
    make_pending_ref(w, monkeypatch)
    before = tree_hash(w.base)
    r = run("set-current", "--store", w.store, "--dataset", PERP_D, "--data-version", w.dv(PERP), "--apply")
    assert r.code == 5 and r.env["error"]["type"] == "PendingRefUpdateError"
    assert tree_hash(w.base) == before


@pytest.mark.parametrize("command", ["repair", "rebuild"])
def test_repair_and_rebuild_refuse_over_an_error(run, w, command):
    corrupt_object(w)
    before = tree_hash(w.base)
    dry = run(command, "--store", w.store)
    assert dry.code == 3 and dry.env["error"] is None
    r = run(command, "--store", w.store, "--apply")
    assert r.code == 5 and r.env["error"]["type"] == "RepairRefusedError"
    assert tree_hash(w.base) == before


def _errors(w: W) -> list[tuple[str, list, int, str]]:
    missing = w.base / "missing"
    return [
        ("missing-store-verify", ["verify", "--store", missing], 6, "StoreFormatError"),
        ("missing-store-list", ["list", "--store", missing], 6, "StoreFormatError"),
        ("missing-store-ingest", ["ingest", "--store", missing, "--dataset", SPOT_D, w.inp / "new.csv"], 6,
         "StoreFormatError"),
        ("phase2-dir-is-no-store", ["verify", "--store", w.p2], 6, "StoreFormatError"),
        ("missing-input-dry", ["ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "nope.csv"], 6,
         "FileNotFoundError"),
        ("fail-on-gaps", ["ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "gappy.csv", "--fail-on-gaps",
                          "--apply"], 6, None),
        ("unknown-snapshot", ["snapshot", "show", "--store", w.store, "--id", "a" * 64], 6, "SnapshotNotFoundError"),
        ("import-empty-source", ["import-legacy", "--store", w.store, "--source", w.base / "empty"], 4, None),
        ("import-empty-source-apply", ["import-legacy", "--store", w.store, "--source", w.base / "empty", "--apply"],
         4, None),
        ("register-empty-source", ["register-report", "--store", w.store, "--source", w.base / "empty"], 4, None),
        ("import-missing-source", ["import-legacy", "--store", w.store, "--source", missing], 6, "LegacyImportError"),
        ("lower-case-symbol", ["versions", "--store", w.store, "--dataset", "binance/spot/btcusdt/15m"], 64, "UsageError"),
        ("slash-symbol", ["versions", "--store", w.store, "--dataset", "binance/spot/BTC/USDT/15m"], 64, "UsageError"),
        ("unknown-timeframe", ["versions", "--store", w.store, "--dataset", "binance/spot/BTCUSDT/7m"], 64, "UsageError"),
        ("no-exchange", ["versions", "--store", w.store, "--dataset", "spot/BTCUSDT/15m"], 64, "UsageError"),
        ("other-exchange", ["versions", "--store", w.store, "--dataset", "kraken/spot/BTCUSDT/15m"], 64, "UsageError"),
        ("unknown-instrument", ["versions", "--store", w.store, "--dataset", "binance/swap/BTCUSDT/15m"], 64,
         "UsageError"),
        ("bad-hex", ["set-current", "--store", w.store, "--dataset", SPOT_D, "--data-version", "XYZ"], 64, "UsageError"),
        ("upper-hex", ["set-current", "--store", w.store, "--dataset", SPOT_D, "--data-version", "A" * 64], 64,
         "UsageError"),
        ("short-id", ["snapshot", "show", "--store", w.store, "--id", "a" * 63], 64, "UsageError"),
        ("member-without-version", ["snapshot", "create", "--store", w.store, "--member", SPOT_D], 64, "UsageError"),
        ("both-snapshot-forms", ["snapshot", "create", "--store", w.store, "--current", SPOT_D, "--member",
                                 f"{SPOT_D}@{'a' * 64}"], 64, "UsageError"),
        ("no-snapshot-form", ["snapshot", "create", "--store", w.store], 64, "UsageError"),
        ("unknown-kind", ["complete-pending", "--store", w.store, "--kind", "all"], 64, "UsageError"),
        ("unknown-command", ["frobnicate", "--store", w.store], 64, "UsageError"),
        ("no-command", [], 64, "UsageError"),
        ("bad-format", ["--format", "xml", "verify", "--store", w.store], 64, "UsageError"),
    ]


@pytest.mark.parametrize("case", [c[0] for c in _errors(W(Path("/x")))])
def test_every_error_has_its_exit_code_and_never_zero(run, w, case):
    _, argv, code, etype = next(c for c in _errors(w) if c[0] == case)
    before = tree_hash(w.base)
    r = run(*argv)
    assert r.code == code != 0 and r.env["exit_code"] == code
    if code == 4:
        assert r.env["error"] is None and r.env["result"]["ok"] is False
    else:
        assert r.env["result"] is None and r.err.count("\n") == 1
        assert etype is None or r.env["error"]["type"] == etype
    assert tree_hash(w.base) == before


def test_corrupt_target_is_a_store_error(run, w):
    corrupt_object(w)
    r = run("set-current", "--store", w.store, "--dataset", SPOT_D, "--data-version", w.dv(), "--apply")
    assert r.code == 6 and r.env["error"]["type"] == "ObjectCorruptError"


def test_unrecognized_report_source_entry_is_not_ok(run, w):
    shutil.copytree(w.reports, w.base / "rep2")
    (w.base / "rep2" / "junk.txt").write_bytes(b"x")
    r = run("register-report", "--store", w.store, "--source", w.base / "rep2", "--apply")
    assert r.code == 4 and r.env["result"]["unrecognized"] == ["junk.txt"]


def test_an_internal_error_exits_70_with_a_traceback(run, w, monkeypatch):
    def boom(root, **_k):
        raise RuntimeError(f"internt fel i {os.path.realpath(root)}")
    monkeypatch.setattr(ig, "verify_store", boom)
    r = run("verify", "--store", w.store)
    assert r.code == 70 and r.env["error"] == {"type": "RuntimeError", "message": "internt fel i <store>"}
    assert "Traceback" in r.err and str(w.base) not in r.err and os.path.realpath(w.base) not in r.err


# ================================================================== utdata
def _battery(w: W) -> list[list]:
    sid_args = ["--current", SPOT_D, "--current", PERP_D]
    return [
        ["list", "--store", w.store], ["versions", "--store", w.store, "--dataset", SPOT_D],
        ["verify", "--store", w.store], ["repair", "--store", w.store], ["rebuild", "--store", w.store],
        ["import-legacy", "--store", w.store, "--source", w.p2],
        ["register-report", "--store", w.store, "--source", w.reports],
        ["ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv"],
        ["ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv", "--apply"],
        ["ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "nope.csv"],
        ["ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "old.csv", "--apply"],
        ["snapshot", "create", "--store", w.store, *sid_args],
        ["snapshot", "create", "--store", w.store, *sid_args, "--apply"],
        ["register-report", "--store", w.store, "--source", w.reports, "--apply"],
        ["rebuild", "--store", w.store, "--apply"],
        ["complete-pending", "--store", w.store, "--kind", "ingest"],
        ["verify", "--store", w.base / "missing"],
        ["import-legacy", "--store", w.store, "--source", w.base / "missing"],
        ["init", "--store", w.p2, "--apply"],
    ]


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_output_is_deterministic_canonical_and_free_of_absolute_paths(run, w, twin, fmt):
    roots = {str(w.base), os.path.realpath(w.base), str(twin.base), os.path.realpath(twin.base)}
    for a, b in zip(_battery(w), _battery(twin), strict=True):
        ra, rb = run("--format", fmt, *a), run("--format", fmt, *b)
        assert (ra.code, ra.out, ra.err) == (rb.code, rb.out, rb.err), a
        assert not any(root in ra.out + ra.err for root in roots), a
        if fmt == "json":
            assert ra.out == vs.canonical_json_bytes(json.loads(ra.out)).decode()
            assert ra.env["format"] == "btc-quant/datastore-cli/v1" and ra.env["exit_code"] == ra.code
        else:
            assert ra.out.startswith(f"datastore {' '.join(a[:2]) if a[0] == 'snapshot' else a[0]}: exit {ra.code}")


def test_text_is_a_rendering_of_the_json(run, w):
    j = run("snapshot", "create", "--store", w.store, "--current", SPOT_D)
    t = run("--format", "text", "snapshot", "create", "--store", w.store, "--current", SPOT_D)
    assert t.out.splitlines()[0] == "datastore snapshot create: exit 0 (dry-run: ingenting skrevs)"
    assert f'snapshot_id: "{j.env["result"]["snapshot_id"]}"' in t.out
    assert f'members[0].data_version: "{w.dv()}"' in t.out
    listing = run("--format", "text", "list", "--store", w.store)
    assert "OBS: dataset och versioner kommer från katalogen" in listing.out


# ================================================================== källor och läsare
def test_sources_and_inputs_are_never_written(run, w):
    store = fresh_store(w.base)
    before = {d: tree_hash(d) for d in (w.p2, w.reports, w.inp)}
    assert run("import-legacy", "--store", store, "--source", w.p2, "--apply").code == 0
    assert run("register-report", "--store", store, "--source", w.reports, "--apply").code == 0
    assert run("ingest", "--store", store, "--dataset", SPOT_D, w.inp / "new.csv", "--apply").code == 0
    assert {d: tree_hash(d) for d in (w.p2, w.reports, w.inp)} == before
    assert ig.verify_store(store).exit_status == 0


def test_list_and_versions_read_the_catalog_and_current_refs(run, w):
    before = tree_hash(w.base)
    listing = run("list", "--store", w.store)
    assert tree_hash(w.base) == before
    res = listing.env["result"]
    assert (res["datasets_from"], res["current_from"]) == ("catalog", "files")
    assert [(d["dataset"], d["dataset_id"], d["current"]["data_version"]) for d in res["datasets"]] == \
        [(PERP_D, catalog.dataset_id(PERP), w.dv(PERP)), (SPOT_D, catalog.dataset_id(SPOT), w.dv(SPOT))]
    legacy_dv = w.dv()
    new = run("ingest", "--store", w.store, "--dataset", SPOT_D, w.inp / "new.csv", "--apply")
    new_dv = new.env["result"]["record"]["data_version"]
    before = tree_hash(w.base)
    versions = run("versions", "--store", w.store, "--dataset", SPOT_D)
    assert tree_hash(w.base) == before
    rows = versions.env["result"]["versions"]
    assert sorted((r["data_version"], r["current"]) for r in rows) == sorted([(legacy_dv, False), (new_dv, True)])
    conn = catalog.open_catalog(w.store / vs.CATALOG_FILE, readonly=True)
    try:
        lib_rows = catalog.list_dataset_versions(conn, catalog.dataset_id(SPOT))
    finally:
        conn.close()
    assert [{k: v for k, v in r.items() if k != "current"} for r in rows] == lib_rows
    assert versions.env["result"]["current"]["data_version"] == new_dv


# ================================================================== complete-pending, rebuild
def test_complete_pending_ingest(run, w, monkeypatch):
    make_pending_ingest(w, monkeypatch)
    pending = w.vs().ingest_history()[-1]
    assert run("verify", "--store", w.store).code == 2
    dry = run("complete-pending", "--store", w.store, "--kind", "ingest")
    assert dry.code == 0 and [f["code"] for f in dry.env["result"]["pending"]] == ["PENDING_INGEST"]
    r = run("complete-pending", "--store", w.store, "--kind", "ingest", "--apply")
    assert r.code == 0 and r.env["result"]["completed"]["ingest_id"] == pending.ingest_id
    assert run("verify", "--store", w.store).code == 0
    again = run("complete-pending", "--store", w.store, "--kind", "ingest", "--apply")
    assert again.code == 0 and again.env["result"] == {"kind": "ingest", "completed": None}


def test_complete_pending_ref(run, w, monkeypatch):
    make_pending_ref(w, monkeypatch)
    assert run("verify", "--store", w.store).code == 2
    r = run("complete-pending", "--store", w.store, "--kind", "ref", "--apply")
    assert r.code == 0 and [u["dataset"] for u in r.env["result"]["completed"]] == [SPOT_D]
    assert run("verify", "--store", w.store).code == 0


def test_rebuild_of_a_clean_catalog_is_identical(run, w):
    before = dump_sha(w.store)
    dry = run("rebuild", "--store", w.store)
    assert dry.code == 0 and dry.env["result"]["after"] is None and dry.env["result"]["identical"] is None
    r = run("rebuild", "--store", w.store, "--apply")
    res = r.env["result"]
    assert r.code == 0 and res["applied"] is True and res["identical"] is True
    assert res["catalog_dump_sha256_before"] == res["catalog_dump_sha256_after"] == before == dump_sha(w.store)
    assert res["after"]["exit_status"] == 0 and res["blocking"] == []


def test_rebuild_of_a_missing_catalog_reproduces_the_dump(run, w):
    before = dump_sha(w.store)
    (w.store / vs.CATALOG_FILE).unlink()
    r = run("rebuild", "--store", w.store, "--apply")
    res = r.env["result"]
    assert r.code == 0 and res["catalog_dump_sha256_before"] is None and res["identical"] is False
    assert res["catalog_dump_sha256_after"] == before == dump_sha(w.store)


def test_rebuild_never_touches_tmp_leftovers(run, w):
    leftover = w.store / "tmp" / ("0" * 32 + ".snapshot.json")
    leftover.write_bytes(b"x")
    r = run("rebuild", "--store", w.store, "--apply")
    assert r.code == 1 and r.env["result"]["identical"] is True and leftover.read_bytes() == b"x"
    assert ig.rebuild_catalog_store(w.store, apply=True).after.codes == {"TMP_LEFTOVER"}


# ================================================================== tunt lager (statisk kontroll)
ALLOWED_IMPORTS = {"__future__", "argparse", "hashlib", "json", "os", "sys", "traceback", "collections.abc",
                   "contextlib", "dataclasses", "datetime", "pathlib", "typing", "btc_trading_system.core.types",
                   "btc_trading_system.data", "btc_trading_system.data.schema",
                   "btc_trading_system.data_validation.errors", "btc_trading_system.database",
                   "btc_trading_system.database.refs"}
FORBIDDEN_CALLS = {"open", "write_bytes", "write_text", "unlink", "rename", "link", "symlink", "symlink_to",
                   "hardlink_to", "chmod", "mkdir", "makedirs", "rmdir", "rmtree", "remove", "touch", "connect",
                   "flock", "fsync", "StoreWriter", "_WriterLock", "execute", "executemany"}
LIBRARY_ALIASES = {"vs", "integrity", "legacy", "qreg", "catalog"}


def test_cli_is_a_thin_layer():
    tree = ast.parse((ROOT / "btc_trading_system/database/cli.py").read_text(encoding="utf-8"))
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module)
        elif isinstance(node, ast.Call):
            f = node.func
            name = f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else None
            assert name not in FORBIDDEN_CALLS, ast.unparse(node)
            if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id == "os":
                assert f.attr == "stat", ast.unparse(node)
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) \
                and node.value.id in LIBRARY_ALIASES:
            assert not node.attr.startswith("_"), ast.unparse(node)
    assert imports == ALLOWED_IMPORTS


# ================================================================== riktiga ingången (subprocess)
def sub(cwd: Path, *argv, path: str | None = None, script: bool = False) -> R:
    env = {"PATH": path or os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONPATH": str(ROOT),
           "PYTHONDONTWRITEBYTECODE": "1"}
    entry = [str(ROOT / "scripts" / "datastore.py")] if script else ["-m", "btc_trading_system.database.cli"]
    r = subprocess.run([sys.executable, *entry, *[str(a) for a in argv]], cwd=cwd, env=env,
                       capture_output=True, text=True, timeout=300, check=False)
    return R(r.returncode, r.stdout, r.stderr)


def fake_git(d: Path) -> str:
    d.mkdir()
    g = d / "git"
    g.write_text(f'#!/bin/sh\ncase "$1" in\n  rev-parse) echo {FAKE_HEAD} ;;\n  status) ;;\n  *) exit 1 ;;\nesac\n')
    g.chmod(0o755)
    return f"{d}:{os.environ.get('PATH', '/usr/bin:/bin')}"


def test_subprocess_write_group(w, tmp_path):
    store = tmp_path / "s"
    assert sub(tmp_path, "init", "--store", store, "--apply").code == 0
    t0 = datetime.now(UTC)
    dry = sub(tmp_path, "ingest", "--store", store, "--dataset", SPOT_D, w.inp / "new.csv")
    t1 = datetime.now(UTC)
    version, clean = quality_cli.git_state()
    ctx = dry.env["context"]
    assert dry.code == 0 and (ctx["code_version"], ctx["worktree_clean"]) == (version, clean)
    assert t0 <= datetime.fromisoformat(ctx["now"]) <= t1
    real = sub(tmp_path, "ingest", "--store", store, "--dataset", SPOT_D, w.inp / "new.csv", "--apply")
    assert real.code == (0 if clean is True else 5)
    if real.code == 5:
        assert real.env["error"]["type"] == "WorktreeNotCleanError" and not (store / "ingests").exists()
    t0 = datetime.now(UTC)
    r = sub(tmp_path, "ingest", "--store", store, "--dataset", SPOT_D, w.inp / "new.csv", "--apply",
            path=fake_git(tmp_path / "bin"))
    t1 = datetime.now(UTC)
    assert r.code == 0, r.err
    rec = r.env["result"]["record"]
    assert rec["code"] == {"version": FAKE_HEAD, "worktree_clean": True}
    assert t0 <= datetime.fromisoformat(rec["params"]["now"]) <= t1
    dv = rec["data_version"]
    s = sub(tmp_path, "set-current", "--store", store, "--dataset", SPOT_D, "--data-version", dv, "--apply")
    assert s.code == 0 and s.env["result"]["changed"] is False
    snap = sub(tmp_path, "snapshot", "create", "--store", store, "--current", SPOT_D, "--apply")
    assert snap.code == 0 and snap.env["result"]["outcome"] == "created"
    assert ig.verify_store(store).exit_status == 0


def test_subprocess_read_group(w, tmp_path):
    with w.vs().writer() as wr:
        sid = wr.snapshot_current([SPOT, PERP]).snapshot.snapshot_id
    before = tree_hash(w.base)
    listing = sub(tmp_path, "list", "--store", w.store)
    assert listing.code == 0 and [d["dataset"] for d in listing.env["result"]["datasets"]] == [PERP_D, SPOT_D]
    versions = sub(tmp_path, "versions", "--store", w.store, "--dataset", SPOT_D)
    assert versions.code == 0 and [v["data_version"] for v in versions.env["result"]["versions"]] == [w.dv()]
    show = sub(tmp_path, "--format", "text", "snapshot", "show", "--store", w.store, "--id", sid)
    assert show.code == 0 and show.out.startswith("datastore snapshot show: exit 0\n") and sid in show.out
    assert tree_hash(w.base) == before


def test_subprocess_integrity_group(w, tmp_path):
    v = sub(tmp_path, "verify", "--store", w.store)
    assert v.code == 0 and v.env["result_sha256"] == ig.verify_store(w.store).report_sha256
    corrupt_object(w)
    assert sub(tmp_path, "verify", "--store", w.store).code == 3
    assert sub(tmp_path, "repair", "--store", w.store, "--apply").code == 5
    r = sub(tmp_path, "rebuild", "--store", w.store)
    assert r.code == 3 and r.env["result"]["blocking"][0]["code"] == "OBJECT_CORRUPT"


def test_subprocess_sources_group(w, tmp_path):
    store = fresh_store(tmp_path)
    before = (tree_hash(w.p2), tree_hash(w.reports))
    assert sub(tmp_path, "import-legacy", "--store", store, "--source", w.p2, "--apply").code == 0
    r = sub(tmp_path, "register-report", "--store", store, "--source", w.reports, "--apply")
    assert r.code == 0 and r.env["result"]["ok"] is True
    rb = sub(tmp_path, "rebuild", "--store", store, "--apply")
    assert rb.code == 0 and rb.env["result"]["identical"] is True
    assert (tree_hash(w.p2), tree_hash(w.reports)) == before


def test_subprocess_usage_and_pending_group(w, tmp_path):
    u = sub(tmp_path)
    assert u.code == 64 and u.env["error"]["type"] == "UsageError" and u.err.startswith("datastore -: UsageError")
    c = sub(tmp_path, "complete-pending", "--store", w.store, "--kind", "ingest")
    assert c.code == 0 and c.env["result"] == {"kind": "ingest", "pending": []}
    s = sub(tmp_path, "verify", "--store", w.store, script=True)
    assert s.code == 0 and s.env["command"] == "verify"
