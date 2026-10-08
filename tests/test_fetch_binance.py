"""Etapp B steg 2: verktyget för Binance-arkivet (scripts/data/fetch_binance.py; SB-S2-*).

Hermetiskt: ett falskt arkiv (URL -> bytes) ersätter nätet; HTTP-lagret testas mot en lokal server på
127.0.0.1. T1-T16 enligt designen (~/phase4_work/scratch/stageB/step2/DESIGN_step2.md avsnitt 8).
"""
import fcntl
import hashlib
import http.server
import importlib.util
import json
import os
import stat
import subprocess
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("fetch_binance_under_test", ROOT / "scripts/data/fetch_binance.py")
assert _SPEC is not None and _SPEC.loader is not None
fb = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = fb                       # dataclasses slår upp modulen
_SPEC.loader.exec_module(fb)

NOW = datetime(2030, 1, 1, tzinfo=UTC)
TARGETS_SHA256 = "79faeea5dc7821adbe5e98319d9f93393a29517fba421d7cacfa7028b017cc62"   # T1
INVENTORY_SHA256 = "0312d05ce23a1bb0ec8fc87c7acf23f42b5169690b8a1e459048c04f2a2f134c" # T2, falska arkivet


@pytest.fixture(autouse=True)
def fake_repo(tmp_path_factory, monkeypatch):
    """Regeln "inte i repot" prövas mot ett eget repo-stand-in, så att testerna inte beror på var pytest
    lägger tmp_path (mutationskörarens --basetemp ligger i overlayn). Det riktiga repot: test_t11_*."""
    repo = tmp_path_factory.mktemp("repo")
    monkeypatch.setattr(fb, "ROOT", repo)
    return repo


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


# ------------------------------------------------------------------ falskt arkiv
def fake_files() -> dict[str, bytes | list[bytes]]:
    files: dict[str, bytes | list[bytes]] = {}
    for t in fb.targets():
        data = f"not a zip; bytes of {t.path}\n".encode()            # verktyget öppnar aldrig zip-filer
        files[t.url] = data
        files[t.checksum_url] = f"{sha(data)}  {t.name}\n".encode()
    return files


class FakeFetcher:
    """URL -> bytes (eller en lista: ett svar per anrop, sista upprepas); errors: URL -> undantag per anrop."""

    def __init__(self, files=None, errors=None):
        self.files = fake_files() if files is None else files
        self.errors = errors or {}
        self.calls: list[str] = []

    def chunks(self, url):
        self.calls.append(url)
        errs = self.errors.get(url)
        if errs:
            e = errs.pop(0)
            if e is not None:
                raise e
        if url not in self.files:
            raise fb.FetchError(url, "http", 404)
        data = self.files[url]
        if isinstance(data, list):
            data = data.pop(0) if len(data) > 1 else data[0]
        for i in range(0, len(data), 7):
            yield data[i:i + 7]


def run(argv, capsys, *, fetcher=None, git=("c0ffee", True), sleeps=None):
    code = fb.main(argv, fetcher=fetcher or FakeFetcher(), clock=lambda: NOW, git=lambda: git,
                   sleep=(sleeps.append if sleeps is not None else lambda s: None))
    out = json.loads(capsys.readouterr().out)
    assert out["exit_code"] == code
    assert set(out) == {"format", "command", "applied", "exit_code", "context", "result", "result_sha256", "error"}
    return code, out


def tree(root: Path) -> dict[str, tuple]:
    """Innehåll: relativ sökväg -> (typ, läge, sha256 för filer)."""
    out = {}
    for p in [root, *sorted(root.rglob("*"))]:
        st = os.lstat(p)
        kind = "d" if stat.S_ISDIR(st.st_mode) else "l" if stat.S_ISLNK(st.st_mode) else "f"
        out[str(p.relative_to(root))] = (kind, stat.S_IMODE(st.st_mode),
                                         sha(p.read_bytes()) if kind == "f" else None)
    return out


def tree_hash(root: Path) -> str:
    """Strikt: innehåll, läge och mtime för varje post (för "skriver ingenting")."""
    h = hashlib.sha256()
    for p in [root, *sorted(root.rglob("*"))]:
        st = os.lstat(p)
        h.update(f"{p.relative_to(root)}|{st.st_mode:o}|{st.st_mtime_ns}|".encode())
        if stat.S_ISREG(st.st_mode):
            h.update(p.read_bytes())
    return h.hexdigest()


def fetched(tmp_path, capsys, name="arch"):
    root = tmp_path / name
    code, out = run(["fetch", "--root", str(root), "--apply"], capsys)
    assert code == 0, out
    return root


def writable(p: Path):
    os.chmod(p.parent, 0o755)
    os.chmod(p, 0o644)


def sealed_again(p: Path):
    os.chmod(p, 0o444)
    os.chmod(p.parent, 0o555)


# ================================================================== T1 målen
def test_t1_targets_are_exactly_the_scope_of_d_b_a():
    ts = fb.targets()
    assert len(ts) == 405 and len({t.path for t in ts}) == 405
    assert fb.months()[0] == "2020-01" and fb.months()[-1] == "2026-09" and len(fb.months()) == 81
    assert ts[0].url == "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2020-01.zip"
    assert ts[-1].url == ("https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/"
                          "BTCUSDT-fundingRate-2026-09.zip")
    assert all("/monthly/" in t.path and "/daily/" not in t.path for t in ts)
    assert all("2017" not in t.name and "2018" not in t.name and "2019" not in t.name for t in ts)
    assert all(t.checksum_url == t.url + ".CHECKSUM" for t in ts)
    assert sha("\n".join(t.url for t in ts).encode()) == TARGETS_SHA256


# ================================================================== T2 första hämtningen
def test_t2_fetch_into_an_empty_root(tmp_path, capsys):
    root = tmp_path / "arch"
    code, out = run(["fetch", "--root", str(root), "--apply"], capsys)
    assert code == 0 and out["applied"] is True and out["error"] is None
    assert out["context"] == {"code_version": "c0ffee", "worktree_clean": True}
    assert out["result"]["counts"] == {"targets": 405, "originals": 405, "revisions": 0, "missing": 0}
    assert sorted(os.listdir(root)) == sorted(fb.ROOT_ENTRIES)
    assert (root / "ARCHIVE.json").read_bytes() == b'{\n  "format": "btc-quant/binance-archive/v1"\n}\n'
    for t in fb.targets():
        d = root / "objects" / t.path
        assert stat.S_IMODE(os.lstat(d).st_mode) == 0o555
        assert sorted(os.listdir(d)) == [t.name, t.name + ".CHECKSUM"]
        for f in d.iterdir():
            assert stat.S_IMODE(os.lstat(f).st_mode) == 0o444
    assert os.listdir(root / "tmp") == []
    inv = os.listdir(root / "inventory")
    assert inv == [f"{INVENTORY_SHA256}.json"]
    run_rec = json.loads((root / "runs" / "000001.json").read_bytes())
    assert run_rec["started_at"] == NOW.isoformat() and run_rec["inventory_sha256"] == INVENTORY_SHA256
    assert run_rec["requests"] == 810 and run_rec["failures"] == [] and len(run_rec["downloaded"]) == 405
    assert out["result"]["run"]["inventory_sha256"] == INVENTORY_SHA256
    code, out = run(["verify", "--root", str(root)], capsys)
    assert code == 0 and out["result"]["findings"] == [] and out["result"]["inventory_sha256"] == INVENTORY_SHA256


# ================================================================== T3 idempotens
def test_t3_a_second_fetch_makes_no_request_and_writes_only_its_run_record(tmp_path, capsys):
    root = fetched(tmp_path, capsys)
    before = tree(root)
    f = FakeFetcher()
    code, out = run(["fetch", "--root", str(root), "--apply"], capsys, fetcher=f)
    assert code == 0 and f.calls == [] and out["result"]["run"]["requests"] == 0
    after = tree(root)
    assert {k: v for k, v in after.items() if k != "runs/000002.json"} == before
    assert after["runs/000002.json"][0] == "f"
    assert run(["verify", "--root", str(root)], capsys)[0] == 0


# ================================================================== T4 fel checksumma
def _bad_target():
    return fb.targets()[100]


def test_t4_a_checksum_mismatch_is_never_stored(tmp_path, capsys):
    files = fake_files()
    t = _bad_target()
    files[t.url] = b"other bytes\n"
    f = FakeFetcher(files)
    root = tmp_path / "arch"
    code, out = run(["fetch", "--root", str(root), "--apply"], capsys, fetcher=f)
    assert code == 1
    assert not os.path.lexists(root / "objects" / t.path)
    assert f.calls.count(t.url) == 2 and f.calls.count(t.checksum_url) == 2            # ett omförsök
    assert out["result"]["run"]["failures"] == [{"path": t.path, "kind": "checksum_mismatch", "status": None,
                                                 "attempts": 2}]
    assert out["result"]["counts"]["originals"] == 404 and os.listdir(root / "tmp") == []
    code, out = run(["verify", "--root", str(root)], capsys)
    assert code == 1 and [x["code"] for x in out["result"]["findings"]] == ["MISSING"]


def test_t4_a_mismatch_that_clears_on_the_retry_is_stored(tmp_path, capsys):
    files = fake_files()
    t = _bad_target()
    files[t.url] = [b"corrupted in transit\n", files[t.url]]
    root = tmp_path / "arch"
    code, _ = run(["fetch", "--root", str(root), "--apply"], capsys, fetcher=FakeFetcher(files))
    assert code == 0 and (root / "objects" / t.path / t.name).read_bytes() == fake_files()[t.url]


# ================================================================== T5 CHECKSUM-formatet
NAME = "BTCUSDT-1m-2020-01.zip"
GOOD = f"{'a' * 64}  {NAME}"


@pytest.mark.parametrize("text", [f"{'a' * 64}  BTCUSDT-1m-2020-02.zip\n", GOOD + "\nextra\n", GOOD.upper() + "\n",
                                  f"{'a' * 63}  {NAME}\n", f"{'a' * 65}  {NAME}\n", f"{'a' * 64} {NAME}\n",
                                  GOOD + "\n" + " " * 4096, f"{'a' * 64}  {NAME} \n", "\u00e9" + GOOD])
def test_t5_malformed_checksum_files_are_refused(text):
    with pytest.raises(fb.ChecksumFormatError):
        fb.parse_checksum(text.encode("utf-8"), NAME)


def test_t5_well_formed_checksum_files_are_accepted():
    assert fb.parse_checksum((GOOD + "\n").encode(), NAME) == "a" * 64
    assert fb.parse_checksum(GOOD.encode(), NAME) == "a" * 64


def test_t5_a_malformed_checksum_file_fails_its_target_without_retry(tmp_path, capsys):
    files = fake_files()
    t = _bad_target()
    files[t.checksum_url] = f"{sha(b'x')}  wrong-name.zip\n".encode()
    f = FakeFetcher(files)
    root = tmp_path / "arch"
    code, out = run(["fetch", "--root", str(root), "--apply"], capsys, fetcher=f)
    assert code == 1 and f.calls.count(t.checksum_url) == 1 and t.url not in f.calls
    assert out["result"]["run"]["failures"][0]["kind"] == "checksum_format"
    assert not os.path.lexists(root / "objects" / t.path)


# ================================================================== T6 HTTP- och nätverksfel
def test_t6_http_404_is_not_retried(tmp_path, capsys):
    files = fake_files()
    t = _bad_target()
    del files[t.url]
    f, sleeps = FakeFetcher(files), []
    code, out = run(["fetch", "--root", str(tmp_path / "a"), "--apply"], capsys, fetcher=f, sleeps=sleeps)
    assert code == 1 and f.calls.count(t.url) == 1 and sleeps == []
    assert out["result"]["run"]["failures"] == [{"path": t.path, "kind": "http", "status": 404, "attempts": 1}]


def test_t6_5xx_and_network_errors_are_retried_three_times(tmp_path, capsys):
    t = _bad_target()
    errs = {t.url: [fb.FetchError(t.url, "http", 503, retry=True)] * 3}
    f, sleeps = FakeFetcher(errors=errs), []
    code, out = run(["fetch", "--root", str(tmp_path / "a"), "--apply"], capsys, fetcher=f, sleeps=sleeps)
    assert code == 1 and f.calls.count(t.url) == 3 and sleeps == [fb.RETRY_PAUSE_S] * 2
    assert out["result"]["run"]["failures"] == [{"path": t.path, "kind": "http", "status": 503, "attempts": 3}]


def test_t6_a_transient_network_error_recovers(tmp_path, capsys):
    t = _bad_target()
    f = FakeFetcher(errors={t.url: [fb.FetchError(t.url, "network", retry=True), None]})
    root = tmp_path / "a"
    code, _ = run(["fetch", "--root", str(root), "--apply"], capsys, fetcher=f)
    assert code == 0 and f.calls.count(t.url) == 2 and (root / "objects" / t.path).is_dir()


# ================================================================== T7 avbrott
CRASH_POINTS = {
    "1 stage created": ("_stream", "start", 1),
    "2 CHECKSUM written": ("_stream", "start", 2),
    "3 zip written": ("_stream", "after", 2),
    "5 files read-only": ("_publish_dir", "start", 1),
    "6 renamed, not sealed": ("_seal_dir", "start", 1),
    "6 published": ("_publish_inventory", "start", 1),
    "7 inventory written": ("_publish_run", "start", 1),
}
CRASH_CODE = """
import os, sys
sys.path.insert(0, {tests!r})
import test_fetch_binance as T
from pathlib import Path
T.fb.ROOT = Path({repo!r})
name, when, n = {point!r}
orig = getattr(T.fb, name)
count = [0]
def wrapped(*a, **k):
    count[0] += 1
    if count[0] == n and when == "start":
        os._exit(77)
    r = orig(*a, **k)
    if count[0] == n and when == "after":
        os._exit(77)
    return r
setattr(T.fb, name, wrapped)
T.fb.main(["fetch", "--root", {root!r}, "--apply"], fetcher=T.FakeFetcher(), clock=lambda: T.NOW,
          git=lambda: ("c0ffee", True), sleep=lambda s: None)
"""


@pytest.mark.parametrize("point", list(CRASH_POINTS))
def test_t7_a_crash_at_every_write_step_is_recovered(tmp_path, capsys, point, fake_repo):
    root = tmp_path / "arch"
    r = subprocess.run([sys.executable, "-c", CRASH_CODE.format(tests=str(ROOT / "tests"), point=CRASH_POINTS[point],
                                                                root=str(root), repo=str(fake_repo))],
                       capture_output=True, text=True, check=False, cwd=tmp_path)
    assert r.returncode == 77, r.stderr
    code, out = run(["verify", "--root", str(root)], capsys)
    codes = {x["code"] for x in out["result"]["findings"]}
    assert code == 1 and all(fb.CODES[c] == 1 for c in codes), out
    if point.startswith(("1", "2", "3", "5")):
        assert "TMP_LEFTOVER" in codes
    elif point.startswith("6 renamed"):
        assert {"OBJECT_DIR_MODE", "INVENTORY_BEHIND"} <= codes
    else:
        assert "INVENTORY_BEHIND" in codes
    code, out = run(["fetch", "--root", str(root), "--apply"], capsys)
    assert code == 0 and out["result"]["run"]["inventory_sha256"] == INVENTORY_SHA256
    assert run(["verify", "--root", str(root)], capsys)[0] == 0
    assert os.listdir(root / "tmp") == []


# ================================================================== T8 tmp
def test_t8_only_stage_leftovers_are_removed(tmp_path, capsys):
    root = fetched(tmp_path, capsys)
    stage = root / "tmp" / ("stage-" + "a" * 32)
    stage.mkdir()
    (stage / "partial").write_bytes(b"x")
    os.chmod(stage, 0o555)
    assert run(["verify", "--root", str(root)], capsys)[0] == 1
    code, out = run(["fetch", "--root", str(root), "--apply"], capsys)
    assert code == 0 and out["result"]["run"]["tmp_removed"] == [stage.name] and os.listdir(root / "tmp") == []


@pytest.mark.parametrize("kind", ["other_name", "file", "symlink_inside", "subdir_inside", "symlink_stage"])
def test_t8_any_other_tmp_entry_stops_everything(tmp_path, capsys, kind):
    root = fetched(tmp_path, capsys)
    tmp = root / "tmp"
    name = "stage-" + "b" * 32
    if kind == "other_name":
        (tmp / "precious").mkdir()
    elif kind == "file":
        (tmp / name).write_bytes(b"x")
    elif kind == "symlink_inside":
        (tmp / name).mkdir()
        (tmp / name / "link").symlink_to(root / "ARCHIVE.json")
    elif kind == "subdir_inside":
        (tmp / name / "sub").mkdir(parents=True)
    else:
        (tmp_path / "elsewhere").mkdir()
        (tmp / name).symlink_to(tmp_path / "elsewhere")
    before = tree_hash(root)
    f = FakeFetcher()
    assert run(["verify", "--root", str(root)], capsys)[0] == 3
    code, out = run(["fetch", "--root", str(root), "--apply"], capsys, fetcher=f)
    assert code == 3 and f.calls == [] and "TMP_UNKNOWN" in {x["code"] for x in out["result"]["findings"]}
    assert tree_hash(root) == before


# ================================================================== T9 ändrade sparade filer
def _damage(root: Path, kind: str):
    t = fb.targets()[7]
    d = root / "objects" / t.path
    if kind == "zip":
        writable(d / t.name)
        (d / t.name).write_bytes(b"changed\n")
        sealed_again(d / t.name)
    elif kind == "checksum":
        writable(d / (t.name + ".CHECKSUM"))
        (d / (t.name + ".CHECKSUM")).write_bytes(f"{'0' * 64}  {t.name}\n".encode())
        sealed_again(d / (t.name + ".CHECKSUM"))
    elif kind == "mode":
        os.chmod(d, 0o755)
        os.chmod(d / t.name, 0o644)
        os.chmod(d, 0o555)
    elif kind == "extra_file":
        os.chmod(d, 0o755)
        (d / "extra").write_bytes(b"x")
        os.chmod(d, 0o555)
    elif kind == "unknown_object":
        (root / "objects" / "data" / "spot" / "monthly" / "klines" / "BTCUSDT" / "1m" / "BTCUSDT-1m-2019-12.zip").mkdir()
    elif kind == "marker":
        os.chmod(root / "ARCHIVE.json", 0o644)
        (root / "ARCHIVE.json").write_bytes(b"{}\n")
        os.chmod(root / "ARCHIVE.json", 0o444)


@pytest.mark.parametrize("kind", ["zip", "checksum", "mode", "extra_file", "unknown_object", "marker"])
def test_t9_a_changed_stored_file_stops_before_any_request(tmp_path, capsys, kind):
    root = fetched(tmp_path, capsys)
    _damage(root, kind)
    before = tree_hash(root)
    assert run(["verify", "--root", str(root)], capsys)[0] == 3
    f = FakeFetcher()
    code, _ = run(["fetch", "--root", str(root), "--apply"], capsys, fetcher=f)
    assert code == 3 and f.calls == []
    assert run(["recheck", "--root", str(root), "--apply"], capsys, fetcher=f)[0] == 3 and f.calls == []
    assert tree_hash(root) == before


# ================================================================== T10 recheck (D-B-e)
def test_t10_recheck_without_changes(tmp_path, capsys):
    root = fetched(tmp_path, capsys)
    f = FakeFetcher()
    code, out = run(["recheck", "--root", str(root), "--apply"], capsys, fetcher=f)
    assert code == 0 and out["result"]["run"]["changes"] == [] and len(f.calls) == 405
    assert all(u.endswith(".CHECKSUM") for u in f.calls)


def test_t10_a_revised_file_is_stored_separately_and_the_original_is_kept(tmp_path, capsys):
    root = fetched(tmp_path, capsys)
    t = fb.targets()[3]
    orig = tree(root / "objects")
    files = fake_files()
    new = b"revised by Binance\n"
    files[t.url], files[t.checksum_url] = new, f"{sha(new)}  {t.name}\n".encode()
    before_dry = tree_hash(root)
    code, out = run(["recheck", "--root", str(root)], capsys, fetcher=FakeFetcher(files))
    assert code == 2 and out["result"]["changes"] == [{"path": t.path, "change": "REVISION_AVAILABLE",
                                                       "sha256": sha(new)}]
    assert tree_hash(root) == before_dry                                              # utan --apply: ingenting
    code, out = run(["recheck", "--root", str(root), "--apply"], capsys, fetcher=FakeFetcher(files))
    assert code == 2 and out["result"]["run"]["changes"] == [{"path": t.path, "change": "REVISION_STORED",
                                                              "sha256": sha(new)}]
    rev = root / "revisions" / t.path / sha(new)
    assert (rev / t.name).read_bytes() == new and stat.S_IMODE(os.lstat(rev).st_mode) == 0o555
    assert tree(root / "objects") == orig
    assert out["result"]["counts"]["revisions"] == 1
    code, out = run(["verify", "--root", str(root)], capsys)
    assert code == 0
    snap = tree(root)
    f = FakeFetcher(files)
    code, out = run(["recheck", "--root", str(root), "--apply"], capsys, fetcher=f)
    assert code == 2 and out["result"]["run"]["changes"][0]["change"] == "REVISION_KNOWN"
    assert t.url not in f.calls
    assert {k: v for k, v in tree(root).items() if not k.startswith("runs/")} == \
           {k: v for k, v in snap.items() if not k.startswith("runs/")}


def test_t10_a_changed_checksum_file_with_the_same_value_is_reported_only(tmp_path, capsys):
    root = fetched(tmp_path, capsys)
    t = fb.targets()[5]
    files = fake_files()
    files[t.checksum_url] = files[t.checksum_url].rstrip(b"\n")                      # andra bytes, samma värde
    before = tree(root)
    code, out = run(["recheck", "--root", str(root), "--apply"], capsys, fetcher=FakeFetcher(files))
    assert code == 2 and out["result"]["run"]["changes"] == [
        {"path": t.path, "change": "CHECKSUM_FILE_CHANGED", "sha256": sha(fake_files()[t.url])}]
    assert {k: v for k, v in tree(root).items() if not k.startswith("runs/")} == \
           {k: v for k, v in before.items() if not k.startswith("runs/")}


# ================================================================== T11 rotens säkerhet
def test_t11_an_existing_directory_without_marker_is_never_used(tmp_path, capsys):
    raw = tmp_path / "raw"
    (raw / "binance").mkdir(parents=True)
    (raw / "binance" / "x.zip").write_bytes(b"keep")
    empty = tmp_path / "empty"
    empty.mkdir()
    for d in (raw, empty):
        before = tree_hash(d)
        f = FakeFetcher()
        code, out = run(["fetch", "--root", str(d), "--apply"], capsys, fetcher=f)
        assert code == 5 and out["error"]["type"] == "RefusedError" and f.calls == []
        assert str(tmp_path) not in out["error"]["message"]
        assert tree_hash(d) == before


def test_t11_symlinked_root_root_in_the_repo_and_missing_parent_are_refused(tmp_path, capsys):
    real = fetched(tmp_path, capsys, "real")
    (tmp_path / "link").symlink_to(real)
    for root in (tmp_path / "link", fb.ROOT / "never_created", fb.ROOT, tmp_path / "no" / "parent"):
        code, _ = run(["fetch", "--root", str(root), "--apply"], capsys)
        assert code == 5
    assert not (fb.ROOT / "never_created").exists() and not (tmp_path / "no").exists()


def test_t11_the_real_repository_is_refused():
    for root in (ROOT, ROOT / "never_created", ROOT / "scripts" / "data"):
        with pytest.raises(fb.RefusedError):
            fb.check_root(root, ROOT)
    assert not (ROOT / "never_created").exists()


def test_t11_an_invalid_marker_is_an_error(tmp_path, capsys):
    root = tmp_path / "arch"
    root.mkdir()
    (root / "ARCHIVE.json").write_bytes(b'{"format": "something else"}\n')
    before = tree_hash(root)
    for argv in (["fetch", "--root", str(root), "--apply"], ["verify", "--root", str(root)],
                 ["plan", "--root", str(root)], ["recheck", "--root", str(root)]):
        code, out = run(argv, capsys)
        assert code == 3 and out["result"]["findings"][0]["code"] == "MARKER_INVALID"
    assert tree_hash(root) == before


# ================================================================== T12 låset
def test_t12_a_held_lock_refuses_the_run(tmp_path, capsys):
    root = fetched(tmp_path, capsys)
    fd = os.open(root / ".lock", os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        before = tree_hash(root)
        f = FakeFetcher()
        code, _ = run(["fetch", "--root", str(root), "--apply"], capsys, fetcher=f)
        assert code == 5 and f.calls == [] and tree_hash(root) == before
    finally:
        os.close(fd)


# ================================================================== T13 HTTP-lagret
class _Handler(http.server.BaseHTTPRequestHandler):
    hits: ClassVar[list[str]] = []

    def do_GET(self):
        _Handler.hits.append(self.path)
        if self.path == "/ok":
            body = b"0123456789" * 10
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/ok")
            self.end_headers()
        else:
            self.send_response(int(self.path.strip("/")) if self.path.strip("/").isdigit() else 404)
            self.send_header("Content-Length", "0")
            self.end_headers()

    def log_message(self, *a):
        pass


@pytest.fixture
def server():
    srv = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    _Handler.hits = []
    yield srv
    srv.shutdown()
    srv.server_close()


def test_t13_the_http_fetcher_reads_from_the_allowed_host(server, monkeypatch):
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")                      # ignoreras
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.delenv("NO_PROXY", raising=False)
    port = server.server_port
    f = fb.HttpFetcher(netloc=f"127.0.0.1:{port}", scheme="http")
    assert fb._fetch_bytes(f, f"http://127.0.0.1:{port}/ok", 1000) == b"0123456789" * 10


def test_t13_other_hosts_and_schemes_are_refused_before_connecting(server):
    port = server.server_port
    f = fb.HttpFetcher(netloc=f"127.0.0.1:{port}", scheme="http")
    for url in (f"http://localhost:{port}/ok", f"https://127.0.0.1:{port}/ok", f"http://127.0.0.1:{port}/ok?x=1"):
        with pytest.raises(fb.FetchError) as e:
            fb._fetch_bytes(f, url, 1000)
        assert e.value.kind == "refused_url"
    assert _Handler.hits == []
    d = fb.HttpFetcher()
    for url in ("http://data.binance.vision/x", "https://data.binance.vision.evil.example/x",
                "https://other.example/data.binance.vision/x"):
        with pytest.raises(fb.FetchError) as e:
            fb._fetch_bytes(d, url, 1000)
        assert e.value.kind == "refused_url"


def test_t13_redirects_are_never_followed(server):
    port = server.server_port
    f = fb.HttpFetcher(netloc=f"127.0.0.1:{port}", scheme="http")
    with pytest.raises(fb.FetchError) as e:
        fb._fetch_bytes(f, f"http://127.0.0.1:{port}/redirect", 1000)
    assert (e.value.kind, e.value.status, e.value.retry) == ("http", 302, False)
    assert _Handler.hits == ["/redirect"]


def test_t13_size_limit_and_status_codes(server, tmp_path):
    port = server.server_port
    f = fb.HttpFetcher(netloc=f"127.0.0.1:{port}", scheme="http")
    with pytest.raises(fb.FetchError) as e:
        fb._stream(f, f"http://127.0.0.1:{port}/ok", tmp_path / "out", 50)
    assert e.value.kind == "too_large"
    for status, retry in ((404, False), (403, False), (500, True), (503, True)):
        with pytest.raises(fb.FetchError) as e:
            fb._fetch_bytes(f, f"http://127.0.0.1:{port}/{status}", 1000)
        assert (e.value.kind, e.value.status, e.value.retry) == ("http", status, retry)


def test_t13_a_connection_error_is_a_retryable_network_error():
    f = fb.HttpFetcher(netloc="127.0.0.1:1", scheme="http", timeout=5)
    with pytest.raises(fb.FetchError) as e:
        fb._fetch_bytes(f, "http://127.0.0.1:1/x", 1000)
    assert (e.value.kind, e.value.retry) == ("network", True)


# ================================================================== T14 skrivfria kommandon
def test_t14_dry_runs_plan_and_verify_write_nothing(tmp_path, capsys):
    root = tmp_path / "arch"
    code, out = run(["fetch", "--root", str(root)], capsys)
    assert code == 1 and out["applied"] is False and not root.exists()
    assert out["result"]["root_exists"] is False and len(out["result"]["findings"]) == 405
    root = fetched(tmp_path, capsys)
    before = tree_hash(root)
    f = FakeFetcher()
    for argv in (["plan", "--root", str(root)], ["verify", "--root", str(root)], ["fetch", "--root", str(root)],
                 ["recheck", "--root", str(root)]):
        assert run(argv, capsys, fetcher=f)[0] == 0
    assert tree_hash(root) == before
    assert all(u.endswith(".CHECKSUM") for u in f.calls) and len(f.calls) == 405      # bara recheck


# ================================================================== T15 CLI
def test_t15_usage_errors_and_abbreviations(tmp_path, capsys):
    for argv in ([], ["fetch"], ["fetch", "--root", str(tmp_path / "a"), "--app"], ["verify", "--root", "x", "--apply"],
                 ["unknown", "--root", "x"]):
        code, out = run(argv, capsys)
        assert code == 64 and out["error"]["type"] == "UsageError" and out["command"] is None
    assert not (tmp_path / "a").exists()


@pytest.mark.parametrize("git", [("c0ffee", False), ("unknown", None)])
def test_t15_apply_requires_a_clean_worktree(tmp_path, capsys, git):
    root = tmp_path / "arch"
    f = FakeFetcher()
    code, _ = run(["fetch", "--root", str(root), "--apply"], capsys, fetcher=f, git=git)
    assert code == 5 and not root.exists() and f.calls == []
    root = fetched(tmp_path, capsys, "b")
    before = tree_hash(root)
    assert run(["recheck", "--root", str(root), "--apply"], capsys, fetcher=f, git=git)[0] == 5
    assert tree_hash(root) == before and f.calls == []


def test_t15_verify_of_a_missing_root_is_refused(tmp_path, capsys):
    assert run(["verify", "--root", str(tmp_path / "none")], capsys)[0] == 5


def test_t15_the_result_hash_covers_the_result(tmp_path, capsys):
    root = fetched(tmp_path, capsys)
    _, out = run(["verify", "--root", str(root)], capsys)
    assert out["result_sha256"] == sha(fb.canonical_json_bytes(out["result"]))


# ================================================================== T16 förteckningen
def test_t16_the_inventory_is_deterministic_sorted_and_timeless(tmp_path, capsys):
    a, b = fetched(tmp_path, capsys, "a"), fetched(tmp_path, capsys, "b")
    ia, ib = (root / "inventory" / f"{INVENTORY_SHA256}.json" for root in (a, b))
    assert ia.read_bytes() == ib.read_bytes()
    doc = json.loads(ia.read_bytes())
    entries = doc["entries"]
    assert [e["path"] for e in entries] == sorted(t.path for t in fb.targets())
    assert set(entries[0]) == {"path", "series", "url", "kind", "size", "sha256", "checksum", "checksum_file_sha256"}
    assert all(e["sha256"] == e["checksum"] for e in entries)
    text = ia.read_text()
    assert "2030" not in text and str(tmp_path) not in text and "c0ffee" not in text
    assert doc["counts"] == {"originals": 405, "revisions": 0, "missing": 0}
