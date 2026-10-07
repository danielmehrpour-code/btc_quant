"""Etapp B steg 1: dokumentverktygen i scripts/docs/ (P4-S13-b, D-B-f).

Säkerhetsreglerna för run_examples (--scratch, märkt arbetskatalog), att COMMANDS är exakt runbookens
kommandon i docs/PHASE4_DATASTORE.md avsnitt 9 (statiskt, ingen körning), och check_refs-tolkaren på
fast text. Ingen test löser `fil:rad`-referenser mot koden (P4-S9-f).
"""
import importlib.util
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "PHASE4_DATASTORE.md"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"docs_tool_{name}_under_test", ROOT / "scripts/docs" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


rx = _load("run_examples")
cr = _load("check_refs")


# ================================================================== run_examples: säkerhet
@pytest.mark.parametrize("where", ["/", "home", "repo", "inside_repo", "data", "inside_data"])
def test_scratch_is_refused(tmp_path, monkeypatch, where):
    home, repo, data = tmp_path / "home", tmp_path / "home/btc_quant", tmp_path / "home/btc_data"
    for d in (repo, data):
        d.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: home)
    scratch = {"/": Path("/"), "home": home, "repo": repo, "inside_repo": repo / "x",
               "data": data, "inside_data": data / "x"}[where]
    with pytest.raises(rx.ExamplesSafetyError):
        rx.check_scratch(scratch, repo, data)
    with pytest.raises(rx.ExamplesSafetyError):
        rx.prepare_work(scratch, repo, data)
    assert not (scratch / rx.WORK_NAME).exists()


def test_symlinked_scratch_into_data_is_refused(tmp_path):
    repo, data = tmp_path / "repo", tmp_path / "btc_data"
    repo.mkdir()
    data.mkdir()
    (tmp_path / "link").symlink_to(data)
    with pytest.raises(rx.ExamplesSafetyError):
        rx.check_scratch(tmp_path / "link", repo, data)


def test_the_real_repository_and_btc_data_are_refused():
    with pytest.raises(rx.ExamplesSafetyError):
        rx.check_scratch(rx.ROOT)
    with pytest.raises(rx.ExamplesSafetyError):
        rx.check_scratch(rx.BTC_DATA / "x")


def _sandbox(tmp_path):
    repo, data, scratch = tmp_path / "repo", tmp_path / "btc_data", tmp_path / "scratch"
    for d in (repo, data, scratch):
        d.mkdir()
    return repo, data, scratch


def test_prepare_work_creates_a_marked_directory_and_replaces_only_a_marked_one(tmp_path):
    repo, data, scratch = _sandbox(tmp_path)
    work = rx.prepare_work(scratch, repo, data)
    assert work == scratch / rx.WORK_NAME and (work / rx.MARKER).is_file()
    (work / "old").mkdir(mode=0o555)                                    # som ett store-objekt (0555)
    again = rx.prepare_work(scratch, repo, data)
    assert again == work and sorted(p.name for p in work.iterdir()) == [rx.MARKER]


@pytest.mark.parametrize("kind", ["no_marker", "marker_is_dir", "marker_is_symlink", "work_is_symlink", "work_is_file"])
def test_an_unmarked_work_directory_is_never_removed(tmp_path, kind):
    repo, data, scratch = _sandbox(tmp_path)
    work = scratch / rx.WORK_NAME
    keep = tmp_path / "keep"
    keep.mkdir()
    (keep / "precious").write_text("x")
    (keep / rx.MARKER).write_text("x")
    if kind == "work_is_symlink":
        work.symlink_to(keep)
    elif kind == "work_is_file":
        work.write_text("x")
    else:
        work.mkdir()
        (work / "precious").write_text("x")
        if kind == "marker_is_dir":
            (work / rx.MARKER).mkdir()
        elif kind == "marker_is_symlink":
            (work / rx.MARKER).symlink_to(keep / rx.MARKER)
    with pytest.raises(rx.ExamplesSafetyError):
        rx.prepare_work(scratch, repo, data)
    assert (keep / "precious").is_file()
    if kind in ("no_marker", "marker_is_dir", "marker_is_symlink"):
        assert (work / "precious").is_file()


def _git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout.strip()


def _repo_with_one_commit(path: Path) -> Path:
    path.mkdir()
    _git(path, "init", "-q")
    (path / "a.txt").write_text("a\n")
    _git(path, "add", "a.txt")
    _git(path, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "c")
    return path


def test_choose_repo_uses_a_clean_repo_itself(tmp_path):
    repo = _repo_with_one_commit(tmp_path / "repo")
    assert rx.choose_repo(tmp_path, repo) == (repo, _git(repo, "rev-parse", "HEAD"))
    assert not (tmp_path / "clone").exists()


def test_choose_repo_clones_head_without_hard_links_when_dirty(tmp_path):
    repo = _repo_with_one_commit(tmp_path / "repo")
    (repo / "a.txt").write_text("changed\n")
    work = tmp_path / "work"
    work.mkdir()
    target, head = rx.choose_repo(work, repo)
    assert target == work / "clone" and head == _git(repo, "rev-parse", "HEAD")
    assert (target / "a.txt").read_text() == "a\n" and _git(target, "status", "--porcelain") == ""
    repo_inodes = {p.stat().st_ino for p in (repo / ".git").rglob("*") if p.is_file()}
    assert not repo_inodes & {p.stat().st_ino for p in (target / ".git").rglob("*") if p.is_file()}


def test_check_package_requires_the_package_from_the_given_repo(tmp_path):
    repo = tmp_path / "repo"
    (repo / "btc_trading_system").mkdir(parents=True)
    (repo / "btc_trading_system" / "__init__.py").write_text("")
    rx.check_package(repo, tmp_path)
    with pytest.raises(RuntimeError):
        rx.check_package(tmp_path / "empty", tmp_path)                 # faller tillbaka på en annan installation


# ================================================================== run_examples: runbooken
def runbook_commands() -> tuple[set[tuple[str, int]], list[str]]:
    """((kommando, exitkod) för varje $DS-rad med exakt en exitkod, $DS-rader utan en sådan)."""
    text = DOC.read_text(encoding="utf-8")
    start = text.index("\n## 9. ")
    sec = text[start:text.index("\n## 10. ", start)]
    pairs, other = set(), []
    for line in sec.splitlines():
        if not line.startswith("$DS"):
            continue
        cmd, _, comment = line.partition("#")
        m = re.match(r" exit (\d+)(?:$|[,:])", comment)
        if m:
            pairs.add((" ".join(cmd.split()), int(m.group(1))))
        else:
            other.append(" ".join(line.split()))
    return pairs, other


def test_commands_equal_the_runbook_in_section_9():
    pairs, other = runbook_commands()
    examples = {(" ".join(cmd.split()), code) for cmd, code in rx.COMMANDS.values()}
    assert examples == pairs
    assert other == ['$DS versions --store "$STORE" --dataset binance/spot/BTCUSDT/15m # pick DV',
                     '$DS verify --store "$STORE" # exit 0 / 1 / 2 / 3']


def test_commands_are_numbered_and_normalised():
    assert list(rx.COMMANDS) == [f"R{i:02d}" for i in range(1, 33)]
    for cmd, _ in rx.COMMANDS.values():
        assert cmd == " ".join(cmd.split()) and cmd.startswith("$DS")


# ================================================================== check_refs: tolkaren
CANNED = """\
| X | y | `versioned_store.py:12`, `:30-31` | `btc_trading_system/database/cli.py:5-7` |
Text `docs/PHASE4_DATASTORE.md:3` and `configs/default.yaml:21`, not `foo.txt:1` or `x.py`.
Decisions P4-S6b-i, P4-S13-b, G-24, P1-05, U-09, V3-A and twice P1-05; not PX-01 or G-1.
"""


def test_code_refs_are_parsed_with_continuations_and_bare_module_names():
    assert cr.code_refs(CANNED) == [
        ("btc_trading_system/database/versioned_store.py", 12, 12),
        ("btc_trading_system/database/versioned_store.py", 30, 31),
        ("btc_trading_system/database/cli.py", 5, 7),
        ("docs/PHASE4_DATASTORE.md", 3, 3),
        ("configs/default.yaml", 21, 21),
    ]


def test_decision_ids_are_found_and_looked_up():
    assert cr.decision_ids(CANNED) == {"P4-S6b-i", "P4-S13-b", "G-24", "P1-05", "U-09", "V3-A"}
    known = cr.known_decisions("| G-24 | All | x |\n| P1-05 | 1 | y |\n| not | a | row\n")
    assert known == {"G-24", "P1-05"}
    assert cr.decision_ids(CANNED) - known == {"P4-S6b-i", "P4-S13-b", "U-09", "V3-A"}
