"""PHASE 4 steg 11: mutationskörarens säkerhet, mönsterkontroll och mängdernas form (scripts/mutation/)."""
import hashlib
import importlib.util
import os
import stat
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("mutation_runner_under_test", ROOT / "scripts/mutation/runner.py")
assert _SPEC is not None and _SPEC.loader is not None
runner = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(runner)


def tree_hash(root: Path) -> str:
    hh = hashlib.sha256()
    for p in [root, *sorted(root.rglob("*"))]:
        st = os.lstat(p)
        hh.update(f"{p.relative_to(root)}|{st.st_mode:o}|{st.st_mtime_ns}|".encode())
        if stat.S_ISREG(st.st_mode):
            hh.update(p.read_bytes())
        elif stat.S_ISLNK(st.st_mode):
            hh.update(os.readlink(p).encode())
    return hh.hexdigest()


def overlay(path: Path) -> Path:
    path.mkdir(parents=True)
    (path / runner.MARKER).write_text("abc\n")
    return path


# ================================================================== säkerhet
def _repo_itself(s, repo):
    return repo


def _inside_repo(s, repo):
    return overlay(repo / "sub")


def _contains_repo(s, repo):
    o = overlay(s / "outer")
    (o / "repo").mkdir()
    return o


def _outside_scratch(s, repo):
    return overlay(s.parent / "elsewhere")


def _scratch_itself(s, repo):
    (s / runner.MARKER).write_text("abc\n")
    return s


def _no_marker(s, repo):
    o = s / "plain"
    o.mkdir()
    return o


def _marker_is_symlink(s, repo):
    o = s / "linked-marker"
    o.mkdir()
    (s / "marker-target").write_text("abc\n")
    (o / runner.MARKER).symlink_to(s / "marker-target")
    return o


def _has_git(s, repo):
    o = overlay(s / "with-git")
    (o / ".git").mkdir()
    return o


def _symlink_into_repo(s, repo):
    (s / "link").symlink_to(repo)
    return s / "link"


UNSAFE = {"repo_itself": _repo_itself, "inside_repo": _inside_repo, "contains_repo": _contains_repo,
          "outside_scratch": _outside_scratch, "scratch_itself": _scratch_itself, "no_marker": _no_marker,
          "marker_is_symlink": _marker_is_symlink, "has_git": _has_git, "symlink_into_repo": _symlink_into_repo}


@pytest.fixture
def area(tmp_path):
    """(scratch, repo): en scratch-yta och ett separat låtsasrepo, båda under tmp_path."""
    scratch, repo = tmp_path / "scratch", tmp_path / "repo"
    scratch.mkdir()
    repo.mkdir()
    (repo / ".git").mkdir()
    return scratch, repo


@pytest.mark.parametrize("case", sorted(UNSAFE))
def test_unsafe_overlays_are_refused_and_nothing_is_written(area, case):
    scratch, repo = area
    target = UNSAFE[case](scratch, repo)
    if case == "contains_repo":                            # repot ligger i overlayn
        repo = target / "repo"
    before = tree_hash(scratch.parent)
    with pytest.raises(runner.MutationSafetyError):
        runner.check_overlay(target, scratch, repo)
    assert tree_hash(scratch.parent) == before


def test_unsafe_scratch_roots_are_refused(area):
    _, repo = area
    for bad in (repo, repo / "inner", Path("/"), Path.home()):
        with pytest.raises(runner.MutationSafetyError):
            runner.check_scratch(bad, repo)
    with pytest.raises(runner.MutationSafetyError):
        runner.check_overlay(overlay(repo / "inner" / "ov"), repo / "inner", repo)


def test_a_proper_overlay_is_accepted(area):
    scratch, repo = area
    o = overlay(scratch / "mut-x")
    assert runner.check_overlay(o, scratch, repo) == Path(os.path.realpath(o))
    assert runner.check_scratch(scratch, repo) == Path(os.path.realpath(scratch))


def test_the_runner_refuses_the_real_repository():
    with pytest.raises(runner.MutationSafetyError):
        runner.check_scratch(ROOT)
    with pytest.raises(runner.MutationSafetyError):
        runner.check_overlay(ROOT, ROOT.parent)


# ================================================================== mängderna
def test_every_pattern_occurs_exactly_once_in_the_repository():
    """Mönstervakt (P4-S11-f): en kodändring som bryter ett harness-mönster fallerar här direkt."""
    assert runner.pattern_problems(ROOT, runner.all_sets()) == []


def test_sets_are_well_formed():
    sets = runner.all_sets()
    assert [m.NAME for m in sets] == list(runner.SET_ORDER)
    assert set(runner.MINUTES) == set(runner.SET_ORDER)
    for mod in sets:
        assert mod.MUTATIONS and mod.TESTS, mod.NAME
        for f in runner.test_files(mod):
            assert (ROOT / f).is_file(), (mod.NAME, f)
        for name, edits in mod.MUTATIONS.items():
            assert edits, name
            for rel, old, new in edits:
                assert (ROOT / rel).is_file() and old != new, (mod.NAME, name)


def test_every_spec_item_maps_to_existing_mutations():
    spec = (ROOT / "docs/PHASE4_SPEC.md").read_text(encoding="utf-8")
    block = spec[spec.index("MUTATION TESTS"):spec.index("REAL-DATA MIGRATION")]
    items = [line[2:].strip() for line in block.splitlines() if line.startswith("- ")]
    assert sorted(items) == sorted(runner.SPEC_MAP)
    names = {(m.NAME, n) for m in runner.all_sets() for n in m.MUTATIONS}
    for item, refs in runner.SPEC_MAP.items():
        assert refs and all(ref in names for ref in refs), item


# ================================================================== utdata
def test_killers_are_parsed_whole():
    out = ("..F\n"
           "FAILED tests/test_database_cli.py::test_lock[snapshot create] - assert 0 == 5\n"
           "FAILED tests/test_database_refs.py::test_a - AssertionError: x - y\n"
           "ERROR tests/test_database_ingest.py::test_b[x-1]\n"
           "ERROR tests/test_database_legacy.py - ImportError: boom\n"
           "FAILED tests/test_database_cli.py::test_lock[snapshot create] - assert 0 == 5\n"
           "1 failed, 2 passed in 0.1s\n")
    assert runner.parse_killers(out) == ["test_database_cli::test_lock[snapshot create]",
                                         "test_database_ingest::test_b[x-1]", "test_database_legacy",
                                         "test_database_refs::test_a"]
