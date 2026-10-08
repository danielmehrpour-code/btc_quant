"""Gemensam körare för Phase 4:s mutationstester (PHASE 4, steg 11).

Användning:  python scripts/mutation/run.py --scratch DIR [--sets all|step2,step5,...] [--commit REV]
                                            [--parallel N] [--only PREFIX] [--check]

OVERLAY: varje mängd (sets/<namn>.py) körs i en egen, ny katalog DIR/mut-<mängd>-<uuid>/ som fylls med
`git archive <commit>` ur detta repo och märks med filen .mutation-overlay. En mutation i taget:
exakt textersättning (mönstret måste finnas exakt en gång), testerna körs, filen återställs.

SÄKERHET (MutationSafetyError, ingenting skrivs): kontrolleras när overlayn skapas och igen före varje
mutation. --scratch får inte ligga i repots arbetskatalog och får inte vara hemkatalogen eller /.
Overlayns verkliga sökväg måste ligga i --scratch, får inte vara repots arbetskatalog, ligga i den
eller innehålla den, får inte innehålla .git och måste bära märkfilen.

UTDATA (en fil per mängd i körkatalogen DIR/mutation-run-<commit>-<tid>/):
  === SET <mängd> on <commit> (<n> mutations)
  === BASELINE (unmutated overlay): <pytest-sammanfattning>
  === MUTATION <namn>: KILLED|SURVIVED (<pytest-sammanfattning>)
     killed by: <testfil>::<test-id>
  KILLED <k>/<n>
  overlay restored: True|False
plus summary.json och summary.txt. Exit 0 endast om varje baslinje är grön, varje mutation dödad och
varje overlay återställd.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SETS_DIR = Path(__file__).resolve().parent / "sets"
MARKER = ".mutation-overlay"
ENV = {"PYTHONDONTWRITEBYTECODE": "1", "PATH": "/usr/bin:/bin"}
SET_ORDER = ("step2", "step3", "step4", "step5", "step6", "step6b", "step7", "step8", "step10", "step11", "stageb2", "stageb3")
# Uppmätt körtid i minuter (senaste körningarna); styr ordningen "längst först" i parallellt läge.
MINUTES = {"step10": 41.1, "step6b": 38.5, "step6": 34.9, "step5": 27.9, "step7": 4.2, "step8": 3.9,
           "step4": 3.5, "step3": 3.2, "step11": 3.0, "step2": 1.1, "stageb2": 34.6, "stageb3": 0.2}
# docs/PHASE4_SPEC.md "MUTATION TESTS" -> mutationer som bevisar punkten (mängd, mutationsnamn).
SPEC_MAP: dict[str, tuple[tuple[str, str], ...]] = {
    "skipped SHA verification": (("step2", "4 skip data file sha256 check"),
                                 ("step5", "1 verify trusts version.json instead of hashing (SHA bypassed)")),
    "attempted immutable-object overwrite": (("step2", "5 remove target-exists guard"),
                                             ("step3", "7 snapshot publish overwrites (replace, not link)")),
    "ref publication before durable object publication": (
        ("step11", "N3 ingest publishes the ref before the object"),
        ("step11", "N4 object files not fsynced before the rename"),
        ("step3", "2 set_current skips object verification")),
    "PinnedStore incorrectly following current ref": (("step3", "3 PinnedStore.read follows current ref"),
                                                      ("step7", "8 PinnedStore.read_range follows the current ref")),
    "range-read boundary off-by-one": (("step7", "1 start off-by-one (bar at start dropped)"),
                                       ("step7", "2 end off-by-one (bar at end included)")),
    "foreign keys disabled": (("step11", "N1 foreign keys disabled"),),
    "unsorted snapshot members": (("step3", "6 snapshot members unsorted (input order)"),),
    "lock bypass": (("step11", "N2 writer lock does not flock"),
                    ("step6", "10 writer lock not taken")),
    "skipping verification of an existing object": (("step4", "7 reused object not verified before the ref moves"),
                                                     ("step6", ("11 existing object reused without full verification "
                                                                "(sha256 bypassed)"))),
    "future data exposed through as_of": (("step2", "2 drop - TIMEFRAMES[timeframe]"),
                                          ("step7", "4 as_of future leak (as_of used instead of causal_cutoff)")),
}
_KILLER = re.compile(r"^(?:FAILED|ERROR) tests/(?P<file>[^\s:]+?)\.py(?:::(?P<test>.+?))?(?: - .*)?$")


class MutationSafetyError(Exception):
    """Overlay- eller scratch-sökvägen är inte tillåten; ingenting har skrivits."""


# ------------------------------------------------------------------ säkerhet
def check_scratch(scratch: Path, repo: Path = ROOT) -> Path:
    s, r = Path(os.path.realpath(scratch)), Path(os.path.realpath(repo))
    if s in (Path("/"), Path(os.path.realpath(Path.home()))):
        raise MutationSafetyError(f"--scratch får inte vara {s}")
    if s == r or r in s.parents:
        raise MutationSafetyError(f"--scratch {s} ligger i repots arbetskatalog {r}")
    return s


def check_overlay(overlay: Path, scratch: Path, repo: Path = ROOT) -> Path:
    s = check_scratch(scratch, repo)
    o, r = Path(os.path.realpath(overlay)), Path(os.path.realpath(repo))
    if s not in o.parents:
        raise MutationSafetyError(f"overlayn {o} ligger inte i --scratch {s}")
    if o == r or r in o.parents or o in r.parents:
        raise MutationSafetyError(f"overlayn {o} är, ligger i eller innehåller repots arbetskatalog {r}")
    if os.path.lexists(o / ".git"):
        raise MutationSafetyError(f"overlayn {o} innehåller .git")
    if not (o / MARKER).is_file() or (o / MARKER).is_symlink():
        raise MutationSafetyError(f"overlayn {o} saknar märkfilen {MARKER}")
    return o


# ------------------------------------------------------------------ mängder
def load_set(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"mutation_set_{name}", SETS_DIR / f"{name}.py")
    if spec is None or spec.loader is None:
        raise ValueError(f"okänd mängd: {name}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def all_sets() -> list[ModuleType]:
    return [load_set(n) for n in SET_ORDER]


def test_files(mod: ModuleType) -> list[str]:
    return sorted({t.split("::")[0] for t in mod.TESTS})


def pattern_problems(root: Path, sets: list[ModuleType]) -> list[str]:
    """Varje mönster finns exakt en gång i `root`; old != new; varje muterad fil kompilerar."""
    problems = []
    for mod in sets:
        for name, edits in mod.MUTATIONS.items():
            text: dict[str, str] = {}
            for rel, old, new in edits:
                if rel not in text:
                    text[rel] = (root / rel).read_text(encoding="utf-8")
                n = text[rel].count(old)
                if n != 1 or old == new:
                    problems.append(f"{mod.NAME} {name}: mönstret finns {n} gånger i {rel}" if n != 1
                                    else f"{mod.NAME} {name}: old == new")
                    continue
                text[rel] = text[rel].replace(old, new)
            for rel, src in text.items():
                try:
                    compile(src, rel, "exec")
                except SyntaxError as e:
                    problems.append(f"{mod.NAME} {name}: {rel} kompilerar inte: {e}")
    return problems


def parse_killers(stdout: str) -> list[str]:
    out = set()
    for line in stdout.splitlines():
        m = _KILLER.match(line)
        if m:
            out.add(m["file"] + (f"::{m['test']}" if m["test"] else ""))
    return sorted(out)


# ------------------------------------------------------------------ körning
def make_overlay(scratch: Path, commit: str, name: str, repo: Path = ROOT) -> Path:
    s = check_scratch(scratch, repo)
    d = s / f"mut-{name}-{uuid.uuid4().hex}"
    d.mkdir()
    archive = subprocess.run(["git", "-C", str(repo), "archive", commit], capture_output=True, check=True)
    subprocess.run(["tar", "-x", "-C", str(d)], input=archive.stdout, check=True)
    (d / MARKER).write_text(commit + "\n", encoding="utf-8")
    return check_overlay(d, s, repo)


def run_pytest(overlay: Path, tests: tuple[str, ...], n: int) -> tuple[list[str], str]:
    r = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-rfE",
                        f"--basetemp={overlay / f'.bt-{n}'}", *tests], cwd=overlay, capture_output=True,
                       text=True, timeout=3600, env=ENV, check=False)
    lines = r.stdout.strip().splitlines()
    return parse_killers(r.stdout), (lines[-1] if lines else r.stderr.strip()[-300:])


def run_set(mod: ModuleType, scratch: Path, commit: str, run_dir: Path, only: str = "") -> dict[str, Any]:
    t0 = time.monotonic()
    overlay = make_overlay(scratch, commit, mod.NAME)
    out = (run_dir / f"{mod.NAME}.txt").open("w", encoding="utf-8")

    def emit(line: str) -> None:
        out.write(line + "\n")
        out.flush()

    selected = {k: v for k, v in mod.MUTATIONS.items() if k.startswith(only)}
    emit(f"=== SET {mod.NAME} on {commit} ({len(selected)} mutations)")
    files = sorted({rel for edits in selected.values() for rel, _, _ in edits})
    orig = {rel: (overlay / rel).read_text(encoding="utf-8") for rel in files}
    result: dict[str, Any] = {"set": mod.NAME, "commit": commit, "tests": list(mod.TESTS), "mutations": []}
    base_killers, base_summary = run_pytest(overlay, mod.TESTS, 0)
    green = not base_killers and " passed" in base_summary and "failed" not in base_summary
    emit(f"=== BASELINE (unmutated overlay): {base_summary}")
    result.update(baseline=base_summary, baseline_green=green, baseline_killers=base_killers)
    try:
        if green:
            for i, (name, edits) in enumerate(selected.items(), start=1):
                check_overlay(overlay, scratch)
                text = dict(orig)
                for rel, old, new in edits:
                    if text[rel].count(old) != 1:
                        raise ValueError(f"{mod.NAME} {name}: mönstret finns {text[rel].count(old)} gånger i {rel}")
                    text[rel] = text[rel].replace(old, new)
                for rel, src in text.items():
                    compile(src, rel, "exec")
                    (overlay / rel).write_text(src, encoding="utf-8")
                t1 = time.monotonic()
                killers, summary = run_pytest(overlay, mod.TESTS, i)
                for rel, src in orig.items():
                    (overlay / rel).write_text(src, encoding="utf-8")
                emit(f"=== MUTATION {name}: {'KILLED' if killers else 'SURVIVED'} ({summary})")
                for k in killers:
                    emit(f"   killed by: {k}")
                result["mutations"].append({"name": name, "killed": bool(killers), "killers": killers,
                                            "summary": summary, "seconds": round(time.monotonic() - t1, 1)})
    finally:
        for rel, src in orig.items():
            (overlay / rel).write_text(src, encoding="utf-8")
        restored = all((overlay / rel).read_text(encoding="utf-8") == src for rel, src in orig.items())
    killed = sum(m["killed"] for m in result["mutations"])
    emit(f"KILLED {killed}/{len(selected)}")
    emit(f"overlay restored: {restored}")
    out.close()
    result.update(killed=killed, total=len(selected), restored=restored, overlay=str(overlay),
                  seconds=round(time.monotonic() - t0, 1))
    return result


def ok(result: dict[str, Any]) -> bool:
    return bool(result["baseline_green"] and result["restored"] and result["killed"] == result["total"])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Phase 4 mutationstester i overlay-kopior.")
    ap.add_argument("--scratch", required=True, type=Path)
    ap.add_argument("--sets", default="all")
    ap.add_argument("--commit", default="HEAD")
    ap.add_argument("--parallel", type=int, default=1)
    ap.add_argument("--only", default="")
    ap.add_argument("--check", action="store_true", help="bara mönsterkontrollen mot repots filer")
    a = ap.parse_args(argv)
    names = list(SET_ORDER) if a.sets == "all" else a.sets.split(",")
    sets = [load_set(n) for n in names]
    if a.check:
        problems = pattern_problems(ROOT, sets)
        print("\n".join(problems) if problems else "ALL PATTERNS EXACTLY ONCE")
        return 1 if problems else 0
    scratch = check_scratch(a.scratch)
    commit = subprocess.run(["git", "-C", str(ROOT), "rev-parse", a.commit], capture_output=True, text=True,
                            check=True).stdout.strip()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = scratch / f"mutation-run-{commit[:12]}-{stamp}"
    run_dir.mkdir()
    order = sorted(sets, key=lambda m: -MINUTES.get(m.NAME, 0.0)) if a.parallel > 1 else sets
    with ThreadPoolExecutor(max_workers=max(1, a.parallel)) as pool:
        results = list(pool.map(lambda m: run_set(m, scratch, commit, run_dir, a.only), order))
    results.sort(key=lambda r: names.index(r["set"]))
    summary = {"commit": commit, "parallel": a.parallel, "sets": results}
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [f"{r['set']}: KILLED {r['killed']}/{r['total']}, baseline {'green' if r['baseline_green'] else 'RED'}, "
             f"restored {r['restored']}, {r['seconds'] / 60:.1f} min" for r in results]
    (run_dir / "summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"run directory: {run_dir}")
    return 0 if all(ok(r) for r in results) else 1
