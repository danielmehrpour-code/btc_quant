"""Kör varje runbook-kommando i docs/PHASE4_DATASTORE.md avsnitt 9 mot en hermetisk syntetisk store och
kontrollerar exitkod och utdatans form (P4-S9-e, P4-S13-b). Rör aldrig ~/btc_data.

Användning:  python scripts/docs/run_examples.py --scratch DIR

KOMMANDON: COMMANDS innehåller exakt runbookens kommandorader (blanksteg normaliserade) med förväntad
exitkod; tests/test_docs_tools.py jämför dem statiskt med avsnitt 9 i båda riktningarna.

ARBETSKATALOG: DIR/docs-examples/, märkt med filen .docs-examples. En befintlig arbetskatalog tas bort
bara om den bär märkfilen; resultatet skrivs till DIR/docs-examples/examples_result.txt.

SÄKERHET (ExamplesSafetyError, ingenting skrivs): --scratch följer mutationskörarens regel
(scripts/mutation/runner.py check_scratch: inte /, inte hemkatalogen, inte i repots arbetskatalog) och
får dessutom inte vara eller ligga i ~/btc_data.

REPO: exemplen körs mot detta repo om dess arbetskatalog är ren, annars mot en ren klon av HEAD i
DIR/docs-examples/clone (ingest --apply kräver ren arbetskatalog, P4-S8-g). Varje delprocess får
PYTHONPATH=<repo>, och det kontrolleras att paketet importeras därifrån. Utdatan anger repo och commit.

Exit 0 endast om alla exempel är OK.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
BTC_DATA = Path.home() / "btc_data"
MARKER = ".docs-examples"
WORK_NAME = "docs-examples"
PY = sys.executable
ENVELOPE = {"format", "command", "applied", "exit_code", "context", "result", "result_sha256", "error"}
SPOT, PERP = "binance/spot/BTCUSDT/15m", "binance/perp/BTCUSDT/15m"

# Runbookens kommandorader (docs/PHASE4_DATASTORE.md avsnitt 9) -> förväntad exitkod.
COMMANDS: dict[str, tuple[str, int]] = {
    "R01": ('$DS init --store "$STORE"', 0),
    "R02": ('$DS init --store "$STORE" --apply', 0),
    "R03": ('$DS import-legacy --store "$STORE" --source "$P2"', 0),
    "R04": ('$DS import-legacy --store "$STORE" --source "$P2" --apply', 0),
    "R05": ('$DS register-report --store "$STORE" --source "$REPORTS"', 0),
    "R06": ('$DS register-report --store "$STORE" --source "$REPORTS" --apply', 0),
    "R07": ('$DS verify --store "$STORE"', 0),
    "R08": ('$DS list --store "$STORE"', 0),
    "R09": (f'$DS versions --store "$STORE" --dataset {SPOT}', 0),
    "R10": (f'$DS ingest --store "$STORE" --dataset {SPOT} "$RAW"/new/spot/*.csv', 0),
    "R11": (f'$DS ingest --store "$STORE" --dataset {SPOT} "$RAW"/new/spot/*.csv --apply', 0),
    "R12": (f'$DS ingest --store "$STORE" --dataset {SPOT} "$RAW"/old/spot/*.csv --apply', 5),
    "R13": (f'$DS versions --store "$STORE" --dataset {SPOT}', 0),
    "R14": (f'$DS set-current --store "$STORE" --dataset {SPOT} --data-version "$DV"', 0),
    "R15": (f'$DS set-current --store "$STORE" --dataset {SPOT} --data-version "$DV" --apply', 0),
    "R16": (f'$DS snapshot create --store "$STORE" --current {SPOT} --current {PERP}', 0),
    "R17": (f'$DS snapshot create --store "$STORE" --current {SPOT} --current {PERP} --apply', 0),
    "R18": ('$DS snapshot show --store "$STORE" --id "$SNAP"', 0),
    "R19": ('$DS --format text verify --store "$STORE"', 0),
    "R20": ('$DS rebuild --store "$STORE"', 0),
    "R21": ('$DS rebuild --store "$STORE" --apply', 0),
    "R22": ('$DS verify --store "$STORE"', 1),
    "R23": ('$DS repair --store "$STORE"', 1),
    "R24": ('$DS repair --store "$STORE" --apply', 0),
    "R25": ('$DS verify --store "$STORE"', 2),
    "R26": ('$DS complete-pending --store "$STORE" --kind ingest', 0),
    "R27": ('$DS complete-pending --store "$STORE" --kind ingest --apply', 0),
    "R28": ('$DS verify --store "$STORE"', 0),
    "R29": ('$DS verify --store "$STORE"', 3),
    "R30": ('$DS repair --store "$STORE" --apply', 5),
    "R31": ('$DS rebuild --store "$STORE" --apply', 5),
    "R32": ("$DS verify", 64),
}


class ExamplesSafetyError(Exception):
    """--scratch eller arbetskatalogen är inte tillåten; ingenting har skrivits eller tagits bort."""


def _load_runner() -> Any:
    spec = importlib.util.spec_from_file_location("docs_examples_mutation_runner",
                                                  ROOT / "scripts" / "mutation" / "runner.py")
    if spec is None or spec.loader is None:
        raise ImportError("scripts/mutation/runner.py saknas")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ------------------------------------------------------------------ säkerhet
def check_scratch(scratch: Path, repo: Path = ROOT, data: Path = BTC_DATA) -> Path:
    runner = _load_runner()
    try:
        s = runner.check_scratch(scratch, repo)
    except runner.MutationSafetyError as e:
        raise ExamplesSafetyError(str(e)) from None
    d = Path(os.path.realpath(data))
    if s == d or d in s.parents:
        raise ExamplesSafetyError(f"--scratch {s} är eller ligger i {d}")
    return s


def prepare_work(scratch: Path, repo: Path = ROOT, data: Path = BTC_DATA) -> Path:
    """Ny, märkt arbetskatalog DIR/docs-examples; en befintlig tas bort bara om den bär märkfilen."""
    work = check_scratch(scratch, repo, data) / WORK_NAME
    if os.path.lexists(work):
        if work.is_symlink() or not work.is_dir() or not (work / MARKER).is_file() or (work / MARKER).is_symlink():
            raise ExamplesSafetyError(f"{work} finns men saknar märkfilen {MARKER}; tas inte bort")
        subprocess.run(["chmod", "-R", "u+w", str(work)], check=True)   # store-objekt är 0555 (P4-S2-01)
        shutil.rmtree(work)
    work.mkdir(parents=True)
    (work / MARKER).write_text("run_examples\n", encoding="utf-8")
    return work


def choose_repo(work: Path, repo: Path = ROOT) -> tuple[Path, str]:
    """Repot om dess arbetskatalog är ren, annars en ren klon av HEAD (utan hårda länkar) i work/clone."""
    status = subprocess.run(["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True,
                            check=True)
    if status.stdout.strip() == "":
        target = repo
    else:
        target = work / "clone"
        subprocess.run(["git", "clone", "--quiet", "--no-hardlinks", str(repo), str(target)], check=True)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=target, capture_output=True, text=True,
                          check=True).stdout.strip()
    return target, head


def check_package(repo: Path, cwd: Path) -> None:
    """En delprocess med PYTHONPATH=<repo> måste importera btc_trading_system från <repo>, inte från en
    annan installation (t.ex. den redigerbara installationen i .venv)."""
    r = subprocess.run([PY, "-c", "import btc_trading_system; print(btc_trading_system.__file__)"],
                       env={**os.environ, "PYTHONPATH": str(repo)}, cwd=cwd, capture_output=True, text=True,
                       check=False)
    found = Path(r.stdout.strip()).resolve() if r.returncode == 0 and r.stdout.strip() else None
    if found is None or found.parents[1] != repo.resolve():
        raise RuntimeError(f"paketet importeras från {found}, inte från {repo}")


# ------------------------------------------------------------------ fixturer
def fixtures(work: Path, repo: Path) -> dict[str, str]:
    """Syntetiska Binance-CSV:er -> Phase 2-store (Phase 2-CLI) -> Phase 3-rapporter (Phase 3-CLI)."""
    check_package(repo, work)
    sys.path[:0] = [str(repo), str(repo / "tests")]
    from data_helpers import make_klines, write_csv
    base = {**os.environ, "PYTHONPATH": str(repo)}
    raw = work / "raw"
    for sub, n, seed in (("legacy/spot", 200, 1), ("legacy/perp", 200, 2), ("new/spot", 300, 5),
                         ("old/spot", 100, 3), ("crash/spot", 400, 6)):
        d = raw / sub
        d.mkdir(parents=True)
        write_csv(d / "BTCUSDT-15m-2024-01.csv", make_klines("15m", n, seed))
    env = {"STORE": str(work / "store"), "P2": str(work / "p2"), "REPORTS": str(work / "reports"),
           "RAW": str(raw), "DS": f"{PY} -m btc_trading_system.database.cli", "PYTHONPATH": str(repo)}
    for inst in ("spot", "perp"):
        r = subprocess.run([PY, "scripts/ingest_binance.py", "--store", env["P2"], "--instrument", inst,
                            "--timeframe", "15m", *map(str, sorted((raw / "legacy" / inst).iterdir()))],
                           cwd=repo, env=base, capture_output=True, text=True, check=False)
        if r.returncode != 0:
            raise RuntimeError(r.stderr)
        r = subprocess.run([PY, "scripts/data_quality_report.py", "dataset", "--store", env["P2"],
                            "--out", env["REPORTS"], "--instrument", inst, "--timeframe", "15m"],
                           cwd=repo, env=base, capture_output=True, text=True, check=False)
        if r.returncode not in (0, 1, 3):                              # PASS/WARN, FAIL, INCOMPLETE
            raise RuntimeError(r.stderr)
    return env


def sh(cmd: str, env: dict[str, str], work: Path) -> tuple[int, str, str]:
    r = subprocess.run(["bash", "-c", cmd], env={**os.environ, **env}, cwd=work, capture_output=True,
                       text=True, check=False)
    return r.returncode, r.stdout, r.stderr


def pyfix(code: str, env: dict[str, str], work: Path) -> None:
    """Fixturssteg i en egen process (t.ex. en verklig krasch med os._exit)."""
    r = subprocess.run([PY, "-c", code], env={**os.environ, **env}, cwd=work, capture_output=True, text=True,
                       check=False)
    if r.returncode not in (0, 77):
        raise RuntimeError(r.stderr)


# ------------------------------------------------------------------ exempel
Check = Callable[[dict[str, Any]], Any]


class Examples:
    def __init__(self, env: dict[str, str], work: Path) -> None:
        self.env, self.work = env, work
        self.results: list[tuple[str, str, int, int, bool, str]] = []

    def ex(self, eid: str, check: Check | None = None, *, text: bool = False) -> dict[str, Any] | None:
        cmd, code = COMMANDS[eid]
        rc, out, err = sh(cmd, self.env, self.work)
        ok, note = rc == code, ""
        obj = None
        try:
            if text:
                ok = ok and out.startswith("datastore ")
                note = out.splitlines()[0] if out else ""
            else:
                obj = json.loads(out)
                ok = ok and set(obj) == ENVELOPE and obj["exit_code"] == rc
                ok = ok and (obj["error"] is None) == (rc in (0, 1, 2, 3, 4))
                ok = ok and (err == "") == (obj["error"] is None)
                if check is not None:
                    res = check(obj)
                    ok = ok and res is not False
                    note = res if isinstance(res, str) else ""
        except Exception as e:                                         # noqa: BLE001 - rapportera i stället för att avbryta
            ok, note = False, f"{type(e).__name__}: {e}"
        self.results.append((eid, cmd, code, rc, ok, note))
        if not ok:
            print(f"FAIL {eid}: rc={rc}\nOUT={out[:2000]}\nERR={err[:2000]}")
        return obj


def run(work: Path, env: dict[str, str]) -> list[tuple[str, str, int, int, bool, str]]:
    x = Examples(env, work)
    ex = x.ex
    # ---------------------------------------------------------------- 1. ny store + legacy-import
    ex("R01", lambda o: o["applied"] is False and o["result"] == {"already_initialised": False, "predicted": False}
       and not Path(env["STORE"]).exists())
    ex("R02", lambda o: o["applied"] is True and o["result"]["initialised"] is True)
    ex("R03", lambda o: o["result"]["applied"] is False and o["result"]["ok"] is True
       and [d["status"] for d in o["result"]["datasets"]] == ["planned", "planned"])
    o = ex("R04", lambda o: o["result"]["ok"] is True
           and [(d["status"], d["publication"], d["record"], d["ref_outcome"]) for d in o["result"]["datasets"]]
           == [("imported", "published", "recorded", "set")] * 2
           and o["result"]["source_tree_sha256_before"] == o["result"]["source_tree_sha256_after"])
    assert o is not None
    legacy_dv = {d["relpath"]: d["data_version"] for d in o["result"]["datasets"]}[SPOT]
    ex("R05", lambda o: [r["status"] for r in o["result"]["reports"]] == ["planned", "planned"])
    ex("R06", lambda o: [(r["status"], r["file"], r["catalog"]) for r in o["result"]["reports"]]
       == [("registered", "stored", "inserted")] * 2)
    ex("R07", lambda o: o["result"]["exit_status"] == 0 and o["result"]["best_effort"] is False
       and o["result_sha256"] is not None and all(s["status"] == "performed" for s in o["result"]["scope"]))
    # ---------------------------------------------------------------- 2. läsning
    ex("R08", lambda o: [(d["dataset"], d["current"]["data_version"] is not None) for d in o["result"]["datasets"]]
       == [(PERP, True), (SPOT, True)] and o["result"]["datasets_from"] == "catalog")
    ex("R09", lambda o: [(v["data_version"], v["current"]) for v in o["result"]["versions"]] == [(legacy_dv, True)])
    # ---------------------------------------------------------------- 3. ingest
    ex("R10", lambda o: o["result"]["predicted"] is False and set(o["context"]) == {"now", "code_version", "worktree_clean"}
       and o["result"]["current"]["data_version"] == legacy_dv
       and f'context: code_version={o["context"]["code_version"][:12]} worktree_clean={o["context"]["worktree_clean"]}')
    o = ex("R11", lambda o: o["context"]["worktree_clean"] is True
           and (o["result"]["record"]["publication"], o["result"]["record"]["ref_outcome"]) == ("published", "changed")
           and o["result"]["record"]["code"] == {"version": o["context"]["code_version"], "worktree_clean": True})
    assert o is not None
    new_dv = o["result"]["record"]["data_version"]
    ex("R12", lambda o: o["error"]["type"] == "VersionRegressionError" and "--allow-regression" in o["error"]["message"])
    ex("R13", lambda o: sorted((v["data_version"], v["current"]) for v in o["result"]["versions"])
       == sorted([(legacy_dv, False), (new_dv, True)]))
    # ---------------------------------------------------------------- 4. refs och snapshots
    env["DV"] = legacy_dv
    ex("R14", lambda o: (o["result"]["changed"], o["result"]["previous_data_version"], o["result"]["target_exists"])
       == (True, new_dv, True))
    ex("R15", lambda o: (o["result"]["changed"], o["result"]["data_version"]) == (True, legacy_dv))
    ex("R16", lambda o: o["result"]["outcome"] == "created" and len(o["result"]["members"]) == 2)
    o = ex("R17", lambda o: o["result"]["outcome"] == "created")
    assert o is not None
    env["SNAP"] = o["result"]["snapshot_id"]
    ex("R18", lambda o: o["result"]["snapshot_id"] == env["SNAP"]
       and {"/".join(m["dataset"][k] for k in ("exchange", "instrument", "symbol", "timeframe")): m["data_version"]
            for m in o["result"]["members"]}[SPOT] == legacy_dv
       and all(set(m) == {"dataset", "dataset_id", "data_version"} for m in o["result"]["members"]))
    ex("R19", text=True)
    ex("R20", lambda o: o["result"]["applied"] is False and o["result"]["identical"] is None)
    ex("R21", lambda o: o["result"]["identical"] is True
       and o["result"]["catalog_dump_sha256_before"] == o["result"]["catalog_dump_sha256_after"])
    # ---------------------------------------------------------------- 5. återställning: katalogen borta (fixturen tar bort den)
    Path(env["STORE"], "catalog.sqlite").unlink()
    ex("R22", lambda o: [f["code"] for f in o["result"]["findings"] if f["severity"] != "INFO"] == ["CATALOG_MISSING"])
    ex("R23", lambda o: o["result"]["plan"] == {"tmp_removals": [], "rebuild_catalog": True, "blocking": []})
    ex("R24", lambda o: o["result"]["after"]["exit_status"] == 0)
    # ---------------------------------------------------------------- 6. återställning: pending ingest (verklig krasch)
    pyfix("import os, glob\n"
          "from datetime import datetime, timezone\n"
          "from btc_trading_system.data.schema import DatasetKey\n"
          "from btc_trading_system.database import versioned_store as vs\n"
          "vs.StoreWriter._append_ref_log = lambda *a: os._exit(77)\n"
          "st = vs.VersionedStore(os.environ['STORE'])\n"
          "with st.writer() as w:\n"
          "    w.ingest(DatasetKey('spot', 'BTCUSDT', '15m'), sorted(glob.glob(os.environ['RAW'] + '/crash/spot/*.csv')),\n"
          "             now=datetime.now(timezone.utc), code_version='fixture', worktree_clean=True)\n", env, work)
    ex("R25", lambda o: "PENDING_INGEST" in {f["code"] for f in o["result"]["findings"]})
    ex("R26", lambda o: [f["code"] for f in o["result"]["pending"]] == ["PENDING_INGEST"])
    ex("R27", lambda o: o["result"]["completed"]["record"]["ref_outcome"] == "changed")
    ex("R28")
    # ---------------------------------------------------------------- 7. ERROR: bevis behålls (kopia, fixturen förstör)
    bad = work / "store-bad"
    shutil.copytree(env["STORE"], bad, symlinks=True)
    pyfix("import os\n"
          "from btc_trading_system.data.schema import DatasetKey\n"
          "from btc_trading_system.database import versioned_store as vs\n"
          "st = vs.VersionedStore(os.environ['BAD'])\n"
          "k = DatasetKey('perp', 'BTCUSDT', '15m')\n"
          "d = st.object_dir(k, st.current_ref(k).ref.data_version)\n"
          "p = d / 'bars.parquet'\n"
          "b = p.read_bytes(); i = b.find(b'parquet-cpp-arrow')\n"
          "os.chmod(d, 0o755); os.chmod(p, 0o644)\n"
          "p.write_bytes(b[:i] + b'Parquet' + b[i + 7:])\n"
          "os.chmod(p, 0o444); os.chmod(d, 0o555)\n", {**env, "BAD": str(bad)}, work)
    env["STORE"] = str(bad)
    ex("R29", lambda o: "OBJECT_CORRUPT" in {f["code"] for f in o["result"]["findings"]})
    ex("R30", lambda o: o["error"]["type"] == "RepairRefusedError")
    ex("R31", lambda o: o["error"]["type"] == "RepairRefusedError")
    # ---------------------------------------------------------------- 8. användning
    ex("R32", lambda o: o["error"]["type"] == "UsageError")
    if [r[0] for r in x.results] != list(COMMANDS):
        raise RuntimeError("varje exempel i COMMANDS ska köras exakt en gång, i ordning")
    return x.results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Kör runbook-exemplen i docs/PHASE4_DATASTORE.md avsnitt 9.",
                                 allow_abbrev=False)
    ap.add_argument("--scratch", required=True, type=Path, help="arbetsrot utanför repot och ~/btc_data")
    args = ap.parse_args(argv)
    work = prepare_work(args.scratch)
    repo, head = choose_repo(work)
    env = fixtures(work, repo)
    results = run(work, env)
    lines = [f"repo {'clean clone of HEAD' if repo != ROOT else 'working tree'} at {head}"]
    lines += [f"{eid} | expected {code} | got {rc} | {'OK' if ok else 'FAIL'} | {cmd}" + (f" | {note}" if note else "")
              for eid, cmd, code, rc, ok, note in results]
    bad_n = sum(not r[4] for r in results)
    lines.append(f"{len(results) - bad_n}/{len(results)} examples OK")
    (work / "examples_result.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    return 1 if bad_n else 0


if __name__ == "__main__":
    sys.exit(main())
