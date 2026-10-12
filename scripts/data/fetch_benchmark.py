"""Hämtar och kontrollerar jämförelsedata (etapp B steg 5; SB-S5-a, SB-S5-b, SB-S5-c; D-B-e).

Användning:  python scripts/data/fetch_benchmark.py plan    --root R
             python scripts/data/fetch_benchmark.py fetch   --root R [--apply]
             python scripts/data/fetch_benchmark.py recheck --root R [--apply]
             python scripts/data/fetch_benchmark.py build   --root R [--apply]
             python scripts/data/fetch_benchmark.py verify  --root R

OMFATTNING: konstanten TARGETS (Riksbankens SEKUSDPMI 2019-12-01..2026-09-30). K2:s NAV läggs till först efter
BlackRocks skriftliga tillstånd, som ett eget beslut (SB-S5-b); inga iShares-värdar finns i ALLOWED_HOSTS.

ARKIV (som SB-S2-b):
  ARCHIVE.json                                 formatmarkör, 0444
  .lock                                        skrivlås (flock)
  objects/<källa>/<serie>/<från>_<till>/       första godkända svaret: response.json
  revisions/<källa>/<serie>/<från>_<till>/<sha256>/response.json   senare avvikande svar (D-B-e)
  canonical/<källa>-<serie>.csv                kanonisk fil ur objektet (build); dess sha256 pinnas i
                                               configs/benchmark_pins_v1.yaml genom beslut
  inventory/<sha256>.json                      deterministisk förteckning, innehållsadresserad
  runs/<seq>.json                              en post per --apply-körning (tidpunkter bara här)
  tmp/stage-<32 hex>/                          staging
Filer 0444, objekt- och revisionskataloger 0555. Inget publicerat ändras eller tas bort.

NÄTVERK (G-21, som SB-S2-f): bara https och värdar i ALLOWED_HOSTS, inga omdirigeringar, inga proxyvariabler,
tidsgräns och storleksgräns, omförsök bara vid nätverksfel och HTTP 5xx. Ett svar lagras bara om det kan tolkas
som en SWEA-serie (benchmark.fx.parse_swea_observations); utdata visar aldrig några värden, bara antal och hash.

EXIT: 0 komplett och verifierat; 1 ofullständigt; 2 recheck: skillnad; 3 ERROR; 5 vägrat (lås, rot,
oren arbetskatalog vid --apply); 64 användning; 70 internt.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:                                   # körs som skript från repots rot
    sys.path.insert(0, str(ROOT))

from btc_trading_system.benchmark.fx import FxDataError, parse_swea_observations
from btc_trading_system.benchmark.series import load_benchmark_config

ARCHIVE_FORMAT = "btc-quant/benchmark-archive/v1"
INVENTORY_FORMAT = "btc-quant/benchmark-archive-inventory/v1"
RUN_FORMAT = "btc-quant/benchmark-archive-run/v1"
CLI_FORMAT = "btc-quant/fetch-benchmark/v1"
ALLOWED_HOSTS = frozenset({"api.riksbank.se"})
SWEA_BASE = "https://api.riksbank.se/swea/v1/Observations/"


@dataclass(frozen=True)
class Target:
    source: str
    series: str
    first: date
    last: date

    @property
    def key(self) -> str:
        return f"{self.source}/{self.series}/{self.first.isoformat()}_{self.last.isoformat()}"

    @property
    def url(self) -> str:
        return f"{SWEA_BASE}{self.series}/{self.first.isoformat()}/{self.last.isoformat()}"

    @property
    def canonical_name(self) -> str:
        return f"{self.source}-{self.series}.csv"


TARGETS = (Target("riksbank", "SEKUSDPMI", date(2019, 12, 1), date(2026, 9, 30)),)
MAX_BYTES = 8 * 1024 * 1024
TIMEOUT_S = 60
NETWORK_ATTEMPTS = 3
RETRY_PAUSE_S = 2.0
USER_AGENT = "btc-quant-fetch-benchmark/1"
ROOT_ENTRIES = {"ARCHIVE.json", ".lock", "objects", "revisions", "canonical", "inventory", "runs", "tmp"}
DIRS = ("objects", "revisions", "canonical", "inventory", "runs", "tmp")
RESPONSE = "response.json"
STAGE_RE = re.compile(r"stage-[0-9a-f]{32}")
SHA_RE = re.compile(r"[0-9a-f]{64}")
RUN_RE = re.compile(r"(\d{6})\.json")

EXIT_OK, EXIT_INCOMPLETE, EXIT_CHANGED, EXIT_ERROR = 0, 1, 2, 3
EXIT_REFUSED, EXIT_USAGE, EXIT_INTERNAL = 5, 64, 70
CODES = {"MISSING": EXIT_INCOMPLETE, "TMP_LEFTOVER": EXIT_INCOMPLETE, "INVENTORY_BEHIND": EXIT_INCOMPLETE,
         "CANONICAL_MISSING": EXIT_INCOMPLETE,
         "MARKER_INVALID": EXIT_ERROR, "UNKNOWN_ENTRY": EXIT_ERROR, "OBJECT_INVALID": EXIT_ERROR,
         "REVISION_INVALID": EXIT_ERROR, "INVENTORY_CORRUPT": EXIT_ERROR, "CANONICAL_INVALID": EXIT_ERROR,
         "RUN_CORRUPT": EXIT_ERROR}

Opener = Callable[[urllib.request.Request, float], Any]
GitState = Callable[[], tuple[str, bool | None]]
Clock = Callable[[], datetime]


class RefusedError(Exception):
    """Vägrat innan något skrevs (exit 5)."""


class FetchError(Exception):
    def __init__(self, url: str, kind: str, status: int | None = None, *, retry: bool = False) -> None:
        super().__init__(f"{kind} {status or ''} {url}".strip())
        self.kind, self.status, self.retry = kind, status, retry


def canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode() + b"\n"


# ---------------------------------------------------------------- nätverk
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        raise urllib.error.HTTPError(req.full_url, code, "redirect refused", headers, fp)


def default_opener() -> Opener:
    op = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    return lambda req, timeout: op.open(req, timeout=timeout)


def check_url(url: str) -> None:
    u = urllib.parse.urlsplit(url)
    if u.scheme != "https" or u.hostname not in ALLOWED_HOSTS or u.port not in (None, 443) or u.username:
        raise RefusedError(f"URL utanför allowlisten: {url}")


def download(url: str, opener: Opener, sleep: Callable[[float], None]) -> bytes:
    check_url(url)
    last: FetchError | None = None
    for attempt in range(NETWORK_ATTEMPTS):
        if attempt:
            sleep(RETRY_PAUSE_S)
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
        try:
            with opener(req, TIMEOUT_S) as resp:
                status = getattr(resp, "status", 200)
                if status != 200:
                    raise FetchError(url, "http", status, retry=status >= 500)
                data = resp.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise FetchError(url, "too_large")
            return bytes(data)
        except urllib.error.HTTPError as e:
            last = FetchError(url, "http", e.code, retry=e.code >= 500)
        except FetchError as e:
            last = e
        except (urllib.error.URLError, OSError) as e:
            last = FetchError(url, f"network:{type(e).__name__}", retry=True)
        if not last.retry:
            break
    assert last is not None
    raise last


# ---------------------------------------------------------------- arkivet
def _write_ro(path: Path, data: bytes) -> None:
    with open(path, "xb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(path, 0o444)


def _git_state() -> tuple[str, bool | None]:
    try:
        head = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True,
                              check=True).stdout.strip()
        dirty = subprocess.run(["git", "-C", str(ROOT), "status", "--porcelain"], capture_output=True, text=True,
                               check=True).stdout.strip()
        return head, not dirty
    except (OSError, subprocess.CalledProcessError):
        return "unknown", None


def check_root(root: Path, *, create: bool) -> None:
    if not root.parent.is_dir():
        raise RefusedError(f"rotens förälder finns inte: {root.parent}")
    if root.exists():
        if not root.is_dir() or root.is_symlink():
            raise RefusedError(f"roten är ingen katalog: {root}")
        entries = set(os.listdir(root))
        if entries and "ARCHIVE.json" not in entries:
            raise RefusedError(f"katalogen saknar ARCHIVE.json och är inte tom: {root}")
        if not entries and not create:
            raise RefusedError(f"tom katalog utan arkiv: {root}")
    elif not create:
        raise RefusedError(f"arkivet finns inte: {root}")


@contextmanager
def writer_lock(root: Path) -> Iterator[None]:
    fd = os.open(root / ".lock", os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            raise RefusedError("arkivet är låst av en annan skrivare") from e
        yield
    finally:
        os.close(fd)


def ensure_layout(root: Path) -> None:
    root.mkdir(exist_ok=True)
    marker = root / "ARCHIVE.json"
    if not marker.exists():
        _write_ro(marker, canonical_json({"format": ARCHIVE_FORMAT}))
    for d in DIRS:
        (root / d).mkdir(exist_ok=True)


def marker_ok(root: Path) -> bool:
    try:
        return (root / "ARCHIVE.json").read_bytes() == canonical_json({"format": ARCHIVE_FORMAT})
    except OSError:
        return False


def _validate(data: bytes) -> int:
    """Antal observationer om svaret är en giltig SWEA-serie; annars FxDataError."""
    cfg = load_benchmark_config()
    return len(parse_swea_observations(data, cfg.fx_availability, cfg.fx_scale_exponent))


def _publish(root: Path, rel: Path, data: bytes) -> None:
    """Skriver data som rel/response.json via staging; katalogen blir 0555."""
    stage = root / "tmp" / f"stage-{uuid.uuid4().hex}"
    stage.mkdir()
    _write_ro(stage / RESPONSE, data)
    target = root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    os.rename(stage, target)
    os.chmod(target, 0o555)


def _versions(root: Path, t: Target) -> list[dict[str, Any]]:
    out = []
    obj = root / "objects" / t.key / RESPONSE
    if obj.exists():
        b = obj.read_bytes()
        out.append({"kind": "object", "sha256": hashlib.sha256(b).hexdigest(), "size": len(b)})
    rev = root / "revisions" / t.key
    if rev.is_dir():
        for h in sorted(os.listdir(rev)):
            b = (rev / h / RESPONSE).read_bytes()
            out.append({"kind": "revision", "sha256": hashlib.sha256(b).hexdigest(), "size": len(b)})
    return out


def _canonical_bytes(data: bytes) -> bytes:
    cfg = load_benchmark_config()
    frame = parse_swea_observations(data, cfg.fx_availability, cfg.fx_scale_exponent)
    lines = ["obs_date,value_scaled"] + [f"{d.isoformat()},{int(v)}"
                                         for d, v in zip(frame["obs_date"], frame["value_scaled"])]
    return ("\n".join(lines) + "\n").encode("ascii")


def inventory(root: Path) -> dict[str, Any]:
    entries = []
    for t in TARGETS:
        canon = root / "canonical" / t.canonical_name
        entries.append({"key": t.key, "url": t.url, "versions": _versions(root, t),
                        "canonical": ({"name": t.canonical_name,
                                       "sha256": hashlib.sha256(canon.read_bytes()).hexdigest()}
                                      if canon.exists() else None)})
    return {"format": INVENTORY_FORMAT, "entries": entries}


def write_inventory(root: Path) -> str:
    data = canonical_json(inventory(root))
    h = hashlib.sha256(data).hexdigest()
    p = root / "inventory" / f"{h}.json"
    if not p.exists():
        _write_ro(p, data)
    return h


def write_run(root: Path, record: dict[str, Any]) -> None:
    seqs = [int(m.group(1)) for n in os.listdir(root / "runs") if (m := RUN_RE.fullmatch(n))]
    seq = max(seqs, default=0) + 1
    _write_ro(root / "runs" / f"{seq:06d}.json", canonical_json({"format": RUN_FORMAT, "seq": seq, **record}))


# ---------------------------------------------------------------- kommandon
def cmd_plan(root: Path) -> tuple[int, dict[str, Any]]:
    missing = [t.key for t in TARGETS if not (root / "objects" / t.key / RESPONSE).exists()]
    return (EXIT_INCOMPLETE if missing else EXIT_OK), {"targets": len(TARGETS), "missing": missing}


def cmd_fetch(root: Path, apply: bool, opener: Opener, sleep: Callable[[float], None]) -> tuple[int, dict[str, Any]]:
    todo = [t for t in TARGETS if not (root / "objects" / t.key / RESPONSE).exists()]
    if not apply:
        return (EXIT_INCOMPLETE if todo else EXIT_OK), {"would_fetch": [t.key for t in todo], "requests": 0}
    fetched, failures = [], []
    for t in todo:
        try:
            data = download(t.url, opener, sleep)
            n = _validate(data)
        except (FetchError, FxDataError) as e:
            failures.append({"key": t.key, "error": str(e)})
            continue
        _publish(root, Path("objects") / t.key, data)
        fetched.append({"key": t.key, "sha256": hashlib.sha256(data).hexdigest(), "observations": n})
    return (EXIT_INCOMPLETE if failures else EXIT_OK), {"fetched": fetched, "failures": failures,
                                                       "requests": len(todo)}


def cmd_recheck(root: Path, apply: bool, opener: Opener, sleep: Callable[[float], None]) -> tuple[int, dict[str, Any]]:
    changes, failures = [], []
    for t in TARGETS:
        obj = root / "objects" / t.key / RESPONSE
        if not obj.exists():
            failures.append({"key": t.key, "error": "MISSING"})
            continue
        try:
            data = download(t.url, opener, sleep)
            _validate(data)
        except (FetchError, FxDataError) as e:
            failures.append({"key": t.key, "error": str(e)})
            continue
        h = hashlib.sha256(data).hexdigest()
        known = {v["sha256"] for v in _versions(root, t)}
        if h in known:
            continue
        changes.append({"key": t.key, "sha256": h})
        if apply:
            _publish(root, Path("revisions") / t.key / h, data)
    code = EXIT_CHANGED if changes else (EXIT_INCOMPLETE if failures else EXIT_OK)
    return code, {"changes": changes, "failures": failures, "requests": len(TARGETS) - sum(
        1 for f in failures if f["error"] == "MISSING")}


def cmd_build(root: Path, apply: bool) -> tuple[int, dict[str, Any]]:
    built, missing = [], []
    for t in TARGETS:
        obj = root / "objects" / t.key / RESPONSE
        canon = root / "canonical" / t.canonical_name
        if not obj.exists():
            missing.append(t.key)
            continue
        data = _canonical_bytes(obj.read_bytes())
        if canon.exists():
            if canon.read_bytes() != data:
                raise RuntimeError(f"kanonisk fil {canon.name} avviker från objektet")
            continue
        if apply:
            _write_ro(canon, data)
        built.append({"name": t.canonical_name, "sha256": hashlib.sha256(data).hexdigest()})
    return (EXIT_INCOMPLETE if missing else EXIT_OK), {"built": built, "missing": missing}


def findings(root: Path) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []

    def add(code: str, subject: str) -> None:
        out.append({"code": code, "subject": subject})

    if not marker_ok(root):
        add("MARKER_INVALID", "ARCHIVE.json")
        return out
    for n in sorted(set(os.listdir(root)) - ROOT_ENTRIES):
        add("UNKNOWN_ENTRY", n)
    keys = {t.key: t for t in TARGETS}
    for top, kind in (("objects", "OBJECT_INVALID"), ("revisions", "REVISION_INVALID")):
        base = root / top
        for dirpath, dirnames, filenames in os.walk(base):
            rel = Path(dirpath).relative_to(base).as_posix()
            if not filenames:
                continue
            key = rel if top == "objects" else rel.rsplit("/", 1)[0]
            if key not in keys or filenames != [RESPONSE] or dirnames:
                add("UNKNOWN_ENTRY", f"{top}/{rel}")
                continue
            p = Path(dirpath) / RESPONSE
            b = p.read_bytes()
            ok = stat.S_IMODE(p.stat().st_mode) == 0o444 and stat.S_IMODE(Path(dirpath).stat().st_mode) == 0o555
            if top == "revisions":
                ok = ok and rel.rsplit("/", 1)[1] == hashlib.sha256(b).hexdigest()
            try:
                _validate(b)
            except FxDataError:
                ok = False
            if not ok:
                add(kind, f"{top}/{rel}")
    for t in TARGETS:
        obj = root / "objects" / t.key / RESPONSE
        if not obj.exists():
            add("MISSING", t.key)
            continue
        canon = root / "canonical" / t.canonical_name
        if not canon.exists():
            add("CANONICAL_MISSING", t.canonical_name)
            continue
        try:
            expected = _canonical_bytes(obj.read_bytes())
        except FxDataError:
            continue                                            # objektet är redan OBJECT_INVALID
        if canon.read_bytes() != expected or stat.S_IMODE(canon.stat().st_mode) != 0o444:
            add("CANONICAL_INVALID", t.canonical_name)
    for n in sorted(os.listdir(root / "canonical")):
        if n not in {t.canonical_name for t in TARGETS}:
            add("UNKNOWN_ENTRY", f"canonical/{n}")
    for n in sorted(os.listdir(root / "tmp")):
        add("TMP_LEFTOVER" if STAGE_RE.fullmatch(n) else "UNKNOWN_ENTRY", f"tmp/{n}")
    current = hashlib.sha256(canonical_json(inventory(root))).hexdigest()
    for n in sorted(os.listdir(root / "inventory")):
        m = re.fullmatch(r"([0-9a-f]{64})\.json", n)
        if not m or hashlib.sha256((root / "inventory" / n).read_bytes()).hexdigest() != m.group(1):
            add("INVENTORY_CORRUPT", f"inventory/{n}")
    if not (root / "inventory" / f"{current}.json").exists():
        add("INVENTORY_BEHIND", current)
    for n in sorted(os.listdir(root / "runs")):
        if not RUN_RE.fullmatch(n):
            add("RUN_CORRUPT", f"runs/{n}")
    return out


def cmd_verify(root: Path) -> tuple[int, dict[str, Any]]:
    f = findings(root)
    return max((CODES[x["code"]] for x in f), default=EXIT_OK), {"findings": f}


def emit(payload: dict[str, Any]) -> None:
    body = {"format": CLI_FORMAT, **payload}
    body["result_sha256"] = hashlib.sha256(canonical_json(body)).hexdigest()
    sys.stdout.write(canonical_json(body).decode())


def main(argv: list[str] | None = None, *, opener: Opener | None = None, git_state: GitState = _git_state,
         clock: Clock = lambda: datetime.now(UTC), sleep: Callable[[float], None] = time.sleep) -> int:
    ap = argparse.ArgumentParser(description="Hämtar och kontrollerar jämförelsedata (SB-S5-c).")
    ap.add_argument("command", choices=("plan", "fetch", "recheck", "build", "verify"))
    ap.add_argument("--root", required=True, type=Path)
    ap.add_argument("--apply", action="store_true")
    try:
        a = ap.parse_args(argv)
    except SystemExit as e:
        return EXIT_OK if e.code == 0 else EXIT_USAGE
    if a.apply and a.command in ("plan", "verify"):
        return EXIT_USAGE
    root = Path(os.path.abspath(a.root))
    try:
        writes = a.apply and a.command in ("fetch", "recheck", "build")
        check_root(root, create=writes or a.command != "verify")    # plan/torrkörning: saknad rot = tom
        if not writes:
            if root.exists() and os.listdir(root) and not marker_ok(root):
                emit({"command": a.command, "exit_code": EXIT_ERROR, "findings": [{"code": "MARKER_INVALID"}]})
                return EXIT_ERROR
            if a.command == "verify":
                code, payload = cmd_verify(root)
            elif a.command == "plan":
                code, payload = cmd_plan(root)
            elif a.command == "build":
                code, payload = cmd_build(root, False)
            elif a.command == "fetch":
                code, payload = cmd_fetch(root, False, opener or default_opener(), sleep)
            else:
                code, payload = cmd_recheck(root, False, opener or default_opener(), sleep)
            emit({"command": a.command, "exit_code": code, **payload})
            return code
        head, clean = git_state()
        if clean is not True:
            raise RefusedError("--apply kräver en ren arbetskatalog (som SB-S2-d)")
        if root.exists() and os.listdir(root) and not marker_ok(root):
            emit({"command": a.command, "exit_code": EXIT_ERROR, "findings": [{"code": "MARKER_INVALID"}]})
            return EXIT_ERROR
        ensure_layout(root)
        with writer_lock(root):
            started = clock()
            if a.command == "fetch":
                code, payload = cmd_fetch(root, True, opener or default_opener(), sleep)
            elif a.command == "recheck":
                code, payload = cmd_recheck(root, True, opener or default_opener(), sleep)
            else:
                code, payload = cmd_build(root, True)
            inv = write_inventory(root)
            write_run(root, {"command": a.command, "code_version": head, "worktree_clean": clean,
                             "started_at": started.isoformat(), "finished_at": clock().isoformat(),
                             "inventory_sha256": inv, "exit_code": code, **payload})
        emit({"command": a.command, "exit_code": code, "inventory_sha256": inv, **payload})
        return code
    except RefusedError as e:
        sys.stderr.write(f"vägrat: {e}\n")
        return EXIT_REFUSED
    except Exception:  # noqa: BLE001 - internt fel: traceback utan sökvägar
        sys.stderr.write(traceback.format_exc().replace(str(root), "<root>"))
        return EXIT_INTERNAL


if __name__ == "__main__":
    sys.exit(main())
