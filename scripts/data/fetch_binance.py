"""Hämtar och kontrollerar Binance-arkivets månadsfiler (etapp B steg 2; D-B-a, D-B-b, D-B-e, SB-S2-*).

Användning:  python scripts/data/fetch_binance.py plan    --root R
             python scripts/data/fetch_binance.py fetch   --root R [--apply]
             python scripts/data/fetch_binance.py verify  --root R
             python scripts/data/fetch_binance.py recheck --root R [--apply]

OMFATTNING (SB-S2-e): konstanten SERIES x månaderna 2020-01..2026-09 = 405 filer, var och en med sin
<namn>.CHECKSUM. Listan räknas fram här; arkivets fillistning används inte.

ARKIV (SB-S2-b):
  ARCHIVE.json                               formatmarkör, 0444, skrivs en gång
  .lock                                      skrivlås (flock)
  objects/<arkivkatalog>/<namn>/             första verifierade versionen: <namn> + <namn>.CHECKSUM
  revisions/<arkivkatalog>/<namn>/<sha256>/  senare avvikande version (D-B-e)
  inventory/<sha256>.json                    deterministisk förteckning, innehållsadresserad
  runs/<seq>.json                            en post per --apply-körning (tidpunkter bara här)
  tmp/stage-<32 hex>/                        staging
Filer 0444, objekt- och revisionskataloger 0555. Ingenting publicerat ändras, ersätts eller tas bort.

NÄTVERK (SB-S2-f, G-21): bara https://data.binance.vision/, inga omdirigeringar, inga proxyvariabler,
storleksgränser, tidsgräns, begränsade omförsök. Det enda som tolkas i hämtade bytes är CHECKSUM-raden
(exakt "<64 hex>  <namn>"); zip-filerna öppnas aldrig.

EXIT: 0 komplett och verifierat (recheck: ingen skillnad); 1 ofullständigt (saknade mål, misslyckade
hämtningar, tmp-rester, förteckningen efter, oförseglad objektkatalog); 2 recheck: skillnad; 3 ERROR
(ingenting skrivs); 5 vägrat (lås, osäker rot, oren arbetskatalog vid --apply); 64 användning; 70 internt.
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
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn, Protocol

ROOT = Path(__file__).resolve().parents[2]
HOST = "data.binance.vision"
BASE_URL = f"https://{HOST}/"
ARCHIVE_FORMAT = "btc-quant/binance-archive/v1"
INVENTORY_FORMAT = "btc-quant/binance-archive-inventory/v1"
RUN_FORMAT = "btc-quant/binance-archive-run/v1"
CLI_FORMAT = "btc-quant/fetch-binance/v1"
FIRST_MONTH, LAST_MONTH = (2020, 1), (2026, 9)
SERIES = (
    ("spot-1m", "data/spot/monthly/klines/BTCUSDT/1m/", "BTCUSDT-1m-{month}.zip"),
    ("spot-15m", "data/spot/monthly/klines/BTCUSDT/15m/", "BTCUSDT-15m-{month}.zip"),
    ("perp-1m", "data/futures/um/monthly/klines/BTCUSDT/1m/", "BTCUSDT-1m-{month}.zip"),
    ("perp-15m", "data/futures/um/monthly/klines/BTCUSDT/15m/", "BTCUSDT-15m-{month}.zip"),
    ("funding", "data/futures/um/monthly/fundingRate/BTCUSDT/", "BTCUSDT-fundingRate-{month}.zip"),
)
MAX_CHECKSUM_BYTES = 4096
MAX_DATA_BYTES = 64 * 1024 * 1024
TIMEOUT_S = 60
NETWORK_ATTEMPTS = 3            # nätverksfel och HTTP 5xx
MISMATCH_ATTEMPTS = 2           # fel checksumma: ett omförsök
RETRY_PAUSE_S = 2.0             # paus före ett nätverksomförsök
USER_AGENT = "btc-quant-fetch-binance/1"
CHUNK = 1024 * 1024
ROOT_ENTRIES = {"ARCHIVE.json", ".lock", "objects", "revisions", "inventory", "runs", "tmp"}
DIRS = ("objects", "revisions", "inventory", "runs", "tmp")
STAGE_RE = re.compile(r"stage-[0-9a-f]{32}")
SHA_RE = re.compile(r"[0-9a-f]{64}")
INVENTORY_RE = re.compile(r"([0-9a-f]{64})\.json")
RUN_RE = re.compile(r"(\d{6})\.json")

EXIT_OK, EXIT_INCOMPLETE, EXIT_CHANGED, EXIT_ERROR = 0, 1, 2, 3
EXIT_REFUSED, EXIT_USAGE, EXIT_INTERNAL = 5, 64, 70
# Fyndkod -> exitkod (värsta fyndet vinner).
CODES = {
    "MISSING": EXIT_INCOMPLETE, "TMP_LEFTOVER": EXIT_INCOMPLETE, "INVENTORY_BEHIND": EXIT_INCOMPLETE,
    "OBJECT_DIR_MODE": EXIT_INCOMPLETE,
    "MARKER_INVALID": EXIT_ERROR, "UNKNOWN_ENTRY": EXIT_ERROR, "OBJECT_INVALID": EXIT_ERROR,
    "REVISION_INVALID": EXIT_ERROR, "INVENTORY_CORRUPT": EXIT_ERROR, "RUN_CORRUPT": EXIT_ERROR,
    "TMP_UNKNOWN": EXIT_ERROR,
}

Clock = Callable[[], datetime]
GitState = Callable[[], tuple[str, bool | None]]


class RefusedError(Exception):
    """Vägrat innan något skrevs: osäker rot, låset upptaget eller oren arbetskatalog (exit 5)."""


class UsageError(Exception):
    """Fel argument (exit 64)."""


class FetchError(Exception):
    """En hämtning misslyckades; `retry` anger om ett nätverksomförsök är tillåtet."""

    def __init__(self, url: str, kind: str, status: int | None = None, *, retry: bool = False) -> None:
        super().__init__(f"{kind} {status or ''} {url}".strip())
        self.url, self.kind, self.status, self.retry = url, kind, status, retry


class MarkerInvalidError(Exception):
    """ARCHIVE.json finns men är inte exakt formatmarkören (exit 3, ingenting skrivs)."""


class _Exhausted(Exception):
    def __init__(self, error: FetchError, attempts: int) -> None:
        super().__init__(str(error))
        self.error, self.attempts = error, attempts


class ChecksumFormatError(Exception):
    """CHECKSUM-filen följer inte exakt formatet "<64 hex>  <namn>"."""


# ------------------------------------------------------------------ mål
@dataclass(frozen=True)
class Target:
    series: str
    directory: str
    name: str

    @property
    def path(self) -> str:
        return self.directory + self.name

    @property
    def url(self) -> str:
        return BASE_URL + self.path

    @property
    def checksum_url(self) -> str:
        return self.url + ".CHECKSUM"


def months() -> list[str]:
    out, (y, m) = [], FIRST_MONTH
    while (y, m) <= LAST_MONTH:
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def targets() -> list[Target]:
    return [Target(sid, d, pattern.format(month=mo)) for sid, d, pattern in SERIES for mo in months()]


def parse_checksum(data: bytes, name: str) -> str:
    """Värdet i en CHECKSUM-fil; exakt en rad "<64 hex gemener>  <namn>" med valfritt radslut."""
    if len(data) > MAX_CHECKSUM_BYTES:
        raise ChecksumFormatError("för stor")
    try:
        text = data.decode("ascii")
    except UnicodeDecodeError:
        raise ChecksumFormatError("inte ASCII") from None
    m = re.fullmatch(r"([0-9a-f]{64})  (\S+)\n?", text)
    if m is None:
        raise ChecksumFormatError("fel format")
    if m.group(2) != name:
        raise ChecksumFormatError(f"fel filnamn {m.group(2)!r}")
    return m.group(1)


# ------------------------------------------------------------------ nätverk
class Fetcher(Protocol):
    def chunks(self, url: str) -> Iterator[bytes]: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None                                                   # 3xx blir HTTPError, följs aldrig


class HttpFetcher:
    """HTTPS-hämtning från exakt en värd; ingen omdirigering, inga proxyvariabler, standard-TLS."""

    def __init__(self, netloc: str = HOST, scheme: str = "https", timeout: float = TIMEOUT_S) -> None:
        self.netloc, self.scheme, self.timeout = netloc, scheme, timeout
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    def check_url(self, url: str) -> None:
        p = urllib.parse.urlsplit(url)
        if p.scheme != self.scheme or p.netloc != self.netloc or p.query or p.fragment:
            raise FetchError(url, "refused_url")

    def chunks(self, url: str) -> Iterator[bytes]:
        self.check_url(url)
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                while True:
                    block = resp.read(CHUNK)
                    if not block:
                        return
                    yield block
        except urllib.error.HTTPError as e:
            raise FetchError(url, "http", e.code, retry=e.code >= 500) from None
        except (urllib.error.URLError, OSError) as e:
            raise FetchError(url, "network", retry=True) from e


def _consume(fetcher: Fetcher, url: str, limit: int, sink: Callable[[bytes], Any]) -> tuple[str, int]:
    """Läser svaret block för block till sink med sha256 och storleksgräns (avbryts vid gränsen)."""
    h, size = hashlib.sha256(), 0
    gen = fetcher.chunks(url)
    try:
        for block in gen:
            size += len(block)
            if size > limit:
                raise FetchError(url, "too_large")
            h.update(block)
            sink(block)
    finally:
        close = getattr(gen, "close", None)
        if close is not None:
            close()
    return h.hexdigest(), size


def _stream(fetcher: Fetcher, url: str, dest: Path, limit: int) -> tuple[str, int]:
    """Skriver svaret till dest (ny fil) med sha256 under skrivningen och storleksgräns; fsync."""
    fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    try:
        result = _consume(fetcher, url, limit, lambda b: os.write(fd, b))
        os.fsync(fd)
    finally:
        os.close(fd)
    return result


def _fetch_bytes(fetcher: Fetcher, url: str, limit: int) -> bytes:
    """Liten fil (CHECKSUM vid recheck) till minnet; ingenting skrivs."""
    parts: list[bytes] = []
    _consume(fetcher, url, limit, parts.append)
    return b"".join(parts)


# ------------------------------------------------------------------ filsystem
def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _sha256_file(path: Path) -> tuple[str, int]:
    h, size = hashlib.sha256(), 0
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        while True:
            block = os.read(fd, CHUNK)
            if not block:
                break
            size += len(block)
            h.update(block)
    finally:
        os.close(fd)
    return h.hexdigest(), size


def _read(path: Path) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        chunks: list[bytes] = []
        while True:
            block = os.read(fd, CHUNK)
            if not block:
                return b"".join(chunks)
            chunks.append(block)
    finally:
        os.close(fd)


def canonical_json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, indent=2) + "\n").encode()


def _mkdirs(path: Path, stop: Path) -> None:
    """Skapar path och saknade föräldrar upp till stop; fsync av varje förälder som fått en ny katalog."""
    missing = []
    p = path
    while p != stop and not os.path.lexists(p):
        missing.append(p)
        p = p.parent
    for d in reversed(missing):
        d.mkdir()
        _fsync_dir(d.parent)


def _publish_file(root: Path, data: bytes, final: Path) -> bool:
    """Skriver data som final (0444) via tmp och link (skriver aldrig över). False om final redan finns."""
    stage = root / "tmp" / f"stage-{uuid.uuid4().hex}"
    stage.mkdir()
    f = stage / "file"
    fd = os.open(f, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(f, 0o444)
    try:
        os.link(f, final)
        created = True
    except FileExistsError:
        created = False
    _fsync_dir(final.parent)
    os.unlink(f)
    stage.rmdir()
    return created


def _seal_dir(path: Path) -> None:
    os.chmod(path, 0o555)
    _fsync_dir(path.parent)


def _publish_dir(root: Path, stage: Path, target: Path) -> None:
    """Staging-katalog -> mål med rename (målet får inte finnas), sedan 0555. Filerna är redan 0444."""
    _mkdirs(target.parent, root)
    if os.path.lexists(target):
        raise FileExistsError(f"målet finns redan: {target}")
    os.rename(stage, target)
    _fsync_dir(target.parent)
    _fsync_dir(root / "tmp")
    _seal_dir(target)


def _remove_stage(stage: Path) -> None:
    os.chmod(stage, 0o755)
    for p in stage.iterdir():
        p.unlink()
    stage.rmdir()


# ------------------------------------------------------------------ granskning
@dataclass
class Scan:
    """Läsbar bild av arkivet efter full kontroll (inga skrivningar)."""
    findings: list[dict[str, str]] = field(default_factory=list)
    originals: dict[str, dict[str, Any]] = field(default_factory=dict)       # path -> entry
    revisions: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    stages: list[str] = field(default_factory=list)
    unsealed: list[str] = field(default_factory=list)
    last_run: dict[str, Any] | None = None
    inventories: set[str] = field(default_factory=set)

    def add(self, code: str, path: str, detail: str = "") -> None:
        self.findings.append({"code": code, "path": path, "detail": detail})

    @property
    def errors(self) -> bool:
        return any(CODES[f["code"]] == EXIT_ERROR for f in self.findings)


def _check_entry_dir(d: Path, name: str, rel: str) -> tuple[dict[str, Any] | None, str, bool]:
    """(entry, fel, oförseglad) för en objekt- eller revisionskatalog med <namn> och <namn>.CHECKSUM."""
    st = os.lstat(d)
    if not stat.S_ISDIR(st.st_mode):
        return None, "inte en katalog", False
    if sorted(os.listdir(d)) != sorted([name, name + ".CHECKSUM"]):
        return None, "fel innehåll", False
    for f in (name, name + ".CHECKSUM"):
        fst = os.lstat(d / f)
        if not stat.S_ISREG(fst.st_mode) or stat.S_IMODE(fst.st_mode) != 0o444:
            return None, f"{f}: inte en vanlig fil med 0444", False
    cbytes = _read(d / (name + ".CHECKSUM"))
    try:
        value = parse_checksum(cbytes, name)
    except ChecksumFormatError as e:
        return None, f"CHECKSUM: {e}", False
    sha, size = _sha256_file(d / name)
    if sha != value:
        return None, "sha256 skiljer sig från CHECKSUM", False
    entry = {"path": rel, "size": size, "sha256": sha, "checksum": value,
             "checksum_file_sha256": hashlib.sha256(cbytes).hexdigest()}
    return entry, "", stat.S_IMODE(st.st_mode) != 0o555


def _walk(base: Path, is_dir: Callable[[str], bool], is_leaf: Callable[[str], bool], scan: Scan,
          label: str) -> list[str]:
    """Lövkataloger under base (relativa sökvägar, sorterade). Allt som varken är en tillåten katalog eller
    ett löv blir UNKNOWN_ENTRY; symlänkar följs aldrig."""
    found: list[str] = []
    if not os.path.lexists(base):
        return found
    stack = [""]
    while stack:
        rel = stack.pop()
        for entry in sorted(os.listdir(base / rel)):
            sub = f"{rel}{entry}"
            is_real_dir = stat.S_ISDIR(os.lstat(base / sub).st_mode)
            if is_real_dir and is_leaf(sub):
                found.append(sub)
            elif is_real_dir and is_dir(sub + "/"):
                stack.append(sub + "/")
            else:
                scan.add("UNKNOWN_ENTRY", f"{label}/{sub}")
    return sorted(found)


def scan_archive(root: Path) -> Scan:
    """Full, skrivfri kontroll av ett arkiv med giltig markör."""
    scan = Scan()
    for entry in sorted(os.listdir(root)):
        st = os.lstat(root / entry)
        if entry not in ROOT_ENTRIES:
            scan.add("UNKNOWN_ENTRY", entry)
        elif entry in DIRS and not stat.S_ISDIR(st.st_mode):
            scan.add("UNKNOWN_ENTRY", entry, "inte en katalog")
        elif entry == ".lock" and not stat.S_ISREG(st.st_mode):
            scan.add("UNKNOWN_ENTRY", entry, "inte en vanlig fil")
    tg = {t.path: t for t in targets()}
    prefixes = {t.directory[: i + 1] for t in tg.values() for i, c in enumerate(t.directory) if c == "/"}
    # objects/<arkivkatalog>/<namn>/
    for rel in _walk(root / "objects", prefixes.__contains__, tg.__contains__, scan, "objects"):
        t = tg[rel]
        info, err, unsealed = _check_entry_dir(root / "objects" / rel, t.name, rel)
        if info is None:
            scan.add("OBJECT_INVALID", f"objects/{rel}", err)
            continue
        if unsealed:
            scan.unsealed.append(f"objects/{rel}")
            scan.add("OBJECT_DIR_MODE", f"objects/{rel}")
        scan.originals[rel] = {**info, "series": t.series, "url": t.url, "kind": "original"}
    # revisions/<arkivkatalog>/<namn>/<sha256>/
    def rev_leaf(sub: str) -> bool:
        path, _, sha = sub.rpartition("/")
        return path in tg and SHA_RE.fullmatch(sha) is not None

    for rel in _walk(root / "revisions", lambda d: d in prefixes or d[:-1] in tg, rev_leaf, scan, "revisions"):
        path, _, sha = rel.rpartition("/")
        t = tg[path]
        info, err, unsealed = _check_entry_dir(root / "revisions" / rel, t.name, path)
        if info is None or info["sha256"] != sha or path not in scan.originals \
                or scan.originals[path]["sha256"] == sha:
            scan.add("REVISION_INVALID", f"revisions/{rel}", err or "sha256 passar inte katalogen eller originalet")
            continue
        if unsealed:
            scan.unsealed.append(f"revisions/{rel}")
            scan.add("OBJECT_DIR_MODE", f"revisions/{rel}")
        scan.revisions.setdefault(path, []).append({**info, "series": t.series, "url": t.url, "kind": "revision"})
    # tmp/
    if os.path.lexists(root / "tmp"):
        for entry in sorted(os.listdir(root / "tmp")):
            p = root / "tmp" / entry
            ok = STAGE_RE.fullmatch(entry) is not None and stat.S_ISDIR(os.lstat(p).st_mode) and all(
                stat.S_ISREG(os.lstat(p / f).st_mode) for f in os.listdir(p))
            if ok:
                scan.stages.append(entry)
                scan.add("TMP_LEFTOVER", f"tmp/{entry}")
            else:
                scan.add("TMP_UNKNOWN", f"tmp/{entry}")
    # inventory/<sha256>.json
    if os.path.lexists(root / "inventory"):
        for entry in sorted(os.listdir(root / "inventory")):
            p = root / "inventory" / entry
            m = INVENTORY_RE.fullmatch(entry)
            st = os.lstat(p)
            if m is None or not stat.S_ISREG(st.st_mode) or stat.S_IMODE(st.st_mode) != 0o444 \
                    or hashlib.sha256(_read(p)).hexdigest() != m.group(1):
                scan.add("INVENTORY_CORRUPT", f"inventory/{entry}")
            else:
                scan.inventories.add(m.group(1))
    # runs/<seq>.json
    if os.path.lexists(root / "runs"):
        records = []
        for entry in sorted(os.listdir(root / "runs")):
            p = root / "runs" / entry
            m = RUN_RE.fullmatch(entry)
            st = os.lstat(p)
            rec: Any = None
            if m is not None and stat.S_ISREG(st.st_mode) and stat.S_IMODE(st.st_mode) == 0o444:
                try:
                    rec = json.loads(_read(p))
                except ValueError:
                    rec = None
            if not isinstance(rec, dict) or rec.get("format") != RUN_FORMAT or m is None \
                    or rec.get("seq") != int(m.group(1)):
                scan.add("RUN_CORRUPT", f"runs/{entry}")
            else:
                records.append(rec)
        if records:
            scan.last_run = max(records, key=lambda r: r["seq"])
    return scan


def add_missing(scan: Scan) -> None:
    for t in targets():
        if t.path not in scan.originals and not any(f["path"] == f"objects/{t.path}" for f in scan.findings):
            scan.add("MISSING", t.path)


def inventory_bytes(scan: Scan) -> bytes:
    entries = [*scan.originals.values(), *(e for revs in scan.revisions.values() for e in revs)]
    entries.sort(key=lambda e: (e["path"], e["kind"] != "original", e["sha256"]))
    doc = {"format": INVENTORY_FORMAT,
           "scope": {"first_month": months()[0], "last_month": months()[-1], "targets": len(targets()),
                     "series": [{"id": s, "directory": d, "pattern": p} for s, d, p in SERIES]},
           "counts": {"originals": len(scan.originals), "revisions": sum(map(len, scan.revisions.values())),
                      "missing": len(targets()) - len(scan.originals)},
           "entries": entries}
    return canonical_json_bytes(doc)


def _finish_scan(scan: Scan) -> None:
    """MISSING och INVENTORY_BEHIND (bara när inga fel finns: förteckningen ska då beskriva arkivet)."""
    add_missing(scan)
    if not scan.errors and scan.originals:
        sha = hashlib.sha256(inventory_bytes(scan)).hexdigest()
        if scan.last_run is None or scan.last_run.get("inventory_sha256") != sha or sha not in scan.inventories:
            scan.add("INVENTORY_BEHIND", "inventory", sha)


def exit_of(findings: list[dict[str, str]]) -> int:
    return max((CODES[f["code"]] for f in findings), default=EXIT_OK)


# ------------------------------------------------------------------ rot och lås
def check_root(root: Path, repo: Path | None = None) -> tuple[Path, bool]:
    """(realpath, finns). Vägrar symlänkad rot, rot i repot (standard: ROOT), saknad förälder och en
    befintlig katalog utan ARCHIVE.json."""
    repo = ROOT if repo is None else repo
    if os.path.islink(root):
        raise RefusedError("roten är en symlänk")
    r, rp = Path(os.path.realpath(root)), Path(os.path.realpath(repo))
    if r == rp or rp in r.parents:
        raise RefusedError("roten ligger i repots arbetskatalog")
    if not os.path.lexists(r):
        if not r.parent.is_dir():
            raise RefusedError("roten saknar förälderkatalog")
        return r, False
    if not r.is_dir():
        raise RefusedError("roten är inte en katalog")
    if not os.path.lexists(r / "ARCHIVE.json"):
        raise RefusedError("befintlig katalog utan ARCHIVE.json; används aldrig")
    return r, True


def marker_ok(root: Path) -> bool:
    p = root / "ARCHIVE.json"
    st = os.lstat(p)
    return stat.S_ISREG(st.st_mode) and stat.S_IMODE(st.st_mode) == 0o444 \
        and _read(p) == canonical_json_bytes({"format": ARCHIVE_FORMAT})


def init_root(root: Path) -> None:
    root.mkdir()
    _fsync_dir(root.parent)
    fd = os.open(root / "ARCHIVE.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o444)
    try:
        os.write(fd, canonical_json_bytes({"format": ARCHIVE_FORMAT}))
        os.fsync(fd)
    finally:
        os.close(fd)
    _fsync_dir(root)


def ensure_dirs(root: Path) -> None:
    for d in DIRS:
        if not os.path.lexists(root / d):
            (root / d).mkdir()
            _fsync_dir(root)


@contextmanager
def writer_lock(root: Path) -> Iterator[None]:
    fd = os.open(root / ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RefusedError("en annan skrivare håller låset") from None
        yield
    finally:
        os.close(fd)


# ------------------------------------------------------------------ hämtning
@dataclass
class Session:
    root: Path
    fetcher: Fetcher
    sleep: Callable[[float], None]
    requests: int = 0
    failures: list[dict[str, Any]] = field(default_factory=list)

    def stream(self, url: str, dest: Path, limit: int) -> tuple[str, int]:
        self.requests += 1
        return _stream(self.fetcher, url, dest, limit)

    def get(self, url: str, limit: int) -> tuple[bytes, int]:
        """Liten fil till minnet med samma omförsöksregel som download; (bytes, försök)."""
        attempts = 0
        while True:
            attempts += 1
            self.requests += 1
            try:
                return _fetch_bytes(self.fetcher, url, limit), attempts
            except FetchError as e:
                if e.retry and attempts < NETWORK_ATTEMPTS:
                    self.sleep(RETRY_PAUSE_S)
                    continue
                raise _Exhausted(e, attempts) from None

    def new_stage(self) -> Path:
        stage = self.root / "tmp" / f"stage-{uuid.uuid4().hex}"
        stage.mkdir()
        return stage


def download(s: Session, t: Target, target_dir: Path, checksum: bytes | None = None) -> dict[str, Any] | None:
    """Hämtar t till target_dir (steg 1-6). checksum: redan hämtad CHECKSUM (recheck). None vid misslyckande."""
    net_failures = mismatches = 0
    while True:
        stage = s.new_stage()                                                  # 1
        try:
            cpath = stage / (t.name + ".CHECKSUM")
            if checksum is None:
                s.stream(t.checksum_url, cpath, MAX_CHECKSUM_BYTES)            # 2
            else:
                fd = os.open(cpath, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
                try:
                    os.write(fd, checksum)
                    os.fsync(fd)
                finally:
                    os.close(fd)
            value = parse_checksum(_read(cpath), t.name)
            sha, size = s.stream(t.url, stage / t.name, MAX_DATA_BYTES)        # 3
            if sha != value:                                                   # 4
                mismatches += 1
                _remove_stage(stage)
                if mismatches < MISMATCH_ATTEMPTS:
                    continue
                s.failures.append({"path": t.path, "kind": "checksum_mismatch", "status": None,
                                   "attempts": net_failures + mismatches})
                return None
            for f in (cpath, stage / t.name):                                  # 5
                os.chmod(f, 0o444)
            _publish_dir(s.root, stage, target_dir)                            # 6
            return {"path": t.path, "size": size, "sha256": sha}
        except ChecksumFormatError as e:
            _remove_stage(stage)
            s.failures.append({"path": t.path, "kind": "checksum_format", "status": None, "detail": str(e),
                               "attempts": net_failures + mismatches + 1})
            return None
        except FetchError as e:
            _remove_stage(stage)
            net_failures += 1
            if e.retry and net_failures < NETWORK_ATTEMPTS:
                s.sleep(RETRY_PAUSE_S)
                continue
            s.failures.append({"path": t.path, "kind": e.kind, "status": e.status,
                               "attempts": net_failures + mismatches})
            return None


# ------------------------------------------------------------------ kommandon
@dataclass(frozen=True)
class Out:
    result: dict[str, Any]
    exit_code: int


def _scan_result(scan: Scan) -> dict[str, Any]:
    return {"findings": scan.findings, "counts": {
        "targets": len(targets()), "originals": len(scan.originals),
        "revisions": sum(map(len, scan.revisions.values())), "tmp_leftovers": len(scan.stages)}}


def _open_existing(root: Path) -> Path:
    r, exists = check_root(root)
    if not exists:
        raise RefusedError("roten finns inte")
    return r


def cmd_plan(root: Path) -> Out:
    r, exists = check_root(root)
    if not exists:
        res = {"root_exists": False, "findings": [{"code": "MISSING", "path": t.path, "detail": ""} for t in targets()],
               "counts": {"targets": len(targets()), "originals": 0, "revisions": 0, "tmp_leftovers": 0}}
        return Out(res, EXIT_INCOMPLETE)
    if not marker_ok(r):
        return Out({"root_exists": True, "findings": [{"code": "MARKER_INVALID", "path": "ARCHIVE.json", "detail": ""}]},
                   EXIT_ERROR)
    scan = scan_archive(r)
    add_missing(scan)
    return Out({"root_exists": True, **_scan_result(scan)}, exit_of(scan.findings))


def cmd_verify(root: Path) -> Out:
    r = _open_existing(root)
    if not marker_ok(r):
        return Out({"findings": [{"code": "MARKER_INVALID", "path": "ARCHIVE.json", "detail": ""}]}, EXIT_ERROR)
    scan = scan_archive(r)
    _finish_scan(scan)
    res = _scan_result(scan)
    res["inventory_sha256"] = hashlib.sha256(inventory_bytes(scan)).hexdigest() if not scan.errors else None
    return Out(res, exit_of(scan.findings))


def _clean_tmp(root: Path, scan: Scan) -> list[str]:
    for name in scan.stages:
        _remove_stage(root / "tmp" / name)
    if scan.stages:
        _fsync_dir(root / "tmp")
    return list(scan.stages)


def _publish_inventory(root: Path, data: bytes) -> str:
    sha = hashlib.sha256(data).hexdigest()
    _publish_file(root, data, root / "inventory" / f"{sha}.json")
    return sha


def _publish_run(root: Path, rec: dict[str, Any]) -> None:
    if not _publish_file(root, canonical_json_bytes(rec), root / "runs" / f"{rec['seq']:06d}.json"):
        raise FileExistsError(f"körningsposten {rec['seq']} finns redan")


def _prepare_apply(root: Path, git: GitState, create: bool) -> tuple[Path, dict[str, Any]]:
    r, exists = check_root(root)
    if not exists and not create:
        raise RefusedError("roten finns inte")
    version, clean = git()
    if clean is not True:
        raise RefusedError("--apply kräver en ren arbetskatalog")
    if exists and not marker_ok(r):
        raise MarkerInvalidError("ARCHIVE.json är ogiltig")
    if not exists:
        init_root(r)
    return r, {"code_version": version, "worktree_clean": clean}


def _run_apply(command: str, root: Path, ctx: dict[str, Any], clock: Clock,
               body: Callable[[Session, Scan], dict[str, Any]], fetcher: Fetcher,
               sleep: Callable[[float], None]) -> Out:
    started = clock()
    with writer_lock(root):
        ensure_dirs(root)
        scan = scan_archive(root)
        if any(CODES[f["code"]] == EXIT_ERROR for f in scan.findings):
            return Out(_scan_result(scan), EXIT_ERROR)                          # före varje anrop, inget skrivs
        removed = _clean_tmp(root, scan)
        for rel in scan.unsealed:                                               # innehållet är redan verifierat
            _seal_dir(root / rel)
        s = Session(root, fetcher, sleep)
        extra = body(s, scan)
        final = scan_archive(root)
        _finish_scan(final)
        inv = inventory_bytes(final)
        sha = _publish_inventory(root, inv)
        runs = [int(m.group(1)) for e in os.listdir(root / "runs") if (m := RUN_RE.fullmatch(e))]
        finished = clock()
        missing = [f["path"] for f in final.findings if f["code"] == "MISSING"]
        code = extra.pop("exit_code", EXIT_INCOMPLETE if missing or s.failures else EXIT_OK)
        rec = {"format": RUN_FORMAT, "seq": max(runs, default=0) + 1, "run_id": uuid.uuid4().hex,
               "command": command, "started_at": started.isoformat(), "finished_at": finished.isoformat(),
               "duration_s": round((finished - started).total_seconds(), 3), "code": ctx,
               "tmp_removed": removed, "sealed": scan.unsealed, "requests": s.requests, "failures": s.failures,
               "inventory_sha256": sha, "exit_code": code, **extra}
        _publish_run(root, rec)
    res = {"run": {k: rec[k] for k in sorted(rec) if k not in ("started_at", "finished_at", "duration_s", "run_id")},
           "counts": {"targets": len(targets()), "originals": len(final.originals),
                      "revisions": sum(map(len, final.revisions.values())), "missing": len(missing)}}
    return Out(res, code)


def cmd_fetch(root: Path, apply: bool, git: GitState, clock: Clock, fetcher: Fetcher,
              sleep: Callable[[float], None]) -> tuple[Out, dict[str, Any] | None]:
    if not apply:
        return cmd_plan(root), None
    r, ctx = _prepare_apply(root, git, create=True)

    def body(s: Session, scan: Scan) -> dict[str, Any]:
        downloaded = []
        for t in targets():
            if t.path in scan.originals:
                continue
            got = download(s, t, r / "objects" / t.path)
            if got is not None:
                downloaded.append(got["path"])
        return {"downloaded": downloaded}

    return _run_apply("fetch", r, ctx, clock, body, fetcher, sleep), ctx


def _recheck(s: Session, scan: Scan, apply: bool) -> dict[str, Any]:
    changes: list[dict[str, Any]] = []
    for path in sorted(scan.originals):
        t = next(x for x in targets() if x.path == path)
        orig = scan.originals[path]
        known = {orig["sha256"], *(e["sha256"] for e in scan.revisions.get(path, []))}
        try:
            cbytes, attempts = s.get(t.checksum_url, MAX_CHECKSUM_BYTES)
        except _Exhausted as x:
            s.failures.append({"path": path, "kind": x.error.kind, "status": x.error.status, "attempts": x.attempts})
            continue
        try:
            value = parse_checksum(cbytes, t.name)
        except ChecksumFormatError as e:
            s.failures.append({"path": path, "kind": "checksum_format", "status": None, "detail": str(e),
                               "attempts": attempts})
            continue
        if value == orig["sha256"]:
            if hashlib.sha256(cbytes).hexdigest() != orig["checksum_file_sha256"]:
                changes.append({"path": path, "change": "CHECKSUM_FILE_CHANGED", "sha256": value})
            continue
        if value in known:
            changes.append({"path": path, "change": "REVISION_KNOWN", "sha256": value})
            continue
        if not apply:
            changes.append({"path": path, "change": "REVISION_AVAILABLE", "sha256": value})
            continue
        got = download(s, t, s.root / "revisions" / path / value, checksum=cbytes)
        if got is not None:
            changes.append({"path": path, "change": "REVISION_STORED", "sha256": got["sha256"]})
    return {"changes": changes}


def cmd_recheck(root: Path, apply: bool, git: GitState, clock: Clock, fetcher: Fetcher,
                sleep: Callable[[float], None]) -> tuple[Out, dict[str, Any] | None]:
    if not apply:
        r = _open_existing(root)
        if not marker_ok(r):
            return Out({"findings": [{"code": "MARKER_INVALID", "path": "ARCHIVE.json", "detail": ""}]},
                       EXIT_ERROR), None
        scan = scan_archive(r)
        if scan.errors:
            return Out(_scan_result(scan), EXIT_ERROR), None
        s = Session(r, fetcher, sleep)
        res = _recheck(s, scan, apply=False)
        code = EXIT_CHANGED if res["changes"] else (EXIT_INCOMPLETE if s.failures else EXIT_OK)
        return Out({**res, "failures": s.failures, "requests": s.requests}, code), None
    r, ctx = _prepare_apply(root, git, create=False)

    def body(s: Session, scan: Scan) -> dict[str, Any]:
        res = _recheck(s, scan, apply=True)
        res["exit_code"] = EXIT_CHANGED if res["changes"] else (EXIT_INCOMPLETE if s.failures else EXIT_OK)
        return res

    return _run_apply("recheck", r, ctx, clock, body, fetcher, sleep), ctx


# ------------------------------------------------------------------ CLI
class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise UsageError(message)


def _parser() -> argparse.ArgumentParser:
    ap = _Parser(prog="fetch_binance.py", description="Binance-arkivets månadsfiler (etapp B steg 2).",
                 allow_abbrev=False)
    sub = ap.add_subparsers(dest="command", required=True, parser_class=_Parser)
    for name, apply in (("plan", False), ("fetch", True), ("verify", False), ("recheck", True)):
        p = sub.add_parser(name, allow_abbrev=False)
        p.add_argument("--root", required=True, type=Path)
        if apply:
            p.add_argument("--apply", action="store_true")
    return ap


def git_state(root: Path = ROOT) -> tuple[str, bool | None]:
    """(commit, ren arbetskatalog); ('unknown', None) om git saknas eller inte är ett repo."""
    try:
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=False)
        status = subprocess.run(["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True,
                                check=False)
    except OSError:
        return "unknown", None
    if head.returncode != 0 or status.returncode != 0:
        return "unknown", None
    return head.stdout.strip(), status.stdout.strip() == ""


def _envelope(command: str | None, applied: bool, code: int, context: dict[str, Any] | None,
              result: dict[str, Any] | None, error: dict[str, str] | None) -> dict[str, Any]:
    return {"format": CLI_FORMAT, "command": command, "applied": applied, "exit_code": code, "context": context,
            "result": result,
            "result_sha256": None if result is None else hashlib.sha256(canonical_json_bytes(result)).hexdigest(),
            "error": error}


def _clean(text: str, root: Path | None) -> str:
    if root is None:
        return text
    for form in sorted({os.path.abspath(root), os.path.realpath(root)}, key=len, reverse=True):
        if form != os.sep:
            text = text.replace(form, "<root>")
    return text


def main(argv: list[str] | None = None, *, fetcher: Fetcher | None = None, clock: Clock | None = None,
         git: GitState | None = None, sleep: Callable[[float], None] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    try:
        a = _parser().parse_args(argv)
    except UsageError as e:
        sys.stdout.buffer.write(canonical_json_bytes(
            _envelope(None, False, EXIT_USAGE, None, None, {"type": "UsageError", "message": str(e)})))
        return EXIT_USAGE
    applied = bool(getattr(a, "apply", False))
    f, c = fetcher or HttpFetcher(), clock or (lambda: datetime.now(UTC))
    g, sl = git or git_state, sleep or time.sleep
    ctx: dict[str, Any] | None = None
    try:
        if a.command == "plan":
            out = cmd_plan(a.root)
        elif a.command == "verify":
            out = cmd_verify(a.root)
        elif a.command == "fetch":
            out, ctx = cmd_fetch(a.root, applied, g, c, f, sl)
        else:
            out, ctx = cmd_recheck(a.root, applied, g, c, f, sl)
        env = _envelope(a.command, applied, out.exit_code, ctx, out.result, None)
    except MarkerInvalidError:
        env = _envelope(a.command, applied, EXIT_ERROR, ctx,
                        {"findings": [{"code": "MARKER_INVALID", "path": "ARCHIVE.json", "detail": ""}]}, None)
    except RefusedError as e:
        env = _envelope(a.command, applied, EXIT_REFUSED, ctx, None,
                        {"type": "RefusedError", "message": _clean(str(e), a.root)})
    except Exception as e:  # noqa: BLE001 - CLI-gräns: allt annat ger exit 70
        sys.stderr.write(_clean(traceback.format_exc(), a.root))
        env = _envelope(a.command, applied, EXIT_INTERNAL, ctx, None,
                        {"type": type(e).__name__, "message": _clean(str(e), a.root)})
    sys.stdout.buffer.write(canonical_json_bytes(env))
    sys.stdout.flush()
    return int(env["exit_code"])


if __name__ == "__main__":
    sys.exit(main())
