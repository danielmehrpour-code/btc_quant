"""Djup verifiering och begränsad reparation av versionslagret (PHASE 4, steg 5).

VERIFY (verify_store) är strikt läsande: skriver, skapar, byter namn på eller ändrar
rättigheter för ingenting och öppnar aldrig katalogen skrivbart. lock=True tar LOCK_SH|LOCK_NB
på den BEFINTLIGA .lock (aldrig O_CREAT); håller en skrivare låset -> StoreLockedError direkt,
ingen partiell rapport. lock=False (eller saknad .lock) -> rapporten bär "best_effort": true.
Alla objekt verifieras fullt med lagrets egen _verify_object (alla hashar, content_hash,
OHLCV-validering); katalogen litas aldrig på. Rapporten listar fynd OCH vilka kontroller som
utförts (scope). Ingen väggklocka, inga absoluta sökvägar: samma lager -> samma bytes.

Allvarlighet och exit-status (värsta fyndet avgör):
  ERROR 3 (repareras aldrig), PENDING 2 (endast explicit slutförande), REPAIRABLE 1,
  WARN 1 (repareras inte), INFO 0.

KATALOGEN SOM VITTNE
  Katalogen är härledd men också det enda andra vittnet till oföränderlig historik.
  Rader som saknas i katalogen (katalogen ligger efter filerna) -> REPAIRABLE.
  Rader som filerna saknar eller som avviker för oföränderliga entiteter -> ERROR
  (CATALOG_CONFLICT); katalogen byggs aldrig om över dem.
  Härledning = det en skrivare har synkat efter avslutade operationer: väntande poster
  (en D1-väntande ref_log-rad, en väntande ingest och ingest-poster länkade till en
  väntande ref_log-rad) ingår inte.

REPAIR (repair_store) är dry-run som standard (apply=False). Ordning under skrivarlåset
(_WriterLock, utan att öppna katalogen):
  1. verify (färsk, under låset)   2. plan   3. ERROR -> RepairRefusedError, inget ändras
  4. hot journal rullas tillbaka av SQLite (öppna skrivbart en gång)
  5. katalogen byggs om i tmp/ och ersätts atomiskt (catalog.rebuild_catalog)
  6. tmp/-rester tas bort post för post (bara kända mönster; en fils rättigheter ändras
     aldrig, symlänkar följs aldrig, bara kataloger INOM tmp/ får u+w); fsync av tmp/
  7. verify igen
Repair skriver aldrig utanför tmp/ förutom catalog.sqlite (och dess journal via SQLite),
rör aldrig objects/, refs/, ref_log.jsonl, snapshots/, ingests/ eller STORE.json och slutför
aldrig väntande ref-uppdateringar eller ingests.
Repair rör heller aldrig quality/ (steg 6b).

IMPORTER (steg 6): imports/<import_id>.json verifieras strikt (legacy.py); varje importpost
måste peka på ett befintligt objekt och stämma med dess manifest (bas- och innehållsnycklar),
och varje ref_log-post med action=import_legacy måste ha en importpost. Importposter har ingen
katalogtabell. Repair rör aldrig imports/.

KVALITETSRAPPORTER (steg 6b): quality/<report_sha256>.quality.json verifieras strikt
(quality_registry.py: namn == sha256 av bytena, 0444, strikt tolkning, Phase 3:s load_report,
låst config); varje datasetpost måste peka på ett befintligt objekt och stämma med dess manifest
(rows, first/last open_time). quality_reports/quality_report_datasets härleds från filerna,
jämförs och byggs om som övriga tabeller. En rapport räknas som referens till sina objekt.
Repair rör aldrig quality/.

REBUILD (steg 8, rebuild_catalog_store) bygger om den härledda katalogen OVILLKORLIGT, dry-run som
standard (apply=False). Samma skydd som repair: skrivarlåset (_WriterLock, utan att öppna
katalogen; symlänkad .lock vägras), färsk verify under låset, ERROR -> RepairRefusedError och
ingenting ändras. Ombyggnaden använder steg 5-mekanismen (_rebuild_catalog: hot journal rullas
tillbaka, bygge i tmp/, atomisk ersättning), sedan verify igen. Resultatet bär katalogdumpens
sha256 före och efter och om de är identiska (deterministisk dump/rebuild). Rebuild rör aldrig
tmp/-rester (bara sin egen staging) och inget annat än catalog.sqlite.

FUNDING (etapp B steg 4, SB-S4-a A2): objects/binance/perp/<SYMBOL>/funding/<data_version>/ verifieras
fullt med lagrets _verify_funding_object; funding_datasets, funding_versions, funding_version_sources
och snapshot_funding_members härleds från objekten och snapshotfilerna (v2) och jämförs som övriga
tabeller. En funding-version refereras bara av snapshots.

MIGRERING (SB-S4-MIG): en oskadad katalog med äldre schema ger CATALOG_MIGRATION_REQUIRED (PENDING,
exit 2): den kräver ett explicit migrate_catalog och jämförs inte rad för rad innan dess. Repair och
rebuild vägrar då med apply=True (RepairRefusedError), så att ingen ombyggnad migrerar katalogen i
smyg; de bygger aldrig om en katalog vars schema bara är äldre.
"""
from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from btc_trading_system.data import funding as fd
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data_validation.errors import DataError
from btc_trading_system.database import catalog
from btc_trading_system.database import legacy as lg
from btc_trading_system.database import quality_registry as qreg
from btc_trading_system.database import versioned_store as vs
from btc_trading_system.database.lineage import IngestRecord
from btc_trading_system.database.refs import compact_json_bytes

REPORT_FORMAT = "btc-quant/verify-report/v1"
SEVERITY_EXIT = {"ERROR": 3, "PENDING": 2, "REPAIRABLE": 1, "WARN": 1, "INFO": 0}
_SEVERITY_RANK = {"ERROR": 0, "PENDING": 1, "REPAIRABLE": 2, "WARN": 3, "INFO": 4}

# Fyndkod -> allvarlighet (en enda tabell; testas mot designen).
CODES: dict[str, str] = {
    "ROOT_LAYOUT": "ERROR",
    "STORE_FILE_INVALID": "ERROR",
    "ROOT_FOREIGN_ENTRY": "WARN",
    "LOCK_MISSING": "WARN",
    "SYMLINK": "ERROR",
    "OBJECT_LAYOUT": "ERROR",
    "OBJECT_CORRUPT": "ERROR",
    "OBJECT_DIR_MODE": "WARN",
    "OBJECT_UNREGISTERED": "REPAIRABLE",
    "OBJECT_UNREFERENCED": "INFO",
    "REF_LOG_CORRUPT": "ERROR",
    "REF_FILE_CORRUPT": "ERROR",
    "REF_FOREIGN_ENTRY": "ERROR",
    "REF_TARGET_MISSING": "ERROR",
    "PENDING_REF_UPDATE": "PENDING",
    "SNAPSHOT_CORRUPT": "ERROR",
    "SNAPSHOT_MEMBER_MISSING": "ERROR",
    "INGEST_CORRUPT": "ERROR",
    "INGEST_LOG_MISMATCH": "ERROR",
    "INGEST_TARGET_MISSING": "ERROR",
    "PENDING_INGEST": "PENDING",
    "IMPORT_CORRUPT": "ERROR",
    "IMPORT_TARGET_MISSING": "ERROR",
    "IMPORT_LOG_MISMATCH": "ERROR",
    "QUALITY_CORRUPT": "ERROR",
    "QUALITY_TARGET_MISSING": "ERROR",
    "QUALITY_BINDING_MISMATCH": "ERROR",
    "CATALOG_MISSING": "REPAIRABLE",
    "CATALOG_UNREADABLE": "REPAIRABLE",
    "CATALOG_NEWER_SCHEMA": "ERROR",
    "CATALOG_MIGRATION_REQUIRED": "PENDING",
    "CATALOG_STRUCTURE": "REPAIRABLE",
    "CATALOG_BEHIND": "REPAIRABLE",
    "CATALOG_CONFLICT": "ERROR",
    "TMP_LEFTOVER": "REPAIRABLE",
    "TMP_UNKNOWN": "WARN",
}
REBUILD_CODES = frozenset({"CATALOG_MISSING", "CATALOG_UNREADABLE", "CATALOG_STRUCTURE",
                           "CATALOG_BEHIND", "OBJECT_UNREGISTERED"})

# Katalogtabeller i FK-ordning, med kolumner och primärnyckelns längd (första kolumnerna).
CATALOG_TABLES: tuple[tuple[str, tuple[str, ...], int], ...] = (
    ("datasets", ("dataset_id", "exchange", "instrument", "symbol", "timeframe"), 1),
    ("dataset_versions", vs._DV_COLUMNS, 2),
    ("version_sources", vs._VS_COLUMNS, 3),
    ("funding_datasets", ("dataset_id", "exchange", "instrument", "symbol"), 1),
    ("funding_versions", vs._FV_COLUMNS, 2),
    ("funding_version_sources", vs._FVS_COLUMNS, 3),
    ("ref_log", ("seq", "entry_sha256", "prev_entry_sha256", "dataset_id", "data_version",
                 "previous_data_version", "action"), 1),
    ("refs", ("dataset_id", "data_version", "ref_log_seq", "ref_file_sha256"), 1),
    ("snapshots", ("snapshot_id", "snapshot_file_sha256", "member_count"), 1),
    ("snapshot_members", ("snapshot_id", "dataset_id", "data_version"), 2),
    ("snapshot_funding_members", ("snapshot_id", "dataset_id", "data_version"), 2),
    ("ingest_events", ("seq", "ingest_id", "record_sha256", "dataset_id", "data_version",
                       "previous_data_version", "params_json", "code_version", "worktree_clean",
                       "publication", "ref_outcome", "ref_log_seq", "duplicates_dropped"), 1),
    ("ingest_sources", ("ingest_seq", "source_index", "name", "sha256", "rows"), 2),
    ("quality_reports", qreg.QR_COLUMNS, 1),
    ("quality_report_datasets", qreg.QRD_COLUMNS, 2),
)
_REF_TABLES = frozenset({"ref_log", "refs", "ingest_events", "ingest_sources"})

SCOPE_CHECKS: tuple[str, ...] = (
    "root.layout", "root.store_file", "root.lock",
    "objects.layout", "objects.verify_full", "objects.dir_mode", "objects.references",
    "ref_log.parse", "refs.files", "refs.consistency", "refs.targets",
    "snapshots.files", "snapshots.members",
    "ingests.chain", "ingests.ref_log", "ingests.targets",
    "imports.records", "imports.targets", "imports.ref_log",
    "quality.files", "quality.targets",
    "catalog.open", "catalog.structure_full",
    *(f"catalog.rows.{t}" for t, _, _ in CATALOG_TABLES),
    "tmp.entries",
)

_ROOT_DIRS = frozenset({"objects", "refs", "snapshots", "ingests", "imports", "quality", "tmp"})
_ROOT_FILES = frozenset({vs.STORE_FILE, vs.LOCK_FILE, vs.REF_LOG_FILE, vs.CATALOG_FILE,
                         vs.CATALOG_FILE + "-journal"})
_H = r"[0-9a-f]{32}"
_TMP_DIR_RE = re.compile(rf"{_H}")
_TF = "|".join(re.escape(tf) for tf in catalog.SCHEMA_TIMEFRAMES)
_TMP_FILE_RE = re.compile(
    rf"{_H}\.ingest\.json|{_H}\.snapshot\.json|{_H}\.ref_log\.jsonl|{_H}\.(?:{_TF})\.json"
    rf"|{_H}\.import\.json"
    rf"|{_H}\.quality\.json"
    rf"|\.store-{_H}\.json|\.catalog-{_H}\.sqlite\.tmp(?:-journal|-wal|-shm)?")


class RepairRefusedError(vs.StoreError):
    """repair(apply=True) med ERROR-fynd: ingenting ändras."""

    def __init__(self, message: str, report: VerifyReport) -> None:
        super().__init__(message)
        self.report = report


# ------------------------------------------------------------------ rapport
@dataclass(frozen=True)
class Finding:
    code: str
    subject: str
    detail: str

    def __post_init__(self) -> None:
        if self.code not in CODES:
            raise ValueError(f"okänd fyndkod: {self.code!r}")

    @property
    def severity(self) -> str:
        return CODES[self.code]

    def sort_key(self) -> tuple[int, str, str, str]:
        return (_SEVERITY_RANK[self.severity], self.code, self.subject, self.detail)

    def to_json_obj(self) -> dict[str, str]:
        return {"code": self.code, "severity": self.severity, "subject": self.subject,
                "detail": self.detail}


@dataclass(frozen=True)
class ScopeItem:
    check: str
    status: str                     # "performed" | "skipped"
    reason: str

    def to_json_obj(self) -> dict[str, str]:
        return {"check": self.check, "status": self.status, "reason": self.reason}


@dataclass(frozen=True)
class VerifyReport:
    findings: tuple[Finding, ...]
    scope: tuple[ScopeItem, ...]
    counts: tuple[tuple[str, int], ...]
    catalog_dump_sha256: str | None
    best_effort: bool

    @property
    def exit_status(self) -> int:
        return max((SEVERITY_EXIT[f.severity] for f in self.findings), default=0)

    @property
    def codes(self) -> frozenset[str]:
        return frozenset(f.code for f in self.findings)

    def by_code(self, code: str) -> tuple[Finding, ...]:
        return tuple(f for f in self.findings if f.code == code)

    def to_json_obj(self) -> dict[str, Any]:
        return {"format": REPORT_FORMAT, "best_effort": self.best_effort,
                "exit_status": self.exit_status,
                "scope": [s.to_json_obj() for s in self.scope],
                "counts": dict(self.counts),
                "catalog_dump_sha256": self.catalog_dump_sha256,
                "findings": [f.to_json_obj() for f in self.findings]}

    def to_json_bytes(self) -> bytes:
        return (json.dumps(self.to_json_obj(), sort_keys=True, indent=2) + "\n").encode()

    @property
    def report_sha256(self) -> str:
        return hashlib.sha256(self.to_json_bytes()).hexdigest()


@dataclass(frozen=True)
class RepairPlan:
    tmp_removals: tuple[str, ...]          # tmp/<namn>, sorterade
    rebuild_catalog: bool
    blocking: tuple[Finding, ...]          # alla ERROR-fynd


@dataclass(frozen=True)
class RepairResult:
    before: VerifyReport
    plan: RepairPlan
    applied: bool
    after: VerifyReport | None


@dataclass(frozen=True)
class RebuildResult:
    before: VerifyReport
    blocking: tuple[Finding, ...]          # alla ERROR-fynd (rebuild vägrar då)
    applied: bool
    after: VerifyReport | None

    @property
    def catalog_dump_sha256_before(self) -> str | None:
        return self.before.catalog_dump_sha256

    @property
    def catalog_dump_sha256_after(self) -> str | None:
        return None if self.after is None else self.after.catalog_dump_sha256

    @property
    def identical(self) -> bool | None:
        """None i dry-run; annars True exakt när den ombyggda katalogens dump == den tidigare."""
        if self.after is None:
            return None
        old = self.before.catalog_dump_sha256
        return old is not None and old == self.after.catalog_dump_sha256


# ------------------------------------------------------------------ härledda katalograder
Row = tuple[Any, ...]


@dataclass
class _Derived:
    rows: dict[str, set[Row]]
    excluded_versions: set[tuple[str, str]]          # objekt som finns men inte verifierar
    unknown_tables: dict[str, str]                   # tabell -> skäl (kunde inte härledas)


class _RefOnlyView(vs.VersionedStore):
    """Läsvy som kör lagrets egen strikta ref-tillståndslogik utan ingest-kontrollen,
    så att ref- och ingest-problem rapporteras var för sig."""

    def _check_ingests(self, entries: tuple[vs._LogEntry, ...]) -> None:
        return None


class _Verifier:
    def __init__(self, store: vs.VersionedStore, best_effort: bool) -> None:
        self.store = store
        self.root = store.root
        self.best_effort = best_effort
        self.findings: list[Finding] = []
        self.scope: dict[str, ScopeItem] = {}
        self.counts: dict[str, int] = {}
        self.derived = _Derived({t: set() for t, _, _ in CATALOG_TABLES}, set(), {})
        self.valid_objects: dict[tuple[str, str], DatasetKey] = {}
        self.valid_funding: dict[tuple[str, str], fd.FundingKey] = {}
        self.object_manifests: dict[tuple[str, str], dict[str, Any]] = {}
        self.object_dirs: set[tuple[str, str]] = set()         # alla med giltig layout
        self.referenced: set[tuple[str, str]] = set()
        self.entries: tuple[vs._LogEntry, ...] | None = None
        self.pending_entry: vs._LogEntry | None = None
        self.ref_state_ok = False
        self.catalog_dump: str | None = None

    # ---- hjälp
    def add(self, code: str, subject: str, detail: str = "") -> None:
        detail = detail.replace(str(self.root) + os.sep, "").replace(str(self.root), ".")
        self.findings.append(Finding(code, subject, detail))

    def done(self, check: str) -> None:
        self.scope[check] = ScopeItem(check, "performed", "")

    def skip(self, check: str, reason: str) -> None:
        self.scope[check] = ScopeItem(check, "skipped", reason)

    def rel(self, path: Path) -> str:
        return path.relative_to(self.root).as_posix()

    def report(self) -> VerifyReport:
        missing = [c for c in SCOPE_CHECKS if c not in self.scope]
        if missing:
            raise AssertionError(f"verify: kontroller utan status: {missing}")
        return VerifyReport(
            findings=tuple(sorted(self.findings, key=Finding.sort_key)),
            scope=tuple(self.scope[c] for c in SCOPE_CHECKS),
            counts=tuple(sorted(self.counts.items())),
            catalog_dump_sha256=self.catalog_dump, best_effort=self.best_effort)

    # ---- körning
    def run(self) -> VerifyReport:
        self.check_root()
        self.check_objects()
        self.check_refs()
        self.check_snapshots()
        self.check_ingests()
        self.check_imports()
        self.check_quality()
        self.check_references()
        self.check_catalog()
        self.check_tmp()
        return self.report()

    # ---- rot
    def check_root(self) -> None:
        present: dict[str, os.stat_result] = {}
        for p in sorted(self.root.iterdir()):
            st = os.lstat(p)
            present[p.name] = st
            if stat.S_ISLNK(st.st_mode):
                self.add("SYMLINK", p.name, "symlänk i lagrets rot")
            elif p.name in _ROOT_DIRS:
                if not stat.S_ISDIR(st.st_mode):
                    self.add("ROOT_LAYOUT", p.name, "ska vara en katalog")
            elif p.name in _ROOT_FILES:
                if not stat.S_ISREG(st.st_mode):
                    self.add("ROOT_LAYOUT", p.name, "ska vara en vanlig fil")
            else:
                self.add("ROOT_FOREIGN_ENTRY", p.name, "okänd post i lagrets rot")
        for name in ("objects", "tmp"):
            if name not in present:
                self.add("ROOT_LAYOUT", name, "saknas")
        self.done("root.layout")
        store_st = present.get(vs.STORE_FILE)
        if store_st is None or not stat.S_ISREG(store_st.st_mode):
            self.add("STORE_FILE_INVALID", vs.STORE_FILE, "inte en vanlig fil")
        self.done("root.store_file")
        if vs.LOCK_FILE not in present:
            self.add("LOCK_MISSING", vs.LOCK_FILE, "skrivarlåsfilen saknas")
        self.done("root.lock")

    def _real_dir(self, name: str) -> Path | None:
        p = self.root / name
        try:
            st = os.lstat(p)
        except FileNotFoundError:
            return None
        return p if stat.S_ISDIR(st.st_mode) else None

    # ---- objekt
    def check_objects(self) -> None:
        objects = self._real_dir("objects")
        if objects is None:
            for c in ("objects.layout", "objects.verify_full", "objects.dir_mode"):
                self.skip(c, "objects/ saknas eller är inte en katalog")
            return
        candidates: list[tuple[Path, tuple[str, ...]]] = []
        self._walk_objects(objects, (), candidates)
        self.done("objects.layout")
        for d, parts in candidates:
            subject = self.rel(d)
            if parts[3] == fd.FUNDING_KIND:
                self._check_funding_object(d, parts, subject)
                continue
            try:
                key = catalog.dataset_key(exchange=parts[0], instrument=parts[1], symbol=parts[2],
                                          timeframe=parts[3])
            except ValueError as e:
                self.add("OBJECT_LAYOUT", subject, f"ogiltig datasetnyckel: {e}")
                continue
            dv = parts[4]
            if not catalog.is_sha256_hex(dv):
                self.add("OBJECT_LAYOUT", subject, "katalognamnet är inte en data_version")
                continue
            did = catalog.dataset_id(key)
            self.object_dirs.add((did, dv))
            self.derived.rows["datasets"].add(
                (did, key.exchange, key.instrument.value, key.symbol, key.timeframe))
            if stat.S_IMODE(os.lstat(d).st_mode) != vs.DIR_MODE:
                self.add("OBJECT_DIR_MODE", subject,
                         f"läge {stat.S_IMODE(os.lstat(d).st_mode):o} != {vs.DIR_MODE:o}")
            try:
                info, manifest, _ = self.store._verify_object(key, dv)
            except (DataError, OSError, ValueError) as e:
                self.add("OBJECT_CORRUPT", subject, str(e))
                self.derived.excluded_versions.add((did, dv))
                continue
            self.valid_objects[(did, dv)] = key
            self.object_manifests[(did, dv)] = manifest
            self.derived.rows["dataset_versions"].add(_version_row(info, manifest))
            for i, r in enumerate(vs._parse_sources(manifest.get("sources"))):
                self.derived.rows["version_sources"].add((did, dv, i, r["name"], r["sha256"], r["rows"]))
        self.done("objects.verify_full")
        self.done("objects.dir_mode")
        self.counts["objects_valid"] = len(self.valid_objects)
        self.counts["objects_corrupt"] = len(self.derived.excluded_versions)
        self.counts["datasets"] = len(self.derived.rows["datasets"])
        self.counts["funding_objects_valid"] = len(self.valid_funding)
        self.counts["funding_datasets"] = len(self.derived.rows["funding_datasets"])

    def _check_funding_object(self, d: Path, parts: tuple[str, ...], subject: str) -> None:
        """objects/binance/perp/<SYMBOL>/funding/<dv> (SB-S4-a A2)."""
        try:
            key = catalog.funding_dataset_key(exchange=parts[0], instrument=parts[1], symbol=parts[2])
        except ValueError as e:
            self.add("OBJECT_LAYOUT", subject, f"ogiltig funding-nyckel: {e}")
            return
        dv = parts[4]
        if not catalog.is_sha256_hex(dv):
            self.add("OBJECT_LAYOUT", subject, "katalognamnet är inte en data_version")
            return
        did = catalog.funding_dataset_id(key)
        self.object_dirs.add((did, dv))
        self.derived.rows["funding_datasets"].add((did, key.exchange, key.instrument.value, key.symbol))
        if stat.S_IMODE(os.lstat(d).st_mode) != vs.DIR_MODE:
            self.add("OBJECT_DIR_MODE", subject, f"läge {stat.S_IMODE(os.lstat(d).st_mode):o} != {vs.DIR_MODE:o}")
        try:
            info, manifest, _ = self.store._verify_funding_object(key, dv)
        except (DataError, OSError, ValueError) as e:
            self.add("OBJECT_CORRUPT", subject, str(e))
            self.derived.excluded_versions.add((did, dv))
            return
        self.valid_funding[(did, dv)] = key
        self.derived.rows["funding_versions"].add(vs.funding_version_row(info, manifest))
        self.derived.rows["funding_version_sources"].update(vs.funding_source_rows(info, manifest))

    def _walk_objects(self, d: Path, parts: tuple[str, ...],
                      out: list[tuple[Path, tuple[str, ...]]]) -> None:
        for p in sorted(d.iterdir()):
            st = os.lstat(p)
            if stat.S_ISLNK(st.st_mode):
                self.add("SYMLINK", self.rel(p), "symlänk under objects/")
            elif not stat.S_ISDIR(st.st_mode):
                self.add("OBJECT_LAYOUT", self.rel(p), "fil på fel nivå under objects/")
            elif len(parts) == 4:
                out.append((p, (*parts, p.name)))
            else:
                self._walk_objects(p, (*parts, p.name), out)

    def _object_exists(self, did: str, dv: str) -> bool:
        return (did, dv) in self.object_dirs

    # ---- refs / ref_log
    def check_refs(self) -> None:
        try:
            self.entries = vs.parse_ref_log(self.store._read_ref_log())
        except (vs.RefCorruptError, OSError) as e:
            self.add("REF_LOG_CORRUPT", vs.REF_LOG_FILE, str(e))
        self.done("ref_log.parse")
        refs: dict[str, tuple[str, vs._RefFile]] = {}
        files_ok = True
        refs_root = self.root / vs.REFS_DIR
        if os.path.lexists(refs_root):
            if stat.S_ISDIR(os.lstat(refs_root).st_mode):
                files_ok = self._walk_refs(refs_root, refs)
            else:
                files_ok = False
        self.done("refs.files")
        if self.entries is None or not files_ok:
            self.skip("refs.consistency", "ref_log eller ref-filer är ogiltiga")
        else:
            try:
                state = _RefOnlyView(self.root)._load_ref_state()
            except (vs.RefCorruptError, OSError) as e:
                self.add("REF_LOG_CORRUPT", vs.REF_LOG_FILE, f"ref-filer och ref_log går inte ihop: {e}")
            else:
                self.ref_state_ok = True
                self.pending_entry = state.pending
                if state.pending is not None:
                    self.add("PENDING_REF_UPDATE", f"{vs.REF_LOG_FILE}#{state.pending.seq}",
                             f"{state.pending.ref.key.relpath}: anropa complete_pending_ref_updates()")
            self.done("refs.consistency")
        for entry in self.entries or ():
            self.referenced.add((entry.ref.dataset_id, entry.ref.data_version))
            if not self._object_exists(entry.ref.dataset_id, entry.ref.data_version):
                self.add("REF_TARGET_MISSING", f"{vs.REF_LOG_FILE}#{entry.seq}",
                         f"{entry.ref.key.relpath}/{entry.ref.data_version} saknas")
        for did, (subject, rf) in sorted(refs.items()):
            self.referenced.add((did, rf.ref.data_version))
            if not self._object_exists(did, rf.ref.data_version):
                self.add("REF_TARGET_MISSING", subject, f"{rf.ref.data_version} saknas")
        self.done("refs.targets")
        self.counts["ref_log_entries"] = len(self.entries or ())
        self.counts["refs"] = len(refs)
        if self.ref_state_ok and self.entries is not None:
            pending_seq = self.pending_entry.seq if self.pending_entry is not None else None
            self.derived.rows["ref_log"] = {
                (e.seq, e.entry_sha256, e.prev_entry_sha256, e.ref.dataset_id, e.ref.data_version,
                 e.previous_data_version, e.action) for e in self.entries if e.seq != pending_seq}
            self.derived.rows["refs"] = {(did, rf.ref.data_version, rf.ref_log_seq, rf.file_sha256)
                                         for did, (_, rf) in refs.items()}
        else:
            for t in ("ref_log", "refs"):
                self.derived.unknown_tables[t] = "ref-tillståndet är ogiltigt"

    def _walk_refs(self, refs_root: Path, out: dict[str, tuple[str, vs._RefFile]]) -> bool:
        ok = True
        stack = [refs_root]
        while stack:
            d = stack.pop()
            for p in sorted(d.iterdir()):
                st = os.lstat(p)
                subject = self.rel(p)
                if stat.S_ISLNK(st.st_mode):
                    self.add("SYMLINK", subject, "symlänk under refs/")
                    ok = False
                    continue
                if stat.S_ISDIR(st.st_mode):
                    stack.append(p)
                    continue
                parts = p.relative_to(refs_root).parts
                try:
                    if len(parts) != 4 or not parts[3].endswith(".json"):
                        raise ValueError("fel djup eller filändelse")
                    key = catalog.dataset_key(exchange=parts[0], instrument=parts[1], symbol=parts[2],
                                              timeframe=parts[3][:-5])
                except ValueError as e:
                    self.add("REF_FOREIGN_ENTRY", subject, str(e))
                    ok = False
                    continue
                try:
                    rf = self.store._read_ref_file(p, key)
                    if self.entries is not None:
                        vs._current_from(rf, self.entries, rf.ref.dataset_id)
                except (vs.RefCorruptError, OSError) as e:
                    self.add("REF_FILE_CORRUPT", subject, str(e))
                    ok = False
                    continue
                out[rf.ref.dataset_id] = (subject, rf)
        return ok

    # ---- snapshots
    def check_snapshots(self) -> None:
        snaps = self.root / vs.SNAPSHOTS_DIR
        n = 0
        if os.path.lexists(snaps) and stat.S_ISDIR(os.lstat(snaps).st_mode):
            for p in sorted(snaps.iterdir()):
                st = os.lstat(p)
                subject = self.rel(p)
                if stat.S_ISLNK(st.st_mode):
                    self.add("SYMLINK", subject, "symlänk under snapshots/")
                    continue
                sid = p.name[:-5] if p.name.endswith(".json") else ""
                if not stat.S_ISREG(st.st_mode) or not catalog.is_sha256_hex(sid):
                    self.add("SNAPSHOT_CORRUPT", subject, "okänd post i snapshots/")
                    continue
                try:
                    snap = self.store.read_snapshot(sid)
                except (vs.SnapshotCorruptError, ValueError, OSError) as e:
                    self.add("SNAPSHOT_CORRUPT", subject, str(e))
                    continue
                n += 1
                self.derived.rows["snapshots"].add(
                    (sid, hashlib.sha256(p.read_bytes()).hexdigest(), len(snap.members)))
                for m in snap.members:
                    self.derived.rows["snapshot_members"].add((sid, m.dataset_id, m.data_version))
                    self.referenced.add((m.dataset_id, m.data_version))
                    if not self._object_exists(m.dataset_id, m.data_version):
                        self.add("SNAPSHOT_MEMBER_MISSING", subject,
                                 f"{m.key.relpath}/{m.data_version} saknas")
                for f in snap.funding:                     # v2 (SB-S4-a A2)
                    self.derived.rows["snapshot_funding_members"].add((sid, f.dataset_id, f.data_version))
                    self.referenced.add((f.dataset_id, f.data_version))
                    if not self._object_exists(f.dataset_id, f.data_version):
                        self.add("SNAPSHOT_MEMBER_MISSING", subject,
                                 f"{f.key.relpath}/{f.data_version} saknas")
        self.done("snapshots.files")
        self.done("snapshots.members")
        self.counts["snapshots"] = n

    # ---- ingests
    def check_ingests(self) -> None:
        records: tuple[tuple[IngestRecord, str], ...] | None
        try:
            records = self.store._load_ingests()
        except (vs.IngestCorruptError, OSError) as e:
            self.add("INGEST_CORRUPT", vs.INGESTS_DIR, str(e))
            records = None
        self.done("ingests.chain")
        pending_ingest: IngestRecord | None = None
        derived_ok = False
        if records is None or self.entries is None:
            self.skip("ingests.ref_log", "ingest-kedjan eller ref_log är ogiltig")
        else:
            try:
                with self.store._permit_pending_ingest():
                    self.store._check_ingests(self.entries)
            except (vs.IngestCorruptError, OSError) as e:
                self.add("INGEST_LOG_MISMATCH", vs.INGESTS_DIR, str(e))
            else:
                if records:
                    last = records[-1][0]
                    if last.ref_outcome == "changed" and (last.ref_log_seq or 0) > len(self.entries):
                        pending_ingest = last
                        self.add("PENDING_INGEST", f"{vs.INGESTS_DIR}/{last.ingest_id}.json",
                                 f"seq {last.seq}: anropa complete_pending_ingest()")
                if self.ref_state_ok:
                    self._derive_ingest_rows(records, pending_ingest)
                    derived_ok = True
            self.done("ingests.ref_log")
        for rec, _ in records or ():
            self.referenced.add((rec.ref.dataset_id, rec.ref.data_version))
            if not self._object_exists(rec.ref.dataset_id, rec.ref.data_version):
                self.add("INGEST_TARGET_MISSING", f"{vs.INGESTS_DIR}/{rec.ingest_id}.json",
                         f"{rec.ref.key.relpath}/{rec.ref.data_version} saknas")
        self.done("ingests.targets")
        self.counts["ingests"] = len(records or ())
        if not derived_ok:
            for t in ("ingest_events", "ingest_sources"):
                self.derived.unknown_tables[t] = "ingest- eller ref-tillståndet är ogiltigt"

    def _derive_ingest_rows(self, records: tuple[tuple[IngestRecord, str], ...],
                            pending_ingest: IngestRecord | None) -> None:
        pending_seq = self.pending_entry.seq if self.pending_entry is not None else None
        for rec, sha in records:
            if rec is pending_ingest or (rec.ref_log_seq is not None and rec.ref_log_seq == pending_seq):
                continue                                   # väntande: aldrig synkad av en skrivare
            self.derived.rows["ingest_events"].add(
                (rec.seq, rec.ingest_id, sha, rec.ref.dataset_id, rec.ref.data_version,
                 rec.previous_data_version, compact_json_bytes(rec.params).decode("ascii"),
                 rec.code_version, None if rec.worktree_clean is None else int(rec.worktree_clean),
                 rec.publication, rec.ref_outcome, rec.ref_log_seq, rec.duplicates_dropped))
            for i, s in enumerate(rec.sources):
                self.derived.rows["ingest_sources"].add((rec.seq, i, s.name, s.sha256, s.rows))

    # ---- importer (steg 6)
    def check_imports(self) -> None:
        records: list[lg.ImportRecord] = []
        d = self._real_dir(lg.IMPORTS_DIR)
        if d is not None:
            for p in sorted(d.iterdir()):
                subject = self.rel(p)
                if stat.S_ISLNK(os.lstat(p).st_mode):
                    self.add("SYMLINK", subject, "symlänk under imports/")
                    continue
                try:
                    records.append(lg._read_import_file(p)[0])
                except (lg.ImportCorruptError, OSError) as e:
                    self.add("IMPORT_CORRUPT", subject, str(e))
        self.done("imports.records")
        recorded: set[tuple[str, str]] = set()
        for rec in records:
            did, dv = rec.ref.dataset_id, rec.ref.data_version
            subject = f"{lg.IMPORTS_DIR}/{rec.import_id}.json"
            recorded.add((did, dv))
            self.referenced.add((did, dv))
            if not self._object_exists(did, dv):
                self.add("IMPORT_TARGET_MISSING", subject, f"{rec.ref.key.relpath}/{dv} saknas")
            elif (did, dv) in self.object_manifests:
                conflicts = lg.manifest_conflicts(rec.manifest, self.object_manifests[(did, dv)])
                if conflicts:
                    self.add("IMPORT_CORRUPT", subject, f"stämmer inte med objektets manifest: {conflicts}")
        self.done("imports.targets")
        if self.entries is None:
            self.skip("imports.ref_log", "ref_log är ogiltig")
        else:
            for entry in self.entries:
                if entry.action == "import_legacy" \
                        and (entry.ref.dataset_id, entry.ref.data_version) not in recorded:
                    self.add("IMPORT_LOG_MISMATCH", f"{vs.REF_LOG_FILE}#{entry.seq}",
                             f"{entry.ref.key.relpath}/{entry.ref.data_version}: import_legacy utan importpost")
            self.done("imports.ref_log")
        self.counts["imports"] = len(records)

    # ---- kvalitetsrapporter (steg 6b)
    def check_quality(self) -> None:
        reports: list[qreg.RegisteredReport] = []
        d = self._real_dir(qreg.QUALITY_DIR)
        if d is not None:
            for p in sorted(d.iterdir()):
                subject = self.rel(p)
                if stat.S_ISLNK(os.lstat(p).st_mode):
                    self.add("SYMLINK", subject, "symlänk under quality/")
                    continue
                try:
                    rep = qreg._read_quality_file(p)[0]
                except (qreg.QualityCorruptError, OSError) as e:
                    self.add("QUALITY_CORRUPT", subject, str(e))
                    continue
                problem = qreg.config_problem(rep)
                if problem is not None:
                    self.add("QUALITY_CORRUPT", subject, problem)
                    continue
                reports.append(rep)
        self.done("quality.files")
        for rep in reports:
            subject = f"{qreg.QUALITY_DIR}/{rep.report_sha256}{qreg.QUALITY_SUFFIX}"
            row, ds_rows = rep.catalog_rows()
            self.derived.rows["quality_reports"].add(row)
            self.derived.rows["quality_report_datasets"].update(ds_rows)
            for ref, binding in zip(rep.datasets, rep.bindings, strict=True):
                did, dv = ref.dataset_id, ref.data_version
                self.referenced.add((did, dv))
                if not self._object_exists(did, dv):
                    self.add("QUALITY_TARGET_MISSING", subject, f"{ref.key.relpath}/{dv} saknas")
                elif (did, dv) in self.object_manifests:
                    m = self.object_manifests[(did, dv)]
                    have = (m["rows"], m["first_open_time"], m["last_open_time"])
                    if have != binding:
                        self.add("QUALITY_BINDING_MISMATCH", subject,
                                 f"{ref.key.relpath}/{dv}: rapporten {binding} != objektet {have}")
        self.done("quality.targets")
        self.counts["quality_reports"] = len(reports)

    # ---- referenser (INFO)
    def check_references(self) -> None:
        for (did, dv), key in sorted(self.valid_objects.items()):
            if (did, dv) not in self.referenced:
                self.add("OBJECT_UNREFERENCED", f"objects/{key.relpath}/{dv}",
                         "refereras inte av ref_log, refs, snapshots eller ingests")
        for (did, dv), fkey in sorted(self.valid_funding.items()):
            if (did, dv) not in self.referenced:
                self.add("OBJECT_UNREFERENCED", f"objects/{fkey.relpath}/{dv}", "refereras inte av någon snapshot")
        self.done("objects.references")

    # ---- katalog
    def check_catalog(self) -> None:
        path = self.root / vs.CATALOG_FILE
        conn = self._open_catalog(path)
        self.done("catalog.open")
        if conn is None:
            why = ("katalogen kräver migrering" if "CATALOG_MIGRATION_REQUIRED" in {f.code for f in self.findings}
                   else "katalogen kan inte öppnas")
            self.skip("catalog.structure_full", why)
            for t, _, _ in CATALOG_TABLES:
                self.skip(f"catalog.rows.{t}", why)
            return
        try:
            problems = catalog.check_catalog(conn, full=True)
            for p in problems:
                self.add("CATALOG_STRUCTURE", vs.CATALOG_FILE, p)
            self.done("catalog.structure_full")
            if problems:
                for t, _, _ in CATALOG_TABLES:
                    self.skip(f"catalog.rows.{t}", "katalogens struktur är skadad")
                return
            self.catalog_dump = catalog.dump_sha256(conn)
            for table, cols, nkey in CATALOG_TABLES:
                if table in self.derived.unknown_tables:
                    self.skip(f"catalog.rows.{table}", self.derived.unknown_tables[table])
                    continue
                have = {tuple(r) for r in conn.execute(f"SELECT {', '.join(cols)} FROM {table}")}
                self._compare(table, nkey, have, self.derived.rows[table])
                self.done(f"catalog.rows.{table}")
        finally:
            conn.close()

    def _open_catalog(self, path: Path) -> sqlite3.Connection | None:
        if not os.path.lexists(path):
            self.add("CATALOG_MISSING", vs.CATALOG_FILE, "katalogen saknas")
            return None
        if not stat.S_ISREG(os.lstat(path).st_mode):
            return None                                    # ROOT_LAYOUT / SYMLINK redan rapporterat
        journal = path.with_name(path.name + "-journal")
        if journal.exists() and journal.stat().st_size > 0:
            self.add("CATALOG_UNREADABLE", vs.CATALOG_FILE, "avbruten skrivning (hot journal)")
            return None
        try:
            return catalog.open_catalog(path, readonly=True)
        except catalog.CatalogMigrationRequiredError as e:   # oskadat, äldre schema (SB-S4-MIG)
            self.add("CATALOG_MIGRATION_REQUIRED", vs.CATALOG_FILE, str(e))
        except catalog.CatalogVersionError as e:
            if self._catalog_version(path) > len(catalog.MIGRATIONS):
                self.add("CATALOG_NEWER_SCHEMA", vs.CATALOG_FILE, str(e))
            else:
                self.add("CATALOG_UNREADABLE", vs.CATALOG_FILE, str(e))
        except (catalog.CatalogError, sqlite3.Error, OSError) as e:
            self.add("CATALOG_UNREADABLE", vs.CATALOG_FILE, str(e))
        return None

    @staticmethod
    def _catalog_version(path: Path) -> int:
        try:
            conn = catalog._connect(path, "ro")
        except (sqlite3.Error, OSError):
            return 0
        try:
            return catalog._user_version(conn)
        except sqlite3.Error:
            return 0
        finally:
            conn.close()

    def _compare(self, table: str, nkey: int, have: set[Row], want: set[Row]) -> None:
        excluded = self.derived.excluded_versions
        if table in ("dataset_versions", "version_sources", "funding_versions", "funding_version_sources"):
            have = {r for r in have if (r[0], r[1]) not in excluded}
        have_k = {r[:nkey]: r for r in have}
        want_k = {r[:nkey]: r for r in want}
        for k in sorted(set(have_k) | set(want_k), key=repr):
            subject = f"catalog:{table}/{'/'.join(str(x) for x in k)}"
            h, w = have_k.get(k), want_k.get(k)
            if h == w:
                continue
            if h is None and table == "dataset_versions":
                key = self.valid_objects[(k[0], k[1])]
                self.add("OBJECT_UNREGISTERED", f"objects/{key.relpath}/{k[1]}", "saknar katalograd")
            elif h is None and table == "funding_versions":
                fkey = self.valid_funding[(k[0], k[1])]
                self.add("OBJECT_UNREGISTERED", f"objects/{fkey.relpath}/{k[1]}", "saknar katalograd")
            elif h is None:
                self.add("CATALOG_BEHIND", subject, "raden saknas i katalogen")
            elif w is None:
                self.add("CATALOG_CONFLICT", subject, "katalogen har en rad som filerna saknar")
            elif table == "refs" and h[2] < w[2]:
                self.add("CATALOG_BEHIND", subject, f"ref_log_seq {h[2]} < {w[2]}")
            else:
                self.add("CATALOG_CONFLICT", subject, f"katalogen {h!r} != filerna {w!r}")

    # ---- tmp/
    def check_tmp(self) -> None:
        tmp = self._real_dir("tmp")
        n = 0
        if tmp is not None:
            for p in sorted(tmp.iterdir()):
                n += 1
                if classify_tmp_entry(p):
                    self.add("TMP_LEFTOVER", self.rel(p), "känd stagingrest")
                else:
                    self.add("TMP_UNKNOWN", self.rel(p), "okänd post i tmp/ (tas aldrig bort)")
        self.done("tmp.entries")
        self.counts["tmp_entries"] = n


def _version_row(info: vs.VersionInfo, manifest: dict[str, Any]) -> Row:
    """Samma rad som StoreWriter._register skriver."""
    return (info.dataset_id, info.data_version, info.data_file_sha256, info.data_file_bytes,
            info.manifest_sha256, info.version_sha256, int(manifest["schema_version"]), info.rows,
            vs._iso_to_ms(info.first_open_time), vs._iso_to_ms(info.last_open_time),
            int(manifest["gap_count"]), int(manifest["missing_bars_total"]))


def classify_tmp_entry(path: Path) -> bool:
    """True om posten är en känd stagingrest som repair får ta bort (lstat, följer aldrig länkar)."""
    st = os.lstat(path)
    if stat.S_ISLNK(st.st_mode):
        return False
    if stat.S_ISDIR(st.st_mode):
        return _TMP_DIR_RE.fullmatch(path.name) is not None and _plain_tree(path)
    return stat.S_ISREG(st.st_mode) and _TMP_FILE_RE.fullmatch(path.name) is not None


def _plain_tree(d: Path) -> bool:
    for p in d.iterdir():
        st = os.lstat(p)
        if stat.S_ISDIR(st.st_mode):
            if not _plain_tree(p):
                return False
        elif not stat.S_ISREG(st.st_mode):
            return False
    return True


# ------------------------------------------------------------------ verify
def _verify(store: vs.VersionedStore, best_effort: bool) -> tuple[VerifyReport, _Derived]:
    v = _Verifier(store, best_effort)
    report = v.run()
    return report, v.derived


def verify_store(root: str | Path, *, lock: bool = True) -> VerifyReport:
    """Djup, strikt läsande verifiering. Se modulens docstring."""
    store = vs.VersionedStore(root)
    if not lock:
        return _verify(store, best_effort=True)[0]
    try:
        fd = os.open(store.root / vs.LOCK_FILE, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as e:
        if e.errno in (errno.ENOENT, errno.ELOOP):
            return _verify(store, best_effort=True)[0]      # LOCK_MISSING / SYMLINK rapporteras
        raise
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as e:
            if e.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                raise vs.StoreLockedError(f"{vs.LOCK_FILE}: en skrivare håller låset; ingen rapport") from e
            raise
        return _verify(store, best_effort=False)[0]
    finally:
        os.close(fd)                                        # släpper även flock


# ------------------------------------------------------------------ repair
def plan_repair(report: VerifyReport) -> RepairPlan:
    return RepairPlan(
        tmp_removals=tuple(sorted(f.subject for f in report.findings if f.code == "TMP_LEFTOVER")),
        rebuild_catalog=bool(report.codes & REBUILD_CODES),
        blocking=tuple(f for f in report.findings if f.severity == "ERROR"))


def repair_store(root: str | Path, *, apply: bool = False) -> RepairResult:
    """Begränsad reparation, dry-run som standard. Se modulens docstring."""
    store = vs.VersionedStore(root)
    lock_path = store.root / vs.LOCK_FILE
    if lock_path.is_symlink():
        raise vs.StoreError(f"{vs.LOCK_FILE} är en symlänk; repair vägrar")
    lock = vs._WriterLock(lock_path)
    lock.acquire()
    try:
        before, derived = _verify(store, best_effort=False)
        plan = plan_repair(before)
        if not apply:
            return RepairResult(before, plan, applied=False, after=None)
        if plan.blocking:
            raise RepairRefusedError(
                f"repair vägrar: {len(plan.blocking)} ERROR-fynd; ingenting ändrades", before)
        _refuse_if_migration_required(before, "repair")
        if plan.rebuild_catalog:
            _rebuild_catalog(store, derived)
        for rel in plan.tmp_removals:
            _remove_tmp_entry(store, rel)
        if plan.tmp_removals:
            vs._fsync_dir(store.tmp)
        after, _ = _verify(store, best_effort=False)
        return RepairResult(before, plan, applied=True, after=after)
    finally:
        lock.release()


def rebuild_catalog_store(root: str | Path, *, apply: bool = False) -> RebuildResult:
    """Ovillkorlig katalogombyggnad (steg 8), dry-run som standard. Se modulens docstring (REBUILD)."""
    store = vs.VersionedStore(root)
    lock_file = store.root / vs.LOCK_FILE
    if lock_file.is_symlink():
        raise vs.StoreError(f"{vs.LOCK_FILE} är en symlänk; rebuild vägrar")
    writer_lock = vs._WriterLock(lock_file)
    writer_lock.acquire()
    try:
        report, rows = _verify(store, best_effort=False)
        errors = tuple(x for x in report.findings if x.severity == "ERROR")
        if not apply:
            return RebuildResult(report, errors, applied=False, after=None)
        if errors:
            raise RepairRefusedError(
                f"rebuild vägrar: {len(errors)} ERROR-fynd; ingenting ändrades", report)
        _refuse_if_migration_required(report, "rebuild")
        _rebuild_catalog(store, rows)
        rebuilt, _ = _verify(store, best_effort=False)
        return RebuildResult(report, errors, applied=True, after=rebuilt)
    finally:
        writer_lock.release()


def _rebuild_catalog(store: vs.VersionedStore, derived: _Derived) -> None:
    path = store.catalog_path
    journal = path.with_name(path.name + "-journal")
    if journal.exists() and journal.stat().st_size > 0:
        try:                                                # SQLite rullar tillbaka avbruten skrivning
            catalog.open_catalog(path).close()
        except catalog.CatalogMigrationRequiredError as e:  # efter rollback: bara äldre schema
            raise vs.StoreError(f"{e}; ombyggnaden vägrar, kör migrate-catalog (SB-S4-MIG)") from e
        except (catalog.CatalogError, sqlite3.Error):
            pass                                            # katalogen ersätts ändå
    catalog.rebuild_catalog(path, _populate(derived), staging_dir=store.tmp)


def _refuse_if_migration_required(report: VerifyReport, what: str) -> None:
    """En katalog som bara har ett äldre schema byggs aldrig om (det vore en migrering i smyg)."""
    if "CATALOG_MIGRATION_REQUIRED" in report.codes:
        raise RepairRefusedError(f"{what} vägrar: katalogen kräver migrering (migrate-catalog); "
                                 "ingenting ändrades", report)


def _populate(derived: _Derived) -> Callable[[sqlite3.Connection], None]:
    if derived.unknown_tables:
        raise vs.StoreError(f"katalogen kan inte härledas: {sorted(derived.unknown_tables)}")

    def populate(conn: sqlite3.Connection) -> None:
        for table, cols, _ in CATALOG_TABLES:
            rows = sorted(derived.rows[table], key=repr)
            conn.executemany(f"INSERT INTO {table} ({', '.join(cols)}) "
                             f"VALUES ({', '.join(':' + c for c in cols)})",
                             [dict(zip(cols, r, strict=True)) for r in rows])
    return populate


def _remove_tmp_entry(store: vs.VersionedStore, rel: str) -> None:
    name = Path(rel).name
    path = store.tmp / name
    if rel != f"tmp/{name}" or not classify_tmp_entry(path):     # kontrolleras igen, under låset
        raise vs.StoreError(f"{rel}: inte en känd stagingrest; tas inte bort")
    if stat.S_ISDIR(os.lstat(path).st_mode):
        _remove_plain_tree(path)
    else:
        os.unlink(path)                                     # namnet, aldrig innehåll eller läge


def _remove_plain_tree(d: Path) -> None:
    """Tar bort ett stagingträd nedifrån och upp. Endast KATALOGER får u+w; filers läge
    ändras aldrig (en fil kan vara en hårdlänk till en publicerad post). Följer aldrig länkar."""
    st = os.lstat(d)
    if not stat.S_ISDIR(st.st_mode):
        raise vs.StoreError(f"{d.name}: inte en katalog")
    os.chmod(d, stat.S_IMODE(st.st_mode) | 0o700)
    for p in sorted(d.iterdir()):
        pst = os.lstat(p)
        if stat.S_ISDIR(pst.st_mode):
            _remove_plain_tree(p)
        elif stat.S_ISREG(pst.st_mode):
            os.unlink(p)
        else:
            raise vs.StoreError(f"{p.name}: oväntad posttyp i stagingträd")
    os.rmdir(d)
