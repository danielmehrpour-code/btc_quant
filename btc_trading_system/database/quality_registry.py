"""Registrering av Phase 3-kvalitetsrapporter i versionslagret (PHASE 4, steg 6b).

KÄLLAN (en Phase 3 --out-katalog, t.ex. ~/btc_data/quality_reports_v1/run1) är SKRIVSKYDDAD:
ingenting i den skrivs, skapas, byter namn, chmod:as eller tas bort. Filer öppnas
O_RDONLY|O_NOFOLLOW, symlänkar följs aldrig. Källans trädhash (samma metod som
~/phase4_work/treehash.sh) beräknas före och efter; ändrades den -> failure="SOURCE_CHANGED".
Inga igenkända rapporter -> failure="SOURCE_EMPTY" (SOURCE_CHANGED väger tyngst). Registreringen
skriver ENDAST i ett separat VersionedStore-rot (källa och lager får inte överlappa).

FLÖDE (under skrivarlåset, även i dry-run)
  1. källroten kontrolleras; väntande ref-uppdatering/ingest -> fel innan något läses;
     den låsta Phase 3-konfigurationen (configs/data_quality.yaml) måste ha LOCKED_CONFIG_SHA256
  2. upptäckt (platt): <stem>.quality.json (+ <stem>.quality.md); allt annat -> `unrecognized`
     (en ensam .md är ingen rapport). Ordning: filnamn.
  3. PLAN för varje rapport (ingenting skrivs; ett fel -> rapporten "failed", körningen fortsätter):
     A. JSON- och Markdown-bytes läses EN gång
     B. strikt tolkning: Phase 3:s load_report, exakta nycklar, kanoniska bytes
        (== report_to_json), filnamn == report_stem, overall == overall_status(checks)
     C. Markdown == report_to_markdown(rapporten), byte för byte (lagras inte)
     D. config.canonical_sha256 == LOCKED_CONFIG_SHA256 och config.version == CONFIG_VERSION
     E. varje data_version: objektet finns (annars TARGET_MISSING) och verifieras FULLT; ett objekt
        som finns men inte verifierar är ett fel i mållagret -> HELA körningen avbryts innan något
        skrivs; rows/first_open_time/last_open_time == objektets manifest (BINDING_MISMATCH)
     F. reproduktion: Phase 3:s egen byggare körs om på lagrets verifierade objekt med den låsta
        konfigurationen; code.version och code.worktree_clean tas från rapporten; för
        datasetrapporter måste coverage.duplicates_dropped vara lika med värdet i en
        proveniensspost för exakt den data_version (importpostens legacy-manifest eller en
        ingest-post); report_to_json(omkörningen) == de inlästa bytena (NOT_REPRODUCIBLE)
     G. befintlig quality/<sha>.quality.json verifieras strikt (skadad -> QualityCorruptError);
        befintliga katalograder måste vara exakt de härledda (annars CatalogCorruptError)
  4. apply=True, per planerad rapport (skrivordning):
     1) katalograd för varje målobjekt om den saknas (StoreWriter._register, idempotent)
     2) tmp/<H>.quality.json (0444, fsync; load_report körs på exakt den filen)
     3) link -> quality/<report_sha256>.quality.json (skrivs aldrig över), fsync av katalogen
     4) stagingnamnet tas bort
     5) quality_reports + quality_report_datasets i EN transaktion
  apply=False (standard): steg 1-3 plus en exakt förutsägelse av 4; skriver ingenting.

LAGRING
  quality/<report_sha256>.quality.json ÄR källrapporten byte för byte (report_sha256 = sha256 av
  bytena). Katalograderna är en ren funktion av filerna (rebuild fungerar från filerna). Refs,
  ref_log, objects/, ingests/, imports/ och snapshots/ rörs aldrig.

KRASCH (per rapport): 1) ingen skada; 2) TMP_LEFTOVER; 3) TMP_LEFTOVER + CATALOG_BEHIND;
  4) CATALOG_BEHIND; återhämtning: omregistrering eller repair.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal

import pandas as pd

from btc_trading_system.data.schema import Instrument
from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.data_validation import quality_report as qr
from btc_trading_system.data_validation.errors import DataError, DataIntegrityError
from btc_trading_system.database import catalog
from btc_trading_system.database import legacy as lg
from btc_trading_system.database import versioned_store as vs
from btc_trading_system.database.refs import DatasetVersionRef

QUALITY_DIR = "quality"
QUALITY_SUFFIX = ".quality.json"
MARKDOWN_SUFFIX = ".quality.md"
REGISTER_REPORT_FORMAT = "btc-quant/quality-register-report/v1"
LOCKED_CONFIG_SHA256 = "d1350cde29f02294bb327176fd9ba2c4df4ac5187f72ff54f6c674588319b9a0"   # P3-01
FAILURE_CODES = ("SOURCE_UNREADABLE", "REPORT_INVALID", "MARKDOWN_MISMATCH", "CONFIG_MISMATCH",
                 "TARGET_MISSING", "BINDING_MISMATCH", "NOT_REPRODUCIBLE")
FILE_STORED: Final = "stored"
FILE_EXISTING: Final = "existing"
CATALOG_INSERTED: Final = "inserted"
CATALOG_EXISTING: Final = "existing"
QR_COLUMNS = ("report_sha256", "report_type", "overall", "report_schema_version", "config_version",
              "config_sha256", "code_version", "worktree_clean", "dataset_count")
QRD_COLUMNS = ("report_sha256", "position", "dataset_id", "data_version")

_TOP_KEYS = frozenset({"schema_version", "report_type", "overall", "config", "code", "datasets",
                       "checks", "notes"})
_EXTRA_KEYS = {"dataset": frozenset({"coverage"}), "tf_aggregation": frozenset(),
               "cross_instrument": frozenset()}
_DATASET_COUNT = {"dataset": 1, "tf_aggregation": 2, "cross_instrument": 2}
_REF_KEYS = frozenset({"exchange", "instrument", "symbol", "timeframe", "data_version", "rows",
                       "first_open_time", "last_open_time"})
_CHECK_KEYS = frozenset({"name", "status", "causal", "reason", "findings", "details"})
_COVERAGE_KEYS = frozenset({"expected_slots", "present_bars", "missing_bars", "gap_count",
                            "largest_gap_bars", "duplicates_dropped", "zero_volume_bars"})

Row = tuple[Any, ...]


class QualityRegistrationError(vs.StoreError):
    """Registreringen som helhet kan inte köras (ogiltig/överlappande rot, källan är ett
    versionslager, den låsta konfigurationen har fel hash)."""


class QualityReportError(DataError, ValueError):
    """Rapportbytena klarar inte den strikta tolkningen."""


class QualityCorruptError(DataIntegrityError):
    """En lagrad quality/-fil är skadad."""


class _ReportFailure(Exception):
    """Internt: en källrapport kan inte registreras (rapporteras, avbryter inte körningen)."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


# ------------------------------------------------------------------ strikt tolkning (ingen I/O)
@dataclass(frozen=True)
class RegisteredReport:
    report_sha256: str
    report_type: str
    overall: str
    schema_version: int
    config_version: int
    config_sha256: str
    code_version: str
    worktree_clean: bool | None
    datasets: tuple[DatasetVersionRef, ...]               # rapportens ordning (position 0, 1)
    bindings: tuple[tuple[int, str, str], ...]            # (rows, first_open_time, last_open_time)

    def catalog_rows(self) -> tuple[Row, tuple[Row, ...]]:
        """(quality_reports-raden, quality_report_datasets-raderna) i QR_COLUMNS/QRD_COLUMNS-ordning."""
        clean = None if self.worktree_clean is None else int(self.worktree_clean)
        report = (self.report_sha256, self.report_type, self.overall, self.schema_version,
                  self.config_version, self.config_sha256, self.code_version, clean, len(self.datasets))
        return report, tuple((self.report_sha256, i, ref.dataset_id, ref.data_version)
                             for i, ref in enumerate(self.datasets))


def _parse(data: bytes, name: str | None) -> tuple[RegisteredReport, dict[str, Any]]:
    def bad(msg: str) -> QualityReportError:
        return QualityReportError(msg)
    try:
        obj = json.loads(data.decode("utf-8"))
    except ValueError as e:
        raise bad(f"rapporten är inte UTF-8-JSON: {e}") from e
    if not isinstance(obj, dict):
        raise bad("rapporten är inte ett JSON-objekt")
    kind = obj.get("report_type")
    if kind not in catalog.REPORT_TYPES:
        raise bad(f"okänd report_type {kind!r}")
    unknown = sorted(set(obj) - (_TOP_KEYS | _EXTRA_KEYS[kind]))
    missing = sorted((_TOP_KEYS | _EXTRA_KEYS[kind]) - set(obj))
    if unknown or missing:
        raise bad(f"rapportnycklar: okända {unknown}, saknade {missing}")
    if not _is_int(obj["schema_version"]) or obj["schema_version"] != qr.SCHEMA_VERSION:
        raise bad(f"schema_version {obj['schema_version']!r} != {qr.SCHEMA_VERSION}")
    cfg, code = obj["config"], obj["code"]
    if not isinstance(cfg, dict) or set(cfg) != {"version", "canonical_sha256"} \
            or not _is_int(cfg["version"]) or cfg["version"] < 1 \
            or not catalog.is_sha256_hex(cfg["canonical_sha256"]):
        raise bad("ogiltigt config-fält")
    if not isinstance(code, dict) or set(code) != {"version", "worktree_clean"} \
            or not isinstance(code["version"], str) or not code["version"] \
            or not (code["worktree_clean"] is None or isinstance(code["worktree_clean"], bool)):
        raise bad("ogiltigt code-fält")
    entries = obj["datasets"]
    if not isinstance(entries, list) or len(entries) != _DATASET_COUNT[kind]:
        raise bad(f"{kind} kräver {_DATASET_COUNT[kind]} dataset")
    refs: list[DatasetVersionRef] = []
    bindings: list[tuple[int, str, str]] = []
    for d in entries:
        if not isinstance(d, dict) or set(d) != _REF_KEYS:
            raise bad("datasetpost har fel nycklar")
        try:
            key = catalog.dataset_key(exchange=d["exchange"], instrument=d["instrument"],
                                      symbol=d["symbol"], timeframe=d["timeframe"])
            refs.append(DatasetVersionRef(key, d["data_version"]))
        except (ValueError, TypeError, AttributeError) as e:
            raise bad(f"ogiltig datasetpost: {e}") from e
        if not _is_int(d["rows"]) or d["rows"] < 1 \
                or not isinstance(d["first_open_time"], str) or not isinstance(d["last_open_time"], str):
            raise bad("ogiltiga rows/first_open_time/last_open_time")
        bindings.append((d["rows"], d["first_open_time"], d["last_open_time"]))
    keys = [r.key for r in refs]
    if kind == "tf_aggregation" and ((keys[0].instrument, keys[0].symbol) != (keys[1].instrument, keys[1].symbol)
                                     or keys[0].timeframe == keys[1].timeframe):
        raise bad("tf-rapport kräver samma instrument och symbol, olika timeframes")
    if kind == "cross_instrument" and (keys[0].instrument is not Instrument.SPOT
                                       or keys[1].instrument is not Instrument.PERP
                                       or (keys[0].symbol, keys[0].timeframe) != (keys[1].symbol, keys[1].timeframe)):
        raise bad("xinst-rapport kräver (spot, perp) med samma symbol och timeframe")
    checks = obj["checks"]
    if not isinstance(checks, list) or not checks \
            or any(not isinstance(c, dict) or set(c) != _CHECK_KEYS for c in checks):
        raise bad("checks har fel form")
    try:
        overall = qc.overall_status(qc.Status(c["status"]) for c in checks)
    except ValueError as e:
        raise bad(f"ogiltig kontrollstatus: {e}") from e
    if obj["overall"] != overall:
        raise bad(f"overall {obj['overall']!r} != {overall!r} enligt checks")
    if kind == "dataset" and (not isinstance(obj["coverage"], dict) or set(obj["coverage"]) != _COVERAGE_KEYS):
        raise bad("coverage har fel nycklar")
    if not isinstance(obj["notes"], list) or any(not isinstance(n, str) for n in obj["notes"]):
        raise bad("notes har fel form")
    try:
        canonical = qr.report_to_json(obj)
    except (TypeError, ValueError) as e:
        raise bad(f"rapporten kan inte kodas kanoniskt: {e}") from e
    if canonical != data:
        raise bad("rapporten är inte kanoniskt kodad (report_to_json)")
    if name is not None and name != qr.report_stem(obj) + QUALITY_SUFFIX:
        raise bad(f"filnamnet {name!r} != report_stem + {QUALITY_SUFFIX!r}")
    rep = RegisteredReport(
        report_sha256=hashlib.sha256(data).hexdigest(), report_type=kind, overall=overall,
        schema_version=obj["schema_version"], config_version=cfg["version"],
        config_sha256=cfg["canonical_sha256"], code_version=code["version"],
        worktree_clean=code["worktree_clean"], datasets=tuple(refs), bindings=tuple(bindings))
    return rep, obj


def parse_report_bytes(data: bytes, *, name: str | None = None) -> RegisteredReport:
    """Strikt tolkning av rapportbytena (ingen I/O); name -> måste vara report_stem + .quality.json."""
    return _parse(data, name)[0]


def config_problem(rep: RegisteredReport) -> str | None:
    """None om rapporten gäller den låsta Phase 3-konfigurationen (P3-01)."""
    if rep.config_sha256 != LOCKED_CONFIG_SHA256 or rep.config_version != qc.CONFIG_VERSION:
        return (f"config {rep.config_version}/{rep.config_sha256} != låst "
                f"{qc.CONFIG_VERSION}/{LOCKED_CONFIG_SHA256}")
    return None


def _load_report(path: Path, rep: RegisteredReport, obj: dict[str, Any]) -> None:
    """Phase 3:s egen läsväg (oförändrad) måste ge exakt det tolkade innehållet."""
    loaded = qr.load_report(path, [ref.data_version for ref in rep.datasets])
    if loaded != obj:
        raise QualityReportError("load_report gav ett annat innehåll")


# ------------------------------------------------------------------ lagrade rapporter (inget lås)
def _read_quality_file(path: Path) -> tuple[RegisteredReport, dict[str, Any]]:
    name = path.name
    sha = name[:-len(QUALITY_SUFFIX)] if name.endswith(QUALITY_SUFFIX) else ""
    if not catalog.is_sha256_hex(sha):
        raise QualityCorruptError(f"okänd fil i {QUALITY_DIR}/: {name}")
    st = os.lstat(path)
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode) or st.st_mode & 0o222:
        raise QualityCorruptError(f"{name}: inte en skrivskyddad vanlig fil")
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != sha:
        raise QualityCorruptError(f"{name}: sha256 av innehållet matchar inte filnamnet")
    try:
        rep, obj = _parse(data, None)
        _load_report(path, rep, obj)
    except (QualityReportError, qr.ReportBindingError) as e:
        raise QualityCorruptError(f"{name}: {e}") from e
    return rep, obj


def read_registered_reports(store: vs.VersionedStore) -> tuple[RegisteredReport, ...]:
    """Alla registrerade rapporter, strikt verifierade, sorterade på report_sha256."""
    root = store.root / QUALITY_DIR
    if not os.path.lexists(root):
        return ()
    if not stat.S_ISDIR(os.lstat(root).st_mode):
        raise QualityCorruptError(f"{QUALITY_DIR}/ är inte en katalog")
    return tuple(_read_quality_file(p)[0] for p in sorted(root.iterdir()))


# ------------------------------------------------------------------ rapport
ReportStatus = Literal["registered", "planned", "failed"]


@dataclass(frozen=True)
class ReportRegistrationResult:
    name: str
    status: ReportStatus
    report_sha256: str | None = None
    report_type: str | None = None
    overall: str | None = None
    datasets: tuple[tuple[str, str], ...] = ()          # (relpath, data_version), rapportens ordning
    file: Literal["stored", "existing"] | None = None
    catalog: Literal["inserted", "existing"] | None = None
    failure: str | None = None                          # en av FAILURE_CODES
    detail: str = ""

    def to_json_obj(self) -> dict[str, Any]:
        return {"name": self.name, "status": self.status, "report_sha256": self.report_sha256,
                "report_type": self.report_type, "overall": self.overall,
                "datasets": [list(d) for d in self.datasets], "file": self.file,
                "catalog": self.catalog, "failure": self.failure, "detail": self.detail}


@dataclass(frozen=True)
class QualityRegistrationReport:
    applied: bool
    reports: tuple[ReportRegistrationResult, ...]
    unrecognized: tuple[str, ...]
    source_tree_sha256_before: str
    source_tree_sha256_after: str
    failure: str | None                                 # "SOURCE_CHANGED", "SOURCE_EMPTY" eller None

    @property
    def ok(self) -> bool:
        return (self.failure is None and not self.unrecognized
                and all(r.status != "failed" for r in self.reports))

    def to_json_obj(self) -> dict[str, Any]:
        return {"format": REGISTER_REPORT_FORMAT, "applied": self.applied, "ok": self.ok,
                "failure": self.failure,
                "source_tree_sha256_before": self.source_tree_sha256_before,
                "source_tree_sha256_after": self.source_tree_sha256_after,
                "unrecognized": list(self.unrecognized),
                "reports": [r.to_json_obj() for r in self.reports]}

    def to_json_bytes(self) -> bytes:
        return vs.canonical_json_bytes(self.to_json_obj())


# ------------------------------------------------------------------ källa
def _check_roots(source_root: Path, store_root: Path) -> None:
    try:
        st = os.lstat(source_root)
    except OSError as e:
        raise QualityRegistrationError(f"källroten finns inte: {e.strerror}") from e
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise QualityRegistrationError("källroten måste vara en katalog (inte en symlänk)")
    if os.path.lexists(source_root / vs.STORE_FILE):
        raise QualityRegistrationError("källan är ett versionslager, inte en rapportkatalog")
    src = Path(os.path.realpath(source_root))
    dst = Path(os.path.realpath(store_root))
    if src == dst or src in dst.parents or dst in src.parents:
        raise QualityRegistrationError("källa och lager får inte vara samma katalog eller ligga i varandra")


@dataclass(frozen=True)
class _SourceReport:
    name: str                                           # <stem>.quality.json
    problem: str | None                                 # inte vanliga filer / .md saknas


def _discover(src: Path) -> tuple[list[_SourceReport], list[str]]:
    """Platt Phase 3 --out-katalog; allt annat -> unrecognized. Följer inga länkar."""
    with os.scandir(src) as it:
        entries = {e.name: e for e in it}
    found: list[_SourceReport] = []
    unrecognized: list[str] = []
    for name, e in sorted(entries.items()):
        if name.endswith(QUALITY_SUFFIX) and len(name) > len(QUALITY_SUFFIX):
            md = entries.get(name[:-len(QUALITY_SUFFIX)] + MARKDOWN_SUFFIX)
            problem = None
            if md is None:
                problem = f"{MARKDOWN_SUFFIX}-filen saknas"
            elif not (e.is_file(follow_symlinks=False) and md.is_file(follow_symlinks=False)):
                problem = "JSON- eller Markdown-filen är inte en vanlig fil (symlänk eller annan typ)"
            found.append(_SourceReport(name, problem))
        elif name.endswith(MARKDOWN_SUFFIX) and (name[:-len(MARKDOWN_SUFFIX)] + QUALITY_SUFFIX) in entries:
            continue                                     # hör till sin JSON
        else:
            unrecognized.append(name)
    return found, unrecognized


def _read_source_file(path: Path) -> bytes:
    """En läsning, O_RDONLY|O_NOFOLLOW; måste vara en vanlig fil."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as e:
        raise _ReportFailure("SOURCE_UNREADABLE", f"{path.name}: {e.strerror}") from e
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise _ReportFailure("SOURCE_UNREADABLE", f"{path.name}: inte en vanlig fil")
        chunks = []
        while chunk := os.read(fd, 1 << 20):
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


# ------------------------------------------------------------------ plan
@dataclass(frozen=True)
class _Target:
    info: vs.VersionInfo
    manifest: dict[str, Any]
    frame: pd.DataFrame


@dataclass(frozen=True)
class _Plan:
    name: str
    data: bytes
    rep: RegisteredReport
    obj: dict[str, Any]
    targets: tuple[_Target, ...]
    file_exists: bool
    catalog_exists: bool


class _Planner:
    """Steg 3 (A-G). Läser bara; ett målobjekt som finns men inte verifierar kastar."""

    def __init__(self, store: vs.VersionedStore, conn: Any, src: Path, cfg: qc.QualityConfig) -> None:
        self.store, self.conn, self.src, self.cfg = store, conn, src, cfg
        self.targets: dict[tuple[str, str], _Target] = {}
        self.provenance: dict[tuple[str, str], list[int]] = {}
        for rec in store.ingest_history():                                   # strikt; skadad -> kastar
            self.provenance.setdefault((rec.ref.dataset_id, rec.ref.data_version), []).append(
                rec.duplicates_dropped)
        for irec, _ in lg.read_import_records(store):                       # strikt; skadad -> kastar
            dd = irec.manifest.get("duplicates_dropped")
            if isinstance(dd, int) and not isinstance(dd, bool):
                self.provenance.setdefault((irec.ref.dataset_id, irec.ref.data_version), []).append(dd)

    def target(self, ref: DatasetVersionRef) -> _Target:
        k = (ref.dataset_id, ref.data_version)
        if k not in self.targets:
            if not os.path.lexists(self.store.object_dir(ref.key, ref.data_version)):
                raise _ReportFailure("TARGET_MISSING", f"{ref.key.relpath}/{ref.data_version} saknas i lagret")
            info, manifest, frame = self.store._verify_object(ref.key, ref.data_version)   # kastar
            self.targets[k] = _Target(info, manifest, frame)
        return self.targets[k]

    def plan(self, sr: _SourceReport) -> _Plan:
        if sr.problem is not None:                                          # A
            raise _ReportFailure("SOURCE_UNREADABLE", f"{sr.name}: {sr.problem}")
        path = self.src / sr.name
        data = _read_source_file(path)
        md = _read_source_file(self.src / (sr.name[:-len(QUALITY_SUFFIX)] + MARKDOWN_SUFFIX))
        try:                                                                # B
            rep, obj = _parse(data, sr.name)
            _load_report(path, rep, obj)
        except (QualityReportError, qr.ReportBindingError, ValueError) as e:
            raise _ReportFailure("REPORT_INVALID", str(e)) from e
        if md != qr.report_to_markdown(obj).encode():                       # C
            raise _ReportFailure("MARKDOWN_MISMATCH", "Markdown-filen != report_to_markdown(rapporten)")
        problem = config_problem(rep)                                       # D
        if problem is not None:
            raise _ReportFailure("CONFIG_MISMATCH", problem)
        targets: list[_Target] = []                                         # E
        for ref, binding in zip(rep.datasets, rep.bindings, strict=True):
            t = self.target(ref)
            have = (t.manifest["rows"], t.manifest["first_open_time"], t.manifest["last_open_time"])
            if have != binding:
                raise _ReportFailure("BINDING_MISMATCH",
                                     f"{ref.key.relpath}/{ref.data_version}: rapporten {binding} != objektet {have}")
            targets.append(t)
        self.reproduce(rep, obj, data, targets)                             # F
        path_q = self.store.root / QUALITY_DIR / f"{rep.report_sha256}{QUALITY_SUFFIX}"
        file_exists = os.path.lexists(path_q)                               # G
        if file_exists:
            _read_quality_file(path_q)                                      # skadad -> kastar
        return _Plan(sr.name, data, rep, obj, tuple(targets), file_exists, self.catalog_state(rep))

    def reproduce(self, rep: RegisteredReport, obj: dict[str, Any], data: bytes,
                  targets: Sequence[_Target]) -> None:
        version, clean = rep.code_version, rep.worktree_clean
        keys = [ref.key for ref in rep.datasets]
        manifests = [dict(t.manifest) for t in targets]
        if rep.report_type == "dataset":
            dd = obj["coverage"]["duplicates_dropped"]
            ref = rep.datasets[0]
            known = self.provenance.get((ref.dataset_id, ref.data_version), [])
            if not _is_int(dd) or dd not in known:
                raise _ReportFailure("NOT_REPRODUCIBLE",
                                     f"duplicates_dropped {dd!r} finns inte i någon proveniensspost för "
                                     f"{ref.key.relpath}/{ref.data_version} (kända: {sorted(set(known))})")
            manifests[0]["duplicates_dropped"] = dd
        try:
            if rep.report_type == "dataset":
                rebuilt = qr.build_dataset_report(targets[0].frame, keys[0], manifests[0], self.cfg,
                                                  version, clean)
            elif rep.report_type == "tf_aggregation":
                rebuilt = qr.build_tf_report(targets[0].frame, keys[0], manifests[0], targets[1].frame,
                                             keys[1], manifests[1], self.cfg, code_version=version,
                                             worktree_clean=clean)
            else:
                rebuilt = qr.build_xinst_report(targets[0].frame, keys[0], manifests[0], targets[1].frame,
                                                keys[1], manifests[1], self.cfg, code_version=version,
                                                worktree_clean=clean)
        except (qr.Phase2SuspectError, qr.ReportBindingError, DataError, ValueError) as e:
            raise _ReportFailure("NOT_REPRODUCIBLE", f"Phase 3-omkörningen misslyckades: {e}") from e
        if qr.report_to_json(rebuilt) != data:
            raise _ReportFailure("NOT_REPRODUCIBLE", "Phase 3-omkörningen på lagrets objekt ger andra bytes")

    def catalog_state(self, rep: RegisteredReport) -> bool:
        """True om raderna finns (och är exakt de härledda); False om de saknas helt."""
        want, want_ds = rep.catalog_rows()
        have = self.conn.execute(f"SELECT {', '.join(QR_COLUMNS)} FROM quality_reports "
                                 "WHERE report_sha256 = ?", (rep.report_sha256,)).fetchone()
        have_ds = tuple(tuple(r) for r in self.conn.execute(
            f"SELECT {', '.join(QRD_COLUMNS)} FROM quality_report_datasets "
            "WHERE report_sha256 = ? ORDER BY position", (rep.report_sha256,)))
        if have is None and not have_ds:
            return False
        if have is None or tuple(have) != want or have_ds != want_ds:
            raise catalog.CatalogCorruptError(
                f"{rep.report_sha256}: katalogens kvalitetsrader matchar inte rapporten")
        return True


# ------------------------------------------------------------------ skrivning
def _publish_file(store: vs.VersionedStore, p: _Plan) -> None:
    """tmp (0444, fsync, load_report) -> link (aldrig överskrivning) -> fsync av katalogen."""
    path = store.root / QUALITY_DIR / f"{p.rep.report_sha256}{QUALITY_SUFFIX}"
    vs._mkdirs_durable(path.parent, store.root)
    staged = store.tmp / f"{uuid.uuid4().hex}{QUALITY_SUFFIX}"
    try:
        with staged.open("xb") as f:
            f.write(p.data)
        os.chmod(staged, vs.FILE_MODE)
        vs._fsync_file(staged)
        _load_report(staged, p.rep, p.obj)                  # Phase 3-läsvägen på exakt de bytena
        try:
            vs._link(staged, path)                          # rapportfil: aldrig överskrivning
        except FileExistsError as e:
            raise vs.ImmutableObjectError(f"{path.name}: rapportfilen finns redan; skrivs aldrig över") from e
    finally:
        staged.unlink(missing_ok=True)
    vs._fsync_dir(path.parent)


def _insert_rows(conn: Any, rep: RegisteredReport) -> None:
    report, datasets = rep.catalog_rows()
    with catalog.transaction(conn):
        conn.execute(f"INSERT INTO quality_reports ({', '.join(QR_COLUMNS)}) "
                     f"VALUES ({', '.join('?' * len(QR_COLUMNS))})", report)
        conn.executemany(f"INSERT INTO quality_report_datasets ({', '.join(QRD_COLUMNS)}) "
                         f"VALUES ({', '.join('?' * len(QRD_COLUMNS))})", datasets)


def _apply(w: vs.StoreWriter, store: vs.VersionedStore, p: _Plan) -> None:
    for t in p.targets:                                                     # 1
        w._register(t.info, t.manifest)
    if not p.file_exists:                                                   # 2-4
        _publish_file(store, p)
    if not p.catalog_exists:                                                # 5
        _insert_rows(w._conn, p.rep)


def register_quality_reports(store: vs.VersionedStore, source_root: str | Path, *,
                             apply: bool = False) -> QualityRegistrationReport:
    """Registrerar Phase 3-rapporter. Dry-run som standard (apply=False skriver ingenting).
    Se modulens docstring."""
    src = Path(source_root)
    _check_roots(src, store.root)
    cfg = qc.load_quality_config()
    if cfg.canonical_sha256 != LOCKED_CONFIG_SHA256:
        raise QualityRegistrationError(
            f"configs/data_quality.yaml har canonical_sha256 {cfg.canonical_sha256}, låst är {LOCKED_CONFIG_SHA256}")
    roots = sorted({str(src), os.path.realpath(src), str(store.root), os.path.realpath(store.root)},
                   key=len, reverse=True)

    def clean(text: str) -> str:
        for r in roots:
            text = text.replace(r, "<källa>" if r in (str(src), os.path.realpath(src)) else "<lager>")
        return text

    with store.writer() as w:
        state = store._load_ref_state()                  # väntande ingest -> PendingIngestError
        if state.pending is not None:
            raise vs.PendingRefUpdateError(
                f"väntande ref-uppdatering (ref_log seq {state.pending.seq}); "
                "anropa complete_pending_ref_updates() först")
        before = lg.source_tree_sha256(src)
        found, unrecognized = _discover(src)
        planner = _Planner(store, w._conn, src, cfg)
        planned: list[_Plan | ReportRegistrationResult] = []
        for sr in found:                                 # PLAN: ingenting skrivs här
            try:
                planned.append(planner.plan(sr))
            except _ReportFailure as f:
                planned.append(ReportRegistrationResult(name=sr.name, status="failed", failure=f.code,
                                                        detail=clean(f.detail)))
        results: list[ReportRegistrationResult] = []
        for item in planned:
            if isinstance(item, ReportRegistrationResult):
                results.append(item)
                continue
            if apply:
                _apply(w, store, item)
            results.append(ReportRegistrationResult(
                name=item.name, status="registered" if apply else "planned",
                report_sha256=item.rep.report_sha256, report_type=item.rep.report_type,
                overall=item.rep.overall,
                datasets=tuple((ref.key.relpath, ref.data_version) for ref in item.rep.datasets),
                file=FILE_EXISTING if item.file_exists else FILE_STORED,
                catalog=CATALOG_EXISTING if item.catalog_exists else CATALOG_INSERTED))
        after = lg.source_tree_sha256(src)
    failure: str | None = None
    if after != before:                                  # källan ändrades: väger tyngst
        failure = "SOURCE_CHANGED"
    elif not found:                                      # inga igenkända rapporter: aldrig ok
        failure = "SOURCE_EMPTY"
    return QualityRegistrationReport(applied=apply, reports=tuple(results),
                                     unrecognized=tuple(unrecognized),
                                     source_tree_sha256_before=before, source_tree_sha256_after=after,
                                     failure=failure)
