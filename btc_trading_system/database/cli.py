"""Datastore-CLI för versionslagret (PHASE 4, steg 8). Ett tunt lager över biblioteket.

Användning:  python -m btc_trading_system.database.cli [--format json|text] <kommando> --store P ...
             python scripts/datastore.py ...  (samma main)

KOMMANDON (--store krävs alltid; ingen standardrot, ingen miljövariabel)
  init              --store P [--apply]                               VersionedStore.init
  import-legacy     --store P --source P [--apply]                    legacy.import_legacy
  ingest            --store P --dataset D FIL... [--fail-on-gaps] [--allow-regression] [--apply]
                                                                      StoreWriter.ingest
  list              --store P                                         katalog (läs) + current_ref
  versions          --store P --dataset D                             katalog (läs) + current_ref
  set-current       --store P --dataset D --data-version DV [--apply] StoreWriter.set_current
  snapshot create   --store P (--current D ... | --member D@DV ...) [--apply]
                                                                      snapshot_current / create_snapshot
  snapshot show     --store P --id ID                                 read_snapshot
  register-report   --store P --source P [--apply]                    register_quality_reports
  verify            --store P                                         integrity.verify_store
  repair            --store P [--apply]                               integrity.repair_store
  rebuild           --store P [--apply]                               integrity.rebuild_catalog_store
  complete-pending  --store P --kind ingest|ref [--apply]             complete_pending_ingest /
                                                                      complete_pending_ref_updates
  D = binance/<instrument>/<SYMBOL>/<timeframe>, strikt kanonisk (catalog.dataset_key; normaliseras
  aldrig). DV och ID = 64 hex (gemener).

INGET SKRIVS UTAN --apply. Utan --apply visar varje muterande kommando en förhandsvisning byggd
enbart av läsande biblioteksanrop (import-legacy, register-report, repair: bibliotekets egen
dry-run). Varje skrivning går genom skrivarlåset (i biblioteket eller via store.writer()).

INGEST (P4-S4-c, P4-S4-d): CLI:t hämtar now (UTC) och code_version/worktree_clean (git) EN gång,
skickar dem explicit och visar dem i utdata. --apply vägras (exit 5, ingenting skrivs) om
worktree_clean inte är True (okänd eller smutsig arbetskatalog). allow_regression=True endast med
--allow-regression.

EXITKODER
  0   lyckades (verify/repair/rebuild: lagret är rent)
  1/2/3  verify: REPAIRABLE eller WARN / PENDING / ERROR (VerifyReport.exit_status). repair och
      rebuild avslutar med allvarligheten för lagret de lämnar (dry-run: före, apply: efter).
  4   import-legacy/register-report kördes men rapporten har ok == false
  5   vägrad av lagrets tillstånd, ingenting ändrat (lås, väntande ref/ingest, regressionsskydd,
      repair/rebuild över ERROR, ingest --apply utan ren arbetskatalog)
  6   annat lager-/datafel (DataError, OSError, ValueError/TypeError från ett biblioteksanrop)
  64  användningsfel (argparse och ogiltiga argumentvärden; före varje biblioteksanrop)
  70  internt fel (traceback på stderr)

UTDATA: standard ett kanoniskt JSON-objekt på stdout (versioned_store.canonical_json_bytes);
--format text ger en läsbar återgivning av samma objekt. Fel: "error" i objektet och en rad på
stderr. Sökvägarna --store, --source och indatafilernas kataloger ersätts i meddelanden med
<store>, <source>, <input>.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import traceback
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn

from btc_trading_system.core.types import require_utc
from btc_trading_system.data import quality_cli
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data_validation.errors import DataError
from btc_trading_system.database import catalog, integrity, legacy
from btc_trading_system.database import quality_registry as qreg
from btc_trading_system.database import versioned_store as vs
from btc_trading_system.database.refs import DatasetVersionRef, DataSnapshot

CLI_FORMAT = "btc-quant/datastore-cli/v1"
EXIT_OK = 0
EXIT_NOT_OK = 4
EXIT_REFUSED = 5
EXIT_STORE_ERROR = 6
EXIT_USAGE = 64
EXIT_INTERNAL = 70
REFUSED = (vs.StoreLockedError, vs.PendingRefUpdateError, vs.PendingIngestError,
           vs.VersionRegressionError, integrity.RepairRefusedError)
MUTATING = frozenset({"init", "import-legacy", "ingest", "set-current", "snapshot create",
                      "register-report", "repair", "rebuild", "complete-pending"})
Clock = Callable[[], datetime]
GitState = Callable[[], tuple[str, bool | None]]


class UsageError(Exception):
    """Ogiltiga argument; upptäcks före varje biblioteksanrop."""


class WorktreeNotCleanError(Exception):
    """ingest --apply kräver worktree_clean is True (okänd eller smutsig arbetskatalog vägras)."""


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:       # argparse avslutar annars med 2 (= PENDING)
        raise UsageError(message)


@dataclass(frozen=True)
class _Out:
    result: dict[str, Any]
    exit_code: int
    result_sha256: str | None = None


@dataclass
class _Context:
    clock: Clock
    git: GitState
    resolved: dict[str, Any] | None = None


# ------------------------------------------------------------------ argument
def _dataset(text: str) -> DatasetKey:
    parts = text.split("/")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(f"dataset måste vara binance/<instrument>/<SYMBOL>/<timeframe>: {text!r}")
    try:
        return catalog.dataset_key(exchange=parts[0], instrument=parts[1], symbol=parts[2], timeframe=parts[3])
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"ogiltigt dataset {text!r}: {e}") from e


def _sha(text: str) -> str:
    if not catalog.is_sha256_hex(text):
        raise argparse.ArgumentTypeError(f"måste vara 64 hex-tecken (gemener): {text!r}")
    return text


def _member(text: str) -> DatasetVersionRef:
    ds, sep, dv = text.rpartition("@")
    if not sep:
        raise argparse.ArgumentTypeError(f"medlem måste vara <dataset>@<data_version>: {text!r}")
    return DatasetVersionRef(_dataset(ds), _sha(dv))


def _add_apply(p: argparse.ArgumentParser) -> None:
    p.add_argument("--apply", action="store_true", default=False,
                   help="utför ändringen (utan --apply skrivs ingenting)")


def _parser() -> argparse.ArgumentParser:
    ap = _Parser(prog="datastore", description="Phase 4 datastore-kommandon (versionslagret).",
                 allow_abbrev=False)
    ap.add_argument("--format", choices=("json", "text"), default="json")
    sub = ap.add_subparsers(dest="command", required=True)

    def command(name: str, *, apply: bool) -> argparse.ArgumentParser:
        p = sub.add_parser(name, allow_abbrev=False)
        p.add_argument("--store", required=True)
        if apply:
            _add_apply(p)
        return p

    command("init", apply=True)
    p = command("import-legacy", apply=True)
    p.add_argument("--source", required=True)
    p = command("ingest", apply=True)
    p.add_argument("--dataset", type=_dataset, required=True)
    p.add_argument("files", nargs="+")
    p.add_argument("--fail-on-gaps", action="store_true")
    p.add_argument("--allow-regression", action="store_true")
    command("list", apply=False)
    p = command("versions", apply=False)
    p.add_argument("--dataset", type=_dataset, required=True)
    p = command("set-current", apply=True)
    p.add_argument("--dataset", type=_dataset, required=True)
    p.add_argument("--data-version", type=_sha, required=True)
    snap = sub.add_parser("snapshot", allow_abbrev=False).add_subparsers(dest="snapshot_command", required=True)
    p = snap.add_parser("create", allow_abbrev=False)
    p.add_argument("--store", required=True)
    form = p.add_mutually_exclusive_group(required=True)
    form.add_argument("--current", type=_dataset, action="append")
    form.add_argument("--member", type=_member, action="append")
    _add_apply(p)
    p = snap.add_parser("show", allow_abbrev=False)
    p.add_argument("--store", required=True)
    p.add_argument("--id", type=_sha, required=True)
    p = command("register-report", apply=True)
    p.add_argument("--source", required=True)
    command("verify", apply=False)
    command("repair", apply=True)
    command("rebuild", apply=True)
    p = command("complete-pending", apply=True)
    p.add_argument("--kind", choices=("ingest", "ref"), required=True)
    return ap


def _pre_format(argv: list[str]) -> str:
    pre = _Parser(add_help=False, allow_abbrev=False)
    pre.add_argument("--format", choices=("json", "text"), default="json")
    try:
        return str(pre.parse_known_args(argv)[0].format)
    except UsageError:
        return "json"


# ------------------------------------------------------------------ utdata
class _Cleaner:
    """Ersätter de givna rötterna (absolut och realpath) i meddelanden; längsta först."""

    def __init__(self, a: argparse.Namespace) -> None:
        pairs = [(getattr(a, "store", None), "<store>"), (getattr(a, "source", None), "<source>")]
        pairs += [(os.path.dirname(os.path.abspath(f)), "<input>") for f in getattr(a, "files", None) or ()]
        subs: dict[str, str] = {}
        for path, tag in pairs:
            if not path:
                continue
            for form in (os.path.abspath(path), os.path.realpath(path)):
                if form != os.sep:
                    subs.setdefault(form, tag)
        self._subs = sorted(subs.items(), key=lambda kv: len(kv[0]), reverse=True)

    def __call__(self, text: str) -> str:
        for form, tag in self._subs:
            text = text.replace(form, tag)
        return text


def _envelope(command: str | None, applied: bool, exit_code: int, context: dict[str, Any] | None,
              out: _Out | None, error: dict[str, str] | None) -> dict[str, Any]:
    return {"format": CLI_FORMAT, "command": command, "applied": applied, "exit_code": exit_code,
            "context": context, "result": None if out is None else out.result,
            "result_sha256": None if out is None else out.result_sha256, "error": error}


def _flatten(prefix: str, value: Any, lines: list[str]) -> None:
    if isinstance(value, dict) and value:
        for k in sorted(value):
            _flatten(f"{prefix}.{k}" if prefix else k, value[k], lines)
    elif isinstance(value, list) and value:
        for i, item in enumerate(value):
            _flatten(f"{prefix}[{i}]", item, lines)
    else:
        lines.append(f"{prefix}: {json.dumps(value, ensure_ascii=False, sort_keys=True)}")


def _text(env: dict[str, Any]) -> str:
    command = env["command"] or "-"
    mode = "" if command not in MUTATING else " (utfört)" if env["applied"] else " (dry-run: ingenting skrevs)"
    lines = [f"datastore {command}: exit {env['exit_code']}{mode}"]
    ctx = env["context"]
    if ctx is not None:
        lines.append(f"kontext: now={ctx['now']} code_version={ctx['code_version']} "
                     f"worktree_clean={json.dumps(ctx['worktree_clean'])}")
        if ctx["worktree_clean"] is not True:
            lines.append("VARNING: arbetskatalogen är inte ren eller okänd; ingest --apply vägras")
    if command in ("list", "versions") and env["error"] is None:
        lines.append("OBS: dataset och versioner kommer från katalogen (härledd, kan ligga efter "
                     "filerna); kör verify för en jämförelse")
    if env["error"] is not None:
        lines.append(f"FEL {env['error']['type']}: {env['error']['message']}")
    if env["result"] is not None:
        _flatten("", env["result"], lines)
    return "\n".join(lines) + "\n"


def _emit(fmt: str, env: dict[str, Any]) -> int:
    if fmt == "text":
        sys.stdout.write(_text(env))
    else:
        sys.stdout.write(vs.canonical_json_bytes(env).decode())
    if env["error"] is not None:
        sys.stderr.write(f"datastore {env['command'] or '-'}: {env['error']['type']}: "
                         f"{env['error']['message']}\n")
    return int(env["exit_code"])


# ------------------------------------------------------------------ JSON för biblioteksresultat
def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _current_json(cur: vs.CurrentRef) -> dict[str, Any]:
    return {"data_version": None if cur.ref is None else cur.ref.data_version,
            "ref_log_seq": cur.ref_log_seq, "pending": cur.pending,
            "pending_data_version": cur.pending_data_version}


def _snapshot_out(snap: DataSnapshot, outcome: str | None) -> _Out:
    result: dict[str, Any] = {"snapshot_id": snap.snapshot_id,
                              "members": [{"dataset": m.key.relpath, **m.to_json_obj()} for m in snap.members]}
    if outcome is not None:
        result["outcome"] = outcome
    return _Out(result, EXIT_OK, _sha256(snap.to_file_bytes()))


def _ref_update_json(u: vs.RefUpdate) -> dict[str, Any]:
    return {"dataset": u.ref.key.relpath, "dataset_id": u.ref.dataset_id, "data_version": u.ref.data_version,
            "changed": u.changed, "previous_data_version": u.previous_data_version,
            "ref_log_seq": u.ref_log_seq}


def _ingest_json(out: vs.IngestOutcome) -> _Out:
    return _Out({"ingest_id": out.ingest_id, "record_sha256": out.record_sha256,
                 "record": out.record.to_json_obj()}, EXIT_OK, out.record_sha256)


def _plan_json(plan: integrity.RepairPlan) -> dict[str, Any]:
    return {"tmp_removals": list(plan.tmp_removals), "rebuild_catalog": plan.rebuild_catalog,
            "blocking": [f.to_json_obj() for f in plan.blocking]}


def _severity_exit(before: integrity.VerifyReport, after: integrity.VerifyReport | None) -> int:
    return (before if after is None else after).exit_status


@contextmanager
def _writer(store: vs.VersionedStore) -> Iterator[vs.StoreWriter]:
    with store.writer() as w:
        yield w


# ------------------------------------------------------------------ kommandon
def _init(a: argparse.Namespace, ctx: _Context) -> _Out:
    existed = os.path.lexists(Path(a.store) / vs.STORE_FILE)
    if not a.apply:
        return _Out({"already_initialised": existed, "predicted": False}, EXIT_OK)
    vs.VersionedStore.init(a.store)
    return _Out({"already_initialised": existed, "initialised": True}, EXIT_OK)


def _import_legacy(a: argparse.Namespace, ctx: _Context) -> _Out:
    rep = legacy.import_legacy(vs.VersionedStore(a.store), a.source, apply=a.apply)
    return _Out(rep.to_json_obj(), EXIT_OK if rep.ok else EXIT_NOT_OK, _sha256(rep.to_json_bytes()))


def _ingest(a: argparse.Namespace, ctx: _Context) -> _Out:
    store = vs.VersionedStore(a.store)
    now = ctx.clock()
    code_version, clean = ctx.git()
    ctx.resolved = {"now": now.isoformat() if isinstance(now, datetime) else repr(now),
                    "code_version": code_version, "worktree_clean": clean}
    now = require_utc(now, "now")
    if not a.apply:
        files = [{"name": Path(f).name, "bytes": os.stat(f).st_size, "sha256": vs.sha256_file(Path(f))}
                 for f in a.files]
        return _Out({"predicted": False, "dataset": a.dataset.relpath,
                     "dataset_id": catalog.dataset_id(a.dataset),
                     "files": sorted(files, key=lambda x: (x["name"], x["sha256"])),
                     "current": _current_json(store.current_ref(a.dataset)),
                     "fail_on_gaps": a.fail_on_gaps, "allow_regression": a.allow_regression}, EXIT_OK)
    if clean is not True:
        raise WorktreeNotCleanError(f"ingest --apply kräver en ren arbetskatalog (code_version={code_version}, "
                                    f"worktree_clean={json.dumps(clean)}); ingenting skrevs")
    with _writer(store) as w:
        out = w.ingest(a.dataset, list(a.files), now=now, code_version=code_version, worktree_clean=clean,
                       fail_on_gaps=a.fail_on_gaps, allow_regression=a.allow_regression)
    return _ingest_json(out)


def _list(a: argparse.Namespace, ctx: _Context) -> _Out:
    store = vs.VersionedStore(a.store)
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        rows = catalog.list_datasets(conn)
    finally:
        conn.close()
    datasets = []
    for row in rows:
        key = catalog.dataset_key(exchange=row["exchange"], instrument=row["instrument"],
                                  symbol=row["symbol"], timeframe=row["timeframe"])
        datasets.append({**row, "dataset": key.relpath, "current": _current_json(store.current_ref(key))})
    return _Out({"datasets_from": "catalog", "current_from": "files", "datasets": datasets}, EXIT_OK)


def _versions(a: argparse.Namespace, ctx: _Context) -> _Out:
    store = vs.VersionedStore(a.store)
    conn = catalog.open_catalog(store.catalog_path, readonly=True)
    try:
        rows = catalog.list_dataset_versions(conn, catalog.dataset_id(a.dataset))
    finally:
        conn.close()
    cur = store.current_ref(a.dataset)
    current = None if cur.ref is None else cur.ref.data_version
    return _Out({"versions_from": "catalog", "current_from": "files", "dataset": a.dataset.relpath,
                 "current": _current_json(cur),
                 "versions": [{**row, "current": row["data_version"] == current} for row in rows]}, EXIT_OK)


def _set_current(a: argparse.Namespace, ctx: _Context) -> _Out:
    store = vs.VersionedStore(a.store)
    if not a.apply:                                   # förhandsvisning: målet verifieras först vid --apply
        cur = store.current_ref(a.dataset)
        previous = None if cur.ref is None else cur.ref.data_version
        return _Out({"dataset": a.dataset.relpath, "dataset_id": catalog.dataset_id(a.dataset),
                     "data_version": a.data_version, "changed": previous != a.data_version,
                     "previous_data_version": previous, "current": _current_json(cur),
                     "target_exists": store.has_version(a.dataset, a.data_version)}, EXIT_OK)
    with _writer(store) as w:
        return _Out(_ref_update_json(w.set_current(a.dataset, a.data_version)), EXIT_OK)


def _snapshot_create(a: argparse.Namespace, ctx: _Context) -> _Out:
    store = vs.VersionedStore(a.store)
    if not a.apply:
        if a.current is not None:
            members = []
            for key in a.current:
                cur = store.current_ref(key)
                if cur.ref is None:
                    raise vs.NoCurrentRefError(f"ingen aktuell ref för {key.relpath}")
                members.append(cur.ref)
        else:
            members = list(a.member)
        snap = DataSnapshot.of(members)
        try:
            store.read_snapshot(snap.snapshot_id)
            outcome = "reused"
        except vs.SnapshotNotFoundError:
            outcome = "created"
        return _snapshot_out(snap, outcome)
    with _writer(store) as w:
        res = w.snapshot_current(a.current) if a.current is not None else w.create_snapshot(a.member)
    return _snapshot_out(res.snapshot, res.outcome)


def _snapshot_show(a: argparse.Namespace, ctx: _Context) -> _Out:
    return _snapshot_out(vs.VersionedStore(a.store).read_snapshot(a.id), None)


def _register_report(a: argparse.Namespace, ctx: _Context) -> _Out:
    rep = qreg.register_quality_reports(vs.VersionedStore(a.store), a.source, apply=a.apply)
    return _Out(rep.to_json_obj(), EXIT_OK if rep.ok else EXIT_NOT_OK, _sha256(rep.to_json_bytes()))


def _verify(a: argparse.Namespace, ctx: _Context) -> _Out:
    rep = integrity.verify_store(a.store)
    return _Out(rep.to_json_obj(), rep.exit_status, rep.report_sha256)


def _repair(a: argparse.Namespace, ctx: _Context) -> _Out:
    res = integrity.repair_store(a.store, apply=a.apply)
    return _Out({"applied": res.applied, "before": res.before.to_json_obj(), "plan": _plan_json(res.plan),
                 "after": None if res.after is None else res.after.to_json_obj()},
                _severity_exit(res.before, res.after))


def _rebuild(a: argparse.Namespace, ctx: _Context) -> _Out:
    res = integrity.rebuild_catalog_store(a.store, apply=a.apply)
    return _Out({"applied": res.applied, "before": res.before.to_json_obj(),
                 "blocking": [f.to_json_obj() for f in res.blocking],
                 "after": None if res.after is None else res.after.to_json_obj(),
                 "catalog_dump_sha256_before": res.catalog_dump_sha256_before,
                 "catalog_dump_sha256_after": res.catalog_dump_sha256_after,
                 "identical": res.identical},
                _severity_exit(res.before, res.after))


def _complete_pending(a: argparse.Namespace, ctx: _Context) -> _Out:
    if not a.apply:
        rep = integrity.verify_store(a.store)
        return _Out({"kind": a.kind, "pending": [f.to_json_obj() for f in rep.findings if f.severity == "PENDING"]},
                    EXIT_OK)
    store = vs.VersionedStore(a.store)
    with _writer(store) as w:
        if a.kind == "ingest":
            done = w.complete_pending_ingest()
            return _Out({"kind": "ingest", "completed": None if done is None else _ingest_json(done).result},
                        EXIT_OK, None if done is None else done.record_sha256)
        updates = w.complete_pending_ref_updates()
    return _Out({"kind": "ref", "completed": [_ref_update_json(u) for u in updates]}, EXIT_OK)


HANDLERS: dict[str, Callable[[argparse.Namespace, _Context], _Out]] = {
    "init": _init, "import-legacy": _import_legacy, "ingest": _ingest, "list": _list,
    "versions": _versions, "set-current": _set_current, "snapshot create": _snapshot_create,
    "snapshot show": _snapshot_show, "register-report": _register_report, "verify": _verify,
    "repair": _repair, "rebuild": _rebuild, "complete-pending": _complete_pending,
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


# ------------------------------------------------------------------ main
def main(argv: list[str] | None = None, *, clock: Clock | None = None, git: GitState | None = None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    fmt = _pre_format(argv)
    try:
        a = _parser().parse_args(argv)
    except UsageError as e:
        return _emit(fmt, _envelope(None, False, EXIT_USAGE, None, None, {"type": "UsageError", "message": str(e)}))
    command = f"snapshot {a.snapshot_command}" if a.command == "snapshot" else a.command
    applied = bool(getattr(a, "apply", False))
    clean = _Cleaner(a)
    ctx = _Context(clock or _utc_now, git or quality_cli.git_state)
    try:
        out = HANDLERS[command](a, ctx)
    except (WorktreeNotCleanError, *REFUSED) as e:
        message = clean(str(e))
        if isinstance(e, vs.VersionRegressionError):
            message += " (CLI: --allow-regression)"
        return _emit(fmt, _envelope(command, applied, EXIT_REFUSED, ctx.resolved, None,
                                    {"type": type(e).__name__, "message": message}))
    except (DataError, OSError, ValueError, TypeError) as e:
        return _emit(fmt, _envelope(command, applied, EXIT_STORE_ERROR, ctx.resolved, None,
                                    {"type": type(e).__name__, "message": clean(str(e))}))
    except Exception as e:  # noqa: BLE001 - CLI-gräns: allt annat ger exit 70
        sys.stderr.write(clean(traceback.format_exc()))
        return _emit(fmt, _envelope(command, applied, EXIT_INTERNAL, ctx.resolved, None,
                                    {"type": type(e).__name__, "message": clean(str(e))}))
    return _emit(fmt, _envelope(command, applied, out.exit_code, ctx.resolved, out, None))


if __name__ == "__main__":
    sys.exit(main())
