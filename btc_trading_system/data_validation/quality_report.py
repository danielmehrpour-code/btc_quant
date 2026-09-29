"""Phase 3: deterministiska kvalitetsrapporter (JSON + Markdown).

Varje rapport binds till datasetens data_version (verifierad mot innehållet),
konfigurationens kanoniska hash och kodversionen. Ingen väggklocktid ingår.
Rapporter skrivs endast till en angiven katalog, aldrig till datalagret.
"""
from __future__ import annotations

import json
import math
from collections.abc import Sequence
from enum import Enum
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data.schema import DatasetKey, Instrument
from btc_trading_system.data.storage import atomic_write_bytes, content_hash
from btc_trading_system.data_validation import anomalies as an
from btc_trading_system.data_validation import consistency as cs
from btc_trading_system.data_validation import invariants as inv
from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.data_validation.ohlcv import compute_gap_flags

SCHEMA_VERSION = 1
NOTES = (
    "Phase 3 upptäcker och rapporterar; inga data har ändrats.",
    "causal=true: flaggan för bar t använder endast barer <= t (skalor: < t).",
    "causal=false: granskning över hela datasetet; får inte matas in i beslut.",
    "UNKNOWN betyder otillräckligt underlag och räknas aldrig som PASS.",
    "Tröskelvärden är låsta i configs/data_quality.yaml (se config.canonical_sha256).",
)


class ReportBindingError(ValueError):
    """Rapporten matchar inte datasetet (data_version) eller indata är inkonsistent."""


class Phase2SuspectError(RuntimeError):
    """Phase 3 har hittat något som kan vara ett Phase 2-fel. Stoppa och rapportera."""


# ------------------------------------------------------------------ hjälp
def _jsonable(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, bool | np.bool_):
        return bool(obj)
    if isinstance(obj, int | np.integer):
        return int(obj)
    if isinstance(obj, float | np.floating):
        value = float(obj)
        return None if math.isnan(value) or math.isinf(value) else value
    if obj is None or isinstance(obj, str):
        return obj
    raise TypeError(f"ej JSON-serialiserbar typ i rapport: {type(obj).__name__}")


def _check_dict(res: qc.CheckResult) -> dict[str, Any]:
    return {
        "name": res.name,
        "status": res.status.value,
        "causal": res.causal,
        "reason": res.reason,
        "findings": [
            {"code": f.code, "severity": f.severity.value, "count": f.count,
             "message": f.message, "examples": list(f.examples)}
            for f in res.findings
        ],
        "details": res.details,
    }


def _verify_binding(frame: pd.DataFrame, manifest: dict[str, Any], key: DatasetKey) -> None:
    if manifest.get("data_version") != content_hash(frame):
        raise ReportBindingError(f"{key.relpath}: data matchar inte manifestets data_version")
    for field in ("instrument", "symbol", "timeframe", "exchange"):
        expected = key.instrument.value if field == "instrument" else getattr(key, field)
        if manifest.get(field) != expected:
            raise ReportBindingError(f"{key.relpath}: manifest.{field} != {expected}")


def _dataset_ref(key: DatasetKey, manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "exchange": key.exchange,
        "instrument": key.instrument.value,
        "symbol": key.symbol,
        "timeframe": key.timeframe,
        "data_version": manifest["data_version"],
        "rows": manifest["rows"],
        "first_open_time": manifest["first_open_time"],
        "last_open_time": manifest["last_open_time"],
    }


def _envelope(report_type: str, checks: Sequence[qc.CheckResult], datasets: list[dict],
              cfg: qc.QualityConfig, code_version: str, worktree_clean: bool | None,
              extra: dict[str, Any] | None = None) -> dict[str, Any]:
    report = {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "overall": qc.overall_status(c.status for c in checks),
        "config": {"version": cfg.version, "canonical_sha256": cfg.canonical_sha256},
        "code": {"version": code_version, "worktree_clean": worktree_clean},
        "datasets": datasets,
        "checks": [_check_dict(c) for c in checks],
        "notes": list(NOTES),
        **(extra or {}),
    }
    return _jsonable(report)


# ------------------------------------------------------------------ byggare
def build_dataset_report(frame: pd.DataFrame, key: DatasetKey, manifest: dict[str, Any],
                         cfg: qc.QualityConfig, code_version: str,
                         worktree_clean: bool | None) -> dict[str, Any]:
    """frame = DatasetStore.read()-format (kolumn open_time, kanoniska kolumner)."""
    _verify_binding(frame, manifest, key)
    bars = qc.ensure_time_index(frame)
    ns = qc.index_ns(bars.index)
    step = int(TIMEFRAMES[key.timeframe].total_seconds()) * 10**9
    expected = int((ns[-1] - ns[0]) // step + 1) if len(ns) else 0
    _, gaps = compute_gap_flags(ns, key.timeframe)
    missing = expected - len(bars)
    if missing != int(manifest.get("missing_bars_total", -1)):
        raise Phase2SuspectError(
            f"{key.relpath}: saknade barer enligt data ({missing}) != manifest "
            f"missing_bars_total ({manifest.get('missing_bars_total')}); möjligt Phase 2-fel"
        )
    coverage = {
        "expected_slots": expected,
        "present_bars": len(bars),
        "missing_bars": missing,
        "gap_count": len(gaps),
        "largest_gap_bars": int(gaps["missing_bars"].max()) if len(gaps) else 0,
        "duplicates_dropped": manifest.get("duplicates_dropped"),
        "zero_volume_bars": int((bars["volume"].to_numpy(dtype=float) == 0).sum()),
    }
    checks = (
        inv.check_invariants(bars, cfg.invariants),
        *an.anomaly_checks(bars, key.timeframe, cfg.anomalies),
        cs.replay_check(bars, key.timeframe, cfg.replay),
    )
    return _envelope("dataset", checks, [_dataset_ref(key, manifest)], cfg, code_version,
                     worktree_clean, {"coverage": coverage})


def build_tf_report(lower: pd.DataFrame, lower_key: DatasetKey, lower_manifest: dict[str, Any],
                    higher: pd.DataFrame, higher_key: DatasetKey,
                    higher_manifest: dict[str, Any], cfg: qc.QualityConfig, code_version: str,
                    worktree_clean: bool | None) -> dict[str, Any]:
    if (lower_key.instrument, lower_key.symbol) != (higher_key.instrument, higher_key.symbol):
        raise ReportBindingError("tf-rapport kräver samma instrument och symbol")
    _verify_binding(lower, lower_manifest, lower_key)
    _verify_binding(higher, higher_manifest, higher_key)
    check = cs.tf_aggregation_check(lower, lower_key.timeframe, higher, higher_key.timeframe,
                                    cfg.consistency)
    refs = [_dataset_ref(lower_key, lower_manifest), _dataset_ref(higher_key, higher_manifest)]
    return _envelope("tf_aggregation", (check,), refs, cfg, code_version, worktree_clean)


def build_xinst_report(spot: pd.DataFrame, spot_key: DatasetKey, spot_manifest: dict[str, Any],
                       perp: pd.DataFrame, perp_key: DatasetKey, perp_manifest: dict[str, Any],
                       cfg: qc.QualityConfig, code_version: str,
                       worktree_clean: bool | None) -> dict[str, Any]:
    if spot_key.instrument is not Instrument.SPOT or perp_key.instrument is not Instrument.PERP:
        raise ReportBindingError("xinst-rapport kräver (spot, perp) i den ordningen")
    if (spot_key.symbol, spot_key.timeframe) != (perp_key.symbol, perp_key.timeframe):
        raise ReportBindingError("xinst-rapport kräver samma symbol och timeframe")
    _verify_binding(spot, spot_manifest, spot_key)
    _verify_binding(perp, perp_manifest, perp_key)
    checks = cs.xinst_checks(spot, perp, spot_key.timeframe, cfg.consistency)
    refs = [_dataset_ref(spot_key, spot_manifest), _dataset_ref(perp_key, perp_manifest)]
    return _envelope("cross_instrument", checks, refs, cfg, code_version, worktree_clean)


# ------------------------------------------------------------------ serialisering
def report_to_json(report: dict[str, Any]) -> bytes:
    text = json.dumps(report, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
    return (text + "\n").encode()


def report_stem(report: dict[str, Any]) -> str:
    ds = report["datasets"]
    kind = report["report_type"]
    if kind == "dataset":
        d = ds[0]
        return f"dataset_{d['instrument']}_{d['symbol']}_{d['timeframe']}_{d['data_version'][:16]}"
    if kind == "tf_aggregation":
        lo, hi = ds
        return (f"tf_{lo['instrument']}_{lo['symbol']}_{lo['timeframe']}-{hi['timeframe']}_"
                f"{lo['data_version'][:12]}_{hi['data_version'][:12]}")
    s, p = ds
    return (f"xinst_{s['symbol']}_{s['timeframe']}_"
            f"{s['data_version'][:12]}_{p['data_version'][:12]}")


def report_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# Datakvalitetsrapport: {report['report_type']}",
        "",
        f"**Samlad status: {report['overall']}**",
        "",
        f"- Konfiguration: `{report['config']['canonical_sha256']}`",
        (
            f"- Kod: `{report['code']['version']}` (ren arbetskatalog: "
            f"{report['code']['worktree_clean']})"
        ),
        "",
        "## Dataset",
        "",
        "| Instrument | Symbol | Timeframe | Rader | Första | Sista | data_version |",
        "|---|---|---|---|---|---|---|",
    ]
    lines += [
        f"| {d['instrument']} | {d['symbol']} | {d['timeframe']} | {d['rows']} | "
        f"{d['first_open_time']} | {d['last_open_time']} | `{d['data_version']}` |"
        for d in report["datasets"]
    ]
    if "coverage" in report:
        lines += ["", "## Täckning", ""]
        lines += [f"- {k}: {v}" for k, v in sorted(report["coverage"].items())]
    lines += ["", "## Kontroller", "", "| Kontroll | Status | Kausal | Fynd | Orsak |",
              "|---|---|---|---|---|"]
    lines += [
        f"| {c['name']} | {c['status']} | {c['causal']} | {len(c['findings'])} | "
        f"{c['reason'] or ''} |"
        for c in report["checks"]
    ]
    for c in report["checks"]:
        if not c["findings"]:
            continue
        lines += ["", f"### {c['name']}", ""]
        for f in c["findings"]:
            lines.append(f"- **{f['code']}** ({f['severity']}, {f['count']}): {f['message']}")
            lines += [f"  - `{e}`" for e in f["examples"]]
    lines += ["", "## Noter", ""]
    lines += [f"- {n}" for n in report["notes"]]
    return "\n".join(lines) + "\n"


def write_report(report: dict[str, Any], out_dir: str | Path) -> tuple[Path, Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = report_stem(report)
    json_path, md_path = out / f"{stem}.quality.json", out / f"{stem}.quality.md"
    atomic_write_bytes(json_path, report_to_json(report))
    atomic_write_bytes(md_path, report_to_markdown(report).encode())
    return json_path, md_path


def load_report(path: str | Path, expected_data_versions: Sequence[str]) -> dict[str, Any]:
    """Läser en rapport och kräver att den gäller exakt dessa data_version (i ordning)."""
    report = json.loads(Path(path).read_text(encoding="utf-8"))
    got = [d["data_version"] for d in report.get("datasets", [])]
    if got != list(expected_data_versions):
        raise ReportBindingError(
            f"rapporten gäller {got}, förväntade {list(expected_data_versions)}"
        )
    if report.get("schema_version") != SCHEMA_VERSION:
        raise ReportBindingError(f"okänd schema_version {report.get('schema_version')}")
    return report
