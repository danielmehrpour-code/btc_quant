"""Kvalitetskontroller för funding-räntor (etapp B steg 4, SB-S4-d). Phase 3-stil: upptäcker och
rapporterar, ändrar aldrig data (P3-03); FAIL förblir FAIL.

Kontroller på en funding-ram (data/funding.py), utan tröskel vald ur datan:
  funding_structure  kolumner, dtypes, tz-aware UTC, hela millisekunder, minst en rad
  funding_order      calc_time strikt stigande och unik
  funding_interval   funding_interval_hours > 0
  funding_coverage   avståndet mellan två på varandra följande rader = föregående rads
                     funding_interval_hours, inom spacing_tolerance_ms (låst i
                     configs/funding_quality.yaml; motivering där)
Konfigurationen läses strikt och låses av ett test (filens och den kanoniska sha256, som P3-01).
Kontrollerna kräver en strukturellt giltig ram; annars UNKNOWN för de beroende kontrollerna.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from btc_trading_system.data.funding import FUNDING_COLUMNS
from btc_trading_system.data_validation.quality_config import (
    MAX_EXAMPLES,
    CheckResult,
    Finding,
    QualityConfigError,
    Severity,
    Status,
    canonical_json,
    overall_status,
)

FUNDING_QUALITY_CONFIG_VERSION = 1
DEFAULT_FUNDING_QUALITY_PATH = Path(__file__).resolve().parents[2] / "configs" / "funding_quality.yaml"
CHECK_NAMES = ("funding_structure", "funding_order", "funding_interval", "funding_coverage")
_HOUR_MS = 3_600_000


@dataclass(frozen=True)
class FundingQualityConfig:
    spacing_tolerance_ms: int
    canonical_sha256: str
    file_sha256: str | None = None


@dataclass(frozen=True)
class FundingQualityReport:
    checks: tuple[CheckResult, ...]
    overall: str
    config_sha256: str
    rows: int


def parse_funding_quality_config(data: Any, file_sha256: str | None = None) -> FundingQualityConfig:
    if not isinstance(data, dict) or set(data) != {"version", "coverage"}:
        raise QualityConfigError("funding_quality: nycklarna måste vara exakt version, coverage")
    if data["version"] != FUNDING_QUALITY_CONFIG_VERSION or isinstance(data["version"], bool):
        raise QualityConfigError(f"funding_quality: version måste vara {FUNDING_QUALITY_CONFIG_VERSION}")
    cov = data["coverage"]
    if not isinstance(cov, dict) or set(cov) != {"spacing_tolerance_ms"}:
        raise QualityConfigError("funding_quality.coverage: nyckeln måste vara exakt spacing_tolerance_ms")
    tol = cov["spacing_tolerance_ms"]
    if not isinstance(tol, int) or isinstance(tol, bool) or not 0 < tol < _HOUR_MS:
        raise QualityConfigError("funding_quality.coverage.spacing_tolerance_ms: heltal i (0, 3600000)")
    return FundingQualityConfig(tol, hashlib.sha256(canonical_json(data)).hexdigest(), file_sha256)


def load_funding_quality_config(path: str | Path | None = None) -> FundingQualityConfig:
    raw = Path(path or DEFAULT_FUNDING_QUALITY_PATH).read_bytes()
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise QualityConfigError(f"funding_quality: ogiltig YAML: {e}") from e
    return parse_funding_quality_config(data, file_sha256=hashlib.sha256(raw).hexdigest())


def _iso(ms: int) -> str:
    return pd.Timestamp(int(ms), unit="ms", tz="UTC").isoformat()


def _structure(df: pd.DataFrame) -> tuple[CheckResult, np.ndarray | None]:
    problems: list[str] = []
    if tuple(df.columns) != FUNDING_COLUMNS:
        problems.append(f"kolumner {tuple(df.columns)} != {FUNDING_COLUMNS}")
    elif len(df) < 1:
        problems.append("inga rader")
    else:
        ct = df["calc_time"]
        if not isinstance(ct.dtype, pd.DatetimeTZDtype) or str(ct.dt.tz) != "UTC" or ct.isna().any():
            problems.append("calc_time är inte tz-aware UTC utan NaT")
        for c in ("funding_interval_hours", "funding_rate_e12"):
            if df[c].dtype != np.dtype("int64"):
                problems.append(f"{c} är inte int64")
    ms: np.ndarray | None = None
    if not problems:
        ns = df["calc_time"].dt.tz_localize(None).to_numpy().astype("datetime64[ns]").astype("int64")
        if (ns % 1_000_000 != 0).any():
            problems.append("calc_time är inte hela millisekunder")
        else:
            ms = ns // 1_000_000
    if problems:
        f = Finding("FUNDING_STRUCTURE", Severity.FAIL, len(problems), "; ".join(problems))
        return CheckResult("funding_structure", Status.FAIL, causal=True, findings=(f,)), None
    return CheckResult("funding_structure", Status.PASS, causal=True, details={"rows": len(df)}), ms


def _unknown(name: str) -> CheckResult:
    return CheckResult(name, Status.UNKNOWN, causal=True, reason="ramen är strukturellt ogiltig")


def check_funding(df: pd.DataFrame, cfg: FundingQualityConfig) -> FundingQualityReport:
    structure, ms = _structure(df)
    if ms is None:
        checks = (structure, *(_unknown(n) for n in CHECK_NAMES[1:]))
        return FundingQualityReport(checks, overall_status(c.status for c in checks), cfg.canonical_sha256, len(df))
    iv = df["funding_interval_hours"].to_numpy(dtype="int64")
    d = np.diff(ms)
    bad_order = np.nonzero(d <= 0)[0]
    order = CheckResult("funding_order", Status.PASS, causal=True) if not len(bad_order) else CheckResult(
        "funding_order", Status.FAIL, causal=True, findings=(Finding(
            "FUNDING_NOT_STRICTLY_INCREASING", Severity.FAIL, len(bad_order),
            "calc_time är inte strikt stigande och unik",
            tuple(_iso(ms[i + 1]) for i in bad_order[:MAX_EXAMPLES])),))
    bad_iv = np.nonzero(iv <= 0)[0]
    interval = CheckResult("funding_interval", Status.PASS, causal=True) if not len(bad_iv) else CheckResult(
        "funding_interval", Status.FAIL, causal=True, findings=(Finding(
            "FUNDING_INTERVAL_NOT_POSITIVE", Severity.FAIL, len(bad_iv), "funding_interval_hours <= 0",
            tuple(_iso(ms[i]) for i in bad_iv[:MAX_EXAMPLES])),))
    expected = iv[:-1] * _HOUR_MS
    bad_cov = np.nonzero(np.abs(d - expected) > cfg.spacing_tolerance_ms)[0]
    details = {"spacing_tolerance_ms": cfg.spacing_tolerance_ms, "pairs": len(d)}
    coverage = CheckResult("funding_coverage", Status.PASS, causal=True, details=details) if not len(bad_cov) \
        else CheckResult("funding_coverage", Status.FAIL, causal=True, details=details, findings=(Finding(
            "FUNDING_SPACING", Severity.FAIL, len(bad_cov),
            "avståndet till nästa rad avviker från funding_interval_hours mer än toleransen",
            tuple(f"{_iso(ms[i])} -> {_iso(ms[i + 1])}" for i in bad_cov[:MAX_EXAMPLES])),))
    checks = (structure, order, interval, coverage)
    return FundingQualityReport(checks, overall_status(c.status for c in checks), cfg.canonical_sha256, len(df))
