"""Kvalitetskontroller för jämförelsedata (etapp B steg 5, SB-S5-a, SB-S5-g). Phase 3-stil: upptäcker och
rapporterar, ändrar aldrig data (P3-03); FAIL förblir FAIL.

Kontroller (gränser ur configs/benchmark_v1.yaml, låsta före all hämtning, P3-02):
  fx_gap    dygn där senast publicerade kurs är äldre än gap_max_days -> FX_GAP_LONG (FAIL);
            ifyllda dygn räknas (FX_FILLED, INFO)
  nav_gap   samma för NAV -> NAV_GAP_LONG (FAIL), NAV_FILLED (INFO)
  nav_jump  |NAV_t / NAV_(t-1) - 1| > nav_jump_threshold mellan två på varandra följande NAV -> NAV_JUMP (WARN)
Fynd innehåller aldrig belopp, nivåer eller procent: bara kod, antal och datum. Därmed får en rapport för
FINAL TEST visa att en händelse inträffat men inget om storleken (SB-S5-f, C5).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pandas as pd

from btc_trading_system.benchmark.series import BenchmarkConfig
from btc_trading_system.data_validation.quality_config import (
    MAX_EXAMPLES,
    CheckResult,
    Finding,
    Severity,
    Status,
    overall_status,
)


@dataclass(frozen=True)
class BenchmarkQualityReport:
    checks: tuple[CheckResult, ...]
    overall: str
    config_sha256: str


def _gap(name: str, prefix: str, daily: pd.DataFrame, age_col: str, cfg: BenchmarkConfig) -> CheckResult:
    age = daily[age_col].to_numpy(dtype="int64")
    days = list(daily["day"])
    long_ = [d for d, a in zip(days, age) if a > cfg.gap_max_days]
    filled = [d for d, a in zip(days, age) if 0 < a <= cfg.gap_max_days]
    findings = []
    if filled:
        findings.append(Finding(f"{prefix}_FILLED", Severity.INFO, len(filled),
                                "dygn ifyllda bakåt med senast publicerade observation",
                                tuple(d.isoformat() for d in filled[:MAX_EXAMPLES])))
    if long_:
        findings.append(Finding(f"{prefix}_GAP_LONG", Severity.FAIL, len(long_),
                                "senast publicerade observation är äldre än gap_max_days",
                                tuple(d.isoformat() for d in long_[:MAX_EXAMPLES])))
    status = Status.FAIL if long_ else Status.PASS
    return CheckResult(name, status, causal=True, details={"days": len(days), "gap_max_days": cfg.gap_max_days},
                       findings=tuple(findings))


def _jump(nav_obs: pd.DataFrame, cfg: BenchmarkConfig) -> CheckResult:
    v = [Decimal(int(x)) for x in nav_obs["value_scaled"]]
    dates = list(nav_obs["obs_date"])
    hits = [dates[i] for i in range(1, len(v)) if abs(v[i] / v[i - 1] - 1) > cfg.nav_jump_threshold]
    if not hits:
        return CheckResult("nav_jump", Status.PASS, causal=True, details={"observations": len(v)})
    f = Finding("NAV_JUMP", Severity.WARN, len(hits), "dygnsförändring över nav_jump_threshold",
                tuple(d.isoformat() for d in hits[:MAX_EXAMPLES]))
    return CheckResult("nav_jump", Status.WARN, causal=True, details={"observations": len(v)}, findings=(f,))


def check_benchmark(cfg: BenchmarkConfig, fx_daily: pd.DataFrame, nav_daily: pd.DataFrame | None = None,
                    nav_obs: pd.DataFrame | None = None) -> BenchmarkQualityReport:
    """fx_daily: fx.daily_values(...) (kolumn age_days); nav_daily: samma för NAV; nav_obs: kanonisk NAV-ram."""
    checks = [_gap("fx_gap", "FX", fx_daily, "age_days", cfg)]
    if nav_daily is not None:
        checks.append(_gap("nav_gap", "NAV", nav_daily, "age_days", cfg))
    if nav_obs is not None:
        checks.append(_jump(nav_obs, cfg))
    return BenchmarkQualityReport(tuple(checks), overall_status(c.status for c in checks), cfg.canonical_sha256)


def render_events(report: BenchmarkQualityReport) -> str:
    """Rapporttext: per kontroll status, och per fynd kod, antal och datum. Inga belopp, nivåer eller procent."""
    lines = [f"overall: {report.overall}"]
    for c in report.checks:
        lines.append(f"{c.name}: {c.status}")
        for f in c.findings:
            lines.append(f"  {f.code} count={f.count} dates={','.join(f.examples)}")
    return "\n".join(lines) + "\n"
