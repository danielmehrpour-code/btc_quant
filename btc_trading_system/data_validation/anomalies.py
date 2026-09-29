"""Phase 3, del B: kausala anomaliflaggor per bar (WARN, aldrig FAIL).

KAUSALITET: skalor och medianer beräknas över tidsfönstret
[open_time_t - W, open_time_t) (closed="left"): baren själv ingår aldrig och
inga senare barer används. Flaggan för bar t är därför känd vid barens
available_at. Otillräckligt underlag ger <NA>, aldrig False.
Den enda icke-kausala delen är sammanfattningen av STALE-sekvensernas fulla
längd i efterhand (granskning i rapporten, markerad som sådan).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import pandas as pd

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data_validation import quality_config as qc

IQR_TO_SIGMA = 1.349
ANOMALY_CODES = ("RETURN_OUTLIER", "RANGE_OUTLIER", "VOLUME_OUTLIER", "STALE_RUN")
REQUIRED_COLUMNS = ("open", "high", "low", "close", "volume", "gap_before")


@dataclass(frozen=True)
class WindowParams:
    timeframe: str
    bar: timedelta
    window: timedelta
    window_bars: int
    min_obs: int
    stale_bars: int


def window_params(timeframe: str, cfg: qc.AnomaliesConfig) -> WindowParams:
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"okänd timeframe {timeframe!r}")
    bar = TIMEFRAMES[timeframe]
    window = max(timedelta(days=cfg.window_min_days), bar * cfg.window_min_bars)
    window_bars = window // bar
    min_obs = math.ceil(cfg.min_obs_fraction * window_bars)
    stale_bars = max(cfg.stale_min_bars, math.ceil(timedelta(minutes=cfg.stale_min_minutes) / bar))
    return WindowParams(timeframe, bar, window, window_bars, min_obs, stale_bars)


def log_returns(bars: pd.DataFrame) -> pd.Series:
    """r_t = ln(C_t / C_{t-1}); NaN för första raden och för bar direkt efter en lucka."""
    close = bars["close"].astype(float)
    r = np.log(close / close.shift(1))
    gap = bars["gap_before"].to_numpy(dtype=bool)
    return r.mask(pd.Series(gap, index=bars.index))


def _masked(values: pd.Series, known: pd.Series) -> pd.arrays.BooleanArray:
    v = values.to_numpy(dtype=bool, na_value=False)
    k = known.to_numpy(dtype=bool, na_value=False)
    return pd.arrays.BooleanArray(v & k, ~k)


def stale_run_length(bars: pd.DataFrame) -> np.ndarray:
    """R_t: längden HITTILLS av en sekvens frusna barer (V=0 och O=H=L=C).

    En lucka (gap_before) startar en ny sekvens. Beror bara på rader <= t.
    """
    o = bars["open"].to_numpy(dtype=float)
    h = bars["high"].to_numpy(dtype=float)
    lo = bars["low"].to_numpy(dtype=float)
    c = bars["close"].to_numpy(dtype=float)
    frozen = (bars["volume"].to_numpy(dtype=float) == 0) & (o == h) & (h == lo) & (lo == c)
    gap = bars["gap_before"].to_numpy(dtype=bool)
    prev = np.concatenate([[False], frozen[:-1]])
    starts = frozen & (~prev | gap)
    positions = np.arange(len(frozen))
    last_start = np.maximum.accumulate(np.where(starts, positions, -1))
    return np.where(frozen, positions - last_start + 1, 0).astype(np.int64)


def compute_anomaly_flags(
    bars: pd.DataFrame, timeframe: str, cfg: qc.AnomaliesConfig
) -> pd.DataFrame:
    """Kausala flaggor per bar. Flaggkolumner är nullable boolean (<NA> = okänt)."""
    bars = qc.ensure_time_index(bars)
    qc.require_sorted_unique(bars, "anomalies")
    qc.require_columns(bars, REQUIRED_COLUMNS, "anomalies")
    p = window_params(timeframe, cfg)

    def trailing(series: pd.Series) -> pd.api.typing.Rolling:
        return series.rolling(p.window, closed="left", min_periods=p.min_obs)

    r = log_returns(bars)
    rr = trailing(r)
    r_med = rr.median()
    r_scale = (rr.quantile(0.75) - rr.quantile(0.25)) / IQR_TO_SIGMA
    r_z = (r - r_med).abs() / np.maximum(r_scale, cfg.return_scale_floor)

    close = bars["close"].astype(float)
    ratio = (bars["high"].astype(float) - bars["low"].astype(float)) / close
    ratio_med = trailing(ratio).median()
    range_limit = cfg.range_ratio_threshold * np.maximum(ratio_med, cfg.range_floor)

    vol = bars["volume"].astype(float)
    vol_med = trailing(vol).median()
    vol_known = vol_med.notna() & (vol_med > 0)
    vol_ratio = (vol / vol_med.where(vol_known)).where(vol_known)

    run = stale_run_length(bars)
    out = pd.DataFrame(
        {
            "log_return": r,
            "return_median": r_med,
            "return_scale": r_scale,
            "return_z": r_z,
            "return_outlier": _masked(r_z > cfg.return_z_threshold, r_z.notna()),
            "range_ratio": ratio,
            "range_median": ratio_med,
            "range_outlier": _masked(ratio > range_limit, ratio_med.notna()),
            "volume_ratio": vol_ratio,
            "volume_outlier": _masked(vol_ratio > cfg.volume_ratio_threshold, vol_known),
            "stale_run_length": run,
            "stale_run": run >= p.stale_bars,
        },
        index=bars.index,
    )
    return out


def _flag_check(
    name: str, code: str, flag: pd.Series, index: pd.DatetimeIndex, details: dict
) -> qc.CheckResult:
    known = flag.notna().to_numpy()
    hit = flag.to_numpy(dtype=bool, na_value=False)
    evaluable = int(known.sum())
    flagged = int(hit.sum())
    details = {
        **details,
        "bars_total": len(flag),
        "bars_evaluable": evaluable,
        "bars_unknown": len(flag) - evaluable,
        "bars_flagged": flagged,
    }
    if evaluable == 0:
        return qc.CheckResult(
            name, qc.Status.UNKNOWN, True, (), details,
            reason="ingen bar hade tillräckligt underlag (uppvärmning/luckor)",
        )
    findings: tuple[qc.Finding, ...] = ()
    if flagged:
        findings = (
            qc.Finding(
                code, qc.Severity.WARN, flagged,
                f"{flagged} av {evaluable} bedömbara barer flaggade",
                qc.examples_from_mask(index, hit),
            ),
        )
    status = qc.Status.WARN if flagged else qc.Status.PASS
    return qc.CheckResult(name, status, True, findings, details)


def stale_runs(bars: pd.DataFrame, min_length: int) -> list[dict]:
    """GRANSKNING (icke-kausal): varje sekvens fulla längd, för sekvenser >= min_length.

    En sekvens slutar vid bar e om nästa bar inte fortsätter räkningen (R_{e+1} != R_e + 1).
    """
    run = stale_run_length(bars)
    nxt = np.concatenate([run[1:], [0]])
    ends = np.flatnonzero((run > 0) & (nxt != run + 1))
    return [
        {
            "start": bars.index[int(e) - int(run[e]) + 1].isoformat(),
            "end": bars.index[int(e)].isoformat(),
            "length": int(run[e]),
        }
        for e in ends
        if run[e] >= min_length
    ]


def anomaly_checks(
    bars: pd.DataFrame, timeframe: str, cfg: qc.AnomaliesConfig
) -> tuple[qc.CheckResult, ...]:
    bars = qc.ensure_time_index(bars)
    p = window_params(timeframe, cfg)
    params = {
        "window_seconds": int(p.window.total_seconds()),
        "window_bars": p.window_bars,
        "min_obs": p.min_obs,
    }
    if len(bars) == 0:
        return tuple(
            qc.CheckResult(n, qc.Status.UNKNOWN, True, (), dict(params), reason="inga barer")
            for n in ("return_outlier", "range_outlier", "volume_outlier", "stale_run")
        )
    f = compute_anomaly_flags(bars, timeframe, cfg)
    idx = bars.index
    ret = _flag_check(
        "return_outlier", "RETURN_OUTLIER", f["return_outlier"], idx,
        {**params, "z_threshold": cfg.return_z_threshold, "scale_floor": cfg.return_scale_floor},
    )
    rng = _flag_check(
        "range_outlier", "RANGE_OUTLIER", f["range_outlier"], idx,
        {**params, "ratio_threshold": cfg.range_ratio_threshold, "floor": cfg.range_floor},
    )
    vol = _flag_check(
        "volume_outlier", "VOLUME_OUTLIER", f["volume_outlier"], idx,
        {**params, "ratio_threshold": cfg.volume_ratio_threshold},
    )
    runs = stale_runs(bars, p.stale_bars)
    stale_hit = f["stale_run"].to_numpy(dtype=bool)
    flagged = int(stale_hit.sum())
    stale_details = {
        "stale_min_bars": p.stale_bars,
        "bars_total": len(bars),
        "bars_evaluable": len(bars),
        "bars_unknown": 0,
        "bars_flagged": flagged,
        "frozen_bars_total": int((f["stale_run_length"] > 0).sum()),
        "max_run_length": int(f["stale_run_length"].max()),
        "runs_reaching_threshold": len(runs),
        "runs_examples_audit": runs[: qc.MAX_EXAMPLES],
    }
    stale_findings: tuple[qc.Finding, ...] = ()
    if flagged:
        stale_findings = (
            qc.Finding(
                "STALE_RUN", qc.Severity.WARN, flagged,
                f"{len(runs)} fryst(a) sekvens(er) >= {p.stale_bars} barer",
                qc.examples_from_mask(idx, stale_hit),
            ),
        )
    stale = qc.CheckResult(
        "stale_run", qc.Status.WARN if flagged else qc.Status.PASS, True,
        stale_findings, stale_details,
    )
    return ret, rng, vol, stale
