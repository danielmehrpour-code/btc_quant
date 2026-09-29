"""Phase 3, del C: konsistens mellan timeframes, mellan instrument och upprepade block.

GRANSKNINGSLÄGE (causal=False): dessa kontroller använder hela datasetet och
returnerar endast rapporter, aldrig kolumner per bar. De får inte matas in i beslut.
"""
from __future__ import annotations

import hashlib
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data_validation import anomalies as an
from btc_trading_system.data_validation import quality_config as qc

PRICE_FIELDS = ("open", "high", "low", "close")
SUM_FIELDS = ("volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume")
AGG_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
)
SIG_FLOAT_FIELDS = ("open", "high", "low", "close", "volume", "quote_volume",
                    "taker_buy_base_volume", "taker_buy_quote_volume")
SIG_DTYPE = np.dtype([("f", "<f8", (8,)), ("n", "<i8")])
DAY_NS = 86_400 * 10**9


def _td_ns(td: timedelta) -> int:
    return int(td // timedelta(microseconds=1)) * 1000


def _iso(ns_values: np.ndarray) -> tuple[str, ...]:
    return tuple(
        pd.Timestamp(int(v), tz="UTC").isoformat() for v in ns_values[: qc.MAX_EXAMPLES]
    )


# ------------------------------------------------------------------ C1
def tf_aggregation_check(
    lower: pd.DataFrame,
    lower_tf: str,
    higher: pd.DataFrame,
    higher_tf: str,
    cfg: qc.ConsistencyConfig,
) -> qc.CheckResult:
    """Aggregerar kompletta lägre fönster och jämför alla nio Binance-fält."""
    tau, big = _td_ns(TIMEFRAMES[lower_tf]), _td_ns(TIMEFRAMES[higher_tf])
    if big <= tau or big % tau != 0:
        raise ValueError(f"{higher_tf} är inte en högre multipel av {lower_tf}")
    k = big // tau
    lower = qc.ensure_time_index(lower)
    higher = qc.ensure_time_index(higher)
    qc.require_columns(lower, AGG_FIELDS, "tf_aggregation(lower)")
    qc.require_columns(higher, AGG_FIELDS, "tf_aggregation(higher)")
    qc.require_sorted_unique(higher, "tf_aggregation(higher)")
    base = {"lower_timeframe": lower_tf, "higher_timeframe": higher_tf, "bars_per_window": k}
    zero_fields = dict.fromkeys(AGG_FIELDS, 0)
    empty = {**base, "windows_total": 0, "windows_complete": 0, "windows_checked": 0,
             "windows_incomplete": 0, "windows_higher_missing": 0, "windows_mismatched": 0,
             "mismatches_by_field": zero_fields}
    if len(lower) == 0:
        return qc.CheckResult("tf_aggregation", qc.Status.UNKNOWN, False, (), empty,
                              reason="lägre dataset är tomt")

    lo_ns = qc.index_ns(lower.index)
    cov_start, cov_end = int(lo_ns.min()), int(lo_ns.max()) + tau        # [start, end)
    first_h = -(-cov_start // big) * big
    windows = np.arange(first_h, cov_end - big + 1, big, dtype=np.int64)
    if len(windows) == 0:
        return qc.CheckResult("tf_aggregation", qc.Status.UNKNOWN, False, (), empty,
                              reason="inget högre fönster ryms helt inom lägre täckning")

    # Komplett <=> faktiska tidsstämplar A(h) == förväntade E(h) = {h + i*tau}, utan dubbletter.
    frame = lower.reset_index()
    frame["_ns"] = lo_ns
    frame["_w"] = (lo_ns // big) * big
    frame["_on_grid"] = ((lo_ns - frame["_w"].to_numpy()) % tau) == 0
    per_w = frame.groupby("_w").agg(
        n=("_ns", "size"), n_unique=("_ns", "nunique"), n_grid=("_on_grid", "sum")
    )
    complete_mask = (per_w["n"] == k) & (per_w["n_unique"] == k) & (per_w["n_grid"] == k)
    complete = np.intersect1d(per_w.index[complete_mask].to_numpy(dtype=np.int64), windows)
    incomplete = np.setdiff1d(windows, complete)

    sel = frame[frame["_w"].isin(complete)].sort_values("_ns", kind="stable")
    g = sel.groupby("_w", sort=True)
    agg = pd.DataFrame(
        {
            "open": g["open"].first(),
            "high": g["high"].max(),
            "low": g["low"].min(),
            "close": g["close"].last(),
            **{f: g[f].sum() for f in SUM_FIELDS},
            "trade_count": g["trade_count"].sum(),
        }
    )
    hi_ns = qc.index_ns(higher.index)
    hi_frame = higher.reset_index(drop=True)
    hi_frame.index = hi_ns
    present = np.isin(complete, hi_ns)
    missing_higher = complete[~present]
    checked = complete[present]

    mismatch_any = np.zeros(len(checked), dtype=bool)
    by_field: dict[str, int] = dict(zero_fields)
    field_hits: dict[str, np.ndarray] = {}
    a = agg.loc[checked]
    x = hi_frame.loc[checked]
    for f in AGG_FIELDS:
        if f in PRICE_FIELDS:
            bad = a[f].to_numpy(dtype=float) != x[f].to_numpy(dtype=float)
        elif f == "trade_count":
            bad = a[f].to_numpy(dtype=np.int64) != x[f].to_numpy(dtype=np.int64)
        else:
            av, xv = a[f].to_numpy(dtype=float), x[f].to_numpy(dtype=float)
            bad = np.abs(av - xv) > (cfg.agg_abs_tol + cfg.agg_rel_tol * np.abs(xv))
        by_field[f] = int(bad.sum())
        field_hits[f] = bad
        mismatch_any |= bad

    findings: list[qc.Finding] = []
    if mismatch_any.any():
        fields = ", ".join(f"{f}={n}" for f, n in by_field.items() if n)
        findings.append(qc.Finding(
            "TF_AGGREGATION_MISMATCH", qc.Severity.FAIL, int(mismatch_any.sum()),
            f"aggregerade {lower_tf}-fönster avviker från {higher_tf}: {fields}",
            _iso(checked[mismatch_any]),
        ))
    if len(missing_higher):
        findings.append(qc.Finding(
            "TF_HIGHER_MISSING", qc.Severity.WARN, len(missing_higher),
            f"kompletta {lower_tf}-fönster saknar {higher_tf}-bar", _iso(missing_higher),
        ))
    if len(incomplete):
        findings.append(qc.Finding(
            "TF_INCOMPLETE", qc.Severity.INFO, len(incomplete),
            "fönster där A(h) != E(h); räknade men inte kontrollerade", _iso(incomplete),
        ))
    details = {
        **base,
        "windows_total": len(windows),
        "windows_complete": len(complete),
        "windows_checked": len(checked),
        "windows_incomplete": len(incomplete),
        "windows_higher_missing": len(missing_higher),
        "windows_mismatched": int(mismatch_any.sum()),
        "mismatches_by_field": by_field,
    }
    if len(checked) == 0:
        return qc.CheckResult("tf_aggregation", qc.Status.UNKNOWN, False, tuple(findings),
                              details, reason="0 kontrollerbara fönster")
    if mismatch_any.any():
        status = qc.Status.FAIL
    elif len(missing_higher):
        status = qc.Status.WARN
    else:
        status = qc.Status.PASS
    return qc.CheckResult("tf_aggregation", status, False, tuple(findings), details)


# ------------------------------------------------------------------ C2
def pick_lag(corrs: dict[int, float]) -> int:
    """Högst korrelation; lika -> minsta |lag|; därefter minsta lag."""
    return max(corrs, key=lambda lag: (corrs[lag], -abs(lag), -lag))


def _pearson(x: np.ndarray, y: np.ndarray) -> float:
    xm, ym = x - x.mean(), y - y.mean()
    den = float(np.sqrt((xm * xm).sum() * (ym * ym).sum()))
    return float((xm * ym).sum() / den) if den > 0 else float("nan")


def xinst_checks(
    spot: pd.DataFrame, perp: pd.DataFrame, timeframe: str, cfg: qc.ConsistencyConfig
) -> tuple[qc.CheckResult, qc.CheckResult, qc.CheckResult]:
    spot = qc.ensure_time_index(spot)
    perp = qc.ensure_time_index(perp)
    qc.require_sorted_unique(spot, "xinst(spot)")
    qc.require_sorted_unique(perp, "xinst(perp)")
    step = _td_ns(TIMEFRAMES[timeframe])
    s_ns, p_ns = qc.index_ns(spot.index), qc.index_ns(perp.index)
    unknown = "inga överlappande tidsstämplar"
    if len(s_ns) == 0 or len(p_ns) == 0 or max(s_ns[0], p_ns[0]) > min(s_ns[-1], p_ns[-1]):
        return (
            qc.CheckResult("xinst_alignment", qc.Status.UNKNOWN, False, reason=unknown),
            qc.CheckResult("xinst_basis", qc.Status.UNKNOWN, False, reason=unknown),
            qc.CheckResult("xinst_lag", qc.Status.UNKNOWN, False, reason=unknown),
        )
    lo, hi = max(s_ns[0], p_ns[0]), min(s_ns[-1], p_ns[-1])

    # --- alignment
    s_in = s_ns[(s_ns >= lo) & (s_ns <= hi)]
    p_in = p_ns[(p_ns >= lo) & (p_ns <= hi)]
    only_s, only_p = np.setdiff1d(s_in, p_in), np.setdiff1d(p_in, s_in)
    common = np.intersect1d(s_in, p_in)
    align_findings: list[qc.Finding] = []
    if len(only_s):
        align_findings.append(qc.Finding("XINST_ALIGNMENT", qc.Severity.WARN, len(only_s),
                                         "tidsstämplar endast i spot", _iso(only_s)))
    if len(only_p):
        align_findings.append(qc.Finding("XINST_ALIGNMENT", qc.Severity.WARN, len(only_p),
                                         "tidsstämplar endast i perp", _iso(only_p)))
    alignment = qc.CheckResult(
        "xinst_alignment", qc.Status.WARN if align_findings else qc.Status.PASS, False,
        tuple(align_findings),
        {"overlap_start": _iso(np.array([lo]))[0], "overlap_end": _iso(np.array([hi]))[0],
         "common_bars": len(common), "only_spot": len(only_s), "only_perp": len(only_p)},
    )

    # --- basis
    s_close = pd.Series(spot["close"].to_numpy(dtype=float), index=s_ns)
    p_close = pd.Series(perp["close"].to_numpy(dtype=float), index=p_ns)
    if len(common) == 0:
        basis = qc.CheckResult("xinst_basis", qc.Status.UNKNOWN, False,
                               reason="inga gemensamma barer")
    else:
        b = (p_close.loc[common].to_numpy() / s_close.loc[common].to_numpy() - 1.0) * 1e4
        absb = np.abs(b)
        fail, warn = absb > cfg.basis_fail_bps, absb > cfg.basis_warn_bps
        q = np.quantile(b, [0.01, 0.5, 0.99])
        bfind: list[qc.Finding] = []
        if fail.any():
            bfind.append(qc.Finding("XINST_BASIS", qc.Severity.FAIL, int(fail.sum()),
                                    f"|basis| > {cfg.basis_fail_bps} bps", _iso(common[fail])))
        if warn.any():
            bfind.append(qc.Finding("XINST_BASIS", qc.Severity.WARN, int(warn.sum()),
                                    f"|basis| > {cfg.basis_warn_bps} bps", _iso(common[warn])))
        status = qc.Status.FAIL if fail.any() else qc.Status.WARN if warn.any() else qc.Status.PASS
        basis = qc.CheckResult(
            "xinst_basis", status, False, tuple(bfind),
            {"bars": len(common), "p01_bps": float(q[0]), "p50_bps": float(q[1]),
             "p99_bps": float(q[2]), "min_bps": float(b.min()), "max_bps": float(b.max()),
             "warn_bps": cfg.basis_warn_bps, "fail_bps": cfg.basis_fail_bps},
        )

    # --- lag (diagnostisk; ger aldrig FAIL)
    r_s = pd.Series(an.log_returns(spot).to_numpy(dtype=float), index=s_ns)
    r_p = pd.Series(an.log_returns(perp).to_numpy(dtype=float), index=p_ns)
    lags = range(-cfg.lag_max_bars, cfg.lag_max_bars + 1)
    rows: list[dict[str, Any]] = []
    corrs: dict[int, float] = {}
    for lag in lags:
        shifted = pd.Series(r_p.to_numpy(), index=r_p.index - lag * step)
        pair = pd.concat([r_s, shifted], axis=1, join="inner").dropna()
        c = float("nan")
        if len(pair):
            c = _pearson(pair.iloc[:, 0].to_numpy(), pair.iloc[:, 1].to_numpy())
        corrs[lag] = c
        rows.append({"lag": lag, "n_pairs": len(pair), "corr": None if np.isnan(c) else c})
    lag_details = {"lags": rows, "min_pairs": cfg.lag_min_pairs, "min_corr0": cfg.lag_min_corr0}
    too_few = [r["lag"] for r in rows if r["n_pairs"] < cfg.lag_min_pairs]
    if too_few:
        lag_res = qc.CheckResult("xinst_lag", qc.Status.UNKNOWN, False, (), lag_details,
                                 reason=f"färre än {cfg.lag_min_pairs} par för lag {too_few}")
    elif any(np.isnan(v) for v in corrs.values()):
        lag_res = qc.CheckResult("xinst_lag", qc.Status.UNKNOWN, False, (), lag_details,
                                 reason="korrelation odefinierad (nollvarians)")
    else:
        best = pick_lag(corrs)
        lag_details = {**lag_details, "best_lag": best, "corr0": corrs[0]}
        lfind: list[qc.Finding] = []
        if best != 0:
            lfind.append(qc.Finding("XINST_LAG", qc.Severity.WARN, 1,
                                    f"högst korrelation vid lag {best} (diagnostisk signal)"))
        elif corrs[0] < cfg.lag_min_corr0:
            lfind.append(qc.Finding("XINST_LAG", qc.Severity.WARN, 1,
                                    f"svag samvariation: corr0={corrs[0]:.4f}"))
        lag_res = qc.CheckResult("xinst_lag", qc.Status.WARN if lfind else qc.Status.PASS,
                                 False, tuple(lfind), lag_details)
    return alignment, basis, lag_res


# ------------------------------------------------------------------ C3
def block_signature(block: pd.DataFrame) -> str:
    """SHA-256 av struct.pack('<8dq', O,H,L,C,V,QV,TB,TQ,N) per bar i tidsordning."""
    arr = np.empty(len(block), dtype=SIG_DTYPE)
    arr["f"] = block[list(SIG_FLOAT_FIELDS)].to_numpy(dtype="<f8")
    arr["n"] = block["trade_count"].to_numpy(dtype="<i8")
    return hashlib.sha256(arr.tobytes()).hexdigest()


def replay_check(bars: pd.DataFrame, timeframe: str, cfg: qc.ReplayConfig) -> qc.CheckResult:
    bar = TIMEFRAMES[timeframe]
    if bar > TIMEFRAMES[cfg.max_timeframe]:
        return qc.CheckResult("replayed_block", qc.Status.UNKNOWN, False,
                              details={"max_timeframe": cfg.max_timeframe},
                              reason=f"not applicable: {timeframe} > {cfg.max_timeframe}")
    bars = qc.ensure_time_index(bars)
    qc.require_columns(bars, (*SIG_FLOAT_FIELDS, "trade_count"), "replayed_block")
    per_day = timedelta(days=1) // bar
    frame = bars.reset_index()
    frame["_ns"] = qc.index_ns(bars.index)
    frame["_day"] = frame["_ns"] // DAY_NS
    frame = frame.sort_values("_ns", kind="stable")
    groups: dict[str, list[str]] = {}
    total = qualifying = 0
    for day, block in frame.groupby("_day", sort=True):
        total += 1
        closes = block["close"].to_numpy(dtype=float)
        volume_total = float(block["volume"].to_numpy(dtype=float).sum())
        if len(block) != per_day or len(np.unique(closes)) < 2 or volume_total <= 0:
            continue
        qualifying += 1
        date = pd.Timestamp(int(day) * DAY_NS, tz="UTC").date().isoformat()
        groups.setdefault(block_signature(block), []).append(date)
    dupes = sorted(((sig, days) for sig, days in groups.items() if len(days) > 1),
                   key=lambda item: item[1][0])
    details = {
        "bars_per_block": per_day,
        "blocks_total": total,
        "blocks_qualifying": qualifying,
        "blocks_not_qualifying": total - qualifying,
        "duplicate_group_count": len(dupes),
        "duplicate_groups": [{"signature": s, "days": d} for s, d in dupes[: qc.MAX_EXAMPLES]],
    }
    if qualifying < 2:
        return qc.CheckResult("replayed_block", qc.Status.UNKNOWN, False, (), details,
                              reason=f"endast {qualifying} kvalificerade block (< 2)")
    if dupes:
        n_days = sum(len(d) for _, d in dupes)
        finding = qc.Finding("REPLAYED_BLOCK", qc.Severity.FAIL, n_days,
                             f"{len(dupes)} grupp(er) av dygn med identisk signatur",
                             tuple(", ".join(d) for _, d in dupes[: qc.MAX_EXAMPLES]))
        return qc.CheckResult("replayed_block", qc.Status.FAIL, False, (finding,), details)
    return qc.CheckResult("replayed_block", qc.Status.PASS, False, (), details)
