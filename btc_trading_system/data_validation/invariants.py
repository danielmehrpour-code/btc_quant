"""Phase 3, del A: ekonomiska invarianter per bar.

Varje flagga beror enbart på baren själv (trivialt kausal). Brott ger FAIL.
Ändrar aldrig data och påverkar inte Phase 2-ingest.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from btc_trading_system.data_validation import quality_config as qc

INVARIANT_CODES = (
    "VWAP_OUTSIDE_RANGE",
    "TAKER_BASE_EXCEEDS_VOLUME",
    "TAKER_QUOTE_EXCEEDS_QUOTE",
    "VOLUME_WITHOUT_TRADES",
    "TRADES_WITHOUT_VOLUME",
    "PRICE_MOVE_WITHOUT_VOLUME",
)
REQUIRED_COLUMNS = (
    "high",
    "low",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
)


def invariant_flags(bars: pd.DataFrame, cfg: qc.InvariantsConfig) -> pd.DataFrame:
    """En bool-kolumn per invariant (True = brott), samma index som indata."""
    bars = qc.ensure_time_index(bars)
    qc.require_columns(bars, REQUIRED_COLUMNS, "invariants")
    hi = bars["high"].to_numpy(dtype=float)
    lo = bars["low"].to_numpy(dtype=float)
    vol = bars["volume"].to_numpy(dtype=float)
    quote = bars["quote_volume"].to_numpy(dtype=float)
    tb = bars["taker_buy_base_volume"].to_numpy(dtype=float)
    tq = bars["taker_buy_quote_volume"].to_numpy(dtype=float)
    trades = bars["trade_count"].to_numpy(dtype=np.int64)

    has_vol = vol > 0
    vwap = quote / np.where(has_vol, vol, 1.0)
    tol = cfg.vwap_rel_tol
    flags = {
        "VWAP_OUTSIDE_RANGE": has_vol & ((vwap < lo * (1.0 - tol)) | (vwap > hi * (1.0 + tol))),
        "TAKER_BASE_EXCEEDS_VOLUME": (tb - vol) > (cfg.taker_abs_tol + cfg.taker_rel_tol * vol),
        "TAKER_QUOTE_EXCEEDS_QUOTE": (tq - quote)
        > (cfg.taker_abs_tol + cfg.taker_rel_tol * quote),
        "VOLUME_WITHOUT_TRADES": has_vol & (trades == 0),
        "TRADES_WITHOUT_VOLUME": (trades > 0) & (vol == 0),
        "PRICE_MOVE_WITHOUT_VOLUME": (vol == 0) & (hi != lo),
    }
    return pd.DataFrame(flags, index=bars.index)


def check_invariants(bars: pd.DataFrame, cfg: qc.InvariantsConfig) -> qc.CheckResult:
    bars = qc.ensure_time_index(bars)
    if len(bars) == 0:
        return qc.CheckResult("invariants", qc.Status.UNKNOWN, True, reason="inga barer")
    flags = invariant_flags(bars, cfg)
    counts = {code: int(flags[code].sum()) for code in INVARIANT_CODES}
    findings = tuple(
        qc.Finding(
            code,
            qc.Severity.FAIL,
            counts[code],
            f"{counts[code]} bar(er) bryter invarianten {code}",
            qc.examples_from_mask(bars.index, flags[code].to_numpy()),
        )
        for code in INVARIANT_CODES
        if counts[code] > 0
    )
    details = {
        "bars_total": len(bars),
        "bars_with_volume": int((bars["volume"].to_numpy(dtype=float) > 0).sum()),
        "violations": counts,
    }
    status = qc.Status.FAIL if findings else qc.Status.PASS
    return qc.CheckResult("invariants", status, True, findings, details)
