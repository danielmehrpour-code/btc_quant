"""Validering av kanoniska OHLCV-frames + explicit gap-detektion.

check_ohlcv() samlar ALLA problem i en ValidationReport (kastar inte).
Anroparen väljer: report.raise_if_errors().

Issue-koder (severity):
  EMPTY, SCHEMA_MISSING_COLUMN, SCHEMA_DTYPE, TZ_NOT_UTC, NULL_TIMESTAMP,
  NULL_VALUES, DUPLICATE_TIMESTAMPS, NOT_MONOTONIC, OFF_GRID,
  CLOSE_TIME_MISMATCH, OHLC_INCONSISTENT, NON_POSITIVE_PRICE,
  NEGATIVE_VALUE, FUTURE_BAR, GAP_FLAG_MISMATCH,
  TIMEFRAME_MISMATCH                                    -> error
  GAP                                                   -> warning
  ZERO_VOLUME                                           -> info
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
import pandas as pd

from btc_trading_system.core.types import require_utc
from btc_trading_system.data.schema import (FLOAT_COLUMNS, PRICE_COLUMNS,
                                            REQUIRED_COLUMNS, timeframe_ns,
                                            to_epoch_ns)
from btc_trading_system.data_validation.errors import DataValidationError

GAP_COLUMNS = ["gap_start", "gap_end", "missing_bars", "resumes_at"]
MIN_ADJACENT_SHARE = 0.5   # < 50 % intilliggande barer => nästan säkert fel timeframe


@dataclass(frozen=True)
class Issue:
    code: str
    severity: str            # "error" | "warning" | "info"
    message: str
    count: int = 0
    examples: tuple = ()


@dataclass
class ValidationReport:
    issues: list[Issue] = field(default_factory=list)
    rows: int = 0
    gaps: pd.DataFrame = field(default_factory=lambda: empty_gaps())

    @property
    def errors(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def ok(self) -> bool:
        return not self.errors

    def codes(self) -> set[str]:
        return {i.code for i in self.issues}

    def has(self, code: str) -> bool:
        return code in self.codes()

    def add(self, code: str, severity: str, message: str, count: int = 0, examples=()) -> None:
        self.issues.append(Issue(code, severity, message, count, tuple(examples)))

    def raise_if_errors(self, ignore: tuple[str, ...] = (), context: str = "") -> None:
        errs = [i for i in self.errors if i.code not in ignore]
        if errs:
            sub = ValidationReport(issues=errs, rows=self.rows, gaps=self.gaps)
            raise DataValidationError(sub, context)


def empty_gaps() -> pd.DataFrame:
    return pd.DataFrame({
        "gap_start": pd.Series([], dtype="datetime64[ns, UTC]"),
        "gap_end": pd.Series([], dtype="datetime64[ns, UTC]"),
        "missing_bars": pd.Series([], dtype="int64"),
        "resumes_at": pd.Series([], dtype="datetime64[ns, UTC]"),
    })


def _ts(ns: np.ndarray) -> pd.Series:
    return pd.Series(pd.to_datetime(np.asarray(ns, dtype="int64"), unit="ns", utc=True)
                     ).dt.as_unit("ns")


def compute_gap_flags(open_ns: np.ndarray, timeframe: str) -> tuple[np.ndarray, pd.DataFrame]:
    """Kräver sorterade, unika, rutnätsjusterade tider (int64 ns).

    gap_before[i] = True om baren FÖRE bar i saknas (kausal: beror bara på i-1, i).
    Första baren: False (okänt, inget att jämföra med).
    Inga barer fabriceras; luckor rapporteras explicit.
    """
    tf = timeframe_ns(timeframe)
    n = len(open_ns)
    flags = np.zeros(n, dtype=bool)
    if n < 2:
        return flags, empty_gaps()
    d = np.diff(open_ns)
    idx = np.nonzero(d > tf)[0] + 1            # index för första baren efter luckan
    flags[idx] = True
    if len(idx) == 0:
        return flags, empty_gaps()
    prev = open_ns[idx - 1]
    nxt = open_ns[idx]
    gaps = pd.DataFrame({
        "gap_start": _ts(prev + tf),
        "gap_end": _ts(nxt - tf),
        "missing_bars": (nxt - prev) // tf - 1,
        "resumes_at": _ts(nxt),
    })
    gaps["missing_bars"] = gaps["missing_bars"].astype("int64")
    return flags, gaps


def _examples(mask: np.ndarray, ts_ns: np.ndarray, k: int = 3) -> tuple:
    pos = np.nonzero(mask)[0][:k]
    return tuple(f"row {int(p)} @ {pd.Timestamp(int(ts_ns[p]), tz='UTC').isoformat()}" for p in pos)


def check_ohlcv(df: pd.DataFrame, timeframe: str, now: datetime | None = None) -> ValidationReport:
    tf = timeframe_ns(timeframe)          # ValueError vid okänd timeframe
    rep = ValidationReport(rows=len(df))

    if len(df) == 0:
        rep.add("EMPTY", "error", "dataset är tomt")
        return rep
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        rep.add("SCHEMA_MISSING_COLUMN", "error", f"saknade kolumner: {missing}", len(missing))
        return rep

    ot = df["open_time"]
    if not pd.api.types.is_datetime64_any_dtype(ot):
        rep.add("SCHEMA_DTYPE", "error", f"open_time har dtype {ot.dtype}, förväntade datetime64[UTC]")
        return rep
    if ot.dt.tz is None:
        rep.add("TZ_NOT_UTC", "error", "open_time är tz-naiv")
        return rep
    if str(ot.dt.tz) != "UTC":
        rep.add("TZ_NOT_UTC", "error", f"open_time har tidszon {ot.dt.tz}, måste vara UTC")
        return rep
    if ot.isna().any():
        rep.add("NULL_TIMESTAMP", "error", "NaT i open_time", int(ot.isna().sum()))
        return rep

    bad_dtype = [c for c in FLOAT_COLUMNS if df[c].dtype != np.float64]
    if not pd.api.types.is_integer_dtype(df["trade_count"]):
        bad_dtype.append("trade_count")
    if "gap_before" in df.columns and df["gap_before"].dtype != bool:
        bad_dtype.append("gap_before")
    if bad_dtype:
        rep.add("SCHEMA_DTYPE", "error", f"fel dtype i kolumner: {bad_dtype}", len(bad_dtype))
        return rep

    ns = to_epoch_ns(ot)

    # --- värden ---
    nonfinite = np.zeros(len(df), dtype=bool)
    for c in FLOAT_COLUMNS:
        nonfinite |= ~np.isfinite(df[c].to_numpy())
    if nonfinite.any():
        rep.add("NULL_VALUES", "error", "NaN/inf i numeriska kolumner",
                int(nonfinite.sum()), _examples(nonfinite, ns))
    fin = ~nonfinite

    # --- tidsstruktur ---
    dup = int(ot.duplicated().sum())
    if dup:
        rep.add("DUPLICATE_TIMESTAMPS", "error", "duplicerade open_time", dup,
                _examples(ot.duplicated(keep=False).to_numpy(), ns))
    non_mono = bool(np.any(np.diff(ns) < 0))
    if non_mono:
        bad = np.concatenate([[False], np.diff(ns) < 0])
        rep.add("NOT_MONOTONIC", "error", "open_time är inte stigande", int(bad.sum()), _examples(bad, ns))
    off = (ns % tf) != 0
    if off.any():
        rep.add("OFF_GRID", "error", f"open_time ligger inte på {timeframe}-rutnätet",
                int(off.sum()), _examples(off, ns))

    if "close_time" in df.columns:
        ct = df["close_time"]
        if not pd.api.types.is_datetime64_any_dtype(ct) or ct.dt.tz is None or str(ct.dt.tz) != "UTC":
            rep.add("SCHEMA_DTYPE", "error", "close_time måste vara datetime64[UTC]")
        else:
            # Binance: close_time = open_time + tf - 1 ms (ms-filer) eller - 1 µs (µs-filer)
            delta = ns + tf - to_epoch_ns(ct)
            wrong = ~np.isin(delta, (1_000_000, 1_000))
            if wrong.any():
                rep.add("CLOSE_TIME_MISMATCH", "error",
                        f"close_time stämmer inte med timeframe {timeframe} (fel fil/timeframe?)",
                        int(wrong.sum()), _examples(wrong, ns))

    # --- OHLC ---
    o, h, l, c = (df[x].to_numpy() for x in PRICE_COLUMNS)
    with np.errstate(invalid="ignore"):
        nonpos = fin & ((o <= 0) | (h <= 0) | (l <= 0) | (c <= 0))
        incons = fin & ((l > np.minimum(o, c)) | (h < np.maximum(o, c)) | (l > h))
        neg = fin & ((df["volume"].to_numpy() < 0) | (df["quote_volume"].to_numpy() < 0)
                     | (df["taker_buy_base_volume"].to_numpy() < 0)
                     | (df["taker_buy_quote_volume"].to_numpy() < 0)
                     | (df["trade_count"].to_numpy() < 0))
    if nonpos.any():
        rep.add("NON_POSITIVE_PRICE", "error", "pris <= 0", int(nonpos.sum()), _examples(nonpos, ns))
    if incons.any():
        rep.add("OHLC_INCONSISTENT", "error", "low/high stämmer inte med open/close",
                int(incons.sum()), _examples(incons, ns))
    if neg.any():
        rep.add("NEGATIVE_VALUE", "error", "negativ volym/antal", int(neg.sum()), _examples(neg, ns))

    # --- framtida/ofullständiga barer ---
    if now is not None:
        now_ns = pd.Timestamp(require_utc(now, "now")).as_unit("ns").value
        fut = (ns + tf) > now_ns
        if fut.any():
            rep.add("FUTURE_BAR", "error", "bar ej avslutad vid 'now' (ofullständig/framtida)",
                    int(fut.sum()), _examples(fut, ns))

    zero = fin & (df["volume"].to_numpy() == 0)
    if zero.any():
        rep.add("ZERO_VOLUME", "info", "barer med volym 0", int(zero.sum()))

    # --- gaps (endast om tidsaxeln är sorterad, unik och på rutnätet) ---
    if not dup and not non_mono and not off.any():
        if len(ns) >= 3:
            share = float(np.mean(np.diff(ns) == tf))
            if share < MIN_ADJACENT_SHARE:
                rep.add("TIMEFRAME_MISMATCH", "error",
                        f"endast {share:.0%} av på varandra följande barer ligger {timeframe} isär "
                        f"(krav >= {MIN_ADJACENT_SHARE:.0%}); fel timeframe-fil?")
        flags, gaps = compute_gap_flags(ns, timeframe)
        rep.gaps = gaps
        if len(gaps):
            rep.add("GAP", "warning",
                    f"{len(gaps)} lucka(or), totalt {int(gaps['missing_bars'].sum())} saknade barer",
                    int(gaps["missing_bars"].sum()))
        if "gap_before" in df.columns and not np.array_equal(df["gap_before"].to_numpy(), flags):
            rep.add("GAP_FLAG_MISMATCH", "error", "gap_before stämmer inte med faktiska luckor",
                    int((df["gap_before"].to_numpy() != flags).sum()))
    return rep
