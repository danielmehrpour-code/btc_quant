"""Läser lokalt nedladdade Binance kline-filer (CSV, ZIP med CSV, Parquet).

STRIKT: tolkar aldrig gissningsvis. Tvetydig eller trasig input -> undantag.

Tidsstämplar accepteras som
  * epoch millisekunder (13 siffror) eller mikrosekunder (16 siffror; Binance
    spot-filer från 2025 anges använda µs). µs måste vara jämna millisekunder.
  * ISO-8601 MED explicit UTC-offset (Z / +00:00). Naiva eller icke-UTC avvisas.
  * (Parquet) tz-aware UTC datetime eller heltal enligt ovan.
Sekunder (10 siffror) och nanosekunder avvisas uttryckligen.
"""
from __future__ import annotations

import hashlib
import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from btc_trading_system.data.schema import FLOAT_COLUMNS, REQUIRED_COLUMNS
from btc_trading_system.data_validation.errors import MalformedDataError, TimezoneError

# Binance data.binance.vision kline-kolumner, i ordning
RAW_COLUMNS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
               "quote_volume", "trade_count", "taker_buy_base_volume",
               "taker_buy_quote_volume", "ignore"]
ALIASES = {
    "count": "trade_count", "number_of_trades": "trade_count", "trades": "trade_count",
    "taker_buy_volume": "taker_buy_base_volume",
    "taker_buy_base_asset_volume": "taker_buy_base_volume",
    "taker_buy_quote_asset_volume": "taker_buy_quote_volume",
    "quote_asset_volume": "quote_volume",
    "open_timestamp": "open_time", "close_timestamp": "close_time",
}
MIN_DATE = pd.Timestamp("2009-01-03", tz="UTC")
_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?(?P<tz>Z|[+-]\d{2}(:?\d{2})?)?$")


@dataclass(frozen=True)
class SourceInfo:
    name: str
    sha256: str
    rows: int


def _rows_msg(mask: np.ndarray, k: int = 3) -> str:
    return "data-rad " + ", ".join(str(int(i)) for i in np.nonzero(mask)[0][:k])


def _parse_time(s: pd.Series, col: str, src: str, allow_sub_ms: bool = False) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        if s.dt.tz is None:
            raise TimezoneError(f"{src}: {col} är tz-naiv")
        if str(s.dt.tz) != "UTC":
            raise TimezoneError(f"{src}: {col} har tidszon {s.dt.tz}, måste vara UTC")
        if s.isna().any():
            raise MalformedDataError(f"{src}: {col} innehåller NaT")
        return s.dt.as_unit("ns")

    txt = s.astype(str).str.strip()
    is_num = txt.str.fullmatch(r"\d+").to_numpy()
    if is_num.all():
        lens = txt.str.len().to_numpy()
        bad = ~np.isin(lens, (13, 16))
        if bad.any():
            raise MalformedDataError(
                f"{src}: {col} har {sorted(set(lens[bad].tolist()))} siffror ({_rows_msg(bad)}); "
                "förväntade 13 (ms) eller 16 (µs). 10 siffror = sekunder och accepteras inte.")
        vals = pd.to_numeric(txt).to_numpy().astype("int64")
        micro = lens == 16
        if not allow_sub_ms and (micro & (vals % 1000 != 0)).any():
            raise MalformedDataError(f"{src}: {col} har µs-värden som inte är hela millisekunder")
        us = np.where(micro, vals, vals * 1000)
        out = pd.Series(pd.to_datetime(us, unit="us", utc=True), index=s.index).dt.as_unit("ns")
    elif not is_num.any():
        vals = txt.to_list()
        bad_fmt = np.array([_ISO.match(v) is None for v in vals])
        if bad_fmt.any():
            raise MalformedDataError(f"{src}: {col} har ogiltig tidsstämpel ({_rows_msg(bad_fmt)}): "
                                     f"{[vals[i] for i in np.nonzero(bad_fmt)[0][:3]]}")
        tzs = [_ISO.match(v).group("tz") for v in vals]
        naive = np.array([t is None for t in tzs])
        if naive.any():
            raise TimezoneError(f"{src}: {col} saknar tidszon ({_rows_msg(naive)}); ange Z eller +00:00")
        nonutc = np.array([t not in ("Z",) and t.replace(":", "").lstrip("+-").strip("0") != "" for t in tzs])
        if nonutc.any():
            raise TimezoneError(f"{src}: {col} har icke-UTC offset ({_rows_msg(nonutc)})")
        try:
            out = pd.Series(pd.to_datetime(vals, utc=True, format="ISO8601"), index=s.index).dt.as_unit("ns")
        except Exception as e:  # noqa: BLE001
            raise MalformedDataError(f"{src}: {col} kunde inte tolkas: {e}") from e
    else:
        raise MalformedDataError(f"{src}: {col} blandar numeriska och textuella tidsstämplar "
                                 f"({_rows_msg(~is_num)})")
    if (out < MIN_DATE).any():
        raise MalformedDataError(f"{src}: {col} har datum före 2009 ({_rows_msg((out < MIN_DATE).to_numpy())})")
    return out


def _to_canonical(raw: pd.DataFrame, src: str) -> pd.DataFrame:
    raw = raw.copy()
    raw.columns = [ALIASES.get(str(c).strip().lower(), str(c).strip().lower()) for c in raw.columns]
    dupcols = sorted({c for c in raw.columns if list(raw.columns).count(c) > 1})
    if dupcols:
        raise MalformedDataError(f"{src}: kolumner förekommer flera gånger (efter alias): {dupcols}")
    missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
    if missing:
        raise MalformedDataError(f"{src}: saknar kolumner {missing}")
    if len(raw) == 0:
        raise MalformedDataError(f"{src}: filen innehåller inga rader")

    out = pd.DataFrame(index=raw.index)
    out["open_time"] = _parse_time(raw["open_time"], "open_time", src)
    for c in FLOAT_COLUMNS:
        num = pd.to_numeric(raw[c], errors="coerce").astype("float64")
        bad = ~np.isfinite(num.to_numpy())
        if bad.any():
            raise MalformedDataError(f"{src}: kolumn {c} har icke-numeriska/icke-finita värden ({_rows_msg(bad)})")
        out[c] = num
    tc = pd.to_numeric(raw["trade_count"], errors="coerce").astype("float64")
    tcv = tc.to_numpy()
    bad = ~np.isfinite(tcv) | (tcv != np.floor(np.where(np.isfinite(tcv), tcv, 0))) | (tcv < 0)
    if bad.any():
        raise MalformedDataError(f"{src}: trade_count måste vara heltal >= 0 ({_rows_msg(bad)})")
    out["trade_count"] = tcv.astype("int64")
    out = out[list(REQUIRED_COLUMNS)]
    if "close_time" in raw.columns:
        out["close_time"] = _parse_time(raw["close_time"], "close_time", src, allow_sub_ms=True)
    return out.reset_index(drop=True)


def _read_parquet(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except ImportError as e:  # pragma: no cover
        raise MalformedDataError("Parquet-läsning kräver pyarrow (pip install pyarrow)") from e


def _read_csv_bytes(data: bytes, src: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(io.BytesIO(data), header=None, dtype=str,
                         keep_default_na=False, na_filter=False)
    except pd.errors.EmptyDataError as e:
        raise MalformedDataError(f"{src}: tom fil") from e
    except pd.errors.ParserError as e:
        raise MalformedDataError(f"{src}: CSV kunde inte tolkas: {e}") from e
    if df.empty:
        raise MalformedDataError(f"{src}: tom fil")
    first = str(df.iat[0, 0]).strip()
    if first[:1].isalpha():                       # headerrad
        df.columns = [str(c).strip() for c in df.iloc[0]]
        df = df.iloc[1:].reset_index(drop=True)
    else:
        if df.shape[1] not in (11, 12):
            raise MalformedDataError(f"{src}: headerlös CSV måste ha 11 eller 12 kolumner, har {df.shape[1]}")
        df.columns = RAW_COLUMNS[: df.shape[1]]
    return df


def read_source_file(path: str | Path) -> tuple[pd.DataFrame, SourceInfo]:
    """Returnerar (frame i kanoniska kolumner [+ close_time], SourceInfo)."""
    p = Path(path)
    if not p.exists():
        raise MalformedDataError(f"filen finns inte: {p}")
    blob = p.read_bytes()
    sha = hashlib.sha256(blob).hexdigest()
    suffix = p.suffix.lower()
    src = p.name
    if suffix == ".zip":
        with zipfile.ZipFile(io.BytesIO(blob)) as z:
            names = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if len(names) != 1:
                raise MalformedDataError(f"{src}: zip måste innehålla exakt en .csv, har {names}")
            raw = _read_csv_bytes(z.read(names[0]), f"{src}:{names[0]}")
    elif suffix == ".csv":
        raw = _read_csv_bytes(blob, src)
    elif suffix == ".parquet":
        raw = _read_parquet(p)
    else:
        raise MalformedDataError(f"{src}: filtyp {suffix!r} stöds inte (.csv, .zip, .parquet)")
    df = _to_canonical(raw, src)
    return df, SourceInfo(name=src, sha256=sha, rows=len(df))
