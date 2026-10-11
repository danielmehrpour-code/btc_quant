"""Funding-räntor för Binance USD-M perpetual (etapp B steg 4, SB-S4-a..d). Ingen lager-I/O.

KÄLLA
  data.binance.vision månadsfiler <SYMBOL>-fundingRate-YYYY-MM.zip med exakt en medlem <stem>.csv,
  UTF-8, radslut LF, huvud exakt `calc_time,funding_interval_hours,last_funding_rate`.
  Kolumnernas betydelse är slutsatser av namnen (ingen officiell källa definierar dem), stärkta av
  en jämförelse med GET /fapi/v1/fundingRate (SB-S4-c B): calc_time = fundingTime (exakt, ms),
  last_funding_rate = fundingRate (exakt). Arkivfiler kan ersättas i efterhand (README för
  binance-public-data); en ersatt fil ger ett nytt innehåll och en ny version, aldrig en ändrad.

KANONISKT SCHEMA (SB-S4-b), kolumner i denna ordning:
  calc_time               datetime64[ns, UTC], hela millisekunder, aldrig avrundad till 8-timmarsnätet
  funding_interval_hours  int64 > 0
  funding_rate_e12        int64: räntan som exakt heltal i enheten 1e-12; tolkas med Decimal (båda
                          notationerna, t.ex. 0.00010000 och 1E-4); en ränta som inte är en exakt
                          multipel av 1e-12 avvisas (ingen avrundning)
  Raderna är strikt stigande och unika på calc_time.

INGEST-REGLER (som Phase 2): filnamnet måste matcha datasetet, varje rad måste ligga i filens månad,
identiska dubbletter mellan filer tas bort och räknas, motstridiga dubbletter avvisar allt
(DUPLICATE_CONFLICT). Indatafilernas ordning påverkar ingenting.

IDENTITET: data_version = sha256 över en kanonisk bytekodning (domäntagg btc-quant/funding/v1,
radantal, kolumnnamn, little-endian int64; calc_time i ms), oberoende av pyarrow och av hur talen
skrevs i källan (P2-01).

KAUSALITET (SB-S4-c A): available_at = calc_time + AVAILABLE_LAG (15 s). Marginalen kommer från
Binances FAQ ("There is a 15-second deviation in the actual funding fee transaction time.",
https://www.binance.com/en/support/faq/introduction-to-binance-futures-funding-rates-360033525031),
inte från datan (P3-02). Ett beslut vid t får bara använda rader med available_at <= t.
"""
from __future__ import annotations

import hashlib
import io
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from btc_trading_system.core.types import require_utc
from btc_trading_system.data.schema import (
    EXCHANGE,
    Instrument,
    normalize_symbol,
    to_epoch_ms,
)
from btc_trading_system.data_validation.errors import (
    DataIntegrityError,
    MalformedDataError,
)

FUNDING_SCHEMA_VERSION = 1
FUNDING_KIND = "funding"
FUNDING_COLUMNS = ("calc_time", "funding_interval_hours", "funding_rate_e12")
RATE_SCALE_EXPONENT = 12                                   # enheten 1e-12
AVAILABLE_LAG = timedelta(seconds=15)                      # SB-S4-c A (Binances FAQ, inte datan)
SOURCE_HEADER = "calc_time,funding_interval_hours,last_funding_rate"
CONTENT_DOMAIN = b"btc-quant/funding/v1\x00"
_FILE_RE = re.compile(r"^(?P<symbol>[A-Z0-9]{5,20})-fundingRate-(?P<year>\d{4})-(?P<month>\d{2})\.zip$")
_RATE_RE = re.compile(r"^-?\d{1,20}(\.\d{1,20})?([eE][-+]?\d{1,2})?$")
_MS_RE = re.compile(r"^\d{13}$")
_INTERVAL_RE = re.compile(r"^[1-9]\d{0,2}$")
_INT64_MAX = 2**63 - 1


class FundingFormatError(MalformedDataError):
    """Funding-källan eller -ramen bryter mot formatet; ingenting skrivs."""


@dataclass(frozen=True)
class FundingKey:
    """Ett funding-dataset: binance/perp/<SYMBOL>/funding. Funding finns bara för perpetual."""
    instrument: Instrument
    symbol: str
    exchange: str = EXCHANGE

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", Instrument(self.instrument))
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        if self.instrument is not Instrument.PERP:
            raise ValueError(f"funding finns bara för perp, inte {self.instrument.value!r}")

    @property
    def relpath(self) -> str:
        return f"{self.exchange}/{self.instrument.value}/{self.symbol}/{FUNDING_KIND}"


@dataclass(frozen=True)
class FundingSource:
    name: str
    sha256: str
    rows: int

    def to_json_obj(self) -> dict[str, Any]:
        return {"name": self.name, "sha256": self.sha256, "rows": self.rows}


# ------------------------------------------------------------------ tolkning
def parse_rate_e12(text: str) -> int:
    """Decimaltext (vanlig eller vetenskaplig notation) -> exakt heltal i enheten 1e-12."""
    if not isinstance(text, str) or not _RATE_RE.fullmatch(text):
        raise FundingFormatError(f"ogiltig ränta: {text!r}")
    with localcontext() as ctx:
        ctx.prec = 80
        try:
            scaled = Decimal(text).scaleb(RATE_SCALE_EXPONENT)
        except InvalidOperation as e:                          # pragma: no cover - regexen utesluter
            raise FundingFormatError(f"ogiltig ränta: {text!r}") from e
        if scaled != scaled.to_integral_value():
            raise FundingFormatError(f"räntan {text!r} är inte en exakt multipel av 1e-12")
        value = int(scaled)
    if abs(value) > _INT64_MAX:
        raise FundingFormatError(f"räntan {text!r} ryms inte i int64 (enheten 1e-12)")
    return value


def _month_bounds_ms(year: int, month: int) -> tuple[int, int]:
    lo = datetime(year, month, 1, tzinfo=UTC)
    hi = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=UTC)
    return int(lo.timestamp()) * 1000, int(hi.timestamp()) * 1000


def read_funding_zip(path: str | Path, key: FundingKey) -> tuple[pd.DataFrame, FundingSource]:
    """En månadsfil -> (kanonisk ram i filens ordning, källa). Strikt; allt avvikande avvisas."""
    path = Path(path)
    m = _FILE_RE.fullmatch(path.name)
    if m is None:
        raise FundingFormatError(f"{path.name}: filnamnet är inte <SYMBOL>-fundingRate-YYYY-MM.zip")
    if m["symbol"] != key.symbol:
        raise FundingFormatError(f"{path.name}: symbolen {m['symbol']} != datasetets {key.symbol}")
    year, month = int(m["year"]), int(m["month"])
    if not 1 <= month <= 12:
        raise FundingFormatError(f"{path.name}: ogiltig månad {m['month']}")
    raw = path.read_bytes()
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            names = zf.namelist()
            member = path.name[:-4] + ".csv"
            if names != [member]:
                raise FundingFormatError(f"{path.name}: medlemmar {names} != [{member!r}]")
            data = zf.read(member)
    except zipfile.BadZipFile as e:
        raise FundingFormatError(f"{path.name}: inte en giltig zip: {e}") from e
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as e:
        raise FundingFormatError(f"{path.name}: inte UTF-8") from e
    if "\r" in text:
        raise FundingFormatError(f"{path.name}: radslut måste vara LF")
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    if not lines or lines[0] != SOURCE_HEADER:
        raise FundingFormatError(f"{path.name}: huvudet är inte exakt {SOURCE_HEADER!r}")
    body = lines[1:]
    if not body:
        raise FundingFormatError(f"{path.name}: inga rader")
    lo, hi = _month_bounds_ms(year, month)
    times, intervals, rates = [], [], []
    for i, line in enumerate(body, start=2):
        cells = line.split(",")
        if len(cells) != 3:
            raise FundingFormatError(f"{path.name} rad {i}: {len(cells)} fält, förväntade 3")
        t_txt, iv_txt, rate_txt = cells
        if not _MS_RE.fullmatch(t_txt):
            raise FundingFormatError(f"{path.name} rad {i}: calc_time {t_txt!r} är inte 13 siffror (ms)")
        if not _INTERVAL_RE.fullmatch(iv_txt):
            raise FundingFormatError(f"{path.name} rad {i}: funding_interval_hours {iv_txt!r} är inte ett "
                                     "positivt heltal")
        t = int(t_txt)
        if not lo <= t < hi:
            raise FundingFormatError(f"{path.name} rad {i}: calc_time {t} ligger utanför filens månad")
        times.append(t)
        intervals.append(int(iv_txt))
        try:
            rates.append(parse_rate_e12(rate_txt))
        except FundingFormatError as e:
            raise FundingFormatError(f"{path.name} rad {i}: {e}") from e
    frame = pd.DataFrame({
        "calc_time": pd.Series(pd.to_datetime(np.asarray(times, dtype="int64"), unit="ms", utc=True)).dt.as_unit("ns"),
        "funding_interval_hours": np.asarray(intervals, dtype="int64"),
        "funding_rate_e12": np.asarray(rates, dtype="int64"),
    })
    return frame, FundingSource(path.name, hashlib.sha256(raw).hexdigest(), len(body))


def build_funding_frame(files: list[str | Path] | tuple[str | Path, ...], key: FundingKey
                        ) -> tuple[pd.DataFrame, tuple[FundingSource, ...], int]:
    """Alla filer -> (kanonisk, validerad ram, källor sorterade på (namn, sha256), borttagna dubbletter)."""
    if not files:
        raise FundingFormatError("inga indatafiler")
    names = [os.path.basename(str(f)) for f in files]
    if len(set(names)) != len(names):
        raise FundingFormatError(f"samma filnamn mer än en gång: {sorted(n for n in set(names) if names.count(n) > 1)}")
    frames, sources = [], []
    for f in files:
        frame, src = read_funding_zip(f, key)
        frames.append(frame)
        sources.append(src)
    allrows = pd.concat(frames, ignore_index=True)
    allrows = allrows.sort_values(list(FUNDING_COLUMNS), kind="mergesort").reset_index(drop=True)
    identical = allrows.duplicated(keep="first")
    dropped = int(identical.sum())
    unique = allrows.loc[~identical].reset_index(drop=True)
    conflict = unique["calc_time"].duplicated(keep=False)
    if conflict.any():
        first = unique.loc[conflict, "calc_time"].iloc[0].isoformat()
        raise FundingFormatError(f"DUPLICATE_CONFLICT: olika värden för samma calc_time ({first} m.fl.)")
    canon = canonicalize_funding(unique)
    check_funding_frame(canon)
    return canon, tuple(sorted(sources, key=lambda s: (s.name, s.sha256))), dropped


# ------------------------------------------------------------------ kanonisk ram
def canonicalize_funding(df: pd.DataFrame) -> pd.DataFrame:
    """Fixerar kolumnordning, dtypes och index. Ändrar inte värden."""
    out = df[list(FUNDING_COLUMNS)].copy().reset_index(drop=True)
    out["calc_time"] = out["calc_time"].dt.as_unit("ns")
    out["funding_interval_hours"] = out["funding_interval_hours"].astype("int64")
    out["funding_rate_e12"] = out["funding_rate_e12"].astype("int64")
    return out


def check_funding_frame(df: pd.DataFrame) -> None:
    """Strukturella invarianter för en lagrad funding-version; brott -> FundingFormatError."""
    if tuple(df.columns) != FUNDING_COLUMNS:
        raise FundingFormatError(f"kolumner {tuple(df.columns)} != {FUNDING_COLUMNS}")
    if len(df) < 1:
        raise FundingFormatError("tom funding-ram")
    ct = df["calc_time"]
    if not isinstance(ct.dtype, pd.DatetimeTZDtype) or str(ct.dt.tz) != "UTC" or ct.isna().any():
        raise FundingFormatError("calc_time måste vara tz-aware UTC utan NaT")
    ns = ct.dt.tz_localize(None).to_numpy().astype("datetime64[ns]").astype("int64")
    if (ns % 1_000_000 != 0).any():
        raise FundingFormatError("calc_time måste vara hela millisekunder")
    if not bool((np.diff(ns) > 0).all()):
        raise FundingFormatError("calc_time måste vara strikt stigande och unik")
    for c in ("funding_interval_hours", "funding_rate_e12"):
        if df[c].dtype != np.dtype("int64"):
            raise FundingFormatError(f"{c} måste vara int64")
    if not bool((df["funding_interval_hours"] > 0).all()):
        raise FundingFormatError("funding_interval_hours måste vara > 0")


def funding_content_hash(df: pd.DataFrame) -> str:
    h = hashlib.sha256(CONTENT_DOMAIN)
    h.update(len(df).to_bytes(8, "little"))
    for c in FUNDING_COLUMNS:
        h.update(c.encode() + b"\x00")
        arr = to_epoch_ms(df[c]).astype("<i8") if c == "calc_time" else df[c].to_numpy(dtype="<i8")
        h.update(np.ascontiguousarray(arr).tobytes())
    return h.hexdigest()


def funding_manifest(key: FundingKey, df: pd.DataFrame, sources: tuple[FundingSource, ...],
                     duplicates_dropped: int) -> dict[str, Any]:
    """Objektets manifest (kanoniskt kodat av lagret). Källorna är versionens lineage (SB-S4-a A2)."""
    return {
        "schema_version": FUNDING_SCHEMA_VERSION, "kind": FUNDING_KIND,
        "exchange": key.exchange, "instrument": key.instrument.value, "symbol": key.symbol,
        "rows": len(df),
        "first_calc_time": df["calc_time"].iloc[0].isoformat(),
        "last_calc_time": df["calc_time"].iloc[-1].isoformat(),
        "data_version": funding_content_hash(df),
        "sources": [s.to_json_obj() for s in sources],
        "duplicates_dropped": duplicates_dropped,
    }


# ------------------------------------------------------------------ kausalitet
def available_at(calc_time: pd.Series) -> pd.Series:
    """När en rad blir känd: calc_time + 15 s (SB-S4-c A)."""
    return calc_time + AVAILABLE_LAG


def funding_cutoff(as_of: datetime) -> pd.Timestamp:
    """Senaste calc_time som får exponeras vid as_of: available_at = calc_time + 15 s <= as_of."""
    return pd.Timestamp(require_utc(as_of, "as_of")) - AVAILABLE_LAG


# ------------------------------------------------------------------ Parquet
class FundingParquet:
    """Fast schema utan pandas-metadata; samma skrivparametrar som Phase 2 ParquetBackend."""

    @staticmethod
    def write(path: Path, df: pd.DataFrame) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq
        ts_type = pa.timestamp("ms", tz="UTC")
        schema = pa.schema([pa.field("calc_time", ts_type, nullable=False),
                            pa.field("funding_interval_hours", pa.int64(), nullable=False),
                            pa.field("funding_rate_e12", pa.int64(), nullable=False)])
        arrays = [pa.array(to_epoch_ms(df["calc_time"]), type=pa.int64()).cast(ts_type),
                  pa.array(df["funding_interval_hours"].to_numpy(dtype="int64")),
                  pa.array(df["funding_rate_e12"].to_numpy(dtype="int64"))]
        table = pa.Table.from_arrays(arrays, schema=schema)
        with path.open("xb") as f:                         # aldrig överskrivning
            pq.write_table(table, f, compression="snappy", use_dictionary=False, write_statistics=True,
                           version="2.6", data_page_version="1.0", row_group_size=131072)

    @staticmethod
    def read(path: Path) -> pd.DataFrame:
        import pyarrow as pa
        import pyarrow.parquet as pq
        t = pq.read_table(str(path))
        if tuple(t.column_names) != FUNDING_COLUMNS:
            raise DataIntegrityError(f"{path.name}: kolumner {t.column_names} != {list(FUNDING_COLUMNS)}")
        ms = t["calc_time"].cast(pa.int64()).to_numpy()
        return pd.DataFrame({
            "calc_time": pd.Series(pd.to_datetime(ms, unit="ms", utc=True)).dt.as_unit("ns"),
            "funding_interval_hours": t["funding_interval_hours"].to_numpy(),
            "funding_rate_e12": t["funding_rate_e12"].to_numpy(),
        })
