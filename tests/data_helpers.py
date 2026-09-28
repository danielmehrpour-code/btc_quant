"""Testhjälp: syntetisk Binance-data + deterministisk testbackend (ersätter Parquet vid test)."""
from __future__ import annotations

import io
import json
import os
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data.ingest import ingest_dataset
from btc_trading_system.data.schema import CANONICAL_COLUMNS, timeframe_ms, to_epoch_ns
from btc_trading_system.data.storage import DatasetStore, canonicalize
from btc_trading_system.data_validation.ohlcv import compute_gap_flags

NOW = datetime(2030, 1, 1, tzinfo=timezone.utc)
START = datetime(2024, 1, 1, tzinfo=timezone.utc)
BINANCE_HEADER = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume",
                  "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"]


def make_klines(tf: str, n: int = 100, seed: int = 0, start: datetime = START) -> pd.DataFrame:
    """Typad frame: kanoniska kolumner + close_time (ingen gap_before). Alla tal är
    heltal/100 eller /1000 så CSV-round-trip blir bit-exakt."""
    rng = np.random.default_rng(seed)
    step = TIMEFRAMES[tf]
    ot = [start + i * step for i in range(n)]
    c0 = 4_000_000
    closes = c0 + np.cumsum(rng.integers(-500, 501, n))
    opens = np.concatenate([[c0], closes[:-1]])
    highs = np.maximum(opens, closes) + rng.integers(0, 300, n)
    lows = np.minimum(opens, closes) - rng.integers(0, 300, n)
    vol_m = rng.integers(1, 5_000_000, n)
    q_c = (closes * vol_m) // 1000
    df = pd.DataFrame({
        "open_time": pd.Series(pd.to_datetime(ot, utc=True)).dt.as_unit("ns"),
        "open": opens / 100, "high": highs / 100, "low": lows / 100, "close": closes / 100,
        "volume": vol_m / 1000, "quote_volume": q_c / 100,
        "trade_count": rng.integers(1, 10_000, n).astype("int64"),
        "taker_buy_base_volume": (vol_m // 2) / 1000, "taker_buy_quote_volume": (q_c // 2) / 100,
    })
    df["close_time"] = (df["open_time"] + step - pd.Timedelta(milliseconds=1)).dt.as_unit("ns")
    return df


def canon_frame(tf: str, n: int = 100, seed: int = 0) -> pd.DataFrame:
    """Ingest-färdig kanonisk frame med korrekt gap_before."""
    df = make_klines(tf, n, seed).drop(columns=["close_time"])
    df["gap_before"], _ = compute_gap_flags(to_epoch_ns(df["open_time"]), tf)
    return canonicalize(df)


def to_rows(df: pd.DataFrame, unit: str = "ms") -> list[list[str]]:
    """unit='us' följer Binance µs-format: close_time = open_time + tf - 1 µs."""
    rows = []
    for r in df.itertuples(index=False):
        if unit == "us":
            ot = int(r.open_time.value // 1_000)
            ct = int((r.close_time.value + 1_000_000) // 1_000) - 1      # (open + tf) i µs - 1
        else:
            ot = int(r.open_time.value // 1_000_000)
            ct = int(r.close_time.value // 1_000_000)
        rows.append([str(ot), f"{r.open:.2f}", f"{r.high:.2f}", f"{r.low:.2f}", f"{r.close:.2f}",
                     f"{r.volume:.3f}", str(ct), f"{r.quote_volume:.2f}", str(int(r.trade_count)),
                     f"{r.taker_buy_base_volume:.3f}", f"{r.taker_buy_quote_volume:.2f}", "0"])
    return rows


def rows_to_text(rows: list[list[str]], header: bool = False) -> str:
    lines = ([",".join(BINANCE_HEADER)] if header else []) + [",".join(r) for r in rows]
    return "\n".join(lines) + "\n"


def write_csv(path: Path, df: pd.DataFrame, header: bool = False, unit: str = "ms") -> Path:
    path.write_text(rows_to_text(to_rows(df, unit), header))
    return path


def write_rows(path: Path, rows: list[list[str]], header: bool = False) -> Path:
    path.write_text(rows_to_text(rows, header))
    return path


def write_zip(path: Path, df: pd.DataFrame, inner: str = "data.csv") -> Path:
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(inner, rows_to_text(to_rows(df)))
    return path


class RawBackend:
    """Deterministisk testbackend (JSON-header + råa kolumnbytes). Endast för tester."""
    extension = ".raw"

    def write(self, path: Path, df: pd.DataFrame) -> None:
        parts, meta = [], []
        for c in CANONICAL_COLUMNS:
            if c == "open_time":
                arr = (df[c].dt.tz_convert("UTC").dt.tz_localize(None).to_numpy()
                       .astype("datetime64[ns]").astype("int64"))
            else:
                arr = df[c].to_numpy()
            b = np.ascontiguousarray(arr).tobytes()
            meta.append([c, str(arr.dtype), len(b)])
            parts.append(b)
        head = json.dumps({"n": len(df), "cols": meta}, sort_keys=True).encode() + b"\n"
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(head + b"".join(parts))
        os.replace(tmp, path)

    def read(self, path: Path) -> pd.DataFrame:
        blob = path.read_bytes()
        head, _, body = blob.partition(b"\n")
        meta = json.loads(head)
        out, off = {}, 0
        for name, dt, nb in meta["cols"]:
            arr = np.frombuffer(body[off:off + nb], dtype=np.dtype(dt)).copy()
            off += nb
            out[name] = (pd.Series(pd.to_datetime(arr, unit="ns", utc=True)).dt.as_unit("ns")
                         if name == "open_time" else arr)
        return pd.DataFrame(out)


def build_store(tmp: Path, instrument: str, tf: str, n: int = 200, seed: int = 0,
                drop: list[int] | None = None, head: int | None = None) -> DatasetStore:
    tmp.mkdir(parents=True, exist_ok=True)
    df = make_klines(tf, n, seed)
    if head is not None:
        df = df.iloc[:head].reset_index(drop=True)      # äkta prefix av samma data
    if drop:
        df = df.drop(index=drop).reset_index(drop=True)
    f = write_csv(tmp / f"{instrument}_{tf}_{seed}.csv", df)
    store = DatasetStore(tmp / f"store_{instrument}_{tf}_{seed}", RawBackend())
    ingest_dataset([f], instrument, tf, store, now=NOW)
    return store
