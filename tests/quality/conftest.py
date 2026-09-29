"""Fixtures för Phase 3-testerna (syntetiska barer + minnesnära testlager)."""
from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data.schema import CANONICAL_COLUMNS, DatasetKey
from btc_trading_system.data.storage import DatasetStore
from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.data_validation.ohlcv import compute_gap_flags

START = datetime(2024, 1, 1, tzinfo=UTC)
FIELDS = (
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


class TestBackend:
    """Deterministisk testbackend (JSON-huvud + råa kolumnbytes). Endast för tester."""

    __test__ = False
    extension = ".bin"

    def write(self, path: Path, df: pd.DataFrame) -> None:
        parts, meta = [], []
        for c in CANONICAL_COLUMNS:
            if c == "open_time":
                arr = qc.index_ns(pd.DatetimeIndex(df[c]))
            else:
                arr = df[c].to_numpy()
            blob = np.ascontiguousarray(arr).tobytes()
            meta.append([c, str(arr.dtype), len(blob)])
            parts.append(blob)
        head = json.dumps({"cols": meta}, sort_keys=True).encode() + b"\n"
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(head + b"".join(parts))
        os.replace(tmp, path)

    def read(self, path: Path) -> pd.DataFrame:
        head, _, body = path.read_bytes().partition(b"\n")
        out, off = {}, 0
        for name, dtype, nbytes in json.loads(head)["cols"]:
            arr = np.frombuffer(body[off : off + nbytes], dtype=np.dtype(dtype)).copy()
            off += nbytes
            if name == "open_time":
                out[name] = pd.Series(pd.to_datetime(arr, unit="ns", utc=True)).dt.as_unit("ns")
            else:
                out[name] = arr
        return pd.DataFrame(out)


def make_bars(
    tf: str,
    n: int,
    seed: int = 0,
    start: datetime = START,
    sigma: float = 0.002,
    price: float = 40000.0,
) -> pd.DataFrame:
    """Provider-format: index open_time (UTC), alla invarianter uppfyllda, inga luckor."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n, freq=TIMEFRAMES[tf], tz="UTC").as_unit("ns")
    idx.name = "open_time"
    close = price * np.exp(np.cumsum(rng.normal(0.0, sigma, n)))
    open_ = np.concatenate([[price], close[:-1]])
    high = np.maximum(open_, close) * (1.0 + np.abs(rng.normal(0.0, sigma / 2, n)))
    low = np.minimum(open_, close) * (1.0 - np.abs(rng.normal(0.0, sigma / 2, n)))
    vol = rng.uniform(1.0, 100.0, n)
    vwap = (high + low) / 2.0
    taker = vol * rng.uniform(0.3, 0.7, n)
    return pd.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": vol,
            "quote_volume": vwap * vol,
            "trade_count": rng.integers(1, 1000, n).astype(np.int64),
            "taker_buy_base_volume": taker,
            "taker_buy_quote_volume": taker * vwap,
            "gap_before": np.zeros(n, dtype=bool),
        },
        index=idx,
    )


def recompute_gaps(bars: pd.DataFrame, tf: str) -> pd.DataFrame:
    out = bars.copy()
    flags, _ = compute_gap_flags(qc.index_ns(out.index), tf)
    out["gap_before"] = flags
    return out


def aggregate(lower: pd.DataFrame, lower_tf: str, higher_tf: str) -> pd.DataFrame:
    """Oberoende referensaggregering (endast kompletta fönster antas i indata)."""
    step = TIMEFRAMES[higher_tf]
    key = lower.index.floor(step)
    grouped = lower.groupby(key, sort=True)
    out = pd.DataFrame(
        {
            "open": grouped["open"].first(),
            "high": grouped["high"].max(),
            "low": grouped["low"].min(),
            "close": grouped["close"].last(),
            "volume": grouped["volume"].sum(),
            "quote_volume": grouped["quote_volume"].sum(),
            "trade_count": grouped["trade_count"].sum().astype(np.int64),
            "taker_buy_base_volume": grouped["taker_buy_base_volume"].sum(),
            "taker_buy_quote_volume": grouped["taker_buy_quote_volume"].sum(),
        }
    )
    out.index.name = "open_time"
    out["gap_before"] = False
    return recompute_gaps(out, higher_tf)


def put_dataset(root: Path, bars: pd.DataFrame, instrument: str, tf: str) -> tuple:
    """Skriver barer till ett testlager via Phase 2 DatasetStore (oförändrad)."""
    store = DatasetStore(root, TestBackend())
    key = DatasetKey(instrument, "BTCUSDT", tf)
    frame = recompute_gaps(bars, tf).reset_index()[list(CANONICAL_COLUMNS)]
    _, gaps = compute_gap_flags(qc.index_ns(pd.DatetimeIndex(frame["open_time"])), tf)
    extra = {
        "duplicates_dropped": 0,
        "gaps": [
            {
                "gap_start": g.gap_start.isoformat(),
                "gap_end": g.gap_end.isoformat(),
                "missing_bars": int(g.missing_bars),
            }
            for g in gaps.itertuples()
        ],
        "zero_volume_bars": int((frame["volume"] == 0).sum()),
    }
    manifest = store.write(key, frame, extra)
    return store, key, manifest


@pytest.fixture
def qh() -> SimpleNamespace:
    return SimpleNamespace(
        START=START,
        FIELDS=FIELDS,
        make_bars=make_bars,
        recompute_gaps=recompute_gaps,
        aggregate=aggregate,
        put_dataset=put_dataset,
        TestBackend=TestBackend,
        timedelta=timedelta,
    )


@pytest.fixture
def cfg() -> qc.QualityConfig:
    return qc.load_quality_config()
