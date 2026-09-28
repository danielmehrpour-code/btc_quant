"""Kanoniskt schema för OHLCV-dataset (Binance BTC/USDT spot + perp)."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

import pandas as pd

from btc_trading_system.core.types import TIMEFRAMES

SCHEMA_VERSION = 1
EXCHANGE = "binance"


class Instrument(str, Enum):
    SPOT = "spot"
    PERP = "perp"          # Binance USD-M perpetual (BTCUSDT)


PRICE_COLUMNS = ("open", "high", "low", "close")
FLOAT_COLUMNS = ("open", "high", "low", "close", "volume", "quote_volume",
                 "taker_buy_base_volume", "taker_buy_quote_volume")
INT_COLUMNS = ("trade_count",)
REQUIRED_COLUMNS = ("open_time", "open", "high", "low", "close", "volume",
                    "quote_volume", "trade_count",
                    "taker_buy_base_volume", "taker_buy_quote_volume")
CANONICAL_COLUMNS = REQUIRED_COLUMNS + ("gap_before",)

_SYMBOL_RE = re.compile(r"^[A-Z0-9]{5,20}$")


def normalize_symbol(symbol: str) -> str:
    """'BTC/USDT', 'btc-usdt', 'BTCUSDT' -> 'BTCUSDT'."""
    s = symbol.strip().upper().replace("/", "").replace("-", "")
    if not _SYMBOL_RE.match(s):
        raise ValueError(f"ogiltig symbol: {symbol!r}")
    return s


def timeframe_ms(timeframe: str) -> int:
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"okänd timeframe {timeframe!r}; giltiga: {list(TIMEFRAMES)}")
    return TIMEFRAMES[timeframe] // timedelta(milliseconds=1)


def timeframe_ns(timeframe: str) -> int:
    return timeframe_ms(timeframe) * 1_000_000


def to_epoch_ns(ts: pd.Series) -> "pd.api.extensions.ExtensionArray":
    """tz-aware UTC-serie -> int64 nanosekunder (numpy)."""
    return (ts.dt.tz_convert("UTC").dt.tz_localize(None).to_numpy()
            .astype("datetime64[ns]").astype("int64"))


def to_epoch_ms(ts: pd.Series):
    return (ts.dt.tz_convert("UTC").dt.tz_localize(None).to_numpy()
            .astype("datetime64[ms]").astype("int64"))


@dataclass(frozen=True)
class DatasetKey:
    instrument: Instrument
    symbol: str
    timeframe: str
    exchange: str = EXCHANGE

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", Instrument(self.instrument))
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        timeframe_ms(self.timeframe)  # validerar

    @property
    def relpath(self) -> str:
        return f"{self.exchange}/{self.instrument.value}/{self.symbol}/{self.timeframe}"
