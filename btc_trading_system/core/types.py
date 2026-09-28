"""Domäntyper. Tidskontraktet (anti look-ahead) tvingas här.

TIDSKONTRAKT
------------
* Bar.open_time  = när baren BÖRJAR (UTC, tz-aware).
* Bar.available_at = open_time + timeframe = första ögonblicket då baren
  (high/low/close/volume) är känd. Ingen beslutslogik får använda en bar
  före available_at.
* Signal.generated_at måste vara en tidpunkt då informationen fanns.
* Order kan tidigast skapas vid/efter signalen, Fill strikt efter Order.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

TIMEFRAMES: dict[str, timedelta] = {
    "1m": timedelta(minutes=1), "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15), "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1), "4h": timedelta(hours=4), "1d": timedelta(days=1),
}


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class CausalityError(ValueError):
    """Kastas när något bryter mot tidskontraktet (möjlig look-ahead)."""


def require_utc(ts: datetime, name: str = "timestamp") -> datetime:
    if ts.tzinfo is None or ts.utcoffset() != timedelta(0):
        raise ValueError(f"{name} måste vara tz-aware UTC, fick {ts!r}")
    return ts.astimezone(timezone.utc)


@dataclass(frozen=True)
class Bar:
    symbol: str
    timeframe: str
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __post_init__(self) -> None:
        require_utc(self.open_time, "open_time")
        if self.timeframe not in TIMEFRAMES:
            raise ValueError(f"okänd timeframe {self.timeframe}")
        if not (self.low <= min(self.open, self.close) and self.high >= max(self.open, self.close)):
            raise ValueError("OHLC inkonsistent")
        if self.volume < 0:
            raise ValueError("negativ volym")

    @property
    def available_at(self) -> datetime:
        return self.open_time + TIMEFRAMES[self.timeframe]


@dataclass(frozen=True)
class Signal:
    signal_id: str
    strategy_id: str
    symbol: str
    side: Side
    strength: float
    generated_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.generated_at, "generated_at")


@dataclass(frozen=True)
class Order:
    order_id: str
    signal_id: str
    symbol: str
    side: Side
    quantity: float
    order_type: OrderType
    created_at: datetime
    limit_price: float | None = None

    def __post_init__(self) -> None:
        require_utc(self.created_at, "created_at")
        if self.quantity <= 0:
            raise ValueError("quantity måste vara > 0")
        if self.order_type is OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit-order kräver limit_price")


@dataclass(frozen=True)
class Fill:
    order_id: str
    price: float
    quantity: float
    fee: float
    slippage: float
    filled_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.filled_at, "filled_at")
        if self.quantity <= 0 or self.price <= 0:
            raise ValueError("fill kräver positivt pris och kvantitet")
        if self.fee < 0:
            raise ValueError("fee < 0")


def validate_causality(signal: Signal, order: Order, fill: Fill) -> None:
    """SIGNAL -> ORDER -> FILL måste vara kausalt ordnade.

    signal.generated_at <= order.created_at < fill.filled_at
    (fill strikt efter order: 'signalpris = fillpris' är förbjudet.)
    """
    if order.signal_id != signal.signal_id:
        raise CausalityError("order tillhör inte signalen")
    if fill.order_id != order.order_id:
        raise CausalityError("fill tillhör inte ordern")
    if order.created_at < signal.generated_at:
        raise CausalityError("order skapad före signal")
    if fill.filled_at <= order.created_at:
        raise CausalityError("fill måste ske strikt efter order")
