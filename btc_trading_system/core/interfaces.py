"""Kärninterfaces. Alla moduler är utbytbara bakom dessa ABC:er."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Sequence

import pandas as pd

from btc_trading_system.core.types import Bar, Fill, Order, Signal


class DataProvider(ABC):
    """Levererar marknadsdata.

    LOOK-AHEAD-KONTRAKT: get_bars(as_of=t) får ENDAST returnera barer med
    available_at <= t. En halvfärdig bar får aldrig läcka ut.
    """
    @abstractmethod
    def get_bars(self, symbol: str, timeframe: str, start: datetime,
                 end: datetime, as_of: datetime | None = None) -> pd.DataFrame: ...


class FeatureEngine(ABC):
    """Beräknar features.

    KAUSALITETSKONTRAKT: feature-värdet på rad t får bara bero på rader <= t.
    Verifieras för varje feature med data_validation.causality.assert_causal
    (trunkeringsinvarians) + explicit .shift-logik för multi-timeframe.
    """
    @property
    @abstractmethod
    def lookback(self) -> int: ...

    @abstractmethod
    def compute(self, bars: pd.DataFrame) -> pd.DataFrame: ...


class Strategy(ABC):
    @abstractmethod
    def on_features(self, features: pd.Series, now: datetime) -> Signal | None: ...


class RiskEngine(ABC):
    """Separerad från Strategy. Kan förminska eller vetoa en order."""
    @abstractmethod
    def check(self, signal: Signal, portfolio: "Portfolio") -> float:
        """Returnerar godkänd kvantitet (0 = veto)."""

    @abstractmethod
    def kill_switch_active(self) -> bool: ...


class Portfolio(ABC):
    @abstractmethod
    def apply_fill(self, fill: Fill) -> None: ...
    @abstractmethod
    def equity(self, mark_price: float) -> float: ...
    @abstractmethod
    def position(self, symbol: str) -> float: ...


class OrderManager(ABC):
    @abstractmethod
    def create_order(self, signal: Signal, quantity: float, now: datetime) -> Order: ...


class ExecutionEngine(ABC):
    """Simulerar/utför fills. Får ej anta signalpris == fillpris."""
    @abstractmethod
    def execute(self, order: Order, next_bars: Sequence[Bar]) -> Fill | None: ...


class Broker(ABC):
    @abstractmethod
    def submit(self, order: Order) -> str: ...
    @abstractmethod
    def cancel_all(self) -> None: ...


class Backtester(ABC):
    @abstractmethod
    def run(self, run_id: str) -> dict[str, Any]: ...


class PerformanceAnalyzer(ABC):
    @abstractmethod
    def analyze(self, equity: pd.Series, trades: pd.DataFrame) -> dict[str, float]: ...


class Monitor(ABC):
    @abstractmethod
    def alert(self, level: str, event: str, detail: str) -> None: ...
