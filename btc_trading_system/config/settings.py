"""Konfiguration. Live trading är AV som standard och kräver flera checks.

STRIKT: okända nycklar, fel typer och tvetydiga datum är fel, inte tyst ignorerade.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from btc_trading_system.core.types import TIMEFRAMES, require_utc


class LiveTradingNotAllowed(RuntimeError):
    pass


class ConfigError(ValueError):
    pass


def _strict_bool(v: Any, name: str) -> bool:
    if type(v) is not bool:                     # 'false' (str) och 0/1 accepteras INTE
        raise ConfigError(f"{name} måste vara bool (true/false utan citattecken), fick {v!r}")
    return v


@dataclass(frozen=True)
class TradingConfig:
    live: bool = False
    paper: bool = False

    def __post_init__(self) -> None:
        _strict_bool(self.live, "trading.live")
        _strict_bool(self.paper, "trading.paper")


@dataclass(frozen=True)
class RiskConfig:
    risk_per_trade: float = 0.005
    max_leverage: float = 1.0
    max_daily_loss: float = 0.02
    max_drawdown: float = 0.20


@dataclass(frozen=True)
class ExecutionConfig:
    slippage_model: str = "fixed"
    slippage_bps: float = 5.0
    taker_fee_bps: float = 5.0
    maker_fee_bps: float = 2.0
    min_latency_bars: int = 1


@dataclass(frozen=True)
class MarketConfig:
    symbol: str = "BTCUSDT"
    venue: str = "binance"
    instrument: str = "spot"

    def __post_init__(self) -> None:
        from btc_trading_system.data.schema import Instrument, normalize_symbol
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        if self.venue != "binance":
            raise ConfigError(f"market.venue {self.venue!r} stöds inte ännu")
        Instrument(self.instrument)          # ValueError om ej spot/perp


@dataclass(frozen=True)
class DataSplitConfig:
    """Exklusiva gränser: TRAIN = [start, train_until), VALIDATION = [train_until,
    validation_until), FINAL TEST = [validation_until, ...). Tidszon måste anges (UTC)."""
    train_until: datetime | None = None
    validation_until: datetime | None = None

    def __post_init__(self) -> None:
        if (self.train_until is None) != (self.validation_until is None):
            raise ConfigError("data_split: ange både train_until och validation_until")
        if self.train_until is not None:
            require_utc(self.train_until, "data_split.train_until")
            require_utc(self.validation_until, "data_split.validation_until")
            if not self.train_until < self.validation_until:
                raise ConfigError("data_split: train_until måste vara före validation_until")


@dataclass(frozen=True)
class Settings:
    market: MarketConfig = field(default_factory=MarketConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    data_split: DataSplitConfig = field(default_factory=DataSplitConfig)
    primary_timeframe: str = "15m"

    def __post_init__(self) -> None:
        if not 0 < self.risk.risk_per_trade <= 0.02:
            raise ValueError("risk_per_trade måste vara i (0, 2%]")
        if self.risk.max_leverage < 1:
            raise ValueError("max_leverage >= 1")
        if self.execution.min_latency_bars < 1:
            raise ValueError("min_latency_bars >= 1 (ingen same-bar fill)")
        if self.primary_timeframe not in TIMEFRAMES:
            raise ConfigError(f"timeframe.primary {self.primary_timeframe!r} okänd; giltiga: {list(TIMEFRAMES)}")


_SECTIONS = {"market": MarketConfig, "risk": RiskConfig, "execution": ExecutionConfig,
             "trading": TradingConfig}
_TOP_LEVEL = set(_SECTIONS) | {"timeframe", "data_split"}


def _section(raw: dict, name: str, cls: type) -> Any:
    sub = raw.get(name) or {}
    if not isinstance(sub, dict):
        raise ConfigError(f"{name} måste vara en mapping")
    allowed = {f.name for f in fields(cls)}
    unknown = set(sub) - allowed
    if unknown:
        raise ConfigError(f"okända nycklar i {name}: {sorted(unknown)} (tillåtna: {sorted(allowed)})")
    return cls(**sub)


def _parse_dt(v: Any, name: str) -> datetime:
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as e:
            raise ConfigError(f"{name}: ogiltigt datum {v!r}") from e
    else:  # t.ex. YAML-datum utan tid (date) är tvetydigt om inkl/exkl
        raise ConfigError(f"{name}: ange ISO-tid med UTC, t.ex. '2023-01-01T00:00:00Z' (fick {v!r})")
    if dt.tzinfo is None:
        raise ConfigError(f"{name}: tidszon saknas; ange Z")
    return require_utc(dt, name)


def load_settings(path: str | Path) -> Settings:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    unknown = set(raw) - _TOP_LEVEL
    if unknown:
        raise ConfigError(f"okända toppnivånycklar: {sorted(unknown)}")
    tf = raw.get("timeframe") or {}
    if set(tf) - {"primary"}:
        raise ConfigError(f"okända nycklar i timeframe: {sorted(set(tf) - {'primary'})}")
    ds = raw.get("data_split") or {}
    if set(ds) - {"train_until", "validation_until"}:
        raise ConfigError(f"okända nycklar i data_split: {sorted(set(ds) - {'train_until', 'validation_until'})}")
    split = DataSplitConfig(
        train_until=_parse_dt(ds["train_until"], "data_split.train_until") if "train_until" in ds else None,
        validation_until=_parse_dt(ds["validation_until"], "data_split.validation_until") if "validation_until" in ds else None,
    )
    return Settings(
        **{name: _section(raw, name, cls) for name, cls in _SECTIONS.items()},
        data_split=split,
        primary_timeframe=tf.get("primary", "15m"),
    )


def assert_live_allowed(settings: Settings, env: dict[str, str] | None = None,
                        paper_validated: bool = False) -> None:
    """Flera oberoende säkerhetskontroller. Alla måste vara uppfyllda.
    OBS: paper_validated är en flagga från anroparen; den ska ersättas av en
    faktisk kontroll mot sparade paper-vs-backtest-resultat (Phase 18)."""
    env = os.environ if env is None else env
    if settings.trading.live is not True:
        raise LiveTradingNotAllowed("trading.live är inte true")
    if env.get("LIVE_TRADING", "false").lower() != "true":
        raise LiveTradingNotAllowed("env LIVE_TRADING != true")
    if env.get("LIVE_TRADING_CONFIRM") != "I_UNDERSTAND_THE_RISK":
        raise LiveTradingNotAllowed("saknar bekräftelsefras")
    if paper_validated is not True:
        raise LiveTradingNotAllowed("paper trading ej validerad mot backtest")
    if not (env.get("EXCHANGE_API_KEY") and env.get("EXCHANGE_API_SECRET")):
        raise LiveTradingNotAllowed("API-nycklar saknas i environment")
