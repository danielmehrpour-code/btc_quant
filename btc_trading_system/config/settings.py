"""Konfiguration. Live trading är AV som standard och kräver flera checks.

STRIKT: okända nycklar, fel typer och tvetydiga datum är fel, inte tyst ignorerade.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta
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


SPLIT_SEGMENTS = ("train", "validation", "final_test")
MIN_SPLIT_GAP = timedelta(days=7)            # minsta lucka (embargo) mellan två delar (D-B-h punkt 2)


@dataclass(frozen=True)
class SplitSegment:
    """En del av datauppdelningen: exklusiva UTC-gränser [start, end) på open_time."""
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        require_utc(self.start, "data_split.start")
        require_utc(self.end, "data_split.end")
        if not self.start < self.end:
            raise ConfigError(f"data_split: start {self.start.isoformat()} måste vara före end {self.end.isoformat()}")

    def contains(self, open_time: datetime) -> bool:
        return self.start <= open_time < self.end


@dataclass(frozen=True)
class DataSplitConfig:
    """TRAIN, VALIDATION och FINAL TEST som SplitSegment, i den ordningen, utan överlappning och med
    minst MIN_SPLIT_GAP mellan en del och nästa (nästa start - föregående end). En bar hör till den del
    där dess open_time ligger; i en lucka eller utanför hör den till ingen. Alla tre eller ingen (inga
    datum = platshållare, P1-04/P1-05)."""
    train: SplitSegment | None = None
    validation: SplitSegment | None = None
    final_test: SplitSegment | None = None

    def __post_init__(self) -> None:
        segs = [self.train, self.validation, self.final_test]
        if any(s is None for s in segs) and any(s is not None for s in segs):
            raise ConfigError("data_split: ange alla tre delarna (train, validation, final_test) eller ingen")
        for (a_name, a), (b_name, b) in zip(zip(SPLIT_SEGMENTS, segs), zip(SPLIT_SEGMENTS[1:], segs[1:])):
            if a is None or b is None:
                continue
            if b.start < a.end:
                raise ConfigError(f"data_split: {b_name} börjar före slutet av {a_name} (ordning/överlappning)")
            if b.start - a.end < MIN_SPLIT_GAP:
                raise ConfigError(f"data_split: luckan mellan {a_name} och {b_name} är kortare än {MIN_SPLIT_GAP.days} dagar")

    def segment_of(self, open_time: datetime) -> str | None:
        """Delens namn för en bar med denna open_time; None i en lucka, utanför eller utan datum."""
        require_utc(open_time, "open_time")
        for name in SPLIT_SEGMENTS:
            seg = getattr(self, name)
            if seg is not None and seg.contains(open_time):
                return name
        return None


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


def _parse_split(ds: Any) -> DataSplitConfig:
    """data_split: {train, validation, final_test}, var och en null (platshållare) eller {start, end}."""
    if ds is None:
        return DataSplitConfig()
    if not isinstance(ds, dict):
        raise ConfigError("data_split måste vara en mapping")
    if set(ds) != set(SPLIT_SEGMENTS):
        raise ConfigError(f"data_split måste ha exakt nycklarna {list(SPLIT_SEGMENTS)}, fick {sorted(ds)}")
    segs: dict[str, SplitSegment | None] = {}
    for name in SPLIT_SEGMENTS:
        v = ds[name]
        if v is None:
            segs[name] = None
            continue
        if not isinstance(v, dict) or set(v) != {"start", "end"}:
            raise ConfigError(f"data_split.{name} måste vara null eller {{start, end}}, fick {v!r}")
        segs[name] = SplitSegment(_parse_dt(v["start"], f"data_split.{name}.start"),
                                  _parse_dt(v["end"], f"data_split.{name}.end"))
    return DataSplitConfig(**segs)


def load_settings(path: str | Path) -> Settings:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    unknown = set(raw) - _TOP_LEVEL
    if unknown:
        raise ConfigError(f"okända toppnivånycklar: {sorted(unknown)}")
    tf = raw.get("timeframe") or {}
    if set(tf) - {"primary"}:
        raise ConfigError(f"okända nycklar i timeframe: {sorted(set(tf) - {'primary'})}")
    split = _parse_split(raw.get("data_split"))
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
