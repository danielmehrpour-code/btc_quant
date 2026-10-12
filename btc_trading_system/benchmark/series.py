"""Dagliga SEK-serier för jämförelsen (etapp B steg 5, SB-S5-a, SB-S5-b, SB-S5-f) och definitionsfilen
configs/benchmark_v1.yaml (fondens identitet, valutakälla, kostnader, kvalitetsgränser, kapital).

Regler:
- Varje dagligt värde avser slutet av UTC-dygnet och använder senast publicerade observation bakåt (fx.value_asof).
  Köp vid segmentets start använder senast publicerade observation före starten.
- Fond i ISK: köp vid start och försäljning vid slut med faktorn (1 - växling)(1 - courtage)(1 - halv spread) per
  order; i stressfallet multipliceras varje sats med stress_multiplier. Fondens avgift ligger i NAV och dras inte igen.
- FINAL TEST spärras: en serie som överlappar FINAL TEST vägras utan uttrycklig flagga för Gate 4 (SB-S5-f).
- Exakt aritmetik: andelarna hålls som bråk (Fraction); värden lämnas som Decimal med hög precision.
"""
from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, localcontext
from fractions import Fraction
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from btc_trading_system.benchmark.fx import Availability, utc_day_end, value_asof
from btc_trading_system.config.settings import DataSplitConfig
from btc_trading_system.core.types import require_utc
from btc_trading_system.data_validation.quality_config import canonical_json

BENCHMARK_CONFIG_VERSION = 1
DEFAULT_BENCHMARK_PATH = Path(__file__).resolve().parents[2] / "configs" / "benchmark_v1.yaml"
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")
_HHMM = re.compile(r"([01][0-9]|2[0-3]):[0-5][0-9]")


class BenchmarkConfigError(ValueError):
    """configs/benchmark_v1.yaml är ogiltig."""


class FinalTestBlockedError(PermissionError):
    """Serien överlappar FINAL TEST och flaggan för Gate 4 är inte satt (SB-S5-f)."""


@dataclass(frozen=True)
class Costs:
    fx_fee_rate: Decimal
    commission_rate: Decimal
    half_spread_rate: Decimal
    stress_multiplier: Decimal

    def factor(self, stress: bool = False) -> Decimal:
        """Andel av beloppet som återstår efter en order (köp eller försäljning)."""
        m = self.stress_multiplier if stress else Decimal(1)
        return (1 - m * self.fx_fee_rate) * (1 - m * self.commission_rate) * (1 - m * self.half_spread_rate)


@dataclass(frozen=True)
class BenchmarkConfig:
    fund_name: str
    isin: str
    index: str
    currency: str
    ter: Decimal
    nav_availability: Availability
    data_order: tuple[str, ...]
    fx_source: str
    fx_series: str
    fx_scale_exponent: int
    fx_availability: Availability
    costs: Costs
    gap_max_days: int
    nav_jump_threshold: Decimal
    capital_sek: Decimal
    cash_interest_rate: Decimal
    canonical_sha256: str
    file_sha256: str | None = None


def _keys(d: Any, keys: set[str], name: str) -> dict[str, Any]:
    if not isinstance(d, dict) or set(d) != keys:
        raise BenchmarkConfigError(f"benchmark.{name}: nycklarna måste vara exakt {sorted(keys)}")
    return d


def _dec(v: Any, name: str, lo: Decimal = Decimal(0), hi: Decimal | None = None) -> Decimal:
    if not isinstance(v, str):
        raise BenchmarkConfigError(f"benchmark.{name}: ett decimaltal som sträng")
    try:
        d = Decimal(v)
    except ArithmeticError as e:
        raise BenchmarkConfigError(f"benchmark.{name}: ogiltigt decimaltal {v!r}") from e
    if not d.is_finite() or d < lo or (hi is not None and d > hi):
        raise BenchmarkConfigError(f"benchmark.{name}: utanför tillåtet intervall")
    return d


def _str(v: Any, name: str) -> str:
    if not isinstance(v, str) or not v.strip():
        raise BenchmarkConfigError(f"benchmark.{name}: en icke-tom sträng")
    return v


def _avail(d: Any, name: str) -> Availability:
    d = _keys(d, {"days_after", "time", "tz"}, name)
    if type(d["days_after"]) is not int or not 0 <= d["days_after"] <= 7:
        raise BenchmarkConfigError(f"benchmark.{name}.days_after: heltal 0..7")
    if not isinstance(d["time"], str) or not _HHMM.fullmatch(d["time"]):
        raise BenchmarkConfigError(f"benchmark.{name}.time: HH:MM")
    try:
        return Availability(d["days_after"], time.fromisoformat(d["time"]), _str(d["tz"], f"{name}.tz"))
    except Exception as e:
        raise BenchmarkConfigError(f"benchmark.{name}: {e}") from e


def parse_benchmark_config(data: Any, file_sha256: str | None = None) -> BenchmarkConfig:
    top = _keys(data, {"version", "fund", "fx", "costs", "quality", "capital_sek", "cash_interest_rate"}, "(topp)")
    if top["version"] != BENCHMARK_CONFIG_VERSION or isinstance(top["version"], bool):
        raise BenchmarkConfigError(f"benchmark.version måste vara {BENCHMARK_CONFIG_VERSION}")
    f = _keys(top["fund"], {"name", "isin", "index", "currency", "ter", "accumulating", "account",
                            "nav_available", "data_order"}, "fund")
    if not isinstance(f["isin"], str) or not _ISIN.fullmatch(f["isin"]):
        raise BenchmarkConfigError("benchmark.fund.isin: ogiltigt ISIN")
    if f["accumulating"] is not True:
        raise BenchmarkConfigError("benchmark.fund.accumulating: måste vara true (utdelningar i NAV)")
    if f["account"] != "ISK":
        raise BenchmarkConfigError("benchmark.fund.account: måste vara ISK (G-23)")
    if not isinstance(f["currency"], str) or not re.fullmatch(r"[A-Z]{3}", f["currency"]):
        raise BenchmarkConfigError("benchmark.fund.currency: tre versaler")
    order = f["data_order"]
    if not isinstance(order, list) or not order or not all(isinstance(x, str) and x for x in order):
        raise BenchmarkConfigError("benchmark.fund.data_order: en icke-tom lista med text")
    fx = _keys(top["fx"], {"source", "series", "scale_exponent", "available"}, "fx")
    if fx["source"] != "riksbank":
        raise BenchmarkConfigError("benchmark.fx.source: bara riksbank (SB-S5-a)")
    if type(fx["scale_exponent"]) is not int or not 0 <= fx["scale_exponent"] <= 9:
        raise BenchmarkConfigError("benchmark.fx.scale_exponent: heltal 0..9")
    c = _keys(top["costs"], {"fx_fee_rate", "commission_rate", "half_spread_rate", "stress_multiplier"}, "costs")
    one = Decimal(1)
    costs = Costs(_dec(c["fx_fee_rate"], "costs.fx_fee_rate", hi=one),
                  _dec(c["commission_rate"], "costs.commission_rate", hi=one),
                  _dec(c["half_spread_rate"], "costs.half_spread_rate", hi=one),
                  _dec(c["stress_multiplier"], "costs.stress_multiplier", lo=one))
    if costs.factor(stress=True) <= 0:
        raise BenchmarkConfigError("benchmark.costs: stressfallet ger en icke-positiv faktor")
    q = _keys(top["quality"], {"gap_max_days", "nav_jump_threshold"}, "quality")
    if type(q["gap_max_days"]) is not int or not 1 <= q["gap_max_days"] <= 31:
        raise BenchmarkConfigError("benchmark.quality.gap_max_days: heltal 1..31")
    return BenchmarkConfig(
        fund_name=_str(f["name"], "fund.name"), isin=f["isin"], index=_str(f["index"], "fund.index"),
        currency=f["currency"], ter=_dec(f["ter"], "fund.ter", hi=Decimal("0.05")),
        nav_availability=_avail(f["nav_available"], "fund.nav_available"), data_order=tuple(order),
        fx_source=fx["source"], fx_series=_str(fx["series"], "fx.series"), fx_scale_exponent=fx["scale_exponent"],
        fx_availability=_avail(fx["available"], "fx.available"), costs=costs, gap_max_days=q["gap_max_days"],
        nav_jump_threshold=_dec(q["nav_jump_threshold"], "quality.nav_jump_threshold", lo=Decimal("0.01"), hi=one),
        capital_sek=_dec(top["capital_sek"], "capital_sek", lo=one),
        cash_interest_rate=_dec(top["cash_interest_rate"], "cash_interest_rate", hi=one),
        canonical_sha256=hashlib.sha256(canonical_json(data)).hexdigest(), file_sha256=file_sha256)


def load_benchmark_config(path: str | Path | None = None) -> BenchmarkConfig:
    raw = Path(path or DEFAULT_BENCHMARK_PATH).read_bytes()
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise BenchmarkConfigError(f"benchmark: ogiltig YAML: {e}") from e
    return parse_benchmark_config(data, file_sha256=hashlib.sha256(raw).hexdigest())


def require_allowed(start: datetime, end: datetime, split: DataSplitConfig, gate4: bool = False) -> None:
    """Vägrar [start, end) som överlappar FINAL TEST utan flaggan för Gate 4, och vägrar utan låst uppdelning."""
    start, end = require_utc(start, "start"), require_utc(end, "end")
    if not start < end:
        raise ValueError("start måste vara före end")
    ft = split.final_test
    if ft is None:
        raise FinalTestBlockedError("ingen låst datauppdelning; FINAL TEST kan inte avgränsas")
    if start < ft.end and ft.start < end and not gate4:
        raise FinalTestBlockedError("perioden överlappar FINAL TEST; kräver flaggan för Gate 4 (SB-S5-f)")


def _dec_of(x: Fraction) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 60
        return Decimal(x.numerator) / Decimal(x.denominator)


@dataclass(frozen=True)
class FundRun:
    daily: pd.DataFrame          # day, value_sek (Decimal), nav_obs_date, fx_obs_date, nav_age_days, fx_age_days
    invested_sek: Decimal        # värdet direkt efter köpet
    liquidation_sek: Decimal     # värdet vid sista dygnsslutet efter försäljningskostnaden


def fund_in_isk(nav: pd.DataFrame, fx: pd.DataFrame, start: datetime, days: Sequence[date], cfg: BenchmarkConfig,
                split: DataSplitConfig, *, stress: bool = False, gate4: bool = False) -> FundRun:
    """Fond i ISK från segmentets start: köp med senast publicerade NAV och kurs före start, dagliga värden vid
    slutet av varje UTC-dygn, försäljning vid sista dygnsslutet."""
    if not days:
        raise ValueError("fund_in_isk: minst en dag")
    require_allowed(start, utc_day_end(days[-1]), split, gate4)
    scale = Fraction(10) ** (-cfg.fx_scale_exponent)
    nav0, fx0 = value_asof(nav, start), value_asof(fx, start)
    factor = Fraction(cfg.costs.factor(stress))
    invested = Fraction(cfg.capital_sek) * factor
    units = invested / (nav0.value_scaled * scale * fx0.value_scaled * scale)
    rows = []
    for d in days:
        t = utc_day_end(d)
        n, x = value_asof(nav, t), value_asof(fx, t)
        v = units * n.value_scaled * scale * x.value_scaled * scale
        rows.append((d, _dec_of(v), n.obs_date, x.obs_date, (d - n.obs_date).days, (d - x.obs_date).days, v))
    last = rows[-1][-1]
    daily = pd.DataFrame([r[:-1] for r in rows], columns=["day", "value_sek", "nav_obs_date", "fx_obs_date",
                                                          "nav_age_days", "fx_age_days"])
    return FundRun(daily, _dec_of(invested), _dec_of(last * factor))


def day_end_closes(bars: pd.DataFrame) -> dict[date, Decimal]:
    """close för 1m-baren med open_time = 23:59 UTC per dygn (den som stänger vid dygnets slut), som Decimal ur
    flyttalets kortaste representation."""
    if not {"open_time", "close"} <= set(bars.columns):
        raise ValueError("day_end_closes: kolumnerna open_time och close krävs")
    ot = pd.to_datetime(bars["open_time"], utc=True)
    mask = (ot.dt.hour == 23) & (ot.dt.minute == 59) & (ot.dt.second == 0)
    return {t.date(): Decimal(repr(float(c))) for t, c in zip(ot[mask], bars["close"][mask])}


def btc_in_sek(day_end_close_usd: dict[date, Decimal], fx: pd.DataFrame, days: Sequence[date],
               split: DataSplitConfig, *, start: datetime, fx_scale_exponent: int, gate4: bool = False) -> pd.DataFrame:
    """BTC i SEK vid slutet av varje UTC-dygn: close för 1m-baren som stänger vid dygnets slut x kurs (A-1: USDT=USD)."""
    if not days:
        raise ValueError("btc_in_sek: minst en dag")
    require_allowed(start, utc_day_end(days[-1]), split, gate4)
    scale = Fraction(10) ** (-fx_scale_exponent)
    rows = []
    for d in days:
        if d not in day_end_close_usd:
            raise KeyError(f"close saknas för {d.isoformat()}")
        x = value_asof(fx, utc_day_end(d))
        rows.append((d, _dec_of(Fraction(day_end_close_usd[d]) * x.value_scaled * scale), x.obs_date))
    return pd.DataFrame(rows, columns=["day", "value_sek", "fx_obs_date"])
