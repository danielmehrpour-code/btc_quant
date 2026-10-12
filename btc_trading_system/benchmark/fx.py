"""USD/SEK från Riksbankens SWEA-API (etapp B steg 5, SB-S5-a).

Källformat (Riksbanken, kontrollerat 2026-10-12): JSON-lista [{"date": "YYYY-MM-DD", "value": <tal>}].
`value` tolkas med Decimal direkt ur JSON-texten (aldrig float) och lagras som heltal i enheter av 10^-scale
SEK per USD; ett värde som inte kan representeras exakt avvisas. Datumen ska vara strikt stigande och unika.

Kausalitet: available_at = observationsdatum + days_after kl. time i tz (konservativt, publiceringstiden är
OVERIFIERAD, Q-E). `value_asof(t)` ger senaste kurs med available_at <= t (merge_asof bakåt, LOOKAHEAD p. 5) och
är den enda vägen för beslut och dagliga värden. `rate_for_tax_date(d)` ger kursen för affärsdagen (eller senast
före, helg) för skatteredovisning i efterhand; den får aldrig bli indata till ett beslut (T-K2).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from btc_trading_system.core.types import require_utc

OBS_COLUMNS = ("obs_date", "value_scaled", "available_at")   # även NAV-ramar (nav.py)


class FxDataError(ValueError):
    """Källdata eller förfrågan om växelkurs är ogiltig."""


@dataclass(frozen=True)
class Availability:
    """available_at = datum + days_after dagar, kl. at, i tidszonen tz."""
    days_after: int
    at: time
    tz: str

    def __post_init__(self) -> None:
        if self.days_after < 0:
            raise FxDataError("days_after måste vara >= 0")
        ZoneInfo(self.tz)

    def available_at(self, d: date) -> datetime:
        local = datetime.combine(d + timedelta(days=self.days_after), self.at, tzinfo=ZoneInfo(self.tz))
        return local.astimezone(UTC)


def scale_exact(value: Decimal, scale_exponent: int, name: str) -> int:
    try:
        scaled = value.scaleb(scale_exponent)
    except InvalidOperation as e:
        raise FxDataError(f"{name}: ogiltigt värde {value!r}") from e
    if not scaled.is_finite() or scaled != scaled.to_integral_value() or scaled <= 0:
        raise FxDataError(f"{name}: {value} är inte ett positivt exakt värde med {scale_exponent} decimaler")
    return int(scaled)


def parse_swea_observations(raw: bytes, availability: Availability, scale_exponent: int = 6) -> pd.DataFrame:
    """Rå SWEA-svar -> kanonisk ram (obs_date, value_scaled int64, available_at UTC)."""
    try:
        data: Any = json.loads(raw.decode("utf-8"), parse_float=Decimal, parse_int=Decimal)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise FxDataError(f"SWEA: ogiltig JSON: {e}") from e
    if not isinstance(data, list) or not data:
        raise FxDataError("SWEA: svaret måste vara en icke-tom lista")
    dates: list[date] = []
    rates: list[int] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict) or set(row) != {"date", "value"}:
            raise FxDataError(f"SWEA rad {i}: nycklarna måste vara exakt date, value")
        if not isinstance(row["date"], str) or len(row["date"]) != 10:
            raise FxDataError(f"SWEA rad {i}: datum YYYY-MM-DD")
        try:
            d = date.fromisoformat(row["date"])
        except ValueError as e:
            raise FxDataError(f"SWEA rad {i}: ogiltigt datum {row['date']!r}") from e
        if not isinstance(row["value"], Decimal):
            raise FxDataError(f"SWEA rad {i}: value måste vara ett tal")
        dates.append(d)
        rates.append(scale_exact(row["value"], scale_exponent, f"SWEA rad {i}"))
    if any(b <= a for a, b in pairwise(dates)):
        raise FxDataError("SWEA: datumen måste vara strikt stigande och unika")
    return pd.DataFrame({
        "obs_date": pd.Series(dates, dtype="object"),
        "value_scaled": np.asarray(rates, dtype="int64"),
        "available_at": pd.Series([availability.available_at(d) for d in dates], dtype="datetime64[ns, UTC]"),
    })


def _check_frame(frame: pd.DataFrame) -> None:
    if tuple(frame.columns) != OBS_COLUMNS or frame.empty:
        raise FxDataError(f"observationsramen måste ha kolumnerna {OBS_COLUMNS} och minst en rad")


@dataclass(frozen=True)
class ValueAt:
    value_scaled: int
    obs_date: date
    available_at: datetime


def value_asof(frame: pd.DataFrame, t: datetime) -> ValueAt:
    """Senaste observation med available_at <= t. Inga kurser publicerade före t -> fel."""
    _check_frame(frame)
    t = require_utc(t, "t")
    i = int(frame["available_at"].searchsorted(pd.Timestamp(t), side="right")) - 1
    if i < 0:
        raise FxDataError(f"ingen observation publicerad före {t.isoformat()}")
    return ValueAt(int(frame["value_scaled"].iat[i]), frame["obs_date"].iat[i],
                  frame["available_at"].iat[i].to_pydatetime())


def rate_for_tax_date(frame: pd.DataFrame, d: date) -> ValueAt:
    """Kursen för affärsdagen d, eller senaste före (helg/helgdag). Bara för skatteredovisning i efterhand."""
    _check_frame(frame)
    obs = list(frame["obs_date"])
    i = int(np.searchsorted(np.asarray([o.toordinal() for o in obs]), d.toordinal(), side="right")) - 1
    if i < 0:
        raise FxDataError(f"ingen kurs den {d.isoformat()} eller före")
    return ValueAt(int(frame["value_scaled"].iat[i]), obs[i], frame["available_at"].iat[i].to_pydatetime())


def utc_day_end(d: date) -> datetime:
    """Slutet av UTC-dygnet d = d+1 00:00 UTC (SB-S5-a)."""
    return datetime.combine(d + timedelta(days=1), time(0), tzinfo=UTC)


def daily_values(frame: pd.DataFrame, days: list[date], gap_max_days: int) -> pd.DataFrame:
    """Värde (kurs eller NAV) vid slutet av varje UTC-dygn, med senaste publicerade kurs bakåt. age_days = dagar sedan kursens
    observationsdatum; filled = kursen gäller inte dygnet självt; gap_long = age_days > gap_max_days (FAIL i
    kvalitetsrapporten). Ingen interpolation."""
    rows = []
    for d in days:
        r = value_asof(frame, utc_day_end(d))
        age = (d - r.obs_date).days
        rows.append((d, r.value_scaled, r.obs_date, age, age > 0, age > gap_max_days))
    return pd.DataFrame(rows, columns=["day", "value_scaled", "obs_date", "age_days", "filled", "gap_long"])
