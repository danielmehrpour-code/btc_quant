"""Fondens NAV (etapp B steg 5, SB-S5-b). Byggs i detta steg bara mot syntetiska fixturer: K2:s NAV hämtas först
efter BlackRocks skriftliga tillstånd, och källans filformat tolkas i det steget (egen parser, eget beslut).

Kanonisk ram = samma schema som växelkursen (fx.OBS_COLUMNS): obs_date (NAV-datum), value_scaled (NAV i
enheter av 10^-scale av fondens valuta, exakt heltal), available_at (NAV-datum + days_after kl. time i tz;
konservativt eftersom publiceringstiden är OVERIFIERAD). Värden tolkas med Decimal ur text, aldrig float.
"""
from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from decimal import Decimal, InvalidOperation
from itertools import pairwise

import numpy as np
import pandas as pd

from btc_trading_system.benchmark.fx import Availability, FxDataError, scale_exact


class NavDataError(ValueError):
    """NAV-data är ogiltig."""


def canonicalize_nav(records: Iterable[tuple[str, str]], availability: Availability,
                     scale_exponent: int = 6) -> pd.DataFrame:
    """[(\"YYYY-MM-DD\", \"<nav>\"), ...] -> kanonisk ram; datum strikt stigande och unika."""
    dates: list[date] = []
    values: list[int] = []
    for i, rec in enumerate(records):
        if not (isinstance(rec, tuple) and len(rec) == 2 and all(isinstance(x, str) for x in rec)):
            raise NavDataError(f"NAV rad {i}: (datum, värde) som text")
        try:
            d = date.fromisoformat(rec[0])
            v = Decimal(rec[1])
        except (ValueError, InvalidOperation) as e:
            raise NavDataError(f"NAV rad {i}: ogiltigt datum eller värde") from e
        try:
            values.append(scale_exact(v, scale_exponent, f"NAV rad {i}"))
        except FxDataError as e:
            raise NavDataError(str(e)) from e
        dates.append(d)
    if not dates:
        raise NavDataError("NAV: inga rader")
    if any(b <= a for a, b in pairwise(dates)):
        raise NavDataError("NAV: datumen måste vara strikt stigande och unika")
    return pd.DataFrame({
        "obs_date": pd.Series(dates, dtype="object"),
        "value_scaled": np.asarray(values, dtype="int64"),
        "available_at": pd.Series([availability.available_at(d) for d in dates], dtype="datetime64[ns, UTC]"),
    })
