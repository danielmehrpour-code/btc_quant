"""Generellt look-ahead-test: trunkeringsinvarians.

Om en feature är kausal måste värdet på rad t vara identiskt oavsett om
raderna > t finns eller inte. Beräknar vi på hela serien och på serien
avklippt vid k ska alla rader < k vara lika. Om framtida rader påverkar
en tidigare rad (t.ex. shift(-1), centrerat fönster, global normalisering)
skiljer sig resultatet -> testet fallerar.
"""
from __future__ import annotations

from typing import Callable

import numpy as np
import pandas as pd


MIN_ROWS = 8


class LookAheadError(AssertionError):
    pass


def assert_causal(fn: Callable[[pd.DataFrame], pd.DataFrame | pd.Series],
                  df: pd.DataFrame, cut_points: list[int] | None = None,
                  atol: float = 1e-12) -> None:
    n = len(df)
    if n < MIN_ROWS:
        raise ValueError(f"assert_causal kräver minst {MIN_ROWS} rader (fick {n}); för få rader ger falskt godkänt")
    cuts = sorted({k for k in (cut_points or [n // 4, n // 2, (3 * n) // 4, n - 1]) if 1 <= k < n})
    if not cuts:
        raise ValueError("inga giltiga cut_points")
    full = fn(df)
    for k in cuts:
        part = fn(df.iloc[:k])
        a = full.iloc[:k].to_numpy(dtype=float)
        b = part.to_numpy(dtype=float)
        if a.shape != b.shape or not np.allclose(a, b, atol=atol, equal_nan=True):
            raise LookAheadError(f"Look-ahead: resultat före rad {k} ändras av framtida data")
