"""LocalOHLCVProvider: DataProvider (Phase 1-interface) ovanpå DatasetStore.

KAUSALITETSKONTRAKT (Phase 1):  get_bars(as_of=t) returnerar ENDAST barer med
available_at <= t, där available_at = open_time + timeframe.

Semantik
  * [start, end) på open_time (end exklusiv).
  * as_of=None  => ingen kausalitetsfiltrering (all lagrad, avslutad data).
    Backtester/paper ska ALLTID skicka as_of; `require_as_of=True` tvingar det.
  * Returnerad frame: DatetimeIndex 'open_time' (UTC), kolumner
    open, high, low, close, volume, quote_volume, trade_count,
    taker_buy_base_volume, taker_buy_quote_volume, gap_before, available_at.
  * gap_before[t] beror bara på bar t-1 och t (kausal). Ingen "gap_after"
    exponeras, eftersom det skulle avslöja framtida data.
  * Ingen resampling här (multi-timeframe är ett feature-steg med egen kausalitetslogik).
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from btc_trading_system.core.interfaces import DataProvider
from btc_trading_system.core.types import TIMEFRAMES, Bar, require_utc
from btc_trading_system.data.schema import DatasetKey, Instrument, to_epoch_ns
from btc_trading_system.data.storage import DatasetStore
from btc_trading_system.data_validation.ohlcv import compute_gap_flags


class LocalOHLCVProvider(DataProvider):
    def __init__(self, store: DatasetStore | str | Path, instrument: Instrument | str,
                 require_as_of: bool = False) -> None:
        self.store = store if isinstance(store, DatasetStore) else DatasetStore(store)
        self.instrument = Instrument(instrument)
        self.require_as_of = require_as_of
        self._cache: dict[DatasetKey, tuple[str, pd.DataFrame]] = {}

    def _key(self, symbol: str, timeframe: str) -> DatasetKey:
        return DatasetKey(self.instrument, symbol, timeframe)

    def _load(self, key: DatasetKey) -> pd.DataFrame:
        # Manifestet (litet) läses varje gång: re-ingest invaliderar cachen.
        version = self.store.read_manifest(key)["data_version"]
        cached = self._cache.get(key)
        if cached is None or cached[0] != version:
            df, manifest = self.store.read(key)       # verifierar hash + validering
            df = df.set_index("open_time")
            df["available_at"] = df.index + TIMEFRAMES[key.timeframe]
            self._cache[key] = (manifest["data_version"], df)
        return self._cache[key][1]

    def manifest(self, symbol: str, timeframe: str) -> dict:
        return self.store.read_manifest(self._key(symbol, timeframe))

    def get_bars(self, symbol: str, timeframe: str, start: datetime, end: datetime,
                 as_of: datetime | None = None) -> pd.DataFrame:
        start = require_utc(start, "start")
        end = require_utc(end, "end")
        if end < start:
            raise ValueError("end < start")
        if as_of is None and self.require_as_of:
            raise ValueError("as_of krävs (require_as_of=True) för att garantera kausalitet")
        key = self._key(symbol, timeframe)
        df = self._load(key)
        idx = df.index
        lo = idx.searchsorted(pd.Timestamp(start), side="left")
        hi = idx.searchsorted(pd.Timestamp(end), side="left")
        if as_of is not None:
            as_of = require_utc(as_of, "as_of")
            # available_at <= as_of  <=>  open_time <= as_of - timeframe
            hi = min(hi, idx.searchsorted(pd.Timestamp(as_of) - TIMEFRAMES[timeframe], side="right"))
        return df.iloc[lo:max(lo, hi)].copy()

    def get_gaps(self, symbol: str, timeframe: str, as_of: datetime | None = None) -> pd.DataFrame:
        """Luckor. Med as_of: bara luckor vars efterföljande bar redan var avslutad
        (annars skulle framtida datas existens läcka)."""
        key = self._key(symbol, timeframe)
        df = self._load(key)
        _, gaps = compute_gap_flags(to_epoch_ns(df.reset_index()["open_time"]), timeframe)
        if as_of is not None:
            as_of = require_utc(as_of, "as_of")
            gaps = gaps[gaps["resumes_at"] + TIMEFRAMES[timeframe] <= pd.Timestamp(as_of)]
        return gaps.reset_index(drop=True)


def frame_to_bars(df: pd.DataFrame, symbol: str, timeframe: str) -> list[Bar]:
    """Konverterar provider-output till Phase 1 `Bar`-objekt (validerar OHLC/UTC igen)."""
    return [Bar(symbol=symbol, timeframe=timeframe, open_time=ts.to_pydatetime(),
                open=r.open, high=r.high, low=r.low, close=r.close, volume=r.volume)
            for ts, r in zip(df.index, df.itertuples())]
