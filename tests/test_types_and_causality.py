from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from btc_trading_system.core.types import (Bar, CausalityError, Fill, Order,
                                           OrderType, Side, Signal, validate_causality)
from btc_trading_system.data_validation.causality import LookAheadError, assert_causal

T0 = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def _bar(**kw):
    d = dict(symbol="BTCUSDT", timeframe="15m", open_time=T0, open=100.0,
             high=101.0, low=99.0, close=100.5, volume=10.0)
    d.update(kw)
    return Bar(**d)


def test_bar_available_at_is_close_time():
    assert _bar().available_at == T0 + timedelta(minutes=15)


def test_naive_timestamp_rejected():
    with pytest.raises(ValueError):
        _bar(open_time=datetime(2024, 1, 1, 12, 0))


def test_non_utc_offset_rejected():
    tz = timezone(timedelta(hours=2))
    with pytest.raises(ValueError):
        _bar(open_time=datetime(2024, 1, 1, 12, 0, tzinfo=tz))


def test_inconsistent_ohlc_rejected():
    with pytest.raises(ValueError):
        _bar(high=98.0)


def _chain(fill_at):
    sig = Signal("s1", "mom", "BTCUSDT", Side.LONG, 1.0, T0)
    order = Order("o1", "s1", "BTCUSDT", Side.LONG, 0.1, OrderType.MARKET, T0)
    fill = Fill("o1", 100.0, 0.1, 0.05, 0.02, fill_at)
    return sig, order, fill


def test_fill_strictly_after_order_ok():
    validate_causality(*_chain(T0 + timedelta(minutes=15)))


def test_same_time_fill_rejected():
    with pytest.raises(CausalityError):
        validate_causality(*_chain(T0))


def test_fill_before_order_rejected():
    with pytest.raises(CausalityError):
        validate_causality(*_chain(T0 - timedelta(seconds=1)))


def test_order_before_signal_rejected():
    sig, order, fill = _chain(T0 + timedelta(minutes=15))
    early = Order("o1", "s1", "BTCUSDT", Side.LONG, 0.1, OrderType.MARKET, T0 - timedelta(minutes=1))
    with pytest.raises(CausalityError):
        validate_causality(sig, early, fill)


def test_limit_order_requires_price():
    with pytest.raises(ValueError):
        Order("o", "s", "BTCUSDT", Side.LONG, 1.0, OrderType.LIMIT, T0)


# ---- assert_causal måste själv fungera: fånga fusk, släppa igenom kausalt ----
def _df(n=60):
    rng = np.random.default_rng(0)
    return pd.DataFrame({"close": 100 + rng.normal(0, 1, n).cumsum()})


def test_causal_feature_passes():
    assert_causal(lambda d: d["close"].rolling(5).mean(), _df())


def test_shift_minus_one_is_detected():
    with pytest.raises(LookAheadError):
        assert_causal(lambda d: d["close"].shift(-1), _df())


def test_centered_window_is_detected():
    with pytest.raises(LookAheadError):
        assert_causal(lambda d: d["close"].rolling(5, center=True).mean(), _df())


def test_global_normalisation_is_detected():
    with pytest.raises(LookAheadError):
        assert_causal(lambda d: (d["close"] - d["close"].mean()) / d["close"].std(), _df())


def test_expanding_normalisation_passes():
    f = lambda d: (d["close"] - d["close"].expanding().mean()) / d["close"].expanding().std()
    assert_causal(f, _df())
