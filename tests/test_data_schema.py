import pytest

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data.schema import (DatasetKey, Instrument, normalize_symbol, timeframe_ms)


def test_symbol_normalisation():
    for s in ("BTC/USDT", "btcusdt", "BTC-USDT", " BTCUSDT "):
        assert normalize_symbol(s) == "BTCUSDT"


def test_invalid_symbols_rejected():
    for s in ("", "BTC_USDT", "BT", "BTC USDT", "BTC/USDT/X!"):
        with pytest.raises(ValueError):
            normalize_symbol(s)


def test_timeframe_ms_all_supported_timeframes():
    expect = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
              "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
    assert set(expect) == set(TIMEFRAMES)
    for tf, ms in expect.items():
        assert timeframe_ms(tf) == ms


def test_unknown_timeframe_rejected():
    with pytest.raises(ValueError):
        timeframe_ms("3m")


def test_dataset_key_paths_separate_spot_and_perp():
    a = DatasetKey(Instrument.SPOT, "BTC/USDT", "15m")
    b = DatasetKey("perp", "BTCUSDT", "15m")
    assert a.relpath == "binance/spot/BTCUSDT/15m"
    assert b.relpath == "binance/perp/BTCUSDT/15m"
    assert a != b


def test_dataset_key_rejects_bad_instrument_and_timeframe():
    with pytest.raises(ValueError):
        DatasetKey("margin", "BTCUSDT", "15m")
    with pytest.raises(ValueError):
        DatasetKey("spot", "BTCUSDT", "2h")
