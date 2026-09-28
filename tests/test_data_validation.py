from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data.schema import to_epoch_ns
from btc_trading_system.data_validation.errors import DataValidationError
from btc_trading_system.data_validation.ohlcv import check_ohlcv, compute_gap_flags
from data_helpers import NOW, make_klines


def _codes(df, tf="15m", now=NOW):
    return check_ohlcv(df, tf, now=now).codes()


def test_clean_data_passes_for_every_timeframe():
    for tf in TIMEFRAMES:
        rep = check_ohlcv(make_klines(tf, 60), tf, now=NOW)
        assert rep.ok and not rep.has("GAP"), (tf, rep.issues)


def test_empty_frame():
    assert "EMPTY" in _codes(make_klines("15m", 5).iloc[0:0])


def test_missing_column():
    assert "SCHEMA_MISSING_COLUMN" in _codes(make_klines("15m", 5).drop(columns=["volume"]))


def test_timestamps_as_strings_rejected():
    df = make_klines("15m", 5)
    df["open_time"] = df["open_time"].astype(str)
    assert "SCHEMA_DTYPE" in _codes(df)


def test_timestamps_as_integers_rejected():
    df = make_klines("15m", 5)
    df["open_time"] = df["open_time"].astype("int64")
    assert "SCHEMA_DTYPE" in _codes(df)


def test_naive_timestamps_rejected():
    df = make_klines("15m", 5)
    df["open_time"] = df["open_time"].dt.tz_localize(None)
    assert "TZ_NOT_UTC" in _codes(df)


def test_non_utc_timezone_rejected():
    df = make_klines("15m", 5)
    df["open_time"] = df["open_time"].dt.tz_convert("Europe/Stockholm")
    rep = check_ohlcv(df, "15m")
    assert rep.has("TZ_NOT_UTC") and not rep.ok


def test_nat_timestamp_rejected():
    df = make_klines("15m", 5)
    df.loc[2, "open_time"] = pd.NaT
    assert "NULL_TIMESTAMP" in _codes(df)


def test_duplicate_timestamps_detected():
    df = make_klines("15m", 10)
    df = pd.concat([df, df.iloc[[3]]]).sort_values("open_time", kind="stable").reset_index(drop=True)
    rep = check_ohlcv(df, "15m", now=NOW)
    assert rep.has("DUPLICATE_TIMESTAMPS") and rep.errors[0].count >= 1
    assert len(rep.gaps) == 0                      # gap-analys hoppas över när axeln är trasig


def test_non_monotonic_detected():
    df = make_klines("15m", 10)
    df = df.iloc[[0, 1, 3, 2, 4, 5, 6, 7, 8, 9]].reset_index(drop=True)
    assert "NOT_MONOTONIC" in _codes(df)


def test_off_grid_detected():
    df = make_klines("15m", 10)
    df["open_time"] = df["open_time"] + pd.Timedelta(seconds=1)
    assert "OFF_GRID" in _codes(df)


def test_sub_millisecond_offset_detected():
    df = make_klines("1m", 10)
    df["open_time"] = df["open_time"] + pd.Timedelta(microseconds=1)
    assert "OFF_GRID" in _codes(df, "1m")


def test_wrong_timeframe_detected_via_close_time():
    df = make_klines("15m", 10)                    # 15m-data granskas som 5m
    assert "CLOSE_TIME_MISMATCH" in _codes(df, "5m")


def test_ohlc_inconsistent():
    df = make_klines("15m", 10)
    df.loc[4, "high"] = df.loc[4, "close"] - 1.0
    assert "OHLC_INCONSISTENT" in _codes(df)


def test_low_above_open():
    df = make_klines("15m", 10)
    df.loc[4, "low"] = max(df.loc[4, "open"], df.loc[4, "close"]) + 1.0
    assert "OHLC_INCONSISTENT" in _codes(df)


def test_non_positive_price():
    df = make_klines("15m", 10)
    df.loc[4, "low"] = 0.0
    assert "NON_POSITIVE_PRICE" in _codes(df)


def test_negative_volume():
    df = make_klines("15m", 10)
    df.loc[4, "volume"] = -1.0
    assert "NEGATIVE_VALUE" in _codes(df)


def test_nan_and_inf_detected():
    df = make_klines("15m", 10)
    df.loc[2, "close"] = np.nan
    df.loc[3, "volume"] = np.inf
    rep = check_ohlcv(df, "15m", now=NOW)
    assert rep.has("NULL_VALUES") and rep.errors[0].count == 2


def test_wrong_float_dtype_detected():
    df = make_klines("15m", 10)
    df["close"] = df["close"].astype("float32")
    assert "SCHEMA_DTYPE" in _codes(df)


def test_future_and_incomplete_bar_detected():
    df = make_klines("15m", 10)
    last_avail = df["open_time"].iloc[-1] + pd.Timedelta(minutes=15)
    assert "FUTURE_BAR" not in _codes(df, now=last_avail.to_pydatetime())            # exakt avslutad = ok
    assert "FUTURE_BAR" in _codes(df, now=(last_avail - pd.Timedelta(seconds=1)).to_pydatetime())


def test_naive_now_rejected():
    with pytest.raises(ValueError):
        check_ohlcv(make_klines("15m", 3), "15m", now=NOW.replace(tzinfo=None))


def test_unknown_timeframe_rejected():
    with pytest.raises(ValueError):
        check_ohlcv(make_klines("15m", 3), "2h")


def test_zero_volume_is_info_not_error():
    df = make_klines("15m", 10)
    df.loc[3, "volume"] = 0.0
    rep = check_ohlcv(df, "15m", now=NOW)
    assert rep.ok and rep.has("ZERO_VOLUME")


def test_missing_bars_reported_as_explicit_gaps():
    full = make_klines("15m", 40)
    df = full.drop(index=[10, 11, 12, 30]).reset_index(drop=True)
    rep = check_ohlcv(df, "15m", now=NOW)
    assert rep.ok and rep.has("GAP")                                     # gap = varning, inte fel
    g = rep.gaps
    assert list(g["missing_bars"]) == [3, 1]
    assert g["gap_start"].iloc[0] == full["open_time"].iloc[10]
    assert g["gap_end"].iloc[0] == full["open_time"].iloc[12]
    assert g["resumes_at"].iloc[0] == full["open_time"].iloc[13]
    assert g["gap_start"].iloc[1] == g["gap_end"].iloc[1] == full["open_time"].iloc[30]


def test_gap_flags_are_causal_and_exact():
    full = make_klines("1h", 30)
    df = full.drop(index=[5, 6]).reset_index(drop=True)
    flags, gaps = compute_gap_flags(to_epoch_ns(df["open_time"]), "1h")
    assert flags.sum() == 1 and flags[5]          # flaggan sitter på baren EFTER luckan
    assert not flags[0] and not flags[4]          # ingen flagga på baren före luckan
    assert len(gaps) == 1 and gaps["missing_bars"].iloc[0] == 2


def test_consecutive_gaps_across_all_timeframes():
    for tf in TIMEFRAMES:
        full = make_klines(tf, 20)
        df = full.drop(index=[3, 4, 5, 6, 7, 8]).reset_index(drop=True)
        _, gaps = compute_gap_flags(to_epoch_ns(df["open_time"]), tf)
        assert list(gaps["missing_bars"]) == [6], tf


def test_no_gaps_and_single_row():
    f, g = compute_gap_flags(to_epoch_ns(make_klines("15m", 10)["open_time"]), "15m")
    assert not f.any() and len(g) == 0
    f, g = compute_gap_flags(to_epoch_ns(make_klines("15m", 1)["open_time"]), "15m")
    assert not f.any() and len(g) == 0


def test_gap_before_column_must_match_reality():
    full = make_klines("15m", 20)
    df = full.drop(index=[8]).reset_index(drop=True)
    df["gap_before"] = False                       # ljuger: det finns en lucka
    assert "GAP_FLAG_MISMATCH" in _codes(df)


def test_raise_if_errors_carries_report():
    df = make_klines("15m", 10)
    df.loc[1, "volume"] = -5.0
    with pytest.raises(DataValidationError) as ei:
        check_ohlcv(df, "15m").raise_if_errors(context="unit")
    assert ei.value.report.has("NEGATIVE_VALUE")
