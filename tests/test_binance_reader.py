import pandas as pd
import pytest

import btc_trading_system.data.binance_reader as br
from btc_trading_system.data.binance_reader import read_source_file
from btc_trading_system.data_validation.errors import MalformedDataError, TimezoneError
from data_helpers import (BINANCE_HEADER, make_klines, to_rows, write_csv, write_rows, write_zip)


def _same_values(a: pd.DataFrame, b: pd.DataFrame):
    cols = [c for c in b.columns if c in a.columns]
    pd.testing.assert_frame_equal(a[cols].reset_index(drop=True), b[cols].reset_index(drop=True))


def test_headerless_csv_roundtrip_exact(tmp_path):
    src = make_klines("15m", 50)
    got, info = read_source_file(write_csv(tmp_path / "a.csv", src))
    _same_values(got, src)
    assert info.rows == 50 and len(info.sha256) == 64
    assert str(got["open_time"].dt.tz) == "UTC"


def test_header_csv_futures_style(tmp_path):
    src = make_klines("1h", 20)
    got, _ = read_source_file(write_csv(tmp_path / "f.csv", src, header=True))
    _same_values(got, src)


def test_zip_input(tmp_path):
    src = make_klines("5m", 20)
    got, info = read_source_file(write_zip(tmp_path / "z.zip", src))
    _same_values(got, src)
    assert info.name == "z.zip"


def test_microsecond_timestamps_accepted_and_equal_to_ms(tmp_path):
    src = make_klines("1m", 20)
    a, _ = read_source_file(write_csv(tmp_path / "ms.csv", src, unit="ms"))
    b, _ = read_source_file(write_csv(tmp_path / "us.csv", src, unit="us"))
    pd.testing.assert_frame_equal(a.drop(columns=["close_time"]), b.drop(columns=["close_time"]))
    # µs close_time = open + tf - 1 µs (Binance), ms close_time = open + tf - 1 ms
    assert ((b["close_time"] - b["open_time"]) == pd.Timedelta(minutes=1) - pd.Timedelta(microseconds=1)).all()
    assert ((a["close_time"] - a["open_time"]) == pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)).all()


def test_iso_utc_timestamps_accepted(tmp_path):
    src = make_klines("15m", 5)
    rows = to_rows(src)
    for r, t in zip(rows, src["open_time"]):
        r[0] = t.strftime("%Y-%m-%dT%H:%M:%SZ")
        r[6] = str(int(src["close_time"].iloc[rows.index(r)].value // 1_000_000))
    got, _ = read_source_file(write_rows(tmp_path / "iso.csv", rows, header=True))
    assert list(got["open_time"]) == list(src["open_time"])


def _bad_ts(tmp_path, value, exc, row=2):
    rows = to_rows(make_klines("15m", 5))
    rows[row][0] = value
    with pytest.raises(exc):
        read_source_file(write_rows(tmp_path / "bad.csv", rows))


def test_malformed_timestamp_text(tmp_path):
    _bad_ts(tmp_path, "abc", MalformedDataError)


def test_malformed_timestamp_trailing_garbage(tmp_path):
    _bad_ts(tmp_path, "1704067200000x", MalformedDataError)


def test_seconds_timestamp_rejected(tmp_path):
    rows = to_rows(make_klines("15m", 5))
    for r in rows:
        r[0] = r[0][:10]
    with pytest.raises(MalformedDataError, match="sekunder"):
        read_source_file(write_rows(tmp_path / "s.csv", rows))


def test_nanosecond_timestamp_rejected(tmp_path):
    _bad_ts(tmp_path, "1704067200000000000", MalformedDataError)


def test_sub_millisecond_microseconds_rejected(tmp_path):
    rows = to_rows(make_klines("15m", 5), unit="us")
    rows[1][0] = str(int(rows[1][0]) + 1)
    with pytest.raises(MalformedDataError):
        read_source_file(write_rows(tmp_path / "us.csv", rows))


def test_empty_timestamp_cell(tmp_path):
    _bad_ts(tmp_path, "", MalformedDataError)


def test_mixed_numeric_and_iso_rejected(tmp_path):
    _bad_ts(tmp_path, "2024-01-01T00:30:00Z", MalformedDataError)


def test_timestamp_before_2009_rejected(tmp_path):
    rows = to_rows(make_klines("15m", 3))
    for r in rows:
        r[0] = "946684800000"
    with pytest.raises(MalformedDataError):
        read_source_file(write_rows(tmp_path / "old.csv", rows))


def test_naive_iso_timestamp_is_timezone_violation(tmp_path):
    rows = to_rows(make_klines("15m", 3))
    for r in rows:
        r[0] = "2024-01-01 00:00:00"
    with pytest.raises(TimezoneError):
        read_source_file(write_rows(tmp_path / "naive.csv", rows, header=True))


def test_non_utc_offset_is_timezone_violation(tmp_path):
    rows = to_rows(make_klines("15m", 3))
    for r in rows:
        r[0] = "2024-01-01T02:00:00+02:00"
    with pytest.raises(TimezoneError):
        read_source_file(write_rows(tmp_path / "off.csv", rows, header=True))


def test_non_numeric_price_rejected(tmp_path):
    rows = to_rows(make_klines("15m", 5))
    rows[3][4] = "abc"
    with pytest.raises(MalformedDataError, match="close"):
        read_source_file(write_rows(tmp_path / "p.csv", rows))


def test_nan_and_inf_cells_rejected(tmp_path):
    for bad in ("nan", "inf", ""):
        rows = to_rows(make_klines("15m", 5))
        rows[1][5] = bad
        with pytest.raises(MalformedDataError):
            read_source_file(write_rows(tmp_path / f"n_{len(bad)}.csv", rows))


def test_trade_count_must_be_non_negative_integer(tmp_path):
    for bad in ("1.5", "-1", "x"):
        rows = to_rows(make_klines("15m", 5))
        rows[1][8] = bad
        with pytest.raises(MalformedDataError, match="trade_count"):
            read_source_file(write_rows(tmp_path / "t.csv", rows))


def test_wrong_column_count_headerless(tmp_path):
    rows = [r[:7] for r in to_rows(make_klines("15m", 5))]
    with pytest.raises(MalformedDataError, match="kolumner"):
        read_source_file(write_rows(tmp_path / "c.csv", rows))


def test_header_missing_required_column(tmp_path):
    p = tmp_path / "h.csv"
    p.write_text(",".join(c for c in BINANCE_HEADER if c != "volume") + "\n1,2\n")
    with pytest.raises(MalformedDataError, match="volume"):
        read_source_file(p)


def test_header_aliases_supported(tmp_path):
    src = make_klines("15m", 4)
    p = tmp_path / "alias.csv"
    hdr = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume",
           "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"]
    p.write_text(",".join(hdr) + "\n" + "\n".join(",".join(r) for r in to_rows(src)) + "\n")
    got, _ = read_source_file(p)
    _same_values(got, src)


def test_empty_and_header_only_files(tmp_path):
    p = tmp_path / "empty.csv"
    p.write_text("")
    with pytest.raises(MalformedDataError):
        read_source_file(p)
    p2 = tmp_path / "hdr.csv"
    p2.write_text(",".join(BINANCE_HEADER) + "\n")
    with pytest.raises(MalformedDataError):
        read_source_file(p2)


def test_unsupported_extension_and_missing_file(tmp_path):
    p = tmp_path / "x.txt"
    p.write_text("1")
    with pytest.raises(MalformedDataError):
        read_source_file(p)
    with pytest.raises(MalformedDataError):
        read_source_file(tmp_path / "nope.csv")


def test_zip_needs_exactly_one_csv(tmp_path):
    import zipfile
    p = tmp_path / "two.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("a.csv", "x")
        z.writestr("b.csv", "y")
    with pytest.raises(MalformedDataError):
        read_source_file(p)
    q = tmp_path / "none.zip"
    with zipfile.ZipFile(q, "w") as z:
        z.writestr("readme.txt", "x")
    with pytest.raises(MalformedDataError):
        read_source_file(q)


# ---- Parquet-källor: läsaren mockas eftersom pyarrow saknas i sandboxen ----
def _with_parquet(tmp_path, frame):
    p = tmp_path / "in.parquet"
    p.write_bytes(b"x")
    old = br._read_parquet
    br._read_parquet = lambda path: frame
    try:
        return read_source_file(p)
    finally:
        br._read_parquet = old


def test_parquet_source_tz_aware_utc_ok(tmp_path):
    src = make_klines("15m", 6)
    got, _ = _with_parquet(tmp_path, src.copy())
    _same_values(got, src)


def test_parquet_source_integer_ms_ok(tmp_path):
    src = make_klines("15m", 6)
    raw = src.copy()
    raw["open_time"] = raw["open_time"].map(lambda t: t.value // 1_000_000)
    raw["close_time"] = raw["close_time"].map(lambda t: t.value // 1_000_000)
    got, _ = _with_parquet(tmp_path, raw)
    _same_values(got, src)


def test_parquet_source_naive_datetime_rejected(tmp_path):
    raw = make_klines("15m", 6)
    raw["open_time"] = raw["open_time"].dt.tz_localize(None)
    with pytest.raises(TimezoneError):
        _with_parquet(tmp_path, raw)


def test_parquet_source_non_utc_rejected(tmp_path):
    raw = make_klines("15m", 6)
    raw["open_time"] = raw["open_time"].dt.tz_convert("America/New_York")
    with pytest.raises(TimezoneError):
        _with_parquet(tmp_path, raw)
