import pandas as pd
import pytest

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data.ingest import ingest_dataset
from btc_trading_system.data.storage import DatasetStore
from btc_trading_system.data_validation.errors import DataValidationError, MalformedDataError
from data_helpers import (NOW, RawBackend, make_klines, write_csv, write_zip)


def _st(tmp_path, name="store"):
    return DatasetStore(tmp_path / name, RawBackend())


def test_basic_ingest_and_manifest(tmp_path):
    src = make_klines("15m", 100)
    st = _st(tmp_path)
    res = ingest_dataset([write_csv(tmp_path / "a.csv", src)], "spot", "15m", st, now=NOW)
    df, man = st.read(res.key)
    assert len(df) == 100 and man["gap_count"] == 0 and man["duplicates_dropped"] == 0
    assert man["sources"][0]["name"] == "a.csv" and len(man["sources"][0]["sha256"]) == 64
    assert not df["gap_before"].any()
    assert res.report.ok


def test_all_timeframes_ingest(tmp_path):
    for tf in TIMEFRAMES:
        st = _st(tmp_path, f"s_{tf}")
        res = ingest_dataset([write_csv(tmp_path / f"{tf}.csv", make_klines(tf, 40))], "perp", tf, st, now=NOW)
        assert res.manifest["rows"] == 40 and res.manifest["timeframe"] == tf


def test_files_in_any_order_are_merged_sorted(tmp_path):
    full = make_klines("15m", 300)
    a = write_csv(tmp_path / "a.csv", full.iloc[:150])
    b = write_csv(tmp_path / "b.csv", full.iloc[150:])
    s1, s2 = _st(tmp_path, "s1"), _st(tmp_path, "s2")
    r1 = ingest_dataset([b, a], "spot", "15m", s1, now=NOW)
    r2 = ingest_dataset([write_csv(tmp_path / "full.csv", full)], "spot", "15m", s2, now=NOW)
    assert r1.manifest["data_version"] == r2.manifest["data_version"]


def test_identical_overlap_deduplicated_and_counted(tmp_path):
    full = make_klines("15m", 300)
    a = write_csv(tmp_path / "a.csv", full.iloc[:200])
    b = write_csv(tmp_path / "b.csv", full.iloc[150:])
    res = ingest_dataset([a, b], "spot", "15m", _st(tmp_path), now=NOW)
    assert res.duplicates_dropped == 50 and res.manifest["rows"] == 300


def test_conflicting_duplicates_fail_and_write_nothing(tmp_path):
    full = make_klines("15m", 100)
    other = full.iloc[40:60].copy()
    other.loc[other.index[5], "volume"] += 1.0
    a, b = write_csv(tmp_path / "a.csv", full), write_csv(tmp_path / "b.csv", other)
    st = _st(tmp_path)
    with pytest.raises(DataValidationError) as ei:
        ingest_dataset([a, b], "spot", "15m", st, now=NOW)
    assert ei.value.report.has("DUPLICATE_CONFLICT")
    assert not (tmp_path / "store").exists()


def test_unsorted_file_rejected(tmp_path):
    src = make_klines("15m", 50).iloc[::-1]
    with pytest.raises(DataValidationError) as ei:
        ingest_dataset([write_csv(tmp_path / "r.csv", src)], "spot", "15m", _st(tmp_path), now=NOW)
    assert ei.value.report.has("NOT_MONOTONIC")


def test_gaps_are_flagged_not_filled(tmp_path):
    full = make_klines("15m", 100)
    src = full.drop(index=[20, 21, 22, 60]).reset_index(drop=True)
    st = _st(tmp_path)
    res = ingest_dataset([write_csv(tmp_path / "g.csv", src)], "spot", "15m", st, now=NOW)
    df, man = st.read(res.key)
    assert len(df) == 96                                   # inga barer fabricerade
    assert man["gap_count"] == 2 and man["missing_bars_total"] == 4
    assert [g["missing_bars"] for g in man["gaps"]] == [3, 1]
    assert list(df.index[df["gap_before"]]) == [20, 57]    # bar efter luckan är flaggad


def test_fail_on_gaps(tmp_path):
    src = make_klines("15m", 50).drop(index=[10]).reset_index(drop=True)
    with pytest.raises(DataValidationError):
        ingest_dataset([write_csv(tmp_path / "g.csv", src)], "spot", "15m", _st(tmp_path),
                       now=NOW, fail_on_gaps=True)


def test_partial_future_bar_rejected(tmp_path):
    src = make_klines("15m", 10)
    mid = (src["open_time"].iloc[-1] + pd.Timedelta(minutes=7)).to_pydatetime()
    with pytest.raises(DataValidationError) as ei:
        ingest_dataset([write_csv(tmp_path / "p.csv", src)], "spot", "15m", _st(tmp_path), now=mid)
    assert ei.value.report.has("FUTURE_BAR")


def test_wrong_timeframe_file_rejected(tmp_path):
    f = write_csv(tmp_path / "m15.csv", make_klines("15m", 30))
    with pytest.raises(DataValidationError) as ei:
        ingest_dataset([f], "spot", "5m", _st(tmp_path), now=NOW)
    assert ei.value.report.has("CLOSE_TIME_MISMATCH")


def test_off_grid_timestamps_rejected(tmp_path):
    src = make_klines("15m", 10)
    src["open_time"] = src["open_time"] + pd.Timedelta(seconds=5)
    src["close_time"] = src["close_time"] + pd.Timedelta(seconds=5)
    with pytest.raises(DataValidationError) as ei:
        ingest_dataset([write_csv(tmp_path / "o.csv", src)], "spot", "15m", _st(tmp_path), now=NOW)
    assert ei.value.report.has("OFF_GRID")


def test_bad_file_aborts_whole_ingest(tmp_path):
    good = write_csv(tmp_path / "good.csv", make_klines("15m", 20))
    bad = tmp_path / "bad.csv"
    bad.write_text("not,a,kline,file\n")
    with pytest.raises(MalformedDataError):
        ingest_dataset([good, bad], "spot", "15m", _st(tmp_path), now=NOW)
    assert not (tmp_path / "store").exists()


def test_spot_and_perp_stored_separately(tmp_path):
    st = _st(tmp_path)
    ingest_dataset([write_csv(tmp_path / "s.csv", make_klines("15m", 30, seed=1))], "spot", "15m", st, now=NOW)
    ingest_dataset([write_csv(tmp_path / "p.csv", make_klines("15m", 30, seed=2))], "perp", "15m", st, now=NOW)
    files = sorted(str(p.relative_to(tmp_path / "store")) for p in (tmp_path / "store").rglob("*.manifest.json"))
    assert files == ["binance/perp/BTCUSDT/15m.manifest.json", "binance/spot/BTCUSDT/15m.manifest.json"]


def test_reingest_is_deterministic(tmp_path):
    f = write_csv(tmp_path / "a.csv", make_klines("15m", 120).drop(index=[30]).reset_index(drop=True))
    s1, s2 = _st(tmp_path, "one"), _st(tmp_path, "two")
    r1 = ingest_dataset([f], "spot", "15m", s1, now=NOW)
    r2 = ingest_dataset([f], "spot", "15m", s2, now=NOW)
    assert s1.data_path(r1.key).read_bytes() == s2.data_path(r2.key).read_bytes()
    assert s1.manifest_path(r1.key).read_bytes() == s2.manifest_path(r2.key).read_bytes()


def test_csv_variants_give_identical_dataset(tmp_path):
    src = make_klines("15m", 60)
    versions = {
        "plain": write_csv(tmp_path / "p.csv", src),
        "header": write_csv(tmp_path / "h.csv", src, header=True),
        "micro": write_csv(tmp_path / "u.csv", src, unit="us"),
        "zip": write_zip(tmp_path / "z.zip", src),
    }
    hashes = set()
    for name, f in versions.items():
        r = ingest_dataset([f], "spot", "15m", _st(tmp_path, name), now=NOW)
        hashes.add(r.manifest["data_version"])
    assert len(hashes) == 1


def test_symbol_alias_and_bad_arguments(tmp_path):
    f = write_csv(tmp_path / "a.csv", make_klines("15m", 10))
    res = ingest_dataset([f], "spot", "15m", _st(tmp_path), symbol="BTC/USDT", now=NOW)
    assert res.key.symbol == "BTCUSDT"
    with pytest.raises(ValueError):
        ingest_dataset([], "spot", "15m", _st(tmp_path, "x"))
    with pytest.raises(ValueError):
        ingest_dataset([f], "margin", "15m", _st(tmp_path, "y"))
    with pytest.raises(ValueError):
        ingest_dataset([f], "spot", "2h", _st(tmp_path, "z"))
