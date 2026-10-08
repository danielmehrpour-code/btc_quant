"""Regressionstester för defekter hittade i granskningen efter PHASE 2.
Varje test fallerade (eller hade fallerat) på koden före rättningen."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from btc_trading_system.config.settings import (ConfigError, LiveTradingNotAllowed, Settings,
                                                TradingConfig, assert_live_allowed, load_settings)
from btc_trading_system.data.binance_reader import read_source_file
from btc_trading_system.data.ingest import ingest_dataset
from btc_trading_system.data.provider import LocalOHLCVProvider
from btc_trading_system.data.storage import DatasetStore
from btc_trading_system.data_validation.causality import assert_causal
from btc_trading_system.data_validation.errors import DataValidationError, MalformedDataError
from btc_trading_system.data_validation.ohlcv import check_ohlcv
from data_helpers import (NOW, START, RawBackend, build_store, make_klines, to_rows, write_csv,
                          write_rows)

ROOT = Path(__file__).parent.parent
GOOD_ENV = {"LIVE_TRADING": "true", "LIVE_TRADING_CONFIRM": "I_UNDERSTAND_THE_RISK",
            "EXCHANGE_API_KEY": "k", "EXCHANGE_API_SECRET": "s"}


def _st(tmp_path, name="st"):
    return DatasetStore(tmp_path / name, RawBackend())


# ---- R1: Binance µs-filer (open + tf - 1 µs) avvisades felaktigt ----
def test_r1_binance_microsecond_files_ingest(tmp_path):
    src = make_klines("1m", 60)
    f = write_csv(tmp_path / "BTCUSDT-1m-2025-01.csv", src, unit="us")
    res = ingest_dataset([f], "spot", "1m", _st(tmp_path), now=NOW)
    assert res.manifest["rows"] == 60


def test_r1_close_time_other_offsets_still_rejected(tmp_path):
    rows = to_rows(make_klines("1m", 5), unit="us")
    rows[2][6] = str(int(rows[2][0]) + 60_000_000 - 2)          # -2 µs: varken Binance-format
    df, _ = read_source_file(write_rows(tmp_path / "x.csv", rows))
    assert check_ohlcv(df, "1m", now=NOW).has("CLOSE_TIME_MISMATCH")


def test_r1_open_time_sub_ms_still_rejected(tmp_path):
    rows = to_rows(make_klines("1m", 5), unit="us")
    rows[1][0] = str(int(rows[1][0]) + 1)
    with pytest.raises(MalformedDataError):
        read_source_file(write_rows(tmp_path / "x.csv", rows))


# ---- R6: samma bar i ms-fil och µs-fil ska dedupliceras, inte ge konflikt ----
def test_r6_ms_and_us_overlap_deduplicated(tmp_path):
    full = make_klines("1m", 40)
    a = write_csv(tmp_path / "a.csv", full.iloc[:25], unit="ms")
    b = write_csv(tmp_path / "b.csv", full.iloc[15:], unit="us")
    res = ingest_dataset([a, b], "spot", "1m", _st(tmp_path), now=NOW)
    assert res.duplicates_dropped == 10 and res.manifest["rows"] == 40
    ref = ingest_dataset([write_csv(tmp_path / "all.csv", full)], "spot", "1m", _st(tmp_path, "ref"), now=NOW)
    assert res.manifest["data_version"] == ref.manifest["data_version"]


# ---- R2: fel timeframe utan close_time accepterades som "luckor" ----
def test_r2_wrong_timeframe_without_close_time_rejected():
    df = make_klines("1h", 50).drop(columns=["close_time"])
    rep = check_ohlcv(df, "15m", now=NOW)
    assert rep.has("TIMEFRAME_MISMATCH") and not rep.ok


def test_r2_wrong_timeframe_iso_file_rejected_in_ingest(tmp_path):
    d = make_klines("1d", 30).drop(columns=["close_time"])
    d["open_time"] = d["open_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    p = tmp_path / "d.csv"
    d.to_csv(p, index=False)
    with pytest.raises(DataValidationError) as ei:
        ingest_dataset([p], "spot", "1h", _st(tmp_path), now=NOW)
    assert ei.value.report.has("TIMEFRAME_MISMATCH")


def test_r2_realistic_gaps_do_not_trigger_mismatch():
    full = make_klines("15m", 200)
    df = full.drop(index=list(range(20, 60, 2))).reset_index(drop=True)   # 20 enstaka luckor (~10 %)
    rep = check_ohlcv(df.drop(columns=["close_time"]), "15m", now=NOW)
    assert rep.ok and not rep.has("TIMEFRAME_MISMATCH") and rep.has("GAP")


# ---- R3: Binance-filnamn med fel symbol/timeframe accepterades ----
def test_r3_filename_symbol_mismatch_rejected(tmp_path):
    f = write_csv(tmp_path / "ETHUSDT-15m-2024-01.csv", make_klines("15m", 20))
    with pytest.raises(MalformedDataError, match="ETHUSDT"):
        ingest_dataset([f], "spot", "15m", _st(tmp_path), now=NOW)


def test_r3_filename_timeframe_mismatch_rejected(tmp_path):
    f = write_csv(tmp_path / "BTCUSDT-1h-2024-01-31.csv", make_klines("15m", 20))
    with pytest.raises(MalformedDataError, match="1h"):
        ingest_dataset([f], "spot", "15m", _st(tmp_path), now=NOW)


def test_r3_matching_and_custom_filenames_accepted(tmp_path):
    a = write_csv(tmp_path / "BTCUSDT-15m-2024-01.csv", make_klines("15m", 20))
    b = write_csv(tmp_path / "min_egen_fil.csv", make_klines("15m", 20))
    ingest_dataset([a], "spot", "15m", _st(tmp_path, "a"), now=NOW)
    ingest_dataset([b], "spot", "15m", _st(tmp_path, "b"), now=NOW)


# ---- R4: dubblerade kolumner efter alias gav TypeError i stället för tydligt fel ----
def test_r4_duplicate_columns_after_alias(tmp_path):
    p = tmp_path / "dup.csv"
    hdr = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume",
           "count", "trade_count", "taker_buy_volume", "taker_buy_quote_volume"]
    rows = [r[:9] + [r[8]] + r[9:11] for r in to_rows(make_klines("15m", 5))]
    p.write_text(",".join(hdr) + "\n" + "\n".join(",".join(r) for r in rows) + "\n")
    with pytest.raises(MalformedDataError, match="trade_count"):
        read_source_file(p)


# ---- R5: provider-cache såg inte re-ingest ----
def test_r5_provider_sees_reingested_data(tmp_path):
    st = build_store(tmp_path / "c", "spot", "15m", 50, seed=1)
    prov = LocalOHLCVProvider(st, "spot")
    before = prov.get_bars("BTCUSDT", "15m", START, NOW)
    ingest_dataset([write_csv(tmp_path / "new.csv", make_klines("15m", 70, seed=9))], "spot", "15m", st, now=NOW)
    after = prov.get_bars("BTCUSDT", "15m", START, NOW)
    assert len(before) == 50 and len(after) == 70


# ---- R7: config — säkerhetsspärr fick sträng 'false' (truthy) ----
def _yaml(tmp_path, text):
    p = tmp_path / "c.yaml"
    p.write_text(text)
    return p


def test_r7_string_false_for_live_rejected(tmp_path):
    with pytest.raises(ConfigError):
        load_settings(_yaml(tmp_path, "trading: {live: 'false'}\n"))
    with pytest.raises(ConfigError):
        load_settings(_yaml(tmp_path, "trading: {live: 1}\n"))
    with pytest.raises(ConfigError):
        TradingConfig(live="true")


def test_r7_live_gate_requires_real_true():
    s = Settings(trading=TradingConfig(live=True))
    assert_live_allowed(s, GOOD_ENV, paper_validated=True)
    with pytest.raises(LiveTradingNotAllowed):
        assert_live_allowed(s, GOOD_ENV, paper_validated="yes")      # truthy, men inte True


def test_r7_unknown_keys_rejected(tmp_path):
    for text in ("risk_typo: {x: 1}\n", "risk: {risk_per_trad: 0.01}\n",
                 "timeframe: {primry: 15m}\n", "data_split: {train_end: '2023-01-01T00:00:00Z'}\n"):
        with pytest.raises(ConfigError):
            load_settings(_yaml(tmp_path, text))


def test_r7_invalid_timeframe_and_symbol_rejected(tmp_path):
    with pytest.raises(ConfigError):
        load_settings(_yaml(tmp_path, "timeframe: {primary: 7m}\n"))
    with pytest.raises(ValueError):
        load_settings(_yaml(tmp_path, "market: {symbol: 'BTC USDT'}\n"))
    with pytest.raises(ValueError):
        load_settings(_yaml(tmp_path, "market: {instrument: margin}\n"))
    assert load_settings(_yaml(tmp_path, "market: {symbol: 'btc/usdt'}\n")).market.symbol == "BTCUSDT"


def test_r7_data_split_parsed_and_validated(tmp_path):
    s = load_settings(ROOT / "configs" / "default.yaml")
    assert [(seg.start.isoformat(), seg.end.isoformat())
            for seg in (s.data_split.train, s.data_split.validation, s.data_split.final_test)] == [
        ("2020-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"),
        ("2024-01-08T00:00:00+00:00", "2025-07-01T00:00:00+00:00"),
        ("2025-07-08T00:00:00+00:00", "2026-10-01T00:00:00+00:00")]                     # låst, D-B-h
    good = ("data_split: {train: {start: '2021-01-01T00:00:00Z', end: '2022-01-01T00:00:00Z'}, "
            "validation: {start: '2022-01-08T00:00:00Z', end: '2023-01-01T00:00:00Z'}, "
            "final_test: {start: '2023-01-08T00:00:00Z', end: '2024-01-01T00:00:00Z'}}\n")
    assert load_settings(_yaml(tmp_path, good)).data_split.validation.start == datetime(2022, 1, 8, tzinfo=timezone.utc)
    bad = [
        good.replace("'2021-01-01T00:00:00Z'", "2021-01-01"),                                    # YAML-datum: tvetydigt
        good.replace("2021-01-01T00:00:00Z", "2021-01-01T00:00:00"),                              # naiv
        good.replace("2021-01-01T00:00:00Z", "2022-02-01T00:00:00Z"),                             # start efter end
        good.replace("2022-01-08T00:00:00Z", "2022-01-07T23:59:59Z"),                             # lucka < 7 dagar
        good.replace("2021-01-01T00:00:00Z", "2021-01-01T00:00:00+02:00"),                        # inte UTC
        "data_split: {train: null, validation: null}\n",                                         # en del saknas
        good.replace("final_test: {start: '2023-01-08T00:00:00Z', end: '2024-01-01T00:00:00Z'}", "final_test: null"),
    ]
    for text in bad:
        with pytest.raises((ConfigError, ValueError)):
            load_settings(_yaml(tmp_path, text))


# ---- R8: assert_causal gav falskt godkänt vid för få rader ----
def test_r8_assert_causal_requires_enough_rows():
    tiny = pd.DataFrame({"x": np.arange(3, dtype=float)})
    with pytest.raises(ValueError):
        assert_causal(lambda d: d["x"].shift(-1), tiny)


def test_r8_assert_causal_checks_last_row_cut():
    """Läcka som bara påverkar näst sista raden fångas (cut vid n-1)."""
    df = pd.DataFrame({"x": np.arange(40, dtype=float)})

    def leaky(d):
        out = d["x"].copy()
        if len(d) == 40:
            out.iloc[-2] = d["x"].iloc[-1]       # använder sista radens värde
        return out
    from btc_trading_system.data_validation.causality import LookAheadError
    with pytest.raises(LookAheadError):
        assert_causal(leaky, df)
