"""Etapp B steg 5: definitionsfilen, fond i ISK och BTC i SEK, FINAL TEST-spärren, pins (SB-S5-a/b/c/f).
T24 och T25 är handräknade (DESIGN_step5_final.md avsnitt 7). Bara syntetiska data."""
import copy
import hashlib
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal as D
from pathlib import Path

import pandas as pd
import pytest
import yaml

from btc_trading_system.benchmark import fx, nav, pins, series
from btc_trading_system.config.settings import DataSplitConfig, load_settings

REPO = Path(__file__).resolve().parents[1]
CFG = series.load_benchmark_config()
SPLIT = load_settings(REPO / "configs" / "default.yaml").data_split
RAW = yaml.safe_load(series.DEFAULT_BENCHMARK_PATH.read_bytes())
LOCKED_FILE_SHA256 = "d4d1d4dad29e5239b9caa77a4094379c0e18fc93a3f292f5e08df7c25a966cde"
LOCKED_CANONICAL_SHA256 = "722f33423701802c464afbd4edfd11bc69152d0f58996af342ab4a30fbd910cf"
LOCKED_PINS_FILE_SHA256 = "03e407375b7a205e6816c5ace935e7269d13f4a1d7840fd80687c36126be7064"
START = datetime(2024, 1, 8, tzinfo=UTC)                                  # VALIDATION-start


def days(first, n):
    return [first + timedelta(days=i) for i in range(n)]


def const_fx(first, n, value="10"):
    return fx.parse_swea_observations(
        ("[" + ",".join(f'{{"date":"{d.isoformat()}","value":{value}}}' for d in days(first, n)) + "]").encode(),
        CFG.fx_availability)


def const_nav(first, n, value="100"):
    return nav.canonicalize_nav([(d.isoformat(), value) for d in days(first, n)], CFG.nav_availability)


def test_config_is_locked_and_defines_k2():
    raw = series.DEFAULT_BENCHMARK_PATH.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == LOCKED_FILE_SHA256
    assert CFG.file_sha256 == LOCKED_FILE_SHA256 and CFG.canonical_sha256 == LOCKED_CANONICAL_SHA256
    assert (CFG.isin, CFG.index, CFG.currency, CFG.ter) == ("IE00B4L5Y983", "MSCI World Index (Net)", "USD", D("0.0020"))
    assert (CFG.fx_source, CFG.fx_series, CFG.fx_scale_exponent) == ("riksbank", "SEKUSDPMI", 6)
    assert (CFG.costs.fx_fee_rate, CFG.costs.commission_rate, CFG.costs.half_spread_rate,
            CFG.costs.stress_multiplier) == (D("0.0025"), D("0.0025"), D("0.0005"), D(2))
    assert (CFG.gap_max_days, CFG.nav_jump_threshold, CFG.capital_sek) == (5, D("0.15"), D(50000))
    assert CFG.fx_availability == fx.Availability(1, time(0), "Europe/Stockholm")
    assert CFG.nav_availability == fx.Availability(1, time(9), "Europe/Stockholm")


def _mut(fn):
    d = copy.deepcopy(RAW)
    fn(d)
    return d


@pytest.mark.parametrize("change", [
    lambda d: d.update(extra=1), lambda d: d.update(version=2), lambda d: d["fund"].update(isin="IE00B4L5Y98"),
    lambda d: d["fund"].update(accumulating=False), lambda d: d["fund"].update(account="depå"),
    lambda d: d["fund"].update(currency="usd"), lambda d: d["fund"].update(ter=0.002),
    lambda d: d["fund"].update(data_order=[]), lambda d: d["fx"].update(source="ecb"),
    lambda d: d["fx"]["available"].update(time="24:00"), lambda d: d["fx"]["available"].update(days_after=-1),
    lambda d: d["fx"]["available"].update(tz="Mars/Base"), lambda d: d["costs"].update(fx_fee_rate="1.5"),
    lambda d: d["costs"].update(stress_multiplier="0.5"), lambda d: d["costs"].update(commission_rate="0.6"),
    lambda d: d["quality"].update(gap_max_days=0), lambda d: d["quality"].update(nav_jump_threshold="0"),
    lambda d: d.update(capital_sek="0"), lambda d: d["fund"].pop("nav_available"),
])
def test_config_parsed_strictly(change):
    with pytest.raises(series.BenchmarkConfigError):
        series.parse_benchmark_config(_mut(change))


def test_t24_k2_costs_exact():
    f = CFG.costs.factor()
    assert f == D("0.994508746875")
    run = series.fund_in_isk(const_nav(date(2024, 1, 1), 20), const_fx(date(2024, 1, 1), 20), START,
                             days(date(2024, 1, 8), 5), CFG, SPLIT)
    assert run.invested_sek == D("49725.43734375")
    assert run.liquidation_sek == D("49452.38238054414111328125")
    assert D(50000) - run.liquidation_sek == D("547.61761945585888671875")
    assert all(v == D("49725.43734375") for v in run.daily["value_sek"])


def test_t25_k2_stress_exact():
    run = series.fund_in_isk(const_nav(date(2024, 1, 1), 20), const_fx(date(2024, 1, 1), 20), START,
                             days(date(2024, 1, 8), 5), CFG, SPLIT, stress=True)
    assert run.liquidation_sek == D("48909.509088662531250000")


def test_fund_uses_latest_published_nav_and_rate():
    nv = nav.canonicalize_nav([("2024-01-05", "100"), ("2024-01-08", "110")], CFG.nav_availability)
    rates = const_fx(date(2024, 1, 1), 20)
    run = series.fund_in_isk(nv, rates, START, [date(2024, 1, 8), date(2024, 1, 9)], CFG, SPLIT)
    d = run.daily.set_index("day")
    # NAV för 2024-01-08 publiceras 2024-01-09 09:00 Stockholm: inte känd vid dygnsslutet 2024-01-08
    assert d.loc[date(2024, 1, 8), "nav_obs_date"] == date(2024, 1, 5) and d.loc[date(2024, 1, 8), "nav_age_days"] == 3
    assert d.loc[date(2024, 1, 9), "nav_obs_date"] == date(2024, 1, 8)
    assert d.loc[date(2024, 1, 9), "value_sek"] == run.invested_sek * D("1.1")


def test_final_test_is_blocked_without_gate4_flag():
    ft = SPLIT.final_test
    first = ft.start.date()
    with pytest.raises(series.FinalTestBlockedError):
        series.fund_in_isk(const_nav(first - timedelta(days=5), 10), const_fx(first - timedelta(days=5), 10),
                           ft.start, days(first, 2), CFG, SPLIT)
    with pytest.raises(series.FinalTestBlockedError):                     # ett segment som når in i FINAL TEST
        series.require_allowed(datetime(2025, 7, 1, tzinfo=UTC), ft.start + timedelta(seconds=1), SPLIT)
    series.require_allowed(datetime(2025, 7, 1, tzinfo=UTC), ft.start, SPLIT)          # [start, end): når inte in
    series.require_allowed(ft.start, ft.end, SPLIT, gate4=True)
    with pytest.raises(series.FinalTestBlockedError):
        series.require_allowed(START, START + timedelta(days=1), DataSplitConfig())    # ingen låst uppdelning


def test_btc_in_sek_uses_day_end_bar_and_rate():
    bars = pd.DataFrame({"open_time": pd.to_datetime(["2024-01-08T23:58Z", "2024-01-08T23:59Z", "2024-01-09T23:59Z"]),
                         "close": [46000.5, 46001.25, 47000.0]})
    closes = series.day_end_closes(bars)
    assert closes == {date(2024, 1, 8): D("46001.25"), date(2024, 1, 9): D("47000.0")}
    out = series.btc_in_sek(closes, const_fx(date(2024, 1, 1), 20, "10.5"), [date(2024, 1, 8), date(2024, 1, 9)],
                            SPLIT, start=START, fx_scale_exponent=6)
    assert list(out["value_sek"]) == [D("483013.125"), D(493500)]
    with pytest.raises(KeyError):
        series.btc_in_sek(closes, const_fx(date(2024, 1, 1), 20), [date(2024, 1, 10)], SPLIT, start=START,
                          fx_scale_exponent=6)


def test_nav_canonicalization():
    f = nav.canonicalize_nav([("2024-01-05", "100.123456")], CFG.nav_availability)
    assert tuple(f.columns) == fx.OBS_COLUMNS and int(f["value_scaled"].iat[0]) == 100_123_456
    for bad in ([("2024-01-05", "1.0000001")], [("2024-01-05", "x")], [], [("2024-01-06", "1"), ("2024-01-05", "1")],
                [("2024-01-05", 1.0)]):
        with pytest.raises(nav.NavDataError):
            nav.canonicalize_nav(bad, CFG.nav_availability)


# ------------------------------------------------------------------ pins (SB-S5-c)
def test_pins_file_is_locked_and_empty():
    raw = pins.DEFAULT_PINS_PATH.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == LOCKED_PINS_FILE_SHA256
    assert pins.load_pins().pins == {}


def test_read_pinned_verifies_sha256(tmp_path):
    (tmp_path / "canonical").mkdir()
    data = b"obs_date,value_scaled\n2024-01-05,10500000\n"
    (tmp_path / "canonical" / "riksbank-SEKUSDPMI.csv").write_bytes(data)
    h = hashlib.sha256(data).hexdigest()
    p = pins.parse_pins({"version": 1, "pins": {"usdsek": {"path": "canonical/riksbank-SEKUSDPMI.csv", "sha256": h}}})
    assert pins.read_pinned(p, "usdsek", tmp_path) == data
    (tmp_path / "canonical" / "riksbank-SEKUSDPMI.csv").write_bytes(data + b"x")
    with pytest.raises(pins.PinError, match="sha256"):
        pins.read_pinned(p, "usdsek", tmp_path)
    with pytest.raises(pins.PinError):
        pins.read_pinned(p, "k2-nav", tmp_path)


@pytest.mark.parametrize("data", [
    None, {"version": 1}, {"version": 2, "pins": {}}, {"version": 1, "pins": []},
    {"version": 1, "pins": {"X": {"path": "a", "sha256": "0" * 64}}},
    {"version": 1, "pins": {"a": {"path": "../a", "sha256": "0" * 64}}},
    {"version": 1, "pins": {"a": {"path": "/a", "sha256": "0" * 64}}},
    {"version": 1, "pins": {"a": {"path": "a", "sha256": "A" * 64}}},
    {"version": 1, "pins": {"a": {"path": "a"}}},
])
def test_pins_parsed_strictly(data):
    with pytest.raises(pins.PinError):
        pins.parse_pins(data)
