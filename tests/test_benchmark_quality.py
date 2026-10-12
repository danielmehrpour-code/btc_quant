"""Etapp B steg 5: kvalitetskontroller för jämförelsedata (SB-S5-a, SB-S5-g) och T20 (FINAL TEST-händelser
utan belopp). Bara syntetiska data."""
import re
from datetime import date, timedelta

from btc_trading_system.benchmark import fx, nav, series
from btc_trading_system.data_validation import benchmark_quality as bq
from btc_trading_system.data_validation.quality_config import Status

CFG = series.load_benchmark_config()


def fx_frame(dates, value="10"):
    return fx.parse_swea_observations(
        ("[" + ",".join(f'{{"date":"{d.isoformat()}","value":{value}}}' for d in dates) + "]").encode(),
        CFG.fx_availability)


def span(first, n):
    return [first + timedelta(days=i) for i in range(n)]


def codes(rep):
    return {f.code: f for c in rep.checks for f in c.findings}


def test_gap_rules():
    f = fx_frame([date(2024, 3, 1), date(2024, 3, 8)])
    rep = bq.check_benchmark(CFG, fx.daily_values(f, span(date(2024, 3, 1), 8), CFG.gap_max_days))
    c = codes(rep)
    assert rep.overall == "FAIL" and rep.checks[0].status == Status.FAIL
    assert c["FX_GAP_LONG"].count == 1 and c["FX_GAP_LONG"].examples == ("2024-03-07",)
    assert c["FX_FILLED"].count == 5
    ok = bq.check_benchmark(CFG, fx.daily_values(f, span(date(2024, 3, 1), 6), CFG.gap_max_days))
    assert ok.overall == "PASS" and "FX_GAP_LONG" not in codes(ok)


def test_nav_jump_threshold_is_strict():
    nv = nav.canonicalize_nav([("2024-01-02", "100"), ("2024-01-03", "115"), ("2024-01-04", "132.26")],
                              CFG.nav_availability)
    rep = bq.check_benchmark(CFG, fx.daily_values(fx_frame(span(date(2024, 1, 1), 5)), span(date(2024, 1, 2), 3), 5),
                             nav_obs=nv)
    c = codes(rep)
    assert c["NAV_JUMP"].count == 1 and c["NAV_JUMP"].examples == ("2024-01-04",)   # 15 % exakt flaggas inte
    assert rep.overall == "WARN"


def test_nav_gap_is_checked():
    nv = nav.canonicalize_nav([("2024-01-01", "100"), ("2024-01-10", "100")], CFG.nav_availability)
    nd = fx.daily_values(nv, span(date(2024, 1, 2), 9), CFG.gap_max_days)
    rep = bq.check_benchmark(CFG, fx.daily_values(fx_frame(span(date(2023, 12, 30), 15)), span(date(2024, 1, 2), 9), 5),
                             nav_daily=nd)
    assert "NAV_GAP_LONG" in codes(rep) and rep.overall == "FAIL"


def test_t20_final_test_events_show_no_amounts():
    first = date(2025, 7, 10)                                         # syntetiska datum i FINAL TEST
    nv = nav.canonicalize_nav([(first.isoformat(), "123.456789"), ((first + timedelta(days=1)).isoformat(),
                                                                    "98.765432")], CFG.nav_availability)
    rates = fx_frame(span(first - timedelta(days=12), 3), "9.876543")
    rep = bq.check_benchmark(CFG, fx.daily_values(rates, span(first, 2), 5), nav_obs=nv)
    text = bq.render_events(rep)
    assert "NAV_JUMP count=1" in text and "FX_GAP_LONG count=2" in text
    assert "%" not in text
    for amount in ("123", "456", "98", "765", "9.87", "876", "0.2", "20"):
        assert amount not in re.sub(r"\d{4}-\d{2}-\d{2}", "", text), amount
    assert re.sub(r"\d{4}-\d{2}-\d{2}|count=\d+|gap_max_days", "", text).strip(" \n:,=") .count(".") == 0
