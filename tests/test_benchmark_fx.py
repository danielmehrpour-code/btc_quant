"""Etapp B steg 5: USD/SEK (SB-S5-a). T19, T26, T-K1, T-K2; tolkning av SWEA-svar. Bara syntetiska data."""
import json
from datetime import UTC, date, datetime, time
from pathlib import Path

import pandas as pd
import pytest

from btc_trading_system.benchmark import fx

AV = fx.Availability(1, time(0), "Europe/Stockholm")
REPO = Path(__file__).resolve().parents[1]


def swea(rows):
    return json.dumps([{"date": d, "value": v} for d, v in rows]).encode()


def frame(rows):
    return fx.parse_swea_observations(b"[" + b",".join(
        f'{{"date":"{d}","value":{v}}}'.encode() for d, v in rows) + b"]", AV)


def test_parse_exact_decimal_and_schema():
    f = frame([("2024-01-05", "10.1234"), ("2024-01-08", "10.25")])
    assert tuple(f.columns) == fx.OBS_COLUMNS
    assert list(f["value_scaled"]) == [10_123_400, 10_250_000] and f["value_scaled"].dtype == "int64"
    assert list(f["obs_date"]) == [date(2024, 1, 5), date(2024, 1, 8)]


@pytest.mark.parametrize("raw", [
    b"", b"{}", b"[]", b"not json", b'[{"date":"2024-01-05"}]', b'[{"date":"2024-01-05","value":1,"x":1}]',
    b'[{"date":"2024-1-5","value":1}]', b'[{"date":"2024-02-30","value":1}]', b'[{"date":"2024-01-05","value":"1"}]',
    b'[{"date":"2024-01-05","value":1.0000001}]', b'[{"date":"2024-01-05","value":0}]',
    b'[{"date":"2024-01-05","value":-1}]',
    b'[{"date":"2024-01-08","value":1},{"date":"2024-01-05","value":1}]',
    b'[{"date":"2024-01-05","value":1},{"date":"2024-01-05","value":1}]',
])
def test_parse_rejects(raw):
    with pytest.raises(fx.FxDataError):
        fx.parse_swea_observations(raw, AV)


def test_available_at_follows_stockholm_time_including_dst():
    assert AV.available_at(date(2024, 1, 5)) == datetime(2024, 1, 5, 23, tzinfo=UTC)      # CET
    assert AV.available_at(date(2024, 7, 1)) == datetime(2024, 7, 1, 22, tzinfo=UTC)      # CEST


def test_t19_segment_start_and_day_end():
    f = frame([("2024-01-05", "10.5"), ("2024-01-08", "10.6")])
    start = datetime(2024, 1, 8, tzinfo=UTC)                         # VALIDATION-start
    r = fx.value_asof(f, start)
    assert (r.obs_date, r.value_scaled) == (date(2024, 1, 5), 10_500_000)
    d = fx.daily_values(f, [date(2024, 1, 8)], 5)
    assert (d["obs_date"].iat[0], d["value_scaled"].iat[0], d["age_days"].iat[0]) == (date(2024, 1, 8), 10_600_000, 0)
    with pytest.raises(fx.FxDataError):
        fx.value_asof(f, datetime(2024, 1, 5, 22, 59, tzinfo=UTC))   # före första publiceringen


def test_t26_gap_of_5_days_is_filled_6_days_is_long():
    f = frame([("2024-03-01", "10"), ("2024-03-08", "10")])
    days = [date(2024, 3, d) for d in range(1, 9)]
    d = fx.daily_values(f, days, 5).set_index("day")
    assert d.loc[date(2024, 3, 6), "age_days"] == 5 and bool(d.loc[date(2024, 3, 6), "filled"])
    assert not bool(d.loc[date(2024, 3, 6), "gap_long"])
    assert d.loc[date(2024, 3, 7), "age_days"] == 6 and bool(d.loc[date(2024, 3, 7), "gap_long"])
    assert d.loc[date(2024, 3, 8), "age_days"] == 0 and not bool(d.loc[date(2024, 3, 8), "filled"])


def test_tk1_value_asof_is_truncation_invariant():
    f = frame([("2024-01-02", "10.1"), ("2024-01-03", "10.2"), ("2024-01-04", "10.3"), ("2024-01-05", "10.4")])
    grid = pd.date_range("2024-01-02T23:00Z", "2024-01-06T06:00Z", freq="30min")   # från första publiceringen
    for t in grid:
        t = t.to_pydatetime()
        visible = f[f["available_at"] <= pd.Timestamp(t)].reset_index(drop=True)
        assert fx.value_asof(f, t) == fx.value_asof(visible, t)


def test_tk1_negative_control_detects_a_lookahead():
    f = frame([("2024-01-02", "10.1"), ("2024-01-03", "10.2")])

    def cheating(frame_, t):                                         # behandlar obs_date som publiceringstid
        ok = [i for i, d in enumerate(frame_["obs_date"]) if datetime.combine(d, time(0), UTC) <= t]
        return int(frame_["value_scaled"].iat[ok[-1]])

    t = datetime(2024, 1, 3, 12, tzinfo=UTC)
    visible = f[f["available_at"] <= pd.Timestamp(t)].reset_index(drop=True)
    assert cheating(f, t) != fx.value_asof(visible, t).value_scaled   # trunkeringsinvariansen bryts


def test_tk2_tax_rate_is_not_reachable_from_the_decision_path():
    for rel in ("btc_trading_system/benchmark/series.py", "btc_trading_system/benchmark/nav.py",
                "btc_trading_system/benchmark/pins.py", "btc_trading_system/data_validation/benchmark_quality.py"):
        assert "rate_for_tax_date" not in (REPO / rel).read_text(encoding="utf-8")
    f = frame([("2024-01-05", "10.5"), ("2024-01-08", "10.6")])
    trade = datetime(2024, 1, 8, 10, tzinfo=UTC)
    tax = fx.rate_for_tax_date(f, trade.date())
    assert tax.obs_date == date(2024, 1, 8) and tax.available_at > trade          # inte kausal: bara redovisning
    assert fx.value_asof(f, trade).obs_date == date(2024, 1, 5)                    # beslutsvägen
    assert fx.rate_for_tax_date(f, date(2024, 1, 7)).obs_date == date(2024, 1, 5)  # helg: senast före
    with pytest.raises(fx.FxDataError):
        fx.rate_for_tax_date(f, date(2024, 1, 4))


def test_value_asof_requires_utc_and_frame():
    f = frame([("2024-01-05", "10.5")])
    with pytest.raises(ValueError):
        fx.value_asof(f, datetime(2024, 1, 8))  # noqa: DTZ001 - avsiktligt naiv tid
    with pytest.raises(fx.FxDataError):
        fx.value_asof(f.iloc[0:0], datetime(2024, 1, 8, tzinfo=UTC))
