from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from btc_trading_system.core.interfaces import DataProvider
from btc_trading_system.core.types import TIMEFRAMES, Bar
from btc_trading_system.data.provider import LocalOHLCVProvider, frame_to_bars
from btc_trading_system.data_validation.errors import DatasetNotFoundError
from data_helpers import START, build_store

FAR = START + timedelta(days=4000)


def _prov(tmp_path, tf="15m", n=200, instrument="spot", **kw):
    return LocalOHLCVProvider(build_store(tmp_path, instrument, tf, n), instrument, **kw)


def test_implements_phase1_dataprovider_interface(tmp_path):
    p = _prov(tmp_path)
    assert isinstance(p, DataProvider)
    df = p.get_bars("BTC/USDT", "15m", START, FAR)             # Phase 1-signatur, positionella argument
    assert len(df) == 200


def test_output_shape_and_types(tmp_path):
    df = _prov(tmp_path).get_bars("BTCUSDT", "15m", START, FAR)
    assert df.index.name == "open_time" and str(df.index.tz) == "UTC"
    assert df.index.is_monotonic_increasing and df.index.is_unique
    assert (df["available_at"] == df.index + timedelta(minutes=15)).all()
    for c in ("open", "high", "low", "close", "volume", "quote_volume", "trade_count",
              "taker_buy_base_volume", "taker_buy_quote_volume", "gap_before", "available_at"):
        assert c in df.columns


def test_range_is_start_inclusive_end_exclusive(tmp_path):
    p = _prov(tmp_path)
    s, e = START + timedelta(minutes=15 * 10), START + timedelta(minutes=15 * 20)
    df = p.get_bars("BTCUSDT", "15m", s, e)
    assert len(df) == 10 and df.index[0] == pd.Timestamp(s) and df.index[-1] == pd.Timestamp(e - timedelta(minutes=15))
    assert len(p.get_bars("BTCUSDT", "15m", s, s)) == 0


def test_as_of_excludes_incomplete_bar_exact_boundaries(tmp_path):
    p = _prov(tmp_path)
    k = 50
    avail_k = START + timedelta(minutes=15 * (k + 1))          # bar k avslutas här
    at = p.get_bars("BTCUSDT", "15m", START, FAR, as_of=avail_k)
    before = p.get_bars("BTCUSDT", "15m", START, FAR, as_of=avail_k - timedelta(microseconds=1))
    assert at.index[-1] == pd.Timestamp(START + timedelta(minutes=15 * k))          # inkluderad exakt vid available_at
    assert before.index[-1] == pd.Timestamp(START + timedelta(minutes=15 * (k - 1)))  # 1 µs tidigare: exkluderad
    assert len(at) == k + 1 and len(before) == k


def test_as_of_before_first_bar_returns_empty(tmp_path):
    p = _prov(tmp_path)
    assert len(p.get_bars("BTCUSDT", "15m", START, FAR, as_of=START + timedelta(minutes=14))) == 0
    assert len(p.get_bars("BTCUSDT", "15m", START, FAR, as_of=START - timedelta(days=1))) == 0


def test_as_of_never_leaks_for_any_timeframe_bruteforce(tmp_path):
    rng = np.random.default_rng(7)
    for tf, step in TIMEFRAMES.items():
        p = _prov(tmp_path, tf=tf, n=120)
        full = p.get_bars("BTCUSDT", tf, START, FAR)
        span = step * 120
        cands = [START + timedelta(seconds=float(rng.uniform(-3, 123)) * step.total_seconds()) for _ in range(40)]
        cands += [START + step * i for i in range(0, 121, 7)] + [START + step * i - timedelta(microseconds=1) for i in (5, 60)]
        for t in cands:
            got = p.get_bars("BTCUSDT", tf, START, FAR, as_of=t)
            expect = full[full["available_at"] <= pd.Timestamp(t)]
            pd.testing.assert_frame_equal(got, expect)
            assert (got["available_at"] <= pd.Timestamp(t)).all()


def test_decision_loop_can_never_see_the_bar_it_decides_on(tmp_path):
    """Simulerar backtest: vid varje beslutstid t ser man bara barer som är avslutade."""
    p = _prov(tmp_path, n=100)
    for i in range(1, 100):
        t = START + timedelta(minutes=15 * i)                  # tidpunkt då bar i-1 just avslutats
        seen = p.get_bars("BTCUSDT", "15m", START, FAR, as_of=t)
        assert seen.index[-1] == pd.Timestamp(START + timedelta(minutes=15 * (i - 1)))
        assert seen["available_at"].max() <= pd.Timestamp(t)


def test_causality_invariance_future_data_cannot_change_past_result(tmp_path):
    """Samma as_of ska ge IDENTISKT svar oavsett om framtida data finns i datasetet."""
    n_full, m = 300, 180
    full_store = build_store(tmp_path / "full", "spot", "15m", n_full, seed=5)
    trunc_store = build_store(tmp_path / "trunc", "spot", "15m", n_full, seed=5, head=m)   # äkta prefix
    pf, pt = LocalOHLCVProvider(full_store, "spot"), LocalOHLCVProvider(trunc_store, "spot")
    for k in (10, 60, 120, 179):
        t = START + timedelta(minutes=15 * (k + 1))
        a = pf.get_bars("BTCUSDT", "15m", START, FAR, as_of=t)
        b = pt.get_bars("BTCUSDT", "15m", START, FAR, as_of=t)
        pd.testing.assert_frame_equal(a, b)


def test_causality_invariance_with_gaps(tmp_path):
    full_store = build_store(tmp_path / "f", "perp", "15m", 200, seed=3, drop=[50, 51, 120])
    p = LocalOHLCVProvider(full_store, "perp")
    t = START + timedelta(minutes=15 * 100)
    seen = p.get_bars("BTCUSDT", "15m", START, FAR, as_of=t)
    late = p.get_bars("BTCUSDT", "15m", START, FAR, as_of=START + timedelta(minutes=15 * 400))
    pd.testing.assert_frame_equal(seen, late.loc[seen.index])           # historiken ändras inte av senare data
    assert bool(seen["gap_before"].sum() == 1)                           # endast luckan som redan hänt syns


def test_gap_before_visible_and_correct_in_slices(tmp_path):
    p = _prov_with_gaps(tmp_path)
    df = p.get_bars("BTCUSDT", "15m", START, FAR)
    flagged = df.index[df["gap_before"]]
    assert len(flagged) == 2
    sl = p.get_bars("BTCUSDT", "15m", flagged[0], flagged[0] + timedelta(minutes=45))
    assert bool(sl["gap_before"].iloc[0])                                # flaggan följer med även när skivan börjar på baren efter luckan


def _prov_with_gaps(tmp_path):
    st = build_store(tmp_path, "spot", "15m", 200, drop=[40, 41, 100])
    return LocalOHLCVProvider(st, "spot")


def test_get_gaps_respects_as_of(tmp_path):
    p = _prov_with_gaps(tmp_path)
    all_gaps = p.get_gaps("BTCUSDT", "15m")
    assert list(all_gaps["missing_bars"]) == [2, 1]
    early = p.get_gaps("BTCUSDT", "15m", as_of=START + timedelta(minutes=15 * 50))
    assert list(early["missing_bars"]) == [2]                            # den senare luckan är okänd vid t
    none = p.get_gaps("BTCUSDT", "15m", as_of=START + timedelta(minutes=15 * 30))
    assert len(none) == 0
    # luckan blir känd först när bar efter luckan är avslutad
    resume = all_gaps["resumes_at"].iloc[0]
    assert len(p.get_gaps("BTCUSDT", "15m", as_of=(resume + timedelta(minutes=15) - timedelta(microseconds=1)).to_pydatetime())) == 0
    assert len(p.get_gaps("BTCUSDT", "15m", as_of=(resume + timedelta(minutes=15)).to_pydatetime())) == 1


def test_timezone_violations_rejected(tmp_path):
    p = _prov(tmp_path)
    naive = datetime(2024, 1, 1)
    with pytest.raises(ValueError):
        p.get_bars("BTCUSDT", "15m", naive, FAR)
    with pytest.raises(ValueError):
        p.get_bars("BTCUSDT", "15m", START, naive)
    with pytest.raises(ValueError):
        p.get_bars("BTCUSDT", "15m", START, FAR, as_of=naive)
    with pytest.raises(ValueError):
        p.get_bars("BTCUSDT", "15m", START, FAR, as_of=datetime(2024, 1, 2, tzinfo=timezone(timedelta(hours=2))))
    with pytest.raises(ValueError):
        p.get_gaps("BTCUSDT", "15m", as_of=naive)


def test_end_before_start_rejected(tmp_path):
    with pytest.raises(ValueError):
        _prov(tmp_path).get_bars("BTCUSDT", "15m", FAR, START)


def test_require_as_of_mode(tmp_path):
    p = _prov(tmp_path, require_as_of=True)
    with pytest.raises(ValueError):
        p.get_bars("BTCUSDT", "15m", START, FAR)
    assert len(p.get_bars("BTCUSDT", "15m", START, FAR, as_of=FAR)) == 200


def test_unknown_dataset_and_bad_arguments(tmp_path):
    p = _prov(tmp_path)
    with pytest.raises(DatasetNotFoundError):
        p.get_bars("BTCUSDT", "1h", START, FAR)
    with pytest.raises(DatasetNotFoundError):
        p.get_bars("ETHUSDT", "15m", START, FAR)
    with pytest.raises(ValueError):
        p.get_bars("BTCUSDT", "7m", START, FAR)


def test_spot_and_perp_are_isolated(tmp_path):
    spot = LocalOHLCVProvider(build_store(tmp_path, "spot", "15m", 60, seed=1), "spot")
    perp = LocalOHLCVProvider(build_store(tmp_path, "perp", "15m", 60, seed=2), "perp")
    a = spot.get_bars("BTCUSDT", "15m", START, FAR)
    b = perp.get_bars("BTCUSDT", "15m", START, FAR)
    assert not a["close"].equals(b["close"])
    with pytest.raises(DatasetNotFoundError):                            # spot-store har inget perp
        LocalOHLCVProvider(spot.store, "perp").get_bars("BTCUSDT", "15m", START, FAR)


def test_returned_frame_is_a_copy(tmp_path):
    p = _prov(tmp_path)
    a = p.get_bars("BTCUSDT", "15m", START, FAR)
    a.iloc[:, a.columns.get_loc("close")] = -1.0
    b = p.get_bars("BTCUSDT", "15m", START, FAR)
    assert (b["close"] > 0).all()


def test_frame_to_bars_produces_valid_phase1_bars(tmp_path):
    p = _prov(tmp_path, n=50)
    t = START + timedelta(minutes=15 * 20)
    df = p.get_bars("BTCUSDT", "15m", START, FAR, as_of=t)
    bars = frame_to_bars(df, "BTCUSDT", "15m")
    assert len(bars) == 20 and all(isinstance(b, Bar) for b in bars)
    assert all(b.available_at <= t for b in bars)
    assert bars[0].open_time == START


def test_provider_detects_tampered_store(tmp_path):
    st = build_store(tmp_path, "spot", "15m", 50, seed=1)
    other = build_store(tmp_path / "o", "spot", "15m", 50, seed=2)
    from btc_trading_system.data.schema import DatasetKey
    k = DatasetKey("spot", "BTCUSDT", "15m")
    st.data_path(k).write_bytes(other.data_path(k).read_bytes())
    from btc_trading_system.data_validation.errors import DataIntegrityError
    with pytest.raises(DataIntegrityError):
        LocalOHLCVProvider(st, "spot").get_bars("BTCUSDT", "15m", START, FAR)
