"""Phase 3, del B: kausala anomaliflaggor.

Fönsterlogiken jämförs mot en oberoende referens (ren loop) och kausaliteten
testas både med assert_causal och end-to-end via provider.get_bars(as_of=t).
"""
from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data.provider import LocalOHLCVProvider
from btc_trading_system.data_validation import anomalies as an
from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.data_validation.causality import LookAheadError, assert_causal

FLOAT_COLS = ("log_return", "return_median", "return_scale", "return_z", "range_ratio",
              "range_median", "volume_ratio", "stale_run_length")
FLAG_COLS = ("return_outlier", "range_outlier", "volume_outlier", "stale_run")


def _as_float(flags: pd.DataFrame) -> pd.DataFrame:
    out = {c: flags[c].to_numpy(dtype=float, na_value=np.nan) for c in FLOAT_COLS + FLAG_COLS}
    return pd.DataFrame(out, index=flags.index)


def test_window_params_match_locked_spec(cfg):
    expected = {  # tf: (W, W_bars, m, N_stale)
        "1m": (timedelta(days=7), 10080, 5040, 30),
        "5m": (timedelta(days=7), 2016, 1008, 6),
        "15m": (timedelta(days=7), 672, 336, 3),
        "30m": (timedelta(days=7), 336, 168, 3),
        "1h": (timedelta(days=7), 168, 84, 3),
        "4h": (timedelta(hours=400), 100, 50, 3),
        "1d": (timedelta(days=100), 100, 50, 3),
    }
    assert set(expected) == set(TIMEFRAMES)
    for tf, (w, wb, m, ns) in expected.items():
        p = an.window_params(tf, cfg.anomalies)
        assert (p.window, p.window_bars, p.min_obs, p.stale_bars) == (w, wb, m, ns), tf


def _reference(bars: pd.DataFrame, tf: str, cfg) -> pd.DataFrame:
    """Oberoende referens: explicit loop över [t - W, t), bara giltiga värden."""
    p = an.window_params(tf, cfg.anomalies)
    a = cfg.anomalies
    t_ns = qc.index_ns(bars.index)
    w_ns = int(p.window.total_seconds() * 1e9)
    close = bars["close"].to_numpy(dtype=float)
    gap = bars["gap_before"].to_numpy(dtype=bool)
    r = np.full(len(bars), np.nan)
    for i in range(1, len(bars)):
        if not gap[i]:
            r[i] = np.log(close[i] / close[i - 1])
    ratio = (bars["high"].to_numpy(dtype=float) - bars["low"].to_numpy(dtype=float)) / close
    vol = bars["volume"].to_numpy(dtype=float)
    rows = []
    for i in range(len(bars)):
        inside = (t_ns >= t_ns[i] - w_ns) & (t_ns < t_ns[i])
        rv = r[inside & np.isfinite(r)]
        med = scale = z = np.nan
        if len(rv) >= p.min_obs:
            med = float(np.median(rv))
            q75, q25 = np.quantile(rv, [0.75, 0.25])
            scale = (q75 - q25) / 1.349
            if np.isfinite(r[i]):
                z = abs(r[i] - med) / max(scale, a.return_scale_floor)
        ratio_w = ratio[inside]
        rmed = float(np.median(ratio_w)) if len(ratio_w) >= p.min_obs else np.nan
        vol_w = vol[inside]
        vmed = float(np.median(vol_w)) if len(vol_w) >= p.min_obs else np.nan
        vratio = vol[i] / vmed if np.isfinite(vmed) and vmed > 0 else np.nan
        rows.append((med, scale, z, rmed, vratio))
    return pd.DataFrame(rows, index=bars.index,
                        columns=["return_median", "return_scale", "return_z", "range_median",
                                 "volume_ratio"])


def _with_gap_and_spike(qh, tf="4h", n=320):
    bars = qh.make_bars(tf, n, seed=3).drop(index=qh.make_bars(tf, n, seed=3).index[[150, 151]])
    bars = qh.recompute_gaps(bars, tf)
    t = bars.index[260]
    bars.loc[t, "close"] = bars.loc[t, "close"] * 1.08
    bars.loc[t, "high"] = max(bars.loc[t, "high"], bars.loc[t, "close"])
    return bars


def test_matches_independent_reference(qh, cfg):
    bars = _with_gap_and_spike(qh)
    got = an.compute_anomaly_flags(bars, "4h", cfg.anomalies)
    ref = _reference(bars, "4h", cfg)
    for col in ref.columns:
        a, b = got[col].to_numpy(dtype=float), ref[col].to_numpy(dtype=float)
        assert np.array_equal(np.isnan(a), np.isnan(b)), col
        assert np.allclose(a[~np.isnan(a)], b[~np.isnan(b)], rtol=1e-12, atol=0), col


def test_current_bar_excluded_from_its_own_window(qh, cfg):
    """Mutant closed='both' (inkl. baren själv) måste ge avvikelse mot referensen."""
    bars = _with_gap_and_spike(qh)
    p = an.window_params("4h", cfg.anomalies)
    r = an.log_returns(bars)
    mutant = r.rolling(p.window, closed="both", min_periods=p.min_obs).median()
    ref = _reference(bars, "4h", cfg)["return_median"]
    both = mutant.notna() & ref.notna()
    assert not np.allclose(mutant[both], ref[both])


def test_spike_flagged_warmup_and_gap_unknown(qh, cfg):
    bars = qh.make_bars("15m", 800, seed=1)
    t = bars.index[600]
    bars.loc[t, "close"] = bars.loc[t, "close"] * 1.10
    bars.loc[t, "high"] = max(bars.loc[t, "high"], bars.loc[t, "close"])
    f = an.compute_anomaly_flags(bars, "15m", cfg.anomalies)
    assert bool(f.loc[t, "return_outlier"])
    m = an.window_params("15m", cfg.anomalies).min_obs
    assert f["return_outlier"].iloc[:m].isna().all()          # uppvärmning: okänt, inte False
    gapped = qh.recompute_gaps(bars.drop(index=bars.index[[700]]), "15m")
    g = an.compute_anomaly_flags(gapped, "15m", cfg.anomalies)
    after_gap = gapped.index[gapped["gap_before"]][0]
    assert pd.isna(g.loc[after_gap, "return_outlier"])       # avkastning över lucka bedöms inte


def test_range_and_volume_outliers(qh, cfg):
    bars = qh.make_bars("15m", 800, seed=2)
    t1, t2 = bars.index[500], bars.index[650]
    bars.loc[t1, "high"] = bars.loc[t1, "close"] * 1.5
    bars.loc[t2, "volume"] = bars["volume"].median() * 80
    bars.loc[t2, "quote_volume"] = bars.loc[t2, "volume"] * bars.loc[t2, "close"]
    f = an.compute_anomaly_flags(bars, "15m", cfg.anomalies)
    assert bool(f.loc[t1, "range_outlier"]) and not bool(f.loc[t2, "range_outlier"])
    assert bool(f.loc[t2, "volume_outlier"])
    flagged = f["volume_outlier"].to_numpy(dtype=bool, na_value=False)
    assert flagged.sum() == 1


def test_volume_median_zero_is_unknown(qh, cfg):
    bars = qh.make_bars("4h", 200)
    bars["volume"] = 0.0
    f = an.compute_anomaly_flags(bars, "4h", cfg.anomalies)
    assert f["volume_outlier"].isna().all()


def _freeze(bars, positions):
    out = bars.copy()
    for pos in positions:
        t = out.index[pos]
        price = out["close"].iloc[pos - 1]
        out.loc[t, ["open", "high", "low", "close"]] = price
        out.loc[t, ["volume", "quote_volume", "taker_buy_base_volume",
                    "taker_buy_quote_volume"]] = 0.0
        out.loc[t, "trade_count"] = 0
    return out


def test_stale_run_threshold_counting_so_far(qh, cfg):
    bars = _freeze(qh.make_bars("15m", 60), [10, 11, 12, 30, 31])   # 3 i rad, sedan 2 i rad
    f = an.compute_anomaly_flags(bars, "15m", cfg.anomalies)
    assert list(f["stale_run_length"].iloc[9:14]) == [0, 1, 2, 3, 0]
    assert list(f["stale_run"].iloc[9:14]) == [False, False, False, True, False]
    assert not f["stale_run"].iloc[29:33].any()                     # 2 < N_stale = 3
    runs = an.stale_runs(bars, 3)
    assert runs == [{"start": bars.index[10].isoformat(), "end": bars.index[12].isoformat(),
                     "length": 3}]


def test_stale_run_resets_at_gap(qh, cfg):
    bars = _freeze(qh.make_bars("15m", 60), [10, 11, 13, 14])
    bars = qh.recompute_gaps(bars.drop(index=bars.index[[12]]), "15m")
    f = an.compute_anomaly_flags(bars, "15m", cfg.anomalies)
    assert list(f["stale_run_length"].iloc[10:14]) == [1, 2, 1, 2]
    assert not f["stale_run"].any()


def _dense_cuts(n, events=()):
    """Täta klipp (var 7:e rad) + klipp direkt efter varje händelse: standardklippen
    i assert_causal (4 st) kan missa läckor som bara påverkar sällsynta händelser."""
    return sorted({*range(20, n, 7), *(e + 1 for e in events), n - 1})


def test_flags_are_causal_by_truncation(qh, cfg):
    bars = _freeze(_with_gap_and_spike(qh), [200, 201, 202, 203])
    cuts = _dense_cuts(len(bars), events=(149, 150, 200, 203, 260))
    assert_causal(lambda d: _as_float(an.compute_anomaly_flags(d, "4h", cfg.anomalies)), bars,
                  cut_points=cuts)


def test_noncausal_negative_control_is_detected(qh, cfg):
    """En detektor som kräver nästa bar (spik som vänder) MÅSTE underkännas."""
    bars = _with_gap_and_spike(qh)

    def spike_and_revert(d):
        r = an.log_returns(d)
        return ((r.abs() > 0.05) & (r.shift(-1) * r < 0)).astype(float)

    with pytest.raises(LookAheadError):
        assert_causal(spike_and_revert, bars, cut_points=_dense_cuts(len(bars), events=(260,)))

    def centered_median(d):
        return an.log_returns(d).rolling(21, center=True, min_periods=5).median()

    with pytest.raises(LookAheadError):
        assert_causal(centered_median, bars, cut_points=_dense_cuts(len(bars)))


def test_flags_identical_via_provider_as_of(qh, cfg, tmp_path):
    bars = _freeze(_with_gap_and_spike(qh), [200, 201, 202, 203])
    store, _, _ = qh.put_dataset(tmp_path / "s", bars, "spot", "4h")
    prov = LocalOHLCVProvider(store, "spot", require_as_of=True)
    end = bars.index[-1].to_pydatetime() + timedelta(days=1)
    start = bars.index[0].to_pydatetime()
    full = _as_float(an.compute_anomaly_flags(
        prov.get_bars("BTCUSDT", "4h", start, end, as_of=end), "4h", cfg.anomalies))
    step = TIMEFRAMES["4h"]
    for pos in (60, 149, 150, 203, 261, len(bars) - 1):
        t = bars.index[pos].to_pydatetime() + step          # bar pos har precis stängt
        seen = prov.get_bars("BTCUSDT", "4h", start, end, as_of=t)
        got = _as_float(an.compute_anomaly_flags(seen, "4h", cfg.anomalies))
        expect = full.loc[got.index]
        assert got.index[-1] == bars.index[pos]
        assert np.allclose(got.to_numpy(), expect.to_numpy(), equal_nan=True, rtol=0, atol=0), pos


def test_anomaly_checks_status_and_counts(qh, cfg):
    clean = qh.make_bars("15m", 800, seed=5)
    res = {r.name: r for r in an.anomaly_checks(clean, "15m", cfg.anomalies)}
    assert set(res) == {"return_outlier", "range_outlier", "volume_outlier", "stale_run"}
    d = res["return_outlier"].details
    assert d["bars_evaluable"] + d["bars_unknown"] == d["bars_total"] == 800
    short = an.anomaly_checks(qh.make_bars("15m", 100), "15m", cfg.anomalies)
    assert {r.name: r.status for r in short}["return_outlier"] is qc.Status.UNKNOWN
    assert all(r.causal for r in short)


def test_rejects_unsorted_or_duplicate_index(qh, cfg):
    bars = qh.make_bars("15m", 50)
    with pytest.raises(ValueError):
        an.compute_anomaly_flags(bars.iloc[::-1], "15m", cfg.anomalies)
    with pytest.raises(ValueError):
        an.compute_anomaly_flags(pd.concat([bars, bars.iloc[[3]]]).sort_index(), "15m",
                                 cfg.anomalies)
