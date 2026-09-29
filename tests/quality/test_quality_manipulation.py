"""Phase 3: manipulerade dataset ska fångas av RÄTT kontroll med rätt allvarlighetsgrad.

Tabellen i den låsta specifikationen (G) testas rad för rad, plus kontrollfall som
INTE ska ge FAIL och en dokumenterad begränsning (omkastade spot/perp-mappar).
"""
import numpy as np

from btc_trading_system.data_validation import quality_report as qr

N = 800


def _report(qh, cfg, tmp_path, bars, tf="15m", instrument="spot", name="s"):
    store, key, _ = qh.put_dataset(tmp_path / name, bars, instrument, tf)
    frame, manifest = store.read(key)
    return qr.build_dataset_report(frame, key, manifest, cfg, "test", True)


def _pair_report(qh, cfg, tmp_path, spot, perp, tf="15m"):
    s_store, s_key, _ = qh.put_dataset(tmp_path / "sp", spot, "spot", tf)
    p_store, p_key, _ = qh.put_dataset(tmp_path / "pp", perp, "perp", tf)
    s_frame, s_man = s_store.read(s_key)
    p_frame, p_man = p_store.read(p_key)
    return qr.build_xinst_report(s_frame, s_key, s_man, p_frame, p_key, p_man, cfg, "t", True)


def _tf_report(qh, cfg, tmp_path, lower, higher, instrument="spot"):
    store, lo_key, _ = qh.put_dataset(tmp_path / "tf", lower, instrument, "1m")
    _, hi_key, _ = qh.put_dataset(tmp_path / "tf", higher, instrument, "15m")
    lo, lo_m = store.read(lo_key)
    hi, hi_m = store.read(hi_key)
    return qr.build_tf_report(lo, lo_key, lo_m, hi, hi_key, hi_m, cfg, "t", True)


def _status(report):
    return {c["name"]: c["status"] for c in report["checks"]}


def _codes(report, check):
    found = next(c for c in report["checks"] if c["name"] == check)
    return {(f["code"], f["severity"]) for f in found["findings"]}


def _perp_like(spot, rng_seed=5, noise=0.0002):
    rng = np.random.default_rng(rng_seed)
    perp = spot.copy()
    factor = np.exp(rng.normal(0.0, noise, len(spot))) * 1.0003
    for col in ("open", "high", "low", "close"):
        perp[col] = spot[col] * factor
    perp["high"] = perp[["open", "high", "close"]].max(axis=1)
    perp["low"] = perp[["open", "low", "close"]].min(axis=1)
    mid = (perp["high"] + perp["low"]) / 2
    perp["quote_volume"] = mid * perp["volume"]
    perp["taker_buy_quote_volume"] = mid * perp["taker_buy_base_volume"]
    return perp


# ------------------------------------------------------------------ kontrollfall
def test_clean_dataset_is_pass(qh, cfg, tmp_path):
    report = _report(qh, cfg, tmp_path, qh.make_bars("15m", N, seed=21))
    assert report["overall"] == "PASS", _status(report)


def test_genuine_continuing_crash_is_warn_not_fail(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", N, seed=22)
    for pos in range(600, 606):                               # sex barer, -3 % var, fortsätter nedåt
        t, prev = bars.index[pos], bars.index[pos - 1]
        bars.loc[t, "open"] = bars.loc[prev, "close"]
        bars.loc[t, "close"] = bars.loc[t, "open"] * 0.97
        bars.loc[t, "high"] = bars.loc[t, "open"] * 1.001
        bars.loc[t, "low"] = bars.loc[t, "close"] * 0.999
        mid = (bars.loc[t, "high"] + bars.loc[t, "low"]) / 2
        bars.loc[t, "quote_volume"] = mid * bars.loc[t, "volume"]
        bars.loc[t, "taker_buy_quote_volume"] = mid * bars.loc[t, "taker_buy_base_volume"]
    tail = bars.index[606:]
    bars.loc[tail, ["open", "high", "low", "close", "quote_volume", "taker_buy_quote_volume"]] *= (
        bars.loc[bars.index[605], "close"] / bars.loc[bars.index[606], "open"]
    )
    report = _report(qh, cfg, tmp_path, bars)
    status = _status(report)
    assert status["invariants"] == "PASS" and status["return_outlier"] == "WARN"
    assert report["overall"] == "WARN"


# ------------------------------------------------------------------ invarianter (FAIL)
def test_tampered_quote_volume(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", N)
    bars.loc[bars.index[300], "quote_volume"] *= 1.5
    report = _report(qh, cfg, tmp_path, bars)
    assert _codes(report, "invariants") == {("VWAP_OUTSIDE_RANGE", "FAIL")}
    assert report["overall"] == "FAIL"


def test_taker_exceeds_total(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", N)
    t = bars.index[310]
    bars.loc[t, "taker_buy_base_volume"] = bars.loc[t, "volume"] * 1.2
    bars.loc[t, "taker_buy_quote_volume"] = bars.loc[t, "quote_volume"] * 1.2
    assert _codes(_report(qh, cfg, tmp_path, bars), "invariants") == {
        ("TAKER_BASE_EXCEEDS_VOLUME", "FAIL"), ("TAKER_QUOTE_EXCEEDS_QUOTE", "FAIL")}


def test_volume_without_trades_and_trades_without_volume(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", N)
    bars.loc[bars.index[320], "trade_count"] = 0
    t = bars.index[330]
    bars.loc[t, ["volume", "quote_volume", "taker_buy_base_volume",
                 "taker_buy_quote_volume"]] = 0.0
    bars.loc[t, ["open", "high", "low", "close"]] = bars.loc[t, "open"]
    assert _codes(_report(qh, cfg, tmp_path, bars), "invariants") == {
        ("VOLUME_WITHOUT_TRADES", "FAIL"), ("TRADES_WITHOUT_VOLUME", "FAIL")}


def test_price_move_without_volume(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", N)
    t = bars.index[340]
    bars.loc[t, ["volume", "quote_volume", "taker_buy_base_volume",
                 "taker_buy_quote_volume"]] = 0.0
    bars.loc[t, "trade_count"] = 0
    assert _codes(_report(qh, cfg, tmp_path, bars), "invariants") == {
        ("PRICE_MOVE_WITHOUT_VOLUME", "FAIL")}


# ------------------------------------------------------------------ anomalier (WARN)
def test_fat_finger_high(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", N)
    t = bars.index[500]
    bars.loc[t, "high"] = bars.loc[t, "close"] * 10
    report = _report(qh, cfg, tmp_path, bars)
    assert _codes(report, "range_outlier") == {("RANGE_OUTLIER", "WARN")}
    assert _status(report)["invariants"] == "PASS"


def test_close_spike_and_revert(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", N)
    t = bars.index[520]
    bars.loc[t, "close"] *= 1.15
    bars.loc[t, "high"] = max(bars.loc[t, "high"], bars.loc[t, "close"])
    report = _report(qh, cfg, tmp_path, bars)
    finding = next(c for c in report["checks"] if c["name"] == "return_outlier")["findings"][0]
    assert finding["code"] == "RETURN_OUTLIER" and t.isoformat() in finding["examples"]


def test_frozen_feed(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", N)
    for pos in range(540, 546):
        t = bars.index[pos]
        price = bars.loc[bars.index[539], "close"]
        bars.loc[t, ["open", "high", "low", "close"]] = price
        bars.loc[t, ["volume", "quote_volume", "taker_buy_base_volume",
                     "taker_buy_quote_volume"]] = 0.0
        bars.loc[t, "trade_count"] = 0
    report = _report(qh, cfg, tmp_path, bars)
    assert _codes(report, "stale_run") == {("STALE_RUN", "WARN")}
    stale = next(c for c in report["checks"] if c["name"] == "stale_run")
    assert stale["details"]["runs_examples_audit"][0]["length"] == 6


# ------------------------------------------------------------------ konsistens
def test_month_in_wrong_scale(qh, cfg, tmp_path):
    spot = qh.make_bars("15m", N, seed=23)
    perp = _perp_like(spot)
    scaled = spot.copy()
    cols = ["open", "high", "low", "close", "quote_volume", "taker_buy_quote_volume"]
    scaled.loc[scaled.index[400:], cols] *= 100
    report = _report(qh, cfg, tmp_path, scaled)
    finding = next(c for c in report["checks"] if c["name"] == "return_outlier")["findings"][0]
    assert scaled.index[400].isoformat() in finding["examples"]
    pair = _pair_report(qh, cfg, tmp_path, scaled, perp)
    assert ("XINST_BASIS", "FAIL") in _codes(pair, "xinst_basis") and pair["overall"] == "FAIL"


def test_eth_prices_under_btc_name(qh, cfg, tmp_path):
    spot = qh.make_bars("15m", N, seed=24)
    eth = qh.make_bars("15m", N, seed=25, price=3000.0)
    pair = _pair_report(qh, cfg, tmp_path, spot, eth)
    assert _status(pair)["xinst_basis"] == "FAIL"


def test_shifted_file_tf_fail_and_lag_warn(qh, cfg, tmp_path):
    lower = qh.make_bars("1m", 15 * 60, seed=26)
    higher = qh.aggregate(lower, "1m", "15m")
    shifted = higher.shift(1).iloc[1:].copy()                  # 15m-värden en bar för sent
    shifted["trade_count"] = shifted["trade_count"].astype(np.int64)
    shifted["gap_before"] = False
    report = _tf_report(qh, cfg, tmp_path, lower, shifted)
    assert _codes(report, "tf_aggregation") >= {("TF_AGGREGATION_MISMATCH", "FAIL")}
    spot = qh.make_bars("15m", N, seed=27)
    late = _perp_like(spot).shift(1).iloc[1:].copy()
    late["trade_count"] = late["trade_count"].astype(np.int64)
    late["gap_before"] = False
    pair = _pair_report(qh, cfg, tmp_path, spot, late)
    lag = next(c for c in pair["checks"] if c["name"] == "xinst_lag")
    assert lag["status"] == "WARN" and lag["details"]["best_lag"] != 0
    assert _status(pair)["xinst_lag"] != "FAIL"


def test_15m_inconsistent_with_1m(qh, cfg, tmp_path):
    lower = qh.make_bars("1m", 15 * 60, seed=28)
    higher = qh.aggregate(lower, "1m", "15m")
    higher.loc[higher.index[30], "volume"] *= 1.01
    report = _tf_report(qh, cfg, tmp_path, lower, higher)
    check = report["checks"][0]
    assert check["status"] == "FAIL" and check["details"]["mismatches_by_field"]["volume"] == 1


def test_replayed_day(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", 96 * 8, seed=29)
    cols = ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base_volume",
            "taker_buy_quote_volume", "trade_count"]
    positions = [bars.columns.get_loc(c) for c in cols]
    bars.iloc[96 * 6 : 96 * 7, positions] = bars.iloc[96 * 2 : 96 * 3][cols].to_numpy()
    bars["trade_count"] = bars["trade_count"].astype(np.int64)
    report = _report(qh, cfg, tmp_path, bars)
    assert _codes(report, "replayed_block") == {("REPLAYED_BLOCK", "FAIL")}


# ------------------------------------------------------------------ känd begränsning
def test_known_limitation_swapped_spot_perp_not_detected(qh, cfg, tmp_path):
    """Dokumenterat: omkastade spot/perp-mappar kan inte upptäckas robust."""
    spot = qh.make_bars("15m", N, seed=30)
    perp = _perp_like(spot)
    swapped = _pair_report(qh, cfg, tmp_path, perp, spot)
    assert swapped["overall"] in ("PASS", "WARN")
