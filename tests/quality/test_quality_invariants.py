"""Phase 3, del A: invarianter per bar."""
from btc_trading_system.data_validation import invariants as inv
from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.data_validation.causality import assert_causal


def _one(bars, cfg, code):
    res = inv.check_invariants(bars, cfg.invariants)
    return res, res.details["violations"][code]


def test_clean_data_passes(qh, cfg):
    res = inv.check_invariants(qh.make_bars("15m", 500), cfg.invariants)
    assert res.status is qc.Status.PASS and res.findings == ()
    assert set(res.details["violations"].values()) == {0}


def test_accepts_store_format_with_open_time_column(qh, cfg):
    res = inv.check_invariants(qh.make_bars("15m", 50).reset_index(), cfg.invariants)
    assert res.status is qc.Status.PASS


def test_empty_is_unknown(qh, cfg):
    res = inv.check_invariants(qh.make_bars("15m", 5).iloc[0:0], cfg.invariants)
    assert res.status is qc.Status.UNKNOWN and res.reason


def test_vwap_outside_range_and_tolerance_edges(qh, cfg):
    tol = cfg.invariants.vwap_rel_tol
    bars = qh.make_bars("15m", 20)
    t = bars.index[5]
    vol = bars.loc[t, "volume"]
    bars.loc[t, "quote_volume"] = bars.loc[t, "high"] * (1 + 0.5 * tol) * vol    # inom tolerans
    assert _one(bars, cfg, "VWAP_OUTSIDE_RANGE")[1] == 0
    bars.loc[t, "quote_volume"] = bars.loc[t, "high"] * (1 + 2 * tol) * vol      # utanför
    res, n = _one(bars, cfg, "VWAP_OUTSIDE_RANGE")
    assert n == 1 and res.status is qc.Status.FAIL
    assert res.findings[0].examples == (t.isoformat(),)
    bars = qh.make_bars("15m", 20)
    bars.loc[t, "quote_volume"] = bars.loc[t, "low"] * (1 - 2 * tol) * vol       # under low
    assert _one(bars, cfg, "VWAP_OUTSIDE_RANGE")[1] == 1


def test_taker_base_and_quote(qh, cfg):
    bars = qh.make_bars("15m", 20)
    t = bars.index[3]
    bars.loc[t, "taker_buy_base_volume"] = bars.loc[t, "volume"] + 5e-9   # inom abs-tolerans 1e-8
    assert _one(bars, cfg, "TAKER_BASE_EXCEEDS_VOLUME")[1] == 0
    bars.loc[t, "taker_buy_base_volume"] = bars.loc[t, "volume"] + 1e-6
    assert _one(bars, cfg, "TAKER_BASE_EXCEEDS_VOLUME")[1] == 1
    bars = qh.make_bars("15m", 20)
    bars.loc[t, "taker_buy_quote_volume"] = bars.loc[t, "quote_volume"] * 1.01
    assert _one(bars, cfg, "TAKER_QUOTE_EXCEEDS_QUOTE")[1] == 1


def test_volume_and_trades_consistency(qh, cfg):
    bars = qh.make_bars("15m", 20)
    bars.loc[bars.index[2], "trade_count"] = 0
    assert _one(bars, cfg, "VOLUME_WITHOUT_TRADES")[1] == 1
    bars = qh.make_bars("15m", 20)
    t = bars.index[4]
    bars.loc[t, ["volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]] = 0.0
    bars.loc[t, ["high", "low"]] = bars.loc[t, "open"]
    bars.loc[t, "close"] = bars.loc[t, "open"]
    assert _one(bars, cfg, "TRADES_WITHOUT_VOLUME")[1] == 1        # trade_count > 0 kvar


def test_price_move_without_volume_vs_flat_zero_bar(qh, cfg):
    bars = qh.make_bars("15m", 20)
    t = bars.index[6]
    cols = ["volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]
    bars.loc[t, cols] = 0.0
    bars.loc[t, "trade_count"] = 0
    flat = bars.copy()
    flat.loc[t, ["open", "high", "low", "close"]] = flat.loc[t, "open"]
    res = inv.check_invariants(flat, cfg.invariants)
    assert res.status is qc.Status.PASS                              # platt nollbar är inget brott
    res, n = _one(bars, cfg, "PRICE_MOVE_WITHOUT_VOLUME")
    assert n == 1 and res.status is qc.Status.FAIL


def test_each_code_isolated(qh, cfg):
    """Varje manipulation ska trigga exakt sin egen kod och ingen annan."""
    base = qh.make_bars("15m", 30)
    t = base.index[10]
    cases = {
        "VWAP_OUTSIDE_RANGE": ("quote_volume", base.loc[t, "quote_volume"] * 3.0),
        "TAKER_BASE_EXCEEDS_VOLUME": ("taker_buy_base_volume", base.loc[t, "volume"] + 1.0),
        "TAKER_QUOTE_EXCEEDS_QUOTE": ("taker_buy_quote_volume", base.loc[t, "quote_volume"] + 1.0),
        "VOLUME_WITHOUT_TRADES": ("trade_count", 0),
    }
    for code, (column, value) in cases.items():
        bars = base.copy()
        bars.loc[t, column] = value
        violations = inv.check_invariants(bars, cfg.invariants).details["violations"]
        assert violations[code] == 1, (code, violations)
        assert sum(violations.values()) == 1, (code, violations)


def test_invariant_flags_are_causal(qh, cfg):
    bars = qh.make_bars("15m", 200)
    bars.loc[bars.index[120], "trade_count"] = 0
    assert_causal(lambda d: inv.invariant_flags(d, cfg.invariants).astype(float), bars)
