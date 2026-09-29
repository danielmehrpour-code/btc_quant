"""Phase 3, del C: aggregering mellan timeframes, spot mot perp, upprepade block."""
import hashlib
import json
import struct
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

from btc_trading_system.data_validation import consistency as cs
from btc_trading_system.data_validation import quality_config as qc

GOLDEN_BLOCK_SHA256 = "472cdb49f21e9d8d3c753f9cfa9f9d2ec722641b165cba19cd93118bdf1d937b"


# ------------------------------------------------------------------ C1
def _pair(qh, n_lower=15 * 40, seed=0):
    lower = qh.make_bars("1m", n_lower, seed=seed)
    return lower, qh.aggregate(lower, "1m", "15m")


def test_c1_exact_match_passes(qh, cfg):
    lower, higher = _pair(qh)
    res = cs.tf_aggregation_check(lower, "1m", higher, "15m", cfg.consistency)
    d = res.details
    assert res.status is qc.Status.PASS and not res.causal
    assert (d["windows_total"], d["windows_checked"], d["windows_incomplete"]) == (40, 40, 0)
    assert set(d["mismatches_by_field"]) == set(qh.FIELDS)


def test_c1_every_field_is_checked(qh, cfg):
    lower, higher = _pair(qh)
    t = higher.index[7]
    for field in qh.FIELDS:
        bad = higher.copy()
        if field == "trade_count":
            bad.loc[t, field] = bad.loc[t, field] + 1
        else:
            bad.loc[t, field] = bad.loc[t, field] * 1.001
        res = cs.tf_aggregation_check(lower, "1m", bad, "15m", cfg.consistency)
        by_field = res.details["mismatches_by_field"]
        assert res.status is qc.Status.FAIL, field
        assert by_field[field] == 1 and sum(by_field.values()) == 1, (field, by_field)
        assert res.findings[0].examples == (t.isoformat(),)


def test_c1_prices_compared_exactly_one_ulp(qh, cfg):
    """O/H/L/C ska vara exakt lika: en ändring på en ULP måste ge FAIL."""
    lower, higher = _pair(qh)
    t = higher.index[11]
    for field in ("open", "high", "low", "close"):
        bad = higher.copy()
        bad.loc[t, field] = np.nextafter(bad.loc[t, field], np.inf)
        res = cs.tf_aggregation_check(lower, "1m", bad, "15m", cfg.consistency)
        assert res.status is qc.Status.FAIL, field
        assert res.details["mismatches_by_field"][field] == 1, field


def test_c1_sum_tolerance_edges(qh, cfg):
    lower, higher = _pair(qh)
    t = higher.index[3]
    x = higher.loc[t, "volume"]
    limit = cfg.consistency.agg_abs_tol + cfg.consistency.agg_rel_tol * abs(x)
    ok, bad = higher.copy(), higher.copy()
    ok.loc[t, "volume"] = x + 0.5 * limit
    bad.loc[t, "volume"] = x + 3.0 * limit
    assert cs.tf_aggregation_check(lower, "1m", ok, "15m", cfg.consistency).status is qc.Status.PASS
    assert cs.tf_aggregation_check(lower, "1m", bad, "15m", cfg.consistency).status is qc.Status.FAIL


def test_c1_missing_lower_bar_is_incomplete_not_mismatch(qh, cfg):
    lower, higher = _pair(qh)
    res = cs.tf_aggregation_check(lower.drop(index=lower.index[[20]]), "1m", higher, "15m",
                                  cfg.consistency)
    d = res.details
    assert (d["windows_incomplete"], d["windows_checked"], d["windows_mismatched"]) == (1, 39, 0)
    assert res.status is qc.Status.PASS
    info = [f for f in res.findings if f.code == "TF_INCOMPLETE"]
    assert info and info[0].severity is qc.Severity.INFO and info[0].count == 1


def test_c1_bypassing_phase2_right_count_wrong_timestamps(qh, cfg):
    """k barer men fel tidsstämplar (dubblett / utanför rutnät) -> ofullständigt."""
    lower, higher = _pair(qh)
    w = higher.index[5]
    in_w = (lower.index >= w) & (lower.index < w + pd.Timedelta(minutes=15))
    positions = np.flatnonzero(in_w)
    dup = lower.copy()
    idx = dup.index.to_numpy().copy()
    idx[positions[6]] = idx[positions[5]]                       # dubblett + ett hål, 15 st kvar
    dup.index = pd.DatetimeIndex(idx, name="open_time")
    res = cs.tf_aggregation_check(dup, "1m", higher, "15m", cfg.consistency)
    assert res.details["windows_incomplete"] == 1 and res.details["windows_mismatched"] == 0
    off = lower.copy()
    idx = off.index.to_numpy().copy()
    idx[positions[3]] = idx[positions[3]] + np.timedelta64(30, "s")   # utanför 1m-rutnätet
    off.index = pd.DatetimeIndex(idx, name="open_time")
    res = cs.tf_aggregation_check(off, "1m", higher, "15m", cfg.consistency)
    assert res.details["windows_incomplete"] == 1 and res.details["windows_mismatched"] == 0


def test_c1_higher_missing_warn_and_unknown(qh, cfg):
    lower, higher = _pair(qh)
    res = cs.tf_aggregation_check(lower, "1m", higher.drop(index=higher.index[[9]]), "15m",
                                  cfg.consistency)
    assert res.status is qc.Status.WARN and res.details["windows_higher_missing"] == 1
    short = lower.iloc[3:14]                                   # ryms inte i ett helt fönster
    res = cs.tf_aggregation_check(short, "1m", higher, "15m", cfg.consistency)
    assert res.status is qc.Status.UNKNOWN and res.reason
    with pytest.raises(ValueError):
        cs.tf_aggregation_check(lower, "15m", higher, "1m", cfg.consistency)


# ------------------------------------------------------------------ C2
def _spot_perp(qh, n=800, seed=4, noise=0.0002):
    spot = qh.make_bars("15m", n, seed=seed)
    rng = np.random.default_rng(seed + 100)
    perp = spot.copy()
    factor = np.exp(rng.normal(0.0, noise, n)) * 1.0003
    for col in ("open", "high", "low", "close"):
        perp[col] = spot[col] * factor
    perp["high"] = perp[["open", "high", "close"]].max(axis=1)
    perp["low"] = perp[["open", "low", "close"]].min(axis=1)
    return spot, perp


def _by_name(results):
    return {r.name: r for r in results}


def test_c2_clean_pass(qh, cfg):
    spot, perp = _spot_perp(qh)
    res = _by_name(cs.xinst_checks(spot, perp, "15m", cfg.consistency))
    assert {n: r.status for n, r in res.items()} == dict.fromkeys(res, qc.Status.PASS)
    assert res["xinst_lag"].details["best_lag"] == 0
    assert not any(r.causal for r in res.values())


def test_c2_alignment_warn(qh, cfg):
    spot, perp = _spot_perp(qh)
    res = _by_name(cs.xinst_checks(spot, perp.drop(index=perp.index[[100, 101]]), "15m",
                                   cfg.consistency))
    assert res["xinst_alignment"].status is qc.Status.WARN
    assert res["xinst_alignment"].details["only_spot"] == 2


def test_c2_basis_thresholds(qh, cfg):
    spot, perp = _spot_perp(qh)
    t = perp.index[300]
    for mult, expected in ((1.02, qc.Status.PASS), (1.05, qc.Status.WARN), (1.20, qc.Status.FAIL)):
        p = perp.copy()
        p.loc[t, ["open", "high", "low", "close"]] = spot.loc[t, "close"] * mult
        res = _by_name(cs.xinst_checks(spot, p, "15m", cfg.consistency))
        assert res["xinst_basis"].status is expected, mult


def test_c2_no_overlap_unknown(qh, cfg):
    spot, perp = _spot_perp(qh)
    res = cs.xinst_checks(spot.iloc[:100], perp.iloc[200:], "15m", cfg.consistency)
    assert {r.status for r in res} == {qc.Status.UNKNOWN}


def test_c2_lag_shift_is_warn_never_fail(qh, cfg):
    spot, perp = _spot_perp(qh)
    shifted = perp.shift(1).iloc[1:].copy()                    # perp-värden en bar för sent
    shifted["trade_count"] = shifted["trade_count"].astype(np.int64)
    shifted["gap_before"] = False
    lag = _by_name(cs.xinst_checks(spot, shifted, "15m", cfg.consistency))["xinst_lag"]
    assert lag.status is qc.Status.WARN and lag.details["best_lag"] == 1
    assert all(f.severity is qc.Severity.WARN for f in lag.findings)


def test_c2_lag_minimum_pairs_boundary(qh, cfg):
    spot, perp = _spot_perp(qh, n=900)
    m, lmax = cfg.consistency.lag_min_pairs, cfg.consistency.lag_max_bars
    # n barer -> n-1 avkastningar; lag l ger n-1-|l| par. Minst: n-1-lmax.
    exact = m + lmax + 1
    res = _by_name(cs.xinst_checks(spot.iloc[:exact], perp.iloc[:exact], "15m", cfg.consistency))
    assert min(r["n_pairs"] for r in res["xinst_lag"].details["lags"]) == m
    assert res["xinst_lag"].status is not qc.Status.UNKNOWN
    res = _by_name(cs.xinst_checks(spot.iloc[:exact - 1], perp.iloc[:exact - 1], "15m",
                                   cfg.consistency))
    assert min(r["n_pairs"] for r in res["xinst_lag"].details["lags"]) == m - 1
    assert res["xinst_lag"].status is qc.Status.UNKNOWN and "500" in res["xinst_lag"].reason


def test_c2_weak_correlation_warn(qh, cfg):
    spot = qh.make_bars("15m", 800, seed=1)
    other = qh.make_bars("15m", 800, seed=2)                   # oberoende serie, samma nivå
    lag = _by_name(cs.xinst_checks(spot, other, "15m", cfg.consistency))["xinst_lag"]
    assert lag.status is qc.Status.WARN


def test_pick_lag_tie_rules():
    assert cs.pick_lag({-1: 0.5, 0: 0.9, 1: 0.5}) == 0
    assert cs.pick_lag({-2: 0.7, -1: 0.7, 1: 0.7, 2: 0.1}) == -1        # minsta |l|, sedan minsta l
    assert cs.pick_lag({-1: 0.2, 0: 0.1, 1: 0.3}) == 1


# ------------------------------------------------------------------ C3
def _reference_signature(block: pd.DataFrame) -> str:
    raw = b"".join(
        struct.pack("<8dq", *(float(row[f]) for f in cs.SIG_FLOAT_FIELDS), int(row["trade_count"]))
        for _, row in block.iterrows()
    )
    return hashlib.sha256(raw).hexdigest()


def _golden_block() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=3, freq="8h", tz="UTC", name="open_time")
    return pd.DataFrame(
        {"open": [1.5, 2.0, 3.25], "high": [2.0, 3.0, 4.0], "low": [1.0, 1.5, 3.0],
         "close": [2.0, 3.0, 3.5], "volume": [10.0, 0.5, 7.125],
         "quote_volume": [15.0, 1.25, 25.0], "taker_buy_base_volume": [5.0, 0.25, 3.0],
         "taker_buy_quote_volume": [7.5, 0.625, 10.5], "trade_count": [3, 1, 42]},
        index=idx,
    )


def test_c3_signature_matches_struct_pack_reference(qh):
    block = qh.make_bars("15m", 96, seed=9)
    assert cs.block_signature(block) == _reference_signature(block)
    assert cs.block_signature(_golden_block()) == _reference_signature(_golden_block())


def test_c3_signature_golden_value_locked():
    assert cs.block_signature(_golden_block()) == GOLDEN_BLOCK_SHA256


def test_c3_signature_deterministic_repeated_and_cross_process(qh):
    block = qh.make_bars("15m", 96, seed=9)
    sigs = {cs.block_signature(block) for _ in range(5)}
    assert len(sigs) == 1
    # Exakt överföring till en ny process: float.hex() är bit-exakt (JSON-decimaler är det inte).
    payload = json.dumps({
        "floats": {f: [float(v).hex() for v in block[f]] for f in cs.SIG_FLOAT_FIELDS},
        "trade_count": [int(v) for v in block["trade_count"]],
    })
    code = (
        "import json, sys; import pandas as pd; "
        "from btc_trading_system.data_validation import consistency as cs; "
        "d = json.load(sys.stdin); "
        "cols = {f: [float.fromhex(v) for v in d['floats'][f]] for f in cs.SIG_FLOAT_FIELDS}; "
        "cols['trade_count'] = d['trade_count']; "
        "print(cs.block_signature(pd.DataFrame(cols)))"
    )
    out = subprocess.run([sys.executable, "-c", code], input=payload, capture_output=True,
                         text=True, check=True)
    assert out.stdout.strip() == sigs.pop()


def _days(qh, n_days=5, seed=11):
    return qh.make_bars("15m", 96 * n_days, seed=seed)


def test_c3_clean_pass_and_replay_fail(qh, cfg):
    bars = _days(qh)
    res = cs.replay_check(bars, "15m", cfg.replay)
    assert res.status is qc.Status.PASS and res.details["blocks_qualifying"] == 5
    replay = bars.copy()
    cols = [*cs.SIG_FLOAT_FIELDS, "trade_count"]
    replay.iloc[96 * 3 : 96 * 4, [replay.columns.get_loc(c) for c in cols]] = (
        bars.iloc[96 : 96 * 2][cols].to_numpy()
    )
    replay["trade_count"] = replay["trade_count"].astype(np.int64)
    res = cs.replay_check(replay, "15m", cfg.replay)
    assert res.status is qc.Status.FAIL
    assert res.details["duplicate_groups"][0]["days"] == ["2024-01-02", "2024-01-04"]


def test_c3_one_changed_value_is_not_replay(qh, cfg):
    bars = _days(qh)
    cols = [*cs.SIG_FLOAT_FIELDS, "trade_count"]
    replay = bars.copy()
    replay.iloc[96 * 3 : 96 * 4, [replay.columns.get_loc(c) for c in cols]] = (
        bars.iloc[96 : 96 * 2][cols].to_numpy()
    )
    replay["trade_count"] = replay["trade_count"].astype(np.int64)
    replay.iloc[96 * 3 + 50, replay.columns.get_loc("volume")] += 1e-9
    assert cs.replay_check(replay, "15m", cfg.replay).status is qc.Status.PASS


def test_c3_dead_days_do_not_qualify(qh, cfg):
    bars = _days(qh, n_days=2)
    dead = bars.copy()
    dead[["open", "high", "low", "close"]] = 40000.0
    dead[["volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]] = 0.0
    dead["trade_count"] = 0
    res = cs.replay_check(dead, "15m", cfg.replay)
    assert res.status is qc.Status.UNKNOWN and res.details["blocks_qualifying"] == 0


def test_c3_incomplete_day_does_not_qualify_and_1d_not_applicable(qh, cfg):
    bars = _days(qh, n_days=3).drop(index=_days(qh, n_days=3).index[[10]])
    res = cs.replay_check(bars, "15m", cfg.replay)
    assert res.details["blocks_qualifying"] == 2 and res.details["blocks_not_qualifying"] == 1
    res = cs.replay_check(qh.make_bars("1d", 30), "1d", cfg.replay)
    assert res.status is qc.Status.UNKNOWN and "not applicable" in res.reason
