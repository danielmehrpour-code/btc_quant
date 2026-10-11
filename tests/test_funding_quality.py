"""Etapp B steg 4: kvalitetskontroller för funding (SB-S4-d) och den låsta konfigurationen."""
import hashlib

import numpy as np
import pandas as pd
import pytest
import yaml

from btc_trading_system.data import funding as fd
from btc_trading_system.data_validation import funding_quality as fq
from btc_trading_system.data_validation.quality_config import QualityConfigError, Status

LOCKED_FILE_SHA256 = "63400e8cef8b863d06ec5ef8bf8bef09bfc8c4a68da16e347835f2466ce287ca"
LOCKED_CANONICAL_SHA256 = "46ec1331400fcd610311064b6a86ede791320b9a1d3670cff5ad8111132a6fa8"
H = 3_600_000
T0 = 1704067200000                                           # 2024-01-01T00:00Z


def test_config_is_locked():
    raw = fq.DEFAULT_FUNDING_QUALITY_PATH.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == LOCKED_FILE_SHA256
    cfg = fq.load_funding_quality_config()
    assert cfg.canonical_sha256 == LOCKED_CANONICAL_SHA256 and cfg.file_sha256 == LOCKED_FILE_SHA256
    assert cfg.spacing_tolerance_ms == 30_000                # 2 x 15 s (Binances FAQ), inte datan (P3-02)


@pytest.mark.parametrize("data", [
    None, {}, {"version": 1}, {"version": 1, "coverage": {}, "x": 1},
    {"version": 2, "coverage": {"spacing_tolerance_ms": 1}}, {"version": True, "coverage": {"spacing_tolerance_ms": 1}},
    {"version": 1, "coverage": {"spacing_tolerance_ms": 0}}, {"version": 1, "coverage": {"spacing_tolerance_ms": -1}},
    {"version": 1, "coverage": {"spacing_tolerance_ms": 3_600_000}},
    {"version": 1, "coverage": {"spacing_tolerance_ms": 1.0}}, {"version": 1, "coverage": {"spacing_tolerance_ms": True}},
    {"version": 1, "coverage": {"spacing_tolerance_ms": 1, "other": 2}},
])
def test_config_is_parsed_strictly(data):
    with pytest.raises(QualityConfigError):
        fq.parse_funding_quality_config(data)


def test_config_file_must_be_valid_yaml(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("version: [1\n")
    with pytest.raises(QualityConfigError, match="YAML"):
        fq.load_funding_quality_config(p)
    p.write_text(yaml.safe_dump({"version": 1, "coverage": {"spacing_tolerance_ms": 5}}))
    assert fq.load_funding_quality_config(p).spacing_tolerance_ms == 5


def frame(ms, iv=None):
    iv = [8] * len(ms) if iv is None else iv
    return fd.canonicalize_funding(pd.DataFrame({
        "calc_time": pd.Series(pd.to_datetime(np.asarray(ms, dtype="int64"), unit="ms", utc=True)),
        "funding_interval_hours": np.asarray(iv, dtype="int64"),
        "funding_rate_e12": np.arange(len(ms), dtype="int64")}))


CFG = fq.load_funding_quality_config()


def statuses(rep):
    return {c.name: c.status for c in rep.checks}


def test_clean_series_passes():
    rep = fq.check_funding(frame([T0 + i * 8 * H + (i % 2) * 47 for i in range(30)]), CFG)
    assert rep.overall == "PASS" and set(statuses(rep).values()) == {Status.PASS}
    assert [c.name for c in rep.checks] == list(fq.CHECK_NAMES) and rep.config_sha256 == LOCKED_CANONICAL_SHA256


@pytest.mark.parametrize("delta,ok", [(30_000, True), (-30_000, True), (30_001, False), (-30_001, False)])
def test_spacing_tolerance_is_inclusive(delta, ok):
    ms = [T0, T0 + 8 * H + delta, T0 + 16 * H + delta]
    rep = fq.check_funding(frame(ms), CFG)
    assert statuses(rep)["funding_coverage"] == (Status.PASS if ok else Status.FAIL)


def test_missing_settlement_fails_coverage():
    rep = fq.check_funding(frame([T0, T0 + 8 * H, T0 + 24 * H]), CFG)
    assert rep.overall == "FAIL" and statuses(rep)["funding_coverage"] is Status.FAIL
    f = rep.checks[3].findings[0]
    assert f.code == "FUNDING_SPACING" and f.count == 1
    assert f.examples == ("2024-01-01T08:00:00+00:00 -> 2024-01-02T00:00:00+00:00",)


def test_spacing_uses_the_previous_rows_interval():
    ms = [T0, T0 + 8 * H, T0 + 12 * H, T0 + 16 * H]          # intervallet byts från 8 h till 4 h
    assert fq.check_funding(frame(ms, [8, 4, 4, 4]), CFG).overall == "PASS"
    assert statuses(fq.check_funding(frame(ms, [8, 8, 4, 4]), CFG))["funding_coverage"] is Status.FAIL


def test_order_and_interval_failures():
    rep = fq.check_funding(frame([T0, T0 + 8 * H, T0 + 8 * H]), CFG)
    assert statuses(rep)["funding_order"] is Status.FAIL and rep.checks[1].findings[0].count == 1
    rep = fq.check_funding(frame([T0, T0 + 8 * H], [8, 0]), CFG)
    assert statuses(rep)["funding_interval"] is Status.FAIL


def test_structurally_invalid_frame_fails_and_the_rest_is_unknown():
    bad = frame([T0, T0 + 8 * H]).assign(funding_rate_e12=[0.5, 1.5])
    rep = fq.check_funding(bad, CFG)
    assert statuses(rep) == {"funding_structure": Status.FAIL, "funding_order": Status.UNKNOWN,
                             "funding_interval": Status.UNKNOWN, "funding_coverage": Status.UNKNOWN}
    assert rep.overall == "FAIL"
    sub_ms = frame([T0, T0 + 8 * H]).assign(calc_time=lambda f: f["calc_time"] + pd.Timedelta(1, "us"))
    assert statuses(fq.check_funding(sub_ms, CFG))["funding_structure"] is Status.FAIL
    assert statuses(fq.check_funding(frame([T0])[[]], CFG))["funding_structure"] is Status.FAIL


def test_checks_never_change_the_data():
    f = frame([T0, T0 + 8 * H, T0 + 30 * H])
    copy = f.copy(deep=True)
    fq.check_funding(f, CFG)
    pd.testing.assert_frame_equal(f, copy)
