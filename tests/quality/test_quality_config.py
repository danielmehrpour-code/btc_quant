"""Phase 3: låst konfiguration (hash, strikt laddning)."""
import hashlib

import pytest
import yaml

from btc_trading_system.data_validation import quality_config as qc

LOCKED_FILE_SHA256 = "756509ad305bc1c036fc25fce797a9e722700bd3115c142f43a2fdcae1880d2d"
LOCKED_CANONICAL_SHA256 = "d1350cde29f02294bb327176fd9ba2c4df4ac5187f72ff54f6c674588319b9a0"


def _data():
    return yaml.safe_load(qc.DEFAULT_CONFIG_PATH.read_bytes())


def test_config_file_bytes_are_locked():
    raw = qc.DEFAULT_CONFIG_PATH.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == LOCKED_FILE_SHA256
    assert len(raw) == 658 and raw.count(b"\n") == 27 and b"\r" not in raw


def test_canonical_hash_is_locked():
    cfg = qc.load_quality_config()
    assert cfg.canonical_sha256 == LOCKED_CANONICAL_SHA256
    assert cfg.file_sha256 == LOCKED_FILE_SHA256


def test_locked_values_are_parsed_exactly():
    cfg = qc.load_quality_config()
    assert cfg.invariants == qc.InvariantsConfig(1e-6, 1e-8, 1e-12)
    a = cfg.anomalies
    assert (a.window_min_days, a.window_min_bars, a.min_obs_fraction) == (7, 100, 0.5)
    assert (a.return_z_threshold, a.return_scale_floor) == (10.0, 1e-5)
    assert (a.range_ratio_threshold, a.range_floor, a.volume_ratio_threshold) == (20.0, 1e-5, 50.0)
    assert (a.stale_min_minutes, a.stale_min_bars) == (30, 3)
    c = cfg.consistency
    assert (c.agg_abs_tol, c.agg_rel_tol) == (1e-8, 1e-9)
    assert (c.basis_warn_bps, c.basis_fail_bps) == (300.0, 1000.0)
    assert (c.lag_max_bars, c.lag_min_pairs, c.lag_min_corr0) == (3, 500, 0.5)
    assert cfg.replay.max_timeframe == "4h"


def test_canonical_hash_depends_on_values_not_formatting():
    base = qc.parse_quality_config(_data()).canonical_sha256
    assert base == LOCKED_CANONICAL_SHA256
    changed = _data()
    changed["anomalies"]["return_z_threshold"] = 9.0
    assert qc.parse_quality_config(changed).canonical_sha256 != base


def test_unknown_and_missing_keys_rejected():
    d = _data()
    d["extra"] = {}
    with pytest.raises(qc.QualityConfigError, match="okända toppnivå"):
        qc.parse_quality_config(d)
    d = _data()
    d["anomalies"]["return_z"] = 5.0
    with pytest.raises(qc.QualityConfigError, match="okända nycklar"):
        qc.parse_quality_config(d)
    d = _data()
    del d["consistency"]["lag_min_pairs"]
    with pytest.raises(qc.QualityConfigError, match="saknade nycklar"):
        qc.parse_quality_config(d)
    d = _data()
    del d["replay"]
    with pytest.raises(qc.QualityConfigError, match="replay"):
        qc.parse_quality_config(d)


def test_wrong_types_rejected():
    cases = [
        ("anomalies", "return_z_threshold", 10),      # int där float krävs
        ("anomalies", "return_z_threshold", "10.0"),  # sträng
        ("anomalies", "stale_min_bars", 3.0),         # float där int krävs
        ("anomalies", "stale_min_bars", True),        # bool är inte int här
        ("invariants", "vwap_rel_tol", "1e-6"),       # PyYAML-fallgrop: sträng
        ("replay", "max_timeframe", 4),
    ]
    for section, key, value in cases:
        d = _data()
        d[section][key] = value
        with pytest.raises(qc.QualityConfigError):
            qc.parse_quality_config(d)


def test_out_of_range_rejected():
    cases = [
        ("invariants", "vwap_rel_tol", 0.0),
        ("invariants", "vwap_rel_tol", 0.01),
        ("anomalies", "min_obs_fraction", 1.5),
        ("anomalies", "range_ratio_threshold", 1.0),
        ("anomalies", "stale_min_bars", 1),
        ("consistency", "lag_max_bars", 0),
        ("consistency", "lag_min_pairs", 10),
        ("consistency", "lag_min_corr0", 1.0),
        ("consistency", "basis_fail_bps", 200.0),     # < basis_warn_bps
        ("replay", "max_timeframe", "2h"),
    ]
    for section, key, value in cases:
        d = _data()
        d[section][key] = value
        with pytest.raises(qc.QualityConfigError):
            qc.parse_quality_config(d)


def test_version_and_shape_rejected():
    d = _data()
    d["version"] = 2
    with pytest.raises(qc.QualityConfigError):
        qc.parse_quality_config(d)
    with pytest.raises(qc.QualityConfigError):
        qc.parse_quality_config(["not", "a", "mapping"])


def test_invalid_yaml_file(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("version: [1\n")
    with pytest.raises(qc.QualityConfigError):
        qc.load_quality_config(p)


def test_overall_status_precedence():
    s = qc.Status
    assert qc.overall_status([s.PASS, s.PASS]) == "PASS"
    assert qc.overall_status([s.PASS, s.WARN]) == "WARN"
    assert qc.overall_status([s.WARN, s.UNKNOWN]) == "INCOMPLETE"
    assert qc.overall_status([s.UNKNOWN, s.FAIL, s.WARN]) == "FAIL"
    with pytest.raises(ValueError):
        qc.overall_status([])
