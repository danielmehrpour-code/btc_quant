"""Phase 3: rapporternas determinism, bindning och statuslogik."""
import json

import pytest

from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.data_validation import quality_report as qr

LOCKED_CANONICAL = "d1350cde29f02294bb327176fd9ba2c4df4ac5187f72ff54f6c674588319b9a0"


def _stored(qh, tmp_path, bars, instrument="spot", tf="15m", name="s"):
    store, key, manifest = qh.put_dataset(tmp_path / name, bars, instrument, tf)
    frame, manifest = store.read(key)
    return frame, key, manifest


def _dataset_report(qh, cfg, tmp_path, bars, tf="15m"):
    frame, key, manifest = _stored(qh, tmp_path, bars, tf=tf)
    return qr.build_dataset_report(frame, key, manifest, cfg, "abc123", True)


def test_dataset_report_is_byte_deterministic(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", 800, seed=7)
    frame, key, manifest = _stored(qh, tmp_path, bars)
    a = qr.report_to_json(qr.build_dataset_report(frame, key, manifest, cfg, "abc", True))
    b = qr.report_to_json(qr.build_dataset_report(frame.copy(), key, dict(manifest), cfg, "abc",
                                                  True))
    assert a == b
    assert qr.report_to_markdown(json.loads(a)) == qr.report_to_markdown(json.loads(b))


def test_report_contents_and_bindings(qh, cfg, tmp_path):
    report = _dataset_report(qh, cfg, tmp_path, qh.make_bars("15m", 800, seed=7))
    assert report["overall"] == "PASS"
    assert report["config"]["canonical_sha256"] == LOCKED_CANONICAL
    assert report["code"] == {"version": "abc123", "worktree_clean": True}
    names = [c["name"] for c in report["checks"]]
    assert names == ["invariants", "return_outlier", "range_outlier", "volume_outlier",
                     "stale_run", "replayed_block"]
    causal = {c["name"]: c["causal"] for c in report["checks"]}
    assert causal["replayed_block"] is False and causal["return_outlier"] is True
    assert report["coverage"]["expected_slots"] == 800 and report["coverage"]["missing_bars"] == 0
    text = qr.report_to_json(report).decode()
    assert "NaN" not in text and "Infinity" not in text
    assert not any(k in text for k in ("created_at", "generated_at", "timestamp_now"))


def test_report_rejects_data_not_matching_data_version(qh, cfg, tmp_path):
    frame, key, manifest = _stored(qh, tmp_path, qh.make_bars("15m", 200))
    tampered = frame.copy()
    tampered.loc[5, "volume"] += 1.0
    with pytest.raises(qr.ReportBindingError):
        qr.build_dataset_report(tampered, key, manifest, cfg, "x", True)


def test_phase2_suspect_stops_instead_of_fixing(qh, cfg, tmp_path):
    frame, key, manifest = _stored(qh, tmp_path, qh.make_bars("15m", 200))
    lying = {**manifest, "missing_bars_total": 7}
    with pytest.raises(qr.Phase2SuspectError):
        qr.build_dataset_report(frame, key, lying, cfg, "x", True)


def test_coverage_reports_gaps(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", 800, seed=2)
    bars = qh.recompute_gaps(bars.drop(index=bars.index[[100, 101, 102, 500]]), "15m")
    report = _dataset_report(qh, cfg, tmp_path, bars)
    cov = report["coverage"]
    assert (cov["missing_bars"], cov["gap_count"], cov["largest_gap_bars"]) == (4, 2, 3)


def test_write_and_load_report_binding(qh, cfg, tmp_path):
    report = _dataset_report(qh, cfg, tmp_path, qh.make_bars("15m", 300))
    json_path, md_path = qr.write_report(report, tmp_path / "out")
    first = json_path.read_bytes()
    qr.write_report(report, tmp_path / "out")
    assert json_path.read_bytes() == first and md_path.exists()
    assert json_path.name.startswith("dataset_spot_BTCUSDT_15m_")
    dv = report["datasets"][0]["data_version"]
    assert qr.load_report(json_path, [dv])["overall"] == report["overall"]
    with pytest.raises(qr.ReportBindingError):
        qr.load_report(json_path, ["0" * 64])


def test_tf_report_counts_incomplete_windows(qh, cfg, tmp_path):
    lower = qh.make_bars("1m", 15 * 40, seed=3)
    higher = qh.aggregate(lower, "1m", "15m")
    lower = qh.recompute_gaps(lower.drop(index=lower.index[[40]]), "1m")
    lo = _stored(qh, tmp_path, lower, tf="1m", name="lo")
    hi = _stored(qh, tmp_path, higher, tf="15m", name="hi")
    report = qr.build_tf_report(*lo, *hi, cfg, "x", None)
    d = report["checks"][0]["details"]
    assert (d["windows_total"], d["windows_checked"], d["windows_incomplete"]) == (40, 39, 1)
    assert report["overall"] == "PASS"
    assert qr.report_stem(report).startswith("tf_spot_BTCUSDT_1m-15m_")


def test_xinst_report_requires_spot_then_perp(qh, cfg, tmp_path):
    bars = qh.make_bars("15m", 800, seed=4)
    s = _stored(qh, tmp_path, bars, "spot", name="a")
    p = _stored(qh, tmp_path, bars, "perp", name="b")
    report = qr.build_xinst_report(*s, *p, cfg, "x", True)
    assert [c["name"] for c in report["checks"]] == ["xinst_alignment", "xinst_basis",
                                                     "xinst_lag"]
    with pytest.raises(qr.ReportBindingError):
        qr.build_xinst_report(*p, *s, cfg, "x", True)


def test_overall_incomplete_when_any_check_unknown(qh, cfg, tmp_path):
    report = _dataset_report(qh, cfg, tmp_path, qh.make_bars("1d", 30), tf="1d")
    statuses = {c["name"]: c["status"] for c in report["checks"]}
    assert statuses["replayed_block"] == "UNKNOWN"
    assert report["overall"] == "INCOMPLETE"


def test_jsonable_rejects_unknown_types():
    with pytest.raises(TypeError):
        qr._jsonable({"x": object()})
    assert qr._jsonable({"a": (1, 2.5, float("nan")), "s": qc.Status.WARN}) == {
        "a": [1, 2.5, None], "s": "WARN"}
