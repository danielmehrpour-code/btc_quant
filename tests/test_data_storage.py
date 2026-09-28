import json

import pandas as pd
import pytest

from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data.storage import DatasetStore, canonicalize, content_hash
from btc_trading_system.data_validation.errors import (DataIntegrityError, DatasetNotFoundError,
                                                       DataValidationError)
from data_helpers import RawBackend, canon_frame

KEY = DatasetKey("spot", "BTCUSDT", "15m")


def _store(tmp_path, name="s"):
    return DatasetStore(tmp_path / name, RawBackend())


def test_roundtrip_is_lossless(tmp_path):
    st, df = _store(tmp_path), canon_frame("15m", 80)
    st.write(KEY, df, {})
    got, man = st.read(KEY)
    pd.testing.assert_frame_equal(got, df)
    assert man["rows"] == 80 and man["symbol"] == "BTCUSDT" and man["instrument"] == "spot"


def test_storage_is_byte_deterministic(tmp_path):
    df = canon_frame("15m", 80)
    a, b = _store(tmp_path, "a"), _store(tmp_path, "b")
    ma, mb = a.write(KEY, df, {"k": 1}), b.write(KEY, df.copy(), {"k": 1})
    assert a.data_path(KEY).read_bytes() == b.data_path(KEY).read_bytes()
    assert a.manifest_path(KEY).read_bytes() == b.manifest_path(KEY).read_bytes()
    assert ma == mb


def test_manifest_has_sorted_keys_and_no_wallclock(tmp_path):
    st = _store(tmp_path)
    st.write(KEY, canon_frame("15m", 30), {})
    text = st.manifest_path(KEY).read_text()
    parsed = json.loads(text)
    assert list(parsed) == sorted(parsed)
    assert not any("created" in k or "written" in k for k in parsed)
    assert text.endswith("\n")


def test_data_version_changes_when_one_value_changes():
    df = canon_frame("15m", 50)
    h0 = content_hash(df)
    d2 = df.copy()
    d2.loc[10, "close"] = d2.loc[10, "close"] + 0.01
    assert content_hash(d2) != h0
    d3 = df.copy()
    d3.loc[10, "trade_count"] += 1
    assert content_hash(d3) != h0
    assert content_hash(df.copy()) == h0


def test_hash_depends_on_row_order_and_count():
    df = canon_frame("15m", 50)
    assert content_hash(df.iloc[:-1]) != content_hash(df)


def test_tampered_data_detected(tmp_path):
    st = _store(tmp_path)
    st.write(KEY, canon_frame("15m", 50, seed=1), {})
    other = canon_frame("15m", 50, seed=2)
    st.backend.write(st.data_path(KEY), other)          # data byts ut, manifest oförändrat
    with pytest.raises(DataIntegrityError):
        st.read(KEY)


def test_manifest_rowcount_mismatch_detected(tmp_path):
    st = _store(tmp_path)
    st.write(KEY, canon_frame("15m", 50), {})
    m = json.loads(st.manifest_path(KEY).read_text())
    m["rows"] = 49
    st.manifest_path(KEY).write_text(json.dumps(m))
    with pytest.raises(DataIntegrityError):
        st.read(KEY)


def test_missing_dataset(tmp_path):
    with pytest.raises(DatasetNotFoundError):
        _store(tmp_path).read(KEY)


def test_store_refuses_invalid_frames(tmp_path):
    st = _store(tmp_path)
    df = canon_frame("15m", 20)
    dup = pd.concat([df, df.iloc[[5]]]).sort_values("open_time", kind="stable").reset_index(drop=True)
    with pytest.raises(DataValidationError):
        st.write(KEY, dup, {})
    assert not st.exists(KEY)


def test_store_refuses_wrong_gap_flags(tmp_path):
    df = canon_frame("15m", 20).drop(index=[7]).reset_index(drop=True)   # lucka, men gap_before fortfarande False
    with pytest.raises(DataValidationError) as ei:
        _store(tmp_path).write(KEY, df, {})
    assert ei.value.report.has("GAP_FLAG_MISMATCH")


def test_no_tmp_files_left_behind(tmp_path):
    st = _store(tmp_path)
    st.write(KEY, canon_frame("15m", 20), {})
    left = [p.name for p in (tmp_path / "s").rglob("*") if p.name.endswith(".tmp")]
    assert left == []


def test_canonicalize_fixes_dtypes_and_order():
    df = canon_frame("15m", 10)
    scrambled = df[list(reversed(df.columns))].copy()
    scrambled["trade_count"] = scrambled["trade_count"].astype("int32")
    out = canonicalize(scrambled)
    assert list(out.columns) == list(df.columns) and out["trade_count"].dtype == "int64"


# ---- Parquet (kräver pyarrow; hoppas över i sandboxen) ----
def test_parquet_backend_roundtrip_and_determinism(tmp_path):
    pytest.importorskip("pyarrow")
    df = canon_frame("15m", 300)
    a, b = DatasetStore(tmp_path / "a"), DatasetStore(tmp_path / "b")      # default = ParquetBackend
    a.write(KEY, df, {})
    b.write(KEY, df.copy(), {})
    assert a.data_path(KEY).read_bytes() == b.data_path(KEY).read_bytes()
    got, man = a.read(KEY)
    pd.testing.assert_frame_equal(got, df)
    assert man["data_version"] == content_hash(df)


def test_parquet_file_has_fixed_schema_and_no_pandas_metadata(tmp_path):
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq
    st = DatasetStore(tmp_path / "p")
    st.write(KEY, canon_frame("15m", 50), {})
    t = pq.read_table(str(st.data_path(KEY)))
    assert str(t.schema.field("open_time").type) == "timestamp[ms, tz=UTC]"
    assert str(t.schema.field("trade_count").type) == "int64"
    assert str(t.schema.field("gap_before").type) == "bool"
    assert b"pandas" not in st.data_path(KEY).read_bytes()


def test_parquet_tamper_detected(tmp_path):
    pytest.importorskip("pyarrow")
    st = DatasetStore(tmp_path / "t")
    st.write(KEY, canon_frame("15m", 50, seed=1), {})
    st.backend.write(st.data_path(KEY), canon_frame("15m", 50, seed=2))
    with pytest.raises(DataIntegrityError):
        st.read(KEY)
