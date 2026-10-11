"""Etapp B steg 4: funding-källan, kanoniska ramen, identiteten och kausaliteten (data/funding.py)."""
import hashlib
from datetime import UTC, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest
from funding_helpers import FKEY, month_rows, write_month, zip_bytes

from btc_trading_system.data import funding as fd
from btc_trading_system.data.schema import Instrument


# ------------------------------------------------------------------ räntan
@pytest.mark.parametrize("text,value", [
    ("0.00010000", 100_000_000), ("1E-4", 100_000_000), ("1.0E-4", 100_000_000), ("1e-4", 100_000_000),
    ("-0.00012345", -123_450_000), ("9.9E-9", 9_900), ("-9E-9", -9_000), ("9E-9", 9_000),
    ("0.00000000", 0), ("-0.00000000", 0), ("0", 0), ("0.000000000001", 1), ("1E-12", 1),
    ("0.0375", 37_500_000_000), ("1.5E+1", 15_000_000_000_000),
])
def test_both_notations_parse_to_the_same_exact_integer(text, value):
    assert fd.parse_rate_e12(text) == value


@pytest.mark.parametrize("text", ["1E-13", "0.0000000000001", "1.5E-12", "0.0000123456789012345"])
def test_precision_loss_is_refused(text):
    with pytest.raises(fd.FundingFormatError, match="exakt multipel"):
        fd.parse_rate_e12(text)


@pytest.mark.parametrize("text", ["", " 1", "1 ", "+1", "1,0", "nan", "inf", "1e", ".5", "5.", "0x1", "1E-123",
                                  "1_000", "−1"])
def test_malformed_rates_are_refused(text):
    with pytest.raises(fd.FundingFormatError):
        fd.parse_rate_e12(text)


def test_rate_outside_int64_is_refused():
    assert fd.parse_rate_e12("9223372.036854775807") == 2**63 - 1
    with pytest.raises(fd.FundingFormatError, match="int64"):
        fd.parse_rate_e12("9223372.036854775808")


# ------------------------------------------------------------------ nyckeln
def test_funding_key_is_perp_only_and_has_its_own_relpath():
    assert FKEY.relpath == "binance/perp/BTCUSDT/funding" and FKEY.instrument is Instrument.PERP
    with pytest.raises(ValueError, match="bara för perp"):
        fd.FundingKey("spot", "BTCUSDT")


# ------------------------------------------------------------------ en månadsfil
def test_month_file_is_read_exactly(tmp_path):
    rows = [("1704067200000", "8", "0.00010000"), ("1704096000002", "8", "-9E-9"), ("1704124800047", "4", "1.0E-4")]
    path = write_month(tmp_path, 2024, 1, rows)
    frame, source = fd.read_funding_zip(path, FKEY)
    assert list(frame.columns) == list(fd.FUNDING_COLUMNS)
    assert [t.isoformat() for t in frame["calc_time"]] == [
        "2024-01-01T00:00:00+00:00", "2024-01-01T08:00:00.002000+00:00", "2024-01-01T16:00:00.047000+00:00"]
    assert frame["funding_interval_hours"].tolist() == [8, 8, 4]
    assert frame["funding_rate_e12"].tolist() == [100_000_000, -9_000, 100_000_000]
    assert str(frame["calc_time"].dtype) == "datetime64[ns, UTC]"
    assert source == fd.FundingSource(path.name, hashlib.sha256(path.read_bytes()).hexdigest(), 3)


@pytest.mark.parametrize("name,match", [
    ("BTCUSDT-fundingRate-2024-1.zip", "filnamnet"), ("BTCUSDT-funding-2024-01.zip", "filnamnet"),
    ("BTCUSDT-fundingRate-2024-01.csv", "filnamnet"), ("ETHUSDT-fundingRate-2024-01.zip", "symbolen"),
    ("BTCUSDT-fundingRate-2024-13.zip", "månad"), ("BTCUSDT-fundingRate-2024-00.zip", "månad"),
])
def test_file_name_must_match_the_dataset(tmp_path, name, match):
    path = tmp_path / name
    path.write_bytes(zip_bytes(name[:-4] + ".csv", fd.SOURCE_HEADER + "\n1704067200000,8,0.0001\n"))
    with pytest.raises(fd.FundingFormatError, match=match):
        fd.read_funding_zip(path, FKEY)


def test_member_set_must_be_exactly_the_csv(tmp_path):
    stem = "BTCUSDT-fundingRate-2024-01"
    body = fd.SOURCE_HEADER + "\n1704067200000,8,0.0001\n"
    for data in (zip_bytes(f"{stem}.csv", body, (("x.csv", body),)), zip_bytes("other.csv", body)):
        (tmp_path / f"{stem}.zip").write_bytes(data)
        with pytest.raises(fd.FundingFormatError, match="medlemmar"):
            fd.read_funding_zip(tmp_path / f"{stem}.zip", FKEY)
    (tmp_path / f"{stem}.zip").write_bytes(b"not a zip")
    with pytest.raises(fd.FundingFormatError, match="zip"):
        fd.read_funding_zip(tmp_path / f"{stem}.zip", FKEY)
    (tmp_path / f"{stem}.zip").write_bytes(zip_bytes(f"{stem}.csv", ""))
    with pytest.raises(fd.FundingFormatError, match="huvudet"):
        fd.read_funding_zip(tmp_path / f"{stem}.zip", FKEY)


def test_non_utf8_and_crlf_are_refused(tmp_path):
    import io
    import zipfile
    stem = "BTCUSDT-fundingRate-2024-01"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(f"{stem}.csv", (fd.SOURCE_HEADER + "\n").encode() + b"\xff\xfe\n")
    (tmp_path / f"{stem}.zip").write_bytes(buf.getvalue())
    with pytest.raises(fd.FundingFormatError, match="UTF-8"):
        fd.read_funding_zip(tmp_path / f"{stem}.zip", FKEY)
    path = write_month(tmp_path / "crlf", 2024, 1, newline="\r\n")
    with pytest.raises(fd.FundingFormatError, match="LF"):
        fd.read_funding_zip(path, FKEY)


@pytest.mark.parametrize("header", ["calc_time,funding_interval_hours", "calc_time,funding_interval_hours,funding_rate",
                                    "CALC_TIME,funding_interval_hours,last_funding_rate",
                                    "calc_time, funding_interval_hours, last_funding_rate"])
def test_header_must_be_exact(tmp_path, header):
    with pytest.raises(fd.FundingFormatError, match="huvudet"):
        fd.read_funding_zip(write_month(tmp_path, 2024, 1, header=header), FKEY)


@pytest.mark.parametrize("row,match", [
    (("1704067200000", "8"), "fält"), (("1704067200000", "8", "0.0001", ""), "fält"),
    (("170406720000", "8", "0.0001"), "13 siffror"), (("1704067200000.0", "8", "0.0001"), "13 siffror"),
    (("1704067200000", "0", "0.0001"), "positivt heltal"), (("1704067200000", "-8", "0.0001"), "positivt heltal"),
    (("1704067200000", "8.0", "0.0001"), "positivt heltal"), (("1704067200000", "08", "0.0001"), "positivt heltal"),
    (("1704067200000", "8", ""), "ränta"), (("1704067200000", "8", "1E-13"), "exakt multipel"),
])
def test_malformed_rows_are_refused(tmp_path, row, match):
    with pytest.raises(fd.FundingFormatError, match=match):
        fd.read_funding_zip(write_month(tmp_path, 2024, 1, [row]), FKEY)


def test_rows_must_lie_in_the_files_month(tmp_path):
    jan, feb = 1704067200000, 1706745600000                     # 2024-01-01, 2024-02-01 (UTC)
    assert fd.read_funding_zip(write_month(tmp_path / "a", 2024, 1, [(str(jan), "8", "0")]), FKEY)[0].shape[0] == 1
    assert fd.read_funding_zip(write_month(tmp_path / "b", 2024, 1, [(str(feb - 1), "8", "0")]), FKEY)[0].shape[0] == 1
    for t in (jan - 1, feb):
        with pytest.raises(fd.FundingFormatError, match="utanför filens månad"):
            fd.read_funding_zip(write_month(tmp_path / str(t), 2024, 1, [(str(t), "8", "0")]), FKEY)
    dec = write_month(tmp_path / "dec", 2023, 12, [("1703980800000", "8", "0")])      # 2023-12-31 00:00
    assert fd.read_funding_zip(dec, FKEY)[0].shape[0] == 1


def test_empty_month_is_refused(tmp_path):
    with pytest.raises(fd.FundingFormatError, match="inga rader"):
        fd.read_funding_zip(write_month(tmp_path, 2024, 1, []), FKEY)


# ------------------------------------------------------------------ flera filer
def test_months_are_combined_in_time_order(tmp_path):
    jan, feb = month_rows(2024, 1), month_rows(2024, 2, days=2)
    a, b = write_month(tmp_path, 2024, 1, jan), write_month(tmp_path, 2024, 2, feb)
    frame, sources, dropped = fd.build_funding_frame([b, a], FKEY)
    assert len(frame) == len(jan) + len(feb) and dropped == 0
    assert [int(t) for t, _, _ in jan + feb] == (frame["calc_time"].astype("int64") // 1_000_000).tolist()
    assert [s.name for s in sources] == [a.name, b.name] and [s.rows for s in sources] == [len(jan), len(feb)]


def test_identical_duplicates_are_dropped_and_counted(tmp_path):
    rows = month_rows(2024, 1, days=2)
    path = write_month(tmp_path, 2024, 1, rows + rows[1:3])
    frame, sources, dropped = fd.build_funding_frame([path], FKEY)
    assert dropped == 2 and len(frame) == len(rows) and sources[0].rows == len(rows) + 2
    pd.testing.assert_frame_equal(frame, fd.build_funding_frame([write_month(tmp_path / "u", 2024, 1, rows)],
                                                                FKEY)[0])


@pytest.mark.parametrize("column", [1, 2])
def test_conflicting_duplicates_refuse_everything(tmp_path, column):
    rows = month_rows(2024, 1, days=2)
    other = list(rows[1])
    other[column] = "4" if column == 1 else "0.00099999"
    with pytest.raises(fd.FundingFormatError, match="DUPLICATE_CONFLICT"):
        fd.build_funding_frame([write_month(tmp_path, 2024, 1, rows + [tuple(other)])], FKEY)


def test_same_file_name_twice_is_refused(tmp_path):
    a = write_month(tmp_path / "a", 2024, 1)
    b = write_month(tmp_path / "b", 2024, 1)
    with pytest.raises(fd.FundingFormatError, match="samma filnamn"):
        fd.build_funding_frame([a, b], FKEY)


def test_data_version_is_independent_of_notation_and_file_order(tmp_path):
    plain = [(t, iv, "0.00010000") for t, iv, _ in month_rows(2024, 1, days=2)]
    sci = [(t, iv, "1E-4") for t, iv, _ in month_rows(2024, 1, days=2)]
    feb = month_rows(2024, 2, days=2)
    a1, a2 = write_month(tmp_path / "p", 2024, 1, plain), write_month(tmp_path / "s", 2024, 1, sci)
    b = write_month(tmp_path / "f", 2024, 2, feb)
    assert a1.read_bytes() != a2.read_bytes()
    f1, s1, _ = fd.build_funding_frame([a1, b], FKEY)
    f2, s2, _ = fd.build_funding_frame([b, a2], FKEY)
    assert fd.funding_content_hash(f1) == fd.funding_content_hash(f2)
    assert [s.name for s in s1] == [s.name for s in s2] == sorted(s.name for s in s1)
    assert s1 != s2                                              # källornas sha256 skiljer
    f3, s3, _ = fd.build_funding_frame([b, a1], FKEY)
    assert s3 == s1 and fd.funding_content_hash(f3) == fd.funding_content_hash(f1)


def test_build_refuses_no_files():
    with pytest.raises(fd.FundingFormatError, match="inga indatafiler"):
        fd.build_funding_frame([], FKEY)


# ------------------------------------------------------------------ identitet
def _frame(ms, iv, rate):
    return fd.canonicalize_funding(pd.DataFrame({
        "calc_time": pd.Series(pd.to_datetime(np.asarray(ms, dtype="int64"), unit="ms", utc=True)),
        "funding_interval_hours": np.asarray(iv, dtype="int64"),
        "funding_rate_e12": np.asarray(rate, dtype="int64")}))


def test_content_hash_is_the_documented_encoding():
    f = _frame([1704067200000, 1704096000002], [8, 8], [100_000_000, -9_000])
    hh = hashlib.sha256(b"btc-quant/funding/v1\x00")
    hh.update((2).to_bytes(8, "little"))
    hh.update(b"calc_time\x00" + np.asarray([1704067200000, 1704096000002], "<i8").tobytes())
    hh.update(b"funding_interval_hours\x00" + np.asarray([8, 8], "<i8").tobytes())
    hh.update(b"funding_rate_e12\x00" + np.asarray([100_000_000, -9_000], "<i8").tobytes())
    assert fd.funding_content_hash(f) == hh.hexdigest() == \
        "27179f0e74d04c328b7b761ff75cd29573a0fe162164cc0d682bee1560f71e3e"
    g = f.copy()
    g.loc[1, "funding_rate_e12"] = -9_001
    assert fd.funding_content_hash(g) != fd.funding_content_hash(f)


@pytest.mark.parametrize("change,match", [
    (lambda f: f.iloc[::-1].reset_index(drop=True), "strikt stigande"),
    (lambda f: pd.concat([f, f.iloc[[-1]]], ignore_index=True), "strikt stigande"),
    (lambda f: f.assign(calc_time=f["calc_time"] + pd.Timedelta(microseconds=1)), "hela millisekunder"),
    (lambda f: f.assign(funding_interval_hours=np.zeros(len(f), dtype="int64")), "> 0"),
    (lambda f: f.assign(funding_rate_e12=f["funding_rate_e12"].astype("float64")), "int64"),
    (lambda f: f.iloc[0:0], "tom"),
    (lambda f: f[["funding_interval_hours", "calc_time", "funding_rate_e12"]], "kolumner"),
    (lambda f: f.assign(calc_time=f["calc_time"].dt.tz_localize(None)), "UTC"),
])
def test_frame_invariants(change, match):
    f = _frame([1704067200000, 1704096000000, 1704124800000], [8, 8, 8], [1, 2, 3])
    fd.check_funding_frame(f)
    with pytest.raises(fd.FundingFormatError, match=match):
        fd.check_funding_frame(change(f))


def test_manifest_is_derived_from_content_and_sources(tmp_path):
    a = write_month(tmp_path, 2024, 1, month_rows(2024, 1, days=1))
    frame, sources, dropped = fd.build_funding_frame([a], FKEY)
    m = fd.funding_manifest(FKEY, frame, sources, dropped)
    assert m == {"schema_version": 1, "kind": "funding", "exchange": "binance", "instrument": "perp",
                 "symbol": "BTCUSDT", "rows": 3, "first_calc_time": "2024-01-01T00:00:00+00:00",
                 "last_calc_time": "2024-01-01T16:00:00+00:00", "data_version": fd.funding_content_hash(frame),
                 "sources": [{"name": a.name, "sha256": hashlib.sha256(a.read_bytes()).hexdigest(), "rows": 3}],
                 "duplicates_dropped": 0}


# ------------------------------------------------------------------ kausalitet (SB-S4-c A)
def test_available_at_is_calc_time_plus_15_seconds():
    assert fd.AVAILABLE_LAG == timedelta(seconds=15)
    t = pd.Series(pd.to_datetime([1704096000002], unit="ms", utc=True))
    assert fd.available_at(t).iloc[0] == pd.Timestamp("2024-01-01T08:00:15.002Z")
    as_of = datetime(2024, 1, 1, 8, 0, 15, 2000, tzinfo=UTC)
    assert fd.funding_cutoff(as_of) == pd.Timestamp("2024-01-01T08:00:00.002Z")
    for bad in (datetime(2024, 1, 1), datetime(2024, 1, 1, tzinfo=timezone(timedelta(hours=1)))):  # noqa: DTZ001 - avsiktligt naiv
        with pytest.raises(ValueError, match="UTC"):
            fd.funding_cutoff(bad)


# ------------------------------------------------------------------ Parquet
def test_parquet_round_trip_is_exact_and_never_overwrites(tmp_path):
    f = _frame([1704067200000, 1704096000002], [8, 4], [2**63 - 1, -(2**63) + 1])
    path = tmp_path / "funding.parquet"
    fd.FundingParquet.write(path, f)
    pd.testing.assert_frame_equal(fd.canonicalize_funding(fd.FundingParquet.read(path)), f)
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        fd.FundingParquet.write(path, f)
    assert path.read_bytes() == before
    other = tmp_path / "other.parquet"
    fd.FundingParquet.write(other, f)
    assert other.read_bytes() == before                         # deterministiska bytes
