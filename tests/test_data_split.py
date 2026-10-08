"""Etapp B steg 3a: schemat för datauppdelningen (DataSplitConfig, D-B-h svar 1 och 2).

Exklusiva UTC-gränser [start, end) på open_time, delarna i ordning, ingen överlappning, minst 7
kalenderdagars lucka. Gränsfallen prövas exakt på gränsen och ±1 µs (datetime) samt ±1 ns
(pd.Timestamp, open_time-kolumnens upplösning). Datumen här är testdatum, inte den låsta uppdelningen.
"""
from datetime import UTC, datetime, timedelta, timezone

import pandas as pd
import pytest

from btc_trading_system.config.settings import (
    MIN_SPLIT_GAP,
    SPLIT_SEGMENTS,
    ConfigError,
    DataSplitConfig,
    SplitSegment,
    _parse_split,
    load_settings,
)

T0, T1 = datetime(2021, 1, 1, tzinfo=UTC), datetime(2022, 1, 1, tzinfo=UTC)
V0, V1 = T1 + timedelta(days=7), datetime(2023, 1, 1, tzinfo=UTC)
F0, F1 = V1 + timedelta(days=7), datetime(2024, 1, 1, tzinfo=UTC)
US, NS = timedelta(microseconds=1), pd.Timedelta(1, "ns")


def split(**over):
    segs = {"train": SplitSegment(T0, T1), "validation": SplitSegment(V0, V1), "final_test": SplitSegment(F0, F1)}
    segs.update(over)
    return DataSplitConfig(**segs)


# ================================================================== schema och validering
def test_the_constants():
    assert SPLIT_SEGMENTS == ("train", "validation", "final_test")
    assert timedelta(days=7) == MIN_SPLIT_GAP


def test_a_valid_split_and_the_placeholder():
    s = split()
    assert (s.train.start, s.train.end, s.final_test.end) == (T0, T1, F1)
    empty = DataSplitConfig()
    assert (empty.train, empty.validation, empty.final_test) == (None, None, None)
    assert empty.segment_of(T0) is None


@pytest.mark.parametrize("missing", SPLIT_SEGMENTS)
def test_all_three_segments_or_none(missing):
    with pytest.raises(ConfigError):
        split(**{missing: None})


def test_start_must_be_before_end():
    for a, b in ((T1, T0), (T0, T0)):
        with pytest.raises(ConfigError):
            SplitSegment(a, b)
    assert SplitSegment(T0, T0 + US).contains(T0)


@pytest.mark.parametrize("bad", [datetime.fromisoformat("2021-01-01T00:00:00"), datetime(2021, 1, 1, tzinfo=timezone(timedelta(hours=2)))])
def test_bounds_must_be_utc(bad):
    with pytest.raises(ValueError):
        SplitSegment(bad, T1)
    with pytest.raises(ValueError):
        SplitSegment(T0 - timedelta(days=400), bad)


def test_segments_must_be_in_order_without_overlap():
    with pytest.raises(ConfigError, match="ordning"):
        split(validation=SplitSegment(T1 - US, V1))                       # börjar före slutet av train
    with pytest.raises(ConfigError, match="ordning"):
        split(train=SplitSegment(V0, V1), validation=SplitSegment(T0, T1))
    with pytest.raises(ConfigError, match="ordning"):
        split(final_test=SplitSegment(V1 - timedelta(days=30), F1))


def test_the_gap_must_be_at_least_seven_days_exactly():
    split(validation=SplitSegment(T1 + MIN_SPLIT_GAP, V1))                 # exakt 7 dagar: tillåtet
    with pytest.raises(ConfigError, match="7 dagar"):
        split(validation=SplitSegment(T1 + MIN_SPLIT_GAP - US, V1))        # 1 µs för kort
    with pytest.raises(ConfigError, match="7 dagar"):
        split(validation=SplitSegment(T1, V1))                             # ingen lucka
    split(final_test=SplitSegment(V1 + MIN_SPLIT_GAP, F1))
    with pytest.raises(ConfigError, match="7 dagar"):
        split(final_test=SplitSegment(V1 + MIN_SPLIT_GAP - US, F1))
    split(validation=SplitSegment(T1 + timedelta(days=30), V1))            # längre lucka: tillåtet


# ================================================================== en bar hör till delen med dess open_time
@pytest.mark.parametrize(("t", "expected"), [
    (T0 - US, None), (T0, "train"), (T0 + US, "train"), (T1 - US, "train"), (T1, None), (T1 + US, None),
    (V0 - US, None), (V0, "validation"), (V1 - US, "validation"), (V1, None),
    (F0 - US, None), (F0, "final_test"), (F1 - US, "final_test"), (F1, None), (F1 + US, None),
    (T1 + timedelta(days=3), None),                                       # mitt i luckan
])
def test_segment_of_at_microsecond_boundaries(t, expected):
    assert split().segment_of(t) == expected


@pytest.mark.parametrize(("bound", "offset", "expected"), [
    ("T0", -1, None), ("T0", 0, "train"), ("T0", 1, "train"),
    ("T1", -1, "train"), ("T1", 0, None), ("T1", 1, None),
    ("V0", -1, None), ("V0", 0, "validation"), ("V1", -1, "validation"), ("V1", 0, None),
    ("F0", -1, None), ("F0", 0, "final_test"), ("F1", -1, "final_test"), ("F1", 0, None), ("F1", 1, None),
])
def test_segment_of_at_nanosecond_boundaries(bound, offset, expected):
    t = pd.Timestamp({"T0": T0, "T1": T1, "V0": V0, "V1": V1, "F0": F0, "F1": F1}[bound]) + offset * NS
    assert split().segment_of(t) == expected


def test_segment_of_requires_utc():
    for bad in (datetime.fromisoformat("2021-06-01T00:00:00"), datetime(2021, 6, 1, tzinfo=timezone(timedelta(hours=1)))):
        with pytest.raises(ValueError):
            split().segment_of(bad)


def test_contains_is_half_open():
    seg = SplitSegment(T0, T1)
    assert seg.contains(T0) and not seg.contains(T1) and seg.contains(pd.Timestamp(T1) - NS)
    assert not seg.contains(pd.Timestamp(T0) - NS)


# ================================================================== YAML
def _yaml(tmp_path, text):
    p = tmp_path / "c.yaml"
    p.write_text(text)
    return p


GOOD = ("data_split:\n"
        "  train: {start: '2021-01-01T00:00:00Z', end: '2022-01-01T00:00:00Z'}\n"
        "  validation: {start: '2022-01-08T00:00:00Z', end: '2023-01-01T00:00:00Z'}\n"
        "  final_test: {start: '2023-01-08T00:00:00Z', end: '2024-01-01T00:00:00Z'}\n")


def test_yaml_is_parsed_into_the_same_split(tmp_path):
    assert load_settings(_yaml(tmp_path, GOOD)).data_split == split()
    assert load_settings(_yaml(tmp_path, "data_split: {train: null, validation: null, final_test: null}\n")
                         ).data_split == DataSplitConfig()
    assert load_settings(_yaml(tmp_path, "trading: {live: false}\n")).data_split == DataSplitConfig()
    assert _parse_split(None) == DataSplitConfig()


@pytest.mark.parametrize("text", [
    "data_split: {}\n",
    "data_split: {train: null, validation: null}\n",
    "data_split: {train: null, validation: null, final_test: null, extra: null}\n",
    "data_split: [1, 2]\n",
    "data_split: {train_until: '2023-01-01T00:00:00Z', validation_until: '2024-01-01T00:00:00Z'}\n",   # gamla schemat
    GOOD.replace("{start: '2021-01-01T00:00:00Z', end: '2022-01-01T00:00:00Z'}", "{start: '2021-01-01T00:00:00Z'}"),
    GOOD.replace("end: '2022-01-01T00:00:00Z'}", "end: '2022-01-01T00:00:00Z', step: 1}"),
    GOOD.replace("{start: '2021-01-01T00:00:00Z', end: '2022-01-01T00:00:00Z'}", "'2021'"),
    GOOD.replace("'2021-01-01T00:00:00Z'", "2021-01-01"),
    GOOD.replace("2021-01-01T00:00:00Z", "2021-01-01T00:00:00"),
    GOOD.replace("2022-01-08T00:00:00Z", "2022-01-07T23:59:59.999999Z"),
    GOOD.replace("final_test: {start: '2023-01-08T00:00:00Z', end: '2024-01-01T00:00:00Z'}", "final_test: null"),
])
def test_yaml_errors_are_refused(tmp_path, text):
    with pytest.raises((ConfigError, ValueError)):
        load_settings(_yaml(tmp_path, text))
