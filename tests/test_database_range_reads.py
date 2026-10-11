"""PHASE 4 steg 7: intervalläsning med exakt LocalOHLCVProvider.get_bars-semantik (Phase 2)."""
import hashlib
import inspect
import os
import random
import stat
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest
from data_helpers import NOW, canon_frame, make_klines, write_csv

from btc_trading_system.core.types import TIMEFRAMES
from btc_trading_system.data.ingest import ingest_dataset
from btc_trading_system.data.provider import LocalOHLCVProvider
from btc_trading_system.data.schema import DatasetKey
from btc_trading_system.data.storage import DatasetStore, ParquetBackend
from btc_trading_system.data_validation.errors import DatasetNotFoundError
from btc_trading_system.database import catalog
from btc_trading_system.database import legacy as lg
from btc_trading_system.database import versioned_store as vs

SPOT1 = DatasetKey("spot", "BTCUSDT", "1m")
SPOT15 = DatasetKey("spot", "BTCUSDT", "15m")
PERP15 = DatasetKey("perp", "BTCUSDT", "15m")
SPECS = ((SPOT1, 70, 1), (SPOT15, 60, 2), (PERP15, 50, 3))
NS = pd.Timedelta(1, "ns")


# ================================================================== hjälp
def build_phase2(root: Path) -> Path:
    """Phase 2-lager (Phase 2:s egen ingest): luckor (rader 5, 6, 20-22 saknas), en nollvolymsbar."""
    p2 = DatasetStore(root, ParquetBackend())
    for key, n, seed in SPECS:
        df = make_klines(key.timeframe, n, seed).drop(index=[5, 6, 20, 21, 22]).reset_index(drop=True)
        df.loc[10, ["volume", "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]] = 0.0
        f = write_csv(root.parent / f"{root.name}-{key.instrument.value}-{key.timeframe}.csv", df)
        ingest_dataset([f], key.instrument, key.timeframe, p2, now=NOW)
    return root


class Env:
    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.p2 = build_phase2(tmp / "p2")
        self.store = vs.VersionedStore.init(tmp / "store")
        assert lg.import_legacy(self.store, self.p2, apply=True).ok
        self.dv = {k: DatasetStore(self.p2, ParquetBackend()).read_manifest(k)["data_version"] for k, _, _ in SPECS}
        with self.store.writer() as w:
            self.snap = w.create_snapshot([self.store.current_ref(k).ref for k, _, _ in SPECS]).snapshot
        self.pinned = self.store.pinned(self.snap.snapshot_id)
        self.provider = {i: LocalOHLCVProvider(DatasetStore(self.p2, ParquetBackend()), i) for i in ("spot", "perp")}

    def oracle(self, key: DatasetKey, start, end, as_of=None) -> pd.DataFrame:
        return self.provider[key.instrument.value].get_bars(key.symbol, key.timeframe, start, end, as_of=as_of)

    def times(self, key: DatasetKey) -> pd.Series:
        return self.store.read_version(key, self.dv[key])[0]["open_time"]


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    return Env(tmp_path_factory.mktemp("range"))


def as_provider(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    out = frame.set_index("open_time")
    out["available_at"] = out.index + TIMEFRAMES[timeframe]
    return out


def same(env, key, got: pd.DataFrame, start, end, as_of=None) -> None:
    pd.testing.assert_frame_equal(as_provider(got, key.timeframe), env.oracle(key, start, end, as_of))


def candidates(times: pd.Series, timeframe: str) -> list[pd.Timestamp]:
    """Varje open_time ±1 ns, ±1 tf; båda sidor av varje lucka; före första och efter sista baren."""
    step = pd.Timedelta(TIMEFRAMES[timeframe])
    out: set[pd.Timestamp] = set()
    for t in times:
        out |= {t, t - NS, t + NS, t - step, t + step}
    first, last = times.iloc[0], times.iloc[-1]
    out |= {first - 2 * step, first - NS, last + step - NS, last + step + NS, last + 2 * step}
    for prev, nxt in zip(times.iloc[:-1], times.iloc[1:], strict=True):
        if nxt - prev > step:
            out |= {prev + step, nxt - step, prev + step + NS, nxt - NS}
    return sorted(out)


def tree_hash(root: Path, *, mtime: bool = False) -> str:
    hh = hashlib.sha256()
    for p in [root, *sorted(root.rglob("*"))]:
        st = os.lstat(p)
        hh.update(f"{p.relative_to(root)}|{st.st_mode:o}|{st.st_mtime_ns if mtime else ''}|".encode())
        if stat.S_ISREG(st.st_mode):
            hh.update(p.read_bytes())
    return hh.hexdigest()


# ================================================================== API
def test_public_api_is_pinned():
    assert str(inspect.signature(vs.range_slice)) == (
        "(frame: 'pd.DataFrame', timeframe: 'str', start: 'datetime', end: 'datetime', "
        "as_of: 'datetime | None' = None) -> 'pd.DataFrame'")
    assert str(inspect.signature(vs.VersionedStore.read_range)) == (
        "(self, key: 'DatasetKey', data_version: 'str', start: 'datetime', end: 'datetime', *, "
        "as_of: 'datetime | None' = None) -> 'tuple[pd.DataFrame, dict[str, Any]]'")
    assert str(inspect.signature(vs.PinnedStore.read_range)) == (
        "(self, key: 'DatasetKey', start: 'datetime', end: 'datetime', *, "
        "as_of: 'datetime | None' = None) -> 'tuple[pd.DataFrame, dict[str, Any]]'")
    assert not hasattr(vs.VersionedStore, "read_current_range")                    # D-S7-b
    assert [n for n in dir(vs.VersionedStore) if "range" in n] == ["read_funding_range", "read_range"]  # SB-S4-a


# ================================================================== exhaustiv ekvivalens
@pytest.mark.parametrize("key", [SPOT1, SPOT15], ids=lambda k: k.timeframe)
def test_exhaustive_equivalence_with_the_provider(env, key):
    """Alla start x end bland kandidaterna (start <= end), as_of=None; alla as_of bland kandidaterna
    för fyra intervall; och 3000 slumpade (start, end, as_of). Rader jämförs exakt (open_time)."""
    frame = env.store.read_version(key, env.dv[key])[0]
    times = frame["open_time"]
    cands = candidates(times, key.timeframe)
    step = pd.Timedelta(TIMEFRAMES[key.timeframe])
    as_ofs = sorted({t + step + d for t in times for d in (-NS, pd.Timedelta(0), NS)}
                    | {cands[0], cands[-1] + step})

    def check(s, e, a):
        got = vs.range_slice(frame, key.timeframe, s, e, a)
        want = env.oracle(key, s, e, a)
        assert np.array_equal(got["open_time"].to_numpy(), want.index.to_numpy()), (s, e, a)
        return len(got)

    n = sizes = 0
    for i, s in enumerate(cands):
        for e in cands[i:]:
            sizes += check(s, e, None)
            n += 1
    ranges = [(cands[0], cands[-1]), (times.iloc[3], times.iloc[-3]), (times.iloc[4], times.iloc[4]),
              (times.iloc[10] - NS, times.iloc[30] + NS)]
    for s, e in ranges:
        for a in as_ofs:
            check(s, e, a)
            n += 1
    rnd = random.Random(7)
    for _ in range(3000):
        s, e = sorted(rnd.sample(cands, 2))
        check(s, e, rnd.choice(as_ofs + [None]))
        n += 1
    assert n > 10_000 and sizes > 0
    for s, e, a in [(cands[0], cands[-1], None), (times.iloc[2], times.iloc[25], times.iloc[20] + step)]:
        same(env, key, vs.range_slice(frame, key.timeframe, s, e, a), s, e, a)       # hela framen


# ================================================================== publika API:t
def _cases(env, key):
    t = env.times(key)
    step = pd.Timedelta(TIMEFRAMES[key.timeframe])
    gap = next(i for i in range(1, len(t)) if t.iloc[i] - t.iloc[i - 1] > step)
    return {
        "exact_start": (t.iloc[3], t.iloc[9], None),
        "exact_end": (t.iloc[3] + NS, t.iloc[9], None),
        "start_one_bar_inside": (t.iloc[3] - NS, t.iloc[9], None),
        "start_one_bar_outside": (t.iloc[3] + NS, t.iloc[9], None),
        "end_one_bar_inside": (t.iloc[3], t.iloc[9] + NS, None),
        "end_one_bar_outside": (t.iloc[3], t.iloc[9] - NS, None),
        "empty_start_eq_end": (t.iloc[5], t.iloc[5], None),
        "empty_inside_gap": (t.iloc[gap - 1] + step, t.iloc[gap] - NS, None),
        "empty_before_first": (t.iloc[0] - 3 * step, t.iloc[0], None),
        "empty_after_last": (t.iloc[-1] + step, t.iloc[-1] + 3 * step, None),
        "spanning_gap": (t.iloc[gap - 2], t.iloc[gap + 2], None),
        "as_of_at_available_at": (t.iloc[2], t.iloc[20], t.iloc[12] + step),
        "as_of_just_before": (t.iloc[2], t.iloc[20], t.iloc[12] + step - NS),
        "as_of_just_after": (t.iloc[2], t.iloc[20], t.iloc[12] + step + NS),
        "as_of_before_start": (t.iloc[10], t.iloc[20], t.iloc[5] + step),
        "as_of_after_end": (t.iloc[2], t.iloc[20], t.iloc[-1] + step),
        "everything": (t.iloc[0], t.iloc[-1] + step, None),
    }


CASES = [
    "exact_start", "exact_end", "start_one_bar_inside", "start_one_bar_outside", "end_one_bar_inside",
    "end_one_bar_outside", "empty_start_eq_end", "empty_inside_gap", "empty_before_first", "empty_after_last",
    "spanning_gap", "as_of_at_available_at", "as_of_just_before", "as_of_just_after", "as_of_before_start",
    "as_of_after_end", "everything"]


@pytest.mark.parametrize("case", CASES)
@pytest.mark.parametrize("key", [SPOT1, SPOT15, PERP15], ids=lambda k: k.relpath)
def test_public_read_range_equals_the_provider(env, key, case):
    s, e, a = _cases(env, key)[case]
    frame, manifest = env.store.read_range(key, env.dv[key], s, e, as_of=a)
    same(env, key, frame, s, e, a)
    assert manifest["data_version"] == env.dv[key]
    pframe, pmanifest = env.pinned.read_range(key, s, e, as_of=a)
    pd.testing.assert_frame_equal(pframe, frame)
    assert pmanifest == manifest


def test_named_boundaries_behave_as_stated(env):
    t = env.times(SPOT15)
    step = pd.Timedelta(TIMEFRAMES["15m"])

    def rr(s, e, a=None):
        return env.store.read_range(SPOT15, env.dv[SPOT15], s, e, as_of=a)[0]["open_time"]
    assert rr(t.iloc[3], t.iloc[9]).iloc[0] == t.iloc[3]                          # start inkluderad
    assert rr(t.iloc[3], t.iloc[9]).iloc[-1] == t.iloc[8]                         # end exkluderad
    assert len(rr(t.iloc[5], t.iloc[5])) == 0
    assert rr(t.iloc[2], t.iloc[20], t.iloc[12] + step).iloc[-1] == t.iloc[12]    # available_at == as_of
    assert rr(t.iloc[2], t.iloc[20], t.iloc[12] + step - NS).iloc[-1] == t.iloc[11]


def test_empty_results_have_the_exact_columns_and_dtypes(env):
    full = env.store.read_version(SPOT15, env.dv[SPOT15])[0]
    t = full["open_time"]
    for s, e, a in [(t.iloc[5], t.iloc[5], None), (t.iloc[10], t.iloc[20], t.iloc[2])]:
        frame = env.store.read_range(SPOT15, env.dv[SPOT15], s, e, as_of=a)[0]
        assert len(frame) == 0 and list(frame.columns) == list(full.columns)
        assert frame.dtypes.equals(full.dtypes) and isinstance(frame.index, pd.RangeIndex)
        same(env, SPOT15, frame, s, e, a)


def test_gap_before_is_kept_as_stored(env):
    full = env.store.read_version(SPOT15, env.dv[SPOT15])[0]
    i = int(np.flatnonzero(full["gap_before"].to_numpy())[0])
    frame = env.store.read_range(SPOT15, env.dv[SPOT15], full["open_time"].iloc[i], full["open_time"].iloc[-1])[0]
    assert bool(frame["gap_before"].iloc[0]) is True
    pd.testing.assert_frame_equal(frame, full.iloc[i:-1].reset_index(drop=True))


def test_as_of_with_a_range_equals_read_version_as_of_filtered(env):
    t = env.times(SPOT1)
    step = pd.Timedelta(TIMEFRAMES["1m"])
    s, e, a = t.iloc[4], t.iloc[40], t.iloc[30] + step - NS
    got = env.store.read_range(SPOT1, env.dv[SPOT1], s, e, as_of=a)[0]
    causal = env.store.read_version(SPOT1, env.dv[SPOT1], as_of=a)[0]
    want = causal[(causal["open_time"] >= s) & (causal["open_time"] < e)].reset_index(drop=True)
    pd.testing.assert_frame_equal(got, want)


# ================================================================== tidszoner och indata
def _jan(env):
    return env.times(SPOT15).iloc[[3, 12]].tolist()


@pytest.mark.parametrize("which", ["start", "end", "as_of"])
def test_naive_datetimes_are_rejected_like_the_provider(env, which):
    s, e = (x.to_pydatetime() for x in _jan(env))
    args = {"start": s, "end": e, "as_of": e}
    args[which] = args[which].replace(tzinfo=None)
    for read in (lambda: env.store.read_range(SPOT15, env.dv[SPOT15], args["start"], args["end"], as_of=args["as_of"]),
                 lambda: env.pinned.read_range(SPOT15, args["start"], args["end"], as_of=args["as_of"]),
                 lambda: env.oracle(SPOT15, args["start"], args["end"], args["as_of"])):
        with pytest.raises(ValueError, match="tz-aware UTC"):
            read()


@pytest.mark.parametrize("tz", [timezone(timedelta(hours=1)), ZoneInfo("Europe/Stockholm")], ids=["+01:00", "Stockholm"])
def test_non_zero_utc_offsets_are_rejected_like_the_provider(env, tz):
    s, e = (x.to_pydatetime().astimezone(tz) for x in _jan(env))
    for read in (lambda: env.store.read_range(SPOT15, env.dv[SPOT15], s, e),
                 lambda: env.pinned.read_range(SPOT15, s, e, as_of=e),
                 lambda: env.oracle(SPOT15, s, e)):
        with pytest.raises(ValueError, match="tz-aware UTC"):
            read()


def test_london_in_july_is_rejected():
    s = datetime(2024, 7, 1, 12, tzinfo=ZoneInfo("Europe/London"))
    with pytest.raises(ValueError, match="tz-aware UTC"):
        vs.range_slice(canon_frame("15m", 5), "15m", s, s + timedelta(hours=1))


@pytest.mark.parametrize("tz", [timezone(timedelta(0)), ZoneInfo("Europe/London"), ZoneInfo("UTC")],
                         ids=["zero-offset", "London-January", "zoneinfo-UTC"])
def test_zero_offset_timezones_are_accepted_with_identical_results(env, tz):
    s, e = (x.to_pydatetime().astimezone(tz) for x in _jan(env))
    assert s.month == 1 and s.utcoffset() == timedelta(0)
    frame = env.store.read_range(SPOT15, env.dv[SPOT15], s, e, as_of=e)[0]
    same(env, SPOT15, frame, s, e, e)
    utc = env.store.read_range(SPOT15, env.dv[SPOT15], *_jan(env), as_of=_jan(env)[1])[0]
    pd.testing.assert_frame_equal(frame, utc)


def test_nanosecond_timestamps_are_exact(env):
    t = env.times(SPOT1)
    s, e = t.iloc[3] + NS, t.iloc[9] - NS
    frame = env.store.read_range(SPOT1, env.dv[SPOT1], s, e)[0]
    same(env, SPOT1, frame, s, e)
    assert frame["open_time"].iloc[0] == t.iloc[4] and frame["open_time"].iloc[-1] == t.iloc[8]


def test_end_before_start_is_rejected_and_equal_is_empty(env):
    s, e = _jan(env)
    with pytest.raises(ValueError, match="end < start"):
        env.store.read_range(SPOT15, env.dv[SPOT15], e, s)
    with pytest.raises(ValueError, match="end < start"):
        env.pinned.read_range(SPOT15, e, s)
    with pytest.raises(ValueError, match="end < start"):
        env.oracle(SPOT15, e, s)
    assert len(env.store.read_range(SPOT15, env.dv[SPOT15], s, s)[0]) == 0


def test_unknown_timeframe_is_rejected():
    with pytest.raises(ValueError, match="okänd timeframe"):
        DatasetKey("spot", "BTCUSDT", "2m")
    with pytest.raises(ValueError, match="okänd timeframe"):
        catalog.dataset_key(exchange="binance", instrument="spot", symbol="BTCUSDT", timeframe="2m")
    s = datetime(2024, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="okänd timeframe"):
        vs.range_slice(canon_frame("15m", 5), "2m", s, s)


@pytest.mark.parametrize("bad", ["2024-01-01T00:00:00+00:00", 1704067200, None])
def test_non_datetime_inputs_fail_before_any_read(env, bad, monkeypatch):
    s, e = _jan(env)
    calls = []
    monkeypatch.setattr(vs.VersionedStore, "_verify_object", lambda *a, **k: calls.append(a))
    for args in ((bad, e), (s, bad)):
        with pytest.raises(TypeError, match="måste vara datetime"):
            env.store.read_range(SPOT15, env.dv[SPOT15], *args)
    if bad is not None:
        with pytest.raises(TypeError, match="måste vara datetime"):
            env.pinned.read_range(SPOT15, s, e, as_of=bad)
    for args in ((s.replace(tzinfo=None), e), (e, s)):
        with pytest.raises(ValueError):
            env.store.read_range(SPOT15, env.dv[SPOT15], *args)
    assert calls == []


# ================================================================== verifiering och exakta versioner
def test_every_read_verifies_the_whole_object_exactly_once(env, monkeypatch):
    s, e = _jan(env)
    calls = []
    real = vs.VersionedStore._verify_object

    def spy(self, key, dv):
        calls.append((key, dv))
        return real(self, key, dv)
    monkeypatch.setattr(vs.VersionedStore, "_verify_object", spy)
    for _ in range(3):
        env.store.read_range(SPOT15, env.dv[SPOT15], s, e)
        env.pinned.read_range(SPOT15, s, e)
    assert calls == [(SPOT15, env.dv[SPOT15])] * 6


def test_corrupt_object_is_never_returned(tmp_path):
    e2 = Env(tmp_path)
    t = e2.times(SPOT15)
    d = e2.store.object_dir(SPOT15, e2.dv[SPOT15])
    os.chmod(d, 0o755)
    p = d / "bars.parquet"
    os.chmod(p, 0o644)
    data = p.read_bytes()
    i = data.find(b"parquet-cpp-arrow")
    p.write_bytes(data[:i] + b"Parquet" + data[i + 7:])                  # samma storlek och innehåll
    os.chmod(p, 0o444)
    os.chmod(d, 0o555)
    with pytest.raises(vs.ObjectCorruptError):
        e2.store.read_range(SPOT15, e2.dv[SPOT15], t.iloc[0], t.iloc[5])
    with pytest.raises(vs.ObjectCorruptError):
        e2.pinned.read_range(SPOT15, t.iloc[0], t.iloc[5])


def test_exact_version_and_pin_are_read_never_the_current_ref(tmp_path):
    e2 = Env(tmp_path)
    t = e2.times(SPOT15)
    perp1 = DatasetKey("perp", "BTCUSDT", "1m")
    with e2.store.writer() as w:
        newer = w.publish_version(SPOT15, canon_frame("15m", 30, seed=9)).info.data_version
        w.set_current(SPOT15, newer)
        w.set_current(perp1, w.publish_version(perp1, canon_frame("1m", 30, seed=4)).info.data_version)
    assert e2.store.current_ref(SPOT15).ref.data_version == newer
    s, e = t.iloc[0], t.iloc[-1] + pd.Timedelta(minutes=15)
    old = e2.store.read_range(SPOT15, e2.dv[SPOT15], s, e)[0]
    pinned = e2.pinned.read_range(SPOT15, s, e)[0]
    pd.testing.assert_frame_equal(old, pinned)
    assert len(old) == len(t)
    with pytest.raises(vs.DatasetNotInSnapshotError):
        e2.pinned.read_range(perp1, s, e)                    # finns i lagret, inte i snapshoten
    with pytest.raises(DatasetNotFoundError):
        e2.store.read_range(SPOT15, hashlib.sha256(b"x").hexdigest(), s, e)


def test_pinned_read_range_never_opens_refs_or_ref_log(env, monkeypatch):
    opened = []
    real_open = Path.open

    def spy(self, *a, **k):
        opened.append(str(self))
        return real_open(self, *a, **k)
    monkeypatch.setattr(Path, "open", spy)
    real_read = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda self: opened.append(str(self)) or real_read(self))
    env.pinned.read_range(SPOT15, *_jan(env))
    assert not [p for p in opened if "/refs/" in p or p.endswith("ref_log.jsonl")]


def test_reads_are_read_only_and_need_no_lock(env):
    before = tree_hash(env.store.root, mtime=True)
    with env.store.writer():                                          # en skrivare håller låset
        env.store.read_range(SPOT15, env.dv[SPOT15], *_jan(env))
        env.pinned.read_range(SPOT15, *_jan(env))
    assert tree_hash(env.store.root, mtime=True) == before


def test_returned_frames_are_independent_copies(env):
    s, e = _jan(env)
    a = env.store.read_range(SPOT15, env.dv[SPOT15], s, e)[0]
    a.loc[0, "close"] = -1.0
    b = env.store.read_range(SPOT15, env.dv[SPOT15], s, e)[0]
    assert b.loc[0, "close"] != -1.0
