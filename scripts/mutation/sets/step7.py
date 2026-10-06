"""Mutationsmängd steg 7: intervalläsning.

Mönstren är ordagranna från ~/phase4_work/mutation/step7_mutate.py (P4-S11-b).
"""
NAME = "step7"
TESTS = ('tests/test_database_range_reads.py',)
MUTATIONS = {
    '1 start off-by-one (bar at start dropped)': [
        ('btc_trading_system/database/versioned_store.py',
         '    lo = int(times.searchsorted(pd.Timestamp(lo_ts), side="left"))\n',
         '    lo = int(times.searchsorted(pd.Timestamp(lo_ts), side="right"))\n'),
    ],
    '2 end off-by-one (bar at end included)': [
        ('btc_trading_system/database/versioned_store.py',
         '    hi = int(times.searchsorted(pd.Timestamp(hi_ts), side="left"))\n',
         '    hi = int(times.searchsorted(pd.Timestamp(hi_ts), side="right"))\n'),
    ],
    '3 as_of boundary (bar with available_at == as_of dropped)': [
        ('btc_trading_system/database/versioned_store.py',
         '        hi = min(hi, int(times.searchsorted(causal_cutoff(as_of, timeframe), side="right")))\n',
         '        hi = min(hi, int(times.searchsorted(causal_cutoff(as_of, timeframe), side="left")))\n'),
    ],
    '4 as_of future leak (as_of used instead of causal_cutoff)': [
        ('btc_trading_system/database/versioned_store.py',
         '        hi = min(hi, int(times.searchsorted(causal_cutoff(as_of, timeframe), side="right")))\n',
         '        hi = min(hi, int(times.searchsorted(pd.Timestamp(require_utc(as_of, "as_of")), side="right")))\n'),
    ],
    '5 naive input silently treated as UTC': [
        ('btc_trading_system/database/versioned_store.py',
         '        require_utc(ts, name)\n    lo, hi = require_utc(start, "start"), require_utc(end, "end")\n',
         "        pass\n    _u = lambda ts: ts.replace(tzinfo=__import__('datetime').timezone.utc) if ts.tzinfo is None else require_utc(ts, 'x')\n    lo, hi = _u(start), _u(end)\n"),
    ],
    '6 non-zero UTC offset converted instead of rejected': [
        ('btc_trading_system/database/versioned_store.py',
         '        require_utc(ts, name)\n    lo, hi = require_utc(start, "start"), require_utc(end, "end")\n',
         "        pass\n    _u = lambda ts: ts.astimezone(__import__('datetime').timezone.utc) if ts.tzinfo is not None else require_utc(ts, 'x')\n    lo, hi = _u(start), _u(end)\n"),
    ],
    '7 verification bypass (bars.parquet decoded directly)': [
        ('btc_trading_system/database/versioned_store.py',
         '        _, manifest, frame = self._verify_object(key, data_version)\n        return range_slice(frame, key.timeframe, start, end, as_of), manifest\n',
         '        d = self.object_dir(key, data_version)\n        manifest = json.loads((d / MANIFEST_FILE).read_bytes())\n        frame = canonicalize(ParquetBackend().read(d / DATA_FILE))\n        return range_slice(frame, key.timeframe, start, end, as_of), manifest\n'),
    ],
    '8 PinnedStore.read_range follows the current ref': [
        ('btc_trading_system/database/versioned_store.py',
         '        return self._store.read_range(pin.key, pin.data_version, start, end, as_of=as_of)\n',
         '        return self._store.read_range(pin.key, self._store.current_ref(pin.key).ref.data_version, start, end, as_of=as_of)\n'),
    ],
    '9 PinnedStore.read_range falls back to the store outside the snapshot': [
        ('btc_trading_system/database/versioned_store.py',
         '        pin = self.version_of(key)\n        return self._store.read_range(pin.key, pin.data_version, start, end, as_of=as_of)\n',
         '        try:\n            pin = self.version_of(key)\n        except DatasetNotInSnapshotError:\n            pin = self._store.current_ref(key).ref\n        return self._store.read_range(pin.key, pin.data_version, start, end, as_of=as_of)\n'),
    ],
    '10 end < start check removed': [
        ('btc_trading_system/database/versioned_store.py',
         '    if hi < lo:\n        raise ValueError("end < start")\n',
         ''),
    ],
    '11 as_of not passed through by read_range': [
        ('btc_trading_system/database/versioned_store.py',
         '        return range_slice(frame, key.timeframe, start, end, as_of), manifest\n',
         '        return range_slice(frame, key.timeframe, start, end), manifest\n'),
    ],
    '12 gap_before recomputed on the slice': [
        ('btc_trading_system/database/versioned_store.py',
         '    return frame.iloc[lo:max(lo, hi)].reset_index(drop=True)\n',
         '    out = frame.iloc[lo:max(lo, hi)].reset_index(drop=True)\n    if len(out):\n        out["gap_before"] = __import__(\'btc_trading_system.data_validation.ohlcv\', fromlist=[\'x\']).compute_gap_flags(__import__(\'btc_trading_system.data.schema\', fromlist=[\'x\']).to_epoch_ns(out["open_time"]), timeframe)[0]\n    return out\n'),
    ],
}
