"""Mutationsmängd steg 2: oföränderliga datasetversioner.

Mönstren är ordagranna från ~/phase4_work/mutation/step2_mutate.py (P4-S11-b).
"""
NAME = "step2"
TESTS = ('tests/test_database_versioned_store.py',)
MUTATIONS = {
    '1 as_of <= -> <': [
        ('btc_trading_system/database/versioned_store.py',
         'mask = frame["open_time"] <= causal_cutoff(as_of, timeframe)',
         'mask = frame["open_time"] < causal_cutoff(as_of, timeframe)'),
    ],
    '2 drop - TIMEFRAMES[timeframe]': [
        ('btc_trading_system/database/versioned_store.py',
         'return pd.Timestamp(require_utc(as_of, "as_of")) - TIMEFRAMES[timeframe]',
         'return pd.Timestamp(require_utc(as_of, "as_of"))'),
    ],
    '3 skip content_hash check': [
        ('btc_trading_system/database/versioned_store.py',
         '        if content_hash(frame) != dv:\n',
         '        if False:\n'),
    ],
    '4 skip data file sha256 check': [
        ('btc_trading_system/database/versioned_store.py',
         '        if sha256_file(data_path) != version["data_file"]["sha256"] \\\n',
         '        if False \\\n'),
    ],
    '5 remove target-exists guard': [
        ('btc_trading_system/database/versioned_store.py',
         '        if os.path.lexists(target):\n            raise ImmutableObjectError(f"{target}: objektet finns redan; skrivs aldrig över")\n',
         ''),
    ],
    '6 remove provenance comparison (M2)': [
        ('btc_trading_system/database/versioned_store.py',
         '            if json.loads(json.dumps(extra)) != stored:\n',
         '            if False:\n'),
    ],
    '7 remove init refusal (M1/M1b)': [
        ('btc_trading_system/database/versioned_store.py',
         '    """Tillåter bara en tom katalog eller spåren av en avbruten init (se INIT)."""\n',
         '    """Tillåter bara en tom katalog eller spåren av en avbruten init (se INIT)."""\n    return\n'),
    ],
    '8 register before publish/verify': [
        ('btc_trading_system/database/versioned_store.py',
         '            staged = self._stage(staging, key, canon, dv, extra)\n            self._publish_dir(staged, target)',
         '            staged = self._stage(staging, key, canon, dv, extra)\n            _vb = (staged / VERSION_FILE).read_bytes()\n            _v = json.loads(_vb)\n            _m = json.loads((staged / MANIFEST_FILE).read_bytes())\n            self._register(VersionInfo(\n                key=key, dataset_id=_v["dataset_id"], data_version=dv,\n                data_file_sha256=_v["data_file"]["sha256"], data_file_bytes=_v["data_file"]["bytes"],\n                manifest_sha256=_v["manifest"]["sha256"], manifest_bytes=_v["manifest"]["bytes"],\n                version_sha256=hashlib.sha256(_vb).hexdigest(), rows=_v["rows"],\n                first_open_time=_v["first_open_time"], last_open_time=_v["last_open_time"]), _m)\n            self._publish_dir(staged, target)'),
    ],
}
