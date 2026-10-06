"""Mutationsmängd steg 6: legacy-import.

Mönstren är ordagranna från ~/phase4_work/mutation/step6_mutate.py (P4-S11-b).
"""
NAME = "step6"
TESTS = ('tests/test_database_legacy.py', 'tests/test_database_integrity.py')
MUTATIONS = {
    '1 Phase 2 read path skipped (no content_hash/rows/check_ohlcv verification)': [
        ('btc_trading_system/database/legacy.py',
         '        frame, manifest = DatasetStore(root, ParquetBackend()).read(cap.key)\n',
         '        frame, manifest = ParquetBackend().read(root / (cap.key.relpath + DATA_SUFFIX)), dict(cap.manifest)\n'),
    ],
    '2 content-derived manifest checks skipped': [
        ('btc_trading_system/database/legacy.py',
         '    wrong = sorted(k for k, v in derived.items() if json.loads(json.dumps(v)) != cap.manifest[k])\n',
         '    wrong: list[str] = []\n'),
    ],
    '3 symlinks followed (discovery and O_NOFOLLOW; two layers)': [
        ('btc_trading_system/database/legacy.py',
         '                    files.setdefault(kind[1], {})[kind[0]] = e3.is_file(follow_symlinks=False)\n',
         '                    files.setdefault(kind[1], {})[kind[0]] = e3.is_file()\n'),
        ('btc_trading_system/database/legacy.py',
         '        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)\n',
         '        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)\n'),
    ],
    '4 manifest re-serialised instead of copied byte for byte': [
        ('btc_trading_system/database/legacy.py',
         '    return _Captured(ds.key, mbytes, dbytes, manifest)\n',
         '    return _Captured(ds.key, vs.canonical_json_bytes(manifest), dbytes, manifest)\n'),
    ],
    '5 data file re-encoded through Phase 2 ParquetBackend instead of copied': [
        ('btc_trading_system/database/legacy.py',
         '    dbytes = _read_source_file(src / (rel + DATA_SUFFIX), rel + DATA_SUFFIX)\n',
         "    dbytes = _read_source_file(src / (rel + DATA_SUFFIX), rel + DATA_SUFFIX)\n    _t = Path(__import__('tempfile').mkdtemp()) / 'x.parquet'\n    ParquetBackend().write(_t, ParquetBackend().read(src / (rel + DATA_SUFFIX)))\n    dbytes = _t.read_bytes()\n"),
    ],
    '6 existing ref to another version is moved (kept policy removed)': [
        ('btc_trading_system/database/legacy.py',
         '    keep = cur is not None and cur.ref.data_version != dv\n',
         '    keep = False\n'),
    ],
    '7 ref moved before the import record is written': [
        ('btc_trading_system/database/legacy.py',
         '        if not has_record:                                               # E: post före ref\n            _publish_record(store, rec)\n        if keep:                                                         # F\n            ref_outcome = REF_KEPT\n        else:\n            ref_outcome = REF_SET if w._move_ref(key, dv, "import_legacy").changed else REF_UNCHANGED\n',
         '        if keep:\n            ref_outcome = REF_KEPT\n        else:\n            ref_outcome = REF_SET if w._move_ref(key, dv, "import_legacy").changed else REF_UNCHANGED\n        if not has_record:\n            _publish_record(store, rec)\n'),
    ],
    '8 ref_log action set_current instead of import_legacy': [
        ('btc_trading_system/database/legacy.py',
         'w._move_ref(key, dv, "import_legacy")',
         'w._move_ref(key, dv, "set_current")'),
    ],
    '9 apply defaults to True (dry-run not the default)': [
        ('btc_trading_system/database/legacy.py',
         'source_root: str | Path, *, apply: bool = False',
         'source_root: str | Path, *, apply: bool = True'),
    ],
    '10 writer lock not taken': [
        ('btc_trading_system/database/legacy.py',
         '    with store.writer() as w:\n',
         "    with __import__('contextlib').nullcontext(vs.StoreWriter(store, catalog.open_catalog(store.catalog_path), type('L', (), {'held': True})())) as w:\n"),
    ],
    '11 existing object reused without full verification (sha256 bypassed)': [
        ('btc_trading_system/database/legacy.py',
         '        info, stored, _ = store._verify_object(key, dv)\n        conflicts = ',
         '        _real = vs.sha256_file\n        vs.sha256_file = lambda p: json.loads((p.parent / vs.VERSION_FILE).read_bytes())["data_file"]["sha256"]\n        try:\n            info, stored, _ = store._verify_object(key, dv)\n        finally:\n            vs.sha256_file = _real\n        conflicts = '),
    ],
    '12 reuse compares source fields (sources, duplicates_dropped)': [
        ('btc_trading_system/database/legacy.py',
         '    return sorted(k for k in COMPARED_MANIFEST_KEYS if legacy.get(k) != stored.get(k))\n',
         '    return sorted(k for k in ALLOWED_MANIFEST_KEYS if legacy.get(k) != stored.get(k))\n'),
    ],
    '13 import record published with replace instead of link': [
        ('btc_trading_system/database/legacy.py',
         '            vs._link(staged, path)                          # importpost: aldrig överskrivning\n',
         '            os.replace(staged, path)\n'),
    ],
    '14 verify misses an import_legacy ref_log entry without a record': [
        ('btc_trading_system/database/integrity.py',
         '                if entry.action == "import_legacy" \\\n',
         '                if False \\\n'),
    ],
    '15 source tree hash after-check removed': [
        ('btc_trading_system/database/legacy.py',
         '    if after != before:                                  # källan ändrades: väger tyngst\n',
         '    if False:\n'),
    ],
    '16 empty-source check removed (zero recognised datasets)': [
        ('btc_trading_system/database/legacy.py',
         '    elif not found:                                      # inga igenkända dataset: aldrig ok\n',
         '    elif False:\n'),
    ],
}
