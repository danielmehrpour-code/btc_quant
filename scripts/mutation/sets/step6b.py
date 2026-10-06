"""Mutationsmängd steg 6b: registrering av kvalitetsrapporter.

Mönstren är ordagranna från ~/phase4_work/mutation/step6b_mutate.py (P4-S11-b).
"""
NAME = "step6b"
TESTS = ('tests/test_database_quality_registry.py', 'tests/test_database_integrity.py')
MUTATIONS = {
    '1 config hash check removed': [
        ('btc_trading_system/database/quality_registry.py',
         '        problem = config_problem(rep)                                       # D\n',
         '        problem = None\n'),
    ],
    '2 target object reused without full verification (sha256 bypassed)': [
        ('btc_trading_system/database/quality_registry.py',
         '            info, manifest, frame = self.store._verify_object(ref.key, ref.data_version)   # kastar\n',
         '            _real = vs.sha256_file\n            vs.sha256_file = lambda p: json.loads((p.parent / vs.VERSION_FILE).read_bytes())["data_file"]["sha256"]\n            try:\n                info, manifest, frame = self.store._verify_object(ref.key, ref.data_version)\n            finally:\n                vs.sha256_file = _real\n'),
    ],
    '3 binding fields (rows/first/last) not compared': [
        ('btc_trading_system/database/quality_registry.py',
         '            if have != binding:\n',
         '            if False:\n'),
    ],
    '4 reproduction skipped': [
        ('btc_trading_system/database/quality_registry.py',
         '        self.reproduce(rep, obj, data, targets)                             # F\n',
         '        pass\n'),
    ],
    '5 canonical-bytes check removed': [
        ('btc_trading_system/database/quality_registry.py',
         '    if canonical != data:\n',
         '    if False:\n'),
    ],
    '6 symlinks followed (discovery and O_NOFOLLOW; two layers)': [
        ('btc_trading_system/database/quality_registry.py',
         '            elif not (e.is_file(follow_symlinks=False) and md.is_file(follow_symlinks=False)):\n',
         '            elif not (e.is_file() and md.is_file()):\n'),
        ('btc_trading_system/database/quality_registry.py',
         '        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)\n',
         '        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)\n'),
    ],
    '7 quality file published with replace instead of link': [
        ('btc_trading_system/database/quality_registry.py',
         '            vs._link(staged, path)                          # rapportfil: aldrig överskrivning\n',
         '            os.replace(staged, path)\n'),
    ],
    '8 existing stored file accepted without re-verification': [
        ('btc_trading_system/database/quality_registry.py',
         '            _read_quality_file(path_q)                                      # skadad -> kastar\n',
         '            pass\n'),
    ],
    '9 catalog rows inserted before the file is durable': [
        ('btc_trading_system/database/quality_registry.py',
         '    if not p.file_exists:                                                   # 2-4\n        _publish_file(store, p)\n    if not p.catalog_exists:                                                # 5\n        _insert_rows(w._conn, p.rep)\n',
         '    if not p.catalog_exists:\n        _insert_rows(w._conn, p.rep)\n    if not p.file_exists:\n        _publish_file(store, p)\n'),
    ],
    '10 apply defaults to True (dry-run not the default)': [
        ('btc_trading_system/database/quality_registry.py',
         '                             apply: bool = False) -> QualityRegistrationReport:',
         '                             apply: bool = True) -> QualityRegistrationReport:'),
    ],
    '11 writer lock not taken': [
        ('btc_trading_system/database/quality_registry.py',
         '    with store.writer() as w:\n',
         "    with __import__('contextlib').nullcontext(vs.StoreWriter(store, catalog.open_catalog(store.catalog_path), type('L', (), {'held': True})())) as w:\n"),
    ],
    '12 source tree hash after-check removed': [
        ('btc_trading_system/database/quality_registry.py',
         '    if after != before:                                  # källan ändrades: väger tyngst\n',
         '    if False:\n'),
    ],
    '13 empty-source check removed (zero recognised reports)': [
        ('btc_trading_system/database/quality_registry.py',
         '    elif not found:                                      # inga igenkända rapporter: aldrig ok\n',
         '    elif False:\n'),
    ],
    '14 verify misses QUALITY_TARGET_MISSING': [
        ('btc_trading_system/database/integrity.py',
         '                if not self._object_exists(did, dv):\n                    self.add("QUALITY_TARGET_MISSING"',
         '                if False:\n                    self.add("QUALITY_TARGET_MISSING"'),
    ],
    '15 Markdown check removed': [
        ('btc_trading_system/database/quality_registry.py',
         '        if md != qr.report_to_markdown(obj).encode():                       # C\n',
         '        if False:\n'),
    ],
    '16 duplicates_dropped provenance check removed': [
        ('btc_trading_system/database/quality_registry.py',
         '            if not _is_int(dd) or dd not in known:\n',
         '            if False:\n'),
    ],
}
