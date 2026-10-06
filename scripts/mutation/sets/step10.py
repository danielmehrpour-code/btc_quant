"""Mutationsmängd steg 10: testluckor.

Mönstren är ordagranna från ~/phase4_work/mutation/step10_mutate.py (P4-S11-b).
"""
NAME = "step10"
TESTS = ('tests/test_database_integrity.py', 'tests/test_database_ingest.py', 'tests/test_database_refs.py', 'tests/test_database_legacy.py', 'tests/test_database_cli.py')
MUTATIONS = {
    '1 rebuild refuses only OBJECT_CORRUPT (error filter narrowed)': [
        ('btc_trading_system/database/integrity.py',
         '        errors = tuple(x for x in report.findings if x.severity == "ERROR")\n',
         '        errors = tuple(x for x in report.findings if x.code == "OBJECT_CORRUPT")\n'),
    ],
    '2 rebuild symlinked-lock check removed': [
        ('btc_trading_system/database/integrity.py',
         '    if lock_file.is_symlink():\n        raise vs.StoreError(f"{vs.LOCK_FILE} är en symlänk; rebuild vägrar")\n',
         ''),
    ],
    '3 rebuild unlinks the catalog before rebuilding (non-atomic)': [
        ('btc_trading_system/database/integrity.py',
         '        _rebuild_catalog(store, rows)\n',
         '        os.unlink(store.catalog_path)\n        _rebuild_catalog(store, rows)\n'),
    ],
    '4 rebuild removes tmp/ leftovers': [
        ('btc_trading_system/database/integrity.py',
         '        rebuilt, _ = _verify(store, best_effort=False)\n',
         '        for rel in plan_repair(report).tmp_removals:\n            _remove_tmp_entry(store, rel)\n        rebuilt, _ = _verify(store, best_effort=False)\n'),
    ],
    '5 complete_pending_ingest skips the pending-ref check': [
        ('btc_trading_system/database/versioned_store.py',
         '        if state.pending is not None:\n            raise PendingRefUpdateError("väntande ref-uppdatering; anropa complete_pending_ref_updates() först")\n        records = store._load_ingests()\n        if not records:\n            return None\n',
         '        records = store._load_ingests()\n        if not records:\n            return None\n'),
    ],
    '6 complete_pending_ingest skips the planned-line check': [
        ('btc_trading_system/database/versioned_store.py',
         '        if entry.seq != rec.ref_log_seq or entry.entry_sha256 != rec.ref_log_entry_sha256:\n            raise IngestCorruptError(f"ingest seq {rec.seq}: planerad ref_log-rad kan inte reproduceras")\n',
         ''),
    ],
    '7 changed immutable catalog row classified CATALOG_BEHIND': [
        ('btc_trading_system/database/integrity.py',
         '                self.add("CATALOG_CONFLICT", subject, f"katalogen {h!r} != filerna {w!r}")\n',
         '                self.add("CATALOG_BEHIND", subject, f"katalogen {h!r} != filerna {w!r}")\n'),
    ],
    '8 verify ignores symlinks under refs/': [
        ('btc_trading_system/database/integrity.py',
         '                self.add("SYMLINK", subject, "symlänk under refs/")\n',
         '                pass\n'),
    ],
    '9 DEV2 Phase 2 read of the captured bytes in tmp/ removed': [
        ('btc_trading_system/database/legacy.py',
         '        _phase2_read(p2, cap)\n',
         ''),
    ],
    '10 ref_log parse accepts an unknown action': [
        ('btc_trading_system/database/versioned_store.py',
         '        if obj["action"] not in REF_ACTIONS:\n            raise RefCorruptError(f"ref_log rad {i}: okänd action {obj[\'action\']!r}")\n',
         ''),
    ],
    '11 CLI snapshot dry-run ref check removed': [
        ('btc_trading_system/database/cli.py',
         '                if cur.ref is None:\n                    raise vs.NoCurrentRefError(f"ingen aktuell ref för {key.relpath}")\n',
         ''),
    ],
}
