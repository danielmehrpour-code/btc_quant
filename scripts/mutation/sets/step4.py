"""Mutationsmängd steg 4: ingest med lineage.

Mönstren är ordagranna från ~/phase4_work/mutation/step4_mutate.py (P4-S11-b).
"""
NAME = "step4"
TESTS = ('tests/test_database_ingest.py',)
MUTATIONS = {
    '1 source fields sent into the object manifest': [
        ('btc_trading_system/database/lineage.py',
         '    content = {k: v for k, v in extra.items() if k not in SOURCE_KEYS}\n',
         '    content = dict(extra)\n'),
        ('btc_trading_system/database/lineage.py',
         '    if unknown:\n        raise IngestExtraError(f"extra till objektet',
         '    if False:\n        raise IngestExtraError(f"extra till objektet'),
    ],
    '2 content-key-only check removed': [
        ('btc_trading_system/database/lineage.py',
         '    if unknown:\n        raise IngestExtraError(f"extra till objektet',
         '    if False:\n        raise IngestExtraError(f"extra till objektet'),
    ],
    '3 reuse does not compare content keys': [
        ('btc_trading_system/database/versioned_store.py',
         '            if stored != mine:\n',
         '            if False:\n'),
    ],
    '4 repeated identical ingest writes no record': [
        ('btc_trading_system/database/versioned_store.py',
         '        record_sha = self._publish_ingest_record(rec)\n',
         '        record_sha = self._publish_ingest_record(rec) if entry is not None else ""\n'),
    ],
    '5 ingest_id without seq/prev_ingest_id': [
        ('btc_trading_system/database/lineage.py',
         '        return hashlib.sha256(INGEST_ID_DOMAIN + compact_json_bytes(self.to_json_obj())).hexdigest()\n',
         '        return hashlib.sha256(INGEST_ID_DOMAIN + compact_json_bytes(\n            {k: v for k, v in self.to_json_obj().items() if k not in ("seq", "prev_ingest_id")})).hexdigest()\n'),
    ],
    '6 ref moved before the ingest record': [
        ('btc_trading_system/database/versioned_store.py',
         '        record_sha = self._publish_ingest_record(rec)\n        # 6) ref via steg 3 (logg före ref)\n        if line is not None and entry is not None:\n            self._append_ref_log(state, line, entry)\n            self._write_ref_file(target, entry)\n',
         '        # 6) ref via steg 3 (logg före ref)\n        if line is not None and entry is not None:\n            self._append_ref_log(state, line, entry)\n            self._write_ref_file(target, entry)\n        record_sha = self._publish_ingest_record(rec)\n'),
    ],
    '7 reused object not verified before the ref moves': [
        ('btc_trading_system/database/versioned_store.py',
         '            info, manifest, _ = store._verify_object(key, dv)   # ingest: aldrig blint återbruk\n',
         '            return "reused"\n'),
    ],
    '8 inserted _load_ref_state line removed': [
        ('btc_trading_system/database/versioned_store.py',
         '        self._check_ingests(entries)                     # steg 4: väntande/inkonsistent ingest\n',
         ''),
    ],
    '9 complete_pending_ingest skips previous-version check': [
        ('btc_trading_system/database/versioned_store.py',
         '        if rec.previous_data_version != (cur.ref.data_version if cur is not None else None):\n',
         '        if False:\n'),
    ],
    '10 record chain check (prev_ingest_id) off': [
        ('btc_trading_system/database/versioned_store.py',
         '            if rec.prev_ingest_id != prev:\n',
         '            if False:\n'),
    ],
    '11 record publication uses replace, not link': [
        ('btc_trading_system/database/versioned_store.py',
         '                _link(staged, path)                               # ingest-post: aldrig överskrivning\n',
         '                os.replace(staged, path)\n'),
    ],
    '12 fail_on_gaps not passed to Phase 2': [
        ('btc_trading_system/database/versioned_store.py',
         '                       now=now, fail_on_gaps=fail_on_gaps)\n',
         '                       now=now, fail_on_gaps=False)\n'),
    ],
    '13 regression guard removed': [
        ('btc_trading_system/database/versioned_store.py',
         '        if previous is not None and previous != dv and not allow_regression:\n',
         '        if False:\n'),
    ],
    '14 capture store allows a second write': [
        ('btc_trading_system/database/versioned_store.py',
         '        if self._writes != 1:\n',
         '        if False:\n'),
    ],
    '15 ingest catalog not synced after completing a pending ref update (fix removed)': [
        ('btc_trading_system/database/versioned_store.py',
         '        if e.action == "ingest":                         # steg 4-fix: ingest-postens katalograd utan fördröjning\n            self._sync_ingest_catalog(store._load_ingests())\n',
         ''),
    ],
}
