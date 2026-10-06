"""Mutationsmängd steg 3: refs, ref_log, snapshots och PinnedStore.

Mönstren är ordagranna från ~/phase4_work/mutation/step3_mutate.py (P4-S11-b).
"""
NAME = "step3"
TESTS = ('tests/test_database_refs.py',)
MUTATIONS = {
    '1 ref written before log': [
        ('btc_trading_system/database/versioned_store.py',
         '        self._append_ref_log(state, line, entry)                         # 2) historik först\n        self._write_ref_file(target, entry)                              # 3) sedan ref\n',
         '        self._write_ref_file(target, entry)                              # 3) sedan ref\n        self._append_ref_log(state, line, entry)                         # 2) historik först\n'),
    ],
    '2 set_current skips object verification': [
        ('btc_trading_system/database/versioned_store.py',
         '        info, manifest, _ = store._verify_object(key, data_version)    # 1) aldrig overifierat mål\n        self._register(info, manifest)\n        target = DatasetVersionRef(key, data_version)\n',
         '        target = DatasetVersionRef(key, data_version)\n'),
    ],
    '3 PinnedStore.read follows current ref': [
        ('btc_trading_system/database/versioned_store.py',
         '        pin = self.version_of(key)\n        return self._store.read_version(pin.key, pin.data_version, as_of=as_of)\n',
         '        pin = self._store.current_ref(key).ref or self.version_of(key)\n        return self._store.read_version(pin.key, pin.data_version, as_of=as_of)\n'),
    ],
    '4 PinnedStore falls back outside snapshot': [
        ('btc_trading_system/database/versioned_store.py',
         '            raise DatasetNotInSnapshotError(f"{key.relpath} ingår inte i snapshot {self.snapshot_id}")\n',
         '            return self._store.current_ref(key).ref  # type: ignore[return-value]\n'),
    ],
    '5 PinnedStore write passthrough': [
        ('btc_trading_system/database/versioned_store.py',
         '    writer = _read_only\n',
         '    def writer(self, *a, **k):\n        return self._store.writer()\n'),
    ],
    '6 snapshot members unsorted (input order)': [
        ('btc_trading_system/database/refs.py',
         '        return cls(tuple(sorted(members, key=lambda m: m.dataset_id)))\n',
         '        return cls(tuple(members))\n'),
        ('btc_trading_system/database/refs.py',
         '        if ids != sorted(ids):\n',
         '        if False:\n'),
    ],
    '7 snapshot publish overwrites (replace, not link)': [
        ('btc_trading_system/database/versioned_store.py',
         '                _link(staged, path)\n',
         '                os.replace(staged, path)\n'),
    ],
    '8 ref_log rewrite drops old prefix': [
        ('btc_trading_system/database/versioned_store.py',
         '        new = current + line + b"\\n"\n',
         '        new = line + b"\\n"\n'),
    ],
    '9 ref_log chain check disabled': [
        ('btc_trading_system/database/versioned_store.py',
         '        if obj["prev_entry_sha256"] != prev:\n',
         '        if False:\n'),
    ],
    '10 PinnedStore.read ignores as_of': [
        ('btc_trading_system/database/versioned_store.py',
         '        return self._store.read_version(pin.key, pin.data_version, as_of=as_of)\n',
         '        return self._store.read_version(pin.key, pin.data_version, as_of=None)\n'),
    ],
    '12 PinnedStore accepts unpublished snapshot': [
        ('btc_trading_system/database/versioned_store.py',
         '        published = store.read_snapshot(snapshot.snapshot_id)   # SnapshotNotFoundError om opublicerad\n        if published != snapshot:\n            raise SnapshotCorruptError(f"snapshot {snapshot.snapshot_id}: publicerad fil avviker")\n',
         ''),
    ],
    '11 pending check skipped in set_current': [
        ('btc_trading_system/database/versioned_store.py',
         '        if state.pending is not None:\n            raise PendingRefUpdateError(\n                f"väntande ref-uppdatering (ref_log seq',
         '        if False:\n            raise PendingRefUpdateError(\n                f"väntande ref-uppdatering (ref_log seq'),
    ],
}
