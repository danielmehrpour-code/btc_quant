"""Mutationsmängd steg 5: verify och repair.

Mönstren är ordagranna från ~/phase4_work/mutation/step5_mutate.py (P4-S11-b).
"""
NAME = "step5"
TESTS = ('tests/test_database_integrity.py',)
MUTATIONS = {
    '1 verify trusts version.json instead of hashing (SHA bypassed)': [
        ('btc_trading_system/database/integrity.py',
         '                info, manifest, _ = self.store._verify_object(key, dv)\n',
         '                real_sha = vs.sha256_file\n                vs.sha256_file = lambda p: json.loads((p.parent / vs.VERSION_FILE).read_bytes())["data_file"]["sha256"]\n                try:\n                    info, manifest, _ = self.store._verify_object(key, dv)\n                finally:\n                    vs.sha256_file = real_sha\n'),
    ],
    '2 verify opens the lock file read-write with O_CREAT': [
        ('btc_trading_system/database/integrity.py',
         '        fd = os.open(store.root / vs.LOCK_FILE, os.O_RDONLY | os.O_NOFOLLOW)\n',
         '        fd = os.open(store.root / vs.LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o644)\n'),
    ],
    '3 tmp pattern check matches every file': [
        ('btc_trading_system/database/integrity.py',
         '    return stat.S_ISREG(st.st_mode) and _TMP_FILE_RE.fullmatch(path.name) is not None\n',
         '    return stat.S_ISREG(st.st_mode)\n'),
    ],
    '4 tmp cleanup chmods files before unlinking': [
        ('btc_trading_system/database/integrity.py',
         '        elif stat.S_ISREG(pst.st_mode):\n            os.unlink(p)\n',
         '        elif stat.S_ISREG(pst.st_mode):\n            os.chmod(p, 0o644)\n            os.unlink(p)\n'),
    ],
    '5 repair proceeds despite ERROR findings': [
        ('btc_trading_system/database/integrity.py',
         '        if plan.blocking:\n',
         '        if False:\n'),
    ],
    '6 repair applies by default': [
        ('btc_trading_system/database/integrity.py',
         'def repair_store(root: str | Path, *, apply: bool = False) -> RepairResult:\n',
         'def repair_store(root: str | Path, *, apply: bool = True) -> RepairResult:\n'),
    ],
    '7 repair runs without the writer lock': [
        ('btc_trading_system/database/integrity.py',
         '    lock.acquire()\n',
         ''),
    ],
    '8 repair completes pending operations implicitly': [
        ('btc_trading_system/database/integrity.py',
         '        after, _ = _verify(store, best_effort=False)\n',
         '        conn = catalog.open_catalog(store.catalog_path)\n        w = vs.StoreWriter(store, conn, lock)\n        w.complete_pending_ref_updates()\n        w.complete_pending_ingest()\n        conn.close()\n        after, _ = _verify(store, best_effort=False)\n'),
    ],
    '9 derivation includes a pending ingest': [
        ('btc_trading_system/database/integrity.py',
         '            if rec is pending_ingest or (',
         '            if ('),
    ],
    '10 CATALOG_CONFLICT classified REPAIRABLE': [
        ('btc_trading_system/database/integrity.py',
         '    "CATALOG_CONFLICT": "ERROR",\n',
         '    "CATALOG_CONFLICT": "REPAIRABLE",\n'),
    ],
    '11 repair unlinks the catalog before rebuilding': [
        ('btc_trading_system/database/integrity.py',
         '    catalog.rebuild_catalog(path, _populate(derived), staging_dir=store.tmp)\n',
         '    path.unlink(missing_ok=True)\n    catalog.rebuild_catalog(path, _populate(derived), staging_dir=store.tmp)\n'),
    ],
    '12 exit status: ERROR mapped below PENDING': [
        ('btc_trading_system/database/integrity.py',
         'SEVERITY_EXIT = {"ERROR": 3,',
         'SEVERITY_EXIT = {"ERROR": 1,'),
    ],
    '13 findings not sorted (insertion order)': [
        ('btc_trading_system/database/integrity.py',
         '            findings=tuple(sorted(self.findings, key=Finding.sort_key)),\n',
         '            findings=tuple(self.findings),\n'),
    ],
    '14 tmp cleanup follows symlinks out of tmp/': [
        ('btc_trading_system/database/integrity.py',
         '    st = os.lstat(path)\n    if stat.S_ISLNK(st.st_mode):\n        return False\n',
         '    st = os.stat(path)\n'),
        ('btc_trading_system/database/integrity.py',
         '    if stat.S_ISDIR(os.lstat(path).st_mode):\n        _remove_plain_tree(path)\n',
         '    if stat.S_ISDIR(os.stat(path).st_mode):\n        __import__("shutil").rmtree(path.resolve())\n'),
        ('btc_trading_system/database/integrity.py',
         '        os.unlink(path)                                     # namnet, aldrig innehåll eller läge\n',
         '        os.unlink(path.resolve())\n'),
    ],
}
