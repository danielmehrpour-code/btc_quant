"""Mutationsmängd steg 8: datastore-CLI.

Mönstren är ordagranna från ~/phase4_work/mutation/step8_mutate.py (P4-S11-b).
"""
NAME = "step8"
TESTS = ('tests/test_database_cli.py',)
MUTATIONS = {
    '1 --apply default flipped to True': [
        ('btc_trading_system/database/cli.py',
         '    p.add_argument("--apply", action="store_true", default=False,\n',
         '    p.add_argument("--apply", action="store_true", default=True,\n'),
    ],
    '2 verify: ERROR report mapped to exit 0': [
        ('btc_trading_system/database/cli.py',
         '    return _Out(rep.to_json_obj(), rep.exit_status, rep.report_sha256)\n',
         '    return _Out(rep.to_json_obj(), 0 if rep.exit_status == 3 else rep.exit_status, rep.report_sha256)\n'),
    ],
    '3 regression guard always on (flag ignored)': [
        ('btc_trading_system/database/cli.py',
         '                       fail_on_gaps=a.fail_on_gaps, allow_regression=a.allow_regression)\n',
         '                       fail_on_gaps=a.fail_on_gaps, allow_regression=False)\n'),
    ],
    '4 regression guard always off': [
        ('btc_trading_system/database/cli.py',
         '                       fail_on_gaps=a.fail_on_gaps, allow_regression=a.allow_regression)\n',
         '                       fail_on_gaps=a.fail_on_gaps, allow_regression=True)\n'),
    ],
    '5 writer-lock bypass (StoreWriter around a lock that claims to be held, no flock)': [
        ('btc_trading_system/database/cli.py',
         '    with store.writer() as w:\n        yield w\n',
         '    fake = vs._WriterLock(store.root / vs.LOCK_FILE)\n    fake._fd = -1\n    conn = catalog.open_catalog(store.catalog_path)\n    try:\n        yield vs.StoreWriter(store, conn, fake)\n    finally:\n        conn.close()\n'),
    ],
    '6 now not resolved from the clock (constant passed)': [
        ('btc_trading_system/database/cli.py',
         '    now = ctx.clock()\n',
         '    now = datetime(2030, 1, 1, tzinfo=UTC)\n'),
    ],
    "7 code_version hard-coded 'unknown'": [
        ('btc_trading_system/database/cli.py',
         '    code_version, clean = ctx.git()\n',
         '    code_version, clean = "unknown", ctx.git()[1]\n'),
    ],
    '8 worktree_clean dropped (always None)': [
        ('btc_trading_system/database/cli.py',
         'code_version=code_version, worktree_clean=clean,\n',
         'code_version=code_version, worktree_clean=None,\n'),
    ],
    '9 catch-all error handler returns 0': [
        ('btc_trading_system/database/cli.py',
         '        return _emit(fmt, _envelope(command, applied, EXIT_INTERNAL, ctx.resolved, None,\n',
         '        return _emit(fmt, _envelope(command, applied, EXIT_OK, ctx.resolved, None,\n'),
    ],
    '10 usage errors exit 2 (argparse default)': [
        ('btc_trading_system/database/cli.py',
         '        return _emit(fmt, _envelope(None, False, EXIT_USAGE, None, None, {"type": "UsageError", "message": str(e)}))\n',
         '        return _emit(fmt, _envelope(None, False, 2, None, None, {"type": "UsageError", "message": str(e)}))\n'),
    ],
    '11 register-report ignores --apply': [
        ('btc_trading_system/database/cli.py',
         '    rep = qreg.register_quality_reports(vs.VersionedStore(a.store), a.source, apply=a.apply)\n',
         '    rep = qreg.register_quality_reports(vs.VersionedStore(a.store), a.source, apply=False)\n'),
    ],
    '12 set-current acts without --apply': [
        ('btc_trading_system/database/cli.py',
         '    if not a.apply:                                   # förhandsvisning: målet verifieras först vid --apply\n',
         '    if False:\n'),
    ],
    '13 path cleaning in messages removed': [
        ('btc_trading_system/database/cli.py',
         '            text = text.replace(form, tag)\n',
         '            pass\n'),
    ],
    '14 rebuild proceeds despite an ERROR finding': [
        ('btc_trading_system/database/integrity.py',
         '        if errors:\n            raise RepairRefusedError(\n                f"rebuild vägrar: {len(errors)} ERROR-fynd; ingenting ändrades", report)\n',
         ''),
    ],
    '15 clean-worktree check removed': [
        ('btc_trading_system/database/cli.py',
         '    if clean is not True:\n',
         '    if False:\n'),
    ],
}
