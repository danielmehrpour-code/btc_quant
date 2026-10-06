"""Mutationsmängd steg 11: luckor mot spec-listan (N1-N4) och körarens säkerhet (R1-R2).

N1-N4 täcker "foreign keys disabled", "lock bypass" i själva skrivarlåset och "ref publication before
durable object publication" (P4-S11-a). R1-R2 bevisar att säkerhetstesterna i
tests/test_mutation_runner.py fångar en borttagen vägran. TESTS anger säkerhetstesterna som nod-id:n,
inte hela filen: mönsterkontrollen i samma fil fallerar för varje mutation och skulle annars döda alla
mutationer trivialt.
"""
NAME = "step11"
TESTS = ("tests/test_database_catalog.py", "tests/test_database_versioned_store.py",
         "tests/test_database_ingest.py",
         "tests/test_mutation_runner.py::test_unsafe_overlays_are_refused_and_nothing_is_written",
         "tests/test_mutation_runner.py::test_unsafe_scratch_roots_are_refused",
         "tests/test_mutation_runner.py::test_a_proper_overlay_is_accepted")
MUTATIONS = {
    "N1 foreign keys disabled": [
        ("btc_trading_system/database/catalog.py",
         ('    conn.execute("PRAGMA foreign_keys = ON")\n'
          '    if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1:\n'
          '        raise CatalogError("foreign_keys kunde inte aktiveras")\n'),
         '    conn.execute("PRAGMA foreign_keys = OFF")\n'),
    ],
    "N2 writer lock does not flock": [
        ("btc_trading_system/database/versioned_store.py",
         "            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\n",
         "            pass\n"),
    ],
    "N3 ingest publishes the ref before the object": [
        ("btc_trading_system/database/versioned_store.py",
         "        publication = self._publish_ingest_version(key, canon, dv, content)\n",
         '        publication = "reused" if store.has_version(key, dv) else "published"\n'),
        ("btc_trading_system/database/versioned_store.py",
         "            self._write_ref_file(target, entry)\n        # 7) katalog sist\n",
         ("            self._write_ref_file(target, entry)\n"
          "        self._publish_ingest_version(key, canon, dv, content)\n        # 7) katalog sist\n")),
    ],
    "N4 object files not fsynced before the rename": [
        ("btc_trading_system/database/versioned_store.py",
         "            _fsync_file(obj / name)\n",
         "            pass\n"),
    ],
    "R1 runner refusal of the repository working tree removed": [
        ("scripts/mutation/runner.py",
         ("    if o == r or r in o.parents or o in r.parents:\n"
          '        raise MutationSafetyError(f"overlayn {o} är, ligger i eller innehåller repots arbetskatalog {r}")\n'),
         ""),
    ],
    "R2 runner scratch-root check removed": [
        ("scripts/mutation/runner.py",
         ("    if s not in o.parents:\n"
          '        raise MutationSafetyError(f"overlayn {o} ligger inte i --scratch {s}")\n'),
         ""),
    ],
}
