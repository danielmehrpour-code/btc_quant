"""Mutationsmängd etapp B steg 4: funding i datalagret (SB-S4-a..f) och explicit katalogmigrering (SB-S4-MIG).

Varje mutation tar bort eller försvagar en regel: ingen automatisk migrering och ingen migrering via repair/rebuild,
migration 1 orörd, snapshot v2 pinnar funding, available_at = calc_time + 15 s, halvöppna intervall, exakt ränta,
dubbletter, källformat, egen id-domän, verify av funding-objekt, proveniens och kvalitetstoleransen.
"""
NAME = "stageb4"
TESTS = ("tests/test_funding_data.py", "tests/test_database_funding.py", "tests/test_catalog_migration.py",
         "tests/test_funding_quality.py", "tests/test_database_catalog.py")
CAT = "btc_trading_system/database/catalog.py"
FD = "btc_trading_system/data/funding.py"
REFS = "btc_trading_system/database/refs.py"
VS = "btc_trading_system/database/versioned_store.py"
IG = "btc_trading_system/database/integrity.py"
FQ = "btc_trading_system/data_validation/funding_quality.py"
MUTATIONS = {
    "1 automatic migration reintroduced on open": [
        (CAT, ('            raise CatalogMigrationRequiredError(\n'
               '                f"{path.name}: schemaversion {version} kräver migrering till {len(MIGRATIONS)} "\n'
               '                "(migrate_catalog; CLI: migrate-catalog --apply)")\n'),
         ('            for m in MIGRATIONS[version:]:\n'
          '                _apply(conn, m)\n')),
    ],
    "2 repair and rebuild may migrate (refusal removed)": [
        (IG, '    if "CATALOG_MIGRATION_REQUIRED" in report.codes:\n', '    if False:\n'),
        (IG, ('        except catalog.CatalogMigrationRequiredError as e:  # efter rollback: bara äldre schema\n'
              '            raise vs.StoreError(f"{e}; ombyggnaden vägrar, kör migrate-catalog (SB-S4-MIG)") from e\n'),
         ''),
    ],
    "3 migration 2 rewrites a migration 1 table": [
        (CAT, '_V2_SQL = (_V2_TABLES.strip() + "\\n\\n"\n',
         '_V2_SQL = ("DROP TRIGGER snapshots_no_update;\\n" + _V2_TABLES.strip() + "\\n\\n"\n'),
    ],
    "4 snapshot v2 identity does not pin funding": [
        (REFS, '        if self.funding:                                  # v2 (SB-S4-a A2)\n', '        if False:\n'),
    ],
    "5 snapshot funding members not verified": [
        (VS, '        for f in snap.funding:                           # v2: varje funding-version verifieras fullt\n',
         '        for f in ():\n'),
    ],
    "6 available_at without the 15 s margin": [
        (FD, 'AVAILABLE_LAG = timedelta(seconds=15)                      # SB-S4-c A (Binances FAQ, inte datan)\n',
         'AVAILABLE_LAG = timedelta(0)\n'),
    ],
    "7 as_of boundary exclusive (row with available_at == as_of dropped)": [
        (VS, '        stop = min(stop, int(calc.searchsorted(fd.funding_cutoff(as_of), side="right")))\n',
         '        stop = min(stop, int(calc.searchsorted(fd.funding_cutoff(as_of), side="left")))\n'),
        (VS, '    return frame.loc[frame["calc_time"] <= fd.funding_cutoff(as_of)].reset_index(drop=True)\n',
         '    return frame.loc[frame["calc_time"] < fd.funding_cutoff(as_of)].reset_index(drop=True)\n'),
    ],
    "8 range end inclusive": [
        (VS, '    stop = int(calc.searchsorted(pd.Timestamp(end_ts), side="left"))\n',
         '    stop = int(calc.searchsorted(pd.Timestamp(end_ts), side="right"))\n'),
    ],
    "9 rate parsed through float": [
        (FD, '            scaled = Decimal(text).scaleb(RATE_SCALE_EXPONENT)\n',
         '            scaled = Decimal(float(text) * 10**RATE_SCALE_EXPONENT)\n'),
    ],
    "10 exact-multiple check removed (silent truncation)": [
        (FD, '        if scaled != scaled.to_integral_value():\n', '        if False:\n'),
    ],
    "11 conflicting duplicates accepted (first kept)": [
        (FD, '    conflict = unique["calc_time"].duplicated(keep=False)\n',
         ('    unique = unique.drop_duplicates("calc_time").reset_index(drop=True)\n'
          '    conflict = unique["calc_time"].duplicated(keep=False)\n')),
    ],
    "12 source header not checked": [
        (FD, '    if not lines or lines[0] != SOURCE_HEADER:\n', '    if not lines:\n'),
    ],
    "13 funding dataset_id uses the OHLCV domain tag": [
        (CAT, '    return hashlib.sha256(FUNDING_DATASET_ID_DOMAIN + funding_dataset_key_json(key).encode("ascii")).hexdigest()\n',
         '    return hashlib.sha256(DATASET_ID_DOMAIN + funding_dataset_key_json(key).encode("ascii")).hexdigest()\n'),
    ],
    "14 verify skips funding objects": [
        (IG, '                self._check_funding_object(d, parts, subject)\n', ''),
    ],
    "15 funding reuse without comparing the sources": [
        (VS, '            if stored != json.loads(json.dumps(manifest)):\n', '            if False:\n'),
    ],
    "16 coverage tolerance ignored": [
        (FQ, '    bad_cov = np.nonzero(np.abs(d - expected) > cfg.spacing_tolerance_ms)[0]\n',
         '    bad_cov = np.nonzero(np.abs(d - expected) > 0)[0]\n'),
    ],
}
