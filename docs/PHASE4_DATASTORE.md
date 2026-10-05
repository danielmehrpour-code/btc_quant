# Phase 4 datastore: reference and operations runbook

This document describes the Phase 4 versioned datastore: what it stores, how it is identified,
which guarantees hold, what a crash leaves behind, and how to operate it through the CLI. It
describes; it decides nothing. The authoritative sources are `docs/PHASE4_SPEC.md` (scope) and
`docs/DECISIONS.md` (every locked decision); where this document and they differ, they win and
this document is wrong.

**References.** Every factual statement cites a code location or a decision ID. Code locations
are `file:line` at commit `168fe0d`. Bare module names (`versioned_store.py`, `integrity.py`,
`legacy.py`, `quality_registry.py`, `catalog.py`, `refs.py`, `lineage.py`, `cli.py`) are in
`btc_trading_system/database/`; tests are in `tests/`. Decision texts are not repeated here; the
IDs point into `docs/DECISIONS.md`.

**Status.** Steps 1-8 of `docs/PHASE4_SPEC.md` are implemented. The real-data migration (Step 12)
and the final verification (Step 13, V5) have not run yet; nothing here describes real data.

Contents
1. Concepts and identities
2. Store layout and file formats
3. The catalog
4. Invariants
5. Write operations: order, crash semantics, recovery
6. Reading
7. Verify, repair, rebuild
8. CLI reference
9. Operations runbook
10. Known limits and open items
11. Spec coverage

---

## 1. Concepts and identities

The store of record is a directory of immutable files; the SQLite catalog is derived from them and
can always be rebuilt (`docs/PHASE4_SPEC.md` "Core principles"; `catalog.py:1-5`).

| Concept | Meaning | Source |
|---|---|---|
| dataset | `exchange + instrument + symbol + timeframe`; exchange is always `binance`; fields must already be canonical, never normalised | `catalog.py:78`, P4-S1-02 |
| dataset version | one dataset at one `data_version`; an immutable object directory | `versioned_store.py:13-18` |
| current ref | which version of a dataset is current; changes only through an append-only log | `versioned_store.py:81-97`, P4-S1-05 |
| snapshot | a set of exact dataset versions, at most one per dataset | `refs.py:1-14`, P4-S3-D3 |
| ingest record | one immutable lineage record per ingest, also for identical content | `lineage.py:1-13`, P4-S4-e |
| import record | one content-addressed record per legacy-imported dataset version | `legacy.py:30-34`, P4-S6-a, P4-S6-b |
| registered quality report | a Phase 3 report stored byte for byte | `quality_registry.py:39-42`, P4-S6b-b |

### Identities and hashes

All hashes are SHA-256, lowercase, 64 hex characters (`catalog.py:104`). Identities are kept
separate and never conflated (`catalog.py:7-12`).

| Identity | Definition | Source |
|---|---|---|
| `dataset_id` | sha256(`btc-quant/dataset-id/v1\0` + canonical JSON of exchange, instrument, symbol, timeframe) | `catalog.py:47`, `catalog.py:91-101`, P4-S1-03 |
| `data_version` | Phase 2 logical content hash, definition unchanged | `btc_trading_system/data/storage.py:42`, P2-01 |
| `data_file_sha256` | sha256 of the physical `bars.parquet` | `versioned_store.py:13-18` |
| `manifest_sha256` | sha256 of the `manifest.json` bytes | `versioned_store.py:13-18` |
| `version_sha256` | sha256 of the `version.json` bytes | `versioned_store.py:13-18` |
| `entry_sha256` (ref_log) | sha256 of one ref_log line without its newline; lines form a hash chain | `versioned_store.py:1211-1217`, P4-S1-05 |
| `snapshot_id` | sha256(`btc-quant/snapshot-id/v1\0` + compact canonical JSON of the members sorted by `dataset_id`) | `refs.py:27`, `refs.py:99-104`, P4-S3-D3 |
| `ingest_id` | sha256(`btc-quant/ingest-id/v1\0` + compact canonical JSON of the record) | `lineage.py:27`, `lineage.py:193`, P4-S4-e |
| `import_id` | sha256(`btc-quant/legacy-import-id/v1\0` + compact canonical JSON of the record) | `legacy.py:71`, `legacy.py:173`, P4-S6-b |
| `report_sha256` | sha256 of the exact Phase 3 report bytes | `quality_registry.py:39-42`, P4-S6b-b |
| catalog `dump_sha256` | sha256 of a deterministic text dump (schema hash + all rows sorted) | `catalog.py:557-574`, P4-S1-06 |
| verify `report_sha256` | sha256 of the canonical verify report bytes | `integrity.py:213`, P4-S5-SCOPE |

---

## 2. Store layout and file formats

```text
<root>/STORE.json                                         store marker, written last by init
<root>/.lock                                              writer lock (flock)
<root>/objects/binance/<instrument>/<SYMBOL>/<tf>/<data_version>/
        bars.parquet  manifest.json  version.json         one immutable dataset version
<root>/refs/binance/<instrument>/<SYMBOL>/<tf>.json       current ref of one dataset
<root>/ref_log.jsonl                                      append-only, hash-chained ref history
<root>/snapshots/<snapshot_id>.json
<root>/ingests/<ingest_id>.json
<root>/imports/<import_id>.json                           extension of the spec layout (P4-S6-a)
<root>/quality/<report_sha256>.quality.json
<root>/tmp/                                               staging only
<root>/catalog.sqlite                                     derived index
```
Sources: `versioned_store.py:1-11`, `versioned_store.py:202-235`, `legacy.py:72`,
`quality_registry.py:70-71`, `integrity.py:158-160`; spec layout in `docs/PHASE4_SPEC.md`.

**Encodings.** "Canonical JSON" means sorted keys, indent 2, trailing newline
(`versioned_store.py:371-373`). "Compact canonical JSON" (used inside identities and ref_log
lines) is defined at `refs.py:32`. Every reader parses strictly and rejects a file whose bytes
are not exactly the canonical encoding (`refs.py:114-131`, `versioned_store.py:521-536`).

**Modes.** Published files are `0444`, object directories `0555`; no API operation mutates an
object (`versioned_store.py:212-213`, P4-S2-01). `STORE.json` is checked as a regular,
non-symlinked file, not by mode (P4-S5-DEV1).

| Path | Format | Written by | Source |
|---|---|---|---|
| `STORE.json` | canonical JSON `{format: "btc-quant/versioned-store", layout_version: 1}`; a newer layout is refused | `VersionedStore.init` (linked last) | `versioned_store.py:639-668`, `versioned_store.py:670-688` |
| `.lock` | empty file; writers hold `flock(LOCK_EX\|LOCK_NB)`, verify holds `LOCK_SH\|LOCK_NB` on the existing file | `init`, writers | `versioned_store.py:599-624`, `integrity.py:861-882`, P4-S5-d |
| `objects/…/bars.parquet` | Phase 2 Parquet, format unchanged | Phase 2 `DatasetStore` in staging | `versioned_store.py:1067-1080` |
| `objects/…/manifest.json` | Phase 2 manifest; on the ingest path its non-base keys are only `gaps`, `zero_volume_bars`; legacy-imported manifests are byte-for-byte copies of the source | publish / ingest / import | `lineage.py:28-29`, P4-S4-01, P4-S6-d |
| `objects/…/version.json` | canonical JSON: `format` `btc-quant/dataset-version/v1`, `dataset_id`, `dataset`, `data_version`, `data_file {path, sha256, bytes}`, `manifest {path, sha256, bytes}`, `rows`, `first_open_time`, `last_open_time` | staging | `versioned_store.py:1081-1094` |
| `refs/…/<tf>.json` | canonical JSON: `format` `btc-quant/ref/v1`, `dataset`, `dataset_id`, `data_version`, `ref_log_seq`, `ref_log_entry_sha256`; `0444` | `set_current`, ingest, import | `versioned_store.py:521-542`, `versioned_store.py:1230-1246` |
| `ref_log.jsonl` | one compact canonical JSON line per entry: `seq` (1..n), `prev_entry_sha256`, `dataset`, `dataset_id`, `data_version`, `previous_data_version`, `action` ∈ {`ingest`, `set_current`, `import_legacy`}; replaced atomically as old bytes + one line | ref moves | `versioned_store.py:484-519`, `versioned_store.py:1211-1228`, `versioned_store.py:230` |
| `snapshots/<id>.json` | canonical JSON: `format` `btc-quant/snapshot/v1`, `snapshot_id`, `members` (sorted); published with `link`, never overwritten | `create_snapshot`, `snapshot_current` | `refs.py:110-131`, `versioned_store.py:1313-1338` |
| `ingests/<id>.json` | canonical JSON record: `seq`, `prev_ingest_id` chain, dataset version, `publication`, `ref_outcome`, `previous_data_version`, ref_log link, `sources`, `duplicates_dropped`, `params {now, fail_on_gaps, allow_regression}`, `code {version, worktree_clean}`; `0444`, `link` | `ingest` | `lineage.py:120-200`, `versioned_store.py:1483-1504` |
| `imports/<id>.json` | canonical JSON record: dataset version, sha256 and size of the source data file and manifest, the source manifest as exact text; no seq, clock, code version or path | `import_legacy` | `legacy.py:121-205`, `legacy.py:560-576`, P4-S6-b |
| `quality/<sha>.quality.json` | the source Phase 3 report, byte for byte; the `.md` is checked but not stored | `register_quality_reports` | `quality_registry.py:39-42`, `quality_registry.py:514-532`, P4-S6b-b, P4-S6b-d |
| `tmp/` | unique staging names only; known patterns listed in section 5.9 | every writer | `integrity.py:161-169`, P4-S5-j |
| `catalog.sqlite` | SQLite, see section 3 | writers, repair, rebuild | `catalog.py:1-27` |

---

## 3. The catalog

The catalog is a derived, rebuildable index (`catalog.py:1-5`). Rules (`catalog.py:14-27`):
STRICT tables, SQLite ≥ 3.37 required (P4-S1-01, `docs/ENVIRONMENT.md`); foreign keys on and
verified on every connection (`catalog.py:301-313`); rollback journal (`journal_mode=DELETE`),
`synchronous=FULL`, never WAL (P4-S1-07); schema versioning by `user_version` plus a hash of each
migration, newer schemas refused (`catalog.py:278-292`, P4-S1-08); every table append-only except
`refs`, which may be updated but not deleted (`catalog.py:119`, `catalog.py:273-274`, P4-S1-04); no
wall-clock time stored (P4-S1-06).

| Table | Content | Source |
|---|---|---|
| `datasets` | one row per dataset key | `catalog.py:136` |
| `dataset_versions`, `version_sources` | one row per object (hashes, sizes, rows, first/last open time, gaps) and its stored sources | `catalog.py:146`, `catalog.py:163` |
| `ref_log`, `refs` | mirror of `ref_log.jsonl` and the ref files; constraints enforce the chain and previous ≠ new | `catalog.py:174-199` |
| `ingest_events`, `ingest_sources` | mirror of the ingest records | `catalog.py:201`, `catalog.py:222` |
| `snapshots`, `snapshot_members` | mirror of the snapshot files | `catalog.py:231`, `catalog.py:237` |
| `quality_reports`, `quality_report_datasets` | rows derived from the stored report files | `catalog.py:245`, `catalog.py:257`, P4-S6b-b |
| `schema_migrations` | migration hashes | `catalog.py:130` |

Import records have no table; the files are the record (P4-S6-c).

**Catalog as witness.** Rows the files lack, or differing rows for immutable entities, are evidence
and are never rebuilt over (`integrity.py:15-22`, P4-S5-c). Derivation from files equals what a
writer has synced after completed operations; pending items are excluded (P4-S5-g, P4-S5-DEV4).

---

## 4. Invariants

| # | Invariant | Enforced at | Decisions |
|---|---|---|---|
| I1 | An object is never overwritten, its manifest and `version.json` never rewritten; an existing `data_version` is reused only after full verification | `versioned_store.py:20-23`, `versioned_store.py:1099-1111`, `versioned_store.py:720` | P4-S2-01, P4-S2-03, P4-S2-M2, P4-S4-01 |
| I2 | Every write holds the exclusive writer lock; a second writer fails at once with `StoreLockedError`; readers take no lock | `versioned_store.py:599-624`, `versioned_store.py:999-1010` | spec "ATOMIC PUBLISHING" |
| I3 | A ref never points to an object that was not durably published and fully verified first; the log is written before the ref file | `versioned_store.py:81-97`, `versioned_store.py:1162-1184` | P4-S3-D1 |
| I4 | A pending ref update or pending ingest blocks every ref and ingest operation until completed explicitly | `versioned_store.py:864-907`, `versioned_store.py:969-996` | P4-S3-D1, P4-S4-b |
| I5 | Snapshots pin exact versions; a `PinnedStore` never reads refs or the ref_log and refuses writes | `versioned_store.py:98-109`, `versioned_store.py:1545-1597` | P4-S3-D3, P4-S3-02 |
| I6 | `as_of=t` exposes only bars with `available_at <= t`; `as_of` never selects a version | `versioned_store.py:64-67`, `versioned_store.py:411-420`, `versioned_store.py:104-109` | P1-01, P4-S3-D4 |
| I7 | Backtests and research read only through a snapshot (`PinnedStore`), never `read_current` | `versioned_store.py:837-845` | P4-S3-D4, P4-S7-b |
| I8 | Every ingest gets its own record, including identical content; source lineage goes only to the record | `versioned_store.py:111-134` | P4-S4-e, P4-S4-01 |
| I9 | An ingest never makes an earlier-ending version current unless `allow_regression` is given | `versioned_store.py:1391-1398` | P4-S4-d |
| I10 | Sources (Phase 2 store, Phase 3 report directory) are never written; their tree hash is checked before and after | `legacy.py:3-8`, `quality_registry.py:3-8` | P4-S6-SELF, P4-S6b-m |
| I11 | Verify writes nothing and never repairs | `integrity.py:3-9` | P4-S5-d |
| I12 | Repair and rebuild refuse entirely while any ERROR finding exists; they touch only `tmp/` and `catalog.sqlite` | `integrity.py:24-36`, `integrity.py:49-55` | P4-S5-e, P4-S5-i, P4-S8-d |
| I13 | The CLI writes nothing without `--apply`; an ingest is recorded only from a clean worktree | `cli.py:26-33` | P4-S8-c, P4-S8-g |
| I14 | No wall clock and no absolute path in the catalog, reports or ref files; the only clock value is an ingest's `params.now` | `catalog.py:26`, `integrity.py:1-9` | P4-S1-06, P4-S5-SCOPE, P4-S4-c |

---

## 5. Write operations: order, crash semantics, recovery

General pattern: stage in `tmp/` under a unique name, fsync, publish with `rename` (directories,
replaced files) or `link` (never-overwritten files), fsync the parent, update the catalog last in
one transaction (`versioned_store.py:25-33`, `versioned_store.py:1235-1246`). The catalog is
written last everywhere, so a crash before it only leaves the catalog behind the files, which
verify reports as REPAIRABLE (P4-S5-c). The CLI adds no write path of its own; its crash semantics
are those of the library call it makes (P4-S8-c, `cli.py:312-315`).

"verify" below means the non-INFO findings after the crash; "recovery" is what returns the
store to exit 0. Most tests below inject an exception; a real process crash additionally skips the
`finally` clean-up of staging names (e.g. `versioned_store.py:1052-1055`, `versioned_store.py:1240-1244`),
so the staged directory or file stays in `tmp/` as TMP_LEFTOVER, which repair removes. Every such
pattern is produced by a real crash (`os._exit`) in `tests/test_database_integrity.py:678-722`.

### 5.1 init
Order: check the directory without the lock, take the lock, check again, create `objects/` and
`tmp/`, build the catalog in `tmp/` and link it in, stage `STORE.json` in `tmp/` and link it
last (`versioned_store.py:639-668`). A missing or empty directory is initialised; a non-empty one
without `STORE.json` is refused and nothing is created (P4-S2-M1). A crash at any stage leaves no
`STORE.json` and only `.lock`, `objects`, `tmp`, `catalog.sqlite` in the root; `init` again
resumes it (P4-S2-M1b, `tests/test_database_versioned_store.py:195-232`).

### 5.2 publish (objects, used by ingest)
Order: Phase 2 writes data and manifest into `tmp/<H>/`, `version.json` is added, files `0444` and
fsync, rename to `objects/…/<dv>`, chmod `0555` and fsync, full re-verification, then the catalog
row (`versioned_store.py:25-33`; order tested at `tests/test_database_versioned_store.py:580`).

| Crash after | State | verify | Recovery | Test |
|---|---|---|---|---|
| staging | `tmp/<H>/` left | TMP_LEFTOVER (1) | repair | `tests/test_database_integrity.py:678-722` |
| rename, before chmod | durable object, directory mode ≠ `0555`, no catalog row (+ `tmp/<H>/` after a process crash) | OBJECT_DIR_MODE, OBJECT_UNREGISTERED (+ TMP_LEFTOVER), exit 1; the same state is pinned for the import path at `tests/test_database_legacy.py:791` | the next publish of the same version verifies fully, then restores `0555` (P4-S2-02); repair for `tmp/` | `tests/test_database_versioned_store.py:653` |
| chmod, before catalog | durable object, no catalog row (+ `tmp/<H>/` after a process crash) | OBJECT_UNREGISTERED (+ TMP_LEFTOVER), exit 1 | the next publish ("reused") or repair | `tests/test_database_versioned_store.py:629` |

### 5.3 set_current
Order: verify the target fully, register it, append to `ref_log.jsonl`, replace the ref file,
sync the catalog (`versioned_store.py:81-97`; order tested at `tests/test_database_refs.py:391`).
Setting the version that is already current is a no-op with no log entry (P4-S3-D5).

| Crash after | State | verify | Recovery | Test |
|---|---|---|---|---|
| before the log | unchanged | as before | none | `tests/test_database_refs.py:407` |
| log, before ref file | one pending entry; readers still see the old ref, with an advisory `pending` flag (P4-S3-01) | PENDING_REF_UPDATE (2) | `complete-pending --kind ref --apply`; every ref operation refuses until then | `tests/test_database_refs.py:418`, `:453`, P4-S3-D1 |
| ref file, before catalog | files current, catalog behind | CATALOG_BEHIND (1) | next writer (even a no-op `set_current`) syncs, or repair | `tests/test_database_refs.py:513` |

A pending entry is completed only if it is the dataset's last entry, its `previous_data_version`
equals the ref file's version (or both are absent) and the target passes full verification again
(`versioned_store.py:1186-1209`, P4-S3-D1).

### 5.4 snapshots
Order: verify every member fully, register, stage the file, `link` (never overwrite), fsync, then
the catalog rows (`versioned_store.py:1286-1357`). Identical members give the same id and outcome
`reused`. A crash after the link and before the catalog is completed by creating the same snapshot
again (`tests/test_database_refs.py:612`); until then verify reports the missing snapshot rows as
CATALOG_BEHIND, which repair also fixes (P4-S5-c). A staging leftover is TMP_LEFTOVER
(`tests/test_database_integrity.py:678-722`). Refused while a ref update or ingest is pending
(P4-S3-D1, P4-S4-b).

### 5.5 ingest
Order (`versioned_store.py:111-134`; tested at `tests/test_database_ingest.py:469`): Phase 2 parses
and validates (writes nothing, P4-S4-FC); regression guard (P4-S4-d); the object is published or
fully verified and reused; the ingest record is linked (commit point of the lineage); the
ref_log line and ref file via 5.3 (`action=ingest`); the catalog last.

| Crash after | State | verify | Recovery | Test |
|---|---|---|---|---|
| object, before record | object published and registered, no record, ref unchanged | nothing above INFO (OBJECT_UNREFERENCED if no ref points at it) | none needed; the next ingest of the same files reuses it with a new record | `tests/test_database_ingest.py:490`, `versioned_store.py:1457-1481` |
| record, before ref_log line | pending ingest | PENDING_INGEST (2) | `complete-pending --kind ingest --apply`; every ref/ingest operation refuses until then | `tests/test_database_ingest.py:510`, P4-S4-b |
| ref_log line, before ref file | pending ref update | PENDING_REF_UPDATE (2) | `complete-pending --kind ref --apply` (also syncs the ingest catalog, P4-S4-FIX1) | `tests/test_database_ingest.py:536`, `tests/test_database_integrity.py:1037` |
| ref file, before catalog | catalog behind | CATALOG_BEHIND (1) | next writer or repair | `tests/test_database_ingest.py:607` |

A pending ingest is completed only if its record is the last one, the planned ref_log line is
reproduced exactly, the previous version matches the ref file and the object passes full
verification again (`versioned_store.py:1427-1455`, P4-S4-b).

### 5.6 legacy import
Per dataset, under the writer lock, after A-C checks on the source (`legacy.py:10-28`): stage the
captured source bytes, publish the object byte for byte, register it, link the import record,
then the ref via 5.3 with `action=import_legacy` only if the dataset has no ref (P4-S6-d,
P4-S6-e). Datasets are processed in `dataset_id` order and fail independently (P4-S6-h).
Expected findings after a crash at each write are pinned at `tests/test_database_legacy.py:789-799`
and the recovery is tested to end byte-identical to an uninterrupted run
(`tests/test_database_legacy.py:801-824`):

| Crash after | verify (non-INFO) | Recovery |
|---|---|---|
| staging | TMP_LEFTOVER | repair, re-import |
| rename (new dataset) | OBJECT_DIR_MODE, OBJECT_UNREGISTERED, CATALOG_BEHIND, TMP_LEFTOVER (P4-S6-DEV3) | re-import, repair |
| before catalog registration | OBJECT_UNREGISTERED, CATALOG_BEHIND | re-import or repair |
| record staged / linked | TMP_LEFTOVER | repair, re-import (record `existing`) |
| ref_log line | PENDING_REF_UPDATE | `complete-pending --kind ref --apply`, then re-import |
| ref file | CATALOG_BEHIND | re-import or repair |

### 5.7 quality-report registration
All reports are planned before anything is written; a target-store problem stops the whole run
(P4-S6b-g, P4-S6b-n). Per report: register target objects if missing, stage the report in `tmp/`
(Phase 3's `load_report` runs on that exact file), `link` into `quality/`, unlink the staged name,
insert the catalog rows (`quality_registry.py:30-37`). The ref layer is never touched, so no
PENDING state exists. Pinned at `tests/test_database_quality_registry.py:801-806`:

| Crash after | verify (non-INFO) | Recovery |
|---|---|---|
| staged | TMP_LEFTOVER | repair, re-register |
| linked | TMP_LEFTOVER, CATALOG_BEHIND | re-register (`existing`) or repair |
| staged name removed | CATALOG_BEHIND | re-register or repair |

### 5.8 repair and rebuild
Repair (`integrity.py:24-36`, P4-S5-f): under the writer lock without opening the catalog, a fresh
verify, refusal on any ERROR (P4-S5-e), hot-journal rollback, catalog rebuilt in `tmp/` and
replaced atomically (P4-S5-k), known `tmp/` leftovers removed one by one (P4-S5-j), verify again.
Crash property (P4-S5-DEV5, `tests/test_database_integrity.py:1058-1089`): immutable files stay
byte-identical, the exit status does not get worse, the only new finding can be repair's own
catalog staging leftover, and a second repair gives a clean store.

Rebuild (`integrity.py:49-55`, `integrity.py:920-941`, P4-S8-d) runs the same rebuild
unconditionally and never removes `tmp/` leftovers. It uses the same mechanism
(`integrity.py:943`), but has no crash test of its own (section 10).

### 5.9 `tmp/` leftovers
Repair removes only these exact names (H = 32 hex) and reports everything else as TMP_UNKNOWN,
which is never removed (`integrity.py:161-169`, `integrity.py:833-858`, P4-S5-j): `H/`,
`H.ingest.json`, `H.snapshot.json`, `H.ref_log.jsonl`, `H.<tf>.json`, `H.import.json`,
`H.quality.json`, `.store-H.json`, `.catalog-H.sqlite.tmp` (+ `-journal`, `-wal`, `-shm`). Each
pattern is produced by a real crash in `tests/test_database_integrity.py:678-722`.

---

## 6. Reading

Readers take no lock (`versioned_store.py:599-627`). Every read of an object verifies it fully
first (`versioned_store.py:699-711`, `versioned_store.py:720`); there is no cache (P4-S7-d).

| Read | Behaviour | Source |
|---|---|---|
| `read_version(key, dv, as_of=)` | exact version; `as_of` filters bars by `available_at <= as_of` | `versioned_store.py:703`, I6 |
| `read_range(key, dv, start, end, as_of=)` | `[start, end)` on `open_time`, exactly `LocalOHLCVProvider.get_bars`; inputs validated before any file is opened | `versioned_store.py:136-147`, `versioned_store.py:712`, P4-S7-a, P4-S7-e |
| `pinned(snapshot_id)` → `PinnedStore.read` / `read_range` | only the snapshot's versions; never refs | `versioned_store.py:860`, `versioned_store.py:1545`, P4-S3-02 |
| `read_current(key, as_of=)` | latest known data cut at `as_of`; not point-in-time, never for backtests; no range variant | `versioned_store.py:837`, P4-S3-D4, P4-S7-b |
| `current_ref`, `read_snapshot`, `read_ingest`, `ingest_history` | strict file reads | `versioned_store.py:826`, `:847`, `:943`, `:952` |

---

## 7. Verify, repair, rebuild

**Verify** (`integrity.verify_store`, `integrity.py:861`) checks every file fully and the catalog
as a second witness, and reports the checks it performed (scope, `integrity.py:145-156`,
P4-S5-SCOPE), counts and findings. Severity and exit status, worst finding wins
(`integrity.py:82`, P4-S5-b):

| Severity | Exit | Repaired? |
|---|---|---|
| ERROR | 3 | never; repair and rebuild refuse |
| PENDING | 2 | only by explicit completion |
| REPAIRABLE | 1 | by repair |
| WARN | 1 | not repaired |
| INFO | 0 | nothing to do |

Every finding code and its severity is in one table, `integrity.py:86-122`. Codes that repair
fixes by a catalog rebuild: `integrity.py:123-124`. Codes by area:

| Area | Codes |
|---|---|
| root | ROOT_LAYOUT, STORE_FILE_INVALID, ROOT_FOREIGN_ENTRY (WARN), LOCK_MISSING (WARN), SYMLINK |
| objects | OBJECT_LAYOUT, OBJECT_CORRUPT, OBJECT_DIR_MODE (WARN), OBJECT_UNREGISTERED (REPAIRABLE), OBJECT_UNREFERENCED (INFO) |
| refs | REF_LOG_CORRUPT, REF_FILE_CORRUPT, REF_FOREIGN_ENTRY, REF_TARGET_MISSING, PENDING_REF_UPDATE (PENDING) |
| snapshots | SNAPSHOT_CORRUPT, SNAPSHOT_MEMBER_MISSING |
| ingests | INGEST_CORRUPT, INGEST_LOG_MISMATCH, INGEST_TARGET_MISSING, PENDING_INGEST (PENDING) |
| imports | IMPORT_CORRUPT, IMPORT_TARGET_MISSING, IMPORT_LOG_MISMATCH (P4-S6-j) |
| quality | QUALITY_CORRUPT, QUALITY_TARGET_MISSING, QUALITY_BINDING_MISMATCH (P4-S6b-l) |
| catalog | CATALOG_MISSING, CATALOG_UNREADABLE, CATALOG_STRUCTURE, CATALOG_BEHIND (REPAIRABLE), CATALOG_NEWER_SCHEMA, CATALOG_CONFLICT |
| tmp | TMP_LEFTOVER (REPAIRABLE), TMP_UNKNOWN (WARN) |

Unmarked codes are ERROR. Import records and registered reports count as references to their
objects (P4-S6-DEV4, P4-S6b-k).

**Repair** and **rebuild**: section 5.8. Both are dry-run by default and refuse on ERROR.

---

## 8. CLI reference

Entry points: `python -m btc_trading_system.database.cli` and `python scripts/datastore.py`
(same `main`, P4-S8-a, P4-S8-b); no console script (P4-S8-j). Full reference in the module
docstring, `cli.py:1-49`.

**Commands** (`cli.py:146-190`). `--store` is required on every command, with no default
(P4-S8-a). `--dataset` is the strict canonical relpath `binance/<instrument>/<SYMBOL>/<tf>`
(P4-S8-i); `--data-version` and `--id` are 64 lowercase hex.

| Command | Arguments | Mutating |
|---|---|---|
| `init` | `--store P [--apply]` | yes |
| `import-legacy` | `--store P --source P [--apply]` | yes |
| `ingest` | `--store P --dataset D FILE... [--fail-on-gaps] [--allow-regression] [--apply]` | yes |
| `list` | `--store P` | no |
| `versions` | `--store P --dataset D` | no |
| `set-current` | `--store P --dataset D --data-version DV [--apply]` | yes |
| `snapshot create` | `--store P (--current D ... \| --member D@DV ...) [--apply]` | yes |
| `snapshot show` | `--store P --id ID` | no |
| `register-report` | `--store P --source P [--apply]` | yes |
| `verify` | `--store P` | no |
| `repair` | `--store P [--apply]` | yes |
| `rebuild` | `--store P [--apply]` | yes |
| `complete-pending` | `--store P --kind ingest\|ref [--apply]` | yes |

**`--apply`.** Without it nothing is written; mutating commands show the library's own dry-run
where one exists (import-legacy, register-report, repair, rebuild) or a read-only preview
(P4-S8-c). Options cannot be abbreviated (P4-S8-IMPL1).

**Ingest context.** The CLI resolves `now` (UTC) and the git state once per ingest and shows them
in `context` (P4-S4-c, P4-S8-f). `ingest --apply` is refused (exit 5) unless the worktree is
clean; the dry-run still exits 0 and shows the values (P4-S8-g, P4-S8-IMPL2, P4-S8-IMPL3).
`--allow-regression` is the only way past the regression guard (P4-S4-d).

**Exit codes** (`cli.py:35-45`, `cli.py:76-83`, P4-S8-h):

| Code | Meaning |
|---|---|
| 0 | success (verify/repair/rebuild: the store is clean) |
| 1 / 2 / 3 | verify severity; repair and rebuild exit with the severity of the store they leave (dry-run: before, apply: after) |
| 4 | import-legacy / register-report ran but the report has `ok: false` |
| 5 | refused, nothing changed: lock held, pending ref or ingest, regression guard, repair/rebuild over ERROR, unclean worktree |
| 6 | other store or data error |
| 64 | usage error |
| 70 | internal error (traceback on stderr) |

**Output** (P4-S8-e, P4-S8-m, P4-S8-IMPL10, P4-S8-IMPL11). Default: one canonical JSON object on
stdout with exactly the keys `format` (`btc-quant/datastore-cli/v1`), `command`, `applied`,
`exit_code`, `context`, `result`, `result_sha256`, `error` (`cli.py:224-230`). `result` is the
library's own JSON where one exists; `result_sha256` is the sha256 of the library's own bytes
where they exist (for verify: the report sha256). On error `result` is null, `error` is
`{type, message}` and stderr gets one line. Absolute paths of `--store`, `--source` and the input
directories are replaced by `<store>`, `<source>`, `<input>` (P4-S8-IMPL9). `--format text`
renders the same object as `key.path: value` lines under a header `datastore <cmd>: exit N`.
Snapshot members are the library's member objects (`dataset` is the structured key, as in the
snapshot file; `cli.py:284-289`).

---

## 9. Operations runbook

Every command in this section was run against a hermetic synthetic store (never `~/btc_data`);
the exit codes shown are the ones observed. Setup:

```bash
source ~/btc_quant/.venv/bin/activate          # docs/ENVIRONMENT.md
DS="python -m btc_trading_system.database.cli"
STORE=/path/to/new/store                        # never inside a source directory (P4-S6-a)
P2=/path/to/phase2/store                        # read only
REPORTS=/path/to/phase3/report/dir              # read only, one flat --out directory (P4-S6b-c)
RAW=/path/to/raw/klines
```

Rules for every operation:
- Run the dry-run first, read it, then repeat the same command with `--apply`.
- After every `--apply`, run `verify`; exit 0 is the only state to continue from.
- Never edit, chmod, move or delete anything inside a store by hand. The only manual action this
  runbook allows is running these commands.
- On exit 3 (ERROR) stop and keep the store as it is: it is evidence (P4-S5-c, P4-S5-e).

### 9.1 Create a store and import the Phase 2 data

```bash
$DS init --store "$STORE"                                   # exit 0: already_initialised false, predicted false
$DS init --store "$STORE" --apply                           # exit 0
$DS import-legacy --store "$STORE" --source "$P2"           # exit 0: every dataset "planned", ok true
$DS import-legacy --store "$STORE" --source "$P2" --apply   # exit 0: imported / published / recorded / set
$DS verify --store "$STORE"                                 # exit 0
```
Check in the import report: `ok` is true, `source_tree_sha256_before == source_tree_sha256_after`,
and every dataset's `ref_outcome` (P4-S6-e). `import_id` and `record_sha256` per dataset are the
values V5 records (P4-S6-b, P4-S6-DEV5). Exit 4 means `ok` is false (`SOURCE_EMPTY`,
`SOURCE_CHANGED`, a failed dataset or an unrecognized entry): read `failure` and the per-dataset
`failure`/`detail`; nothing was written for a failed dataset (P4-S6-h).

### 9.2 Register Phase 3 reports

```bash
$DS register-report --store "$STORE" --source "$REPORTS"           # exit 0: every report "planned"
$DS register-report --store "$STORE" --source "$REPORTS" --apply   # exit 0: registered / stored / inserted
$DS verify --store "$STORE"                                        # exit 0
```
The referenced dataset versions must already be in the store (P4-S6b-g); the locked Phase 3
config is required (P4-S6b-f); each report is reproduced before it is accepted (P4-S6b-i). A
second registration of the same directory gives `existing` / `existing` (P4-S6b-c).

### 9.3 Look at the store

```bash
$DS list --store "$STORE"                                              # exit 0
$DS versions --store "$STORE" --dataset binance/spot/BTCUSDT/15m       # exit 0
$DS --format text verify --store "$STORE"                              # exit 0, first line "datastore verify: exit 0"
```
`list` and `versions` take datasets and versions from the catalog and the current refs from the
files (P4-S8-l); if in doubt, `verify` compares the two.

### 9.4 Ingest new data

```bash
$DS ingest --store "$STORE" --dataset binance/spot/BTCUSDT/15m "$RAW"/new/spot/*.csv           # exit 0
$DS ingest --store "$STORE" --dataset binance/spot/BTCUSDT/15m "$RAW"/new/spot/*.csv --apply   # exit 0
$DS verify --store "$STORE"                                                                     # exit 0
```
Before `--apply`, check `context`: `worktree_clean` must be true and `code_version` the intended
commit (otherwise `--apply` exits 5, `WorktreeNotCleanError`). Check `files` (name, size, sha256)
and `current`. The record shows `publication` (`published` or `reused`) and `ref_outcome`
(`changed` or `unchanged`). Phase 2 input rules: `docs/DATA_INGESTION.md`.

An ingest whose data ends earlier than the current version is refused:
```bash
$DS ingest --store "$STORE" --dataset binance/spot/BTCUSDT/15m "$RAW"/old/spot/*.csv --apply   # exit 5, VersionRegressionError
```
Add `--allow-regression` only if moving the dataset back is the intent; the flag is recorded in
the ingest record (P4-S4-d).

### 9.5 Choose the current version, pin a snapshot

```bash
$DS versions --store "$STORE" --dataset binance/spot/BTCUSDT/15m                         # pick DV
$DS set-current --store "$STORE" --dataset binance/spot/BTCUSDT/15m --data-version "$DV"           # exit 0
$DS set-current --store "$STORE" --dataset binance/spot/BTCUSDT/15m --data-version "$DV" --apply   # exit 0
$DS snapshot create --store "$STORE" --current binance/spot/BTCUSDT/15m --current binance/perp/BTCUSDT/15m           # exit 0, outcome "created"
$DS snapshot create --store "$STORE" --current binance/spot/BTCUSDT/15m --current binance/perp/BTCUSDT/15m --apply   # exit 0
$DS snapshot show --store "$STORE" --id "$SNAP"                                          # exit 0
```
`$SNAP` is `result.snapshot_id` of the applied `snapshot create`. Research and backtests read only
through a snapshot (P4-S3-D4). `--member D@DV` pins explicit versions instead of current ones.

### 9.6 Verify

```bash
$DS verify --store "$STORE"     # exit 0 / 1 / 2 / 3
```
| Exit | Action |
|---|---|
| 0 | none |
| 1 | read the findings: REPAIRABLE → 9.7; WARN only (e.g. TMP_UNKNOWN, OBJECT_DIR_MODE) → investigate, nothing is repaired automatically (P4-S5-i) |
| 2 | a crash left a pending operation → 9.8 |
| 3 | stop; keep the store unchanged; repair and rebuild refuse (9.10) |
| 5 | a writer holds the lock; run again when it has finished (P4-S5-d) |

### 9.7 Repair (REPAIRABLE findings)

Example: the catalog file is missing (`CATALOG_MISSING`).
```bash
$DS verify --store "$STORE"            # exit 1: CATALOG_MISSING
$DS repair --store "$STORE"            # exit 1: plan {tmp_removals: [], rebuild_catalog: true, blocking: []}
$DS repair --store "$STORE" --apply    # exit 0: after.exit_status 0
```
Repair never completes pending operations and never touches `objects/`, `refs/`, `ref_log.jsonl`,
`snapshots/`, `ingests/`, `imports/`, `quality/` or `STORE.json` (`integrity.py:24-36`).

### 9.8 Complete a pending operation (exit 2)

Example: a crash after an ingest record was written (PENDING_INGEST).
```bash
$DS verify --store "$STORE"                                      # exit 2: PENDING_INGEST
$DS complete-pending --store "$STORE" --kind ingest              # exit 0: pending [PENDING_INGEST]
$DS complete-pending --store "$STORE" --kind ingest --apply      # exit 0: completed record
$DS verify --store "$STORE"                                      # exit 0
```
Use `--kind ref` for PENDING_REF_UPDATE. Completion re-verifies the target and refuses if the
state does not match exactly (5.3, 5.5).

### 9.9 Rebuild the catalog

```bash
$DS rebuild --store "$STORE"            # exit 0: identical null (dry-run)
$DS rebuild --store "$STORE" --apply    # exit 0: identical true, dump sha256 before == after
```
On a clean store, `identical: true` is the witness that the catalog is a deterministic function of
the files (P4-S8-d).

### 9.10 ERROR (exit 3)

```bash
$DS verify --store "$STORE"            # exit 3: e.g. OBJECT_CORRUPT
$DS repair --store "$STORE" --apply    # exit 5: RepairRefusedError, nothing changed
$DS rebuild --store "$STORE" --apply   # exit 5: RepairRefusedError, nothing changed
```
Record the verify JSON (its `result_sha256` identifies it) and stop; the owner decides.

### 9.11 Usage errors

```bash
$DS verify                              # exit 64: UsageError (no --store)
```

---

## 10. Known limits and open items

- `rebuild_catalog_store` has no crash test of its own; its rebuild step is the one covered by
  repair's crash tests (5.8).
- `cli.py:286`: the snapshot member key `dataset` set to the relpath is overwritten by the
  structured `dataset` from `DatasetVersionRef.to_json_obj()`; the output therefore equals the
  library's member objects (section 8).
- Linux `rename()` silently replaces an empty target directory; guarded by a check under the lock
  (P4-S2-03).
- The `pending` flag a reader sees is advisory (P4-S3-01).
- Range reads are not wired into `LocalOHLCVProvider` (P4-S7-a).
- Step 12 (real-data migration) and Step 13 (V5) are still to come.

---

## 11. Spec coverage

| `docs/PHASE4_SPEC.md` item | Section |
|---|---|
| catalog, migrations, deterministic schema/IDs, rebuild/dump/query | 1, 3, 7, 9.9 |
| immutable content-addressed versions, refs, version reads, range reads, snapshots, pinned reads, quality-report registration | 2, 5, 6 |
| `DatasetVersionRef`, `DataSnapshot` | 1, 2 |
| deep verification, repair | 7, 5.8 |
| legacy import | 5.6, 9.1 |
| CLI, `scripts/datastore.py` | 8, 9 |
| hash rules, identities | 1 |
| immutable object layout | 2 |
| Phase 2 compatibility | 1 (`data_version`), 5.6 |
| atomic publishing, writer lock | 4, 5 |
| refs, snapshots, pinned reads, causality | 4, 6 |
| ingest lineage | 2, 5.5 |
| integrity/verify, repair limits | 7, 5.8 |
| mutation tests | not described here; harnesses outside the repo until Step 11 (P4-S8-n and the earlier mutation-set decisions) |
| real-data migration | Step 12, not yet run (10) |
