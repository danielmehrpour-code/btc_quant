# Phase 4 specification (approved, verbatim)

**Provenance.** This text was given by the owner as a chat message in the Claude Code
session of 2026-09-30, in reply to the Step 1 request, and declared "the source of truth
for Phase 4". It did not exist in any file before this commit (not in the repo, not in
`~/phase3_verify`, not in `~/btc_data`, not in Claude's project memory).

**Fidelity.** The block below is copied verbatim from that message as it was received in
the session, including its first two lines (context) and its last section (instructions
specific to Step 1). Nothing is reconstructed, added or changed. It could not be compared
byte-for-byte against an independent original because none exists on disk.

**Later decisions** that refine this spec (M1–M4, D1–D5, the Step 4 ingest rule, etc.)
are recorded in `docs/DECISIONS.md`, not here.

```text
You are correct to stop. Do not infer the missing design decisions.

Here is the authoritative Phase 4 specification to use. Treat this as the source of truth for Phase 4.

PROJECT GOAL

Build Phase 4: Database / Storage.

Core principles:
- Parquet remains the immutable store of record.
- SQLite is a derived, rebuildable catalog/index layer.
- No new Python dependencies.
- Preserve Phase 1–3 behavior and compatibility.
- Do not refactor existing code merely for cleanliness.
- Causality is a hard invariant.
- No live trading changes.
- No features, backtesting, execution, ML, funding/OI, order book, or strategy work in Phase 4.

PHASE 4 COMPONENTS

The Phase 4 database package should contain these components:

1. catalog.py
   - SQLite catalog
   - migrations
   - deterministic schema
   - deterministic dataset IDs
   - catalog rebuild/dump/query support

2. versioned_store.py
   - immutable content-addressed dataset versions
   - current-version refs
   - version reads
   - storage-layer range reads
   - snapshots
   - pinned historical reads
   - quality-report registration

3. refs.py
   - DatasetVersionRef
   - DataSnapshot

4. integrity.py
   - deep store verification
   - repair functionality

5. legacy.py
   - import existing Phase 2/3 store into the new versioned layout

6. cli.py
   - Phase 4 datastore commands

7. scripts/datastore.py
   - import-legacy
   - ingest
   - list
   - versions
   - set-current
   - snapshot create/show
   - register-report
   - verify
   - repair
   - rebuild

CATALOG SCHEMA

Required logical tables:

- datasets
- dataset_versions
- version_sources
- refs
- ref_log
- ingest_events
- ingest_sources
- snapshots
- snapshot_members
- quality_reports
- quality_report_datasets

The catalog must use:
- SQLite
- STRICT tables where supported
- foreign keys enabled on every connection
- CHECK constraints
- UNIQUE constraints
- deterministic migration/schema versioning
- reject catalogs with a newer unsupported schema version
- deterministic database dump/rebuild
- rollback journal, not WAL

HASH RULES

All SHA-256 hash fields:
- exactly 64 hexadecimal characters
- lowercase
- validated with CHECK constraints where appropriate

Keep these identities separate:

1. dataset identity
   = exchange + instrument + symbol + timeframe

2. dataset version identity
   = dataset_id + data_version

3. data_version
   = existing Phase 2 logical content identity
   = DO NOT CHANGE ITS DEFINITION

4. data_file_sha256
   = SHA-256 of the physical Parquet representation

5. manifest_sha256
   = SHA-256 of the manifest bytes

Do NOT conflate data_version with physical file hash.

DATASET ID

Dataset identity must be deterministic and canonical.

Canonical dataset key:
exchange + instrument + symbol + timeframe

Use a canonical structured representation rather than an ambiguous concatenation. Normalize the fields according to the existing Phase 2 conventions; do not silently invent a different exchange/instrument/symbol/timeframe representation.

The dataset ID must be deterministic and stable across catalog rebuilds.

IMMUTABLE OBJECT LAYOUT

The new store layout is:

<root>/STORE.json
<root>/.lock

<root>/objects/binance/<instrument>/<SYMBOL>/<timeframe>/<data_version>/
    bars.parquet
    manifest.json
    version.json

<root>/refs/binance/<instrument>/<SYMBOL>/<timeframe>.json

<root>/ref_log.jsonl

<root>/ingests/<ingest_id>.json

<root>/snapshots/<snapshot_id>.json

<root>/quality/<report_sha256>.quality.json

<root>/tmp/

<root>/catalog.sqlite

Parquet objects are immutable.

Once a dataset version exists:
- it must never be overwritten
- its manifest must never be rewritten
- its version metadata must never be rewritten

If the same data_version already exists:
- verify the existing object completely
- reuse it only if verification succeeds
- never blindly skip verification

PHASE 2 COMPATIBILITY

Existing Phase 2 data remains valid.

Existing logical data_version values MUST remain unchanged.

The legacy source is:

~/btc_data/store_quality_v1

The source must remain read-only during migration.

Migration must preserve:
- data_version
- manifest information
- rows
- coverage
- gaps
- data versions
- provider behavior
- as_of causality
- real-data checksums

ATOMIC PUBLISHING

Publishing a new dataset version must be crash-safe.

Use:
- exclusive writer lock
- unique staging directories under tmp/
- fsync of files before publication
- atomic object publication
- atomic ref replacement
- fsync directories where required
- transactional catalog update

The writer lock must prevent concurrent writers.

Readers do not need the writer lock.

A second writer must fail immediately with a clear StoreLockedError.

The system must never publish a ref pointing to an object that was not durably published first.

REFS

A ref represents the current version of one dataset.

The ref must contain enough information to identify the exact dataset and current data_version.

Ref replacement must be atomic.

Ref history must be recorded safely in ref_log.

Do not allow a ref to point to a missing or unverified object.

SNAPSHOTS

Snapshots pin exact dataset versions.

Snapshot identity must be deterministic from a canonical representation of its members.

Snapshot members must be sorted canonically before hashing/storage.

A snapshot must never silently follow a moving current ref.

PINNED READS

Pinned historical reads must always use the explicitly pinned dataset version.

A PinnedStore must never follow the current ref.

Writes through a PinnedStore must fail with ReadOnlyStoreError.

RANGE READS

Add storage-layer time-range reads.

Boundary semantics must be explicit and tested for off-by-one errors.

Do NOT wire range reads into LocalOHLCVProvider in Phase 4.

CAUSALITY

Causality remains a hard invariant.

For all versioned reads:
- as_of=t may only expose bars with available_at <= t
- in-progress/future bars must never be exposed
- exact available_at boundary must be tested
- just-before boundary must exclude the bar
- gap_before semantics must remain unchanged
- pinned historical reads must preserve causality

QUALITY REPORT REGISTRATION

Do not change Phase 3 quality-report generation.

Phase 4 may register existing reports in the catalog.

Registration is allowed only when referenced dataset versions exist.

Preserve:
- report SHA
- report type
- data_version(s)
- config hash
- code version
- overall status

Do not invent or regenerate reports.

INGEST LINEAGE

Every ingest must have lineage, including repeated ingestion of identical content.

The lineage must distinguish:
- newly published version vs reused existing version
- ref changed vs ref unchanged

Record:
- ingest sequence
- ingest record/hash
- dataset identity
- data_version
- sources
- ingest parameters
- code state
- publication/reuse outcome
- ref outcome

Do not collapse repeated ingestion events merely because the resulting data_version is identical.

INTEGRITY / VERIFY

A deep verification must detect at minimum:
- catalog/file mismatches
- missing referenced objects
- corrupted object hashes
- manifest mismatches
- ref inconsistencies
- ref_log inconsistencies
- snapshot inconsistencies
- quality-report inconsistencies
- orphaned objects
- orphaned catalog rows
- abandoned tmp files
- incorrect file permissions where required

Verification must detect if SHA verification is bypassed.

REPAIR

Repair is intentionally limited.

It may:
- clear abandoned tmp state
- rebuild the derived catalog from immutable filesystem objects

It must NOT:
- delete immutable historical objects
- rewrite historical dataset metadata
- silently change refs
- alter data_version values
- rewrite historical lineage

MUTATION TESTS

Tests must explicitly prove that the system catches at least:

- skipped SHA verification
- attempted immutable-object overwrite
- ref publication before durable object publication
- PinnedStore incorrectly following current ref
- range-read boundary off-by-one
- foreign keys disabled
- unsorted snapshot members
- lock bypass
- skipping verification of an existing object
- future data exposed through as_of

REAL-DATA MIGRATION

After implementation, migrate the existing Phase 2/3 data from:

~/btc_data/store_quality_v1

into a fresh Phase 4 store root.

The source must remain untouched.

Verify:
- existing data_versions
- manifest hashes/checksums
- row counts
- coverage
- gaps
- provider outputs
- as_of behavior
- registered quality reports

IMPORTANT SCOPE RULE

Do not implement everything in one step.

Implementation order is:

1. catalog/schema/migrations
2. immutable object/version layer
3. refs/snapshots
4. lineage
5. integrity/repair
6. legacy import
7. range reads
8. CLI
9. docs
10. tests
11. mutation tests
12. real-data migration
13. final verification

For the current task, implement ONLY Step 1: catalog/schema/migrations.

Before modifying any protected Phase 1–3 file:
STOP and show the exact diff and why it is required.

Do not commit.
Do not tag.
Do not reset.
Do not move phase2-verified or phase3-verified.
Do not push.

After Step 1:
- run focused catalog tests
- run the complete existing test suite
- report all changed files
- report all test results
- stop and wait for approval.
```
