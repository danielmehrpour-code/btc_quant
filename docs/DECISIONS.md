# Decision log

One entry per decision that has already been made and locked. This log records decisions;
it does not create them. Sources are `file:line` at commit `6c5d049` (or later if noted),
a commit hash, "session 2026-09-30" when the decision was made in a working session and
had no written record in the repo before this file, or an owner message when the decision
was made in the original chat. For the "session 2026-09-30" entries, this log (approved in
the commit that added it) is the accepted record. Open questions are listed at the end
under **UNCERTAIN** and are not decisions.

Authoritative scope: `docs/MASTER_SPEC.md`. Phase 4 scope: `docs/PHASE4_SPEC.md`.

## Project level

| ID | Phase/step | Decision | Rationale | Source |
|---|---|---|---|---|
| G-01 | All | `docs/MASTER_SPEC.md` is the authoritative master specification (sections 0–72, plus 1.1). | The spec previously existed only in chat history. | commits `3098982`, `6c5d049` |
| G-02 | All | 1.1 PRIMARY OBJECTIVE: maximise realistic net profit after all costs, no return target, no overfitting/look-ahead/leakage; robust out-of-sample, walk-forward and predefined risk constraints. | Profit must come from genuine opportunities, not from backtest artefacts. | `docs/MASTER_SPEC.md:65` |
| G-03 | All | Research first: if no strategy passes the out-of-sample, walk-forward and risk criteria, deploy nothing (cash). The owner makes the deployment decision on criteria pre-registered before out-of-sample evaluation. | The system is a research tool first; live trading stays off by default. | `docs/MASTER_SPEC.md:71` (and the clarifications after it) |
| G-04 | All | Live trading is off by default; enabling it requires explicit configuration and environment confirmation. | Safety. | `docs/MASTER_SPEC.md:61`, `configs/default.yaml:19`, `btc_trading_system/config/settings.py:171` |
| G-05 | Phases 10, 18, 19 | `docs/SPEC_MONITORING_REPORTS.md` is a PROPOSAL, not approved scope; it is reviewed and locked part by part at Phases 10, 18 and 19, and its numeric thresholds are locked before the first paper-trading day. | Capture the monitoring design early without committing to it. | `docs/SPEC_MONITORING_REPORTS.md:3`, commit `bbb715c` |
| G-06 | All | Repo docs and `docs/VERIFICATION.md` are the single source of truth; no parallel project documents are maintained. | A parallel copy would drift. | session 2026-09-30 |
| G-07 | All | Docs are never executable: new docs are committed with mode `100644`. `docs/VERIFICATION.md` keeps its current mode (`100755`) for now. | Documentation is data, not code. | session 2026-09-30; `git ls-files -s docs/` at `6c5d049` |
| G-08 | All | "A new module may add only [import-untyped] mypy errors for third-party libraries without stubs (pandas, yaml). Each one is listed in the verification. Any other new mypy error is a FAIL. This applies until the V3 option B maintenance (stubs/mypy config) is done." | General form of the Phase 3 (V4) and Phase 4 Step 2 acceptances. | owner decision 2026-09-30 |
| G-09 | All | Verification history is not rewritten: a stale statement in `docs/VERIFICATION.md` is corrected by a dated addendum, not by editing the original text. | Keep the verification record auditable. | owner decision 2026-09-30 (V4 tag addendum) |
| G-10 | 4 (close) | The code fingerprint after Phase 4 is recorded in `docs/VERIFICATION.md` as V5 at Phase 4 close. | One verification record per phase close. | owner decision 2026-09-30 |
| G-11 | All (from 2026-09-30) | Decision recording: (1) every new decision (approval with conditions, chosen option, accepted deviation) is added to this log in the same commit as the code it concerns, with ID, phase/step, decision, rationale and source; (2) every step report ends with "New decisions this step:" (a list, or "none"); (3) every phase close adds a V-record to `docs/VERIFICATION.md` (versions, fingerprint, test counts, ruff/mypy, mutations) before the phase tag is created; (4) anything that exists only in chat or in the assistant's memory and affects scope or behaviour is flagged so the owner can decide whether it goes into the repo. | The repo is the single source of truth (G-06); decisions were previously lost in chat (e.g. P6-01, G-01). | owner decision 2026-09-30 (F-1, recorded in the Phase 4 Step 4 commit) |
| G-12 | All (from Phase 4) | Working process: one spec step at a time; stop before touching a protected Phase 1–3 file and show the exact diff; new code is staged in a scratchpad first and shown as full new files, diffs of committed files, sha256, syntax/import/ruff (incl. TRY004)/mypy (G-08), overlay test results and mutation results; only byte-identical files are copied into the repo after approval; no commit, tag, push or reset without explicit approval, and `phaseN-verified` tags are never moved; baselines are checked every step (pytest count, ruff 41, mypy per G-08, code fingerprint, `~/btc_data/store_quality_v1` tree hash via `~/phase4_work/treehash.sh`). The scratchpad lives in `~/phase4_work/scratch/` (persistent), never under `/tmp`; the same applies to overlays and mutation runs (added 2026-10-01). Mutation harnesses are kept in `~/phase4_work/mutation/` until Step 11. | Auditable, reviewable steps; a WSL restart wiped `/tmp` and lost in-progress Step 4 work. | owner decisions 2026-09-30 (F-2) and 2026-10-01 (persistent scratchpad) |
| G-13 | All | `docs/DECISIONS.md` is a living log: headings and structure may be edited, with the change visible in the diff. Recorded decision texts are never changed. A decision is changed only by a new entry that explicitly supersedes it, referencing the old ID. `VERIFICATION.md` is never rewritten (G-09). | Distinguishes the living decision log from the never-rewritten verification record. | owner decision 2026-10-01 |
| G-14 | All | `CLAUDE.md` in the repo root holds the project instructions for Claude Code: rules and pointers only (project, authoritative documents, working process G-12, rules G-11/G-13, protected items, verification commands, environment). It never copies spec or decision text; the documents in `docs/` stay authoritative and win over `CLAUDE.md`. If documents in `docs/` conflict with each other, stop and ask the owner; never choose yourself. It is a doc (mode `100644`, G-07) and outside the code fingerprint. | Gives every session the same rules without creating a parallel spec (G-06). | owner decision 2026-10-01 |

## Phase 1–2: causality, execution contract, data layer

| ID | Phase/step | Decision | Rationale | Source |
|---|---|---|---|---|
| P1-01 | 1 | `available_at = open_time + timeframe`; a decision at time t may only use bars with `available_at <= t`. | Hard anti-look-ahead invariant. | `docs/LOOKAHEAD_POLICY.md:2`, `btc_trading_system/core/types.py:6` |
| P1-02 | 1 | Signal → order → fill are causally ordered; the fill is strictly after the order (`min_latency_bars >= 1`, no same-bar fill), at the next bar's price plus slippage. | "Signal price = fill price" is forbidden. | `docs/LOOKAHEAD_POLICY.md:4`, `btc_trading_system/config/settings.py:104-105`, `btc_trading_system/core/types.py:120-133` |
| P1-03 | 1 | Data split uses exclusive UTC bounds (TRAIN / VALIDATION / FINAL TEST); parameters are chosen on TRAIN, compared on VALIDATION, and FINAL TEST is touched once. | Prevents selection on test data. | `docs/LOOKAHEAD_POLICY.md:8`, `configs/default.yaml:21-26` |
| P1-04 | 1 → 6/7 | "data_split dates are placeholders; they must be locked in their own commit before the first strategy evaluation on real data (Phase 6/7), before any strategy result is seen. The Dec 2024-Jan 2025 period was used only for data quality checks, not for strategy results." | The split must be fixed before any result can influence it. | owner decision 2026-09-30; `configs/default.yaml:22` |
| P2-01 | 2 | `data_version` = sha256 over a canonical byte encoding of the content (fixed column order, dtypes and row order), independent of the pyarrow version. | Logical identity must not depend on the file format. | `btc_trading_system/data/storage.py:6-7` |
| P2-02 | 2 | Code fingerprint = sha256 over `sha256sum` of every file in `btc_trading_system/ tests/ configs/ scripts/ pyproject.toml`; `docs/` is excluded. | Documentation can change without invalidating a verification. | `docs/VERIFICATION.md:23-30` |
| V3-A | after 2 | Decision A: no code change before Phase 3 for the V3 lint/type findings; fixes are deferred (option B) to a possible later maintenance commit. | 0 runtime bugs; fixes would change the fingerprint and need full re-verification. | `docs/VERIFICATION.md:104`, `docs/VERIFICATION.md:147-152` |

## Phase 3: data quality

| ID | Phase/step | Decision | Rationale | Source |
|---|---|---|---|---|
| P3-01 | 3 | Data-quality thresholds are locked in `configs/data_quality.yaml`: file sha256 `756509ad305bc1c036fc25fce797a9e722700bd3115c142f43a2fdcae1880d2d`, canonical sha256 `d1350cde29f02294bb327176fd9ba2c4df4ac5187f72ff54f6c674588319b9a0`. A change requires a visible decision and an updated hash in the tests. | Reproducible, auditable quality verdicts. | `configs/data_quality.yaml:1`, `docs/DATA_QUALITY.md:8-15`, `tests/quality/test_quality_config.py:10` |
| P3-02 | 3 | Thresholds are not adjusted after real data has been analysed. | Avoids fitting the checks to the data. | `docs/DATA_QUALITY.md:16` |
| P3-03 | 3 | Phase 3 never changes data; cleaning, imputation and exclusion belong to later phases and need their own decision. | Detect and report only. | `docs/DATA_QUALITY.md:3-6` |
| P3-04 | 3 (V4) | Real-data finding `TF_AGGREGATION_MISMATCH` (BTCUSDT perp 1m→15m, January 2025, 3 windows) is classified as a source-data inconsistency between the Binance archives; which archive is correct is not established. It remains `FAIL` and is not downgraded. | The locked specification requires FAIL; the implementation was independently confirmed correct. | `docs/VERIFICATION.md:197-232`, `docs/DATA_QUALITY.md:191-197` |

## Phase 6: execution model (decided before Phase 3)

| ID | Phase/step | Decision | Rationale | Source |
|---|---|---|---|---|
| P6-01 | 6 | Conservative execution model: decision at t, execution strictly after t with explicit configurable latency, fill price from market data available after that latency; revise backtester interfaces at Phase 6; keep the Phase 1-2 data layer; no live trading. Verbatim text below. | Resolves the open design questions in `docs/REVIEW_LOG.md:21-25` (which is not edited). | owner message 2026-09-28 17:13 UTC (original chat transcript) |

P6-01, verbatim:

> "For design question 1, use the conservative execution model: the signal/order
> decision happens at time t, execution occurs strictly after t using an explicit
> configurable latency, and the fill price is determined from market data available
> after that latency. Do not assume zero latency or same-bar fills."
>
> "At Phase 6, revise the backtester interfaces as needed to properly support:
> execution latency, realistic fill timing, partial fills, target positions, exits,
> price/equity/time available to risk checks, fees and slippage."
>
> "Keep the existing Phase 1-2 data-layer behavior intact unless a concrete bug is
> found." "Do not implement live trading yet. Keep LIVE_TRADING = False."

It was meant to be appended to `docs/REVIEW_LOG.md` as "Decisions after Review 1" but never
reached the repo; this entry is its record.

## Phase 4 Step 1: catalog (commit `cde77a2`, amended in migration 1 before commit)

| ID | Phase/step | Decision | Rationale | Source |
|---|---|---|---|---|
| P4-S1-01 | 4.1 | STRICT tables are required; SQLite older than 3.37 is refused rather than given a non-STRICT fallback. | The schema must be identical on every machine. | `btc_trading_system/database/catalog.py:15`, `:46` |
| P4-S1-02 | 4.1 | Exchange is limited to `binance`; stored dataset fields must already be canonical (no silent normalisation, e.g. `btc/usdt` is rejected). | Follows Phase 2 `EXCHANGE` and the spec layout. | `btc_trading_system/database/catalog.py:86` |
| P4-S1-03 | 4.1 | `dataset_id` = sha256(`btc-quant/dataset-id/v1\0` + canonical JSON of exchange, instrument, symbol, timeframe); golden value for binance/spot/BTCUSDT/15m: `f607d43c…e052`. | Structured, unambiguous, stable across rebuilds. | `btc_trading_system/database/catalog.py:47`, `tests/test_database_catalog.py:498` |
| P4-S1-04 | 4.1 | All catalog tables are append-only (triggers); `refs` may be updated but never deleted. | Defence in depth for immutable history. | `btc_trading_system/database/catalog.py:22` |
| P4-S1-05 | 4.1 | `ref_log` is a hash chain (`entry_sha256` / `prev_entry_sha256`, unique, no branches). | Tamper-evident ref history. | `btc_trading_system/database/catalog.py:177` |
| P4-S1-06 | 4.1 | No wall-clock time is stored in the catalog; ordering comes from sequence numbers only. | Deterministic dump and rebuild. | `btc_trading_system/database/catalog.py:26` |
| P4-S1-07 | 4.1 | Rollback journal (`journal_mode=DELETE`), `synchronous=FULL`, never WAL. | Spec requirement; simple crash semantics. | `btc_trading_system/database/catalog.py:18` |
| P4-S1-08 | 4.1 | Migration 1 is frozen once committed (pinned SQL sha256 `a277d5b0…e1a6`); any later schema change is a new migration. | Deterministic schema versioning. | `tests/test_database_catalog.py:108`, commit `cde77a2` |
| P4-S1-09 | 4.1 | `ingest_events.duplicates_dropped INTEGER NOT NULL CHECK (>= 0)` was added to migration 1 before it was committed. | Source-dependent lineage belongs to the ingest record (see P4-S4-01). | `btc_trading_system/database/catalog.py:214` |
| P4-S1-10 | 4.1 | `open_catalog` takes `staging_dir`; the store builds its catalog under `tmp/` and links it in without overwriting. | The store root holds only final artefacts. | `btc_trading_system/database/catalog.py:469`, `btc_trading_system/database/versioned_store.py:77` |

## Phase 4 Step 2: immutable dataset versions (commit `ab7ed7f`)

| ID | Phase/step | Decision | Rationale | Source |
|---|---|---|---|---|
| P4-S2-M1 | 4.2 | `init()` only initialises a missing or empty directory; a non-empty directory without `STORE.json` (e.g. a Phase 2 store) is refused with `StoreFormatError` and nothing is created in it. | Never write into a foreign or Phase 2 directory. | `btc_trading_system/database/versioned_store.py:51`, `:1179` |
| P4-S2-M1b | 4.2 | `init()` resumes an interrupted init only if all hold: entries ⊆ {.lock, objects, tmp, catalog.sqlite}, no symlinks; `objects/` empty or missing; `tmp/` holds only `.store-*.json` and `.catalog-*.sqlite.tmp[-journal/-wal/-shm]`; the catalog opens and has 0 datasets and 0 versions. The check runs before and again under the writer lock; leftovers are not deleted. | A crash during init must not make the store unrecoverable. | `btc_trading_system/database/versioned_store.py:51-60`, `:1179` |
| P4-S2-M2 | 4.2 | Reuse is strict: same `data_version` with a different `extra` (any non-base manifest field, including `sources`) raises `ProvenanceConflictError`; the stored manifest is never rewritten. (Refined for the ingest path by P4-S4-01.) | Provenance is never silently dropped. | `btc_trading_system/database/versioned_store.py:35-39`, `:205`, `:811` |
| P4-S2-M3 | 4.2 | One exception type for invalid `sources`: `InvalidSourcesError(StoreError, ValueError)`, for both non-list input and invalid elements. | Keeps `except ValueError` and `except DataError` working. | `btc_trading_system/database/versioned_store.py:209` |
| P4-S2-M4 | 4.2 | Catalog SQL uses explicit column lists and named parameters (no `SELECT *`, no positional `VALUES`); rows are compared by column name. | Robust against column-order mistakes. | `btc_trading_system/database/versioned_store.py:171` |
| P4-S2-01 | 4.2 | Published files are `0444` and object directories `0555`; the API has no operation that mutates an object. | Immutability of the store of record. | `btc_trading_system/database/versioned_store.py:20-23` |
| P4-S2-02 | 4.2 | The reuse path may restore an object directory's mode to `0555`, only after full verification and never touching files. | Completes a publication interrupted between rename and chmod. | `btc_trading_system/database/versioned_store.py:881` |
| P4-S2-03 | 4.2 | Accepted limit: Linux `rename()` silently replaces an empty target directory; guarded by a check under the writer lock; a non-empty target gives `ImmutableObjectError`. | The standard library has no `RENAME_NOREPLACE`. | `btc_trading_system/database/versioned_store.py:70-73` |
| P4-S2-S2 | 4.2 | Abandoned `tmp/` staging is not cleaned up in Step 2; cleanup belongs to repair (Step 5). | Keep Step 2 in scope. | `btc_trading_system/database/versioned_store.py:74-76` |
| P4-S2-04 | 4.2 | mypy baseline may rise by exactly one `[import-untyped]` (pandas) for `versioned_store.py`; the previous errors must remain unchanged line for line. (An instance of the general rule G-08.) | Same category as the 6 accepted in Phase 3 (V4). | session 2026-09-30 |
| P4-S2-05 | 4.2 | Tests are hermetic and never read `~/btc_data` or other user paths; a Phase 2 store is built in `tmp_path` with Phase 2's own `DatasetStore`. | Reproducible on any machine. | commit `ab7ed7f` (`tests/test_database_versioned_store.py`) |

## Phase 4 Step 3: refs, snapshots, PinnedStore (commit `a6fab94`)

| ID | Phase/step | Decision | Rationale | Source |
|---|---|---|---|---|
| P4-S3-D1 | 4.3 | A pending `ref_log` entry (crash between log and ref) blocks `set_current`, `create_snapshot` and `snapshot_current` with `PendingRefUpdateError` until `complete_pending_ref_updates()` is called explicitly. It completes only if the entry is the dataset's last, its `previous_data_version` equals the ref file's version (or both absent), and the target passes full verification again. More than one pending entry is corruption. | Refs are never changed silently. | `btc_trading_system/database/versioned_store.py:90-97`, `:954` |
| P4-S3-D2 | 4.3 | Step 3 changes to `versioned_store.py` are additions only; in `tests/test_database_versioned_store.py` only the API-pin test changed. | Committed Step 2 behaviour stays byte-identical. | commit `a6fab94` |
| P4-S3-D3 | 4.3 | Snapshot members are sorted by `dataset_id`; no names or labels are part of the snapshot ID. | Deterministic, order-independent identity. | `btc_trading_system/database/refs.py:6`, `:10` |
| P4-S3-D4 | 4.3 | No timestamps in `ref_log` or snapshots; `as_of` filters bars inside the chosen version and never selects a historical version. Reproducible research and backtests MUST read through a snapshot (`PinnedStore`); `read_current(as_of=t)` is "latest known data cut at t", not point-in-time, and must never be used for a backtest. | Reproducibility; only snapshots pin versions. | `btc_trading_system/database/versioned_store.py:104-109` |
| P4-S3-D5 | 4.3 | `set_current` to the version the ref already points at is a no-op and writes no `ref_log` entry. | Consistent with the schema rule previous ≠ new. | `btc_trading_system/database/versioned_store.py:927` |
| P4-S3-01 | 4.3 | A reader's `pending` flag is advisory (can be a false positive during a concurrent `set_current`); only a writer's classification is authoritative; what a reader reads is always consistent. | Readers take no lock. | `btc_trading_system/database/versioned_store.py:687-689` |
| P4-S3-02 | 4.3 | `PinnedStore` accepts only a published snapshot: it reloads the snapshot file and requires equality (`SnapshotNotFoundError` / `SnapshotCorruptError` otherwise). | A PinnedStore must only report a durable, reproducible snapshot_id. | `btc_trading_system/database/versioned_store.py:1132` |

## Phase 4 Step 4: requirement recorded in advance (implemented in Step 4, see P4-S4)

| ID | Phase/step | Decision | Rationale | Source |
|---|---|---|---|---|
| P4-S4-01 | 4.4 | On the ingest path, `extra` may contain only `CONTENT_DERIVED_KEYS = {"gaps", "zero_volume_bars"}` (any other key → `ValueError` before anything is written); on reuse exactly these keys are compared with the stored manifest (mismatch → `ObjectCorruptError`); source-dependent fields in legacy manifests are preserved but not compared; new source lineage goes to `ingest_sources` and `ingest_events.duplicates_dropped`. Until Step 4, strict M2 applies. | Re-ingesting identical content from different files yields "reused" plus a new lineage row, never a `ProvenanceConflictError`. | `btc_trading_system/database/versioned_store.py:41-49` |

## Phase 4 Step 4: ingest with lineage

Design approved by the owner on 2026-09-30 (D-S4-a to D-S4-f, regression guard, fail-closed
capture store, 14 mutations), follow-up answers on 2026-10-01. P4-S4-01 is implemented in this
step. Line references are to this commit.

| ID | Phase/step | Decision | Rationale | Source |
|---|---|---|---|---|
| P4-S4-a | 4.4 | Ingest uses its own internal publish path (`_publish_ingest_version`) that compares only `CONTENT_DERIVED_KEYS` on reuse (P4-S4-01); the public `publish_version` is unchanged and keeps strict M2 (P4-S2-M2). | No committed Step 2 function body is edited. | owner decision 2026-09-30 (D-S4-a); `btc_trading_system/database/versioned_store.py:1404` |
| P4-S4-b | 4.4 | The ingest record is written before its `ref_log` line (intent log). A record whose planned `ref_log` line is missing is a pending ingest: `ingest`, `set_current`, `create_snapshot`, `snapshot_current` and `complete_pending_ref_updates` refuse with `PendingIngestError` until `complete_pending_ingest()` is called explicitly. It completes only if the record is the last one, the planned line is reproduced exactly, `previous_data_version` equals the ref file's version (or both absent) and the object passes full verification again. This needs exactly one inserted line in Step 3's `_load_ref_state` (`self._check_ingests(entries)`), which is a no-op when there are no ingest records. Condition: `tests/test_database_refs.py` unchanged and passing, and the mutation removing that line is killed. | Otherwise Step 3 operations could take the `ref_log` seq reserved by a pending ingest. | owner decision 2026-09-30 (D-S4-b); `btc_trading_system/database/versioned_store.py:855`, `:918`, `:1374` |
| P4-S4-c | 4.4 | `now`, `code_version` and `worktree_clean` are required explicit parameters of `ingest()`; the library does no clock or git lookup. | Determinism; the Step 8 CLI resolves git state. | owner decision 2026-09-30 (D-S4-c); `btc_trading_system/database/versioned_store.py:1307` |
| P4-S4-d | 4.4 | An ingest makes the ingested version current (`ref_outcome="unchanged"` if it already is). Regression guard: if the new version's last `open_time` is earlier than the current version's, `ingest()` raises `VersionRegressionError` before anything is written, unless `allow_regression=True` is passed explicitly; the flag is stored in the record's `params` (and `ingest_events.params_json`). Finding: `docs/PHASE4_SPEC.md` defines nothing on an ingest moving the ref backwards (searched for regress/older/backfill/earlier/last_open/coverage/shrink/rollback/move…back; the only hits, "rollback journal" at line 106 and "coverage" at lines 200 and 379, are unrelated), so the guard applies as decided. | Older data must not silently replace newer data. | owner decision 2026-09-30 (D-S4-d); finding confirmed by the owner 2026-10-01; `btc_trading_system/database/versioned_store.py:1338` |
| P4-S4-e | 4.4 | Ingest record types, ID computation and strict parsing live in the new module `btc_trading_system/database/lineage.py` (no I/O, like `refs.py`); the I/O stays in `versioned_store.py`. `ingest_id` = sha256(`btc-quant/ingest-id/v1\0` + compact canonical JSON of the record); every ingest gets a new record (`seq`, `prev_ingest_id` chain), including repeated ingests of identical content. | Separation of pure types from I/O. | owner decision 2026-09-30 (D-S4-e); `btc_trading_system/database/lineage.py:27`, `:120` |
| P4-S4-f | 4.4 → 6b | Quality-report registration (listed as a `versioned_store.py` component in the Phase 4 spec, with no numbered step) becomes its own Step 6b, directly after legacy import (Step 6), as a separate commit. | Closes a gap in the spec's step list. | owner decision 2026-09-30 (D-S4-f) |
| P4-S4-FC | 4.4 | Phase 2 `ingest_dataset()` runs unchanged against a fail-closed capture store (`_CaptureStore`): it accepts exactly one `write(key, frame, extra)` call for the expected key; any other method call (`data_path`, `manifest_path`, `exists`, `read_manifest`, `read`), a second `write` or a different key raises `StoreError` immediately; it writes nothing. | The Phase 2 pipeline is reused byte for byte without any write path of its own. | owner decision 2026-09-30; `btc_trading_system/database/versioned_store.py:517` |
| P4-S4-EX | 4.4 | Exceptions in `lineage.py` follow the Step 2 pattern (P4-S2-M3): one dedicated class per error kind, each subclassing both `DataError` and `ValueError`: `IngestExtraError` (Phase 2 `extra` cannot be split: missing or unknown key, wrong type, invalid source) and `IngestRecordError` (invalid ingest record or source, on construction or strict parsing). No split into `TypeError`/`ValueError`; ruff TRY004 is satisfied. | `except ValueError` and `except DataError` keep catching everything. | owner decision 2026-10-01; `btc_trading_system/database/lineage.py:40`, `:44` |
| P4-S4-API | 4.4 | Step 4 changes to `versioned_store.py` are additions only (including the one P4-S4-b line); in `tests/test_database_versioned_store.py` only the API-pin test changed (`ingest`, `complete_pending_ingest`, `read_ingest`, `ingest_history`). | Same rule as Step 3 (P4-S3-D2). | owner decision 2026-10-01 |
| P4-S4-FIX1 | 4.4 (fix) | `complete_pending_ref_updates()` also syncs the ingest catalog (`ingest_events`, `ingest_sources`) when the completed `ref_log` entry has `action="ingest"` (it belongs to exactly one ingest record, P4-S4-b). Additions only: two inserted lines; no other behaviour change (completing a `set_current` entry does not touch the ingest catalog). Found during Step 5: before the fix, after a crash in `ingest()` between the log line and the ref file, the catalog lacked the ingest row until the next ingest (verify: `CATALOG_BEHIND`). | The catalog equals the files immediately after an explicit completion. | owner decision 2026-10-01; `btc_trading_system/database/versioned_store.py:1155` |

## UNCERTAIN (for the owner to decide; not decisions)

None open. U-01 to U-07 were resolved by the owner on 2026-09-30:

| ID | Resolution |
|---|---|
| U-01 | Phase 6 execution decision found in the original chat: P6-01. |
| U-02 | Expected; the fingerprint is recorded in V5 at Phase 4 close: G-10. |
| U-03 | History is not rewritten; corrected by a dated addendum in `docs/VERIFICATION.md`: G-09. |
| U-04 | Resolved by adding `docs/ENVIRONMENT.md` (separate commit). |
| U-05 | Not locked; placeholders until their own commit: P1-04. |
| U-06 | General rule: G-08. |
| U-07 | Yes: this log is the accepted record for G-06, G-07 and P4-S2-04 (see the introduction). |
