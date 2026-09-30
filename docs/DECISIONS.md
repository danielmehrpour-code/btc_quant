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

## Phase 4 Step 4: requirement recorded in advance (not yet implemented)

| ID | Phase/step | Decision | Rationale | Source |
|---|---|---|---|---|
| P4-S4-01 | 4.4 | On the ingest path, `extra` may contain only `CONTENT_DERIVED_KEYS = {"gaps", "zero_volume_bars"}` (any other key → `ValueError` before anything is written); on reuse exactly these keys are compared with the stored manifest (mismatch → `ObjectCorruptError`); source-dependent fields in legacy manifests are preserved but not compared; new source lineage goes to `ingest_sources` and `ingest_events.duplicates_dropped`. Until Step 4, strict M2 applies. | Re-ingesting identical content from different files yields "reused" plus a new lineage row, never a `ProvenanceConflictError`. | `btc_trading_system/database/versioned_store.py:41-49` |

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
