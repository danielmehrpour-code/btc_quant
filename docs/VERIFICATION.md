# Verification record

## V1 — PHASE 1–2, real pytest (2026-09-28)

**Status: PASSED — 161 passed, 0 failed, 0 errors (3.04 s).** Run by the user; no code changed.

| Item | Value |
|---|---|
| Platform | Linux / WSL2 (Ubuntu 24.04) on Windows 10.0.26200 |
| Python | 3.12.3 |
| pytest | 9.1.1 |
| pandas | 3.0.6 |
| numpy | 2.5.3 |
| pyarrow | 25.0.1 |
| Command | `python -m pytest -v` |
| Tested artifact | `btc_quant_phase2_reviewed.zip`, sha256 `85bae23e7082f3b00da3622b77000de429b4758289ff8328ca76e6a1637e7716` |
| Code fingerprint | `af1f09ed617d19128d279b5ab0d168320be99589882a820a5c3d08c6fdaa9288` — **confirmed identical on the user's tested tree** |
| Lock file | `requirements-lock.txt` (repo root, `pip freeze --exclude-editable`), sha256 `946b357bf09a7ba189fd32017947d453541557482117a7e61dc12f01cd91b627` |
| Git | commit `fac4954ee94c1c2a8d3429f24d41307eb59076cb`, tag `phase2-verified` (tested code + this lock file) |

Recreate the verified environment: `python -m pip install -r requirements-lock.txt && python -m pip install -e . --no-deps`

Code fingerprint = sha256 over `sha256sum` of every file in `btc_trading_system/ tests/ configs/ scripts/ pyproject.toml`
(excluding `__pycache__`/`.pyc`), sorted by path. `docs/` is deliberately excluded so documentation
can change without invalidating a verification. Reproduce (Linux/WSL, from repo root):

```bash
find btc_trading_system tests configs scripts pyproject.toml -type f ! -path "*/__pycache__/*" ! -name "*.pyc" \
  | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -c1-64
```

161 = 158 tests previously run in the sandbox with a pytest stand-in + the 3 Parquet tests that
had never run before (backend round-trip/determinism, fixed schema without pandas metadata,
Parquet tamper detection). This is the first run with real pytest and real pyarrow.

### What this verification covers
- Data layer (reader, validation, gap flags, ingest, deterministic storage, provider), all on
  **synthetic** Binance-format data.
- Causality contract: `get_bars(as_of=t)` and `get_gaps(as_of=t)` never expose data unavailable at t.
- Config strictness and the live-trading gate (live trading remains disabled).
- Phase 1 interfaces are abstract and importable.

### What it does NOT cover (open)
1. ~~Real Binance files never ingested~~ → covered by **V2** below.
2. **Dependency lower bounds** in `pyproject.toml` (pandas>=2.1, numpy>=1.26, pyarrow>=15,
   Python>=3.11) are untested; only the versions above are verified.
3. **Parquet byte-determinism** is verified within one pyarrow version only. `data_version`
   (content hash) is designed to be pyarrow-independent, but the file bytes are not.
4. **Native Windows** is not a supported environment (see ENVIRONMENT.md).
5. ~~`ruff` and `mypy` never run~~ → baseline recorded in **V3** below.

## V2 — Real Binance smoke ingestion (2026-09-28)

**Status: PASSED — exit code 0, all steps passed.** Run by the user in the V1 environment;
no project code changed. Source: user's report. The full log (`smoke_v1.log`) has **not** been
attached to this record yet.

| Item | Value |
|---|---|
| Code | commit `fac4954ee94c1c2a8d3429f24d41307eb59076cb`, tag `phase2-verified`, fingerprint `af1f09ed…a9288` |
| Environment | as V1 (WSL2 Ubuntu 24.04, Python 3.12.3, pandas 3.0.6, numpy 2.5.3, pyarrow 25.0.1) |
| Script | `smoke_ingest_v1.sh` (kept outside the repo), sha256 `5cc8579511cd43243d9f533566169803a52f90b1fb25cfc1b08de488c1a9224d` |
| Source | data.binance.vision, official `.CHECKSUM` verified for every file |

Datasets (BTCUSDT, 15m):

| Instrument | Files | Result |
|---|---|---|
| spot | monthly 2024-12 (ms), monthly 2025-01 (µs), daily 2025-01-31 (µs, overlaps) | 5952 rows, 96 identical duplicates removed |
| perp (USD-M) | monthly 2024-12, 2025-01 | 5952 rows, 0 duplicates |

5952 = 62 days × 96 bars, and the check `rows + missing == 5952` passed, so **0 missing bars /
0 gaps** in both datasets for this period.

Checks passed (reported by user):
- Official Binance checksums; manifest `sources[].sha256` == Binance `.CHECKSUM`.
- Ingest of spot and USD-M perp from real files.
- Determinism: second ingest into a separate store gave byte-identical Parquet and manifest files.
- Negative controls on real files: wrong timeframe (filename and content), wrong symbol → rejected,
  nothing written.
- ms→µs format boundary (2024-12-31 23:45 → 2025-01-01 00:00) continuous, no gap flag.
- `as_of` causality on real data (12:07 → 11:45; 12:00:00 → 11:45; 11:59:59.999999 → 11:30;
  no bar with `available_at > as_of`).
- Spot and perp series not identical.

Binance facts now confirmed by official docs (previously from memory): spot timestamps are in µs
from 2025-01-01 with `close_time = open + tf − 1 µs`; USD-M futures klines in ms; path
`data/futures/um/{monthly,daily}/klines/BTCUSDT/<tf>/`.

**Not yet recorded (needs the log):** `data_version` of both datasets, whether USD-M files had a
header row, spot/perp descriptive statistics (bps difference, volume ratio).

### Scope limits of V2
- Only 2 months, only 15m. Other timeframes and the full history are not smoke-tested.
- Spot vs perp **cannot** be told apart by filename; correctness relies on the separate folders
  used by the script. The "not identical" check only catches duplicated files, not swapped folders.
- The spot/perp numbers printed by the script are descriptive, not a research result.

## V3 — Lint and type-check baseline (2026-09-28)

**Status: BASELINE RECORDED — 0 runtime/logic bugs found. No files changed; `--fix` was NOT run.**
Run by the user in the V1 environment on commit `fac4954ee94c1c2a8d3429f24d41307eb59076cb`
(tag `phase2-verified`); `git status --short` clean before and after. Classification by Claude,
decision **A**: no code change before PHASE 3.

| Item | Value |
|---|---|
| ruff | 0.16.9 — `ruff check . --output-format=concise` → 41 findings, exit 1 (35 auto-fixable, not applied) |
| mypy | 2.3.1 — `mypy btc_trading_system --show-error-codes` → 18 errors in 9 files (31 source files checked), exit 1 |

| Category | ruff | mypy | Total |
|---|---|---|---|
| 1. Real runtime/logic bug | 0 | 0 | **0** |
| 2. Wrong/incomplete type annotation | 0 | 7 | 7 |
| 3. Missing external stubs / environment | 0 | 11 | 11 |
| 4. Lint/style/modernisation, production code | 14 | 0 | 14 |
| 5. Test code only | 27 | 0 | 27 |
| **Total** | **41** | **18** | **59** |

**Category 2 (7, mypy)** — mypy cannot prove invariants the code guarantees; edge cases were
executed and none fail:
- `config/settings.py:85,86` — `validation_until` may be `None`: impossible, line 81 raises
  `ConfigError` unless both or neither are set (all four combinations executed).
- `config/settings.py:168,171,173,177` — `env` annotated `dict | None` but receives `os.environ`
  (a `Mapping`); correct annotation is `Mapping[str, str] | None`. `env=None` path executed: gate
  raises `LiveTradingNotAllowed` as intended.
- `data/binance_reader.py:85` — `_ISO.match(v)` may be `None`: impossible, lines 82–84 raise
  `MalformedDataError` first (executed with garbage and empty input).
- Not reported by any tool, same category: `data/schema.py` `to_epoch_ns` is annotated as returning
  a pandas ExtensionArray but returns a numpy int64 array; `to_epoch_ms` has no return annotation.

**Category 3 (11, mypy)** — `pandas` ×8 (no `pandas-stubs`), `yaml` ×1 (no `types-PyYAML`),
`pyarrow`/`pyarrow.parquet` ×2 (no type information shipped).
**Consequence:** without pandas stubs every pandas value is `Any`, so mypy has effectively **not**
type-checked the data layer. Evidence for data-layer correctness is the test suite (V1), the mutation
checks and the real-data run (V2), not mypy.

**Category 4 (14, ruff, production)** — I001 ×6, UP035 ×3, UP017 ×2, UP037 ×2 (one is the
`schema.py` annotation above), FURB162 ×1 (`settings.py:131`, `Z` replacement redundant on Python
≥ 3.11; behaviour verified identical), RUF100 ×1 (unused `noqa` in `binance_reader.py:94`; the
broad `except` re-raises as `MalformedDataError`, not silent).

**Category 5 (27, ruff, tests)** — I001 ×9, F401 ×5, UP017 ×5, DTZ001 ×2 (intentional naive
datetimes in tests that assert rejection), F841 ×1 (leftover `span`; test coverage unaffected),
RUF059 ×1, C408 ×1, SIM905 ×1, FURB167 ×1.

### Open items from V3 (deferred, option B)
- The repo has **no `[tool.ruff]` / `[tool.mypy]` configuration**; the enabled ruff rule set came from
  the tool version or a user-level config, so this lint result is not reproducible from the repo alone.
- Possible later maintenance commit: annotation fixes (category 2), ruff/mypy config, `pandas-stubs`
  and `types-PyYAML` in `[dev]`, test clean-up. It changes the code fingerprint and requires a full
  re-verification (pytest + ruff + mypy); adding `pandas-stubs` is expected to surface new mypy findings.

## V4 — PHASE 3 data quality (2026-09-29)

**Status: PASSED - all automated verification criteria met.**
Run by the user in WSL2 with `verify_phase3.sh`.
No `phase3-verified` tag has been created.

The data-quality report on real data contains one FAIL finding,
`TF_AGGREGATION_MISMATCH` (perp 1m to 15m). It was investigated and
classified as a source-data inconsistency. It is not a verification failure.

### Artifacts

- Code commit:
  `6eef6c7393fc73e24f6a3e1c5e74fa9795a1e90c`
- Patch `phase3.patch`, sha256:
  `50e496bd1ed36f88935875cad4505f87bb402cdd7229cc0f93ec9ce33168b22b`
- Verify script `verify_phase3.sh` (outside the repo), sha256:
  `b7669a923eef9d557a0c663fac85dfd3724236fd43a41dbf7573ed9e30fcfa56`
- Investigation script `investigate_tf_v1.py` (outside the repo), sha256:
  `5a647a61b228c6c460bc1a446dc86b668410fcfd8f04919ac10802fb6e6cdb6d`
- Log `verify_phase3_v2.log` (outside the repo), sha256:
  `8bf336a351e99c50cdf5cb3db780fff3517a8538ac11292bf18bf7d76b1a0134`
- Documentation `docs/DATA_QUALITY.md`, commit:
  `b3e904ca4cc3cd4675a8122c5a1a6c6fa8b45ee9`

### Environment

- WSL2, Python 3.12.3
- pandas 3.0.6, numpy 2.5.3, pyarrow 25.0.1
- pytest 9.1.1, ruff 0.16.9, mypy 2.3.1

### Automated checks

- pytest: 247 passed, 0 failed, 0 skipped.
- Causality tests: 20/20 passed.
- Mutation tests: 19/19 caught.
- ruff: 41 findings total, equal to the V3 baseline.
  0 new findings in Phase 3 files.
- mypy: 24 errors total = V3 baseline 18 + 6 expected `import-untyped`
  (pandas/yaml) in Phase 3 files. No unexpected new errors.
- Real Binance data: official `.CHECKSUM` verified for all files.
- Reports on real data: two runs byte-identical; store untouched.

### Real-data finding: `TF_AGGREGATION_MISMATCH`

Data: BTCUSDT, January 2025, monthly 1m and 15m archives
from data.binance.vision.

- spot 1m to 15m: PASS. 2976/2976 windows checked, 0 mismatches.
- perp 1m to 15m: FAIL. Exactly 3 mismatching windows:
  1. `2025-01-14T13:30:00+00:00`
  2. `2025-01-14T15:00:00+00:00`
  3. `2025-01-29T01:30:00+00:00`

### Investigation

`investigate_tf_v1.py` reads the Binance zip files directly with `csv`
and `Decimal`. It uses neither pandas nor Phase 2/Phase 3 aggregation code.

- Phase 2 fidelity confirmed. All raw rows are present in the store.
  0 values differ by more than 1 ULP (the verification criterion).
  Max ULP per dataset:
  - spot 1m: 0
  - spot 15m: 1
  - perp 1m: 0
  - perp 15m: 0

  This is not bit-identical for all datasets (spot 15m has max ULP 1).
- Independent `Decimal` aggregation reproduced the same 3 perp mismatches
  and 0 spot mismatches.
- The validator reported the same 3 perp mismatches.

**Classification:** source-data inconsistency between the Binance perp
1m and 15m archives used for the test. It is not established which
archive is correct. Alternative sources (Binance daily archives, REST,
aggTrades) were not checked.

The finding remains `FAIL`, as the locked specification requires.
It was not downgraded.

Addendum (recorded 2026-09-30): after the owner's review, the tag phase3-verified was created at 6eef6c7.

## V5 — PHASE 4 database / storage (2026-10-07)

**Status: PASSED.** Final verification of Phase 4 (Step 13). Decisions: `docs/DECISIONS.md` (P4-*); reference: `docs/PHASE4_DATASTORE.md`.

| Item | Value |
|---|---|
| Platform | Linux 6.18.33.2-microsoft-standard-WSL2 (WSL2 on Windows) |
| python | 3.12.3 |
| pytest | 9.1.1 |
| pandas | 3.0.6 |
| numpy | 2.5.3 |
| pyarrow | 25.0.1 |
| sqlite | 3.45.1 |
| ruff / mypy | ruff 0.16.9 / mypy 2.3.1 (compiled: yes) |
| Verified commit | `c51d20841d271a7035d9c7c65d12132c1e0c9d09` (clean worktree) |
| Last code-changing commit | `20e7581` (later commits change documentation only) |
| Code fingerprint | `8e83efb3cca627ea8f935d275e4d24f21f546c94e4225a6f994b895a1ac8bb19` (method as in V1; clean clone: `8e83efb3cca627ea8f935d275e4d24f21f546c94e4225a6f994b895a1ac8bb19`) |

### Automated checks

- pytest in the repository: 1171 passed in 248.22s (0:04:08).
- pytest in a throwaway clean clone of `c51d208` (worktree clean before and after): 1171 passed in 252.42s (0:04:12).
- ruff: 41 findings, the same count as V3/V4; the set is identical to the Step 11 baseline, and every Phase 4 step added 0 findings.
- mypy: Found 27 errors in 17 files (checked 45 source files) = the V4 baseline 24 + 3 expected `[import-untyped]` (pandas) in `btc_trading_system/database/{versioned_store,legacy,quality_registry}.py` (G-08). No other new error.
- Mutation tests (G-20 (3), P4-S11): all sets on `c51d208` with `--parallel 4`: **125/125 killed**, every baseline green, every overlay restored.

| Set | Killed | summary |
|---|---|---|
| step2 | 8/8 | baseline green, restored True, 1.9 min |
| step3 | 12/12 | baseline green, restored True, 4.8 min |
| step4 | 15/15 | baseline green, restored True, 4.2 min |
| step5 | 14/14 | baseline green, restored True, 28.9 min |
| step6 | 16/16 | baseline green, restored True, 45.7 min |
| step6b | 16/16 | baseline green, restored True, 54.2 min |
| step7 | 12/12 | baseline green, restored True, 5.8 min |
| step8 | 15/15 | baseline green, restored True, 5.4 min |
| step10 | 11/11 | baseline green, restored True, 42.4 min |
| step11 | 6/6 | baseline green, restored True, 3.4 min |

  Run directory `mutation-run-c51d20841d27-20261006T210234Z`; `summary.json` sha256 `56fe208e6e9f792e3162e5f428abafba131a5c254a2c854faf1282666c6b6360`. Memory during the parallel run: peak 1741 MiB used, at least 6073 MiB available, swap 154 MiB at start and 161 MiB at peak (P4-S11-PAR). The Step 11 runs: sequential `b7e6ce43…433f2`, parallel `b1f765f1…c79c48`.
- Runbook examples (`docs/PHASE4_DATASTORE.md` section 9, P4-S9-e): 32/32 examples OK.
- Reference check of `docs/PHASE4_DATASTORE.md`: 172 code references, 85 decision IDs; problems: 0.

### Finding F3 (P4-S10-d)

From `168fe0d` until `98cdead`, `tests/test_database_cli.py::test_subprocess_write_group` failed whenever the
worktree was clean: the CLI subprocess ran without `HOME`, so git did not read the user's global ignore file
(`.claude/settings.local.json`) and reported the worktree as unclean, while the test process reported it clean.
The Step 8 and Step 9 reports ("1068 passed") were true for the dirty worktree they ran in. Fixed in `98cdead`
(the test computes the expected state in the CLI's environment; `.gitignore` ignores the file). The clean-clone
run above is the check that guards against a repeat.

### Real-data migration (Step 12, P4-S12-RES)

Store `~/btc_data/datastore_v1`, migrated with the committed CLI at `856c9fe` (code identical to the
verified commit), after a clean rehearsal on byte-identical copies; all 35 evidence values and all 22 CLI outputs
of the rehearsal and the real run are identical. Evidence: `~/phase4_work/step12_evidence/`
(`real/evidence.json` sha256 `67dfcac62c3d595a6426bec4f7e2e90e1a9383d753f0070dbe7fa19520e8c9a8`, `real/equivalence.json` sha256 `64d87fe5feb5464844c6eab6f8907d248c3262d748b9df7880df2ae4cf18cf85`).

| Dataset | data_version | import_id | record_sha256 |
|---|---|---|---|
| binance/perp/BTCUSDT/15m | `d790427420bc4fcd6389b7da814d1a6d8441db916888a9d79730262239a50833` | `b215724b93e2f6879ecf2a3af0d4a4ab35db905e6150c70e11ebd17f68f64f30` | `070de90db7c307e2f1c009c757a8c94b68f8c6dda679d00b4343516a5f875dc8` |
| binance/perp/BTCUSDT/1m | `a86139cd0fc335214146165fdfeff8ffbc2f69071d221fcc2080c56b039bef7a` | `a2e1eb5c07bc10acb8d89874ce909e1a96aebd7d4df53378ebb4ea8f5f730fe5` | `5650094dd64ae63c16eac2a1c9a630fafe40cf1a725cb0d2f6a6618e89a33ad8` |
| binance/spot/BTCUSDT/15m | `32585a79980d658f97444069ec45f98165f5c144d6453dc1a8f4804a6b1f880f` | `26e553a2b508f385ddcff8057c11a5a9482144670e94959c4598c796101fe011` | `de60bacd0f32ea91fe7048ec613975a4ef0e0c54f845b23002cf37090ee54d13` |
| binance/spot/BTCUSDT/1m | `2185582e271f3bc0ae123a9d5959cfcddd1722f027f04011c139fbc4fbddc202` | `adad797a923bb53e443ec3a6e9f16d9828fa79ab6fca954ad8c885642c6b2534` | `760dea41e2bd6b84f4c2b16a1ebba7078489082d579079a78b39bd3278e38689` |

| Quality report (run1 = run2) | Type | Overall | report_sha256 |
|---|---|---|---|
| `dataset_perp_BTCUSDT_15m_d790427420bc4fcd.quality.json` | dataset | WARN | `1486ee408fc85bce661fee9a37050f8aae971cc71112011bc2022578c8866646` |
| `dataset_perp_BTCUSDT_1m_a86139cd0fc33521.quality.json` | dataset | WARN | `80a5d82f3be535365dfd41ab1ca2833108fff10a783c1d24b1f16c678e10c7bb` |
| `dataset_spot_BTCUSDT_15m_32585a79980d658f.quality.json` | dataset | WARN | `8796585f1433d06872bba9b265b9db8783dc73a4d203faa3ec218d40322a958a` |
| `dataset_spot_BTCUSDT_1m_2185582e271f3bc0.quality.json` | dataset | WARN | `621a892af890e7a2b12e9a83b2b710a95033faa5489bcac1526e1040f8568e57` |
| `tf_perp_BTCUSDT_1m-15m_a86139cd0fc3_d790427420bc.quality.json` | tf_aggregation | FAIL | `a04d6d7d3bf7a7e66ee9ea5e80d6781891fe6a5feec525574a9e3aa198d59351` |
| `tf_spot_BTCUSDT_1m-15m_2185582e271f_32585a79980d.quality.json` | tf_aggregation | PASS | `050cfd4246d5bcd3218ba5477195945811c97aedbbbf587e4090958a3d10f595` |
| `xinst_BTCUSDT_15m_32585a79980d_d790427420bc.quality.json` | cross_instrument | PASS | `dc186744bb02b2cbb826e6f254aed11cce17bc7e3d8fc6abc191fe7032de13dd` |
| `xinst_BTCUSDT_1m_2185582e271f_a86139cd0fc3.quality.json` | cross_instrument | PASS | `d9a4f49e232e793de29d17f67e3b30d1c84f01e1d92de00ad22edd9bf25111af` |

The `tf_aggregation` FAIL of perp 1m-15m is the V4 finding `TF_AGGREGATION_MISMATCH`; it stays FAIL (P3-04).

| Item | Value |
|---|---|
| Snapshot of the four datasets | `36696042d4cd6ab73b358be8f3f13119f30e7424e6d55bbce99c93539fec8e00` |
| Verify report sha256 (Step 12, final) | `866029e39afcf4fb88c822bcc77a15409e1f30cdbf3fb65c9584d55edf87f0a2` |
| Catalog dump sha256 (rebuild identical) | `d499e75f06ee94c8b1dbbb5fac9ac791e4e053914dab98636a12b1408202b620` |
| Store tree hash after migration | `f6c407e437e56a1b9f41f9582ad56ae97cc92ad6fe6f7284cf4cad033cd1b021` |
| Step 13 re-verify of the store | exit 0, report sha256 `866029e39afcf4fb88c822bcc77a15409e1f30cdbf3fb65c9584d55edf87f0a2`; store tree hash before `f6c407e437e56a1b9f41f9582ad56ae97cc92ad6fe6f7284cf4cad033cd1b021`, after `f6c407e437e56a1b9f41f9582ad56ae97cc92ad6fe6f7284cf4cad033cd1b021` |

### Source and data tree hashes

Method: `~/phase4_work/treehash.sh` (paths, modes and contents).

| Directory under `~/btc_data` | Before Step 13 | After Step 13 |
|---|---|---|
| `store_quality_v1` | `a8f2fd15e2450edb77d6064dc090ca4ee7817f94b67e5b60e738d8db6f2be50b` | `a8f2fd15e2450edb77d6064dc090ca4ee7817f94b67e5b60e738d8db6f2be50b` |
| `quality_reports_v1` | `e673a327b8d5d74453f19952be98201ce413fe7f2b9c3cf46d0e7e593a369038` | `e673a327b8d5d74453f19952be98201ce413fe7f2b9c3cf46d0e7e593a369038` |
| `datastore_v1` | `f6c407e437e56a1b9f41f9582ad56ae97cc92ad6fe6f7284cf4cad033cd1b021` | `f6c407e437e56a1b9f41f9582ad56ae97cc92ad6fe6f7284cf4cad033cd1b021` |
| `investigate_tf_v1` | `066bd09c6a2162419b93e49ba6e346d73cba6eb3bad35cc79a0eb1aa42a5f1b5` | `066bd09c6a2162419b93e49ba6e346d73cba6eb3bad35cc79a0eb1aa42a5f1b5` |
| `raw` | `196b8631a791dadaea7692ccf2bfa07324849836c529f53adbf7a03af50288ba` | `196b8631a791dadaea7692ccf2bfa07324849836c529f53adbf7a03af50288ba` |
| `store_smoke_v1` | `bc1b94ce258c2ff985e189ab6249edebffaefab19e347f9bc064d700f0ed50e2` | `bc1b94ce258c2ff985e189ab6249edebffaefab19e347f9bc064d700f0ed50e2` |
| `store_smoke_v1_rerun` | `bc1b94ce258c2ff985e189ab6249edebffaefab19e347f9bc064d700f0ed50e2` | `bc1b94ce258c2ff985e189ab6249edebffaefab19e347f9bc064d700f0ed50e2` |

`store_quality_v1` and `quality_reports_v1` equal their Phase 3 baselines (`a8f2fd15…be50b`, `e673a327…69038`)
throughout Phase 4; they were also checked after every command of the Step 12 migration (24 checks).

