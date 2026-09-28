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
5. `ruff` and `mypy` are installed via `[dev]` but have never been run.

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
