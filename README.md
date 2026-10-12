# btc_quant

A research framework for testing whether a systematic Bitcoin strategy can beat **both** buy-and-hold BTC **and** a global equity index fund (MSCI World type) held in a Swedish investment savings account (ISK), measured in SEK, **net of trading costs and Swedish tax**, on data no strategy decision has seen.

> Status: research infrastructure in progress (roadmap stage B: data and preconditions). No strategy has been built or tested yet, so there are no strategy results, by design (see *Out-of-sample discipline*). Live trading is off by default and is not part of the current work.

## Why this project is interesting
Most hobby trading bots fail because of look-ahead bias, overfitting and ignoring costs and tax. This project is built the other way round: the data, the evaluation rules and the benchmarks are fixed, tested and recorded *before* any strategy result exists. If no strategy passes, the pre-registered outcome is to hold the index fund.

## Key engineering features
- **Out-of-sample discipline**: the data split is locked in its own commit, with exclusive UTC bounds and a 7-day embargo between segments: TRAIN [2020-01-01, 2024-01-01), VALIDATION [2024-01-08, 2025-07-01), FINAL TEST [2025-07-08, 2026-10-01). FINAL TEST is used once, at the last gate, for the candidates selected on VALIDATION; nothing may be tuned on it.
- **Causality by construction**: every bar has `available_at = open_time + timeframe`, and a decision at time *t* may only use data with `available_at <= t`; external series (such as funding rates) are joined backward as-of on their publication time; fills happen strictly after the signal (`docs/LOOKAHEAD_POLICY.md`).
- **Reproducibility**: downloaded files are checked against the source's published checksums and kept unchanged in read-only archives; the datastore is versioned and immutable, and backtests read only through pinned snapshots, never "latest data".
- **Data quality reports**: gap, duplicate, invariant and anomaly checks with locked thresholds; findings are reported, never silently fixed, and a FAIL stays a FAIL.
- **Testing**: unit tests, hand-calculated cases with exact expected values, and mutation testing that proves the tests catch deliberately injected bugs.
- **Benchmark in SEK and Swedish tax model** (in progress, stage B step 5): an MSCI World UCITS ETF converted with the Riksbank's USD/SEK rate, with brokerage, FX fee and spread; ISK standard-income tax including the 2025/2026 basic-level deduction, and crypto capital-gains rules, each rule referenced to the Income Tax Act (SFS 1999:1229). A comparison model only, not tax advice.

## Architecture
```
data.binance.vision ──> scripts/data/fetch_binance.py ──> ~/btc_data/binance_archive_v1
                                                          (read-only, checksum-verified)
                                                                    │
                                                                    v
                           btc_trading_system/data      (readers, ingest, funding schema)
                                                                    │
                                                                    v
                           btc_trading_system/database  (versioned datastore, catalog,
                                                          snapshots / PinnedStore;
                                                          CLI: scripts/datastore.py)
                                                                    │
                ┌───────────────────────────────────────────────────┤
                v                                                   v
  btc_trading_system/data_validation             strategy research (stage C onward;
  (quality reports: scripts/data_quality_report.py)  features, backtesting, strategies,
                                                      risk: placeholders today)
                                                                    │
  benchmark + tax model (stage B step 5, in progress) ──> evaluation <┘
                                                       (locked, hashed config; gates)
```
Configuration lives in `configs/` (`default.yaml` holds the locked data split). The documents in `docs/` are authoritative (`docs/MASTER_SPEC.md`, `docs/ROADMAP.md`, `docs/DECISIONS.md`); this README is only a summary.

## How to run
WSL2 or Linux only; keep the repository in the Linux file system (`docs/ENVIRONMENT.md`).
```bash
python3 -m venv .venv && source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e ".[dev]"

python -m pytest -q                       # unit tests and hand-calculated cases
ruff check . --no-cache                   # lint; known findings are tracked as a baseline, new ones are not accepted
mypy btc_trading_system                   # types; only [import-untyped] may be added
python scripts/mutation/run.py --scratch <dir outside the repo> --sets all --parallel 4   # mutation tests
```
The mutation runner copies the code at a commit into overlays under `--scratch` and refuses any path inside the repository.

## Process
- Design first, then implementation, one step at a time; every decision is logged with its rationale and source in `docs/DECISIONS.md`, and every phase close is recorded in `docs/VERIFICATION.md` before its `phaseN-verified` tag.
- Development is AI-assisted (Claude Code). I write the requirements, review every design and diff, and approve each step; AI writes most of the code under those constraints.
- A staged roadmap with go/no-go gates and a pre-registered stop rule (`docs/ROADMAP.md`).

## Not included
Market and fund data are not in the repository. They are kept in read-only stores outside it, and some providers' terms do not allow redistribution.

## Roadmap
Stages from `docs/ROADMAP.md`:
- [x] A: close Phase 4 (versioned datastore, verified; tag `phase4-verified`)
- [ ] B: data and preconditions (in progress: archive download, data split and funding done; SEK benchmark and tax model in progress; ingest, quality reports and venue choice to follow)
- [ ] C: thin end-to-end slice
- [ ] D: edge test of 10–20 pre-registered hypotheses on VALIDATION
- [ ] E: deepen, then a single run on FINAL TEST
- [ ] F: change of direction (market-neutral funding/basis) if nothing passes, otherwise "hold the index fund"
- [ ] G–H: paper trading, then live with at most 50,000 SEK
