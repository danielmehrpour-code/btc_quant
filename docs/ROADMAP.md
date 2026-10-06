# Roadmap after Phase 4: stages and gates

Status: owner decision (G-24). This document turns docs/MASTER_SPEC.md
section 1.3 (project economics, success and stop criteria) and section 64 (do not build
everything at once) into a binding order of work. It governs the order of work and the
gates; the master spec governs scope. If this document and the master spec or
docs/DECISIONS.md conflict, stop and ask the owner (G-14).

## Principles

1. The riskiest assumption is tested first: that a realistic, net-of-cost edge exists
   for this owner (capital at most 50,000 SEK, Swedish taxes) against both benchmarks
   in 1.3 item 2.
2. A stage starts only when its gate is passed. A gate is passed only when every item
   listed for it is done, committed and recorded in docs/DECISIONS.md.
3. Everything that could be influenced by a result is locked before that result
   exists (1.1, 1.2, P1-05).
4. Quality is never reduced to go faster (G-15). Each stage uses the working process of
   Phase 4: design first, approval with stop conditions, scratchpad, mutation tests
   where errors are most expensive (causality, costs, risk), G-20.
5. Backtests and research read only through snapshots (P4-S3-D4).

## Known starting point

The only real market data in the repository's stores is January 2025
(store_quality_v1, migrated in Phase 4 Step 12). It was used only for data-quality
checks (P1-04). Multi-year history is needed before any data split or edge test is
possible.

## Stage A: close Phase 4

Phase 4 Steps 12 and 13: real-data migration, final verification, V5, tag
phase4-verified.

Gate 0 (before Stage B): the tag phase4-verified exists and V5 is committed.

## Stage B: data and preconditions

1. Historical data: BTCUSDT spot and USD-M perpetual, 1m (15m derived or downloaded as
   in Phase 2), from the earliest reliable year to the most recent complete month, from
   the Binance public archive (G-18). Every downloaded file is checked against the
   archive's published checksum before ingest.
2. Funding-rate history for the perpetual, with its source and availability verified
   (1.2 item 4).
3. Ingest through the committed CLI (datastore ingest, clean worktree), Phase 3 quality
   reports for every dataset, registered in the store. A FAIL stays FAIL; locked
   thresholds are never changed (P3-01, P3-04).
4. Data from the chosen execution venue (U-09) for the same instruments, as far as it
   is available, for validation on that venue (1.2 item 2, U-09).

Gate 1 (before Stage C; nothing in it may depend on a strategy result):
- the data split TRAIN / VALIDATION / FINAL TEST is locked in its own commit (P1-05);
  FINAL TEST is the most recent period no feature, strategy or parameter choice has
  seen;
- the execution venue is chosen and U-09 is resolved;
- the evaluation configuration is locked and hashed: benchmarks (buy-and-hold BTC; a
  global equity index fund of MSCI World type in an ISK, with its fee assumption), the
  Swedish tax model for crypto disposals and for the ISK, the venue's fees, slippage
  and funding assumptions, and the cost stress cases (1.2 item 2, 1.3 item 2);
- the hypothesis registry is locked and hashed: 10 to 20 hypotheses (the exact number
  stated), each with its strategy family, rules, parameter ranges, the data it may use,
  and the pre-registered pass criteria (net of costs and taxes, against both
  benchmarks, with confidence intervals and a multiple-testing correction such as the
  Deflated Sharpe Ratio, and a minimum number of trades). Simple baselines are
  included.

## Stage C: the thin end-to-end slice

The first working version of section 64, built only as far as the edge test needs
(1.3 item 3). Master spec phases: 5 (features), 6 (backtester), 7 (simple momentum
baseline), 8 (risk), 9 (execution simulation), 10 (performance report), each in
minimal form.
- Features: only those the registered hypotheses use, each with causality tests.
- Backtester: decision at t, fill strictly after t with explicit latency, fill price
  from market data after the latency (P6-01); conservative taker execution; fees,
  slippage and funding of the chosen venue; reads through snapshots.
- Risk: volatility-targeted sizing, low leverage cap, exposure limits (1.2 item 5).
- Report: results in SEK on the capital (1.3 item 1), against both benchmarks, net of
  costs and taxes, with confidence intervals, break-even cost and the stress cases.

Gate 2 (before Stage D): the slice is verified: look-ahead and leakage tests pass,
mutation tests on causality, costs and risk are all killed, and a known synthetic
strategy gives its expected result.

## Stage D: the edge test

The registered hypotheses run through the slice: parameters are chosen on TRAIN only,
evaluated on VALIDATION. FINAL TEST is not touched. Every hypothesis and variant tried
is logged, including discarded ones (1.1).

Gate 3 (the stop rule, 1.3 item 4):
- at least one hypothesis meets its pre-registered criteria on VALIDATION: Stage E;
- none does: Stage F.
The outcome is recorded in docs/DECISIONS.md before the next stage starts.

## Stage E: deepen (only after a passing hypothesis)

Walk-forward testing (phase 11), parameter robustness and Monte Carlo, portfolio
evaluation of several uncorrelated candidates (phase 17, 1.2 item 7), the conservative
limit-order fill model (1.2 item 3), fractional-Kelly cap and drawdown rules (1.2 item
5). Additional hypotheses are added only as a new pre-registered batch and count in the
multiple-testing correction.

Gate 4 (before Stage G): FINAL TEST is run once, for the final candidates selected on
VALIDATION. If no candidate passes on FINAL TEST, the project goes to Stage F.

## Stage F: change of direction

Market-neutral funding and basis strategies (phase 14 scope, 1.2 item 4), as a new
pre-registered batch with the same gates (Gate 1 items for this batch, then Stages C
to E as needed). If none passes: the project ends with the outcome "hold the index
fund", recorded in docs/DECISIONS.md.

## Stage G: paper trading

Master spec phases 18 to 20: exchange integration in test or paper mode, monitoring,
the control panel (51.1) and Telegram (52.1), per docs/SPEC_MONITORING_REPORTS.md.
Monitoring thresholds are locked in a hashed configuration before the first
paper-trading day.

Gate 5 (before Stage H): the pre-registered paper-trading criteria are met (including
the minimum number of trades in docs/SPEC_MONITORING_REPORTS.md), and the initial live
allocation and its scaling rule are pre-registered.

## Stage H: live trading

Master spec phase 21: at most 50,000 SEK (1.3 item 1), with the kill switch and
monitoring in place, starting with the pre-registered initial allocation.

## Deferred unless motivated

Machine learning (phase 16) only if simple models do not suffice and the data allows
it (section 19). Order-flow research (phase 15) needs order-book recording, which
starts only by owner decision (U-08).
