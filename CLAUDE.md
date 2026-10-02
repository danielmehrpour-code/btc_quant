# CLAUDE.md: project instructions for btc_quant

Rules and pointers only. The documents below are authoritative; read them, do not copy them here.
Documents in `docs/` win over this file. If documents in `docs/` conflict with each other, stop
and ask the owner; never choose yourself.

## Project
- BTC quant research platform. Research first: if nothing passes out-of-sample, walk-forward and
  risk criteria, deploy nothing (cash). Live trading is off by default (`LIVE_TRADING`).
- Quality and correctness are the only decision criteria; every check, test, review and verification is done in full; complexity only where it has a clear function (G-15).

## Authoritative documents (read, never reconstruct from memory)
- `docs/MASTER_SPEC.md`: scope (sections 0-72, incl. 1.1 PRIMARY OBJECTIVE).
- `docs/PHASE4_SPEC.md`: Phase 4 scope and step order.
- `docs/DECISIONS.md`: all locked decisions (G-*, P*-*). Check it before any design choice.
- `docs/LOOKAHEAD_POLICY.md`: causality rules for every feature, signal and fill.
- `docs/VERIFICATION.md`: verification records (V1-V5), fingerprint method.
- `docs/ENVIRONMENT.md`: supported environment.

## Working process (G-12)
- One spec step at a time; stop and wait for approval after each step.
- Stage new code in `~/phase4_work/scratch/` first (persistent; never `/tmp`). Overlays and
  mutation runs go there too. Show full new files, diffs of committed files, sha256,
  syntax/import/ruff/mypy, overlay test results and mutation results.
- Copy into the repo only byte-identical, approved files.
- No commit, tag, push or reset without explicit approval.
- Mutation harnesses live in `~/phase4_work/mutation/` until Phase 4 Step 11.

## Decision recording (G-11, G-13)
- Every new decision goes into `docs/DECISIONS.md` in the same commit as its code
  (ID, phase/step, decision, rationale, source).
- Every step report ends with "New decisions this step:" (a list, or "none").
- Every phase close adds a V-record to `docs/VERIFICATION.md` before the phase tag.
- Flag anything that exists only in chat or memory and affects scope or behaviour.
- `docs/DECISIONS.md` is a living log: headings/structure may change visibly; recorded decision
  texts never change; a decision changes only by a new entry superseding the old ID.
  `docs/VERIFICATION.md` is never rewritten (G-09); correct it with a dated addendum.

## Protected
- Phase 1-3 files and committed Phase 4 steps: additions only, or justify the change and show
  the exact diff before touching them.
- Never modify data stores under `~/btc_data` (e.g. `store_quality_v1`); read-only.
- Never adjust locked thresholds or splits after seeing real data (P1-04, P3-01, P3-02).
- `phase*-verified` tags are never moved or deleted.
- Backtests and research read through a snapshot (`PinnedStore`), never `read_current` (P4-S3-D4).

## Verification commands (repo root, `.venv`)
```bash
.venv/bin/python -m pytest -q             # count must match the last step report or V-record
.venv/bin/ruff check . --no-cache         # count must match the last step report or V-record; 0 new
.venv/bin/mypy btc_trading_system         # per G-08: no new error other than [import-untyped]
find btc_trading_system tests configs scripts pyproject.toml -type f ! -path "*/__pycache__/*" \
  ! -name "*.pyc" | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -c1-64   # code fingerprint
~/phase4_work/treehash.sh ~/btc_data/store_quality_v1    # must equal ~/phase4_work/BASELINES.md
```

## Environment
- WSL2 (Ubuntu) only; native Windows is not supported. Keep the repo under `~/`. See
  `docs/ENVIRONMENT.md`.
