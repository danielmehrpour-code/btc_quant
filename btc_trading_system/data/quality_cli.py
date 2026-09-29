"""CLI för Phase 3-kvalitetsrapporter. Läser endast från lagret; skriver till --out.

Returkoder: 0 = PASS eller WARN, 1 = FAIL, 3 = INCOMPLETE, 2 = fel (indata/konfiguration),
4 = möjligt Phase 2-fel (stoppa och rapportera; rättas inte inom Phase 3).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from btc_trading_system.data.schema import DatasetKey, Instrument
from btc_trading_system.data.storage import DatasetStore, StorageBackend
from btc_trading_system.data_validation import quality_config as qc
from btc_trading_system.data_validation import quality_report as qr
from btc_trading_system.data_validation.errors import DataError

REPO_ROOT = Path(__file__).resolve().parents[2]
EXIT_CODES = {"PASS": 0, "WARN": 0, "FAIL": 1, "INCOMPLETE": 3}
GitState = Callable[[], tuple[str, bool | None]]


def git_state(root: Path = REPO_ROOT) -> tuple[str, bool | None]:
    """(commit, ren arbetskatalog). ('unknown', None) om git saknas eller inte är ett repo."""
    try:
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True,
                              text=True, check=False)
        status = subprocess.run(["git", "status", "--porcelain"], cwd=root,
                                capture_output=True, text=True, check=False)
    except OSError:
        return "unknown", None
    if head.returncode != 0 or status.returncode != 0:
        return "unknown", None
    return head.stdout.strip(), status.stdout.strip() == ""


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Phase 3 datakvalitetsrapport (read-only mot lagret).")
    sub = ap.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--store", required=True)
    common.add_argument("--out", required=True)
    common.add_argument("--symbol", default="BTCUSDT")
    common.add_argument("--config", default=None)
    d = sub.add_parser("dataset", parents=[common])
    d.add_argument("--instrument", choices=["spot", "perp"], required=True)
    d.add_argument("--timeframe", required=True)
    t = sub.add_parser("tf", parents=[common])
    t.add_argument("--instrument", choices=["spot", "perp"], required=True)
    t.add_argument("--lower", required=True)
    t.add_argument("--higher", required=True)
    x = sub.add_parser("xinst", parents=[common])
    x.add_argument("--timeframe", required=True)
    return ap


def main(argv: list[str] | None = None, backend: StorageBackend | None = None,
         git: GitState | None = None) -> int:
    a = _parser().parse_args(argv)
    store_root, out = Path(a.store).resolve(), Path(a.out).resolve()
    if out == store_root or store_root in out.parents:
        print("QUALITY FAILED: --out får inte ligga i datalagret", file=sys.stderr)
        return 2
    try:
        cfg = qc.load_quality_config(a.config)
        store = DatasetStore(store_root, backend)
        version, clean = (git or git_state)()

        def read(instrument: str, timeframe: str) -> tuple[Any, DatasetKey, dict[str, Any]]:
            key = DatasetKey(Instrument(instrument), a.symbol, timeframe)
            frame, manifest = store.read(key)
            return frame, key, manifest

        if a.command == "dataset":
            f, k, m = read(a.instrument, a.timeframe)
            report = qr.build_dataset_report(f, k, m, cfg, version, clean)
        elif a.command == "tf":
            lo_f, lo_k, lo_m = read(a.instrument, a.lower)
            hi_f, hi_k, hi_m = read(a.instrument, a.higher)
            report = qr.build_tf_report(
                lo_f, lo_k, lo_m, hi_f, hi_k, hi_m, cfg,
                code_version=version, worktree_clean=clean,
            )
        else:
            s_f, s_k, s_m = read("spot", a.timeframe)
            p_f, p_k, p_m = read("perp", a.timeframe)
            report = qr.build_xinst_report(
                s_f, s_k, s_m, p_f, p_k, p_m, cfg,
                code_version=version, worktree_clean=clean,
            )
        json_path, _ = qr.write_report(report, out)
    except qr.Phase2SuspectError as e:
        print(f"PHASE 2 SUSPECT (stoppa och rapportera): {e}", file=sys.stderr)
        return 4
    except (DataError, qc.QualityConfigError, qr.ReportBindingError, ValueError) as e:
        print(f"QUALITY FAILED: {e}", file=sys.stderr)
        return 2
    print(f"{report['overall']} {qr.report_stem(report)} -> {json_path}")
    return EXIT_CODES[report["overall"]]
