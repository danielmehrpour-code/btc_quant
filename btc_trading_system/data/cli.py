"""CLI: python scripts/ingest_binance.py --store data_store --instrument spot --timeframe 15m files..."""
from __future__ import annotations

import argparse
import sys

from btc_trading_system.data.ingest import ingest_dataset
from btc_trading_system.data.storage import DatasetStore, StorageBackend
from btc_trading_system.data_validation.errors import DataError


def main(argv: list[str] | None = None, backend: StorageBackend | None = None) -> int:
    ap = argparse.ArgumentParser(description="Ingest lokala Binance kline-filer (ingen nätverksåtkomst).")
    ap.add_argument("files", nargs="+")
    ap.add_argument("--store", default="data_store")
    ap.add_argument("--instrument", choices=["spot", "perp"], required=True)
    ap.add_argument("--timeframe", required=True)
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--fail-on-gaps", action="store_true")
    a = ap.parse_args(argv)
    try:
        res = ingest_dataset(a.files, a.instrument, a.timeframe, DatasetStore(a.store, backend),
                             symbol=a.symbol, fail_on_gaps=a.fail_on_gaps)
    except DataError as e:
        print(f"INGEST FAILED: {e}", file=sys.stderr)
        return 2
    m = res.manifest
    print(f"OK {res.key.relpath}: {m['rows']} rader {m['first_open_time']} -> {m['last_open_time']} "
          f"| luckor={m['gap_count']} (saknade barer={m['missing_bars_total']}) "
          f"| duplikat borttagna={res.duplicates_dropped} | data_version={m['data_version'][:16]}")
    return 0
