"""Ingest: lokala filer -> validerat, deterministiskt dataset.

Policy (dokumenterad i docs/DATA_INGESTION.md):
  * Varje källfil måste i sig vara stigande i tid och validera (utom duplikat).
  * Filer får ges i vilken ordning som helst; de sorteras efter open_time.
  * IDENTISKA duplikat (t.ex. överlappande månads-/dagsfiler) tas bort och räknas.
  * MOTSTRIDIGA duplikat (samma open_time, olika värden) -> fel. Ingen tyst gissning.
  * Luckor kompletteras ALDRIG (ingen ffill/interpolering); de flaggas via
    gap_before + manifest. `fail_on_gaps=True` gör dem till fel.
  * Barer som inte var avslutade vid `now` avvisas (partiell/framtida bar).
  * Filnamn enligt Binance-mönster (SYMBOL-tf-YYYY-MM[-DD]) måste matcha symbol/timeframe.
  * Allt-eller-inget: vid fel skrivs ingenting.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import re

import pandas as pd

from btc_trading_system.data.binance_reader import SourceInfo, read_source_file
from btc_trading_system.data.schema import DatasetKey, Instrument, to_epoch_ns
from btc_trading_system.data.storage import DatasetStore, canonicalize
from btc_trading_system.data_validation.errors import DataValidationError, MalformedDataError
from btc_trading_system.data_validation.ohlcv import (ValidationReport, check_ohlcv,
                                                      compute_gap_flags)

DEDUP_CODE = "DUPLICATE_TIMESTAMPS"
# Binance-namn: BTCUSDT-15m-2024-01.zip / BTCUSDT-15m-2024-01-31.csv
_BINANCE_NAME = re.compile(r"^([A-Z0-9]+)-([0-9]+[a-zA-Z]+)-\d{4}-\d{2}(?:-\d{2})?\.(?:zip|csv|parquet)$")


def check_filename(name: str, key: DatasetKey) -> None:
    """Om filnamnet följer Binance-mönstret måste symbol och timeframe matcha.
    OBS: spot- och perp-filer har SAMMA namn hos Binance och kan inte skiljas åt här."""
    m = _BINANCE_NAME.match(name)
    if m and (m.group(1) != key.symbol or m.group(2) != key.timeframe):
        raise MalformedDataError(f"{name}: filnamnet anger {m.group(1)} {m.group(2)}, "
                                 f"men ingest gäller {key.symbol} {key.timeframe}")


@dataclass(frozen=True)
class IngestResult:
    key: DatasetKey
    manifest: dict
    report: ValidationReport
    duplicates_dropped: int


def ingest_dataset(files: Sequence[str | Path], instrument: Instrument | str, timeframe: str,
                   store: DatasetStore, symbol: str = "BTCUSDT",
                   now: datetime | None = None, fail_on_gaps: bool = False) -> IngestResult:
    key = DatasetKey(Instrument(instrument), symbol, timeframe)
    if not files:
        raise ValueError("inga filer angivna")
    now = now or datetime.now(timezone.utc)

    frames: list[pd.DataFrame] = []
    sources: list[SourceInfo] = []
    for f in files:
        df, info = read_source_file(f)
        check_filename(info.name, key)
        rep = check_ohlcv(df, timeframe, now=now)
        # duplikat inom/mellan filer hanteras efter sammanslagning; allt annat är fatalt
        rep.raise_if_errors(ignore=(DEDUP_CODE,), context=info.name)
        # close_time är validerad; tas bort så att samma bar från en ms- och en µs-fil
        # (Binance formatbyte 2025) dedupliceras som identisk i stället för att ge konflikt
        frames.append(df.drop(columns=["close_time"], errors="ignore"))
        sources.append(info)

    allf = pd.concat(frames, ignore_index=True)
    allf = allf.sort_values("open_time", kind="stable").reset_index(drop=True)

    # --- dubbletthantering ---
    dup_mask = allf["open_time"].duplicated(keep=False)
    dropped = 0
    if dup_mask.any():
        deduped = allf.drop_duplicates(keep="first").reset_index(drop=True)   # exakt lika rader
        if deduped["open_time"].duplicated().any():
            rep = ValidationReport(rows=len(allf))
            n = int(deduped["open_time"].duplicated().sum())
            rep.add("DUPLICATE_CONFLICT", "error",
                    "samma open_time med olika värden i källfilerna (ingen tyst gissning)", n)
            raise DataValidationError(rep, "ingest")
        dropped = len(allf) - len(deduped)
        allf = deduped

    final = check_ohlcv(allf, timeframe, now=now)
    final.raise_if_errors(context="ingest")

    flags, gaps = compute_gap_flags(to_epoch_ns(allf["open_time"]), timeframe)
    if fail_on_gaps and len(gaps):
        rep = ValidationReport(rows=len(allf), gaps=gaps)
        rep.add("GAP", "error", f"{len(gaps)} lucka(or) och fail_on_gaps=True", len(gaps))
        raise DataValidationError(rep, "ingest")

    allf["gap_before"] = flags
    allf = canonicalize(allf)

    extra = {
        "sources": [{"name": s.name, "rows": s.rows, "sha256": s.sha256}
                    for s in sorted(sources, key=lambda s: s.name)],
        "duplicates_dropped": dropped,
        "gaps": [{"gap_start": g.gap_start.isoformat(), "gap_end": g.gap_end.isoformat(),
                  "missing_bars": int(g.missing_bars)} for g in gaps.itertuples()],
        "zero_volume_bars": int((allf["volume"] == 0).sum()),
    }
    manifest = store.write(key, allf, extra)
    return IngestResult(key, manifest, final, dropped)
