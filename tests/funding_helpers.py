"""Testhjälp för funding (etapp B steg 4): syntetiska månadsfiler i Binance-arkivets format."""
from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from btc_trading_system.data.funding import SOURCE_HEADER, FundingKey

FKEY = FundingKey("perp", "BTCUSDT")
HOUR_MS = 3_600_000


def month_rows(year: int, month: int, *, days: int | None = None, interval: int = 8, jitter_ms: int = 0,
               rate=lambda i: f"{(i % 7 - 3) * 0.00001:.8f}") -> list[tuple[str, str, str]]:
    """Rader (calc_time, interval, ränta som text) var `interval`:e timme från månadens början."""
    start = int(datetime(year, month, 1, tzinfo=UTC).timestamp()) * 1000
    end = int(datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=UTC).timestamp()) * 1000
    if days is not None:
        end = min(end, start + days * 24 * HOUR_MS)
    out, t, i = [], start, 0
    while t < end:
        out.append((str(t + (jitter_ms if i % 2 else 0)), str(interval), rate(i)))
        t += interval * HOUR_MS
        i += 1
    return out


def zip_bytes(member: str, text: str, extra_members: tuple[tuple[str, str], ...] = ()) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member, text)
        for name, body in extra_members:
            zf.writestr(name, body)
    return buf.getvalue()


def write_month(directory: Path, year: int, month: int, rows: list[tuple[str, str, str]] | None = None, *,
                symbol: str = "BTCUSDT", header: str = SOURCE_HEADER, name: str | None = None,
                newline: str = "\n") -> Path:
    rows = month_rows(year, month) if rows is None else rows
    stem = name[:-4] if name else f"{symbol}-fundingRate-{year:04d}-{month:02d}"
    text = newline.join([header, *(",".join(r) for r in rows)]) + newline
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stem}.zip"
    path.write_bytes(zip_bytes(f"{stem}.csv", text))
    return path
