"""Deterministisk datasetlagring + manifest + integritetskontroll.

Layout:  <root>/<exchange>/<instrument>/<SYMBOL>/<timeframe>.<ext>
                                                  <timeframe>.manifest.json
DETERMINISM
  * `data_version` = sha256 över kanonisk bytekodning av innehållet (kolumnordning,
    dtype och radordning fixerade). Oberoende av pyarrow-version.
  * Manifest: sorterade nycklar, ingen väggklocka.
  * ParquetBackend: fast schema utan pandas-metadata, fast kompression/version/
    radgrupper. Byte-identiska filer för samma innehåll och pyarrow-version
    (pyarrow skriver in sin egen versionssträng i filens created_by).
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import pandas as pd

from btc_trading_system.data.schema import (CANONICAL_COLUMNS, FLOAT_COLUMNS, SCHEMA_VERSION,
                                            DatasetKey, to_epoch_ms)
from btc_trading_system.data_validation.errors import (DataIntegrityError,
                                                       DatasetNotFoundError)
from btc_trading_system.data_validation.ohlcv import check_ohlcv


def canonicalize(df: pd.DataFrame) -> pd.DataFrame:
    """Fixerar kolumnordning, dtypes och index. Ändrar inte värden."""
    out = df[list(CANONICAL_COLUMNS)].copy().reset_index(drop=True)
    out["open_time"] = out["open_time"].dt.as_unit("ns")
    for c in FLOAT_COLUMNS:
        out[c] = out[c].astype("float64")
    out["trade_count"] = out["trade_count"].astype("int64")
    out["gap_before"] = out["gap_before"].astype(bool)
    return out


def content_hash(df: pd.DataFrame) -> str:
    h = hashlib.sha256()
    h.update(b"btc-ohlcv-v%d" % SCHEMA_VERSION)
    h.update(len(df).to_bytes(8, "little"))
    for c in CANONICAL_COLUMNS:
        h.update(c.encode() + b"\x00")
        if c == "open_time":
            arr = to_epoch_ms(df[c]).astype("<i8")
        elif c in FLOAT_COLUMNS:
            arr = df[c].to_numpy(dtype="<f8")
        elif c == "trade_count":
            arr = df[c].to_numpy(dtype="<i8")
        else:
            arr = df[c].to_numpy(dtype="uint8")
        h.update(np.ascontiguousarray(arr).tobytes())
    return h.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


class StorageBackend(Protocol):
    extension: str
    def write(self, path: Path, df: pd.DataFrame) -> None: ...
    def read(self, path: Path) -> pd.DataFrame: ...


class ParquetBackend:
    """Officiell lagring. Kräver pyarrow."""
    extension = ".parquet"

    @staticmethod
    def _pa():
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as e:  # pragma: no cover
            raise ImportError("ParquetBackend kräver pyarrow: pip install pyarrow") from e
        return pa, pq

    def write(self, path: Path, df: pd.DataFrame) -> None:
        pa, pq = self._pa()
        ts_type = pa.timestamp("ms", tz="UTC")
        fields, arrays = [], []
        for c in CANONICAL_COLUMNS:
            if c == "open_time":
                fields.append(pa.field(c, ts_type, nullable=False))
                arrays.append(pa.array(to_epoch_ms(df[c]), type=pa.int64()).cast(ts_type))
            elif c in FLOAT_COLUMNS:
                fields.append(pa.field(c, pa.float64(), nullable=False))
                arrays.append(pa.array(df[c].to_numpy(dtype="float64")))
            elif c == "trade_count":
                fields.append(pa.field(c, pa.int64(), nullable=False))
                arrays.append(pa.array(df[c].to_numpy(dtype="int64")))
            else:
                fields.append(pa.field(c, pa.bool_(), nullable=False))
                arrays.append(pa.array(df[c].to_numpy(dtype=bool)))
        table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))   # ingen pandas-metadata
        tmp = path.with_name(path.name + ".tmp")
        pq.write_table(table, str(tmp), compression="snappy", use_dictionary=False,
                       write_statistics=True, version="2.6", data_page_version="1.0",
                       row_group_size=131072)
        os.replace(tmp, path)

    def read(self, path: Path) -> pd.DataFrame:
        pa, pq = self._pa()
        t = pq.read_table(str(path))
        missing = [c for c in CANONICAL_COLUMNS if c not in t.column_names]
        if missing:
            raise DataIntegrityError(f"{path.name}: saknar kolumner {missing}")
        data: dict[str, Any] = {}
        ms = t["open_time"].cast(pa.int64()).to_numpy()
        data["open_time"] = pd.Series(pd.to_datetime(ms, unit="ms", utc=True)).dt.as_unit("ns")
        for c in CANONICAL_COLUMNS[1:]:
            data[c] = t[c].to_numpy()
        return pd.DataFrame(data)


class DatasetStore:
    def __init__(self, root: str | Path, backend: StorageBackend | None = None) -> None:
        self.root = Path(root)
        self.backend = backend if backend is not None else ParquetBackend()

    def data_path(self, key: DatasetKey) -> Path:
        return self.root / f"{key.relpath}{self.backend.extension}"

    def manifest_path(self, key: DatasetKey) -> Path:
        return self.root / f"{key.relpath}.manifest.json"

    def exists(self, key: DatasetKey) -> bool:
        return self.data_path(key).exists() and self.manifest_path(key).exists()

    def write(self, key: DatasetKey, df: pd.DataFrame, extra: dict[str, Any]) -> dict[str, Any]:
        df = canonicalize(df)
        rep = check_ohlcv(df, key.timeframe)
        rep.raise_if_errors(context="store.write")
        path = self.data_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "exchange": key.exchange, "instrument": key.instrument.value,
            "symbol": key.symbol, "timeframe": key.timeframe,
            "rows": len(df),
            "first_open_time": df["open_time"].iloc[0].isoformat(),
            "last_open_time": df["open_time"].iloc[-1].isoformat(),
            "data_version": content_hash(df),
            "gap_count": len(rep.gaps),
            "missing_bars_total": int(rep.gaps["missing_bars"].sum()),
            **extra,
        }
        self.backend.write(path, df)                       # 1) data
        atomic_write_bytes(self.manifest_path(key),        # 2) manifest sist
                           (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode())
        return manifest

    def read_manifest(self, key: DatasetKey) -> dict[str, Any]:
        mp = self.manifest_path(key)
        if not mp.exists():
            raise DatasetNotFoundError(f"inget manifest för {key.relpath}")
        return json.loads(mp.read_text())

    def read(self, key: DatasetKey) -> tuple[pd.DataFrame, dict[str, Any]]:
        if not self.exists(key):
            raise DatasetNotFoundError(f"dataset saknas: {key.relpath} (ingest först)")
        manifest = self.read_manifest(key)
        df = canonicalize(self.backend.read(self.data_path(key)))
        if content_hash(df) != manifest["data_version"]:
            raise DataIntegrityError(f"{key.relpath}: innehållet matchar inte manifestets data_version")
        if len(df) != manifest["rows"]:
            raise DataIntegrityError(f"{key.relpath}: radantal matchar inte manifest")
        check_ohlcv(df, key.timeframe).raise_if_errors(context=f"load {key.relpath}")
        return df, manifest
