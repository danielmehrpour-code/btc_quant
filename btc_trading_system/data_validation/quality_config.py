"""Phase 3: låst datakvalitetskonfiguration och gemensamma resultattyper.

Konfigurationen läses strikt: saknade eller okända nycklar, fel typer och värden
utanför tillåtna intervall är fel. Filens SHA-256 låses av ett test; varje rapport
bär konfigurationens kanoniska hash (sorterade nycklar, JSON utan mellanslag).
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from btc_trading_system.core.types import TIMEFRAMES

CONFIG_VERSION = 1
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "data_quality.yaml"
MAX_EXAMPLES = 10


class QualityConfigError(ValueError):
    """Ogiltig datakvalitetskonfiguration."""


class Status(StrEnum):
    """Status för en enskild kontroll. UNKNOWN = otillräckligt underlag (aldrig PASS)."""

    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"


class Severity(StrEnum):
    """Allvarlighetsgrad för ett fynd. INFO påverkar aldrig status."""

    FAIL = "FAIL"
    WARN = "WARN"
    INFO = "INFO"


@dataclass(frozen=True)
class Finding:
    code: str
    severity: Severity
    count: int
    message: str
    examples: tuple[str, ...] = ()


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: Status
    causal: bool
    findings: tuple[Finding, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)
    reason: str | None = None


def overall_status(statuses: Iterable[Status]) -> str:
    """FAIL > INCOMPLETE (någon UNKNOWN) > WARN > PASS."""
    found = set(statuses)
    if not found:
        raise ValueError("overall_status kräver minst en kontroll")
    if Status.FAIL in found:
        return "FAIL"
    if Status.UNKNOWN in found:
        return "INCOMPLETE"
    if Status.WARN in found:
        return "WARN"
    return "PASS"


# ---------------------------------------------------------------- hjälpfunktioner
def index_ns(index: pd.DatetimeIndex) -> np.ndarray:
    """tz-aware UTC DatetimeIndex -> int64 nanosekunder."""
    naive = index.tz_convert("UTC").tz_localize(None).as_unit("ns")
    return np.asarray(naive.values).astype(np.int64)


def ensure_time_index(bars: pd.DataFrame) -> pd.DataFrame:
    """Accepterar provider-format (index open_time) eller lagerformat (kolumn open_time)."""
    out = bars.set_index("open_time") if "open_time" in bars.columns else bars
    if not isinstance(out.index, pd.DatetimeIndex) or out.index.tz is None:
        raise ValueError("barer måste ha tz-aware DatetimeIndex (open_time, UTC)")
    if str(out.index.tz) != "UTC":
        raise ValueError(f"open_time måste vara UTC, fick {out.index.tz}")
    return out


def require_sorted_unique(bars: pd.DataFrame, name: str) -> None:
    if not bars.index.is_monotonic_increasing or not bars.index.is_unique:
        raise ValueError(f"{name}: open_time måste vara strikt stigande och unik")


def require_columns(bars: pd.DataFrame, columns: Iterable[str], name: str) -> None:
    missing = [c for c in columns if c not in bars.columns]
    if missing:
        raise ValueError(f"{name}: saknar kolumner {missing}")


def examples_from_mask(index: pd.DatetimeIndex, mask: np.ndarray) -> tuple[str, ...]:
    positions = np.flatnonzero(mask)[:MAX_EXAMPLES]
    return tuple(index[int(p)].isoformat() for p in positions)


def canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


# ---------------------------------------------------------------- konfiguration
@dataclass(frozen=True)
class InvariantsConfig:
    vwap_rel_tol: float
    taker_abs_tol: float
    taker_rel_tol: float


@dataclass(frozen=True)
class AnomaliesConfig:
    window_min_days: int
    window_min_bars: int
    min_obs_fraction: float
    return_z_threshold: float
    return_scale_floor: float
    range_ratio_threshold: float
    range_floor: float
    volume_ratio_threshold: float
    stale_min_minutes: int
    stale_min_bars: int


@dataclass(frozen=True)
class ConsistencyConfig:
    agg_abs_tol: float
    agg_rel_tol: float
    basis_warn_bps: float
    basis_fail_bps: float
    lag_max_bars: int
    lag_min_pairs: int
    lag_min_corr0: float


@dataclass(frozen=True)
class ReplayConfig:
    max_timeframe: str


@dataclass(frozen=True)
class QualityConfig:
    version: int
    invariants: InvariantsConfig
    anomalies: AnomaliesConfig
    consistency: ConsistencyConfig
    replay: ReplayConfig
    canonical_sha256: str
    file_sha256: str | None = None


def _tol(v: Any) -> bool:
    return 0.0 < v < 1e-3


def _pos(v: Any) -> bool:
    return v > 0


def _above_one(v: Any) -> bool:
    return v > 1


_SPEC: dict[str, dict[str, tuple[type, Callable[[Any], bool], str]]] = {
    "invariants": {
        "vwap_rel_tol": (float, _tol, "0 < x < 1e-3"),
        "taker_abs_tol": (float, _tol, "0 < x < 1e-3"),
        "taker_rel_tol": (float, _tol, "0 < x < 1e-3"),
    },
    "anomalies": {
        "window_min_days": (int, lambda v: v >= 1, ">= 1"),
        "window_min_bars": (int, lambda v: v >= 10, ">= 10"),
        "min_obs_fraction": (float, lambda v: 0 < v <= 1, "0 < x <= 1"),
        "return_z_threshold": (float, _pos, "> 0"),
        "return_scale_floor": (float, _pos, "> 0"),
        "range_ratio_threshold": (float, _above_one, "> 1"),
        "range_floor": (float, _pos, "> 0"),
        "volume_ratio_threshold": (float, _above_one, "> 1"),
        "stale_min_minutes": (int, lambda v: v >= 1, ">= 1"),
        "stale_min_bars": (int, lambda v: v >= 2, ">= 2"),
    },
    "consistency": {
        "agg_abs_tol": (float, _tol, "0 < x < 1e-3"),
        "agg_rel_tol": (float, _tol, "0 < x < 1e-3"),
        "basis_warn_bps": (float, _pos, "> 0"),
        "basis_fail_bps": (float, _pos, "> 0"),
        "lag_max_bars": (int, lambda v: 1 <= v <= 10, "1..10"),
        "lag_min_pairs": (int, lambda v: v >= 30, ">= 30"),
        "lag_min_corr0": (float, lambda v: 0 < v < 1, "0 < x < 1"),
    },
    "replay": {
        "max_timeframe": (str, lambda v: v in TIMEFRAMES, f"en av {list(TIMEFRAMES)}"),
    },
}


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    sub = data.get(name)
    if not isinstance(sub, dict):
        raise QualityConfigError(f"sektionen {name!r} saknas eller är inte en mapping")
    spec = _SPEC[name]
    unknown = sorted(set(sub) - set(spec))
    missing = sorted(set(spec) - set(sub))
    if unknown:
        raise QualityConfigError(f"okända nycklar i {name}: {unknown}")
    if missing:
        raise QualityConfigError(f"saknade nycklar i {name}: {missing}")
    for key, (kind, ok, rule) in spec.items():
        value = sub[key]
        if type(value) is not kind:        # strikt: bool/int godtas inte som float
            raise QualityConfigError(
                f"{name}.{key} måste vara {kind.__name__}, fick {type(value).__name__} {value!r}"
            )
        if not ok(value):
            raise QualityConfigError(
                f"{name}.{key}={value!r} utanför tillåtet intervall ({rule})"
            )
    return sub


def parse_quality_config(data: Any, file_sha256: str | None = None) -> QualityConfig:
    if not isinstance(data, dict):
        raise QualityConfigError("konfigurationen måste vara en mapping")
    unknown = sorted(set(data) - ({"version"} | set(_SPEC)))
    if unknown:
        raise QualityConfigError(f"okända toppnivånycklar: {unknown}")
    version = data.get("version")
    if type(version) is not int or version != CONFIG_VERSION:
        raise QualityConfigError(f"version måste vara {CONFIG_VERSION}, fick {version!r}")
    inv = _section(data, "invariants")
    ano = _section(data, "anomalies")
    con = _section(data, "consistency")
    rep = _section(data, "replay")
    if not con["basis_warn_bps"] < con["basis_fail_bps"]:
        raise QualityConfigError("consistency.basis_warn_bps måste vara < basis_fail_bps")
    return QualityConfig(
        version=version,
        invariants=InvariantsConfig(**inv),
        anomalies=AnomaliesConfig(**ano),
        consistency=ConsistencyConfig(**con),
        replay=ReplayConfig(**rep),
        canonical_sha256=hashlib.sha256(canonical_json(data)).hexdigest(),
        file_sha256=file_sha256,
    )


def load_quality_config(path: str | Path | None = None) -> QualityConfig:
    p = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    raw = p.read_bytes()
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise QualityConfigError(f"{p}: ogiltig YAML: {e}") from e
    return parse_quality_config(data, file_sha256=hashlib.sha256(raw).hexdigest())
