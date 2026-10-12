"""Pins för kanoniska jämförelsefiler (etapp B steg 5, SB-S5-c): configs/benchmark_pins_v1.yaml.

Varje pin = namn -> (relativ sökväg, sha256). En utvärdering läser en jämförelsefil bara genom `read_pinned`, som
verifierar sha256 innan bytes lämnas ut. Ingen pin, fel sökväg eller annan hash -> fel. Filen låses av ett test.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from btc_trading_system.data_validation.quality_config import canonical_json

PINS_VERSION = 1
DEFAULT_PINS_PATH = Path(__file__).resolve().parents[2] / "configs" / "benchmark_pins_v1.yaml"
_SHA = re.compile(r"[0-9a-f]{64}")
_NAME = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}")


class PinError(ValueError):
    """Pin-filen är ogiltig, eller en fil stämmer inte med sin pin."""


@dataclass(frozen=True)
class Pin:
    path: str
    sha256: str


@dataclass(frozen=True)
class Pins:
    pins: dict[str, Pin]
    canonical_sha256: str
    file_sha256: str | None = None


def parse_pins(data: Any, file_sha256: str | None = None) -> Pins:
    if not isinstance(data, dict) or set(data) != {"version", "pins"}:
        raise PinError("benchmark_pins: nycklarna måste vara exakt version, pins")
    if data["version"] != PINS_VERSION or isinstance(data["version"], bool):
        raise PinError(f"benchmark_pins: version måste vara {PINS_VERSION}")
    raw = data["pins"]
    if not isinstance(raw, dict):
        raise PinError("benchmark_pins.pins: en tabell (får vara tom)")
    pins: dict[str, Pin] = {}
    for name, e in raw.items():
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise PinError(f"benchmark_pins: ogiltigt namn {name!r}")
        if not isinstance(e, dict) or set(e) != {"path", "sha256"}:
            raise PinError(f"benchmark_pins.{name}: måste vara {{path, sha256}}")
        p, h = e["path"], e["sha256"]
        if not isinstance(h, str) or not _SHA.fullmatch(h):
            raise PinError(f"benchmark_pins.{name}.sha256: 64 hex med gemener")
        pp = PurePosixPath(p) if isinstance(p, str) else None
        if pp is None or pp.is_absolute() or ".." in pp.parts or str(pp) != p or not p:
            raise PinError(f"benchmark_pins.{name}.path: relativ sökväg utan '..'")
        pins[name] = Pin(p, h)
    return Pins(pins, hashlib.sha256(canonical_json(data)).hexdigest(), file_sha256)


def load_pins(path: str | Path | None = None) -> Pins:
    raw = Path(path or DEFAULT_PINS_PATH).read_bytes()
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise PinError(f"benchmark_pins: ogiltig YAML: {e}") from e
    return parse_pins(data, file_sha256=hashlib.sha256(raw).hexdigest())


def read_pinned(pins: Pins, name: str, root: str | Path) -> bytes:
    """Bytes för den pinnade filen under root, efter kontroll av sha256."""
    if name not in pins.pins:
        raise PinError(f"ingen pin för {name!r}")
    pin = pins.pins[name]
    data = (Path(root) / pin.path).read_bytes()
    got = hashlib.sha256(data).hexdigest()
    if got != pin.sha256:
        raise PinError(f"{name}: sha256 {got} != pin {pin.sha256}")
    return data
