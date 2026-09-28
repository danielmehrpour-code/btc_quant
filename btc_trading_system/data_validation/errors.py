"""Felhierarki för dataflödet. Inget fel får ignoreras tyst."""
from __future__ import annotations

from typing import Any


class DataError(Exception):
    """Basklass för alla dataproblem."""


class MalformedDataError(DataError):
    """Källfilen kan inte tolkas (fel format, trasiga celler, fel tidsstämplar)."""


class TimezoneError(MalformedDataError):
    """Tidsstämpel saknar tidszon eller är inte UTC."""


class DataIntegrityError(DataError):
    """Lagrad data matchar inte sitt manifest (korruption/manipulation)."""


class DatasetNotFoundError(DataError):
    """Begärt dataset finns inte i lagret."""


class DataValidationError(DataError):
    """Validering misslyckades. `.report` innehåller alla issues."""

    def __init__(self, report: Any, context: str = "") -> None:
        self.report = report
        self.context = context
        errs = [i for i in report.issues if i.severity == "error"]
        head = "; ".join(f"[{i.code}] {i.message}" for i in errs[:5])
        super().__init__(f"{context + ': ' if context else ''}{len(errs)} fel: {head}")
