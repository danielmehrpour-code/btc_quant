"""Parametrar för skattemodellen (etapp B steg 5, SB-S5-d, SB-S5-e): configs/tax_params_v1.yaml.

Läses strikt: exakt de nycklar som filen ska ha, alla belopp och satser som strängar som blir Decimal (aldrig
float), varje värde med lagrum eller verifieringsdatum. Schablonräntan kontrolleras mot regeln i IL 42 kap. 36 §:
schablon_rate == max(slr_30nov_prev + isk_spread_over_slr, isk_floor). Ett inkomstår eller ett ägarvärde som saknas
ger fel, aldrig ett standardvärde. Filens och den kanoniska sha256 låses av ett test (som P3-01).
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

from btc_trading_system.data_validation.quality_config import canonical_json

TAX_PARAMS_VERSION = 1
DEFAULT_TAX_PARAMS_PATH = Path(__file__).resolve().parents[2] / "configs" / "tax_params_v1.yaml"
RATE_KEYS = ("capital_tax", "isk_floor", "isk_spread_over_slr", "loss_deductible_share",
             "deficit_reduction_low", "deficit_reduction_high", "deficit_threshold_sek")
YEAR_KEYS = {"slr_30nov_prev", "schablon_rate", "deduction_cap_sek", "verified"}
OWNER_KEYS = {"other_isk_kf_capital_sek", "other_capital_income_sek", "deficit_absorbable", "deficit_carry_forward"}


class TaxParamsError(ValueError):
    """Parameterfilen eller en förfrågan mot den är ogiltig."""


class Mode(StrEnum):
    """SB-S5-e: current_2026 (primärt) använder current_rules_year:s regler för varje inkomstår."""
    CURRENT = "current_2026"
    HISTORICAL = "historical"


@dataclass(frozen=True)
class YearRules:
    income_year: int
    rules_year: int
    schablon_rate: Decimal
    deduction_cap_sek: Decimal


@dataclass(frozen=True)
class TaxParams:
    capital_tax: Decimal
    isk_floor: Decimal
    isk_spread_over_slr: Decimal
    loss_deductible_share: Decimal
    deficit_reduction_low: Decimal
    deficit_reduction_high: Decimal
    deficit_threshold_sek: Decimal
    years: dict[int, tuple[Decimal, Decimal, Decimal]]       # år -> (slr, schablon_rate, deduction_cap_sek)
    current_rules_year: int
    other_isk_kf_capital_sek: dict[int, Decimal | None]
    other_capital_income_sek: Decimal
    deficit_absorbable: bool
    deficit_carry_forward: bool
    canonical_sha256: str
    file_sha256: str | None = None

    def rules(self, income_year: int, mode: Mode) -> YearRules:
        if income_year not in self.years:
            raise TaxParamsError(f"inkomståret {income_year} saknas i parameterfilen")
        rules_year = self.current_rules_year if Mode(mode) is Mode.CURRENT else income_year
        _, rate, cap = self.years[rules_year]
        return YearRules(income_year, rules_year, rate, cap)

    def other_capital(self, income_year: int) -> Decimal:
        """Ägarens övriga ISK/KF/PEPP-underlag för inkomståret (F-6); saknas -> fel."""
        v = self.other_isk_kf_capital_sek.get(income_year)
        if v is None:
            raise TaxParamsError(f"other_isk_kf_capital_sek saknas för {income_year} (ägarvärde, F-6)")
        return v


def _dec(v: Any, name: str, *, nonneg: bool = True) -> Decimal:
    if not isinstance(v, str):
        raise TaxParamsError(f"{name}: måste vara en sträng med ett decimaltal, fick {v!r}")
    try:
        d = Decimal(v)
    except InvalidOperation as e:
        raise TaxParamsError(f"{name}: ogiltigt decimaltal {v!r}") from e
    if not d.is_finite() or (nonneg and d < 0):
        raise TaxParamsError(f"{name}: måste vara ändligt{' och >= 0' if nonneg else ''}, fick {v!r}")
    return d


def _date_str(v: Any, name: str) -> None:
    if not (isinstance(v, str) and len(v) == 10 and v[4] == "-" and v[7] == "-"):
        raise TaxParamsError(f"{name}: verified måste vara ett datum YYYY-MM-DD, fick {v!r}")


def parse_tax_params(data: Any, file_sha256: str | None = None) -> TaxParams:
    if not isinstance(data, dict) or set(data) != {"version", "rates", "income_years", "current_rules_year", "owner"}:
        raise TaxParamsError("tax_params: nycklarna måste vara exakt version, rates, income_years, current_rules_year, owner")
    if data["version"] != TAX_PARAMS_VERSION or isinstance(data["version"], bool):
        raise TaxParamsError(f"tax_params: version måste vara {TAX_PARAMS_VERSION}")
    rates = data["rates"]
    if not isinstance(rates, dict) or set(rates) != set(RATE_KEYS):
        raise TaxParamsError(f"tax_params.rates: nycklarna måste vara exakt {RATE_KEYS}")
    r: dict[str, Decimal] = {}
    for k in RATE_KEYS:
        e = rates[k]
        if not isinstance(e, dict) or set(e) != {"value", "basis", "verified"} or not isinstance(e["basis"], str) \
                or not e["basis"]:
            raise TaxParamsError(f"tax_params.rates.{k}: måste vara {{value, basis, verified}} med lagrum")
        _date_str(e["verified"], f"rates.{k}")
        r[k] = _dec(e["value"], f"rates.{k}")
    for k in ("capital_tax", "loss_deductible_share", "deficit_reduction_low", "deficit_reduction_high"):
        if r[k] > 1:
            raise TaxParamsError(f"tax_params.rates.{k}: en andel i [0, 1]")
    years_raw = data["income_years"]
    if not isinstance(years_raw, dict) or not years_raw or not all(type(y) is int for y in years_raw):
        raise TaxParamsError("tax_params.income_years: en icke-tom tabell med heltalsår")
    years: dict[int, tuple[Decimal, Decimal, Decimal]] = {}
    for y, e in sorted(years_raw.items()):
        if not isinstance(e, dict) or set(e) != YEAR_KEYS:
            raise TaxParamsError(f"tax_params.income_years.{y}: nycklarna måste vara exakt {sorted(YEAR_KEYS)}")
        _date_str(e["verified"], f"income_years.{y}")
        slr = _dec(e["slr_30nov_prev"], f"income_years.{y}.slr_30nov_prev", nonneg=False)
        rate = _dec(e["schablon_rate"], f"income_years.{y}.schablon_rate")
        cap = _dec(e["deduction_cap_sek"], f"income_years.{y}.deduction_cap_sek")
        if rate != max(slr + r["isk_spread_over_slr"], r["isk_floor"]):
            raise TaxParamsError(f"tax_params.income_years.{y}: schablon_rate {rate} != max(SLR + spread, golv) "
                                 "(IL 42 kap. 36 §)")
        years[y] = (slr, rate, cap)
    if sorted(years) != list(range(min(years), max(years) + 1)):
        raise TaxParamsError("tax_params.income_years: åren måste vara sammanhängande")
    cur = data["current_rules_year"]
    if type(cur) is not int or cur not in years:
        raise TaxParamsError("tax_params.current_rules_year: ett heltalsår som finns i income_years")
    owner = data["owner"]
    if not isinstance(owner, dict) or set(owner) != OWNER_KEYS:
        raise TaxParamsError(f"tax_params.owner: nycklarna måste vara exakt {sorted(OWNER_KEYS)}")
    oc = owner["other_isk_kf_capital_sek"]
    if not isinstance(oc, dict) or set(oc) != set(years):
        raise TaxParamsError("tax_params.owner.other_isk_kf_capital_sek: ett värde eller null per inkomstår")
    other = {y: (None if v is None else _dec(v, f"owner.other_isk_kf_capital_sek.{y}")) for y, v in oc.items()}
    for k in ("deficit_absorbable", "deficit_carry_forward"):
        if not isinstance(owner[k], bool):
            raise TaxParamsError(f"tax_params.owner.{k}: true eller false")
    if owner["deficit_carry_forward"]:
        raise TaxParamsError("tax_params.owner.deficit_carry_forward: modellen för aldrig underskott vidare")
    return TaxParams(
        capital_tax=r["capital_tax"], isk_floor=r["isk_floor"], isk_spread_over_slr=r["isk_spread_over_slr"],
        loss_deductible_share=r["loss_deductible_share"], deficit_reduction_low=r["deficit_reduction_low"],
        deficit_reduction_high=r["deficit_reduction_high"], deficit_threshold_sek=r["deficit_threshold_sek"],
        years=years, current_rules_year=cur, other_isk_kf_capital_sek=other,
        other_capital_income_sek=_dec(owner["other_capital_income_sek"], "owner.other_capital_income_sek",
                                      nonneg=False),
        deficit_absorbable=owner["deficit_absorbable"], deficit_carry_forward=owner["deficit_carry_forward"],
        canonical_sha256=hashlib.sha256(canonical_json(data)).hexdigest(), file_sha256=file_sha256)


def load_tax_params(path: str | Path | None = None) -> TaxParams:
    raw = Path(path or DEFAULT_TAX_PARAMS_PATH).read_bytes()
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise TaxParamsError(f"tax_params: ogiltig YAML: {e}") from e
    return parse_tax_params(data, file_sha256=hashlib.sha256(raw).hexdigest())
