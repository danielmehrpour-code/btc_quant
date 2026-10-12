"""Etapp B steg 5: parameterfilen för skattemodellen (SB-S5-d, SB-S5-e) och T18."""
import copy
import hashlib
from decimal import Decimal as D

import pytest
import yaml

from btc_trading_system.tax import params as tp

LOCKED_FILE_SHA256 = "631d19a703f76052872ba9ee52252669c8acd9b5bd399846bd853fc323bb330e"
LOCKED_CANONICAL_SHA256 = "33cdbecaaef6ba66ec5673f29d2b593bb5e3e02d12f3ef0e2101adb682383aef"
P = tp.load_tax_params()
RAW = yaml.safe_load(tp.DEFAULT_TAX_PARAMS_PATH.read_bytes())


def test_file_is_locked():
    raw = tp.DEFAULT_TAX_PARAMS_PATH.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == LOCKED_FILE_SHA256
    assert P.file_sha256 == LOCKED_FILE_SHA256 and P.canonical_sha256 == LOCKED_CANONICAL_SHA256


def test_t18_schablon_rates_follow_the_rule_and_the_table():
    # Riksgälden 30 nov 2019..2025 och Skatteverkets tabell (läst 2026-10-12): max(SLR + 1 pe, 1,25 %)
    expected = {2020: ("-0.0009", "0.0125"), 2021: ("-0.0010", "0.0125"), 2022: ("0.0023", "0.0125"),
                2023: ("0.0194", "0.0294"), 2024: ("0.0262", "0.0362"), 2025: ("0.0196", "0.0296"),
                2026: ("0.0255", "0.0355")}
    for y, (slr, rate) in expected.items():
        s, r, _ = P.years[y]
        assert (s, r) == (D(slr), D(rate))
        assert r == max(s + D("0.01"), D("0.0125"))
    assert {y: P.years[y][2] for y in P.years} == {2020: 0, 2021: 0, 2022: 0, 2023: 0, 2024: 0,
                                                   2025: D(150000), 2026: D(300000)}


def test_rates_and_owner_values():
    assert (P.capital_tax, P.loss_deductible_share, P.deficit_reduction_low, P.deficit_reduction_high,
            P.deficit_threshold_sek) == (D("0.30"), D("0.70"), D("0.30"), D("0.21"), D(100000))
    assert all(P.other_capital(y) == 0 for y in range(2020, 2027))                           # F-6, SB-S5-IMPL1
    assert P.other_capital_income_sek == 0 and P.deficit_absorbable is False                  # Q-G, F-9
    missing = tp.parse_tax_params(_mut(lambda d: d["owner"]["other_isk_kf_capital_sek"].update({2022: None})))
    with pytest.raises(tp.TaxParamsError, match="F-6"):                                        # null -> fel
        missing.other_capital(2022)


def test_modes():
    assert P.rules(2021, tp.Mode.HISTORICAL).schablon_rate == D("0.0125")
    cur = P.rules(2021, tp.Mode.CURRENT)
    assert (cur.rules_year, cur.schablon_rate, cur.deduction_cap_sek) == (2026, D("0.0355"), D(300000))
    with pytest.raises(tp.TaxParamsError):
        P.rules(2019, tp.Mode.HISTORICAL)
    with pytest.raises(ValueError):
        P.rules(2024, "other")


def _mut(fn):
    d = copy.deepcopy(RAW)
    fn(d)
    return d


@pytest.mark.parametrize("change", [
    lambda d: d.pop("owner"),
    lambda d: d.update(extra=1),
    lambda d: d.update(version=2),
    lambda d: d.update(version=True),
    lambda d: d["rates"]["capital_tax"].update(value=0.30),                 # float, inte sträng
    lambda d: d["rates"]["capital_tax"].update(value="abc"),
    lambda d: d["rates"]["capital_tax"].update(value="1.5"),
    lambda d: d["rates"]["capital_tax"].update(basis=""),
    lambda d: d["rates"]["capital_tax"].update(verified="2026"),
    lambda d: d["rates"].pop("isk_floor"),
    lambda d: d["income_years"][2023].update(schablon_rate="0.0300"),       # bryter IL 42 kap. 36 §
    lambda d: d["income_years"][2021].update(schablon_rate="0.0090"),       # under golvet
    lambda d: d["income_years"].pop(2022),                                  # inte sammanhängande
    lambda d: d["income_years"][2024].update(extra="1"),
    lambda d: d.update(current_rules_year=2027),
    lambda d: d["owner"]["other_isk_kf_capital_sek"].pop(2024),
    lambda d: d["owner"]["other_isk_kf_capital_sek"].update({2024: "-1"}),
    lambda d: d["owner"].update(deficit_absorbable="no"),
    lambda d: d["owner"].update(deficit_carry_forward=True),
])
def test_parsed_strictly(change):
    with pytest.raises(tp.TaxParamsError):
        tp.parse_tax_params(_mut(change))


def test_invalid_yaml(tmp_path):
    p = tmp_path / "t.yaml"
    p.write_text("version: [1\n")
    with pytest.raises(tp.TaxParamsError, match="YAML"):
        tp.load_tax_params(p)
