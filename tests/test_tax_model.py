"""Etapp B steg 5: skattemodellen (SB-S5-d, SB-S5-g). Handräknade fall ur DESIGN_step5_final.md avsnitt 7;
förväntade värden är exakta Decimal och ändras aldrig för att passa koden."""
import dataclasses
from decimal import Decimal as D

import pytest

from btc_trading_system.tax import model as tm
from btc_trading_system.tax.params import Mode, load_tax_params

P = load_tax_params()
H, C = Mode.HISTORICAL, Mode.CURRENT
BASE, SI, SENS = tm.Scenario.BASE, tm.Scenario.BASE_SI, tm.Scenario.SENSITIVITY


def isk_tax(year, ku, other, mode=H, scenario=BASE, p=P):
    return tm.attributed_tax(year, tm.YearInputs(isk_base=ku, other_isk_kf=other),
                             tm.YearInputs(other_isk_kf=other), p, mode, scenario)


def crypto_tax(year, results, mode=H, scenario=BASE, p=P, other_income=D(0)):
    return tm.attributed_tax(year, tm.YearInputs(crypto_results=tuple(results), other_capital_income=other_income),
                             tm.YearInputs(other_capital_income=other_income), p, mode, scenario)


# ------------------------------------------------------------------ ISK
def test_t1_isk_historical_2023():
    ku = tm.isk_capital_base([D(0), D(52000), D(54000), D(53000)], D(50000))
    assert ku == D(52250)
    assert isk_tax(2023, ku, D(0)) == D("460.845")


def test_t2_deduction_2025_covers_small_account():
    assert isk_tax(2025, D(52250), D(0)) == 0


def test_t3_shared_deduction_2025():
    assert isk_tax(2025, D(52250), D(120000)) == D("197.58")


def test_t4_current_2026():
    assert isk_tax(2023, D(52250), D(400000), mode=C) == D("556.4625")


def test_t5_floor_2021():
    assert isk_tax(2021, D(40000), D(0)) == D("150.00")


def test_t6_account_closed_mid_year():
    ku = tm.isk_capital_base([D(50000), D(51000), D(0), D(0)], D(0))
    assert ku == D(25250)
    assert isk_tax(2025, ku, D(300000)) == D("224.22")


def test_t11_mode_current_2026_on_t1():
    ku = tm.isk_capital_base([D(0), D(52000), D(54000), D(53000)], D(50000))
    assert isk_tax(2023, ku, D(0), mode=C) == 0


def test_t16_kf_base_with_half_premiums():
    u_kf = D(100000) + D("0.5") * D(40000)
    assert u_kf == D(120000)
    rules = P.rules(2025, H)
    assert min(rules.schablon_rate * (D(52250) + u_kf), rules.schablon_rate * rules.deduction_cap_sek) == D(4440)
    assert isk_tax(2025, D(52250), u_kf) == D("197.58")


def test_t21_g1_deduction_equals_allowance_formula_without_other_capital_income():
    for ku, expected in ((D(52250), D(0)), (D(200000), D("444.00"))):
        allowance_formula = D("0.30") * D("0.0296") * max(D(0), ku - D(150000))
        assert isk_tax(2025, ku, D(0)) == expected == allowance_formula


def test_t22_g2_deduction_differs_with_other_capital_deficit():
    with_red = dataclasses.replace(P, deficit_absorbable=True)
    args = (tm.YearInputs(isk_base=D(200000), other_capital_income=D(-150000)),
            tm.YearInputs(other_capital_income=D(-150000)))
    assert tm.attributed_tax(2025, *args, with_red, H, BASE) == D("310.80")
    assert tm.attributed_tax(2025, *args, P, H, BASE) == 0                       # F-9 nej
    assert tm.attributed_tax(2025, *args, P, H, SENS) == D("310.80")
    assert D("0.30") * D("0.0296") * (D(200000) - D(150000)) == D("444.00")      # fribeloppsformeln


def test_isk_capital_base_rejects_bad_input():
    with pytest.raises(tm.TaxModelError):
        tm.isk_capital_base([D(1), D(2), D(3)], D(0))
    with pytest.raises(tm.TaxModelError):
        tm.isk_capital_base([D(1), D(2), D(3), D(-4)], D(0))


# ------------------------------------------------------------------ krypto
def test_t7_average_cost_method_and_no_full_netting():
    book = tm.AverageCostBook()
    book.buy(D("0.5"), D(200000))
    book.buy(D("0.5"), D(300000))
    g1 = book.sell(D("0.4"), D(240000))
    g2 = book.sell(D("0.6"), D(270000))
    assert (g1, g2) == (D(40000), D(-30000)) and book.quantity == 0 and book.cost_sek == 0
    assert tm.crypto_capital_income([g1, g2], P.loss_deductible_share) == D(19000)
    assert crypto_tax(2025, [g1, g2]) == D("5700.00")


def test_t8_deficit_reduction_sensitivity():
    assert crypto_tax(2025, [D(-50000)], scenario=SENS) == D("-10500.00")
    assert crypto_tax(2025, [D(-200000)], scenario=SENS) == D("-38400.00")


def test_t9_no_carry_forward_between_years():
    assert crypto_tax(2024, [D(-50000)]) == 0
    assert crypto_tax(2025, [D(50000)]) == D("15000.00")


def test_t10_sek_conversion_per_transaction():
    book = tm.AverageCostBook()
    book.buy(D(1), D(10000) * D("10.00"))
    assert book.sell(D(1), D(10000) * D("11.00")) == D(10000)


def test_t12_s_i_zero_loss_deduction():
    assert crypto_tax(2025, [D(40000), D(-30000)], scenario=SI) == D("12000.00")


def test_t13_s_i_only_loss():
    assert crypto_tax(2025, [D(-50000)], scenario=SI) == 0


def test_t14_s_ii_no_reduction():
    absorbable = dataclasses.replace(P, deficit_absorbable=True)
    assert crypto_tax(2025, [D(-50000)], p=absorbable) == D("-10500.00")
    assert crypto_tax(2025, [D(-50000)], p=P) == 0                                # basfallet = S-ii


def test_t15_base_f9_no():
    assert crypto_tax(2025, [D(-200000)]) == 0
    assert crypto_tax(2025, [D(-200000)], scenario=SENS) == D("-38400.00")


def test_t17_deduction_never_reduces_a_crypto_gain():
    assert crypto_tax(2023, [D(10000)], mode=C) == D("3000.00")


def test_average_cost_book_rejects_oversell():
    book = tm.AverageCostBook()
    book.buy(D(1), D(100))
    with pytest.raises(tm.TaxModelError):
        book.sell(D(2), D(100))


# ------------------------------------------------------------------ kriteriet
def _row(c, b, f):
    return {"candidate": D(c), "btc": D(b), "fund": D(f)}


def test_t23_criterion_requires_base_and_base_plus_s_i():
    assert tm.passes_g23({BASE: _row(110, 100, 105), SI: _row(106, 100, 105)}) is True
    assert tm.passes_g23({BASE: _row(110, 100, 105), SI: _row(104, 100, 105)}) is False
    assert tm.passes_g23({BASE: _row(105, 100, 105), SI: _row(110, 100, 105)}) is False      # lika räcker inte
    with pytest.raises(tm.TaxModelError):
        tm.passes_g23({BASE: _row(110, 100, 105)})


def test_owner_baseline_uses_f6_and_q_g():
    b = tm.owner_baseline(2025, P)
    assert (b.other_isk_kf, b.other_capital_income, b.isk_base, b.crypto_results) == (0, 0, 0, ())
    assert tm.owner_baseline(2022, P).other_isk_kf == 0                                       # SB-S5-IMPL1
    with pytest.raises(ValueError):
        tm.owner_baseline(2019, P)
