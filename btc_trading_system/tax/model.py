"""Skattemodellen som ren funktion (etapp B steg 5, SB-S5-d, SB-S5-g). Ingen I/O, bara Decimal, ingen avrundning
(avrundning bara i presentationen, F-8).

ISK (IL 42 kap. 36-37 §§): kapitalunderlag KU = (värden vid ingången av varje kvartal + insättningar) / 4;
schablonintäkt = KU x schablonränta. Grundnivån är ett AVDRAG i inkomstslaget kapital (IL 42 kap. 45, 48 och
49 §§): avdrag = min(schablonränta x (KU + övrigt ISK/KF-underlag), schablonränta x takbelopp), ISK först.
Krypto (IL 52 kap. 3 och 5 §§; Skatteverket): genomsnittsmetoden; kapitalinkomst = summa vinster - andel x summa
förluster (ingen full kvittning). Skatt 30 % av ett överskott (IL 65 kap. 7 §); ett underskott ger reduktion
30 % upp till 100 000 kr och 21 % därutöver (IL 67 kap. 10 §), bara om den kan avräknas (F-9); inget förs vidare.

Ett alternativs skatt är marginell: personens nettoskatt med alternativets konton minus utan (SB-S5-d).
Kriteriet (SB-S5-g, Q-H, Q-I): en kandidat ska slå båda benchmarkerna i basfallet och i basfallet + S-i.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from btc_trading_system.tax.params import Mode, TaxParams

ZERO = Decimal(0)


class TaxModelError(ValueError):
    """Ogiltig indata till skattemodellen."""


class Scenario(StrEnum):
    """BASE = ägarens svar (F-9: ingen reduktion = S-ii); BASE_SI = BASE + förlustavdrag 0 % (S-i);
    SENSITIVITY = med reduktion (redovisas, avgör inte)."""
    BASE = "base"
    BASE_SI = "base_plus_s_i"
    SENSITIVITY = "sensitivity_reduction"


REQUIRED_SCENARIOS = (Scenario.BASE, Scenario.BASE_SI)


def isk_capital_base(quarter_values: Sequence[Decimal], deposits: Decimal) -> Decimal:
    """KU = (V(1/1) + V(1/4) + V(1/7) + V(1/10) + insättningar) / 4. Ett avslutat konto har värdet 0."""
    if len(quarter_values) != 4:
        raise TaxModelError("isk_capital_base: exakt fyra kvartalsvärden")
    if any(v < 0 for v in quarter_values) or deposits < 0:
        raise TaxModelError("isk_capital_base: värden och insättningar måste vara >= 0")
    return (sum(quarter_values, ZERO) + deposits) / 4


@dataclass
class AverageCostBook:
    """Genomsnittsmetoden för en tillgång (IL 52 kap. 3 §, 48 kap. 7 §). Belopp i SEK, avgifter inräknade."""
    quantity: Decimal = ZERO
    cost_sek: Decimal = ZERO

    def buy(self, quantity: Decimal, cost_sek: Decimal) -> None:
        if quantity <= 0 or cost_sek < 0:
            raise TaxModelError("buy: kvantitet > 0 och kostnad >= 0")
        self.quantity += quantity
        self.cost_sek += cost_sek

    def sell(self, quantity: Decimal, proceeds_sek: Decimal) -> Decimal:
        """Avyttring; returnerar vinst (> 0) eller förlust (< 0) i SEK."""
        if quantity <= 0 or quantity > self.quantity or proceeds_sek < 0:
            raise TaxModelError("sell: 0 < kvantitet <= innehav och ersättning >= 0")
        basis = self.cost_sek * quantity / self.quantity
        self.cost_sek -= basis
        self.quantity -= quantity
        if self.quantity == 0:
            self.cost_sek = ZERO
        return proceeds_sek - basis


def crypto_capital_income(results: Sequence[Decimal], loss_share: Decimal) -> Decimal:
    """Summa vinster minus andel x summa förluster; ingen full kvittning (K4 avsnitt D)."""
    gains = sum((x for x in results if x > 0), ZERO)
    losses = sum((-x for x in results if x < 0), ZERO)
    return gains - loss_share * losses


def deficit_reduction(deficit: Decimal, p: TaxParams) -> Decimal:
    if deficit < 0:
        raise TaxModelError("deficit_reduction: underskottet anges som ett belopp >= 0")
    low = min(deficit, p.deficit_threshold_sek)
    return p.deficit_reduction_low * low + p.deficit_reduction_high * max(ZERO, deficit - p.deficit_threshold_sek)


@dataclass(frozen=True)
class YearInputs:
    """Personens kapitalposter ett inkomstår (allt i SEK). isk_base: modellens ISK-underlag (0 om inget ISK);
    other_isk_kf: övrigt ISK-underlag + KF/PEPP-underlag enligt IL 42 kap. 49 §; crypto_results: varje
    avyttrings vinst/förlust; other_capital_income: övrig kapitalinkomst (Q-G)."""
    isk_base: Decimal = ZERO
    other_isk_kf: Decimal = ZERO
    crypto_results: tuple[Decimal, ...] = ()
    other_capital_income: Decimal = ZERO


def person_net_tax(income_year: int, inputs: YearInputs, p: TaxParams, mode: Mode, scenario: Scenario) -> Decimal:
    """Personens nettoskatt i inkomstslaget kapital: > 0 skatt, < 0 skattereduktion, 0 annars."""
    if inputs.isk_base < 0 or inputs.other_isk_kf < 0:
        raise TaxModelError("person_net_tax: ISK/KF-underlag måste vara >= 0")
    rules = p.rules(income_year, mode)
    rate = rules.schablon_rate
    schablon = rate * (inputs.isk_base + inputs.other_isk_kf)
    deduction = min(schablon, rate * rules.deduction_cap_sek)
    loss_share = ZERO if scenario is Scenario.BASE_SI else p.loss_deductible_share
    k = schablon - deduction + crypto_capital_income(inputs.crypto_results, loss_share) \
        + inputs.other_capital_income
    if k >= 0:
        return p.capital_tax * k
    absorbable = True if scenario is Scenario.SENSITIVITY else p.deficit_absorbable
    return -deficit_reduction(-k, p) if absorbable else ZERO


def attributed_tax(income_year: int, with_alt: YearInputs, without_alt: YearInputs, p: TaxParams, mode: Mode,
                   scenario: Scenario) -> Decimal:
    """Alternativets marginella skatt: nettoskatt med alternativets konton minus utan."""
    return person_net_tax(income_year, with_alt, p, mode, scenario) - \
        person_net_tax(income_year, without_alt, p, mode, scenario)


def owner_baseline(income_year: int, p: TaxParams) -> YearInputs:
    """Personens poster utan något alternativ: ägarens övriga ISK/KF-underlag och övriga kapitalinkomst."""
    return YearInputs(other_isk_kf=p.other_capital(income_year), other_capital_income=p.other_capital_income_sek)


def passes_g23(net: Mapping[Scenario, Mapping[str, Decimal]]) -> bool:
    """SB-S5-g: kandidaten ('candidate') ska slå både 'btc' och 'fund' i varje krävt scenario."""
    for s in REQUIRED_SCENARIOS:
        if s not in net or not {"candidate", "btc", "fund"} <= set(net[s]):
            raise TaxModelError(f"passes_g23: scenariot {s} saknar candidate/btc/fund")
        row = net[s]
        if not (row["candidate"] > row["btc"] and row["candidate"] > row["fund"]):
            return False
    return True
