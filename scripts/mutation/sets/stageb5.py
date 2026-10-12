"""Mutationsmängd etapp B steg 5: skattemodellen och jämförelsedatan i SEK (SB-S5-a..h).

Varje mutation tar bort eller försvagar en regel: golvet i schablonräntan, insättningar i kapitalunderlaget,
ingen full kvittning, takbeloppet för avdraget, marginell tillskrivning, stressfallet S-i, flaggan vid bakåtfyllning,
publiceringsfördröjningen för växelkursen, FINAL TEST-spärren, växlingsavgiften, kontrollen av pins och
allowlisten i hämtningsverktyget.
"""
NAME = "stageb5"
TESTS = ("tests/test_tax_params.py", "tests/test_tax_model.py", "tests/test_benchmark_fx.py",
         "tests/test_benchmark_series.py", "tests/test_benchmark_quality.py", "tests/test_fetch_benchmark.py")
PAR = "btc_trading_system/tax/params.py"
MOD = "btc_trading_system/tax/model.py"
FX = "btc_trading_system/benchmark/fx.py"
SER = "btc_trading_system/benchmark/series.py"
PIN = "btc_trading_system/benchmark/pins.py"
FB = "scripts/data/fetch_benchmark.py"
MUTATIONS = {
    "1 floor removed from the schablon rate rule": [
        (PAR, '        if rate != max(slr + r["isk_spread_over_slr"], r["isk_floor"]):\n',
         '        if rate != slr + r["isk_spread_over_slr"]:\n'),
    ],
    "2 deposits left out of the capital base": [
        (MOD, '    return (sum(quarter_values, ZERO) + deposits) / 4\n', '    return sum(quarter_values, ZERO) / 4\n'),
    ],
    "3 full netting of crypto losses": [
        (MOD, '    return gains - loss_share * losses\n', '    return gains - losses\n'),
    ],
    "4 deduction not capped": [
        (MOD, '    deduction = min(schablon, rate * rules.deduction_cap_sek)\n', '    deduction = schablon\n'),
    ],
    "5 attribution is the tax with the alternative, not the difference": [
        (MOD, ('    return person_net_tax(income_year, with_alt, p, mode, scenario) - \\\n'
               '        person_net_tax(income_year, without_alt, p, mode, scenario)\n'),
         '    return person_net_tax(income_year, with_alt, p, mode, scenario)\n'),
    ],
    "6 stress case S-i does not change the result": [
        (MOD, '    loss_share = ZERO if scenario is Scenario.BASE_SI else p.loss_deductible_share\n',
         '    loss_share = p.loss_deductible_share\n'),
    ],
    "7 backward fill without a flag": [
        (FX, '        rows.append((d, r.value_scaled, r.obs_date, age, age > 0, age > gap_max_days))\n',
         '        rows.append((d, r.value_scaled, r.obs_date, age, False, False))\n'),
    ],
    "8 FX used before its available_at (no publication lag)": [
        (FX, '        local = datetime.combine(d + timedelta(days=self.days_after), self.at, tzinfo=ZoneInfo(self.tz))\n',
         '        local = datetime.combine(d, self.at, tzinfo=ZoneInfo(self.tz))\n'),
    ],
    "9 FINAL TEST block removed": [
        (SER, '    if start < ft.end and ft.start < end and not gate4:\n', '    if False:\n'),
    ],
    "10 FX fee left out of the cost factor": [
        (SER, '        return (1 - m * self.fx_fee_rate) * (1 - m * self.commission_rate) * (1 - m * self.half_spread_rate)\n',
         '        return (1 - m * self.commission_rate) * (1 - m * self.half_spread_rate)\n'),
    ],
    "11 pinned file not verified": [
        (PIN, '    if got != pin.sha256:\n', '    if False:\n'),
    ],
    "12 host allowlist removed from the fetch tool": [
        (FB, '    if u.scheme != "https" or u.hostname not in ALLOWED_HOSTS or u.port not in (None, 443) or u.username:\n',
         '    if u.scheme != "https":\n'),
    ],
}
