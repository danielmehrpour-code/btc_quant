# Review log

## Review 1 — efter PHASE 2 (2026-09-28)

Defekter hittade och rättade (regressionstester: `tests/test_review_regressions.py`,
16 av 19 fallerar mot koden före rättning; 3 är skyddstester för legitim input):

| ID | Allvar | Defekt | Rättning |
|---|---|---|---|
| R1 | Hög | Binance µs-filer (close_time = open + tf − 1 µs) avvisades. Testhjälparen maskerade felet. | close_time får sub-ms; accepterar −1 ms och −1 µs |
| R6 | Hög | Samma bar i ms- och µs-fil hade gett DUPLICATE_CONFLICT | close_time tas bort före dedup |
| R7 | Hög | `trading.live: 'false'` (sträng) är truthy → första live-spärren passerade | strikt bool; `is not True` i spärren |
| R2 | Medel | Fel timeframe utan close_time accepterades som "luckor" | TIMEFRAME_MISMATCH (< 50 % intilliggande) |
| R3 | Medel | ETHUSDT-fil kunde ingestas som BTCUSDT | Binance-filnamn kontrolleras |
| R5 | Medel | Provider-cache såg inte re-ingest | cache nycklas på data_version |
| R7b | Medel | `data_split` och okända config-nycklar ignorerades tyst; tvetydiga datum | strikt parsing, exklusiva UTC-gränser |
| R4 | Låg | Dubblerade kolumner efter alias gav TypeError | tydligt MalformedDataError |
| R8 | Låg | assert_causal gav falskt godkänt vid < 2 rader; kollade inte sista raden | min 8 rader, cut vid n−1 |
| R9 | Låg | pyproject saknade build-system och pytest pythonpath | tillagt |

Öppna designfrågor inför PHASE 6 (ej åtgärdade, kräver beslut): se chattsvar / nedan.
1. Fill-tidpunkt vs `validate_causality` (fill strikt efter order) — next-bar-open är nolllatens.
2. Interfaces saknar pris/equity/tid i RiskEngine.check, delfills, exit-semantik, målposition.
3. `paper_validated` är en flagga från anroparen, inte en verifierad kontroll.
4. Ingen outlier-/stale-price-detektion (PHASE 3).
