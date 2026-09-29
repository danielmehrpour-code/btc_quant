# Phase 3 – Datakvalitet (upptäck och rapportera)

Phase 3 **ändrar aldrig data**. Den läser datalagret från Phase 2 (read-only),
kör kontroller och skriver deterministiska rapporter (JSON + Markdown) till en
katalog som anges med `--out`. Rensning, imputering och uteslutning av data hör
till senare faser och kräver ett eget beslut.

Tröskelvärdena är låsta i `configs/data_quality.yaml`:

| Hash | Värde |
|---|---|
| Filens SHA-256 | `756509ad305bc1c036fc25fce797a9e722700bd3115c142f43a2fdcae1880d2d` |
| Kanonisk SHA-256 (sorterade nycklar, kompakt JSON) | `d1350cde29f02294bb327176fd9ba2c4df4ac5187f72ff54f6c674588319b9a0` |

Båda låses av `tests/quality/test_quality_config.py`. Varje rapport bär den kanoniska
hashen. Tröskelvärden justeras inte efter att riktiga data har analyserats.

## Status

En kontroll får `PASS`, `WARN`, `FAIL` eller `UNKNOWN`. `UNKNOWN` betyder otillräckligt
underlag och räknas **aldrig** som `PASS`. Den samlade statusen är:

`FAIL` (någon FAIL) > `INCOMPLETE` (någon UNKNOWN) > `WARN` > `PASS`

Fynd har allvarlighetsgraden `FAIL`, `WARN` eller `INFO`. `INFO` (t.ex. `TF_INCOMPLETE`)
påverkar aldrig status.

## A. Invarianter per bar (FAIL, kausala)

| Kod | Villkor |
|---|---|
| `VWAP_OUTSIDE_RANGE` | V > 0 och QV/V utanför [L·(1−1e-6), H·(1+1e-6)] |
| `TAKER_BASE_EXCEEDS_VOLUME` | TB − V > 1e-8 + 1e-12·V |
| `TAKER_QUOTE_EXCEEDS_QUOTE` | TQ − QV > 1e-8 + 1e-12·QV |
| `VOLUME_WITHOUT_TRADES` | V > 0 och N = 0 |
| `TRADES_WITHOUT_VOLUME` | N > 0 och V = 0 |
| `PRICE_MOVE_WITHOUT_VOLUME` | V = 0 och H ≠ L |

En platt nollvolymbar (V = 0, O = H = L = C) är inget brott.

## B. Kausala anomaliflaggor (WARN, aldrig FAIL)

Fönster för bar t: tidsintervallet **[t − W, t)** (`closed="left"`). Baren själv
ingår aldrig, och inga senare barer används. W = max(7 dagar, 100·Δ), och
minsta antal giltiga observationer är m = ⌈0,5·W/Δ⌉. Med för få observationer
blir flaggan `<NA>`, aldrig False.

| tf | W | W i barer | m | N_stale |
|---|---|---|---|---|
| 1m | 7 d | 10080 | 5040 | 30 |
| 5m | 7 d | 2016 | 1008 | 6 |
| 15m | 7 d | 672 | 336 | 3 |
| 30m | 7 d | 336 | 168 | 3 |
| 1h | 7 d | 168 | 84 | 3 |
| 4h | 400 h | 100 | 50 | 3 |
| 1d | 100 d | 100 | 50 | 3 |

- `RETURN_OUTLIER`: r_t = ln(C_t / C_{t−1}), NaN efter lucka. Flaggas om
  z = |r − median| / max(IQR/1,349; 1e-5) > 10.
- `RANGE_OUTLIER`: (H−L)/C > 20·max(median; 1e-5).
- `VOLUME_OUTLIER`: V / median > 50. Om medianen är 0 blir flaggan `<NA>`.
- `STALE_RUN`: längden *hittills* av en frusen sekvens (V = 0 och O = H = L = C)
  ≥ N_stale. En lucka startar en ny sekvens.
  Sekvensernas fulla längd visas i `runs_examples_audit`, som är en icke-kausal
  granskning i efterhand.

Kausaliteten verifieras på tre sätt:

1. `assert_causal` med täta klippunkter.
2. End-to-end via `provider.get_bars(as_of=t)`.
3. En oberoende referensimplementation (ren loop).

Referensjämförelsen behövs eftersom felet "baren i sitt eget fönster" inte är
look-ahead och därför inte syns i ett trunkeringstest.

## C. Konsistens (granskning, `causal=false`)

Resultaten är enbart rapporter och aldrig kolumner per bar. De får inte användas i beslut.

- **`tf_aggregation`** (C1): alla nio Binance-fält jämförs.
  - Priser måste vara exakt lika.
  - Summor tillåter |d| ≤ 1e-8 + 1e-9·|x|.
  - `trade_count` måste vara exakt lika.

  Ett fönster är komplett endast om de faktiska tidsstämplarna är exakt
  {h + i·τ}. Ofullständiga fönster räknas (`TF_INCOMPLETE`, INFO) men
  kontrolleras inte. En saknad högre bar ger `TF_HIGHER_MISSING` (WARN), och
  0 kontrollerbara fönster ger `UNKNOWN`.
- **`xinst_alignment`**: tidsstämplar i överlappet som finns bara i ett instrument ger WARN.
- **`xinst_basis`**: |basis| > 300 bps ger WARN och > 1000 bps ger FAIL.
- **`xinst_lag`**: diagnostisk, **aldrig FAIL**. Lag −3..3 räknas på tidsstämplar.
  - Om något lag har färre än 500 par blir kontrollen `UNKNOWN`.
  - ℓ* ≠ 0 ger WARN, och corr0 < 0,5 ger WARN.
  - Vid lika korrelation väljs minsta |ℓ|, därefter minsta ℓ.
- **`replayed_block`** (C3): UTC-dygn för tf ≤ 4h.
  - Ett dygn kvalificerar om det har exakt B barer, minst två olika close och
    volym > 0.
  - Signaturen är SHA-256 av `struct.pack("<8dq", O,H,L,C,V,QV,TB,TQ,N)` per bar.
  - Dubbletter ger FAIL, och färre än 2 kvalificerade dygn ger `UNKNOWN`.
  - För 1d blir kontrollen `UNKNOWN` (not applicable), så dataset-rapporter för
    1d blir alltid `INCOMPLETE`.

## Täckning och Phase 2-misstanke

Dataset-rapporten räknar förväntade slots, saknade barer, antal luckor och största
lucka från data, och jämför med manifestets `missing_bars_total`. Vid avvikelse
avbryts körningen med `Phase2SuspectError` (CLI-returkod 4). Det är ett möjligt
Phase 2-fel, som ska rapporteras och inte rättas inom Phase 3.

## Bindning och determinism

Varje rapport binds till:

- `data_version` för varje dataset, verifierad mot innehållet med `content_hash`.
- Konfigurationens kanoniska hash.
- Kodversionen (`git rev-parse HEAD`) och om arbetskatalogen var ren.

Rapporterna innehåller ingen väggklocktid. Samma data, konfiguration och kod ger
byte-identiska filer. `load_report(path, [data_version, ...])` vägrar en rapport
som gäller andra data.

Kör rapporterna med `--out` **utanför repot**. Annars gör de skrivna filerna
arbetskatalogen smutsig, och nästa körning får `worktree_clean=false`.

## CLI

```
python scripts/data_quality_report.py dataset --store S --out O --instrument spot --timeframe 15m
python scripts/data_quality_report.py tf      --store S --out O --instrument spot --lower 1m --higher 15m
python scripts/data_quality_report.py xinst   --store S --out O --timeframe 15m
```

| Returkod | Betydelse |
|---|---|
| 0 | PASS eller WARN |
| 1 | FAIL |
| 3 | INCOMPLETE |
| 2 | Fel i indata eller konfiguration |
| 4 | Möjligt Phase 2-fel |

`--out` i eller under lagret vägras.

## Verifiering (Phase 3 V2, WSL)

Phase 3 V2-verifieringen uppfyllde **alla automatiska kriterier**. Den kördes med
`verify_phase3.sh` i WSL2 med Python 3.12.3. Ingen verifieringstagg har skapats.

| Post | Värde |
|---|---|
| Commit | `6eef6c7393fc73e24f6a3e1c5e74fa9795a1e90c` |
| Patch SHA-256 | `50e496bd1ed36f88935875cad4505f87bb402cdd7229cc0f93ec9ce33168b22b` |
| pytest | 247 passed, 0 failed, 0 skipped |
| Kausalitet | 20/20 passed |
| Mutationstester | 19/19 fångade |
| Ruff | 41 fynd totalt, exakt Phase 2-baslinjen. 0 nya fynd i Phase 3-filer. |
| mypy | 24 fel totalt: de befintliga 18 plus 6 förväntade `import-untyped`. Inga oväntade nya fel. |
| Rapporter på riktig data | Två körningar var byte-identiska. Lagret var orört. |

### Fynd på riktig Binance-data: `TF_AGGREGATION_MISMATCH`

Data: BTCUSDT januari 2025, månadsarkiv för 1m och 15m från `data.binance.vision`.

- **spot 1m till 15m: `PASS`.** 2976 av 2976 fönster kontrollerade, 0 avvikande.
- **perp 1m till 15m: `FAIL`.** Exakt 3 fönster med `TF_AGGREGATION_MISMATCH`:
  1. `2025-01-14T13:30:00+00:00`
  2. `2025-01-14T15:00:00+00:00`
  3. `2025-01-29T01:30:00+00:00`

### Utredning (`investigate_tf_v1.py`)

Avvikelsen utreddes med `investigate_tf_v1.py`, SHA-256
`5a647a61b228c6c460bc1a446dc86b668410fcfd8f04919ac10802fb6e6cdb6d`. Skriptet läser
Binance-zipfilerna direkt med `csv` och `Decimal`. Aggregeringen använder varken pandas
eller kod från Phase 2 eller Phase 3.

1. **Phase 2-trohet.** Varje rad i rådatan jämfördes med raden i lagret, för alla fält.
   Inga rader saknades. Alla fyra dataset låg inom verifieringens kriterium, som är högst
   1 ULP. Återgivningen är **inte** bitidentisk för alla dataset:

   | Dataset | Max ULP |
   |---|---|
   | spot 1m | 0 |
   | spot 15m | 1 |
   | perp 1m | 0 |
   | perp 15m | 0 |

2. **Oberoende aggregering.** En exakt `Decimal`-aggregering av 1m-rådatan mot 15m-rådatan
   gav samma 3 avvikande perp-fönster, och 0 för spot.
3. **Validatorn.** Validatorns rapport anger samma 3 fönster.

**Slutsats:** de använda perp-arkiven för 1m och 15m från Binance är inkonsistenta med
varandra i dessa 3 fönster. Det är en inkonsistens i datakällan, inte ett fel i implementationen
eller verifieringen. Det är **inte** fastställt vilket av arkiven som är korrekt. Tredje källor,
som Binances dagsarkiv, REST-API:t och aggTrades, kontrollerades inte.

Fyndet ligger kvar som **`FAIL`**. Den låsta specifikationen kräver att
`TF_AGGREGATION_MISMATCH` är FAIL, och fyndet nedgraderas inte.

## Kända begränsningar

- Omkastade spot- och perp-mappar kan inte upptäckas robust. Basis blir bara
  teckenvänd och liten. Detta dokumenteras av ett test.
- Anomaliflaggor kräver uppvärmning (m observationer). Korta dataset ger
  `UNKNOWN` och därmed `INCOMPLETE`.
- En verklig krasch ger `RETURN_OUTLIER` (WARN). Det är avsiktligt: anomalier
  är signaler att granska och inte bevis för fel data.
- Den tvärgående lag-kontrollen jämför bara avkastningar, inte nivåer.
- Kontrollerna ser bara det som finns i lagret. Fel som Phase 2 redan har
  kasserat vid ingest syns inte här.
- `tf_aggregation` avgör inte vilken källa som är rätt när 1m- och 15m-arkiven
  inte stämmer med varandra. Den rapporterar bara att de är inkonsistenta (se avsnittet om verifiering).
