# Data ingestion (PHASE 2) — Binance BTC/USDT, lokala filer

Pipelinen gör **ingen nätverksåtkomst**. Du laddar ner filerna manuellt och matar in dem.

## 1. Vad du ska ladda ner (manuellt)

Källa: <https://data.binance.vision> (Binance Public Data). Mappstruktur enligt min kännedom
(kunde inte verifieras här, ingen nätverksåtkomst — kontrollera i webbläsaren):

| Instrument | Sökväg (månadsfiler) |
|---|---|
| BTC/USDT spot | `data/spot/monthly/klines/BTCUSDT/<tf>/BTCUSDT-<tf>-YYYY-MM.zip` |
| BTC/USDT perp (USD-M) | `data/futures/um/monthly/klines/BTCUSDT/<tf>/BTCUSDT-<tf>-YYYY-MM.zip` |

`<tf>` ∈ `1m 5m 15m 30m 1h 4h 1d`. Dagsfiler (`daily/…-YYYY-MM-DD.zip`) kan blandas in för den
senaste, ofullständiga månaden. Varje zip har en `.CHECKSUM`-fil — verifiera sha256 innan ingest.
COIN-M (`BTCUSD_PERP`) stöds **inte** i Phase 2. Mark price-, index-, funding- och OI-filer kommer
i en senare fas (de har andra scheman).

Föreslagen lokal placering (fritt val): `raw/binance/spot/15m/*.zip`, `raw/binance/perp/15m/*.zip`.

## 2. Accepterade filformat

* `.zip` med **exakt en** `.csv` (originalet från Binance), `.csv`, eller `.parquet`.
* CSV med eller utan headerrad. Headerlöst = Binance-ordning, 11–12 kolumner:
  `open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore`
  Vanliga headernamn/alias mappas (`count`→`trade_count`, `taker_buy_volume`→`taker_buy_base_volume` …).
* Parquet: samma kolumnnamn; `open_time` som tz-aware **UTC** datetime eller heltal (ms/µs).

**Tidsstämplar** (strikt, ingen gissning):

| Input | Resultat |
|---|---|
| 13 siffror (epoch ms) | OK |
| 16 siffror (epoch µs; Binance spot-filer från 2025 anges använda µs) | `open_time`: OK om jämna ms. `close_time`: OK även `…999999` (open + tf − 1 µs) |
| 10 siffror (sekunder), 19 siffror (ns) | **Fel** |
| ISO-8601 med `Z`/`+00:00` | OK |
| ISO utan tidszon / med annan offset | **Fel** (`TimezoneError`) |
| Parquet datetime tz-naiv / ej UTC | **Fel** |
| Före 2009, text, tomma celler, blandade format | **Fel** |

`open_time` = barens **start**. `available_at = open_time + timeframe` = då baren är känd.

## 3. Ingest

```bash
pip install -e ".[dev]"
python scripts/ingest_binance.py --store data_store --instrument spot --timeframe 15m raw/binance/spot/15m/*.zip
python scripts/ingest_binance.py --store data_store --instrument perp --timeframe 15m raw/binance/perp/15m/*.zip
# valfritt: --fail-on-gaps   (luckor blir fel istället för flaggor)
```
Ett anrop bygger om **hela** datasetet (instrument × symbol × timeframe) från de filer du anger.
Allt-eller-inget: vid fel skrivs ingenting.

### Regler
1. Varje fil måste i sig vara tidsstigande, rutnätsjusterad (`open_time % tf == 0`) och konsekvent
   (`close_time == open_time + tf − 1 ms` eller `− 1 µs`, vilket fångar fel timeframe-fil).
   Utan `close_time` (ISO/Parquet): minst 50 % av intilliggande barer måste ligga exakt `tf` isär,
   annars `TIMEFRAME_MISMATCH` (t.ex. 1h-fil ingestad som 15m).
1b. Filnamn enligt Binance-mönstret `SYMBOL-tf-YYYY-MM[-DD].(zip|csv|parquet)` måste matcha
   `--symbol`/`--timeframe`. **Spot- och perp-filer har samma namn hos Binance** — att hålla isär
   dem är ditt ansvar (separata mappar). Egna filnamn kontrolleras inte.
2. Filer får ges i valfri ordning; de sorteras.
3. **Identiska** dubbletter (överlappande filer) tas bort och räknas i manifestet.
   `close_time` ignoreras vid jämförelsen (samma bar i ms- och µs-fil räknas som identisk).
4. **Motstridiga** dubbletter (samma tid, olika värden) → fel `DUPLICATE_CONFLICT`.
5. **Luckor fylls aldrig** (ingen ffill/interpolering). De flaggas: `gap_before` på baren efter luckan
   samt lista i manifestet.
6. Barer som inte var avslutade vid ingest-tillfället (`FUTURE_BAR`) avvisas.
7. OHLC-konsistens, positiva priser, icke-negativa volymer, inga NaN/inf.

## 4. Lagring
```
data_store/binance/<spot|perp>/BTCUSDT/<tf>.parquet
data_store/binance/<spot|perp>/BTCUSDT/<tf>.manifest.json
```
Deterministiskt: fast schema utan pandas-metadata, fast kompression/version/radgrupper, sorterat
manifest utan väggklocka. `data_version` (sha256 av innehållet) ändras bara om datat ändras och är
oberoende av pyarrow-version. Vid läsning verifieras hash, radantal, validering och gap-flaggor
(`DataIntegrityError` vid avvikelse).

## 5. Läsa data
```python
from datetime import datetime, timezone
from btc_trading_system.data.provider import LocalOHLCVProvider

p = LocalOHLCVProvider("data_store", "spot", require_as_of=True)
t = datetime(2024, 3, 1, 12, 0, tzinfo=timezone.utc)
bars = p.get_bars("BTC/USDT", "15m", start=datetime(2024,1,1,tzinfo=timezone.utc), end=t, as_of=t)
```
* `[start, end)` på `open_time`.
* `as_of=t` ⇒ endast barer med `available_at <= t`. Baren som pågår vid `t` returneras aldrig.
* `as_of=None` ⇒ ingen filtrering; använd `require_as_of=True` i backtest/paper.
* `gap_before` beror bara på föregående bar (kausal). `get_gaps(as_of=t)` visar bara luckor vars
  efterföljande bar redan var avslutad vid `t`.

## 6. Kända begränsningar
* Bara OHLCV. Inget resample-steg (multi-timeframe kommer som feature med eget kausalitetstest).
* Spot och perp har olika priser/volymer; blanda dem aldrig utan explicit basis-logik.
* Zero-volume-barer och exchange-underhåll kan skapa luckor; datat är exchange-specifikt (Binance).
* Binance-filer antas oreviderade; ändrar Binance historik ändras `data_version` vid re-ingest.

## 7. Phase 4: versionslagret
Phase 2-lagret ovan är källan för `import-legacy` och skrivs aldrig av Phase 4 (P4-S6-SELF).
Nya data kan ingestas direkt i Phase 4-versionslagret med `datastore ingest`, som kör samma
Phase 2-pipeline oförändrad (P4-S4-FC), alltså samma regler som i avsnitt 2-3, men publicerar
oföränderliga versioner med lineage. Se `docs/PHASE4_DATASTORE.md`, avsnitt 5.5 och 9.4.
