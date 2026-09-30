# Spec-tillägg: övervakning av live/paper och dagliga rapporter

**Status: FÖRSLAG – inte godkänt omfång.** Tillägget låses del för del när projektet
når Phase 10 (statistik), Phase 18 (paper trading) och Phase 19 (monitoring). Alla
numeriska parametrar i avsnitt 8 låses **innan** första dagen med paper- eller
live-data analyseras. Det är samma princip som för trösklarna i Phase 3: inga
justeringar efter att utfallet har setts.

## 1. Principer

1. **Siffror kommer bara ur systemets egna data.** Rapporter och larm genereras
   deterministiskt av `monitoring/` från systemets positioner, fyllnader, konton och
   lagrade data. En språkmodell beräknar, avrundar eller återger aldrig några siffror.
   En valfri kommentar från en språkmodell (avsnitt 5.5) är avstängd som standard och
   alltid tydligt avgränsad.
2. **Beslut fattas bara via regler som bestämts i förväg.** Rapporterna är till för
   överblick. Att stoppa handeln, minska risken eller återuppta den sker bara via
   reglerna i avsnitt 4. Parametrar anpassas aldrig automatiskt till live-data, och
   ingen omoptimering görs på live-data.
3. **Ingen look-ahead.** Varje statistik och rapport med tidpunkt t använder bara data
   och händelser som var kända vid t, med samma `available_at`-kontrakt som i
   Phase 1–4.
4. **Allt är spårbart.** Varje rapport binds till:
   - kod-commit,
   - konfigurationens kanoniska hash,
   - strategiversion,
   - `snapshot_id` för referensbacktesten (Phase 4),
   - rapportens schemaversion.

   Rapporten lagras lokalt som deterministisk JSON och Markdown. Samma indata ger
   byte-identiska filer.
5. **Envägskanal.** Leveranskanalen, till exempel en Telegram-bot, kan bara skicka.
   Den tar aldrig emot kommandon.
6. **Riskmotorn är oberoende.** Riskgränser och kill switch i Phase 8 fungerar helt
   oberoende av övervakningen. Övervakningen är aldrig det enda skyddet.

## 2. Referens: den låsta backtesten

Innan paper trading startar fryses en referens:

- **Backtest-körning:** netto efter avgifter, slippage och funding, och
  **out-of-sample** (walk-forward, Phase 11), aldrig in-sample. Den identifieras med
  `snapshot_id`, commit och konfigurationshash.
- **Referensstatistik:**
  - avkastning per affär rᵢ: medelvärde μ_b och standardavvikelse σ_b,
  - daglig avkastning: μ_b,d och σ_b,d,
  - träffsäkerhet p_b,
  - fördelningen av maximal drawdown per horisont H, via block-bootstrap (avsnitt 3.2).
- **Exekveringsmodell:** modellerad slippage, avgift och latens per ordertyp.

Referensen lagras som en egen, hashad fil och ändras aldrig under en körning. En ny
strategiversion kräver en ny referens och en ny körning.

## 3. Två separata jämförelser

### 3.1 Genomförande: live mot omkörning av backtesten (dagligen)

För varje avslutad period körs backtestern i omkörningsläge. Den använder **samma
marknadsdata**, lagrad som snapshot i Phase 4, samma strategiversion och
konfiguration, och samma beslutstidpunkter som live-handeln. Båda möter exakt samma
marknadsrörelse, så skillnaden speglar bara genomförandet:

  Var(L − R) = σ²_L + σ²_R − 2ρ·σ_L·σ_R ≈ 2σ²(1 − ρ).

Med ρ ≈ 0,99 är standardavvikelsen för skillnaden cirka 0,14σ. Det gör jämförelsen
ungefär 50 gånger effektivare än en jämförelse mot historiska genomsnitt.

**Matchning av affärer.** Live-ordrar paras med omkörningens ordrar via signal-id,
beslutstid och riktning. Varje affär klassas som *matchad*, *missad* (bara i
omkörningen) eller *extra* (bara live).

**Uppdelning av skillnaden**, med kostnad som positivt tecken:

  ΔP&L = Σ IS + Σ ΔAvgift + Σ ΔFunding + P&L(missade) − P&L(extra) + residual

- **Implementation shortfall** per fyllnad:
  ISᵢ = sideᵢ · (fyllnadsprisᵢ − modellprisᵢ) · kvantitetᵢ, där side = +1 för köp och
  −1 för sälj. I baspunkter: ISᵢ / (modellprisᵢ · kvantitetᵢ) · 10⁴.
- **Avgifter och funding:** faktiskt belopp minus modellerat belopp.
- **Latens:** t_fyllnad − t_beslut, jämfört med modellerad latens.
- **Residual:** allt som inte förklaras av posterna ovan. Den ska vara 0 inom
  avrundning. En residual ≠ 0 betyder ett bokföringsfel och ger larm.

**Statistik.** Medelavvikelsen i slippage d̄ (bps) redovisas med ett 95 %
t-konfidensintervall. Flagga sätts om **den nedre gränsen** överstiger
toleransen τ_slip, det vill säga när genomförandet systematiskt är sämre än modellen.
En missad eller extra affär ger alltid WARN, eftersom den ska förklaras och inte
jämnas ut i ett medelvärde.

### 3.2 Edge: strategin mot referensen (löpande, långsamt)

Om edgen finns kvar går bara att avgöra efter många affärer. Kravet på underlag är
ungefär

  n ≈ (z_α + z_β)² / d²,

där d är avvikelsen i standardavvikelser per observation. Exempel: en årlig Sharpe
på 1 ger daglig Sharpe 1/√365 ≈ 0,052, och då krävs ungefär 2 900 dagar för att skilja
den från 0 (α = 0,05 tvåsidigt, styrka 80 %). Dagliga siffror säger därför ingenting om
edgen.

**Primär beslutsregel: nedåtriktad CUSUM** på standardiserade avkastningar per affär,
zᵢ = (rᵢ − μ_b)/σ_b:

  S₀ = 0, Sᵢ = max(0, Sᵢ₋₁ − zᵢ − k), larm när Sᵢ > h.

- k = δ/2, där δ är den förskjutning som ska upptäckas. Standard är att edgen är helt
  borta: δ = μ_b/σ_b.
- h väljs så att den förväntade tiden till falsklarm (ARL₀) uppfyller målet i
  avsnitt 8. ARL₀ **beräknas genom simulering** från bootstrap av referensens affärer,
  inte med ett normalfördelningsantagande, eftersom kryptoavkastningar har tjocka
  svansar.
- Motivering: CUSUM är optimal i Lordens mening (Moustakides 1986) för att upptäcka en
  förändring i fördelningen. Den hanterar också upprepade kontroller korrekt, utan de
  falsklarm som dagliga signifikanstest skulle ge.

**Sekundär beslutsregel: percentil för drawdown.** Den nuvarande maximala drawdownen
över horisont H jämförs med referensens fördelning, framtagen med block-bootstrap
(stationär bootstrap, blocklängd enligt avsnitt 8, för att bevara autokorrelation).
Larm om percentilen ≥ p_DD.

**Informativt, aldrig beslutsgrundande:**

- **Förväntat intervall:** kumulativ avkastning jämfört med μ_b·n ± z·σ_b·√n. Vid
  upprepad visuell kontroll är detta inte ett giltigt test.
- **Träffsäkerhet** med Wilson-intervall, jämfört med p_b.
- **Minsta upptäckbara avvikelse** vid aktuellt n: MDE = (z_α + z_β)·σ_b/√n. Den visar
  vad underlaget faktiskt kan avgöra.
- **Realiserad volatilitet** jämfört med referensens intervall, som tecken på att
  marknaden har förändrats.

**Minsta underlag.** Före N_min affärer redovisas edge-statistiken som
**UNKNOWN (otillräckligt underlag)**, aldrig som PASS. Det följer Phase 3:s semantik.

**Känd begränsning:** en förändrad marknadsregim och en förlorad edge går inte att
skilja statistiskt på kort sikt. CUSUM larmar på båda. Tolkningen görs manuellt och
dokumenteras.

## 4. Beslutsregler (förregistrerade)

| Nivå | Utlösare | Automatisk åtgärd |
|---|---|---|
| INFO | Allt inom gränserna | Ingen |
| WARN | Missad eller extra affär, slippage-flagga, CUSUM > h/2, drawdown-percentil ≥ p_warn, data- eller API-varning | Ingen; markeras i rapporten och skickas direkt som larm |
| ACTION | CUSUM-larm (S > h), drawdown-percentil ≥ p_DD, residual ≠ 0, oväntad position | **Inga nya positioner.** Befintliga positioner hanteras enligt riskmotorns regler. Larm skickas direkt. |
| KILL | Riskmotorns hårda gränser (Phase 8) | Riskmotorns kill switch, oberoende av övervakningen |

- Handeln återupptas bara efter ett manuellt beslut, som loggas med motivering.
- Övervakningen ändrar aldrig strategiparametrar, positionsstorlek eller riskgränser
  på egen hand, utöver att stoppa nya positioner vid ACTION.

## 5. Rapporter

**Dygnsgräns:** P&L per dygn definieras per UTC-dygn, konsekvent med Binance-data.
Rapporttiderna anges i Europe/Stockholm.

### 5.1 Morgonrapport (07:00): läge och gårdagen

Omfattar det senast avslutade UTC-dygnet, plus läget vid rapporttillfället.

1. **Statusrad:** samlad nivå (INFO, WARN, ACTION eller KILL), läge (paper eller live),
   dataaktualitet (senaste `available_at`), API-status och kill switch-status.
2. **Öppna positioner**, för varje position:
   - instrument, riktning, storlek och ingångspris,
   - markeringspris och orealiserat P&L,
   - exponering som andel av eget kapital och hävstång,
   - avstånd till likvidation (perp) och nästa funding.
3. **Gårdagen:**
   - realiserat och orealiserat P&L, i USDT och som andel av eget kapital,
   - avgifter och funding,
   - eget kapital vid dygnets slut,
   - drawdown från toppen.
4. **Riskflaggor:**
   - utnyttjande av varje riskgräns (exponering, hävstång, daglig förlustgräns),
   - alla larm de senaste 24 timmarna,
   - status i Phase 3-kvalitetsrapporten för senaste data.
5. **Edge i korthet:** CUSUM (S/h), drawdown-percentil, antal affärer och MDE, eller
   UNKNOWN före N_min.

### 5.2 Kvällsrapport (20:00): genomförande och jämförelse

Omfattar innevarande UTC-dygn fram till rapporttillfället och markeras som
**ofullständigt dygn**.

1. **Affärer i dag:** antal, och för varje affär tid, riktning, kvantitet,
   fyllnadspris, IS (bps mot modellen) och avgift.
2. **Bästa och sämsta avslutade affär:** realiserat P&L. Extremvärden är
   informativa och inga bevis om strategin.
3. **Live mot omkörning** (avsnitt 3.1), för i dag och kumulativt:
   - P&L live och i omkörningen,
   - uppdelningen i IS, avgifter, funding, missade/extra affärer och residual,
   - varje missad eller extra affär med förklaring (om den finns).
4. **Slippage:** medel i dag, kumulativt medel med 95 % KI, jämfört med modellen och
   τ_slip.
5. **Edge:** kumulativt utfall mot det förväntade intervallet, Wilson-intervallet
   för träffsäkerhet, och MDE.

### 5.3 Format och leverans

- **Meddelandet** är högst cirka 25 rader, i ren text med fast teckenbredd, så att det
  går att läsa i mobilen.
- **Den fullständiga rapporten** lagras lokalt som JSON och Markdown. Meddelandet
  anger dess `report_id` och hash.
- **Leverans:** envägs, till exempel via Telegram Bot API.
  - Token läses från miljön, aldrig från repot.
  - Omförsök sker med `report_id` som idempotensnyckel, så att samma rapport aldrig
    skickas två gånger.
- **Tystnad är aldrig "allt är OK":**
  - Varje rapport har ett löpnummer, så att en saknad rapport syns.
  - Om rapporten inte kan genereras skickas "RAPPORT MISSLYCKADES" med orsak.
  - En separat hjärtslagskontroll larmar om ingen rapport har skickats inom förväntat
    fönster plus marginal.
- **Integritet:**
  - Meddelanden innehåller aldrig API-nycklar eller konto-id.
  - En konfigurationsflagga kan begränsa belopp till procent av eget kapital.

### 5.4 Larm (punkt 52 i master-specifikationen)

Händelsestyrda och direkta, oberoende av rapporttiderna:

- order fylld eller avvisad,
- riskgräns nådd,
- fel i API eller data,
- ovanlig volatilitet,
- oväntad position,
- kill switch utlöst,
- alla ACTION- och WARN-händelser i avsnitt 4.

Larm dedupliceras med en förbestämd avkylningstid per larmtyp.

### 5.5 Valfri kommentar från en språkmodell

Avstängd som standard. Om den aktiveras gäller:

- Den får bara läsa den färdiga rapportens JSON.
- Den visas i ett separat, märkt block ("Kommentar – genererad, inte beräknad").
- Den får aldrig ersätta, avrunda eller räkna om siffror.
- Rapportens hash omfattar inte kommentaren.

## 6. Placering i faserna

| Fas | Innehåll från detta tillägg |
|---|---|
| 10 Performance analytics | CUSUM, block-bootstrap, förväntat intervall, Wilson, MDE, IS-uppdelning; låsning av referensen (avsnitt 2) |
| 11 Walk-forward | Out-of-sample-referensen som avsnitt 2 kräver |
| 18 Paper trading | Omkörning och matchning (3.1), beslutsregler (4), rapporter (5.1–5.3) i paper-läge |
| 19 Monitoring | Leveranskanal, larm (5.4), hjärtslag, deduplicering, integritet |
| 20–21 | Samma rapporter i live-läge; ingen ändring av regler eller trösklar utan ny låsning |

## 7. Acceptanskriterier (när respektive fas implementeras)

- **Determinism:** samma indata ger byte-identiska rapporter, både JSON och Markdown.
- **Ingen look-ahead:** en rapport med tidpunkt t är oförändrad om data efter t läggs
  till. Det testas genom trunkering, som i Phase 3.
- **CUSUM:**
  - ARL₀ verifieras genom simulering och ligger inom målet.
  - En simulerad förskjutning δ upptäcks inom förväntad fördröjning.
  - Mutationer av k, h och tecken fångas av testerna.
- **Omkörning:** en syntetisk live-körning som är identisk med backtesten ger ΔP&L = 0,
  residual = 0 och inga missade eller extra affärer. Inplanterad slippage, avgift eller
  missad affär attribueras till rätt post.
- **Leverans:**
  - Kanalfel ger omförsök utan dubbletter.
  - En saknad rapport upptäcks via löpnumret och hjärtslaget.
  - Inga hemligheter förekommer i meddelanden, vilket testas med en statisk och en
    dynamisk kontroll.
- **Beslutsregler:** ACTION stoppar nya positioner. Handeln återupptas bara via ett
  loggat manuellt beslut.

## 8. Parametrar som ska låsas (före första paper-dagen)

Alla värden nedan är **platshållare** och låses i en hashad konfigurationsfil, som
`data_quality.yaml` i Phase 3.

| Parameter | Förslag | Kommentar |
|---|---|---|
| Morgonrapport | 07:00 Europe/Stockholm | Gäller senast avslutade UTC-dygn |
| Kvällsrapport | 20:00 Europe/Stockholm | Ofullständigt UTC-dygn |
| N_min | 50 affärer | Före detta är edge-statistiken UNKNOWN |
| δ (CUSUM) | μ_b/σ_b | Förskjutningen "edgen är helt borta" |
| ARL₀-mål | ≥ 1 år av affärer | Beräknas genom simulering |
| p_warn / p_DD | 95 % / 99 % | Percentil för drawdown mot bootstrap-fördelningen |
| Horisont H | 30 dagar | För drawdown-percentilen |
| Blocklängd | Politis–White, fryst | Bestäms på referensdata före start |
| τ_slip | Modellens slippage + 2 bps | Gäller KI:s nedre gräns |
| Residual-tolerans | 1e-8 · eget kapital | Bokföringsfel över detta |
| Larmens avkylningstid | 15 minuter per typ | Deduplicering |
| Referens | `snapshot_id`, commit, konfigurationshash | Fylls i vid låsning |

## 9. Uttryckligen utanför omfånget

- Automatisk omoptimering eller parameterjustering baserad på live-data.
- Beslut baserade på enskilda dagars P&L.
- Att en språkmodell genererar, beräknar eller tolkar siffror i rapporterna.
- Kommandon via rapportkanalen.
