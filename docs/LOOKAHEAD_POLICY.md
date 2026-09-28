# Look-ahead policy (gäller varje feature-steg)
1. Bar.open_time = start; Bar.available_at = close. Beslut vid t använder bara barer med available_at <= t.
2. Rad t i en feature får bara bero på rader <= t (rolling/expanding, aldrig center=True, aldrig shift(-n), aldrig global mean/std).
3. Signal på bar t stängs -> order tidigast t -> fill strikt senare (min_latency_bars >= 1), till nästa bars pris + slippage.
4. Multi-timeframe: högre TF-bar används först efter dess available_at (shift(1) efter resample med label=left/closed=left).
5. Externa variabler (funding, OI, ETH) joinas med merge_asof(direction="backward") på publiceringstid.
6. Varje feature MÅSTE ha ett test med assert_causal (trunkeringsinvarians) + ett negativt kontrolltest.
7. Parametrar väljs på TRAIN, jämförs på VALIDATION, FINAL TEST rörs en gång.
