# ORB Backtester v1.3 (SP500 Focus)

## Obiettivo
Questa versione 1.3 mantiene l'engine ORB estendibile a piu mercati, ma concentra i test principali su SP500.

## Logica ORB v1.3
- Timezone ufficiale: America/New_York.
- Opening range: candela M15 09:30-09:45 NY (ricostruita da dati 5m).
- Breakout confermato su M5:
  - LONG: high > orb_high e close > orb_high
  - SHORT: low < orb_low e close < orb_low
- Entry: apertura della candela successiva.
- Stop loss: lato opposto dell'opening range.
- Take profit: RR fisso 1:1.
- Max trade giornalieri: configurabile, default 2.
- Filtro ORB range class testabile da CLI.

## Scenari principali v1.3
Gli scenari principali predefiniti sono:
- market: SP500
- force close: no_time_close
- breakout windows:
  - breakout_window_0945_1000
  - breakout_window_0945_1030
- ORB range filter: all

Nota: altri mercati, 12:00/16:00 e filtri multipli restano supportati via CLI.

## Filtro ORB range class
La classe ORB range (small/medium/large) non e piu solo diagnostica: puo diventare un filtro operativo.

Valori supportati per --orb-range-filters:
- all
- small
- medium
- large
- small+medium
- medium+large
- small+large
- small+large

Interpretazione:
- un filtro include solo i trade la cui orb_range_class appartiene al set richiesto.
- con all, nessun trade viene escluso.

La classificazione usa quantili su orb_range_pct_of_entry ed e centralizzata in una funzione dedicata in backtest.

## Nuove diagnostiche per trade (CSV)
Nel file trades vengono inclusi campi aggiuntivi:
- breakout_time_ny
- entry_time_ny
- exit_time_ny
- breakout_window_label
- minutes_from_0945_to_breakout
- trade_duration_minutes
- orb_range_points
- orb_range_pct_of_entry
- orb_range_class (small/medium/large)

Classificazione ORB range:
- basata su quantili del campo orb_range_pct_of_entry
- default: 0.33, 0.66
- configurabile con --orb-range-quantiles

## Metriche scenario
Per ogni scenario vengono calcolate:
- total trades
- win rate
- loss rate
- profit factor
- average win
- average loss
- expectancy
- average R
- total R
- max drawdown
- final equity

In piu:
- distribuzione trades per breakout window
- distribuzione trades per fascia oraria breakout
- performance per fascia oraria breakout
- performance per classe ORB range
- confronto per orb_range_filter nella tabella finale

## Output in /outputs
Per ogni mercato/scenario:
- trades_<market>_<scenario>.csv
- metrics_<market>_<scenario>.json
- equity_<market>_<scenario>.csv
- breakout_time_stats_<market>_<scenario>.csv
- orb_range_stats_<market>_<scenario>.csv

Esempio scenario label:
- breakout_window_0945_1000_no_time_close_orb_all
- breakout_window_0945_1000_no_time_close_orb_small
- breakout_window_0945_1030_no_time_close_orb_small_plus_medium

## Comandi CLI
Esecuzione base (focus SP500):
```bash
python main.py
```

SP500 con varianti filtro ORB:
```bash
python main.py --period 60d --interval 5m --markets SP500 --force-close-options none --breakout-windows 10:00,10:30 --orb-range-filters all,small,medium,large,small+medium,medium+large
```

Confronto multi-market mantenendo la stessa logica:
```bash
python main.py --period 60d --interval 5m --markets NASDAQ,SP500 --force-close-options none,12:00 --breakout-windows 11:00,10:30,10:00
```

Includere opzionalmente 16:00:
```bash
python main.py --period 60d --interval 5m --markets SP500 --force-close-options none,12:00,16:00 --breakout-windows 10:00,10:30 --orb-range-filters all,small+medium
```

Cambiare soglie classi ORB range:
```bash
python main.py --orb-range-quantiles 0.25,0.75
```

## Robustezza gestita
- scenario senza trade: output creati con metriche zero.
- dataset incompleto: giorni senza candela ORB esclusi, conteggiati in diagnostica.
- timezone: conversione coerente a New York.
- NaN dati OHLC: righe eliminate e conteggiate in diagnostica.
- risk non valido (entry/stop incoerenti): trade scartato e conteggiato.
- filtri ORB restrittivi: output coerenti anche con zero trade.
