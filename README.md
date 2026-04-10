# ORB Backtester v1.6 (Directional Simulation Modes)

## Obiettivo
La v1.6 mantiene invariata la logica ORB e aggiunge la simulazione per direzione:
- both
- long_only
- short_only

Focus operativo principale:
- market: SP500
- force close: no_time_close
- breakout window: 10:30
- orb filters: small, small+large
- rr targets: 1.0, 1.5
- trade direction modes: both, long_only, short_only

## Logica Trading (invariata)
- Timezone: America/New_York
- Opening range: candela 09:30-09:45 (M15 da dati 5m)
- Breakout confermato su M5
- Entry su open candela successiva
- Stop loss sul lato opposto del range
- Take profit con RR target configurabile

## Nuova Sorgente Dati CSV 5m
Il backtester supporta una modalita sorgente primaria da CSV storico.

Nuovi parametri CLI:
- --csv-path data/ES_5Years_8_11_2024.csv
- --csv-timezone UTC
- --m15-incomplete-policy drop|keep

Comportamento:
- se --csv-path e presente, i dati vengono letti dal CSV invece del provider esterno
- i timestamp sono interpretati usando --csv-timezone
- i dati vengono poi convertiti internamente in America/New_York

CSV atteso (colonne):
- Time, Open, High, Low, Close, Volume

Pipeline dati:
- parsing robusto datetime
- sort cronologico
- rimozione duplicati
- conversione numerica OHLCV
- scarto righe malformate/NaN
- controllo high >= low

Diagnostica in log:
- righe lette dal CSV
- righe dopo pulizia
- timezone sorgente/finale
- candele 15m create
- blocchi 15m incompleti e policy applicata

## Resampling 15m Da 5m
Il 15m viene derivato dal 5m con regole OHLCV standard:
- Open: prima open del blocco
- High: max high del blocco
- Low: min low del blocco
- Close: ultima close del blocco
- Volume: somma volumi del blocco

Allineamento:
- resample a 15 minuti con ancoraggio ai quarti d'ora reali
- in timezone America/New_York
- quindi la candela ORB 09:30-09:45 e allineata correttamente

Qualita blocchi 15m:
- ogni candela 15m tiene il conteggio delle candele 5m sorgenti
- blocco completo ideale: 3 candele 5m
- con --m15-incomplete-policy drop (default) i blocchi incompleti vengono scartati
- con --m15-incomplete-policy keep vengono mantenuti e segnalati nei log

## RR Target
Parametro CLI:
- --rr-targets 1.0,1.5

Calcolo TP:
- LONG:
  - risk = entry_price - stop_loss
  - take_profit = entry_price + risk * rr_target
- SHORT:
  - risk = stop_loss - entry_price
  - take_profit = entry_price - risk * rr_target

## Trade Direction Modes
Nuovo parametro CLI:
- --trade-direction-modes both,long_only,short_only

Comportamento:
- both: accetta segnali LONG e SHORT
- long_only: accetta solo segnali LONG
- short_only: accetta solo segnali SHORT

La modalita e parte dello scenario: il backtest viene simulato separatamente per ciascuna modalita.

## Scenario Naming
Ogni scenario include:
- breakout window
- force close
- orb filter
- rr target
- trade direction mode

Esempio:
- breakout_window_0945_1030_no_time_close_orb_small_rr_1_5_both
- breakout_window_0945_1030_no_time_close_orb_small_rr_1_5_long_only
- breakout_window_0945_1030_no_time_close_orb_small_rr_1_5_short_only

## Trade Log
Ogni trade include esplicitamente:
- rr_target
- trade_direction_mode

Restano coerenti:
- risk_points
- reward_points
- result_r

## Output in /outputs
Per ogni scenario:
- trades_<market>_<scenario>.csv
- metrics_<market>_<scenario>.json
- equity_<market>_<scenario>.csv
- breakout_time_stats_<market>_<scenario>.csv
- breakout_minute_stats_<market>_<scenario>.csv
- direction_stats_<market>_<scenario>.csv
- orb_range_stats_<market>_<scenario>.csv

Esempi:
- trades_sp500_breakout_window_0945_1030_no_time_close_orb_small_rr_1_5_short_only.csv
- metrics_sp500_breakout_window_0945_1030_no_time_close_orb_small_plus_large_rr_1_0_long_only.json

## Tabella Finale
Include anche:
- trade_direction_mode
- rr_target

E mantiene:
- total_trades
- win_rate
- profit_factor
- average_r
- total_r
- max_drawdown
- final_equity
- long_trades
- short_trades
- long_avg_r
- short_avg_r
- long_total_r
- short_total_r

## Robustezza
Gestione edge cases:
- modalita direzione non valida
- rr target non valido
- scenari senza trade
- zero long o zero short
- metriche non calcolabili
- output vuoti ma coerenti

## Comandi CLI consigliati
CSV storico (sorgente primaria):
python main.py --csv-path data/ES_5Years_8_11_2024.csv --csv-timezone UTC --markets SP500 --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both

CSV storico con policy prudente esplicita sui 15m incompleti:
python main.py --csv-path data/ES_5Years_8_11_2024.csv --csv-timezone UTC --m15-incomplete-policy drop --markets SP500 --force-close-options none --breakout-windows 10:30 --orb-range-filters small --rr-targets 1.0 --trade-direction-modes both,long_only,short_only

Focus v1.6:
python main.py --period 60d --interval 5m --markets SP500 --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both,long_only,short_only

Confronto RR esteso mantenendo direzioni:
python main.py --period 60d --interval 5m --markets SP500 --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5,2.0,3.0 --trade-direction-modes both,long_only,short_only

Confronto multi-market:
python main.py --period 60d --interval 5m --markets SP500,NASDAQ --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both,long_only,short_only
