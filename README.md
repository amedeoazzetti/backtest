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
Focus v1.6:
python main.py --period 60d --interval 5m --markets SP500 --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both,long_only,short_only

Confronto RR esteso mantenendo direzioni:
python main.py --period 60d --interval 5m --markets SP500 --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5,2.0,3.0 --trade-direction-modes both,long_only,short_only

Confronto multi-market:
python main.py --period 60d --interval 5m --markets SP500,NASDAQ --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both,long_only,short_only
