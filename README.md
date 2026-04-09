# ORB Backtester v1.5 (RR Targets)

## Obiettivo
La v1.5 mantiene invariata la logica di trading ORB e introduce il test multi-target RR.

Focus operativo principale:
- market: SP500
- force close: no_time_close
- breakout window: 10:30
- orb filters: small, small+large
- rr targets: 1.0, 1.5, 2.0, 3.0

## Logica Trading (invariata)
- Timezone: America/New_York
- Opening range: candela 09:30-09:45 (M15 ricostruita da 5m)
- Breakout confermato su M5
- Entry su open candela successiva
- Stop loss sul lato opposto del range

## RR Target Configurabile
Nuovo parametro CLI:
- --rr-targets 1.0,1.5,2.0,3.0

Ogni RR richiesto genera uno scenario separato.

Calcolo TP:
- LONG:
  - risk = entry_price - stop_loss
  - take_profit = entry_price + risk * rr_target
- SHORT:
  - risk = stop_loss - entry_price
  - take_profit = entry_price - risk * rr_target

Regole di validazione:
- rr_target deve essere numerico, finito e > 0
- input non valido produce errore esplicito senza crash runtime

## Scenario Naming con RR
Il nome scenario include sempre il RR:
- breakout_window_0945_1030_no_time_close_orb_small_rr_1_0
- breakout_window_0945_1030_no_time_close_orb_small_rr_1_5
- breakout_window_0945_1030_no_time_close_orb_small_rr_2_0
- breakout_window_0945_1030_no_time_close_orb_small_rr_3_0

## Trade Log
Ogni trade include anche:
- rr_target

Restano coerenti anche:
- risk_points
- reward_points
- result_r

## Output in /outputs
Per ogni scenario RR:
- trades_<market>_<scenario>.csv
- metrics_<market>_<scenario>.json
- equity_<market>_<scenario>.csv
- breakout_time_stats_<market>_<scenario>.csv
- breakout_minute_stats_<market>_<scenario>.csv
- direction_stats_<market>_<scenario>.csv
- orb_range_stats_<market>_<scenario>.csv

Esempio:
- trades_sp500_breakout_window_0945_1030_no_time_close_orb_small_rr_2_0.csv
- metrics_sp500_breakout_window_0945_1030_no_time_close_orb_small_rr_2_0.json
- equity_sp500_breakout_window_0945_1030_no_time_close_orb_small_rr_2_0.csv

## Tabella Finale
Include almeno:
- rr_target
- total_trades
- win_rate
- profit_factor
- average_r
- total_r
- max_drawdown
- final_equity

E mantiene anche:
- long_trades
- short_trades
- long_avg_r
- short_avg_r
- long_total_r
- short_total_r

## Robustezza
Gestione edge cases:
- rr_target non valido
- scenari senza trade
- metriche non calcolabili
- output vuoti ma coerenti
- NaN/missing su dati OHLC

## Comandi CLI Consigliati
Focus v1.5:
python main.py --period 60d --interval 5m --markets SP500 --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5,2.0,3.0

Confronto esteso su piu filtri ORB:
python main.py --period 60d --interval 5m --markets SP500 --force-close-options none --breakout-windows 10:30 --orb-range-filters all,small,medium,large,small+medium,medium+large,small+large --rr-targets 1.0,1.5,2.0,3.0

Confronto multi-market mantenendo RR multipli:
python main.py --period 60d --interval 5m --markets SP500,NASDAQ --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5,2.0
