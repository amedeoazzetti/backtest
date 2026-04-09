# ORB Backtester v1.4 (Directional and Minute Analytics)

## Obiettivo
La v1.4 non introduce nuove regole di trading. Estende la parte analitica per capire meglio l'edge nelle configurazioni piu promettenti.

Focus principale:
- market: SP500
- force close: no_time_close
- breakout windows: 10:00, 10:30
- ORB range filters: small, small+large

## Logica Trading (invariata)
- Timezone ufficiale: America/New_York
- Opening range: candela M15 09:30-09:45 NY (ricostruita da 5m)
- Breakout confermato su M5
- Entry: apertura candela successiva
- Stop loss: lato opposto dell'opening range
- Take profit: RR fisso 1:1

## Filtro ORB Range Class
Filtri supportati da CLI:
- all
- small
- medium
- large
- small+medium
- medium+large
- small+large

Il filtro non cambia la logica dei trade: limita solo i trade considerati nel report dello scenario.

## Nuove Analisi v1.4
### 1. Breakdown Direzionale
Per ogni scenario vengono calcolate metriche separate per:
- BOTH (totale)
- LONG
- SHORT

Metriche per ciascun lato:
- total_trades
- win_rate
- profit_factor
- average_win
- average_loss
- expectancy
- average_r
- total_r
- max_drawdown
- final_equity

### 2. Breakdown per Breakout Minute
Ogni trade include:
- breakout_minute_bucket (es. 09:45, 09:50, 09:55, 10:00...)

Report aggregato per minuto:
- total_trades
- win_rate
- average_r
- total_r
- profit_factor (quando calcolabile)
- note (low_sample per bucket con pochi casi)

## Output in /outputs
Per ogni scenario:
- trades_<market>_<scenario>.csv
- metrics_<market>_<scenario>.json
- equity_<market>_<scenario>.csv
- breakout_time_stats_<market>_<scenario>.csv
- breakout_minute_stats_<market>_<scenario>.csv
- direction_stats_<market>_<scenario>.csv
- orb_range_stats_<market>_<scenario>.csv

Esempio:
- trades_sp500_breakout_window_0945_1030_no_time_close_orb_small_plus_large.csv
- direction_stats_sp500_breakout_window_0945_1030_no_time_close_orb_small_plus_large.csv
- breakout_minute_stats_sp500_breakout_window_0945_1030_no_time_close_orb_small_plus_large.csv

## Tabella Finale
La tabella di riepilogo include anche:
- long_trades
- short_trades
- long_avg_r
- short_avg_r
- long_total_r
- short_total_r

## Robustezza
Gestione edge cases:
- filtro che produce zero trade
- nessun LONG o nessun SHORT
- pochi trade per breakout minute
- metriche non calcolabili

In questi casi:
- il programma non va in crash
- i file vengono comunque salvati
- i report riportano note esplicative (es. no_trades, low_sample)

## Comandi CLI consigliati
Focus v1.4 su SP500:
```bash
python main.py --period 60d --interval 5m --markets SP500 --force-close-options none --breakout-windows 10:00,10:30 --orb-range-filters small,small+large
```

Confronto esteso filtri ORB:
```bash
python main.py --period 60d --interval 5m --markets SP500 --force-close-options none --breakout-windows 10:00,10:30 --orb-range-filters all,small,medium,large,small+medium,medium+large,small+large
```

Confronto multi-market mantenendo analisi v1.4:
```bash
python main.py --period 60d --interval 5m --markets SP500,NASDAQ --force-close-options none --breakout-windows 10:00,10:30 --orb-range-filters small,small+large
```
