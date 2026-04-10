# ORB Backtester (M5-Only Phase)

## Obiettivo Nuova Fase
Il progetto ora usa una base piu rigorosa e semplice:
- sorgente unica di verita: dati M5
- ORB costruita direttamente da 3 candele M5 (09:30, 09:35, 09:40 New York)
- confronto robusto su storico lungo tra SP500 e NASDAQ

Non viene piu usato un dataset 15m separato come base primaria del motore.

## Logica ORB (M5-Only)
Per ogni giornata (America/New_York), la ORB 09:30-09:45 viene costruita da:
- candela 09:30
- candela 09:35
- candela 09:40

Regole:
- open = open 09:30
- high = max(high 09:30, 09:35, 09:40)
- low = min(low 09:30, 09:35, 09:40)
- close = close 09:40
- volume = somma volumi delle 3 candele

Se il triplet non e valido (buchi, duplicati, allineamento errato), la giornata non viene tradata.

## Input Dati
### CSV storico M5
CSV atteso con colonne:
- Time, Open, High, Low, Close, Volume

Parametri principali:
- --csv-path (fallback unico)
- --csv-path-sp500 (CSV dedicato SP500)
- --csv-path-nasdaq (CSV dedicato NASDAQ)
- --csv-timezone (timezone dei timestamp in input, es: UTC)

### Provider esterno (fallback)
Se non passi CSV per un mercato, il tool prova a scaricare via provider esterno.

## Matrice Focus (Nuova Fase)
Default orientati al confronto SP500 vs NASDAQ:
- markets: SP500,NASDAQ
- breakout windows: 10:00,10:30
- orb range filters: all,small,small+large
- rr targets: 1.0,1.5
- trade direction modes: both

## Audit Minimo Integrato
Per ogni mercato il programma stampa e traccia:
- righe M5 lette
- righe valide dopo pulizia
- timezone sorgente e finale
- giornate con ORB valida
- giornate senza ORB valida

Output dedicato per mercato:
- outputs/missing_orb_days_sp500.csv
- outputs/missing_orb_days_nasdaq.csv

Colonna reason in missing_orb_days:
- no_0930_bar
- incomplete_0930_block
- session_missing
- all_5m_missing
- timezone_alignment_issue
- duplicate_orb_bar
- unknown

## Audit Mode (Opzionale)
Abilita diagnostica estesa:
- --audit-mode
- --audit-sample-days 10
- --audit-dates 2024-08-01,2024-08-02
- --audit-trades-limit 20

File generati in outputs/audit:
- dataset_audit_summary.json
- missing_orb_days.csv
- ambiguous_signal_candles.csv
- orb_resample_audit.csv
- trade_replay_audit.csv

## Output Backtest
Per ogni scenario:
- trades_<market>_<scenario>.csv
- metrics_<market>_<scenario>.json
- equity_<market>_<scenario>.csv
- breakout_time_stats_<market>_<scenario>.csv
- breakout_minute_stats_<market>_<scenario>.csv
- direction_stats_<market>_<scenario>.csv
- orb_range_stats_<market>_<scenario>.csv

Output comparativo aggregato:
- outputs/comparison_summary.csv

## Esempi CLI
Confronto completo SP500 + NASDAQ con CSV separati:
python main.py --csv-path-sp500 data/sp500_5m.csv --csv-path-nasdaq data/nasdaq_5m.csv --csv-timezone UTC --markets SP500,NASDAQ --force-close-options none --breakout-windows 10:00,10:30 --orb-range-filters all,small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both

Confronto su un solo CSV (fallback):
python main.py --csv-path data/ES_5Years_8_11_2024.csv --csv-timezone UTC --markets SP500 --force-close-options none --breakout-windows 10:00,10:30 --orb-range-filters all,small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both

Con audit mode attivo:
python main.py --csv-path-sp500 data/sp500_5m.csv --csv-path-nasdaq data/nasdaq_5m.csv --csv-timezone UTC --markets SP500,NASDAQ --force-close-options none --breakout-windows 10:00,10:30 --orb-range-filters all,small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both --audit-mode --audit-sample-days 10 --audit-trades-limit 20
