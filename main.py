"""
CLI principale per backtest ORB v1.6.

Esempi:
    python main.py
    python main.py --period 60d --interval 5m --markets SP500
    python main.py --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both,long_only,short_only
    python main.py --csv-path data/ES_5Years_8_11_2024.csv --csv-timezone UTC --markets SP500
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("Installa yfinance: pip install yfinance")
    sys.exit(1)

from backtest import format_market_report, performance_per_market, run_market_backtest
from config import (
    DEFAULT_BREAKOUT_WINDOWS,
    DEFAULT_FORCE_CLOSE_OPTIONS,
    DEFAULT_MARKETS,
    DEFAULT_ORB_RANGE_FILTERS,
    DEFAULT_RR_TARGETS,
    DEFAULT_TRADE_DIRECTION_MODES,
    MARKET_LABELS,
    build_market_scenarios,
    parse_breakout_windows,
    parse_force_close_options,
    parse_markets,
    parse_orb_range_filters,
    parse_orb_range_quantiles,
    parse_rr_targets,
    parse_trade_direction_modes,
)
from data_utils import load_csv_market_data, prepare_external_market_data
from reporting import save_scenario_outputs, split_primary_secondary, summarize_outputs


def fetch_provider_data(symbol: str, period: str, interval: str) -> pd.DataFrame:
    print(f"Scarico {symbol} | period={period} | interval={interval}")
    df = yf.download(
        tickers=symbol,
        period=period,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df.empty:
        raise ValueError(f"Nessun dato ricevuto per {symbol}")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    return df


def _print_data_diagnostics(market_label: str, diagnostics: dict[str, Any]) -> None:
    print(f"\nData diagnostics [{market_label}] ({diagnostics.get('data_source', 'unknown')}):")

    rows_read = diagnostics.get("csv_rows_read", diagnostics.get("provider_rows_read", 0))
    print(
        "  rows: "
        f"letto={rows_read}, "
        f"pulito_5m={diagnostics.get('rows_after_cleaning', 0)}, "
        f"m15_pronto={diagnostics.get('m15_ready_bars', 0)}"
    )
    print(
        "  timezone: "
        f"sorgente={diagnostics.get('source_timezone', 'n/a')} -> "
        f"finale={diagnostics.get('target_timezone', 'n/a')}"
    )
    print(
        "  pulizia_5m: "
        f"bad_datetime={diagnostics.get('dropped_bad_datetime_rows', 0)}, "
        f"bad_ohlc={diagnostics.get('dropped_bad_ohlc_rows', 0)}, "
        f"high_lt_low={diagnostics.get('dropped_bad_high_low_rows', 0)}, "
        f"duplicati={diagnostics.get('dropped_duplicate_rows', 0)}"
    )
    print(
        "  blocchi_15m: "
        f"totali={diagnostics.get('m15_total_bars', 0)}, "
        f"incompleti={diagnostics.get('m15_incomplete_bars', 0)}, "
        f"policy={diagnostics.get('m15_incomplete_policy', 'n/a')}, "
        f"scartati={diagnostics.get('m15_dropped_incomplete_bars', 0)}"
    )
    print(
        "  orb_0930: "
        f"presenti={diagnostics.get('m15_orb_0930_bars', 0)}, "
        f"incomplete={diagnostics.get('m15_orb_0930_incomplete_bars', 0)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest ORB v1.6")
    parser.add_argument("--csv-path", default=None, help="Percorso CSV 5m sorgente primaria")
    parser.add_argument(
        "--csv-timezone",
        default="UTC",
        help="Timezone dei timestamp CSV (es: UTC, America/New_York)",
    )
    parser.add_argument(
        "--m15-incomplete-policy",
        choices=["drop", "keep"],
        default="drop",
        help="Gestione blocchi 15m incompleti derivati dal 5m (default: drop)",
    )
    parser.add_argument("--period", default="120d", help="Periodo storico (default: 120d)")
    parser.add_argument("--interval", default="5m", help="Timeframe dati (default: 5m)")
    parser.add_argument(
        "--markets",
        default=DEFAULT_MARKETS,
        help=f"Mercati separati da virgola (default: {DEFAULT_MARKETS})",
    )
    parser.add_argument(
        "--force-close-options",
        default=DEFAULT_FORCE_CLOSE_OPTIONS,
        help=f"Scenari force-close, default: {DEFAULT_FORCE_CLOSE_OPTIONS}",
    )
    parser.add_argument(
        "--breakout-windows",
        default=DEFAULT_BREAKOUT_WINDOWS,
        help=f"Fine finestra breakout separata da virgola, default: {DEFAULT_BREAKOUT_WINDOWS}",
    )
    parser.add_argument(
        "--orb-range-filters",
        default=DEFAULT_ORB_RANGE_FILTERS,
        help=(
            "Filtri classe ORB range separati da virgola. "
            "Valori: all, small, medium, large, small+medium, medium+large, small+large"
        ),
    )
    parser.add_argument(
        "--orb-range-quantiles",
        default="0.33,0.66",
        help="Quantili classe ORB range (small/medium/large), esempio: 0.33,0.66",
    )
    parser.add_argument(
        "--rr-targets",
        default=DEFAULT_RR_TARGETS,
        help="Multipli RR separati da virgola, esempio: 1.0,1.5,2.0,3.0",
    )
    parser.add_argument(
        "--trade-direction-modes",
        default=DEFAULT_TRADE_DIRECTION_MODES,
        help="Modalita direzione trade: both,long_only,short_only",
    )
    parser.add_argument("--max-trades-per-day", type=int, default=2, help="Max trade giornalieri (default: 2)")
    parser.add_argument("--capital", type=float, default=10000.0, help="Capitale iniziale equity model")
    parser.add_argument(
        "--risk-per-trade",
        type=float,
        default=0.01,
        help="Rischio per trade nell'equity model (default: 0.01)",
    )
    parser.add_argument("--output-dir", default="outputs", help="Cartella output")
    args = parser.parse_args()

    if args.max_trades_per_day <= 0:
        print("Errore parametri: --max-trades-per-day deve essere > 0")
        sys.exit(1)

    if not (0 < args.risk_per_trade < 1):
        print("Errore parametri: --risk-per-trade deve essere tra 0 e 1")
        sys.exit(1)

    try:
        markets = parse_markets(args.markets)
        force_close_options = parse_force_close_options(args.force_close_options)
        breakout_windows = parse_breakout_windows(args.breakout_windows)
        orb_range_filters = parse_orb_range_filters(args.orb_range_filters)
        orb_range_quantiles = parse_orb_range_quantiles(args.orb_range_quantiles)
        rr_targets = parse_rr_targets(args.rr_targets)
        trade_direction_modes = parse_trade_direction_modes(args.trade_direction_modes)
    except ValueError as exc:
        print(f"Errore parametri: {exc}")
        sys.exit(1)

    if args.csv_path and len(markets) != 1:
        print("Errore parametri: in modalita CSV specifica un solo mercato per run.")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    all_results = []
    drop_incomplete_15m_blocks = args.m15_incomplete_policy == "drop"

    csv_data_5m = None
    csv_data_15m = None
    csv_diagnostics = None
    if args.csv_path:
        try:
            csv_data_5m, csv_data_15m, csv_diagnostics = load_csv_market_data(
                csv_path=args.csv_path,
                csv_timezone=args.csv_timezone,
                drop_incomplete_15m_blocks=drop_incomplete_15m_blocks,
            )
        except Exception as exc:
            print(f"Errore CSV: {exc}")
            sys.exit(1)

    for market_code in markets:
        market_label = MARKET_LABELS[market_code]
        market_scenarios = build_market_scenarios(
            market_code=market_code,
            force_close_options=force_close_options,
            breakout_windows=breakout_windows,
            orb_range_filters=orb_range_filters,
            rr_targets=rr_targets,
            trade_direction_modes=trade_direction_modes,
        )

        if not market_scenarios:
            print(f"Nessuno scenario per {market_label}")
            continue

        symbol = market_scenarios[0].symbol
        print("\n" + "#" * 100)
        print(f"BACKTEST MERCATO: {market_label} ({symbol})")
        print("#" * 100)

        try:
            if csv_data_5m is not None and csv_data_15m is not None and csv_diagnostics is not None:
                market_df_5m = csv_data_5m
                market_df_15m = csv_data_15m
                data_diagnostics = csv_diagnostics
                print(f"Uso sorgente CSV: {args.csv_path}")
            else:
                provider_raw = fetch_provider_data(symbol=symbol, period=args.period, interval=args.interval)
                market_df_5m, market_df_15m, data_diagnostics = prepare_external_market_data(
                    raw_df=provider_raw,
                    source_timezone_fallback="UTC",
                    drop_incomplete_15m_blocks=drop_incomplete_15m_blocks,
                )
        except Exception as exc:
            source_label = "CSV" if args.csv_path else "download"
            print(f"Errore {source_label} {market_label}: {exc}")
            continue

        _print_data_diagnostics(market_label, data_diagnostics)

        for scenario in market_scenarios:
            result = run_market_backtest(
                df=market_df_5m,
                df_15m=market_df_15m,
                scenario=scenario,
                max_trades_per_day=args.max_trades_per_day,
                initial_capital=args.capital,
                risk_per_trade=args.risk_per_trade,
                orb_range_class_config=orb_range_quantiles,
            )

            print("\n" + format_market_report(result))
            paths = save_scenario_outputs(result=result, output_dir=output_dir)
            print(summarize_outputs(paths))
            all_results.append(result)

    print("\n" + "=" * 100)
    print("PERFORMANCE PER MERCATO / SCENARIO")
    print("=" * 100)
    table = performance_per_market(all_results)
    if table.empty:
        print("Nessun risultato disponibile.")
        return

    primary, secondary = split_primary_secondary(table)

    if not primary.empty:
        print(
            "\nScenari focus v1.6 (SP500, no_time_close, 10:30, "
            "small|small+large, RR 1.0|1.5, both|long_only|short_only):"
        )
        print(primary.to_string(index=False))

    if not secondary.empty:
        print("\nAltri scenari eseguiti:")
        print(secondary.to_string(index=False))


if __name__ == "__main__":
    main()
